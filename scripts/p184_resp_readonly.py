"""Read-only Redis access and shared helpers for the P18.4 final gate-campaign probes (G3, G8, G9, G11).

Purpose: one small stdlib RESP2 client so the campaign probes run on the host without a Redis library, without
`docker exec` per sample and without a new container. It speaks only an allowlist of read-only commands (PING, GET,
MGET, PTTL, SCAN, INFO, SLOWLOG GET/LEN, CLIENT LIST, CONFIG GET, SELECT of a DB index for the connection); MONITOR,
writes, FLUSH*, CLIENT KILL, SLOWLOG RESET and CONFIG SET are refused before anything is sent.

Also: a stop flag set by SIGTERM/SIGINT (probes finish their summary instead of dying), container IP resolution (read-only `docker inspect`), nearest-rank quantiles (the same rule as
scripts/p184_g2_mark_trace_transit.py: sorted[min(n-1, int(q*n))], p99 only when n >= 100), and a bounded JSONL writer
that stops writing (and says so) at its byte cap instead of growing without limit.

Boundary: no Redis mutation, no Docker mutation, no secrets read. Values read from Redis are parsed by the callers and
never written whole to evidence.
"""
from __future__ import annotations

import json
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

READ_ONLY = {
    ("PING",), ("GET",), ("MGET",), ("PTTL",), ("SCAN",), ("INFO",), ("SLOWLOG", "GET"), ("SLOWLOG", "LEN"),
    ("CLIENT", "LIST"), ("CONFIG", "GET"), ("SELECT",),
}


class RespError(RuntimeError):
    pass


def allowed(args) -> bool:
    if not args:
        return False
    head = str(args[0]).upper()
    if (head,) in READ_ONLY:
        return True
    return len(args) > 1 and (head, str(args[1]).upper()) in READ_ONLY


def encode(args) -> bytes:
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        data = arg if isinstance(arg, bytes) else str(arg).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(data), data))
    return b"".join(parts)


def parse(reader):
    """Parse one RESP2 reply from a binary file-like object (readline/read)."""
    line = reader.readline()
    if not line.endswith(b"\r\n"):
        raise RespError("connection closed")
    kind, rest = line[:1], line[1:-2]
    if kind == b"+":
        return rest.decode()
    if kind == b"-":
        raise RespError(rest.decode(errors="replace")[:200])
    if kind == b":":
        return int(rest)
    if kind == b"$":
        size = int(rest)
        if size < 0:
            return None
        data = reader.read(size + 2)
        return data[:-2]
    if kind == b"*":
        size = int(rest)
        return None if size < 0 else [parse(reader) for _ in range(size)]
    raise RespError("unknown RESP type")


class ReadOnlyRedis:
    def __init__(self, host: str, port: int = 6379, db: int = 0, timeout: float = 2.0):
        self.host, self.port, self.db, self.timeout = host, port, db, timeout
        self._sock = None
        self._reader = None

    def _connect(self):
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._reader = self._sock.makefile("rb")
        if self.db:
            self._call(("SELECT", self.db))

    def close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = self._reader = None

    def _call(self, args):
        self._sock.sendall(encode(args))
        return parse(self._reader)

    def call(self, *args):
        if not allowed(args):
            raise PermissionError(f"command not in the read-only allowlist: {args[:2]}")
        if self._sock is None:
            self._connect()
        try:
            return self._call(args)
        except (OSError, RespError):
            self.close()
            raise

    def pipeline(self, commands):
        """Send several allowlisted commands in one round trip; returns replies in order."""
        for args in commands:
            if not allowed(args):
                raise PermissionError(f"command not in the read-only allowlist: {args[:2]}")
        if self._sock is None:
            self._connect()
        try:
            self._sock.sendall(b"".join(encode(args) for args in commands))
            return [parse(self._reader) for _ in commands]
        except (OSError, RespError):
            self.close()
            raise

    def scan(self, pattern: str, count: int = 1000, limit: int = 10000):
        keys, cursor = [], b"0"
        while True:
            cursor, batch = self.call("SCAN", cursor, "MATCH", pattern, "COUNT", count)
            keys.extend(item.decode() for item in batch)
            if cursor in (b"0", "0") or len(keys) >= limit:
                return sorted(set(keys))[:limit]


def container_ip(name: str, network: str | None = None) -> str:
    """Read-only `docker inspect` of a container's IP (the named network, else the first one)."""
    out = subprocess.run(["docker", "inspect", name, "--format", "{{json .NetworkSettings.Networks}}"],
                         capture_output=True, text=True, timeout=20, check=True).stdout
    networks = json.loads(out)
    if network is not None:
        return networks[network]["IPAddress"]
    for value in networks.values():
        if value.get("IPAddress"):
            return value["IPAddress"]
    raise RuntimeError(f"no IP for {name}")


def parse_redis_url(url: str) -> tuple[str, int, int]:
    """redis://host[:port][/db] -> (host, port, db). Credentials are refused (none are configured on this host)."""
    if not url.startswith("redis://"):
        raise ValueError("only redis:// URLs")
    rest = url[len("redis://"):]
    if "@" in rest:
        raise ValueError("credentials in a Redis URL are not handled by the read-only probes")
    hostport, _, db = rest.partition("/")
    host, _, port = hostport.partition(":")
    return host, int(port or 6379), int(db or 0)


def quantiles(values):
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    n = len(ordered)
    pick = lambda q: round(ordered[min(n - 1, int(q * n))], 3)  # noqa: E731
    return {"n": n, "p50": pick(.5), "p95": pick(.95), "p99": pick(.99) if n >= 100 else None,
            "max": round(ordered[-1], 3), "min": round(ordered[0], 3)}


class BoundedJsonl:
    """Append-only JSONL evidence created exclusively; stops at max_bytes and records that it stopped."""

    def __init__(self, path: Path, max_bytes: int):
        self.path, self.max_bytes, self.written, self.capped = Path(path), max_bytes, 0, False
        self._handle = self.path.open("x", encoding="utf-8")

    def write(self, value: dict) -> bool:
        if self.capped:
            return False
        line = json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n"
        if self.written + len(line) > self.max_bytes:
            self.capped = True
            self._handle.write(json.dumps({"kind": "BYTE_CAP_REACHED", "at_ns": time.time_ns()}) + "\n")
            self._handle.flush()
            return False
        self._handle.write(line)
        self._handle.flush()
        self.written += len(line)
        return True

    def close(self):
        self._handle.close()


def read_jsonl(path: Path):
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_json_exclusive(path: Path, value) -> None:
    with Path(path).open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(value, indent=1, sort_keys=True) + "\n")


_STOP = threading.Event()


def install_stop_signals() -> None:
    """SIGTERM/SIGINT end a probe's loop early; the probe still writes its summary (clean exit, no lost evidence)."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: _STOP.set())


def stopped() -> bool:
    return _STOP.is_set()


def pause(seconds: float) -> None:
    if seconds > 0:
        _STOP.wait(seconds)

#!/usr/bin/env python3
"""Run a bounded, manifest-derived Phase-3 V2 consumer load receipt.

The host half has no SDK imports and launches one disposable client container.
The inner half uses the production SDK against only the existing V2 Query and
Stream endpoints.  It has no V1 URL, provider URL, Docker socket, order client,
or write-capable market-data mount.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import statistics
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
_IDENTITY_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
_IDENTITY_RE = re.compile(r"[a-z][a-z0-9.-]{2,127}")
_NETWORK_RE = re.compile(r"[A-Za-z0-9_.-]+")
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_ALLOWED_SESSION_COUNTS = frozenset({5, 20, 35, 50})
_FIVE_LIQUID = {
    "BINANCE": frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"}),
    "OKX": frozenset({"BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"}),
}
_PHASE3_CONSUMER_MANIFESTS = {
    "trading-system.paper.stable": "/app/consumers/stable/trading-system-paper.yaml",
    "alpha.binance.paper.stable": "/app/consumers/stable/alpha-binance-paper.yaml",
    "alpha.okx.paper.stable": "/app/consumers/stable/alpha-okx-paper.yaml",
    "monitoring.multivenue.stable": "/app/consumers/stable/monitoring-multivenue.yaml",
}
_ISSUER = "https://identity.qdl.stable.internal"
_AUDIENCE = "qdl-v2-stable"
_ROLES = ("historical_reader", "market_data_reader", "stream_consumer")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_error(error: BaseException) -> dict[str, object]:
    code = getattr(error, "code", None)
    return {
        "type": type(error).__name__,
        "code": str(code) if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_]{1,80}", code) else None,
        "detail_sha256": _sha256(str(getattr(error, "detail", error)).encode()),
    }


def _stream_frame_quality_diagnostic(event, requirement, *, now_ns: int | None = None) -> dict[str, object]:
    """Return bounded non-secret quality context for a rejected stream frame.

    The load receipt needs enough information to distinguish a stale provider
    event from a stale transport/session without recording raw payloads,
    prices, timestamps, cursor material, or identity secrets.
    """

    from qdl.common.v1 import common_pb2

    envelope = getattr(event, "event", None)
    current_ns = time.time_ns() if now_ns is None else now_ns

    def integer(value: object) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def age_ms(value: object) -> int | None:
        timestamp_ns = integer(value)
        if timestamp_ns is None or timestamp_ns <= 0:
            return None
        return max(0, (current_ns - timestamp_ns) // 1_000_000)

    def policy_name(value: object) -> str | None:
        candidate = getattr(value, "value", None)
        return candidate if isinstance(candidate, str) else None

    flags: list[str] = []
    for value in getattr(envelope, "quality_flags", ()):
        try:
            flags.append(
                common_pb2.QualityFlag.Name(int(value)).removeprefix("QUALITY_FLAG_")
            )
        except (TypeError, ValueError):
            flags.append("UNKNOWN")

    return {
        "logical_offset": integer(getattr(event, "logical_offset", None)),
        "source_event_age_ms": age_ms(getattr(envelope, "source_event_time_ns", None)),
        "receive_age_ms": age_ms(getattr(envelope, "received_at_ns", None)),
        "max_freshness_ms": integer(getattr(requirement, "max_freshness_ms", None)),
        "event_recency_policy": policy_name(
            getattr(requirement, "effective_event_recency_policy", None)
        ),
        "max_session_liveness_ms": integer(
            getattr(requirement, "max_session_liveness_ms", None)
        ),
        "stale_policy": policy_name(getattr(requirement, "stale_policy", None)),
        "gap_policy": policy_name(getattr(requirement, "gap_policy", None)),
        "quality_flags": sorted(set(flags)),
        "connection_generation": integer(
            getattr(envelope, "connection_generation", None)
        ),
        "lease_epoch": integer(getattr(envelope, "lease_epoch", None)),
        "authority_revision": integer(
            getattr(envelope, "authority_revision", None)
        ),
        "config_revision": integer(getattr(envelope, "config_revision", None)),
    }


def _percentiles(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50_ms": round(statistics.median(ordered), 3),
        "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "p99_ms": round(ordered[math.ceil(len(ordered) * 0.99) - 1], 3) if len(ordered) >= 100 else None,
        "max_ms": round(ordered[-1], 3),
    }


def _byte_summary(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {
            "n": 0,
            "p50_bytes": None,
            "p95_bytes": None,
            "p99_bytes": None,
            "max_bytes": None,
            "total_bytes": 0,
        }
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "p50_bytes": round(statistics.median(ordered)),
        "p95_bytes": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99_bytes": ordered[math.ceil(len(ordered) * 0.99) - 1] if len(ordered) >= 100 else None,
        "max_bytes": ordered[-1],
        "total_bytes": sum(ordered),
    }


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase-3 profile is not readable JSON") from error
    if not isinstance(value, dict):
        raise ValueError("Phase-3 profile must be an object")
    return value


def _approved_identity_path(value: object) -> bool:
    """Accept a readable file or a Docker-mountable protected state file.

    The private alpha/monitoring directories are intentionally mode `0700` for
    the runtime UID, so the host launcher cannot stat their leaves. Docker
    mounts them read-only and the disposable bootstrap copies them only into
    tmpfs before dropping privileges. A protected path is therefore accepted
    only below the one managed QDL state root; arbitrary host files never are.
    """

    if not isinstance(value, str) or not value or "\n" in value:
        return False
    path = Path(value)
    if not path.is_absolute() or path.name not in {"ca.crt", "client.crt", "client.key", "private.key"}:
        return False
    try:
        exists_as_file = path.is_file()
    except OSError:
        exists_as_file = False
    if exists_as_file:
        return True
    try:
        path.resolve(strict=False).relative_to(_IDENTITY_STATE_ROOT.resolve())
    except (OSError, ValueError):
        return False
    return True


def validate_profile(raw: dict[str, object]) -> dict[str, object]:
    expected = {
        "image", "network", "runtime_dir", "queries", "stream_targets",
        "query_containers", "identities",
    }
    if set(raw) != expected:
        raise ValueError("Phase-3 profile fields are incomplete or unknown")
    image = raw["image"]
    network = raw["network"]
    runtime_dir = raw["runtime_dir"]
    queries = raw["queries"]
    streams = raw["stream_targets"]
    containers = raw["query_containers"]
    identities = raw["identities"]
    if not isinstance(image, str) or not image.strip():
        raise ValueError("Phase-3 profile requires an existing image")
    if not isinstance(network, str) or not _NETWORK_RE.fullmatch(network):
        raise ValueError("Phase-3 profile network is invalid")
    if not isinstance(runtime_dir, str) or not Path(runtime_dir).is_dir():
        raise ValueError("Phase-3 runtime directory is unavailable")
    if not isinstance(queries, list) or len(queries) != 2 or any(
        not isinstance(item, str) or not re.fullmatch(r"https://[A-Za-z0-9_.-]+:[0-9]+", item)
        for item in queries
    ):
        raise ValueError("Phase-3 profile requires exactly two HTTPS Query replicas")
    if not isinstance(streams, list) or len(streams) != 2 or any(
        not isinstance(item, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+:[0-9]+", item)
        for item in streams
    ):
        raise ValueError("Phase-3 profile requires exactly two Stream replicas")
    if not isinstance(containers, list) or len(containers) != 2 or any(
        not isinstance(item, str) or not _NETWORK_RE.fullmatch(item) for item in containers
    ):
        raise ValueError("Phase-3 profile requires exactly two Query container names")
    if not isinstance(identities, list) or not 1 <= len(identities) <= 4:
        raise ValueError("Phase-3 profile requires one to four identities")
    seen: set[str] = set()
    normalized: list[dict[str, object]] = []
    for item in identities:
        if not isinstance(item, dict) or set(item) != {"id", "tls", "jwt"}:
            raise ValueError("Phase-3 identity fields are invalid")
        consumer_id = item["id"]
        tls = item["tls"]
        jwt = item["jwt"]
        if not isinstance(consumer_id, str) or not _IDENTITY_RE.fullmatch(consumer_id) or consumer_id in seen:
            raise ValueError("Phase-3 identity ID is invalid")
        seen.add(consumer_id)
        if not isinstance(tls, dict) or set(tls) != {"ca_file", "cert_file", "key_file"}:
            raise ValueError("Phase-3 identity TLS fields are invalid")
        if not isinstance(jwt, dict) or set(jwt) != {"private_key_file", "key_id"}:
            raise ValueError("Phase-3 identity JWT fields are invalid")
        paths = [*tls.values(), jwt["private_key_file"]]
        if any(not _approved_identity_path(path) for path in paths):
            raise ValueError("Phase-3 identity file is unavailable")
        if not isinstance(jwt["key_id"], str) or not jwt["key_id"].strip():
            raise ValueError("Phase-3 JWT key ID is invalid")
        normalized.append({"id": consumer_id, "tls": dict(tls), "jwt": dict(jwt)})
    return {
        "image": image,
        "network": network,
        "runtime_dir": str(Path(runtime_dir).resolve()),
        "queries": list(queries),
        "stream_targets": list(streams),
        "query_containers": list(containers),
        "identities": normalized,
    }


def _docker_image_id(image: str) -> str:
    value = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if not _DIGEST_RE.fullmatch(value):
        raise ValueError("Phase-3 image did not resolve to a local immutable digest")
    return value


def _docker_stats(containers: list[str]) -> list[dict[str, object]]:
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *containers],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    values = []
    for line in result.stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            values.append(value)
    return values


def _cleanup_exact_container(name: str) -> None:
    existing = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if existing:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, check=True, timeout=30)
    remaining = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"name=^/{name}$"],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    ).stdout.strip()
    if remaining:
        raise RuntimeError("Phase-3 disposable client cleanup failed")


def _identity_slug(consumer_id: str) -> str:
    return consumer_id.replace(".", "-")


def docker_command(
    profile: dict[str, object],
    *,
    name: str,
    image_id: str,
    mode: str,
    sessions: int,
    duration_seconds: int,
) -> tuple[list[str], dict[str, object]]:
    identities = profile["identities"]
    assert isinstance(identities, list)
    inner_identities = []
    mounts: list[str] = []
    for raw in identities:
        assert isinstance(raw, dict)
        consumer_id = str(raw["id"])
        slug = _identity_slug(consumer_id)
        tls = raw["tls"]
        jwt = raw["jwt"]
        assert isinstance(tls, dict) and isinstance(jwt, dict)
        destination = f"/identity/{slug}"
        fields = {
            "ca_file": (str(tls["ca_file"]), f"{destination}/tls/ca.crt"),
            "cert_file": (str(tls["cert_file"]), f"{destination}/tls/client.crt"),
            "key_file": (str(tls["key_file"]), f"{destination}/tls/client.key"),
            "private_key_file": (str(jwt["private_key_file"]), f"{destination}/jwt/private.key"),
        }
        for source, target in fields.values():
            mounts.extend(["--mount", f"type=bind,src={source},dst={target},readonly"])
        inner_identities.append({
            "id": consumer_id,
            "tls": {
                "ca_file": f"/tmp/identity/{slug}/tls/ca.crt",
                "cert_file": f"/tmp/identity/{slug}/tls/client.crt",
                "key_file": f"/tmp/identity/{slug}/tls/client.key",
            },
            "jwt": {"private_key_file": f"/tmp/identity/{slug}/jwt/private.key", "key_id": str(jwt["key_id"])},
        })
    inner = {
        "mode": mode,
        "logical_sessions": sessions,
        "duration_seconds": duration_seconds,
        "catalog": "/runtime/stable-source-bindings.yaml",
        "acquisition": "/runtime/stable-acquisition-bindings.yaml",
        "queries": profile["queries"],
        "stream_targets": profile["stream_targets"],
        "identities": inner_identities,
    }
    cmd = [
        "docker", "run", "--rm", "--name", name,
        "--network", str(profile["network"]),
        "--user", "0:0",
        "--read-only",
        "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=96m",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "256",
        "--memory", "512m",
        "--cpus", "1.0",
        "--label", "qdl.phase3.disposable-load=true",
        "--env", "PYTHONDONTWRITEBYTECODE=1",
        "--env", "PYTHONPATH=/app:/driver",
        "--env", "QDL_PHASE3_LOAD_CONFIG=" + json.dumps(inner, sort_keys=True, separators=(",", ":")),
        "--mount", f"type=bind,src={profile['runtime_dir']},dst=/runtime,readonly",
        "--mount", f"type=bind,src={ROOT / 'qdl/certification/phase3_consumer_load.py'},dst=/app/qdl/certification/phase3_consumer_load.py,readonly",
        "--mount", f"type=bind,src={Path(__file__).resolve()},dst=/driver/phase3_consumer_load_acceptance.py,readonly",
        "--mount", f"type=bind,src={ROOT / 'scripts/phase3_consumer_load_bootstrap.sh'},dst=/driver/phase3_consumer_load_bootstrap.sh,readonly",
        *mounts,
        image_id,
        "/bin/sh", "/driver/phase3_consumer_load_bootstrap.sh",
    ]
    return cmd, inner


def _parse_receipt(stdout: str) -> dict[str, object] | None:
    for line in reversed(stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("schema") == "qdl.phase3.consumer-load.v1":
            return value
    return None


def run_host(args: argparse.Namespace) -> int:
    profile = validate_profile(_read_json(args.profile))
    if {item["id"] for item in profile["identities"]} != set(_PHASE3_CONSUMER_MANIFESTS):
        raise ValueError("Phase-3 requires exactly the four approved crypto consumer identities")
    if args.sessions not in _ALLOWED_SESSION_COUNTS:
        raise ValueError("Phase-3 sessions must be exactly one of 5,20,35,50")
    if args.mode == "final" and (args.sessions != 50 or args.duration_seconds != 300):
        raise ValueError("Phase-3 final acceptance requires exactly 50 sessions for 300 seconds")
    if args.mode == "matrix" and args.duration_seconds != 0:
        raise ValueError("Phase-3 matrix does not accept an observation duration")
    if args.mode == "load" and not 30 <= args.duration_seconds <= 240:
        raise ValueError("Phase-3 staged load duration must be 30..240 seconds")
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    image_id = _docker_image_id(str(profile["image"]))
    name = "qdl-phase3-load-" + uuid.uuid4().hex[:12]
    command, inner = docker_command(
        profile,
        name=name,
        image_id=image_id,
        mode=args.mode,
        sessions=args.sessions,
        duration_seconds=args.duration_seconds,
    )
    before = _docker_stats(list(profile["query_containers"]))
    started = time.monotonic()
    process = None
    cleanup_error = None
    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=max(180, args.duration_seconds + 720),
        )
    finally:
        try:
            _cleanup_exact_container(name)
        except Exception as error:  # cleanup must remain visible in the receipt
            cleanup_error = _safe_error(error)
    after = _docker_stats(list(profile["query_containers"]))
    stdout = process.stdout if process is not None else ""
    stderr = process.stderr if process is not None else ""
    receipt = _parse_receipt(stdout)
    host = {
        "schema": "qdl.phase3.consumer-load-host.v1",
        "status": "PASS" if process is not None and process.returncode == 0 and receipt and receipt.get("status") == "PASS" and cleanup_error is None else "FAIL",
        "image_id": image_id,
        "source_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip(),
        "tool_sha256": _sha256(Path(__file__).read_bytes()),
        "bootstrap_sha256": _sha256((ROOT / "scripts/phase3_consumer_load_bootstrap.sh").read_bytes()),
        "planner_sha256": _sha256((ROOT / "qdl/certification/phase3_consumer_load.py").read_bytes()),
        "mode": args.mode,
        "logical_sessions": args.sessions,
        "duration_seconds": args.duration_seconds,
        "authenticated_identity_count": len(profile["identities"]),
        "query_containers": list(profile["query_containers"]),
        "query_stats_before": before,
        "query_stats_after": after,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "client_returncode": None if process is None else process.returncode,
        "client_stdout_sha256": _sha256(stdout.encode()),
        "client_stderr_sha256": _sha256(stderr.encode()),
        "cleanup_error": cleanup_error,
        "inner_config_sha256": _sha256(json.dumps(inner, sort_keys=True, separators=(",", ":")).encode()),
        "secret_values_recorded": False,
        "order_actions": 0,
    }
    (output / "host.json").write_text(json.dumps(host, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if receipt is not None:
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "schema": host["schema"], "status": host["status"], "output": str(output),
        "client_returncode": host["client_returncode"], "elapsed_seconds": host["elapsed_seconds"],
    }, sort_keys=True))
    return 0 if host["status"] == "PASS" else 1


@dataclass(slots=True)
class _HandoffReservation:
    """Two quota slots reserved for one snapshot-to-stream handoff."""

    stream_at: float
    state: str = "QUERY"


@dataclass(slots=True)
class _Pacer:
    spacing_seconds: float
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    _lock: asyncio.Lock = field(init=False, repr=False)
    _next_at: float | None = field(init=False, default=None)
    _wait_ms: float = field(init=False, default=0.0)
    _operations: Counter[str] = field(init=False, default_factory=Counter)
    _handoff_reservations: int = field(init=False, default=0)
    _incomplete_handoffs: int = field(init=False, default=0)
    _measurement_wait_ms: ContextVar[float] = field(
        init=False,
        repr=False,
        default_factory=lambda: ContextVar("phase3_pacer_measurement_wait_ms", default=0.0),
    )
    _handoff: ContextVar[_HandoffReservation | None] = field(
        init=False,
        repr=False,
        default_factory=lambda: ContextVar("phase3_pacer_handoff", default=None),
    )

    def __post_init__(self) -> None:
        if self.spacing_seconds <= 0:
            raise ValueError("load pacer spacing must be positive")
        self._lock = asyncio.Lock()

    async def acquire(self, operation: str) -> None:
        reservation = self._handoff.get()
        if reservation is not None and reservation.state == "QUERY":
            if operation not in {"snapshot", "warmup"}:
                raise RuntimeError("Phase-3 handoff expected snapshot or warmup")
            async with self._lock:
                reservation.state = "STREAM"
                self._operations[operation] += 1
            return
        if reservation is not None and reservation.state == "STREAM":
            if operation != "stream_subscribe":
                raise RuntimeError("Phase-3 handoff expected stream subscribe")
            async with self._lock:
                wait = max(0.0, reservation.stream_at - self.clock())
                self._operations[operation] += 1
                self._wait_ms += wait * 1000.0
                self._measurement_wait_ms.set(
                    self._measurement_wait_ms.get() + wait * 1000.0
                )
                reservation.state = "COMPLETE"
            if wait:
                await self.sleep(wait)
            return
        async with self._lock:
            now = self.clock()
            target = now if self._next_at is None else max(now, self._next_at)
            self._next_at = target + self.spacing_seconds
            self._operations[operation] += 1
            wait = max(0.0, target - now)
            self._wait_ms += wait * 1000.0
            self._measurement_wait_ms.set(
                self._measurement_wait_ms.get() + wait * 1000.0
            )
        if wait:
            await self.sleep(wait)

    @asynccontextmanager
    async def handoff(self):
        """Reserve consecutive quota slots before a snapshot-to-stream join.

        The signed cursor from a snapshot is only a safe live handoff when its
        matching subscription is not delayed behind unrelated work. Reserving
        both slots first preserves the per-identity leaky-bucket rate while
        limiting the post-snapshot delay to one declared spacing interval.
        """

        if self._handoff.get() is not None:
            raise RuntimeError("Phase-3 handoff reservation cannot be nested")
        async with self._lock:
            now = self.clock()
            snapshot_at = now if self._next_at is None else max(now, self._next_at)
            stream_at = snapshot_at + self.spacing_seconds
            self._next_at = stream_at + self.spacing_seconds
            wait = max(0.0, snapshot_at - now)
            self._wait_ms += wait * 1000.0
            self._measurement_wait_ms.set(
                self._measurement_wait_ms.get() + wait * 1000.0
            )
            self._handoff_reservations += 1
        if wait:
            await self.sleep(wait)
        reservation = _HandoffReservation(stream_at=stream_at)
        token = self._handoff.set(reservation)
        try:
            yield
        finally:
            if reservation.state != "COMPLETE":
                self._incomplete_handoffs += 1
            self._handoff.reset(token)

    def begin_measurement(self):
        return self._measurement_wait_ms.set(0.0)

    def finish_measurement(self, token) -> float:
        wait_ms = self._measurement_wait_ms.get()
        self._measurement_wait_ms.reset(token)
        return wait_ms

    def evidence(self) -> dict[str, object]:
        return {
            "spacing_seconds": round(self.spacing_seconds, 6),
            "queue_wait_ms": round(self._wait_ms, 3),
            "operations": dict(sorted(self._operations.items())),
            "handoff_reservations": self._handoff_reservations,
            "incomplete_handoffs": self._incomplete_handoffs,
        }


class _PacedQueryTransport:
    def __init__(self, delegate, pacer: _Pacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def _call(self, operation: str, *args, **kwargs):
        await self._pacer.acquire(operation)
        return await getattr(self._delegate, operation)(*args, **kwargs)

    async def snapshot(self, *args, **kwargs):
        return await self._call("snapshot", *args, **kwargs)

    async def warmup(self, *args, **kwargs):
        return await self._call("warmup", *args, **kwargs)

    async def warmup_batch(self, *args, **kwargs):
        return await self._call("warmup_batch", *args, **kwargs)

    async def reference_batch(self, *args, **kwargs):
        return await self._call("reference_batch", *args, **kwargs)

    async def feed_status(self, *args, **kwargs):
        return await self._call("feed_status", *args, **kwargs)

    async def instruments(self, *args, **kwargs):
        return await self._call("instruments", *args, **kwargs)

    async def instrument(self, *args, **kwargs):
        return await self._call("instrument", *args, **kwargs)

    async def close(self) -> None:
        await self._delegate.close()


class _PacedStreamTransport:
    def __init__(self, delegate, pacer: _Pacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def subscribe(self, *args, **kwargs):
        await self._pacer.acquire("stream_subscribe")
        async for item in self._delegate.subscribe(*args, **kwargs):
            yield item

    async def close(self) -> None:
        await self._delegate.close()


@dataclass(frozen=True, slots=True)
class _StreamSpec:
    name: str
    consumer_id: str
    product: object
    slow: bool
    purpose: str


@dataclass(frozen=True, slots=True)
class _Identity:
    consumer_id: str
    tls: object
    credential: object
    max_buffer_events: int


def _product_evidence(product) -> dict[str, object]:
    return {
        "consumer_id": product.consumer_id,
        "venue": product.venue,
        "native_symbol": product.native_symbol,
        "feed": product.feed.value,
        "interval": product.interval,
        "delivery": product.delivery.value,
        "source_policy_id": product.source_policy_id,
    }


def _product_group(product, operation: str, replica: str) -> tuple[str, ...]:
    return (
        operation, replica, product.venue, product.native_symbol,
        product.feed.value, product.interval or "",
    )


def _summarize_samples(samples: list[dict[str, object]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, ...], dict[str, list[float] | list[int]]] = defaultdict(
        lambda: {"usable": [], "queue": [], "bytes": []}
    )
    for sample in samples:
        latency = sample.get("usable_ms")
        if isinstance(latency, float):
            groups[tuple(sample["group"])]["usable"].append(latency)
        queue_wait = sample.get("queue_wait_ms")
        if isinstance(queue_wait, float):
            groups[tuple(sample["group"])]["queue"].append(queue_wait)
        response_bytes = sample.get("response_payload_bytes")
        if isinstance(response_bytes, int):
            groups[tuple(sample["group"])]["bytes"].append(response_bytes)
    return [
        {
            "operation": key[0], "replica": key[1], "venue": key[2],
            "native_symbol": key[3], "feed": key[4], "interval": key[5] or None,
            "usable_latency": _percentiles(values["usable"]),
            "client_pacing_wait": _percentiles(values["queue"]),
            "response_payload": _byte_summary(values["bytes"]),
        }
        for key, values in sorted(groups.items())
    ]


def _inside_config() -> dict[str, object]:
    try:
        value = json.loads(os.environ["QDL_PHASE3_LOAD_CONFIG"])
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError("Phase-3 inner configuration is unavailable") from error
    if not isinstance(value, dict):
        raise ValueError("Phase-3 inner configuration is invalid")
    expected = {
        "mode", "logical_sessions", "duration_seconds", "catalog", "acquisition",
        "queries", "stream_targets", "identities",
    }
    if set(value) != expected:
        raise ValueError("Phase-3 inner configuration fields are invalid")
    return value


def _selected_five(products):
    result = tuple(
        item for item in products
        if item.venue in _FIVE_LIQUID and item.native_symbol in _FIVE_LIQUID[item.venue]
    )
    if not result:
        raise ValueError("Phase-3 manifest has no five-liquid V2 products")
    return result


def _identity_map(config: dict[str, object], manifests: dict[str, object]) -> dict[str, _Identity]:
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    from qdl_sdk.tls import WorkloadTlsConfig

    raw_identities = config["identities"]
    assert isinstance(raw_identities, list)
    values: dict[str, _Identity] = {}
    for raw in raw_identities:
        if not isinstance(raw, dict) or set(raw) != {"id", "tls", "jwt"}:
            raise ValueError("Phase-3 inner identity fields are invalid")
        consumer_id = raw["id"]
        tls_raw = raw["tls"]
        jwt_raw = raw["jwt"]
        if not isinstance(consumer_id, str) or not isinstance(tls_raw, dict) or not isinstance(jwt_raw, dict):
            raise ValueError("Phase-3 inner identity is invalid")
        manifest = manifests.get(consumer_id)
        if manifest is None:
            raise ValueError("Phase-3 identity is not in the sealed release route")
        values[consumer_id] = _Identity(
            consumer_id=consumer_id,
            tls=WorkloadTlsConfig(
                tls_raw["ca_file"], tls_raw["cert_file"], tls_raw["key_file"],
            ),
            credential=RotatingJwtCredentialProvider(
                private_key_file=jwt_raw["private_key_file"],
                key_id=jwt_raw["key_id"], algorithm="RS256", issuer=_ISSUER,
                audience=_AUDIENCE, subject=manifest.subject, environment=manifest.environment,
                roles=_ROLES, venues=("BINANCE", "OKX"),
                consumer_manifest_revision=manifest.manifest_revision,
                lifetime_seconds=300, refresh_before_seconds=60,
            ),
            max_buffer_events=manifest.quotas.max_buffer_events,
        )
    if set(values) != set(manifests):
        raise ValueError("Phase-3 identity set differs from the sealed workload scope")
    return values


def _reference_product(product, *, now_ns: int):
    from qdl.certification.reference_l2_acceptance import (
        ReferenceAcceptanceProduct,
        reference_request_for_requirement,
    )
    return ReferenceAcceptanceProduct(
        consumer_id=product.consumer_id,
        consumer_subject=product.consumer_subject,
        manifest_revision=product.manifest_revision,
        manifest_sha256=product.manifest_sha256,
        instrument_uid=product.instrument_uid,
        instrument_id=product.instrument_id,
        venue=product.venue,
        market=product.market,
        native_symbol=product.native_symbol,
        requirement=product.requirement,
        sdk_requirement=reference_request_for_requirement(product.requirement, now_ns=now_ns),
    )


def _response_payload_bytes(response: object) -> int:
    model_dump = getattr(response, "model_dump", None)
    payload = model_dump(mode="json") if callable(model_dump) else response
    return len(
        json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")).encode()
    )


def _measurement_sample(
    *, product, operation: str, replica: str, pacer: _Pacer, token,
    started: float, response: object,
) -> dict[str, object]:
    queue_wait_ms = pacer.finish_measurement(token)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return {
        "group": _product_group(product, operation, replica),
        "usable_ms": max(0.0, elapsed_ms - queue_wait_ms),
        "queue_wait_ms": queue_wait_ms,
        "response_payload_bytes": _response_payload_bytes(response),
    }


async def _read_product(client, product) -> object:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl.certification.reference_l2_acceptance import reference_evidence

    if product.delivery.value == "ON_DEMAND":
        reference = _reference_product(product, now_ns=time.time_ns())
        response = await client.reference_batch([reference.sdk_requirement], require_all=True)
        if response.partial or len(response.results) != 1:
            raise ValueError("PARTIAL_RESULT")
        reference_evidence(reference, response.results[0], observed_at_ns=time.time_ns())
        return response
    response = await client.snapshot(sdk_requirement(product))
    validate_product_view(product, response.data, require_current_quality=True)
    return response


def _stream_product(products):
    rank = {"TRADE": 0, "QUOTE": 1, "BOOK_DELTA": 2, "BOOK_SNAPSHOT": 3, "BAR": 4}
    values = [item for item in products if item.delivery.value == "DURABLE" and item.feed.value in rank]
    if not values:
        raise ValueError("logical session has no streamable durable product")
    return min(values, key=lambda item: (rank[item.feed.value], item.interval or ""))


def _final_bar_product(products):
    """Return the shortest declared durable BAR, or no final-BAR probe.

    A manifest that only consumes live feeds must not be rejected merely
    because it has no BAR requirement. Its supplemental stream remains a
    continuity probe and is explicitly reported as such.
    """
    from qdl.adapters.intervals import canonical_interval_ms

    values = [
        item for item in products
        if item.delivery.value == "DURABLE" and item.feed.value == "BAR"
    ]
    if not values:
        return None
    if any(not isinstance(item.interval, str) or not item.interval for item in values):
        raise ValueError("Phase-3 durable BAR product is missing its canonical interval")
    return min(
        values,
        key=lambda item: (
            canonical_interval_ms(item.interval),
            item.venue,
            item.native_symbol,
        ),
    )


def _supplemental_stream_spec(consumer_id, products) -> _StreamSpec:
    final_bar = _final_bar_product(products)
    if final_bar is not None:
        return _StreamSpec(
            name=f"final-bar-{consumer_id}",
            consumer_id=consumer_id,
            product=final_bar,
            slow=False,
            purpose="FINAL_BAR",
        )
    return _StreamSpec(
        name=f"continuity-{consumer_id}",
        consumer_id=consumer_id,
        product=_stream_product(products),
        slow=False,
        purpose="CONTINUITY",
    )


def _build_stream_specs(plan, products_by_consumer) -> tuple[_StreamSpec, ...]:
    logical = tuple(
        _StreamSpec(
            name=f"logical-{session.ordinal}",
            consumer_id=session.consumer_id,
            product=_stream_product(session.products),
            slow=session.ordinal % 13 == 0,
            purpose="LOGICAL",
        )
        for session in plan.logical_sessions
    )
    supplemental = tuple(
        _supplemental_stream_spec(consumer_id, products)
        for consumer_id, products in sorted(products_by_consumer.items())
    )
    return (*logical, *supplemental)


def _stream_buffer_bound(identity) -> int:
    value = getattr(identity, "max_buffer_events", None)
    if not isinstance(value, int) or not 1 <= value <= 10_000:
        raise ValueError("Phase-3 identity has an invalid sealed stream buffer quota")
    return value


def _make_client(identity, *, queries, stream_targets, pacer, replicated: bool):
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, ReplicatedRestQueryTransport, RestQueryTransport

    replicas = [
        RestQueryTransport(url, timeout_seconds=15.0, tls=identity.tls, credential_provider=identity.credential)
        for url in queries
    ]
    query = ReplicatedRestQueryTransport(replicas, max_attempts=2, cooldown_seconds=1.0) if replicated else replicas[0]
    return AsyncDataLayerClient(
        query_transport=_PacedQueryTransport(query, pacer),
        stream_transport=_PacedStreamTransport(
            GrpcStreamTransport(stream_targets, tls=identity.tls, credential_provider=identity.credential), pacer,
        ),
        consumer_id=identity.consumer_id,
        max_buffer_events=_stream_buffer_bound(identity),
        max_reconnect_attempts=1,
    )


@asynccontextmanager
async def _paced_warmup_then_stream(client, pacer: _Pacer, requirement, **kwargs):
    """Keep the signed snapshot and its first stream join within two quota slots.

    Reconnects after the first subscription deliberately return to normal pacing.
    A failed warmup does not refund its reserved stream slot, so the test client
    never exceeds the same per-identity leaky-bucket allowance.
    """

    async with pacer.handoff():
        async with client.warmup_then_stream(requirement, **kwargs) as session:
            yield session


async def _matrix_read(identity, *, product, replica, queries, stream_targets, pacer, samples, errors) -> None:
    client = _make_client(identity, queries=(replica,), stream_targets=stream_targets, pacer=pacer, replicated=False)
    token = pacer.begin_measurement()
    started = time.perf_counter()
    try:
        response = await _read_product(client, product)
        sample = _measurement_sample(
            product=product,
            operation="REFERENCE_BATCH" if product.delivery.value == "ON_DEMAND" else "SNAPSHOT",
            replica=replica,
            pacer=pacer,
            token=token,
            started=started,
            response=response,
        )
        token = None
        samples.append(sample)
    except Exception as error:
        if token is not None:
            pacer.finish_measurement(token)
        errors.append({"operation": "MATRIX", "replica": replica, "product": _product_evidence(product), "error": _safe_error(error)})
    finally:
        await client.close()


def _matrix_selection(*, sessions, execution_products):
    selected: dict[tuple[str, tuple[str, str, str, str, str]], object] = {}
    for session in sessions:
        for product in session.products:
            selected[(session.consumer_id, product.identity)] = product
    # The trading-system manifest is the shared execution-facing consumer. Add
    # exactly one V2 product per required venue/symbol so every staged matrix
    # proves the declared five-liquid universe without replaying all unchanged
    # manifest products.
    feed_rank = {"QUOTE": 0, "TRADE": 1, "BAR": 2, "BOOK_SNAPSHOT": 3, "BOOK_DELTA": 4}
    for venue, symbols in sorted(_FIVE_LIQUID.items()):
        for symbol in sorted(symbols):
            candidates = [
                product for product in execution_products
                if product.venue == venue and product.native_symbol == symbol
            ]
            if not candidates:
                raise ValueError(f"Phase-3 execution scope misses {venue}:{symbol}")
            product = min(
                candidates,
                key=lambda item: (feed_rank.get(item.feed.value, 99), item.interval or ""),
            )
            selected[(product.consumer_id, product.identity)] = product
    return tuple(product for _, product in sorted(selected.items(), key=lambda item: item[0]))


async def _run_matrix(*, products_by_consumer, identities, queries, stream_targets, pacers, sessions):
    selected = _matrix_selection(
        sessions=sessions,
        execution_products=products_by_consumer["trading-system.paper.stable"],
    )
    samples: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    for product in selected:
        for replica in queries:
            await _matrix_read(
                identities[product.consumer_id], product=product, replica=replica,
                queries=queries, stream_targets=stream_targets, pacer=pacers[product.consumer_id],
                samples=samples, errors=errors,
            )
            if errors:
                break
        if errors:
            break
    return samples, errors, selected


async def _poll_worker(*, client, products, pacer, deadline: float, samples, errors) -> None:
    index = 0
    token = None
    try:
        while time.monotonic() < deadline:
            product = products[index % len(products)]
            index += 1
            token = pacer.begin_measurement()
            started = time.perf_counter()
            response = await _read_product(client, product)
            sample = _measurement_sample(
                product=product,
                operation="REFERENCE_BATCH" if product.delivery.value == "ON_DEMAND" else "SNAPSHOT",
                replica="replicated",
                pacer=pacer,
                token=token,
                started=started,
                response=response,
            )
            token = None
            samples.append(sample)
            await asyncio.sleep(0)
    except Exception as error:
        if token is not None:
            pacer.finish_measurement(token)
        errors.append({"operation": "POLL", "product": _product_evidence(product), "error": _safe_error(error)})
    finally:
        await client.close()


async def _reconnect_probe(*, identity, product, queries, stream_targets, pacer) -> None:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
    from qdl_sdk.models import StreamEvent
    from qdl_sdk.projection import market_data_view_from_stream

    client = _make_client(identity, queries=queries, stream_targets=stream_targets, pacer=pacer, replicated=True)
    try:
        async with _paced_warmup_then_stream(
            client, pacer, sdk_requirement(product)
        ) as first:
            event = await asyncio.wait_for(first.__anext__(), timeout=60.0)
            if not isinstance(event, StreamEvent):
                raise ValueError("reconnect probe did not receive a stream event")
            view = market_data_view_from_stream(event, template=first.warmup.data[-1], requirement=sdk_requirement(product))
            validate_product_view(product, view, require_current_quality=True)
            first.acknowledge(event)
        async with _paced_warmup_then_stream(
            client,
            pacer,
            sdk_requirement(product),
            resume_restored_state=True,
        ) as restored:
            if not restored.state_restored:
                raise ValueError("signed cursor was not restored on reopen")
            event = await asyncio.wait_for(restored.__anext__(), timeout=60.0)
            if not isinstance(event, StreamEvent):
                raise ValueError("restored cursor did not yield a stream event")
            view = market_data_view_from_stream(
                event,
                template=restored.warmup.data[-1],
                requirement=sdk_requirement(product),
            )
            validate_product_view(product, view, require_current_quality=True)
            restored.acknowledge(event)
    finally:
        await client.close()


async def _n_minus_one_probe(*, identity, product, secondary, stream_targets, pacer) -> None:
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.transport import GrpcStreamTransport, ReplicatedRestQueryTransport, RestQueryTransport

    dead = RestQueryTransport("https://127.0.0.1:1", timeout_seconds=2.0, tls=identity.tls, credential_provider=identity.credential)
    live = RestQueryTransport(secondary, timeout_seconds=15.0, tls=identity.tls, credential_provider=identity.credential)
    query = _PacedQueryTransport(ReplicatedRestQueryTransport((dead, live), max_attempts=2, cooldown_seconds=1.0), pacer)
    client = AsyncDataLayerClient(
        query_transport=query,
        stream_transport=_PacedStreamTransport(GrpcStreamTransport(stream_targets, tls=identity.tls, credential_provider=identity.credential), pacer),
        consumer_id=identity.consumer_id,
        max_buffer_events=_stream_buffer_bound(identity),
        max_reconnect_attempts=0,
    )
    try:
        await _read_product(client, product)
        stats = query._delegate.stats()
        if not (stats[0]["failures"] == 1 and stats[1]["successes"] == 1):
            raise ValueError("client-side alternate Query reader was not exercised exactly once")
    finally:
        await client.close()


async def _run_load(*, plan, products_by_consumer, identities, queries, stream_targets, pacers, duration_seconds: int):
    samples: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    counters: Counter[str] = Counter()
    # Each identity has one supplemental stream inside the sealed stream budget.
    # It proves final BAR only where the manifest declares a durable BAR; a
    # live-only consumer instead contributes an explicitly labelled continuity
    # probe and is never misreported as BAR-finality coverage.
    startup: asyncio.Queue = asyncio.Queue()
    stream_specs = _build_stream_specs(plan, products_by_consumer)
    setup_budget = max(
        90.0,
        max(budget.planned_streams * budget.seconds_per_request * 2.0 for budget in plan.identity_budgets) + 60.0,
    )
    setup_deadline = time.monotonic() + setup_budget
    start_observation = asyncio.Event()
    stop_observation = asyncio.Event()
    observation_deadline: float | None = None

    # Establish every signed warmup/cursor and receive one validated event
    # before starting the timed observation. This prevents setup pacing from
    # being reported as load latency, while keeping the first event as evidence
    # that the stream is genuinely live rather than merely constructed.
    async def establish_then_run(spec: _StreamSpec):
        from qdl.certification.phase103_consumer_acceptance import sdk_requirement, validate_product_view
        from qdl_sdk.models import ControlEvent, StreamEvent
        from qdl_sdk.projection import market_data_view_from_stream
        client = _make_client(identities[spec.consumer_id], queries=queries, stream_targets=stream_targets, pacer=pacers[spec.consumer_id], replicated=True)
        started = time.perf_counter()
        admitted = False
        last_stream_event = None
        requirement = sdk_requirement(spec.product)
        try:
            async with _paced_warmup_then_stream(
                client, pacers[spec.consumer_id], requirement
            ) as session:
                while True:
                    event = await asyncio.wait_for(session.__anext__(), timeout=60.0)
                    if isinstance(event, ControlEvent):
                        counters["stream_control_events"] += 1
                        continue
                    if not isinstance(event, StreamEvent):
                        raise ValueError("stream returned an unknown event type")
                    last_stream_event = event
                    view = market_data_view_from_stream(
                        event,
                        template=session.warmup.data[-1],
                        requirement=requirement,
                    )
                    validate_product_view(spec.product, view, require_current_quality=True)
                    session.acknowledge(event)
                    counters[f"stream_event:{spec.name}"] += 1
                    samples.append({
                        "group": _product_group(spec.product, "STREAM", "replicated"),
                        "usable_ms": (time.perf_counter() - started) * 1000.0,
                    })
                    started = time.perf_counter()
                    break
                await startup.put((spec.name, None))
                admitted = True
                await start_observation.wait()
                if spec.slow:
                    await asyncio.sleep(5.0)
                    counters["slow_reader_sessions"] += 1
                while not stop_observation.is_set():
                    if observation_deadline is None:
                        raise RuntimeError("stream observation did not receive a deadline")
                    remaining = observation_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        event = await asyncio.wait_for(session.__anext__(), timeout=min(5.0, remaining))
                    except asyncio.TimeoutError:
                        continue
                    if isinstance(event, ControlEvent):
                        counters["stream_control_events"] += 1
                        continue
                    if not isinstance(event, StreamEvent):
                        raise ValueError("stream returned an unknown event type")
                    last_stream_event = event
                    view = market_data_view_from_stream(event, template=session.warmup.data[-1], requirement=requirement)
                    validate_product_view(spec.product, view, require_current_quality=True)
                    session.acknowledge(event)
                    counters[f"stream_event:{spec.name}"] += 1
                    samples.append({"group": _product_group(spec.product, "STREAM", "replicated"), "usable_ms": (time.perf_counter() - started) * 1000.0})
                    started = time.perf_counter()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            evidence = {
                "operation": "STREAM",
                "stream": spec.name,
                "product": _product_evidence(spec.product),
                "error": _safe_error(error),
            }
            if last_stream_event is not None:
                evidence["stream_quality"] = _stream_frame_quality_diagnostic(
                    last_stream_event, requirement
                )
            errors.append(evidence)
            if not admitted:
                await startup.put((spec.name, "FAILED"))
        finally:
            await client.close()

    stream_tasks = [asyncio.create_task(establish_then_run(spec)) for spec in stream_specs]
    active_tasks: list[asyncio.Task] = []
    started_names: set[str] = set()
    try:
        while len(started_names) < len(stream_specs):
            timeout = max(0.1, setup_deadline - time.monotonic())
            name, failure = await asyncio.wait_for(startup.get(), timeout=timeout)
            if name in started_names:
                continue
            started_names.add(name)
            if failure:
                raise RuntimeError("stream setup failed")
        if errors:
            raise RuntimeError("stream setup reported a typed failure")
        observation_deadline = time.monotonic() + duration_seconds
        start_observation.set()
        poll_tasks = [
            asyncio.create_task(_poll_worker(
                client=_make_client(identities[session.consumer_id], queries=queries, stream_targets=stream_targets, pacer=pacers[session.consumer_id], replicated=True),
                products=session.products, pacer=pacers[session.consumer_id],
                deadline=observation_deadline, samples=samples, errors=errors,
            ))
            for session in plan.logical_sessions
        ]
        reconnect_tasks = [
            asyncio.create_task(_reconnect_probe(
                identity=identities[consumer_id], product=_stream_product(products), queries=queries,
                stream_targets=stream_targets, pacer=pacers[consumer_id],
            ))
            for consumer_id, products in sorted(products_by_consumer.items())
        ]
        nminusone_tasks = [
            asyncio.create_task(_n_minus_one_probe(
                identity=identities[consumer_id], product=_stream_product(products), secondary=queries[1],
                stream_targets=stream_targets, pacer=pacers[consumer_id],
            ))
            for consumer_id, products in sorted(products_by_consumer.items())
        ]
        active_tasks = [*poll_tasks, *reconnect_tasks, *nminusone_tasks]
        while time.monotonic() < observation_deadline:
            await asyncio.sleep(min(1.0, observation_deadline - time.monotonic()))
            if errors:
                raise RuntimeError("load worker reported a typed failure")
            completed = [task for task in active_tasks if task.done() and not task.cancelled()]
            for task in completed:
                error = task.exception()
                if error is not None:
                    raise RuntimeError("load auxiliary worker failed") from error
        stop_observation.set()
        results = await asyncio.gather(*active_tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise RuntimeError("load worker failed after observation") from result
    except Exception as error:
        errors.append({"operation": "LOAD", "error": _safe_error(error)})
        for task in [*active_tasks, *stream_tasks]:
            if not task.done():
                task.cancel()
    finally:
        stop_observation.set()
        start_observation.set()
        await asyncio.gather(*stream_tasks, return_exceptions=True)
    expected_bars = [spec.name for spec in stream_specs if spec.purpose == "FINAL_BAR"]
    missing_bars = [name for name in expected_bars if counters[f"stream_event:{name}"] < 1]
    if missing_bars:
        errors.append({"operation": "FINAL_BAR", "missing_streams": sorted(missing_bars)})
    supplemental = [
        {
            "consumer_id": spec.consumer_id,
            "kind": spec.purpose,
            "final_bar_status": (
                "FINAL_BAR_DECLARED"
                if spec.purpose == "FINAL_BAR"
                else "FINAL_BAR_NOT_DECLARED"
            ),
            "product": _product_evidence(spec.product),
        }
        for spec in stream_specs
        if spec.purpose != "LOGICAL"
    ]
    return samples, errors, counters, supplemental


async def run_inside() -> dict[str, object]:
    config = _inside_config()
    from qdl.certification.phase103_consumer_acceptance import build_manifest_acceptance_scope
    from qdl.certification.phase3_consumer_load import assert_required_instrument_coverage, build_consumer_load_plan
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_deployment import StableAcquisitionPlan

    catalog = StableSourceCatalog.load(config["catalog"])
    acquisition = StableAcquisitionPlan.load(config["acquisition"], catalog=catalog)
    declared_identity_ids = {
        str(item["id"])
        for item in config["identities"]
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    if declared_identity_ids != set(_PHASE3_CONSUMER_MANIFESTS):
        raise ValueError("Phase-3 inner identity scope is not the approved crypto set")
    runtime_manifests = {
        consumer_id: ConsumerManifestLoader.load(path)
        for consumer_id, path in _PHASE3_CONSUMER_MANIFESTS.items()
    }
    identities = _identity_map(config, runtime_manifests)
    scope = build_manifest_acceptance_scope(
        tuple(_PHASE3_CONSUMER_MANIFESTS.values()),
        catalog=catalog,
        acquisition=acquisition,
        expected_consumer_ids=frozenset(_PHASE3_CONSUMER_MANIFESTS),
        schema="qdl.phase105.consumer-acceptance-scope.v1",
    )
    products_by_consumer = {
        consumer_id: _selected_five(tuple(item for item in scope.products if item.consumer_id == consumer_id))
        for consumer_id in sorted(identities)
    }
    required_pairs = tuple(
        (venue, symbol)
        for venue, symbols in sorted(_FIVE_LIQUID.items())
        for symbol in sorted(symbols)
    )
    plan = build_consumer_load_plan(
        manifests=runtime_manifests,
        products_by_consumer=products_by_consumer,
        logical_session_count=int(config["logical_sessions"]),
        required_instruments=required_pairs,
    )
    assert_required_instrument_coverage(plan, required_pairs)
    pacers = {
        budget.consumer_id: _Pacer(budget.seconds_per_request)
        for budget in plan.identity_budgets
    }
    mode = config["mode"]
    queries = tuple(config["queries"])
    streams = tuple(config["stream_targets"])
    started = time.monotonic()
    if mode == "matrix":
        samples, errors, selected_products = await _run_matrix(
            products_by_consumer=products_by_consumer, identities=identities,
            queries=queries, stream_targets=streams, pacers=pacers,
            sessions=plan.logical_sessions,
        )
        selected_count = len(selected_products)
        covered_instruments = tuple(sorted({
            (product.venue, product.native_symbol) for product in selected_products
        }))
        counters = Counter()
        supplemental_streams = []
    elif mode in {"load", "final"}:
        samples, errors, counters, supplemental_streams = await _run_load(
            plan=plan, products_by_consumer=products_by_consumer, identities=identities,
            queries=queries, stream_targets=streams, pacers=pacers,
            duration_seconds=int(config["duration_seconds"]),
        )
        selected_count = sum(len(item.products) for item in plan.logical_sessions)
        covered_instruments = plan.covered_instruments
    else:
        raise ValueError("Phase-3 inner mode is invalid")
    status = "PASS" if not errors else "FAIL"
    return {
        "schema": "qdl.phase3.consumer-load.v1",
        "status": status,
        "mode": mode,
        "logical_session_count": len(plan.logical_sessions),
        "authenticated_identity_count": plan.identity_count,
        "planned_stream_count": plan.stream_count,
        "selected_product_count": selected_count,
        "covered_instruments": [{"venue": venue, "native_symbol": symbol} for venue, symbol in covered_instruments],
        "runtime_contract": {
            "catalog_revision": catalog.catalog_revision,
            "acquisition_revision": acquisition.revision,
            "manifests": [
                {
                    "consumer_id": consumer_id,
                    "manifest_revision": manifest.manifest_revision,
                    "manifest_sha256": manifest.manifest_sha256,
                }
                for consumer_id, manifest in sorted(runtime_manifests.items())
            ],
        },
        "identity_budgets": [
            {
                "consumer_id": item.consumer_id,
                "logical_sessions": item.logical_sessions,
                "requests_per_minute": item.requests_per_minute,
                "test_requests_per_minute": item.test_requests_per_minute,
                "seconds_per_request": round(item.seconds_per_request, 6),
                "max_streams": item.max_streams,
                "planned_streams": item.planned_streams,
                "max_buffer_events": identities[item.consumer_id].max_buffer_events,
            }
            for item in plan.identity_budgets
        ],
        "latency": _summarize_samples(samples),
        "pacer": {consumer_id: pacer.evidence() for consumer_id, pacer in sorted(pacers.items())},
        "stream_counters": dict(sorted(counters.items())),
        "supplemental_streams": supplemental_streams,
        "errors": errors[:20],
        "error_count": len(errors),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "provider_connections": 0,
        "v1_fallback_calls": 0,
        "direct_provider_calls": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
        "test_provenance": False,
        "process_cpu_seconds": round(resource.getrusage(resource.RUSAGE_SELF).ru_utime + resource.getrusage(resource.RUSAGE_SELF).ru_stime, 3),
        "process_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    value.add_argument("--profile", type=Path)
    value.add_argument("--output", type=Path)
    value.add_argument("--mode", choices=("matrix", "load", "final"))
    value.add_argument("--sessions", type=int)
    value.add_argument("--duration-seconds", type=int)
    return value


def main() -> int:
    args = parser().parse_args()
    if args.inside:
        if any(value is not None for value in (args.profile, args.output, args.mode, args.sessions, args.duration_seconds)):
            raise SystemExit("inner Phase-3 client accepts configuration only from its mounted environment")
        try:
            result = asyncio.run(run_inside())
        except Exception as error:
            print(json.dumps({
                "schema": "qdl.phase3.consumer-load.v1", "status": "FAIL",
                "failure": _safe_error(error), "order_actions": 0,
                "secret_values_recorded": False,
            }, sort_keys=True))
            return 1
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0 if result["status"] == "PASS" else 1
    if None in (args.profile, args.output, args.mode, args.sessions, args.duration_seconds):
        raise SystemExit("host Phase-3 run requires --profile --output --mode --sessions --duration-seconds")
    return run_host(args)


if __name__ == "__main__":
    raise SystemExit(main())

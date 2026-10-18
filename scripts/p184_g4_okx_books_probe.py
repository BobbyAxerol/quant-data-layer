#!/usr/bin/env python3
"""Report-only OKX `books` fresh-connection probe + ingestor-netns TCP sampler for every G4 window (owner 2026-10-18).

Purpose. RCA ccea671 (DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md): the G4 book_delta|OKX_INVERSE| tail of final run
p184-final-campaign-20261018T074624Z was venue -> DL on lane okx-public-001 (session gen 67018) only, at the same source
instant for every OKX book binding of that lane, with the internal segments normal. Without same-second venue evidence on
an independent connection that tail can only be UNKNOWN. This probe gives it, per G4 window, outside the scored metrics:

* books: ONE fresh public WebSocket to the URL the ingestor uses (wss://ws.okx.com/ws/v5/public, no key, no order
  path), channel `books` for BTC-USD-SWAP + BTC-USDT-SWAP + ETH-USDT-SWAP. Per push it keeps the local receipt wall
  clock, instrument, action, venue `ts`, seqId / prevSeqId and the frame byte size. No price or size is retained (the
  frame is never JSON-decoded; ts / seqId are read with a bounded regex).
* tcp (host side, no docker exec, no nsenter): the launcher reads the ingestor_okx_swap network namespace through
  /proc/<ingestor pid>/net/{netstat,snmp} at 1 Hz (TcpExt + Tcp counters, deltas of the ones that moved) and
  /proc/<pid>/net/tcp{,6} at 10 Hz (ESTABLISHED sockets to remote port 443: per-second max rx_queue / tx_queue and
  the longest run of consecutive samples in which one socket held unread bytes). A receive queue that stays non-empty
  for >= RX_PERSIST_SAMPLES samples (>= ~100 ms) means the bytes had reached the host but the reader had not taken
  them.

It never gates anything. The scorer (scripts/p184_gate_campaign.py) attaches analyze() / analyze_tcp() per window and,
for each OKX book outlier of the window with venue -> DL above TAIL_MS, tail_attribution(): the fresh connection's
state in the same seconds (HEALTHY / LATE / NO_COVERAGE) and the host receive queue (RX_QUEUED / CLEAN / NO_COVERAGE).
EXTERNAL_SESSION_UPSTREAM needs the fresh connection HEALTHY while the lane is late in the same seconds and a CLEAN
host receive queue; LATE on the fresh connection too is COMMON_VENUE_DELAY; RX_QUEUED is INTERNAL_RECEIVE_QUEUE; any
missing coverage is UNKNOWN (never EXTERNAL).

Modes:
  --launch   (campaign apparatus) one bounded `docker run --rm` of the image query_kn_1 runs (resolved by docker
             inspect), default bridge network, read-only rootfs, no capabilities, --cpus/--memory/--pids-limit, a
             name under the campaign's G4 observer prefix (the foreign-activity guard classifies it as campaign);
             `docker rm -f` by that name on every exit path; the TCP sampler runs in this (host) process. Writes
             <output>/books.jsonl, <output>/tcp.jsonl and <output>/receipt.json. Self-bounded by --seconds.
  --analyze  offline: books / tcp statistics restricted to [--start-ns, --end-ns].
  --worker   (inside the container) the WebSocket reader.
Boundary: no production container is touched or entered (docker inspect + /proc reads only); no write besides the
output directory.
usage: p184_g4_okx_books_probe.py --launch --output DIR --seconds N --name NAME [--ingestor CONTAINER]
       | --analyze DIR --start-ns A --end-ns B
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time

PROBE_VERSION = "qdl.p184.g4-okx-books-probe.v1"
QUERY = "qdl_v2_stable_candidate-query_kn_1-1"
INGESTOR = "qdl_v2_stable_candidate-ingestor_okx_swap-1"
URL = "wss://ws.okx.com/ws/v5/public"  # the ingestor's public URL (config/v2/stable-acquisition-bindings.yaml)
CHANNEL = "books"
INSTRUMENTS = ("BTC-USD-SWAP", "BTC-USDT-SWAP", "ETH-USDT-SWAP")
CPUS, MEMORY, PIDS_LIMIT = 0.5, "192m", 32
MAX_SECONDS = 600
MAX_OUTPUT_BYTES = 24 * 1024 * 1024
EDGE_TOLERANCE_NS = 5_000_000_000      # coverage: pushes within 5 s of both window edges
TAIL_MS = 150.0                        # an OKX book outlier with venue -> DL above this is attributed
HEALTHY_MAX_MS = 150.0                 # fresh connection recv - ts at or below this in the tail's seconds = HEALTHY
TAIL_SLACK_NS = 1_000_000_000          # "the same seconds": venue ts within +-1 s of the tail's source instant
TCP_HZ, SOCKET_HZ = 1, 10
# A reader stall shows as unread bytes on the SAME socket in consecutive 100 ms samples; a single non-empty sample is
# just a frame that arrived between two reads (seen in the 2026-10-18 smoke: 195 bytes once in 18 s, no stall).
RX_PERSIST_SAMPLES = 2
REMOTE_PORT = 443
TCP_COUNTERS = ("TcpExt", "Tcp", "IpExt")
TS = re.compile(rb'"ts":"(\d{10,16})"')
SEQ = re.compile(rb'"seqId":(-?\d+)')
PREV = re.compile(rb'"prevSeqId":(-?\d+)')
ACTION = re.compile(rb'"action":"(snapshot|update)"')
INST = re.compile(rb'"instId":"([A-Z0-9-]{3,40})"')


def pct(values, f):
    """Nearest rank, the observer's v1-v3 convention (index int(f * n) of the sorted values)."""
    values = sorted(values)
    return round(values[min(len(values) - 1, int(f * len(values)))], 1) if values else None


def stats(values):
    return {"n": len(values), "p50": pct(values, .5), "p95": pct(values, .95),
            "max": round(max(values), 1) if values else None}


# ---------------------------------------------------------------- books frames (pure)

def parse_frame(raw: bytes, recv_ns: int) -> dict | None:
    """One `books` data frame -> {'r', 'i', 'a', 'ts', 'seq', 'prev', 'b'} (no payload values), else None."""
    if b'"data"' not in raw:
        return None
    ts, inst = TS.search(raw), INST.search(raw)
    if not ts or not inst:
        return None
    seq, prev, action = SEQ.search(raw), PREV.search(raw), ACTION.search(raw)
    return {"r": recv_ns, "i": inst.group(1).decode(), "a": action.group(1).decode() if action else None,
            "ts": int(ts.group(1)), "seq": int(seq.group(1)) if seq else None,
            "prev": int(prev.group(1)) if prev else None, "b": len(raw)}


def read_rows(lines):
    rows, events = [], []
    for line in lines:
        if not line.strip():
            continue
        item = json.loads(line)
        (rows if "r" in item and "i" in item else events).append(item)
    rows.sort(key=lambda r: r["r"])
    return rows, events


def analyze(lines, start_ns: int, end_ns: int) -> dict:
    """Per-window fresh-connection statistics (report-only): per instrument recv - venue ts, venue ts interval,
    receipt gap, seqId continuity (prevSeqId == previous seqId), coverage (pushes of every instrument within 5 s of
    both edges and no disconnect / error before end_ns)."""
    rows, events = read_rows(lines)
    problems = [e.get("kind") for e in events if e.get("kind") in ("disconnect", "error")
                and e.get("at_ns", 0) < end_ns]
    out = {"probe_version": PROBE_VERSION, "report_only": True, "window": {"start_ns": start_ns, "end_ns": end_ns},
           "channel": CHANNEL, "instruments": {}}
    last = {}
    for r in rows:
        inst = r["i"]
        block = out["instruments"].setdefault(inst, {"_lag": [], "_interval": [], "_gap": [], "seq_breaks": 0,
                                                     "snapshots": 0, "pushes": 0, "_first": None, "_last": None,
                                                     "bytes_max": 0})
        previous = last.get(inst)
        last[inst] = r
        if not start_ns <= r["r"] <= end_ns:
            continue
        block["pushes"] += 1
        block["_first"] = r["r"] if block["_first"] is None else block["_first"]
        block["_last"] = r["r"]
        block["bytes_max"] = max(block["bytes_max"], int(r.get("b") or 0))
        block["_lag"].append(r["r"] / 1e6 - r["ts"])
        if r.get("a") == "snapshot":
            block["snapshots"] += 1
        if previous is not None:
            block["_interval"].append(r["ts"] - previous["ts"])
            block["_gap"].append((r["r"] - previous["r"]) / 1e6)
            if r.get("a") == "update" and r.get("prev") is not None and previous.get("seq") is not None \
                    and r["prev"] != previous["seq"]:
                block["seq_breaks"] += 1
    for inst in INSTRUMENTS:
        block = out["instruments"].get(inst)
        if not block or not block["pushes"]:
            problems.append(f"no push inside the window for {inst}")
            continue
        if not (block["_first"] <= start_ns + EDGE_TOLERANCE_NS and block["_last"] >= end_ns - EDGE_TOLERANCE_NS):
            problems.append(f"{inst}: pushes do not reach both window edges")
    for inst, block in out["instruments"].items():
        out["instruments"][inst] = {"pushes": block["pushes"], "snapshots": block["snapshots"],
                                    "seq_breaks": block["seq_breaks"], "bytes_max": block["bytes_max"],
                                    "recv_minus_venue_ts_ms": stats(block["_lag"]),
                                    "venue_ts_interval_ms": stats(block["_interval"]),
                                    "receipt_gap_ms": stats(block["_gap"])}
    out["covered"] = not problems
    out["coverage_problems"] = problems
    return out


# ---------------------------------------------------------------- tcp sampler (pure parsers + host loop)

def parse_proc_counters(text: str, families=TCP_COUNTERS) -> dict:
    """/proc/<pid>/net/netstat or snmp text -> {'TcpExt:TCPOFOQueue': int, ...} for the given families."""
    out, lines = {}, text.splitlines()
    for head, values in zip(lines[::2], lines[1::2]):
        name, _, keys = head.partition(":")
        vname, _, vals = values.partition(":")
        if name != vname or name not in families:
            continue
        for key, value in zip(keys.split(), vals.split()):
            try:
                out[f"{name}:{key}"] = int(value)
            except ValueError:
                continue
    return out


def parse_proc_tcp(text: str, *, remote_port: int = REMOTE_PORT) -> list[dict]:
    """/proc/<pid>/net/tcp{,6} -> ESTABLISHED sockets to remote_port: [{'local_port', 'tx_queue', 'rx_queue'}]."""
    out = []
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 5 or parts[3] != "01":  # 01 = TCP_ESTABLISHED
            continue
        try:
            local_port = int(parts[1].rsplit(":", 1)[1], 16)
            rport = int(parts[2].rsplit(":", 1)[1], 16)
            tx, rx = (int(x, 16) for x in parts[4].split(":"))
        except (IndexError, ValueError):
            continue
        if rport == remote_port:
            out.append({"local_port": local_port, "tx_queue": tx, "rx_queue": rx})
    return out


def counter_delta(previous: dict, current: dict) -> dict:
    return {k: v - previous[k] for k, v in current.items() if k in previous and v != previous[k]}


class TcpSampler:
    """Host-side reader of one container's netns via /proc/<pid>/net (read-only). One line per second."""

    def __init__(self, pid: int, out, stop: threading.Event, proc_root: str = "/proc") -> None:
        self.pid, self.out, self.stop, self.base = pid, out, stop, Path(proc_root) / str(pid) / "net"
        self.runs: dict[int, int] = {}  # local port -> consecutive samples with unread bytes (spans seconds)

    def counters(self) -> dict:
        values = {}
        for name in ("netstat", "snmp"):
            values.update(parse_proc_counters((self.base / name).read_text()))
        return values

    def sockets(self) -> list[dict]:
        found = []
        for name in ("tcp", "tcp6"):
            path = self.base / name
            if path.exists():
                found.extend(parse_proc_tcp(path.read_text()))
        return found

    def run(self) -> None:
        try:
            previous = self.counters()
        except OSError as exc:
            self.out({"kind": "tcp_error", "at_ns": time.time_ns(), "error_type": type(exc).__name__})
            return
        self.out({"kind": "tcp_start", "at_ns": time.time_ns(), "pid": self.pid, "counters": len(previous)})
        while not self.stop.is_set():
            second_end = time.monotonic() + 1.0 / TCP_HZ
            rx_max, tx_max, sockets_max, persist_max = {}, {}, 0, 0
            try:
                while time.monotonic() < second_end and not self.stop.is_set():
                    found = self.sockets()
                    sockets_max = max(sockets_max, len(found))
                    seen = set()
                    for s in found:
                        port = s["local_port"]
                        seen.add(port)
                        rx_max[port] = max(rx_max.get(port, 0), s["rx_queue"])
                        tx_max[port] = max(tx_max.get(port, 0), s["tx_queue"])
                        self.runs[port] = self.runs.get(port, 0) + 1 if s["rx_queue"] > 0 else 0
                        persist_max = max(persist_max, self.runs[port])
                    self.runs = {p: v for p, v in self.runs.items() if p in seen}
                    time.sleep(max(0.0, min(1.0 / SOCKET_HZ, second_end - time.monotonic())))
                current = self.counters()
            except OSError as exc:  # the process (container) went away: stop, never guess
                self.out({"kind": "tcp_error", "at_ns": time.time_ns(), "error_type": type(exc).__name__})
                return
            self.out({"kind": "tcp", "at_ns": time.time_ns(), "delta": counter_delta(previous, current),
                      "sockets": sockets_max, "rx_queue_max": max(rx_max.values(), default=0),
                      "tx_queue_max": max(tx_max.values(), default=0), "rx_persist_samples_max": persist_max,
                      "rx_queued_ports": sorted(p for p, v in rx_max.items() if v > 0)})
            previous = current
        self.out({"kind": "tcp_done", "at_ns": time.time_ns()})


def analyze_tcp(lines, start_ns: int, end_ns: int) -> dict:
    """Per-window TCP view of the ingestor netns (report-only): summed counter deltas, seconds with a non-empty
    receive queue, coverage (a sample within 2 s of both edges and no sampler error before end_ns)."""
    samples, problems = [], []
    for line in lines:
        if not line.strip():
            continue
        item = json.loads(line)
        if item.get("kind") == "tcp" and start_ns <= item["at_ns"] <= end_ns + TCP_HZ * 1_000_000_000:
            samples.append(item)
        elif item.get("kind") == "tcp_error" and item.get("at_ns", 0) < end_ns:
            problems.append("tcp sampler error")
    totals = {}
    for s in samples:
        for k, v in s["delta"].items():
            totals[k] = totals.get(k, 0) + v
    if not samples or samples[0]["at_ns"] > start_ns + 2_000_000_000 or samples[-1]["at_ns"] < end_ns - 2_000_000_000:
        problems.append("tcp samples do not reach both window edges")
    keep = ("TcpExt:TCPOFOQueue", "TcpExt:TCPRcvQDrop", "TcpExt:TCPBacklogDrop", "TcpExt:PruneCalled",
            "TcpExt:RcvPruned", "TcpExt:TCPTimeouts", "TcpExt:TCPLossProbes", "TcpExt:TCPDSACKRecv",
            "TcpExt:TCPZeroWindowDrop", "Tcp:RetransSegs", "Tcp:InSegs", "Tcp:InErrs")
    return {"report_only": True, "samples": len(samples), "covered": not problems, "coverage_problems": problems,
            "counter_totals": {k: totals.get(k, 0) for k in keep},
            "other_counters_moved": sorted(k for k in totals if k not in keep and totals[k]),
            "rx_persist_samples_threshold": RX_PERSIST_SAMPLES,
            "rx_queued_seconds": [s["at_ns"] for s in samples
                                  if (s.get("rx_persist_samples_max") or 0) >= RX_PERSIST_SAMPLES][:64],
            "rx_nonempty_seconds": sum(1 for s in samples if s.get("rx_queue_max")),
            "rx_queue_max": max((s.get("rx_queue_max", 0) for s in samples), default=None),
            "sockets_max": max((s.get("sockets", 0) for s in samples), default=None)}


# ---------------------------------------------------------------- tail attribution (pure)

def tail_attribution(sample: dict, book_rows, tcp_lines) -> dict:
    """One OKX book outlier (an observer record: source_event_ns, received_at_ns) against the fresh connection and
    the host receive queue in the same seconds. Report-only."""
    source, received = int(sample["source_event_ns"]), int(sample["received_at_ns"])
    lane_ms = round((received - source) / 1e6, 1)
    window = [r for r in book_rows if abs(r["ts"] * 1_000_000 - source) <= TAIL_SLACK_NS]
    lags = [r["r"] / 1e6 - r["ts"] for r in window]
    if not window:
        probe = "NO_COVERAGE"
    elif max(lags) <= HEALTHY_MAX_MS:
        probe = "HEALTHY"
    else:
        probe = "LATE"
    tcp = [json.loads(line) for line in tcp_lines if line.strip()]
    tcp = [t for t in tcp if t.get("kind") == "tcp"
           and source - TAIL_SLACK_NS <= t["at_ns"] <= received + 2 * TAIL_SLACK_NS]
    host = ("NO_COVERAGE" if not tcp else "RX_QUEUED"
            if any((t.get("rx_persist_samples_max") or 0) >= RX_PERSIST_SAMPLES for t in tcp) else "CLEAN")
    if host == "RX_QUEUED":
        attribution = "INTERNAL_RECEIVE_QUEUE"
    elif probe == "NO_COVERAGE" or host == "NO_COVERAGE":
        attribution = "UNKNOWN"
    elif probe == "HEALTHY":
        attribution = "EXTERNAL_SESSION_UPSTREAM"
    else:
        attribution = "COMMON_VENUE_DELAY"
    identity = sample.get("identity") or {}
    return {"native_symbol": identity.get("native_symbol") or sample.get("native_symbol"),
            "source_session_id": identity.get("source_session_id"), "feed": sample.get("feed"),
            "source_event_ns": source, "lane_venue_to_dl_ms": lane_ms,
            "probe_state": probe, "probe_pushes_same_seconds": len(window),
            "probe_same_instrument": any(r["i"] == (identity.get("native_symbol") or sample.get("native_symbol"))
                                         for r in window),
            "probe_recv_minus_ts_max_ms": round(max(lags), 1) if lags else None,
            "host_receive_queue": host, "attribution": attribution}


def window_report(probe_dir: Path, aggregate: dict | None) -> dict | None:
    """Scorer helper: analyze() + analyze_tcp() over the observer window, and tail_attribution() for every OKX book
    outlier of the final aggregate with venue -> DL above TAIL_MS. None when the window had no books probe."""
    d = Path(probe_dir)
    books, tcp = d / "books.jsonl", d / "tcp.jsonl"
    if not books.exists():
        return None
    aggregate = aggregate or {}
    start, end = aggregate.get("observed_start_ns"), aggregate.get("observed_end_ns")
    if not start or not end:
        return {"covered": False, "coverage_problems": ["observer window bounds unknown"], "report_only": True}
    book_lines = books.read_text().splitlines()
    tcp_lines = tcp.read_text().splitlines() if tcp.exists() else []
    out = {"books": analyze(book_lines, int(start), int(end)), "tcp": analyze_tcp(tcp_lines, int(start), int(end)),
           "report_only": True}
    rows, _ = read_rows(book_lines)
    tails = []
    for key, row in sorted((aggregate.get("rows") or {}).items()):
        if not key.startswith(("book_delta|OKX", "book_snapshot|OKX")):
            continue
        for sample in row.get("outliers") or []:
            if not sample.get("source_event_ns") or not sample.get("received_at_ns"):
                continue
            if (sample["received_at_ns"] - sample["source_event_ns"]) / 1e6 > TAIL_MS:
                tails.append({"row": key, **tail_attribution(sample, rows, tcp_lines)})
    out["book_tails"] = tails
    receipt = d / "receipt.json"
    if receipt.exists():
        r = json.loads(receipt.read_text())
        out.update(probe_exit=r.get("exit"), ingestor_pid_stable=r.get("ingestor_pid_stable"))
    out["books_sha256"] = hashlib.sha256(books.read_bytes()).hexdigest()
    out["tcp_sha256"] = hashlib.sha256(tcp.read_bytes()).hexdigest() if tcp.exists() else None
    return out


# ---------------------------------------------------------------- worker (inside the container)

def worker(seconds: int) -> None:
    import asyncio
    import websockets

    def out(d):
        sys.stdout.write(json.dumps(d, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    async def run():
        deadline = time.monotonic() + seconds
        try:
            async with websockets.connect(URL, ping_interval=None, max_queue=None, compression=None,
                                          open_timeout=10, max_size=8 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"op": "subscribe",
                                          "args": [{"channel": CHANNEL, "instId": i} for i in INSTRUMENTS]}))
                out({"kind": "subscribed", "at_ns": time.time_ns(), "args": len(INSTRUMENTS)})
                last_ping = time.monotonic()
                while time.monotonic() < deadline:
                    if time.monotonic() - last_ping > 20:
                        await ws.send("ping")
                        last_ping = time.monotonic()
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, min(5, deadline - time.monotonic())))
                    except asyncio.TimeoutError:
                        continue
                    now = time.time_ns()
                    raw = msg.encode() if isinstance(msg, str) else msg
                    if raw == b"pong":
                        continue
                    row = parse_frame(raw, now)
                    if row is not None:
                        out(row)
                    elif b'"event":"error"' in raw:
                        code = re.search(rb'"code":"(\d+)"', raw)
                        out({"kind": "error", "at_ns": now, "code": code.group(1).decode() if code else None})
        except Exception as exc:  # a disconnect ends the probe; the window becomes not covered, never EXTERNAL
            out({"kind": "disconnect", "at_ns": time.time_ns(), "error_type": type(exc).__name__})
        out({"kind": "done", "at_ns": time.time_ns()})

    asyncio.run(run())


# ---------------------------------------------------------------- launcher (host)

def run_plan(query_inspect: dict, name: str, seconds: int) -> tuple[list[str], dict]:
    """Pure: docker run argv (without the code) and provenance from the query_kn_1 inspect document."""
    image = query_inspect["Image"]
    user = (query_inspect.get("Config") or {}).get("User") or None
    argv = ["docker", "run", "--rm", "-i", "--init", "--name", name, "--label", "qdl.p184.role=g4-okx-books-probe",
            "--cpus", f"{CPUS:g}", "--memory", MEMORY, "--memory-swap", MEMORY, "--pids-limit", str(PIDS_LIMIT),
            "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=8m", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--network", "bridge"]
    if user:
        argv += ["--user", user]
    argv += ["--entrypoint", "python", image]
    provenance = {"probe_version": PROBE_VERSION, "launch_mode": "docker_run_isolated", "container_name": name,
                  "image_id": image, "image_source_container": QUERY, "network": "bridge", "cpus": CPUS,
                  "memory": MEMORY, "pids_limit": PIDS_LIMIT, "seconds": seconds, "url": URL, "channel": CHANNEL,
                  "instruments": list(INSTRUMENTS)}
    return argv, provenance


def remove_container(name: str) -> bool:
    subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
                   check=False)
    for _ in range(20):
        present = subprocess.run(["docker", "ps", "-aq", "--filter", f"name=^/{name}$"], capture_output=True,
                                 text=True, timeout=10, check=False)
        if present.returncode == 0 and not present.stdout.strip():
            return True
        time.sleep(.5)
    return False


def ingestor_identity(name: str) -> dict:
    state = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
    return {"container": name, "pid": int(state["State"]["Pid"]), "started": state["State"]["StartedAt"],
            "image": state["Image"]}


def _terminate(signum, frame):
    raise SystemExit(128 + signum)


def launch(args) -> int:
    args.output.mkdir(parents=True, exist_ok=False)
    query = json.loads(subprocess.check_output(["docker", "inspect", QUERY], text=True))[0]
    argv, provenance = run_plan(query, args.name, args.seconds)
    command = [*argv, "-B", "-c", Path(__file__).read_text(), "--worker", "--seconds", str(args.seconds)]
    before = ingestor_identity(args.ingestor)
    stop = threading.Event()
    lock = threading.Lock()
    previous = signal.signal(signal.SIGTERM, _terminate)
    process, code, written = None, None, 0
    with (args.output / "tcp.jsonl").open("x") as tcp_file:
        def tcp_out(d):
            with lock:
                tcp_file.write(json.dumps(d, separators=(",", ":")) + "\n")
                tcp_file.flush()
        sampler = threading.Thread(target=TcpSampler(before["pid"], tcp_out, stop).run, daemon=True)
        sampler.start()
        try:
            with (args.output / "books.jsonl").open("x") as evidence:
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                           stderr=subprocess.DEVNULL, text=True)
                for line in process.stdout:
                    written += len(line.encode())
                    if written > MAX_OUTPUT_BYTES:
                        evidence.write(json.dumps({"kind": "error", "at_ns": time.time_ns(), "code": "BYTE_CAP"}) + "\n")
                        break
                    evidence.write(line)
                code = process.wait(timeout=30)
        finally:
            stop.set()
            sampler.join(timeout=5)
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            removed = remove_container(args.name)
            signal.signal(signal.SIGTERM, previous)
            try:
                after = ingestor_identity(args.ingestor)
            except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError):
                after = None
            (args.output / "receipt.json").write_text(json.dumps({
                "exit": code, **provenance, "output_bytes": written, "container_absent_after_exit": removed,
                "ingestor_before": before, "ingestor_after": after,
                "ingestor_pid_stable": bool(after and after["pid"] == before["pid"]
                                            and after["started"] == before["started"]),
                "tcp_sampler": {"basis": "/proc/<ingestor pid>/net (host side, read-only)", "counters_hz": TCP_HZ,
                                "sockets_hz": SOCKET_HZ, "remote_port": REMOTE_PORT},
                "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "report_only": True,
                "payload_values_retained": False}, indent=1) + "\n")
    return 0 if code == 0 else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--launch", action="store_true")
    mode.add_argument("--analyze", type=Path)
    mode.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--seconds", type=int, default=150)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--name")
    parser.add_argument("--ingestor", default=INGESTOR)
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--end-ns", type=int)
    args = parser.parse_args(argv)
    if not 1 <= args.seconds <= MAX_SECONDS:
        parser.error("--seconds out of bounds")
    if args.worker:
        worker(args.seconds)
        return 0
    if args.analyze:
        if args.start_ns is None or args.end_ns is None:
            parser.error("--analyze needs --start-ns and --end-ns")
        d = args.analyze
        tcp = (d / "tcp.jsonl").read_text().splitlines() if (d / "tcp.jsonl").exists() else []
        print(json.dumps({"books": analyze((d / "books.jsonl").read_text().splitlines(), args.start_ns, args.end_ns),
                          "tcp": analyze_tcp(tcp, args.start_ns, args.end_ns)}, indent=1))
        return 0
    if not args.output or not args.name or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", args.name):
        parser.error("--launch needs --output and a docker-valid --name")
    return launch(args)


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""A3 evidence apparatus (P18.4 stage-100 amendment A3, owner 2026-10-18): host-local checks and per-lane timelines.

Purpose. config/v2/p184-stage100-a3-external-attribution.json classifies a G4 sample or a G9 eligibility loss as
EXTERNAL only with evidence for the SAME seconds that (a) the venue/session was the origin and (b) nothing local held
the data. This module records the local and DL-side half of that evidence; it never decides a verdict.

* `sample` (whole load window, host side, read-only): for each ingestor container (Binance USD-M, OKX swap)
  - 1 Hz: its cgroup v2 cpu.stat deltas (nr_throttled, throttled_usec, usage_usec) and the scheduler run-queue delay
    of its threads (/proc/<pid>/task/*/schedstat field 2, per-second delta: total and worst thread);
  - ~10 Hz socket receive queues + 1 Hz TcpExt/Tcp counters of its network namespace, by the existing
    p184_g4_okx_books_probe.TcpSampler (/proc/<pid>/net, no nsenter). Blind spot, stated in A3: samples ~100 ms apart
    that see an empty queue cannot exclude a shorter stall, nor frames already read into user space but processed late;
  - every 30 s: the host clock bound (`chronyc -n tracking`, p184_g4_okx_mark_index_probe.clock_sample).
  Output <out>/local.jsonl, <out>/tcp-<RUNTIME>.jsonl, <out>/receipt.json.
* `collect` (after the load, read-only `docker logs -t --since/--until`): whitelisted fields of the structured
  progress/session lines of both ingestors (per lane: generation, frames, last_frame_age, read_gap max + time,
  produce/enqueue max; session events: heartbeat_failed, disconnected, connect_failed, session_ready, staleness),
  the realtime cores (raw_age = core normalize - DL receipt, normalize/commit maxima) and the Stream gateways
  (dispatch_lag cumulative buckets: dispatch - canonical CreateTime, LAG_BOUNDS_MS). Free text other than the
  ingestor's I/O error string (truncated) is never kept. Output <out>/timeline.jsonl + receipt.json.
Pure readers for the A3 scorer: read_local(), read_timeline().

Boundary: no container is entered, restarted or created; docker inspect / docker logs and /proc, /sys reads only;
writes only <out>. Self-bounded by --seconds (sample) and by the given window (collect).
usage: p184_a3_evidence.py sample --out DIR --seconds N
       p184_a3_evidence.py collect --out DIR --since-unix S --until-unix U
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EVIDENCE_VERSION = "qdl.p184.a3-evidence.v1"
PREFIX = "qdl_v2_stable_candidate-"
INGESTORS = {"BINANCE": PREFIX + "ingestor_binance_usdm-1", "OKX": PREFIX + "ingestor_okx_swap-1"}
CORES = tuple(PREFIX + name for name in ("rust_core-1", "rust_core_2-1", "rust_core_3-1"))
GATEWAYS = tuple(PREFIX + name for name in ("stream_kn_1-1", "stream_kn_2-1"))
LAG_BOUNDS_MS = (1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000)  # rust/qdl-stream-gateway/src/hub.rs:317
MAX_SECONDS = 4 * 3600
CLOCK_EVERY_S = 30
SESSION_EVENTS = ("qdl_native_session_heartbeat_failed", "qdl_native_session_disconnected",
                  "qdl_native_connect_failed", "qdl_native_session_ready", "qdl_native_staleness_triggered",
                  "qdl_native_staleness_recycled", "qdl_native_standby_failed", "qdl_native_session_notice",
                  "qdl_native_runtime_retry", "qdl_native_raw_ingestor_started", "qdl_native_raw_ingestor_stopped")
LANE_FIELDS = ("lane", "generation", "frames", "frames_total", "last_frame_age_ms", "stale_frames",
               "staleness_recycles_total", "standby_failures_total", "inflight_max")
SESSION_FIELDS = ("event", "runtime", "service", "lane", "generation", "attempt", "session", "continued",
                  "connect_to_ready_ms", "previous_generation", "previous_last_frame_to_ready_ms", "cause",
                  "last_frame_to_detected_ms", "drain_ms")


def utc_ns(stamp: str) -> int:
    """docker logs -t RFC 3339 (nanoseconds) -> unix ns."""
    head, _, frac = stamp.rstrip("Z").partition(".")
    base = dt.datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    return int(base.timestamp()) * 1_000_000_000 + int((frac + "000000000")[:9])


def _span(value) -> dict | None:
    if not isinstance(value, dict):
        return None
    return {k: value.get(k) for k in ("max", "max_at_ns", "n") if k in value}


# ---------------------------------------------------------------- collect (pure line filters + docker logs)

def filter_line(role: str, container: str, line: str) -> dict | None:
    """One `docker logs -t` line -> whitelisted record or None (non-JSON or unknown event)."""
    stamp, _, text = line.partition(" ")
    if not text.startswith("{"):
        return None
    try:
        item = json.loads(text)
        at_ns = utc_ns(stamp)
    except (ValueError, json.JSONDecodeError):
        return None
    event = item.get("event")
    base = {"role": role, "container": container, "at_ns": at_ns, "event": event}
    if role == "ingestor" and event == "qdl_native_raw_ingestor_progress":
        lanes = []
        for lane in item.get("lanes") or []:
            if isinstance(lane, dict):
                lanes.append({**{k: lane.get(k) for k in LANE_FIELDS},
                              **{name: _span(lane.get(name)) for name in ("read_gap_ms", "produce_ms", "enqueue_ms",
                                                                          "venue_lag_ms")}})
        return {**base, "runtime": item.get("runtime"), "interval_s": item.get("interval_s"), "lanes": lanes}
    if role == "ingestor" and event in SESSION_EVENTS:
        out = {**base, **{k: item.get(k) for k in SESSION_FIELDS if k in item}}
        if isinstance(item.get("error"), str):
            out["error"] = item["error"][:120]
        detail = item.get("detail")
        if isinstance(detail, dict) and isinstance(detail.get("code"), (str, int)):
            out["detail_code"] = str(detail["code"])[:40]
        return out
    if role == "core" and event == "qdl_realtime_core_progress":
        return {**base, **{name: _span(item.get(name)) for name in ("raw_age_websocket_ms", "raw_age_ms",
                                                                    "normalize_ms", "collect_ms", "commit_ms",
                                                                    "commit_wait_ms")}}
    if role == "gateway" and event == "qdl_kn_gateway_metrics":
        lag = item.get("dispatch_lag_ms") or {}
        return {**base, "dispatch_lag_buckets": lag.get("buckets"), "dispatch_lag_n": lag.get("n"),
                "overflow": item.get("overflow"), "refused": item.get("refused"), "lagging": item.get("lagging")}
    return None


def docker_logs(container: str, since_unix: float, until_unix: float) -> list[str]:
    fmt = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # noqa: E731
    result = subprocess.run(["docker", "logs", "-t", "--since", fmt(since_unix), "--until", fmt(until_unix), container],
                            capture_output=True, text=True, timeout=300, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"docker logs failed for {container}")
    return (result.stdout + "\n" + result.stderr).splitlines()


def collect(out: Path, since_unix: float, until_unix: float, *, logs=docker_logs) -> dict:
    out.mkdir(parents=True, exist_ok=False)
    roles = [("ingestor", c) for c in INGESTORS.values()] + [("core", c) for c in CORES] + \
            [("gateway", c) for c in GATEWAYS]
    counts, problems, records = {}, [], []
    for role, container in roles:
        try:
            lines = logs(container, since_unix, until_unix)
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            problems.append(f"{container}: {type(exc).__name__}")
            continue
        kept = [r for r in (filter_line(role, container, line) for line in lines) if r is not None]
        counts[container] = len(kept)
        records.extend(kept)
    records.sort(key=lambda r: (r["at_ns"], r["container"]))
    path = out / "timeline.jsonl"
    with path.open("x") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
    receipt = {"evidence_version": EVIDENCE_VERSION, "mode": "collect", "since_unix": since_unix,
               "until_unix": until_unix, "records_per_container": counts, "problems": problems,
               "timeline_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
               "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "payload_values_retained": False}
    (out / "receipt.json").write_text(json.dumps(receipt, indent=1, sort_keys=True) + "\n")
    return receipt


def read_timeline(lines) -> list[dict]:
    return [json.loads(line) for line in lines if line.strip()]


# ---------------------------------------------------------------- sample (host loop)

def parse_cpu_stat(text: str) -> dict:
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            out[parts[0]] = int(parts[1])
    return out


def parse_schedstat(text: str) -> int | None:
    """/proc/<pid>/task/<tid>/schedstat -> run-queue wait ns (field 2), None if malformed."""
    parts = text.split()
    return int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None


def container_identity(name: str) -> dict:
    state = json.loads(subprocess.check_output(["docker", "inspect", name], text=True, timeout=30))[0]
    return {"container": name, "id": state["Id"], "pid": int(state["State"]["Pid"]),
            "started": state["State"]["StartedAt"], "image": state["Image"]}


class LocalSampler:
    """1 Hz cgroup throttling + thread run-queue delay of one ingestor (host side, read-only)."""

    def __init__(self, runtime: str, identity: dict, out, stop: threading.Event, *, proc_root="/proc",
                 cgroup_root="/sys/fs/cgroup/system.slice") -> None:
        self.runtime, self.identity, self.out, self.stop = runtime, identity, out, stop
        self.proc = Path(proc_root) / str(identity["pid"])
        self.cgroup = Path(cgroup_root) / f"docker-{identity['id']}.scope" / "cpu.stat"

    def threads(self) -> dict:
        values = {}
        for task in (self.proc / "task").iterdir():
            try:
                wait = parse_schedstat((task / "schedstat").read_text())
            except OSError:
                continue
            if wait is not None:
                values[task.name] = wait
        return values

    def run(self) -> None:
        try:
            cpu, waits = parse_cpu_stat(self.cgroup.read_text()), self.threads()
        except OSError as exc:
            self.out({"kind": "local_error", "runtime": self.runtime, "at_ns": time.time_ns(),
                      "error_type": type(exc).__name__})
            return
        while not self.stop.wait(1.0):
            try:
                now_cpu, now_waits = parse_cpu_stat(self.cgroup.read_text()), self.threads()
            except OSError as exc:
                self.out({"kind": "local_error", "runtime": self.runtime, "at_ns": time.time_ns(),
                          "error_type": type(exc).__name__})
                return
            deltas = [now_waits[t] - waits[t] for t in now_waits if t in waits and now_waits[t] >= waits[t]]
            self.out({"kind": "local", "runtime": self.runtime, "at_ns": time.time_ns(),
                      **{f"d_{k}": now_cpu.get(k, 0) - cpu.get(k, 0)
                         for k in ("nr_throttled", "throttled_usec", "usage_usec", "nr_periods")},
                      "runq_wait_ms_total": round(sum(deltas) / 1e6, 3),
                      "runq_wait_ms_max_thread": round(max(deltas, default=0) / 1e6, 3), "threads": len(now_waits)})
            cpu, waits = now_cpu, now_waits


def sample(out: Path, seconds: int) -> int:
    from scripts import p184_g4_okx_books_probe as books
    from scripts import p184_g4_okx_mark_index_probe as markprobe

    out.mkdir(parents=True, exist_ok=False)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    identities = {runtime: container_identity(name) for runtime, name in INGESTORS.items()}
    lock = threading.Lock()
    files = {"local": (out / "local.jsonl").open("x")}
    for runtime in INGESTORS:
        files[runtime] = (out / f"tcp-{runtime}.jsonl").open("x")

    def writer(name):
        def write(item):
            with lock:
                files[name].write(json.dumps(item, separators=(",", ":")) + "\n")
                files[name].flush()
        return write

    threads = []
    for runtime, identity in identities.items():
        threads.append(threading.Thread(target=LocalSampler(runtime, identity, writer("local"), stop).run, daemon=True))
        threads.append(threading.Thread(target=books.TcpSampler(identity["pid"], writer(runtime), stop).run,
                                        daemon=True))
    for thread in threads:
        thread.start()
    clocks = []
    deadline = time.monotonic() + seconds
    try:
        while not stop.is_set() and time.monotonic() < deadline:
            sample_ = markprobe.clock_sample("periodic")
            clocks.append(sample_)
            writer("local")({"kind": "clock", **sample_})
            stop.wait(min(CLOCK_EVERY_S, max(0.0, deadline - time.monotonic())))
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)
        final = markprobe.clock_sample("final")
        clocks.append(final)
        writer("local")({"kind": "clock", **final})
        for handle in files.values():
            handle.close()
        after = {}
        for runtime, name in INGESTORS.items():
            try:
                after[runtime] = container_identity(name)
            except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError):
                after[runtime] = None
        bounds = [c["clock"]["bound_ms"] for c in clocks if c.get("clock")]
        (out / "receipt.json").write_text(json.dumps({
            "evidence_version": EVIDENCE_VERSION, "mode": "sample", "seconds": seconds,
            "ingestors_before": identities, "ingestors_after": after,
            "ingestor_pid_stable": {r: bool(after.get(r) and after[r]["pid"] == identities[r]["pid"]
                                            and after[r]["started"] == identities[r]["started"]) for r in INGESTORS},
            "host_clock_bound_ms_max": max(bounds) if bounds else None, "clock_samples": len(clocks),
            "tcp_blind_spot": "receive-queue samples ~100 ms apart cannot exclude a shorter stall nor frames read into "
                              "user space but processed late",
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "payload_values_retained": False}, indent=1, sort_keys=True) + "\n")
    return 0


def read_local(out: Path) -> dict:
    """{'local': [...], 'clock': [...], 'tcp': {RUNTIME: [lines]}, 'receipt': {...}} of a `sample` directory."""
    out = Path(out)
    local, clock = [], []
    path = out / "local.jsonl"
    for line in (path.read_text().splitlines() if path.exists() else []):
        if line.strip():
            item = json.loads(line)
            (clock if item.get("kind") == "clock" else local).append(item)
    tcp = {runtime: ((out / f"tcp-{runtime}.jsonl").read_text().splitlines()
                     if (out / f"tcp-{runtime}.jsonl").exists() else []) for runtime in INGESTORS}
    receipt = json.loads((out / "receipt.json").read_text()) if (out / "receipt.json").exists() else {}
    return {"local": local, "clock": clock, "tcp": tcp, "receipt": receipt}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--out", type=Path, required=True)
    s.add_argument("--seconds", type=int, required=True)
    c = sub.add_parser("collect")
    c.add_argument("--out", type=Path, required=True)
    c.add_argument("--since-unix", type=float, required=True)
    c.add_argument("--until-unix", type=float, required=True)
    args = parser.parse_args(argv)
    if args.command == "sample":
        if not 1 <= args.seconds <= MAX_SECONDS:
            parser.error("--seconds out of bounds")
        return sample(args.out, args.seconds)
    if args.until_unix <= args.since_unix or args.until_unix - args.since_unix > MAX_SECONDS:
        parser.error("bad collect window")
    print(json.dumps(collect(args.out, args.since_unix, args.until_unix), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

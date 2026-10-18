#!/usr/bin/env python3
"""Report-only OKX venue-direct mark-price + index-tickers probe for every G4 window (G4 amendment 1, 2026-10-18).

Purpose. config/v2/p184-stage100-g4-amendment-1.json scores the paired row mark_index_price|OKX| on its internal split
(Kafka CreateTime - newest component receipt) and reports the composite value. The composite is dominated by how long
the OKX index stays unchanged (index-tickers pushes only on change, else once a minute); attributing that part to the
venue needs paired SAME-WINDOW venue evidence. This probe opens ONE public WebSocket to OKX (no key, no order path),
subscribes mark-price and index-tickers for the six instruments of the DL row and records, per pushed row, the local
receipt wall clock, channel, instrument and venue ts. No prices or other payload values are kept.

It never gates anything by itself. The scorer (scripts/p184_gate_campaign.py, amendment row) reads analyze() per
window: when the probe did not cover a window the attribution of that window is UNKNOWN, and UNKNOWN is not EXTERNAL
(the amended row cannot PASS). Host clock bound: `chronyc -n tracking` before and after (|system offset| + root
dispersion + root delay / 2, chrony's error bound), so recv - venue ts carries that bound plus the venue clock.

Modes:
  --launch   (campaign apparatus) one bounded `docker run --rm` of the image query_kn_1 runs (resolved by docker
             inspect), default bridge network, read-only rootfs, no capabilities, --cpus/--memory/--pids-limit, a
             name under the campaign's G4 observer prefix (the foreign-activity guard classifies it as campaign and
             removes it on abort); `docker rm -f` by that name on every exit path. Writes <output>/probe.jsonl and
             <output>/receipt.json (clock samples, launch provenance). Self-bounded by --seconds.
  --analyze  offline: per-channel cadence, recv - venue ts, venue-only pair gap at each push, coverage, restricted to
             [--start-ns, --end-ns] (the observer window).
  --worker   (inside the container) the WebSocket reader.
Boundary: no production container is touched, no write besides the output directory.
usage: p184_g4_okx_mark_index_probe.py --launch --output DIR --seconds N --name NAME | --analyze FILE --start-ns A --end-ns B
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
import time

PROBE_VERSION = "qdl.p184.g4-okx-mark-index-probe.v1"
QUERY = "qdl_v2_stable_candidate-query_kn_1-1"
URL = "wss://ws.okx.com:8443/ws/v5/public"
MARK = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP", "BTC-USD-SWAP")
INDEX_OF = {"BTC-USDT-SWAP": "BTC-USDT", "ETH-USDT-SWAP": "ETH-USDT", "SOL-USDT-SWAP": "SOL-USDT",
            "BNB-USDT-SWAP": "BNB-USDT", "DOGE-USDT-SWAP": "DOGE-USDT", "BTC-USD-SWAP": "BTC-USD"}
MARK_OF = {v: k for k, v in INDEX_OF.items()}
CPUS, MEMORY, PIDS_LIMIT = 0.25, "128m", 32
MAX_SECONDS = 600
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
EDGE_TOLERANCE_NS = 5_000_000_000   # coverage: probe rows within 5 s of both window edges
CHRONY = re.compile(r"^(?P<k>System time|Root delay|Root dispersion)\s*:\s*(?P<v>[0-9.]+) seconds(?: (?P<d>fast|slow))?")


def pct(values, f):
    """Nearest rank, the observer's v1-v3 convention (index int(f * n) of the sorted values)."""
    values = sorted(values)
    return round(values[min(len(values) - 1, int(f * len(values)))], 1) if values else None


def stats(values):
    return {"n": len(values), "p50": pct(values, .5), "p95": pct(values, .95),
            "max": round(max(values), 1) if values else None}


def parse_chrony(text: str) -> dict | None:
    """{'offset_ms', 'root_delay_ms', 'root_dispersion_ms', 'bound_ms'} from `chronyc -n tracking`, else None."""
    found = {}
    for line in text.splitlines():
        m = CHRONY.match(line.strip())
        if m:
            value = float(m["v"]) * 1000
            found[m["k"]] = -value if m["d"] == "slow" else value
    if len(found) != 3:
        return None
    offset, delay, dispersion = found["System time"], found["Root delay"], found["Root dispersion"]
    return {"offset_ms": round(offset, 6), "root_delay_ms": round(delay, 6), "root_dispersion_ms": round(dispersion, 6),
            "bound_ms": round(abs(offset) + dispersion + delay / 2, 6), "basis": "chronyc -n tracking"}


def clock_sample(label: str) -> dict:
    try:
        out = subprocess.run(["chronyc", "-n", "tracking"], capture_output=True, text=True, timeout=10)
        parsed = parse_chrony(out.stdout) if out.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        parsed = None
    return {"label": label, "at_ns": time.time_ns(), "clock": parsed}


def analyze(lines, start_ns: int, end_ns: int) -> dict:
    """Per-window probe statistics (report-only). Rows outside [start_ns, end_ns] only seed the latest-component
    state. covered: rows of both channels within EDGE_TOLERANCE_NS of both edges, every instrument with >= 1 index
    push inside, and no disconnect / error before end_ns."""
    rows, ended_ns, problems = [], None, []
    for line in lines:
        if not line.strip():
            continue
        item = json.loads(line)
        if "r" in item and "c" in item:
            rows.append(item)
        elif item.get("kind") in ("disconnect", "error"):
            if item.get("at_ns", 0) < end_ns:
                problems.append(item.get("kind"))
        elif item.get("kind") == "done":
            ended_ns = item.get("at_ns")
    rows.sort(key=lambda r: r["r"])
    inside = [r for r in rows if start_ns <= r["r"] <= end_ns]
    channels = {"mark-price": [r for r in inside if r["c"] == "mark-price"],
                "index-tickers": [r for r in inside if r["c"] == "index-tickers"]}
    last_ts, latest, gaps, older = {}, {}, [], {"index": 0, "mark": 0, "same": 0}
    intervals = {"mark-price": [], "index-tickers": []}
    for r in rows:
        key = (r["c"], r["i"])
        sym = r["i"] if r["c"] == "mark-price" else MARK_OF.get(r["i"])
        if sym is None:
            continue
        if start_ns <= r["r"] <= end_ns and key in last_ts:
            intervals[r["c"]].append(r["ts"] - last_ts[key])
        last_ts[key] = r["ts"]
        latest[(sym, r["c"])] = r["ts"]
        mark, index = latest.get((sym, "mark-price")), latest.get((sym, "index-tickers"))
        if start_ns <= r["r"] <= end_ns and mark is not None and index is not None:
            gaps.append(abs(mark - index))
            older["index" if index < mark else "mark" if mark < index else "same"] += 1
    index_insts = {r["i"] for r in channels["index-tickers"]}
    edges_ok = all(ch and ch[0]["r"] <= start_ns + EDGE_TOLERANCE_NS and ch[-1]["r"] >= end_ns - EDGE_TOLERANCE_NS
                   for ch in channels.values())
    missing_index = sorted(set(INDEX_OF.values()) - index_insts)
    if not edges_ok:
        problems.append("rows do not reach both window edges")
    if missing_index:
        problems.append(f"no index push inside the window for {', '.join(missing_index)}")
    out = {"probe_version": PROBE_VERSION, "report_only": True, "window": {"start_ns": start_ns, "end_ns": end_ns},
           "covered": not problems, "coverage_problems": problems, "done_at_ns": ended_ns,
           "rows_in_window": len(inside), "pair_ts_gap_at_push_ms": stats(gaps), "older_component_at_push": older}
    for name, items in channels.items():
        out[name] = {"pushes": len(items), "ts_interval_ms": stats(intervals[name]),
                     "recv_minus_venue_ts_ms": stats([r["r"] / 1e6 - r["ts"] for r in items]),
                     "instruments": len({r["i"] for r in items})}
    return out


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
                                          open_timeout=10) as ws:
                args = ([{"channel": "mark-price", "instId": i} for i in MARK]
                        + [{"channel": "index-tickers", "instId": INDEX_OF[i]} for i in MARK])
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                out({"kind": "subscribed", "at_ns": time.time_ns(), "args": len(args)})
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
                    if msg == "pong":
                        continue
                    d = json.loads(msg)
                    if "data" in d:
                        channel, inst = d["arg"]["channel"], d["arg"]["instId"]
                        for row in d["data"]:
                            out({"r": now, "c": channel, "i": inst, "ts": int(row["ts"])})
                    elif d.get("event") == "error":
                        out({"kind": "error", "at_ns": now, "code": d.get("code")})
        except Exception as exc:  # a disconnect ends the probe; the window becomes UNKNOWN, never EXTERNAL
            out({"kind": "disconnect", "at_ns": time.time_ns(), "error_type": type(exc).__name__})
        out({"kind": "done", "at_ns": time.time_ns()})

    asyncio.run(run())


def run_plan(query_inspect: dict, name: str, seconds: int) -> tuple[list[str], dict]:
    """Pure: docker run argv (without the code) and provenance from the query_kn_1 inspect document."""
    image = query_inspect["Image"]
    user = (query_inspect.get("Config") or {}).get("User") or None
    argv = ["docker", "run", "--rm", "-i", "--init", "--name", name, "--label", "qdl.p184.role=g4-okx-probe",
            "--cpus", f"{CPUS:g}", "--memory", MEMORY, "--memory-swap", MEMORY, "--pids-limit", str(PIDS_LIMIT),
            "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=8m", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--network", "bridge"]
    if user:
        argv += ["--user", user]
    argv += ["--entrypoint", "python", image]
    provenance = {"probe_version": PROBE_VERSION, "launch_mode": "docker_run_isolated", "container_name": name,
                  "image_id": image, "image_source_container": QUERY, "network": "bridge", "cpus": CPUS,
                  "memory": MEMORY, "pids_limit": PIDS_LIMIT, "seconds": seconds, "url": URL,
                  "instruments": {"mark-price": list(MARK), "index-tickers": [INDEX_OF[i] for i in MARK]}}
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


def _terminate(signum, frame):
    raise SystemExit(128 + signum)


def launch(args) -> int:
    args.output.mkdir(parents=True, exist_ok=False)
    query = json.loads(subprocess.check_output(["docker", "inspect", QUERY], text=True))[0]
    argv, provenance = run_plan(query, args.name, args.seconds)
    command = [*argv, "-B", "-c", Path(__file__).read_text(), "--worker", "--seconds", str(args.seconds)]
    clocks = [clock_sample("before")]
    previous = signal.signal(signal.SIGTERM, _terminate)
    process, code, written = None, None, 0
    try:
        with (args.output / "probe.jsonl").open("x") as evidence:
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
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        removed = remove_container(args.name)
        clocks.append(clock_sample("after"))
        signal.signal(signal.SIGTERM, previous)
        bounds = [c["clock"]["bound_ms"] for c in clocks if c.get("clock")]
        (args.output / "receipt.json").write_text(json.dumps({
            "exit": code, **provenance, "output_bytes": written, "container_absent_after_exit": removed,
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "report_only": True,
            "payload_values_retained": False, "host_clock": clocks,
            "host_clock_bound_ms_max": max(bounds) if bounds else None}, indent=1) + "\n")
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
        print(json.dumps(analyze(args.analyze.read_text().splitlines(), args.start_ns, args.end_ns), indent=1))
        return 0
    if not args.output or not args.name or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", args.name):
        parser.error("--launch needs --output and a docker-valid --name")
    return launch(args)


if __name__ == "__main__":
    sys.exit(main())

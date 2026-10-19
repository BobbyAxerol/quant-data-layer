#!/usr/bin/env python3
"""Report-only Binance USD-M fresh-connection probe + ingestor-netns TCP sampler for every G4 window (P18.4 A3).

Purpose. Under the P18.4 stage-100 amendment A3 a G4 / G2 / G9 tail on a Binance USD-M row can only be attributed to the
venue (EXTERNAL) with same-second venue evidence taken on an independent connection; without it the attribution is
UNKNOWN. This probe gives that evidence per G4 window, outside the scored metrics, on TWO fresh public WebSockets (no
key, no order path) opened from a bounded throw-away container:

* public: wss://fstream.binance.com/public/ws - `<sym>@bookTicker` and `<sym>@trade` for BTCUSDT, ETHUSDT, SOLUSDT,
  BNBUSDT, DOGEUSDT, `<sym>@depth@100ms` for BTCUSDT, ETHUSDT;
* market: wss://fstream.binance.com/market/ws - `<sym>@markPrice@1s` for the same five symbols.

Channel provenance (checked in the DL source, not assumed): rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs (the
`/public/ws` routed base and the Binance-only `/market/ws` base: Binance split its USD-M WebSocket into /public,
/market and /private and decommissioned the unrouted form on 2026-04-23; `@markPrice` lives under /market) and
config/v2/stable-acquisition-bindings.yaml (`websocket_url` / `market_websocket_url`, native channels
`<sym>@depth@100ms`, `<sym>@bookTicker`, `<sym>@trade`, `<sym>@markPrice@1s`). The probe uses exactly those streams.

Frames are never JSON-decoded and no payload value (price, size, level) is retained: bounded regexes read only the
event type `e`, symbol `s`, event time `E`, transaction time `T` and, for depthUpdate, `U` / `u` / `pu` (continuity:
`pu` must equal the previous `u` of the symbol; a mismatch is a seq break). markPriceUpdate's `T` is the NEXT FUNDING
TIME (a future instant), not a transaction time, so it is ignored for markPrice. The worker emits per-second
aggregates keyed by (connection, channel, symbol, venue second E // 1000) instead of one line per frame; a key is
flushed once the local clock passes its second + FLUSH_AFTER_S (a later frame for a flushed second emits another row,
read_aggregates() merges them).

Keepalive. ping_interval=None (no client pings), as in the OKX probes: the Binance USD-M WebSocket docs (author's
knowledge, not re-fetched for this file) say the server sends a ping frame every 3 minutes and drops a connection
that sends no pong within 10 minutes; `websockets` answers server pings automatically. Client pings would only spend
the per-connection incoming-message budget (10 messages/s) for nothing: a probe lives at most MAX_SECONDS.
close_timeout=2: the venue answered the close handshake only after ~9 s in the live smoke (default 10 s), which only
delays the end of a probe whose data window is already over.
The receipt time is taken when recv() returns, so a slow probe can only inflate its own lag (towards LATE, never a
false HEALTHY).

The host side (no docker exec, no nsenter) samples the ingestor_binance_usdm network namespace through
/proc/<ingestor pid>/net with TcpSampler / analyze_tcp of scripts/p184_g4_okx_books_probe.py (imported, not copied)
and takes `chronyc -n tracking` before and after with clock_sample() of scripts/p184_g4_okx_mark_index_probe.py.

It never gates anything. The A3 scorer imports read_aggregates() / analyze() / probe_state() / window_report():
probe_state() is the probe's view of one channel (and symbol) in the venue seconds of a tail - HEALTHY / LATE /
INDETERMINATE / NO_COVERAGE. Only HEALTHY or LATE can be external evidence; INDETERMINATE and NO_COVERAGE never are.

Modes:
  --launch   (campaign apparatus) one bounded `docker run --rm` of the image query_kn_1 runs (resolved by docker
             inspect), default bridge network, read-only rootfs, no capabilities, --cpus/--memory/--pids-limit, a
             name chosen by the caller (the campaign's G4 observer prefix); `docker rm -f` by that name on every exit
             path; the TCP sampler runs in this (host) process. Writes <output>/frames.jsonl, <output>/tcp.jsonl and
             <output>/receipt.json. Self-bounded by --seconds (<= MAX_SECONDS) and MAX_OUTPUT_BYTES.
  --analyze  offline: window_report() of DIR restricted to [--start-ns, --end-ns].
  --worker   (inside the container) the WebSocket reader.
Boundary: no production container is touched or entered (docker inspect + /proc reads only); no write besides the
output directory.
usage: p184_g4_binance_probe.py --launch --output DIR --seconds N --name NAME [--ingestor CONTAINER]
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

try:
    _ROOT = Path(__file__).resolve().parents[1]
except NameError:  # --worker inside the container runs as `python -B -c <text>`: no __file__, no sibling scripts
    _ROOT = None
if _ROOT is not None:
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))
    from scripts.p184_g4_okx_books_probe import RX_PERSIST_SAMPLES, TcpSampler, analyze_tcp  # noqa: E402
    from scripts.p184_g4_okx_mark_index_probe import clock_sample  # noqa: E402

PROBE_VERSION = "qdl.p184.g4-binance-probe.v1"
QUERY = "qdl_v2_stable_candidate-query_kn_1-1"
INGESTOR = "qdl_v2_stable_candidate-ingestor_binance_usdm-1"
ROLE_LABEL = "qdl.p184.role=g4-binance-probe"
URLS = {"public": "wss://fstream.binance.com/public/ws",   # stable-acquisition-bindings.yaml websocket_url
        "market": "wss://fstream.binance.com/market/ws"}   # stable-acquisition-bindings.yaml market_websocket_url
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT")
DEPTH_SYMBOLS = ("BTCUSDT", "ETHUSDT")
SUBSCRIPTIONS = {"public": {"bookTicker": SYMBOLS, "trade": SYMBOLS, "depth": DEPTH_SYMBOLS},
                 "market": {"markPrice": SYMBOLS}}
STREAM_SUFFIX = {"bookTicker": "@bookTicker", "trade": "@trade", "depth": "@depth@100ms", "markPrice": "@markPrice@1s"}
CONN_OF = {channel: conn for conn, channels in SUBSCRIPTIONS.items() for channel in channels}
EVENT_CHANNEL = {"bookTicker": "bookTicker", "trade": "trade", "depthUpdate": "depth", "markPriceUpdate": "markPrice"}
CONTINUOUS = ("bookTicker", "depth", "markPrice")  # a row every venue second is expected; trade may be quiet
CPUS, MEMORY, PIDS_LIMIT = 0.5, "192m", 32
MAX_SECONDS = 600
MAX_OUTPUT_BYTES = 32 * 1024 * 1024   # per-event id lines (~180/s x ~90 B over <= 600 s) + aggregates
EDGE_TOLERANCE_NS = 5_000_000_000      # coverage: rows within 5 s of both window edges
FLUSH_AFTER_S = 5                      # a per-second key is flushed once the local clock passes second + 5 s
S_NS = 1_000_000_000
EVENT = re.compile(rb'"e":"([A-Za-z]{1,32})"')
SYMBOL = re.compile(rb'"s":"([A-Z0-9]{2,24})"')
E_TIME = re.compile(rb'"E":(\d{10,16})')
T_TIME = re.compile(rb'"T":(\d{10,16})')
FIRST_ID = re.compile(rb'"U":(\d{1,20})')
FINAL_ID = re.compile(rb'"u":(\d{1,20})')
PREV_FINAL_ID = re.compile(rb'"pu":(-?\d{1,20})')
TRADE_ID = re.compile(rb'"t":(\d{1,20})')
ERROR_CODE = re.compile(rb'"code":\s?(-?\d{1,8})')


def pct(values, f):
    """Nearest rank, the observer's v1-v3 convention (index int(f * n) of the sorted values)."""
    values = sorted(values)
    return round(values[min(len(values) - 1, int(f * len(values)))], 1) if values else None


def stats(values):
    return {"n": len(values), "p50": pct(values, .5), "p95": pct(values, .95),
            "max": round(max(values), 1) if values else None}


def streams(conn: str) -> list[str]:
    """The SUBSCRIBE params of one connection, in the ingestor's native channel spelling."""
    return [f"{sym.lower()}{STREAM_SUFFIX[channel]}" for channel, syms in SUBSCRIPTIONS[conn].items() for sym in syms]


# ---------------------------------------------------------------- frames (pure)

def parse_frame(raw: bytes) -> dict | None:
    """One Binance USD-M event frame -> {'c', 'i', 'E', 'T', 'U', 'u', 'pu'} (ids / times only), else None.

    None for anything that is not one of the four subscribed event types (subscription acks {"result":null,"id":N},
    errors, unknown events). 'T' is None for markPrice (its T is the next funding time); U / u / pu only for depth."""
    event = EVENT.search(raw)
    channel = EVENT_CHANNEL.get(event.group(1).decode()) if event else None
    if channel is None:
        return None
    sym, e_time = SYMBOL.search(raw), E_TIME.search(raw)
    if not sym or not e_time:
        return None
    t_time = T_TIME.search(raw) if channel != "markPrice" else None
    out = {"c": channel, "i": sym.group(1).decode(), "E": int(e_time.group(1)),
           "T": int(t_time.group(1)) if t_time else None, "U": None, "u": None, "pu": None}
    if channel == "depth":
        for key, rx in (("U", FIRST_ID), ("u", FINAL_ID), ("pu", PREV_FINAL_ID)):
            m = rx.search(raw)
            out[key] = int(m.group(1)) if m else None
    elif channel == "bookTicker":
        m = FINAL_ID.search(raw)
        out["u"] = int(m.group(1)) if m else None
    out["id"] = event_id(out, raw)
    return out


def event_id(frame: dict, raw: bytes = b"") -> int | None:
    """The venue identity DL keeps as the canonical source_sequence of the same event (observed in campaign
    092255Z canonical records): bookTicker / depth update id `u` (quote seq = u, book seq = g<gen>:u), trade id `t`
    (trade seq = t), markPrice event time `E` (mark seq = E:E:receipt:receipt:digest). A3 joins on it for SAME-event
    evidence; a different event in the same second is supporting evidence only."""
    if frame["c"] in ("bookTicker", "depth"):
        return frame.get("u")
    if frame["c"] == "trade":
        m = TRADE_ID.search(raw)
        return int(m.group(1)) if m else None
    return frame["E"]


def error_code(raw: bytes) -> str | None:
    """The code of a Binance error reply ({"error":{"code":N,"msg":...},"id":M} or {"code":N,"msg":...}) - a frame
    that is not an event and carries a numeric code - else None (acks {"result":null,"id":N} have none)."""
    if EVENT.search(raw):
        return None
    m = ERROR_CODE.search(raw)
    return m.group(1).decode() if m else None


class Aggregator:
    """Per-second aggregates keyed by (conn, channel, symbol, venue second). emit(row) receives flushed rows."""

    def __init__(self, emit) -> None:
        self.emit = emit
        self.open: dict[tuple, dict] = {}
        self.last_u: dict[tuple, int] = {}  # (conn, symbol) -> previous depthUpdate u (spans seconds)

    def add(self, conn: str, frame: dict, recv_ns: int) -> None:
        second = frame["E"] // 1000
        key = (conn, frame["c"], frame["i"], second)
        row = self.open.get(key)
        if row is None:
            row = self.open[key] = {"k": "agg", "conn": conn, "c": frame["c"], "i": frame["i"], "s": second, "n": 0,
                                    "lag_e_max_ms": None, "lag_e_min_ms": None, "lag_t_max_ms": None,
                                    "recv_first_ns": recv_ns, "recv_last_ns": recv_ns, "seq_breaks": 0}
        recv_ms = recv_ns / 1e6
        lag_e = round(recv_ms - frame["E"], 3)
        row["n"] += 1
        row["lag_e_max_ms"] = lag_e if row["lag_e_max_ms"] is None else max(row["lag_e_max_ms"], lag_e)
        row["lag_e_min_ms"] = lag_e if row["lag_e_min_ms"] is None else min(row["lag_e_min_ms"], lag_e)
        if frame.get("T") is not None:
            lag_t = round(recv_ms - frame["T"], 3)
            row["lag_t_max_ms"] = lag_t if row["lag_t_max_ms"] is None else max(row["lag_t_max_ms"], lag_t)
        row["recv_first_ns"] = min(row["recv_first_ns"], recv_ns)
        row["recv_last_ns"] = max(row["recv_last_ns"], recv_ns)
        if frame["c"] == "depth":
            previous = self.last_u.get((conn, frame["i"]))
            if previous is not None and frame.get("pu") is not None and frame["pu"] != previous:
                row["seq_breaks"] += 1
            if frame.get("u") is not None:
                self.last_u[(conn, frame["i"])] = frame["u"]

    def flush(self, now_ns: int, *, everything: bool = False) -> int:
        due = [k for k in self.open if everything or now_ns >= (k[3] + FLUSH_AFTER_S) * S_NS]
        for key in sorted(due, key=lambda k: (k[3], k[1], k[2])):
            self.emit(self.open.pop(key))
        return len(due)


# ---------------------------------------------------------------- analysis (pure; the A3 scorer imports these)

def read_aggregates(lines) -> tuple[list[dict], list[dict]]:
    """JSONL lines -> (rows merged per (c, i, s), events). Merge: n and seq_breaks summed, lag maxima max'ed,
    lag_e_min min'ed, recv_first min, recv_last max. Rows sorted by (s, c, i)."""
    merged: dict[tuple, dict] = {}
    events = []
    for line in lines:
        if not line.strip():
            continue
        item = json.loads(line)
        if item.get("k") == "ev":
            continue
        if item.get("k") != "agg":
            events.append(item)
            continue
        key = (item["c"], item["i"], item["s"])
        row = merged.get(key)
        if row is None:
            merged[key] = dict(item)
            continue
        row["n"] += item["n"]
        row["seq_breaks"] += item.get("seq_breaks") or 0
        for field, fn in (("lag_e_max_ms", max), ("lag_t_max_ms", max), ("lag_e_min_ms", min),
                          ("recv_first_ns", min), ("recv_last_ns", max)):
            values = [v for v in (row.get(field), item.get(field)) if v is not None]
            row[field] = fn(values) if values else None
    events.sort(key=lambda e: e.get("at_ns", 0))
    return sorted(merged.values(), key=lambda r: (r["s"], r["c"], r["i"])), events


def read_events(lines) -> dict[tuple, dict]:
    """Per-event lines -> {(channel, SYMBOL, id): {'r': first local receipt ns, 'E', 'T'}} (first receipt kept)."""
    out: dict[tuple, dict] = {}
    for line in lines:
        if not line.strip() or '"ev"' not in line:
            continue
        item = json.loads(line)
        if item.get("k") != "ev" or item.get("id") is None:
            continue
        key = (item["c"], item["i"], int(item["id"]))
        if key not in out or item["r"] < out[key]["r"]:
            out[key] = {"r": item["r"], "E": item.get("E"), "T": item.get("T")}
    return out


def _lag(row: dict) -> float | None:
    values = [v for v in (row.get("lag_e_max_ms"), row.get("lag_t_max_ms")) if v is not None]
    return max(values) if values else None


def analyze(lines, start_ns: int, end_ns: int) -> dict:
    """Per-window fresh-connection statistics (report-only). Rows count when their venue second intersects
    [start_ns, end_ns]. Per channel + symbol: seconds with a row, frames, lag (recv - E, recv - T) p50/p95/max over
    the per-second maxima, seq breaks, and for continuous channels the interior seconds without a row. covered: every
    subscribed channel/symbol has a row inside the window, every continuous channel/symbol has rows within
    EDGE_TOLERANCE_NS of both edges, and no disconnect / error before end_ns (the OKX books probe's analyze()
    semantics). trade is exempt from the edge rule because it may be quiet (live smoke 2026-10-18: DOGEUSDT traded in
    10 of 25 s); the liveness of its connection at both edges is carried by bookTicker / depth on the same /public
    connection."""
    rows, events = read_aggregates(lines)
    first_s, last_s = start_ns // S_NS, end_ns // S_NS
    problems = [f"{e.get('kind')} ({e.get('conn')})" for e in events
                if e.get("kind") in ("disconnect", "error") and e.get("at_ns", 0) < end_ns]
    inside: dict[tuple, list[dict]] = {}
    for r in rows:
        if first_s <= r["s"] <= last_s:
            inside.setdefault((r["c"], r["i"]), []).append(r)
    channels: dict[str, dict] = {}
    for conn, subscribed in SUBSCRIPTIONS.items():
        for channel, syms in subscribed.items():
            for sym in syms:
                items = inside.get((channel, sym), [])
                seconds = {r["s"] for r in items}
                interior = range(first_s + 1, last_s)
                channels.setdefault(channel, {})[sym] = {
                    "conn": conn, "seconds": len(seconds), "n": sum(r["n"] for r in items),
                    "lag_e_ms": stats([r["lag_e_max_ms"] for r in items if r.get("lag_e_max_ms") is not None]),
                    "lag_t_ms": stats([r["lag_t_max_ms"] for r in items if r.get("lag_t_max_ms") is not None]),
                    "seq_breaks": sum(r.get("seq_breaks") or 0 for r in items),
                    "interior_seconds_missing": (sum(1 for s in interior if s not in seconds)
                                                 if channel in CONTINUOUS else None)}
                if not items:
                    problems.append(f"no row inside the window for {channel} {sym}")
                elif channel in CONTINUOUS and not (items[0]["s"] * S_NS <= start_ns + EDGE_TOLERANCE_NS
                          and (items[-1]["s"] + 1) * S_NS >= end_ns - EDGE_TOLERANCE_NS):
                    problems.append(f"{channel} {sym}: rows do not reach both window edges")
    return {"probe_version": PROBE_VERSION, "report_only": True, "window": {"start_ns": start_ns, "end_ns": end_ns},
            "channels": channels, "events": {k: sum(1 for e in events if e.get("kind") == k)
                                             for k in ("subscribed", "disconnect", "error", "done")},
            "covered": not problems, "coverage_problems": problems}


def probe_state(rows, events, *, channel: str, symbol: str | None, from_ns: int, to_ns: int,
                healthy_max_ms: float, late_min_ms: float) -> dict:
    """The probe's view of the venue for one channel (and symbol; None = any symbol of the channel) over the venue
    seconds from_ns // 1 s .. to_ns // 1 s inclusive. rows / events as returned by read_aggregates().

    NO_COVERAGE when: the connection carrying the channel had not subscribed by the start of the first second; it
    disconnected (or the probe finished) before the end of the last second; it reported an error inside the range; a
    continuous channel (bookTicker / depth / markPrice) has a second of the range without a row; trade has no row in
    the range. Otherwise lag = max over the rows of max(lag_e_max_ms, lag_t_max_ms): HEALTHY when <= healthy_max_ms,
    LATE when >= late_min_ms, else INDETERMINATE (never external evidence for either class)."""
    conn = CONN_OF[channel]
    first_s, last_s = from_ns // S_NS, to_ns // S_NS
    range_start, range_end = first_s * S_NS, (last_s + 1) * S_NS
    picked = [r for r in rows if r["c"] == channel and (symbol is None or r["i"] == symbol)
              and first_s <= r["s"] <= last_s]
    seconds = {r["s"] for r in picked}
    lags = [x for x in (_lag(r) for r in picked) if x is not None]
    out = {"state": "NO_COVERAGE", "seconds": len(seconds), "lag_max_ms": round(max(lags), 3) if lags else None,
           "n": sum(r["n"] for r in picked), "seq_breaks": sum(r.get("seq_breaks") or 0 for r in picked),
           "reason": None}
    mine = [e for e in events if e.get("conn") in (None, conn)]
    subscribed = [e["at_ns"] for e in mine if e.get("kind") == "subscribed" and e.get("conn") == conn]
    if not subscribed or min(subscribed) > range_start:
        out["reason"] = "connection not subscribed before the range"
    elif any(e.get("kind") in ("disconnect", "done") and e.get("at_ns", 0) < range_end for e in mine):
        out["reason"] = "connection ended before the end of the range"
    elif any(e.get("kind") == "error" and range_start <= e.get("at_ns", 0) < range_end for e in mine):
        out["reason"] = "error inside the range"
    elif channel in CONTINUOUS and len(seconds) < last_s - first_s + 1:
        out["reason"] = f"{last_s - first_s + 1 - len(seconds)} second(s) without a row"
    elif not picked or not lags:
        out["reason"] = "no row in the range"
    if out["reason"]:
        return out
    lag = max(lags)
    out["state"] = "HEALTHY" if lag <= healthy_max_ms else "LATE" if lag >= late_min_ms else "INDETERMINATE"
    return out


def window_report(probe_dir, start_ns: int, end_ns: int) -> dict | None:
    """Scorer helper: analyze() of frames.jsonl + analyze_tcp() of tcp.jsonl over [start_ns, end_ns], the probe exit,
    ingestor pid stability, the host clock bound and the file digests. None when the dir has no frames.jsonl."""
    d = Path(probe_dir)
    frames, tcp, receipt = d / "frames.jsonl", d / "tcp.jsonl", d / "receipt.json"
    if not frames.exists():
        return None
    tcp_lines = tcp.read_text().splitlines() if tcp.exists() else []
    out = {"report_only": True, "probe_version": PROBE_VERSION,
           "frames": analyze(frames.read_text().splitlines(), int(start_ns), int(end_ns)),
           "tcp": analyze_tcp(tcp_lines, int(start_ns), int(end_ns)),
           "probe_exit": None, "ingestor_pid_stable": None, "host_clock_bound_ms_max": None,
           "frames_sha256": hashlib.sha256(frames.read_bytes()).hexdigest(),
           "tcp_sha256": hashlib.sha256(tcp.read_bytes()).hexdigest() if tcp.exists() else None,
           "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest() if receipt.exists() else None}
    if receipt.exists():
        r = json.loads(receipt.read_text())
        out.update(probe_exit=r.get("exit"), ingestor_pid_stable=r.get("ingestor_pid_stable"),
                   host_clock_bound_ms_max=r.get("host_clock_bound_ms_max"))
    return out


# ---------------------------------------------------------------- worker (inside the container)

def worker(seconds: int) -> None:
    import asyncio
    import websockets

    def out(d):
        sys.stdout.write(json.dumps(d, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    aggregator = Aggregator(out)
    deadline = time.monotonic() + seconds

    async def connection(conn: str, request_id: int):
        try:
            async with websockets.connect(URLS[conn], ping_interval=None, max_queue=None, compression=None,
                                          open_timeout=10, close_timeout=2, max_size=8 * 1024 * 1024) as ws:
                params = streams(conn)
                await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": request_id}))
                out({"kind": "subscribed", "conn": conn, "at_ns": time.time_ns(), "streams": len(params)})
                while time.monotonic() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=max(0.1, min(5, deadline - time.monotonic())))
                    except asyncio.TimeoutError:
                        continue
                    now = time.time_ns()
                    raw = msg.encode() if isinstance(msg, str) else msg
                    frame = parse_frame(raw)
                    if frame is not None:
                        aggregator.add(conn, frame, now)
                        # Per event: ids and clocks only (A3 same-event join), never a price or size.
                        out({"k": "ev", "c": frame["c"], "i": frame["i"], "id": frame.get("id"), "E": frame["E"],
                             "T": frame.get("T"), "r": now})
                        continue
                    code = error_code(raw)
                    if code is not None:
                        out({"kind": "error", "conn": conn, "at_ns": now, "code": code})
            out({"kind": "conn_end", "conn": conn, "at_ns": time.time_ns()})
        except Exception as exc:  # a disconnect ends that connection; its channels become NO_COVERAGE, never EXTERNAL
            out({"kind": "disconnect", "conn": conn, "at_ns": time.time_ns(), "error_type": type(exc).__name__})

    async def ticker():
        while time.monotonic() < deadline:
            await asyncio.sleep(max(0.05, min(1.0, deadline - time.monotonic())))
            aggregator.flush(time.time_ns())

    async def run():
        await asyncio.gather(connection("public", 1), connection("market", 2), ticker())
        aggregator.flush(time.time_ns(), everything=True)
        out({"kind": "done", "at_ns": time.time_ns()})

    asyncio.run(run())


# ---------------------------------------------------------------- launcher (host)

def run_plan(query_inspect: dict, name: str, seconds: int) -> tuple[list[str], dict]:
    """Pure: docker run argv (without the code) and provenance from the query_kn_1 inspect document."""
    image = query_inspect["Image"]
    user = (query_inspect.get("Config") or {}).get("User") or None
    argv = ["docker", "run", "--rm", "-i", "--init", "--name", name, "--label", ROLE_LABEL,
            "--cpus", f"{CPUS:g}", "--memory", MEMORY, "--memory-swap", MEMORY, "--pids-limit", str(PIDS_LIMIT),
            "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=8m", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--network", "bridge"]
    if user:
        argv += ["--user", user]
    argv += ["--entrypoint", "python", image]
    provenance = {"probe_version": PROBE_VERSION, "launch_mode": "docker_run_isolated", "container_name": name,
                  "image_id": image, "image_source_container": QUERY, "network": "bridge", "cpus": CPUS,
                  "memory": MEMORY, "pids_limit": PIDS_LIMIT, "seconds": seconds, "urls": dict(URLS),
                  "streams": {conn: streams(conn) for conn in SUBSCRIPTIONS}, "flush_after_s": FLUSH_AFTER_S}
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
    clocks = [clock_sample("before")]
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
            with (args.output / "frames.jsonl").open("x") as evidence:
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
            clocks.append(clock_sample("after"))
            signal.signal(signal.SIGTERM, previous)
            try:
                after = ingestor_identity(args.ingestor)
            except (subprocess.SubprocessError, OSError, ValueError, KeyError, IndexError):
                after = None
            bounds = [c["clock"]["bound_ms"] for c in clocks if c.get("clock")]
            (args.output / "receipt.json").write_text(json.dumps({
                "exit": code, **provenance, "output_bytes": written, "container_absent_after_exit": removed,
                "ingestor_before": before, "ingestor_after": after,
                "ingestor_pid_stable": bool(after and after["pid"] == before["pid"]
                                            and after["started"] == before["started"]),
                "tcp_sampler": {"basis": "/proc/<ingestor pid>/net (host side, read-only)",
                                "implementation": "scripts/p184_g4_okx_books_probe.py TcpSampler",
                                "rx_persist_samples": RX_PERSIST_SAMPLES},
                "host_clock": clocks, "host_clock_bound_ms_max": max(bounds) if bounds else None,
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
        print(json.dumps(window_report(args.analyze, args.start_ns, args.end_ns), indent=1))
        return 0
    if not args.output or not args.name or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", args.name):
        parser.error("--launch needs --output and a docker-valid --name")
    return launch(args)


if __name__ == "__main__":
    sys.exit(main())

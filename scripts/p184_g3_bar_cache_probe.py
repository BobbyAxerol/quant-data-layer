#!/usr/bin/env python3
"""P18.4 gate G3: BAR close -> first visibility of the closed bar in the TS execution cache, per demanded BAR slice.

Purpose: the writer delivery trace carries no BAR records (v2.2.3 scorecard G3 NOT_MEASURED), so this read-only poller
reads the TS market cache keys the writer projects BAR into (TS services/market_data/cache_projector.py _bar_operations:
`cache:market:v2:last_bar:<VENUE>:<PRODUCT>:<venue_symbol>:<interval>`, Redis TRADING_MARKET_REDIS_URL, default
redis_marketdata DB 1) with one MGET per tick, and records for each slice the first tick at which a closed bar
(is_closed true) with a new open time is visible.

Quantity: transit_ms = first_seen_wall_ms - close_ms, close_ms = timestamp_ms (bar open) + interval. The poll period is
an upper-bound bias (a bar is seen at most one period + one RTT late); it is recorded in the summary. Bars already in
the cache at the first tick are not measured. Every close boundary C with start + 5 s < C <= end - 60 s is
expected per slice; an expected close never seen is counted with its lower bound (end - C, which is >= 60 s) and FAILs
the max rule, never dropped.

Gate (TS upgrade/p184-phase-d-gates.md, frozen): p95 <= 5000 ms, max < 60000 ms.

Boundary: read-only (allowlisted MGET/SCAN through scripts/p184_resp_readonly.py), one MGET per tick of <= 64 keys of
~1 KB, bounded by --seconds and --max-bytes. No writes anywhere except its own exclusive output files.
usage: p184_g3_bar_cache_probe.py --binding FILE --seconds N --out DIR [--poll-ms 250]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_resp_readonly import (  # noqa: E402
    install_stop_signals, pause, stopped,
    BoundedJsonl, ReadOnlyRedis, container_ip, quantiles, write_json_exclusive,
)

GATE = {"p95_le_ms": 5000.0, "max_lt_ms": 60000.0}
UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
EXPECT_GRACE_MS = 60_000


def interval_ms(interval: str) -> int:
    return int(interval[:-1]) * UNIT_MS[interval[-1]]


def bar_slices(binding: dict) -> list[dict]:
    rows = []
    for product in binding.get("products", []):
        if product.get("feed") == "BAR" and product.get("interval"):
            rows.append({"venue": product["venue"], "native_symbol": product["native_symbol"],
                         "interval": product["interval"]})
    return sorted(rows, key=lambda r: (r["venue"], r["native_symbol"], r["interval"]))


def resolve_keys(slices, keys) -> dict[str, str | None]:
    """Map 'VENUE|native|interval' -> the last_bar key (VENUE:<product>:<native>:<interval>), or None."""
    out = {}
    for item in slices:
        name = f"{item['venue']}|{item['native_symbol']}|{item['interval']}"
        match = [k for k in keys if k.startswith(f"cache:market:v2:last_bar:{item['venue']}:")
                 and k.endswith(f":{item['native_symbol']}:{item['interval']}")]
        out[name] = match[0] if len(match) == 1 else None
    return out


class BarTracker:
    def __init__(self, slices: list[str], intervals: dict[str, int]):
        self.intervals = intervals
        self.last_open = {name: None for name in slices}
        self.primed = set()
        self.seen = {name: {} for name in slices}  # open_ms -> record
        self.unparsed = 0

    def observe(self, name: str, raw, now_ms: int):
        """Feed one GET value; returns a record when a new closed bar becomes visible, else None."""
        if raw is None:
            return None
        try:
            value = json.loads(raw)
            open_ms, closed = int(value["timestamp_ms"]), value.get("is_closed") is True
        except (ValueError, KeyError, TypeError):
            self.unparsed += 1
            return None
        first_tick = name not in self.primed
        self.primed.add(name)
        if not closed:
            return None
        if first_tick:
            self.last_open[name] = open_ms  # already in the cache at start: not measured
            return None
        if open_ms in self.seen[name] or (self.last_open[name] is not None and open_ms <= self.last_open[name]):
            return None
        self.last_open[name] = open_ms
        close_ms = open_ms + self.intervals[name]
        record = {"slice": name, "open_ms": open_ms, "close_ms": close_ms, "seen_ms": now_ms,
                  "transit_ms": now_ms - close_ms}
        self.seen[name][open_ms] = record
        return record

    def finish(self, start_ms: int, end_ms: int) -> dict:
        per_slice, all_values, unseen = {}, {}, []
        for name, records in self.seen.items():
            step = self.intervals[name]
            first = (start_ms // step + 1) * step
            expected = [c for c in range(first, end_ms - EXPECT_GRACE_MS + 1, step) if c > start_ms + 5000]
            seen_closes = {r["close_ms"] for r in records.values()}
            missing = [c for c in expected if c not in seen_closes]
            values = [r["transit_ms"] for r in records.values()]
            for close in missing:
                unseen.append({"slice": name, "close_ms": close, "lower_bound_ms": end_ms - close})
            bound = values + [end_ms - c for c in missing]
            per_slice[name] = {**quantiles(values), "expected_closes": len(expected), "unseen": len(missing)}
            venue = name.split("|", 1)[0]
            all_values.setdefault(venue, []).extend(bound)
        venues = {venue: quantiles(values) for venue, values in sorted(all_values.items())}
        pooled = [v for values in all_values.values() for v in values]
        return {"per_slice": per_slice, "per_venue": venues, "pooled": quantiles(pooled), "unseen": unseen[:200],
                "unseen_total": len(unseen), "unparsed": self.unparsed}


def verdict(summary: dict, *, unresolved: int) -> str:
    pooled = summary["pooled"]
    if not pooled.get("n"):
        return "NOT_MEASURED"
    for block in summary["per_venue"].values():
        if block.get("n") and (block["p95"] > GATE["p95_le_ms"] or block["max"] >= GATE["max_lt_ms"]):
            return "FAIL"
    if unresolved or any(not s.get("n") and not s["unseen"] for s in summary["per_slice"].values()):
        return "NOT_MEASURED"
    return "PASS"


def run(args) -> dict:
    binding = json.loads(Path(args.binding).read_text())
    slices = bar_slices(binding)
    host = args.host or container_ip(args.container, args.network)
    redis = ReadOnlyRedis(host, args.port, args.db)
    keys = resolve_keys(slices, redis.scan("cache:market:v2:last_bar:*"))
    names = [name for name, key in keys.items() if key]
    intervals = {f"{s['venue']}|{s['native_symbol']}|{s['interval']}": interval_ms(s["interval"]) for s in slices}
    tracker = BarTracker(names, {n: intervals[n] for n in names})
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    evidence = BoundedJsonl(out / "g3-bar-observations.jsonl", args.max_bytes)
    errors = 0
    start_ms = time.time_ns() // 1_000_000
    deadline = time.monotonic() + args.seconds
    period = args.poll_ms / 1000.0
    try:
        while time.monotonic() < deadline and not stopped():
            tick = time.monotonic()
            try:
                values = redis.call("MGET", *[keys[n] for n in names]) if names else []
            except Exception as error:  # a failed read is counted; it never fabricates a visibility time
                errors += 1
                evidence.write({"kind": "read_error", "at_ms": time.time_ns() // 1_000_000,
                                "error": type(error).__name__})
                values = None
            now_ms = time.time_ns() // 1_000_000
            for name, raw in zip(names, values or []):
                record = tracker.observe(name, raw, now_ms)
                if record:
                    evidence.write({"kind": "bar_visible", **record})
            pause(max(0.0, period - (time.monotonic() - tick)))
    finally:
        redis.close()
        evidence.close()
    end_ms = time.time_ns() // 1_000_000
    summary = tracker.finish(start_ms, end_ms)
    unresolved = sorted(name for name, key in keys.items() if key is None)
    report = {"schema": "qdl.p184.g3-bar-cache-probe.v1", "gate": GATE, "window_ms": [start_ms, end_ms],
              "poll_ms": args.poll_ms, "measuring_point": "host MGET reply wall time; upper bound by one poll period",
              "redis": {"container": args.container, "host": host, "db": args.db},
              "demanded_bar_slices": len(slices), "unresolved_slices": unresolved, "read_errors": errors,
              "evidence_capped": evidence.capped, **summary}
    report["verdict"] = verdict(summary, unresolved=len(unresolved))
    write_json_exclusive(out / "g3-summary.json", report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--binding", required=True, help="TS consumer binding JSON (products[] with feed/interval)")
    parser.add_argument("--container", default="redis_marketdata")
    parser.add_argument("--network", default="executor_network")
    parser.add_argument("--host", help="override the resolved container IP")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=1)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--poll-ms", type=int, default=250)
    parser.add_argument("--max-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    install_stop_signals()
    if not 50 <= args.poll_ms <= 5000 or not 0 < args.seconds <= 4 * 3600:
        parser.error("poll-ms must be 50..5000 and seconds 0..14400")
    report = run(args)
    print(json.dumps({k: report[k] for k in ("verdict", "pooled", "unseen_total", "read_errors")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

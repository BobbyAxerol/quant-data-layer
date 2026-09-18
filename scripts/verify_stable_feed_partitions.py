#!/usr/bin/env python3
"""Require every partition of every realtime feed to be fresh, not just the newest.

Read-only. It changes nothing and takes no confirmation flag.

Why it exists. On 2026-09-18 a rollout was accepted against "Binance book newest
age 0 s" while one of that feed's nine partitions had been dead for four minutes:
the eight healthy books answered for the ninth. Two days earlier a bare
`filtered` counter had hidden a whole feed the same way. Each time the measure
was one level coarser than the failure, so this check inverts it - it enumerates
partitions and fails on the worst one, never the best.

A partition is stale when its newest durable record is older than the bound for
its feed. Bounds come from the feed's own cadence, not from a consumer contract:
a BAR partition is allowed its interval, a book or tick partition is not allowed
to be quiet for long. A partition that carries no rows at all is reported, never
skipped, because an empty partition is the failure this check exists to catch.

    python3 -B scripts/verify_stable_feed_partitions.py
    python3 -B scripts/verify_stable_feed_partitions.py --json
    python3 -B scripts/verify_stable_feed_partitions.py --max-age book=30
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

QUERY_ROLE = "qdl_v2_stable_candidate-query_v2_1-1"
SPOOL = "/var/lib/qdl-stable/shared/canonical-cache.sqlite3"

# Seconds a partition of each feed class may go without a durable record.
# `bar` is handled separately because its cadence is the interval it carries.
DEFAULT_MAX_AGE = {
    "book": 30,
    "quote": 30,
    "trade": 120,
    "mark_index_price": 30,
}
# A BAR partition is allowed its own interval plus the settlement the bar edge
# holds. Anything without a recognised interval falls back to the largest.
BAR_INTERVAL_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1_800,
    "1h": 3_600, "2h": 7_200, "4h": 14_400, "6h": 21_600, "8h": 28_800,
    "12h": 43_200, "1d": 86_400, "2d": 172_800, "3d": 259_200, "1w": 604_800,
}
BAR_SETTLEMENT_GRACE_SECONDS = 60

PROBE = """
import json, re, sqlite3, time
connection = sqlite3.connect("file:%s?mode=ro", uri=True)
now = time.time() * 1e9
seek = ("select committed_at_ns from events where stream = ? and partition_key = ? "
        "order by logical_offset desc limit 1")
rows = []
for stream, key in connection.execute("select stream, partition_key from partitions"):
    record = connection.execute(seek, (stream, key)).fetchone()
    rows.append({
        "partition_key": key,
        "age_seconds": None if record is None else (now - record[0]) / 1e9,
    })
print(json.dumps(rows))
""" % SPOOL


def read_partitions(role: str) -> list[dict]:
    result = subprocess.run(
        ["docker", "exec", role, "python", "-c", PROBE],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise SystemExit(f"spool probe failed: {result.stderr.strip()[:300]}")
    return json.loads(result.stdout)


def classify(partition_key: str) -> tuple[str, str, str | None]:
    """venue, feed class and BAR interval, read from the partition key itself."""
    parts = partition_key.split("/")
    feed = parts[1] if len(parts) > 1 else "?"
    venue = "okx" if "okx" in partition_key else ("binance" if "binance" in partition_key else "other")
    interval = None
    if feed == "bar":
        for candidate in sorted(BAR_INTERVAL_SECONDS, key=len, reverse=True):
            if f"-{candidate}-" in partition_key:
                interval = candidate
                break
    return venue, feed, interval


def bound_for(feed: str, interval: str | None, overrides: dict[str, int]) -> int:
    if feed in overrides:
        return overrides[feed]
    if feed == "bar":
        return BAR_INTERVAL_SECONDS.get(interval or "", max(BAR_INTERVAL_SECONDS.values())) \
            + BAR_SETTLEMENT_GRACE_SECONDS
    return DEFAULT_MAX_AGE.get(feed, 120)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", default=QUERY_ROLE)
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--max-age", action="append", default=[],
        help="override one feed's bound, e.g. book=30; repeatable",
    )
    args = parser.parse_args(argv)

    overrides: dict[str, int] = {}
    for item in args.max_age:
        feed, _, value = item.partition("=")
        overrides[feed.strip()] = int(value)

    stale: list[dict] = []
    empty: list[dict] = []
    summary: dict[tuple[str, str], dict] = {}
    for row in read_partitions(args.role):
        venue, feed, interval = classify(row["partition_key"])
        bound = bound_for(feed, interval, overrides)
        entry = summary.setdefault((venue, feed), {"total": 0, "stale": 0, "empty": 0, "worst": 0.0})
        entry["total"] += 1
        record = {
            "partition_key": row["partition_key"], "venue": venue, "feed": feed,
            "interval": interval, "age_seconds": row["age_seconds"], "max_age_seconds": bound,
        }
        if row["age_seconds"] is None:
            entry["empty"] += 1
            empty.append(record)
            continue
        entry["worst"] = max(entry["worst"], row["age_seconds"])
        if row["age_seconds"] > bound:
            entry["stale"] += 1
            stale.append(record)

    if args.json:
        print(json.dumps({"stale": stale, "empty": empty}, indent=2, sort_keys=True))
    else:
        print(f"{'venue':8} {'feed':18} {'n':>3} {'stale':>5} {'empty':>5} {'worst_age_s':>12}")
        for key in sorted(summary):
            item = summary[key]
            flag = "  <== FAIL" if item["stale"] or item["empty"] else ""
            print(f"{key[0]:8} {key[1]:18} {item['total']:3} {item['stale']:5} "
                  f"{item['empty']:5} {item['worst']:12.0f}{flag}")
        for record in stale + empty:
            age = "no rows" if record["age_seconds"] is None else f"{record['age_seconds']:.0f}s"
            print(f"  STALE {age:>10} > {record['max_age_seconds']}s  "
                  f"{record['partition_key'].split('/')[-1]}")

    total_bad = len(stale) + len(empty)
    print(f"\n{sum(v['total'] for v in summary.values())} partitions, "
          f"{len(stale)} stale, {len(empty)} empty")
    return 1 if total_bad else 0


if __name__ == "__main__":
    raise SystemExit(main())

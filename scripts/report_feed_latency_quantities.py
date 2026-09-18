#!/usr/bin/env python3
"""Report the four latency quantities for every realtime feed partition.

Read-only. It changes nothing and takes no confirmation flag.

Why it exists. A single "latency" number for an endpoint has been wrong here
more than once, because four different quantities were being compared as if
they were one. This prints all four, per partition, always:

  1 venue -> received      the wire: the venue's own event timestamp to the
                           moment our ingestor read the frame
  2 received -> published   normalise and hand to Kafka
  3 published -> durable    the spool commit, which is the projector's batch
  4 venue -> durable        the sum, and the first moment a consumer can read it

The fifth quantity an alpha cares about - end to end into the alpha cache - is
not here, because it is not the data layer's to measure: it is the consumer's
Redis cache and is read from `redis_marketdata` db 1.

Quantity 4 is a median over a recent window, not over the whole partition: a
partition that has just been restarted carries a drained backlog whose commit
times say nothing about steady state.

    python3 -B scripts/report_feed_latency_quantities.py
    python3 -B scripts/report_feed_latency_quantities.py --venue binance
    python3 -B scripts/report_feed_latency_quantities.py --window 900 --json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

QUERY_ROLE = "qdl_v2_stable_candidate-query_v2_1-1"
SPOOL = "/var/lib/qdl-stable/shared/canonical-cache.sqlite3"

PROBE = r'''
import json, sqlite3, sys, time
sys.path.insert(0, "/app/generated/python")
from qdl.marketdata.v2 import market_data_pb2 as md

envelope = md.EventEnvelope
connection = sqlite3.connect("file:%(spool)s?mode=ro", uri=True)
now = time.time() * 1e9
cutoff = now - %(window)d * 1e9
seek = ("select committed_at_ns, payload from events "
        "where stream = ? and partition_key = ? order by logical_offset desc limit %(limit)d")

rows = []
for stream, key in connection.execute("select stream, partition_key from partitions"):
    feed = key.split("/")[1] if "/" in key else "?"
    if feed == "bar":
        continue
    records = connection.execute(seek, (stream, key)).fetchall()
    if not records:
        rows.append({"partition_key": key, "feed": feed, "symbol": None, "n": 0})
        continue
    wire, hand, commit, total, period = [], [], [], [], []
    previous_event = None
    symbol = envelope.FromString(records[0][1]).native_symbol
    for committed_at_ns, payload in sorted(records, key=lambda r: r[0]):
        if committed_at_ns < cutoff:
            continue
        message = envelope.FromString(payload)
        event_ns = message.source_event_time_ns
        if not event_ns:
            continue
        wire.append((message.received_at_ns - event_ns) / 1e6)
        hand.append((message.published_at_ns - message.received_at_ns) / 1e6)
        commit.append((committed_at_ns - message.published_at_ns) / 1e6)
        total.append((committed_at_ns - event_ns) / 1e6)
        if previous_event is not None:
            period.append((event_ns - previous_event) / 1e6)
        previous_event = event_ns
    rows.append({
        "partition_key": key, "feed": feed, "symbol": symbol, "n": len(total),
        "newest_age_ms": (now - records[0][0]) / 1e6,
        "venue_to_received_ms": wire, "received_to_published_ms": hand,
        "published_to_durable_ms": commit, "venue_to_durable_ms": total,
        "event_period_ms": period,
    })
print(json.dumps(rows))
'''


def median(values: list[float]) -> float:
    return sorted(values)[len(values) // 2] if values else float("nan")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return float("nan")
    return sorted(values)[min(len(values) - 1, int(len(values) * fraction))]


def read_rows(role: str, window: int, limit: int) -> list[dict]:
    script = PROBE % {"spool": SPOOL, "window": window, "limit": limit}
    result = subprocess.run(
        ["docker", "exec", "-i", role, "python", "-"],
        input=script, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise SystemExit(f"spool probe failed: {result.stderr.strip()[:400]}")
    return json.loads(result.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", default=QUERY_ROLE)
    parser.add_argument("--venue", help="only partitions whose key contains this")
    parser.add_argument("--window", type=int, default=300, help="seconds of history to judge")
    parser.add_argument("--limit", type=int, default=200, help="rows read per partition")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    rows = read_rows(args.role, args.window, args.limit)
    if args.venue:
        rows = [r for r in rows if args.venue.lower() in r["partition_key"].lower()]

    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0

    print(f"{'feed':18} {'symbol':16} {'1 wire':>9} {'2 hand':>8} {'3 commit':>10} "
          f"{'4 total':>9} {'p95':>8} {'period':>8} {'age':>9}   n")
    for row in sorted(rows, key=lambda r: (r["feed"], r.get("symbol") or "")):
        if not row["n"]:
            print(f"{row['feed']:18} {'(no events in the window)':16}")
            continue
        print(
            f"{row['feed']:18} {row['symbol'] or '?':16} "
            f"{median(row['venue_to_received_ms']):7.0f}ms "
            f"{median(row['received_to_published_ms']):6.0f}ms "
            f"{median(row['published_to_durable_ms']):8.0f}ms "
            f"{median(row['venue_to_durable_ms']):7.0f}ms "
            f"{percentile(row['venue_to_durable_ms'], 0.95):6.0f}ms "
            f"{median(row['event_period_ms']):6.0f}ms "
            f"{row['newest_age_ms']:7.0f}ms  {row['n']:4}"
        )
    quiet = [r["partition_key"] for r in rows if not r["n"]]
    print(f"\n{len(rows)} partitions, {len(quiet)} with no event in the last {args.window}s")
    return 1 if quiet else 0


if __name__ == "__main__":
    raise SystemExit(main())

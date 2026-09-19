#!/usr/bin/env python3
"""Which source bindings are actually live, judged by each binding's own budget.

Read-only. It opens the durable spool immutably and compares, per binding, the
age of the newest stored event against that binding's `quality.stale_after_ms`
from the runtime catalog.

Why the budget comes from the catalog. A fixed threshold answers the wrong
question: a 1d bar that is five hours old is healthy - the bar has not closed
and the next one is not due - while a 1m bar five minutes old is a gap. Earlier
reports in this program used one 180 s threshold for every binding and called
long-interval bars "stale", which is a unit error, not a finding. The catalog
already sets the budget per binding (1m -> 180 s, 15m -> 45 min, 1d -> 3 d,
1w -> 21 d, MARK_INDEX_PRICE -> 2 s), so that is what this compares against.

Why the age is read from the envelope and not from the spool row. The spool's
`committed_at_ns` is when the durable append landed, which trails the event and
is not what any gate compares. `qdl/runtime/stable_source.py:483-492` measures a
bar from `bar.close_time_ns`, a PROVIDER_CONFIRMATION binding - every
MARK_INDEX_PRICE binding - from `received_at_ns`, and everything else from
`source_event_time_ns`. Judging mark/index by the venue's own `ts` is precisely
the mistake that basis exists to avoid: OKX `index-tickers` returns a `ts`
roughly a second older than the row it is attached to, so a source-time rule
reports ten healthy bindings as stale. This mirrors the runtime rule instead.

Run it in a throwaway container with the state volume mounted read-only, not
inside a data layer role: a full scan of `events` inside a role with a 512 MiB
limit is what restarted query_v2_1 during an earlier probe.

    docker run --rm \
      -v qdl_v2_stable_candidate_stable_state:/st:ro \
      -v /home/bobby/data_layer:/src:ro \
      -v <runtime-bundle>:/runtime:ro \
      -e PYTHONPATH=/src --entrypoint python qdl-v2-python:<tag> \
      -B /src/scripts/report_binding_liveness.py
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.marketdata.v2 import market_data_pb2                     # noqa: E402
from qdl.runtime.stable_catalog import StableSourceCatalog        # noqa: E402


def newest_by_partition(db: Path) -> dict[tuple[str, str], bytes]:
    """The newest stored payload per (stream, partition_key).

    Newest is taken by `logical_offset`, the spool's own append order, which is
    what `read_tail` returns; `committed_at_ns` is carried only so the caller
    can report append lag separately from event age.
    """
    # `mode=ro`, never `immutable=1`: the spool is being written while this
    # reads it, and `immutable` tells SQLite the file cannot change, which
    # skips locking and silently returns stale or torn pages once the WAL moves.
    connection = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT e.stream, e.partition_key, e.payload, e.committed_at_ns "
            "FROM events AS e JOIN ("
            "  SELECT stream, partition_key, MAX(logical_offset) AS top"
            "  FROM events GROUP BY stream, partition_key"
            ") AS t ON e.stream = t.stream AND e.partition_key = t.partition_key"
            "      AND e.logical_offset = t.top"
        ).fetchall()
    finally:
        connection.close()
    return {(stream, partition): (payload, committed)
            for stream, partition, payload, committed in rows}


def freshness_observed_ns(binding, envelope) -> int:
    """The timestamp the runtime measures this binding's freshness from.

    Mirrors `qdl/runtime/stable_source.py:483-492`. Any divergence here turns
    this report into a second opinion rather than a check.
    """
    if binding.freshness_basis == "PROVIDER_CONFIRMATION":
        return envelope.received_at_ns
    if envelope.WhichOneof("payload") == "bar":
        return envelope.bar.close_time_ns
    return envelope.source_event_time_ns


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", default="/st/shared/canonical-cache.sqlite3")
    parser.add_argument("--catalog", default="/runtime/stable-source-bindings.yaml")
    parser.add_argument("--only-stale", action="store_true",
                        help="print only bindings over their own budget")
    args = parser.parse_args()

    catalog = StableSourceCatalog.load(args.catalog)
    newest = newest_by_partition(Path(args.spool))
    now_ns = time.time_ns()

    live: list[tuple] = []
    stale: list[tuple] = []
    empty: list[str] = []
    for binding in catalog.bindings:
        key = (binding.canonical_stream, binding.partition_key)
        stored = newest.get(key)
        if stored is None:
            empty.append(binding.binding_id)
            continue
        payload, committed = stored
        envelope = market_data_pb2.EventEnvelope()
        envelope.ParseFromString(payload)
        age_ms = (now_ns - freshness_observed_ns(binding, envelope)) / 1e6
        budget_ms = float(binding.stale_after_ms)
        row = (binding.binding_id, binding.feed.value, binding.interval or "-",
               age_ms, budget_ms, age_ms / budget_ms if budget_ms else float("inf"),
               (now_ns - committed) / 1e6)
        (stale if age_ms > budget_ms else live).append(row)

    print(f"catalog bindings : {len(catalog.bindings)}")
    print(f"live             : {len(live)}")
    print(f"over budget      : {len(stale)}")
    print(f"no event stored  : {len(empty)}")
    if not args.only_stale and live:
        worst = sorted(live, key=lambda r: -r[5])[:10]
        print("\nclosest to their budget (live):")
        for bid, feed, interval, age, budget, ratio, append in worst:
            print(f"  {bid:52s} {feed:16s} {interval:4s} "
                  f"age={age/1000:9.1f}s budget={budget/1000:9.1f}s  {ratio*100:5.1f}%"
                  f"  append_lag={append/1000:6.1f}s")
    if stale:
        print("\nover their own budget:")
        for bid, feed, interval, age, budget, ratio, append in sorted(
                stale, key=lambda r: -r[5]):
            print(f"  {bid:52s} {feed:16s} {interval:4s} "
                  f"age={age/1000:9.1f}s budget={budget/1000:9.1f}s  {ratio*100:5.1f}%"
                  f"  append_lag={append/1000:6.1f}s")
    if empty:
        print(f"\nno event stored ({len(empty)}):")
        for bid in sorted(empty):
            print(f"  {bid}")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())

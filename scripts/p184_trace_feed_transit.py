"""P18.4 phase D gates G1, G2 (TRADE/QUOTE) and G7 from the TS writer delivery trace files.

Purpose: summarise, per feed and venue, the end-to-end time from the venue event to the TS execution Redis APPLY reply
(G1 BOOK_DELTA/BOOK_SNAPSHOT, G2 TRADE/QUOTE new-event transit) and the two TS consumer-path segments of G7, read from
the files that TS services/market_data/delivery_trace.py exports (delivery-<worker>-<pid>-<until>[-pNN].json). The MARK
row of G2 stays with scripts/p184_g2_mark_trace_transit.py (component clocks, newest-component rule); BAR (G3) is not
traced by the writer, so this script reports it as absent instead of deriving it from another clock.

Gates (TS upgrade/p184-phase-d-gates.md, frozen 2026-10-09):
* G1 BOOK_DELTA, BOOK_SNAPSHOT: p95 <= 500 ms, p99 <= 1000 ms (only when n >= 100), max < 2000 ms;
* G2 TRADE: p95 <= 500 ms, max < 1500 ms; QUOTE: p95 <= 500 ms, max < 1000 ms (new-event transit only);
* G7 per feed: SDK return -> writer entry p95 <= 100 ms; writer entry -> Redis APPLY reply p95 <= 50 ms.

Definitions:
* venue clock = source_event_time_ns of the SDK stream envelope; only SDK_STREAM_RETURN records carry it. Records of
  other bases (warmup, query, no basis) have no venue clock and are counted, never given a substitute clock;
* new-event transit = redis_reply_wall_ns - source_event_time_ns for the first APPLIED record of each event identity
  (event_id_sha256; when absent, instrument + feed + source_event_time_ns). REFRESHED and DUPLICATE outcomes and a
  repeated identity are the same state re-published (IDR-082 quiet republish) and are counted, not gated;
* transit split (attribution only, not gated), for gated records whose clocks are all present: venue_to_ingest =
  envelope_received_at_ns - source_event_time_ns (ingestor receive; the envelope's received/normalized/published
  stamps are one ingest clock, not a Kafka commit), ingest_to_sdk = sdk_receive_wall_ns - envelope_received_at_ns
  (Data Layer pipeline + Stream + SDK), sdk_to_reply = redis_reply_wall_ns - sdk_receive_wall_ns (TS writer);
* G7 segments use the process monotonic clocks of the same worker: sdk_to_writer = writer_started_monotonic_ns -
  sdk_receive_monotonic_ns; writer_to_redis = redis_reply_monotonic_ns - writer_started_monotonic_ns, over every record
  with a Redis reply (any outcome). TRADE/QUOTE rows of one atomic APPLY batch share the batch start as writer entry.
Negative durations are counted as invalid, never clamped. Quantiles are nearest-rank as in the G2 MARK script.

Boundary: read-only over the given files; no Redis, Docker or network access; it never enables tracing. Output is JSON
on stdout, or --out (created exclusively).
usage: p184_trace_feed_transit.py FILE_OR_GLOB [...] [--skip-seconds 40] [--from-ns N] [--to-ns N] [--out PATH]
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_g2_mark_trace_transit import as_int, load, quantiles, venue_of  # noqa: E402

TRANSIT_GATES = {
    "BOOK_DELTA": {"gate": "G1", "p95_le_ms": 500.0, "p99_le_ms": 1000.0, "max_lt_ms": 2000.0},
    "BOOK_SNAPSHOT": {"gate": "G1", "p95_le_ms": 500.0, "p99_le_ms": 1000.0, "max_lt_ms": 2000.0},
    "TRADE": {"gate": "G2", "p95_le_ms": 500.0, "max_lt_ms": 1500.0},
    "QUOTE": {"gate": "G2", "p95_le_ms": 500.0, "max_lt_ms": 1000.0},
}
G7_GATES = {"sdk_to_writer_ms": 100.0, "writer_to_redis_ms": 50.0}
STREAM_BASIS = "SDK_STREAM_RETURN"


def transit_verdict(block, gate):
    if not block.get("n"):
        return "NO_DATA"
    inside = block["p95"] <= gate["p95_le_ms"] and block["max"] < gate["max_lt_ms"]
    if "p99_le_ms" in gate and block.get("p99") is not None:
        inside = inside and block["p99"] <= gate["p99_le_ms"]
    return "INSIDE" if inside else "OUTSIDE"


def summarise(records, *, skip_seconds=40.0, from_ns=None, to_ns=None):
    starts = [r["writer_started_wall_ns"] for r in records if as_int(r.get("writer_started_wall_ns"))]
    if not starts:
        return {"error": "no records with writer_started_wall_ns"}
    lower = from_ns if from_ns is not None else min(starts) + int(skip_seconds * 1e9)
    upper = to_ns if to_ns is not None else max(starts)
    rows = [r for r in records if as_int(r.get("writer_started_wall_ns")) and lower <= r["writer_started_wall_ns"] <= upper]
    rows.sort(key=lambda r: (r["writer_started_wall_ns"], r.get("redis_reply_wall_ns") or 0))
    outcomes = collections.Counter()
    excluded = collections.Counter()
    invalid = collections.Counter()
    transit = collections.defaultdict(list)
    split = collections.defaultdict(lambda: collections.defaultdict(list))
    segments = collections.defaultdict(lambda: collections.defaultdict(list))
    seen = set()
    for r in rows:
        feed = r.get("feed", "?")
        venue = venue_of(r.get("canonical_instrument_id", "?"))
        outcomes[(feed, venue, r.get("outcome"), r.get("sdk_receive_basis"))] += 1
        receive, started, reply = (as_int(r.get(k)) for k in
                                   ("sdk_receive_monotonic_ns", "writer_started_monotonic_ns", "redis_reply_monotonic_ns"))
        if started is not None and receive is not None:
            value = (started - receive) / 1e6
            if value < 0:
                invalid[(feed, venue, "negative_sdk_to_writer")] += 1
            else:
                segments[feed]["sdk_to_writer_ms"].append(value)
                segments[f"{feed}|{venue}"]["sdk_to_writer_ms"].append(value)
        if started is not None and reply is not None:
            value = (reply - started) / 1e6
            if value < 0:
                invalid[(feed, venue, "negative_writer_to_redis")] += 1
            else:
                segments[feed]["writer_to_redis_ms"].append(value)
                segments[f"{feed}|{venue}"]["writer_to_redis_ms"].append(value)
        if feed not in TRANSIT_GATES:
            continue
        if r.get("outcome") != "APPLIED":
            excluded[(feed, venue, f"outcome_{r.get('outcome')}")] += 1
            continue
        event_time = as_int(r.get("source_event_time_ns"))
        reply_wall = as_int(r.get("redis_reply_wall_ns"))
        if r.get("sdk_receive_basis") != STREAM_BASIS or event_time is None:
            excluded[(feed, venue, f"no_venue_clock_{r.get('sdk_receive_basis')}")] += 1
            continue
        if reply_wall is None:
            invalid[(feed, venue, "missing_reply_clock")] += 1
            continue
        digest = r.get("event_id_sha256")
        identity = (digest if isinstance(digest, str) and len(digest) == 64
                    else (r.get("canonical_instrument_id"), feed, event_time))
        if (feed, identity) in seen:
            excluded[(feed, venue, "repeated_event_identity")] += 1
            continue
        seen.add((feed, identity))
        value = (reply_wall - event_time) / 1e6
        if value < 0:
            invalid[(feed, venue, "negative_transit")] += 1
            continue
        transit[f"{feed}|{venue}"].append(value)
        ingest, sdk_wall = as_int(r.get("envelope_received_at_ns")), as_int(r.get("sdk_receive_wall_ns"))
        if ingest is not None and sdk_wall is not None and event_time <= ingest <= sdk_wall <= reply_wall:
            parts = split[f"{feed}|{venue}"]
            parts["venue_to_ingest_ms"].append((ingest - event_time) / 1e6)
            parts["ingest_to_sdk_ms"].append((sdk_wall - ingest) / 1e6)
            parts["sdk_to_reply_ms"].append((reply_wall - sdk_wall) / 1e6)
        else:
            invalid[(feed, venue, "split_unavailable")] += 1
    result = {"window": {"from_ns": lower, "to_ns": upper, "seconds": round((upper - lower) / 1e9, 1)},
              "gates": {"transit": TRANSIT_GATES, "g7_p95_le_ms": G7_GATES}, "transit": {}, "g7": {}}
    for key in sorted(transit):
        feed = key.split("|", 1)[0]
        block = quantiles(transit[key])
        block["verdict"] = transit_verdict(block, TRANSIT_GATES[feed])
        block["split"] = {name: quantiles(values) for name, values in sorted(split[key].items())}
        result["transit"][key] = block
    for key in sorted(segments):
        block = {name: quantiles(values) for name, values in sorted(segments[key].items())}
        if "|" not in key:
            block["verdict"] = {name: ("NO_DATA" if not block.get(name, {"n": 0})["n"] else
                                       "INSIDE" if block[name]["p95"] <= limit else "OUTSIDE")
                                for name, limit in G7_GATES.items()}
        result["g7"][key] = block
    result["bar_records"] = sum(1 for r in rows if r.get("feed") == "BAR")
    result["outcomes"] = {"|".join(str(p) for p in k): n for k, n in sorted(outcomes.items(), key=str)}
    result["excluded_from_transit"] = {"|".join(k): n for k, n in sorted(excluded.items())}
    result["invalid"] = {"|".join(k): n for k, n in sorted(invalid.items())}
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("inputs", nargs="+", help="trace files or quoted globs")
    parser.add_argument("--skip-seconds", type=float, default=40.0,
                        help="drop the first N s after the first record (writer start-up); ignored with --from-ns")
    parser.add_argument("--from-ns", type=int)
    parser.add_argument("--to-ns", type=int)
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    paths = sorted({path for pattern in args.inputs for path in (glob.glob(pattern) or [pattern])})
    files, records = load(paths)
    report = {"schema": "qdl.p184.trace-feed-transit.v1", "files": len(files), "records": len(records),
              "dropped_at_export_total": sum(f["dropped_at_export"] or 0 for f in files),
              "stop_reasons": dict(collections.Counter(f["stop_reason"] for f in files)),
              **summarise(records, skip_seconds=args.skip_seconds, from_ns=args.from_ns, to_ns=args.to_ns)}
    text = json.dumps(report, indent=1, sort_keys=True)
    if args.out:
        with open(args.out, "x") as handle:
            handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

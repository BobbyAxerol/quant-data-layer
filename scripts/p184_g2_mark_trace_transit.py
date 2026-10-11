"""P18.4 phase D gate G2 (MARK_INDEX_PRICE): new-event transit from the TS writer delivery trace files.

Purpose: summarise, per venue and per instrument, the time from the venue MARK/INDEX event to the TS execution Redis
APPLY reply, read from the files that TS services/market_data/delivery_trace.py exports
(delivery-<worker>-<pid>-<until>[-pNN].json). Gate G2 (TS upgrade/p184-phase-d-gates.md, frozen 2026-10-09):
MARK max < 1000 ms and p95 <= 500 ms per venue, measured per NEW event; the age of a repeated quiet state is reported
separately and not gated.

Definitions (per MARK record with outcome APPLIED and both wall clocks present):
* newest = max(mark_source_event_time_ns, index_source_event_time_ns) (the composite changes when either component does);
* new_event_transit_ms = redis_reply_wall_ns - newest, counted only for the first APPLIED record of each instrument whose
  newest is strictly later than every earlier applied newest of that instrument (gated quantity; same basis as the
  2026-10-09 mark_probe "newest" and tsmark first-apply measurements);
* first_apply_per_mark_event_ms = the same for distinct mark_source_event_time_ns only (tsmark-comparable);
* all_applied_newest_age_ms, mark_age_ms, index_age_ms = every applied record (repeat/quiet ages, NOT gated);
* upstream_ms = sdk_receive_wall_ns - newest (DL + SDK read), writer_ms = redis_reply_wall_ns - sdk_receive_wall_ns.
Negative durations (clock skew) and records without the clocks are counted as invalid, never clamped.
Quantiles are nearest-rank (sorted[min(n-1, int(q*n))]) as in the earlier probes; p99 only when n >= 100.

Boundary: read-only over the given files; no Redis, Docker or network access; it never enables tracing (that is a
runtime env change on the writer, owned by the coordinator). Output is JSON on stdout, or --out (created exclusively).
usage: p184_g2_mark_trace_transit.py FILE_OR_GLOB [...] [--skip-seconds 40] [--from-ns N] [--to-ns N] [--out PATH]
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import sys

FEED = "MARK_INDEX_PRICE"
GATE = {"max_lt_ms": 1000.0, "p95_le_ms": 500.0}


def quantiles(values):
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    n = len(ordered)
    pick = lambda q: round(ordered[min(n - 1, int(q * n))], 1)  # noqa: E731
    return {"n": n, "p50": pick(.5), "p95": pick(.95), "p99": pick(.99) if n >= 100 else None,
            "max": round(ordered[-1], 1), "min": round(ordered[0], 1)}


def as_int(value):
    return value if type(value) is int and value > 0 else None


def venue_of(instrument):
    return instrument.replace(".", ":").split(":", 1)[0].upper()


def load(paths, *, feeds=None, fields=None):
    """Read export files; feeds/fields (optional) keep only those records/keys so a 40-min campaign of ~250 parts
    fits in memory (every record still counts in the per-file metadata)."""
    files, records = [], []
    for path in paths:
        with open(path) as handle:
            report = json.load(handle)
        rows = report.get("records") or []
        starts = [r["writer_started_wall_ns"] for r in rows if as_int(r.get("writer_started_wall_ns"))]
        files.append({"file": path, "worker": report.get("worker"), "part": report.get("part"),
                      "stop_reason": report.get("stop_reason"), "recorded": report.get("recorded"),
                      "dropped_at_export": report.get("dropped_at_export"),
                      "first_start_ns": min(starts) if starts else None, "last_start_ns": max(starts) if starts else None})
        if feeds is not None:
            rows = [r for r in rows if r.get("feed") in feeds]
        if fields is not None:
            rows = [{k: r[k] for k in fields if k in r} for r in rows]
        records.extend(rows)
    return files, records


def summarise(records, *, skip_seconds=40.0, from_ns=None, to_ns=None):
    starts = [r["writer_started_wall_ns"] for r in records if as_int(r.get("writer_started_wall_ns"))]
    if not starts:
        return {"error": "no records with writer_started_wall_ns"}
    lower = from_ns if from_ns is not None else min(starts) + int(skip_seconds * 1e9)
    upper = to_ns if to_ns is not None else max(starts)
    marks = [r for r in records if r.get("feed") == FEED and as_int(r.get("writer_started_wall_ns"))
             and lower <= r["writer_started_wall_ns"] <= upper]
    marks.sort(key=lambda r: (r["writer_started_wall_ns"], r.get("redis_reply_wall_ns") or 0))
    outcomes, bases = collections.Counter(), collections.Counter()
    invalid = collections.Counter()
    series = collections.defaultdict(lambda: collections.defaultdict(list))
    last_newest, last_mark = {}, {}
    for r in marks:
        instrument = r.get("canonical_instrument_id", "?")
        venue = venue_of(instrument)
        outcomes[(venue, r.get("outcome"))] += 1
        bases[(venue, r.get("sdk_receive_basis"))] += 1
        if r.get("outcome") != "APPLIED":
            continue
        reply, mark, index = (as_int(r.get(k)) for k in
                              ("redis_reply_wall_ns", "mark_source_event_time_ns", "index_source_event_time_ns"))
        if reply is None or mark is None or index is None:
            invalid[(venue, "missing_clock")] += 1
            continue
        newest = max(mark, index)
        transit = (reply - newest) / 1e6
        if transit < 0:
            invalid[(venue, "negative_transit")] += 1
            continue
        for key in (venue, instrument):
            series[key]["all_applied_newest_age_ms"].append(transit)
            series[key]["mark_age_ms"].append((reply - mark) / 1e6)
            series[key]["index_age_ms"].append((reply - index) / 1e6)
        receive = as_int(r.get("sdk_receive_wall_ns"))
        if instrument not in last_newest or newest > last_newest[instrument]:
            last_newest[instrument] = newest
            for key in (venue, instrument):
                series[key]["new_event_transit_ms"].append(transit)
                if receive is not None and receive >= newest and reply >= receive:
                    series[key]["upstream_ms"].append((receive - newest) / 1e6)
                    series[key]["writer_ms"].append((reply - receive) / 1e6)
            if receive is None or receive < newest or reply < receive:
                invalid[(venue, "split_unavailable")] += 1
        if instrument not in last_mark or mark > last_mark[instrument]:
            last_mark[instrument] = mark
            for key in (venue, instrument):
                series[key]["first_apply_per_mark_event_ms"].append(transit)
    venues = sorted(k for k in series if ":" not in k)
    result = {"window": {"from_ns": lower, "to_ns": upper, "seconds": round((upper - lower) / 1e9, 1)},
              "gate": GATE, "venues": {}, "instruments": {}}
    for key in sorted(series):
        block = {name: quantiles(values) for name, values in sorted(series[key].items())}
        (result["venues"] if key in venues else result["instruments"])[key] = block
    for venue in venues:
        gated = result["venues"][venue].get("new_event_transit_ms", {"n": 0})
        result["venues"][venue]["g2_verdict"] = (
            "NO_DATA" if not gated["n"] else
            "INSIDE" if gated["max"] < GATE["max_lt_ms"] and gated["p95"] <= GATE["p95_le_ms"] else "OUTSIDE")
    result["outcomes"] = {f"{v}:{o}": n for (v, o), n in sorted(outcomes.items(), key=str)}
    result["sdk_receive_basis"] = {f"{v}:{b}": n for (v, b), n in sorted(bases.items(), key=str)}
    result["invalid"] = {f"{v}:{k}": n for (v, k), n in sorted(invalid.items())}
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
    files, records = load(paths, feeds={FEED})
    report = {"schema": "qdl.p184.g2-mark-trace-transit.v1", "files": len(files),
              "records": len(records), "dropped_at_export_total": sum(f["dropped_at_export"] or 0 for f in files),
              "stop_reasons": dict(collections.Counter(f["stop_reason"] for f in files)),
              **summarise(records, skip_seconds=args.skip_seconds, from_ns=args.from_ns, to_ns=args.to_ns),
              "per_file": files}
    text = json.dumps(report, indent=1, sort_keys=True)
    if args.out:
        with open(args.out, "x") as handle:
            handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

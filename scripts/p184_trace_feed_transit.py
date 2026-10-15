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
* venue clock = source_event_time_ns of the SDK stream envelope; only stream-return records carry it: the Python
  worker's SDK_STREAM_RETURN and the TS native (Rust) writer's NATIVE_STREAM_RETURN (TS rust/crates/market-data-writer
  src/trace.rs, the same tonic-return boundary). Which basis a gated row used is recorded per feed|venue
  (transit[...].stream_basis). Records of other bases (warmup, query, no basis) have no venue clock and are counted,
  never given a substitute clock;
* duplicate_applied_event_id (G10) = APPLIED records whose canonical event_id digest was already APPLIED earlier in the
  window, per feed and venue (records without a digest are not counted, their fallback identity is not unique);
* new-event transit = redis_reply_wall_ns - source_event_time_ns for the first APPLIED record of each event identity
  (event_id_sha256; when absent, instrument + feed + source_event_time_ns). REFRESHED and DUPLICATE outcomes and a
  repeated identity are the same state re-published (IDR-082 quiet republish) and are counted, not gated;
* transit split (attribution only, not gated), for gated records whose clocks are all present: venue_to_ingest =
  envelope_received_at_ns - source_event_time_ns (ingestor receive; the envelope's received/normalized/published
  stamps are one ingest clock, not a Kafka commit), ingest_to_sdk = sdk_receive_wall_ns - envelope_received_at_ns
  (Data Layer pipeline + Stream + SDK), sdk_to_reply = redis_reply_wall_ns - sdk_receive_wall_ns (TS writer);
* gateway split of ingest_to_sdk (attribution only, SDK 2.4.0 gateway clocks, records that carry them): the trace's
  gateway_send_wall_ns (the Stream gateway's gRPC transport took the record's response) splits it into
  ingest_to_gateway_send (Data Layer pipeline + gateway queues) and gateway_send_to_sdk (HTTP/2 + network + client
  buffers + SDK, i.e. the TS writer not pulling); with gateway_dispatch_wall_ns and gateway_dequeue_wall_ns also
  present, ingest_to_gateway_send splits again into ingest_to_gateway_dispatch (Kafka/projection up to the gateway
  hub), gateway_dispatch_to_dequeue (subscription queue wait) and gateway_dequeue_to_send (handoff wait: the transport
  did not pull). Records without the clocks (NOT_MEASURED, an older gateway or writer) are counted under
  gateway_split.not_measured, never given a substitute; clocks out of order are counted under out_of_order;
* G7 segments use the process monotonic clocks of the same worker: sdk_to_writer = writer_started_monotonic_ns -
  sdk_receive_monotonic_ns; writer_to_redis = redis_reply_monotonic_ns - writer_started_monotonic_ns, over every record
  with a Redis reply (any outcome). TRADE/QUOTE rows of one atomic APPLY batch share the batch start as writer entry.
Negative durations are counted as invalid, never clamped. Quantiles are nearest-rank as in the G2 MARK script.

Raw files are split by writer kind (worker name "native-*" = the native writer, else a Python worker): the native raw
file is one bounded file per activation (<= 4096 records, no rotation), so its CAP stop is a SAMPLE boundary, not a
coverage gap; stop reasons and export drops are reported per kind (files_by_kind) so the scorecard can keep the raw
coverage rule for Python-worker feeds only.

Native writer summary (--native-summary GLOB, files summary-native-<worker>-<pid>-<expires>.json, schema
ts.native-writer-summary.v1, TS trace.rs export_summary + latency.rs): per route (provider, symbol, feed) cumulative
fixed-bucket histograms over every apply completed in the activation window (1 ms buckets through 1000 ms, then powers
of two), fed independently of the raw-trace cap. Routes are merged per feed|venue by SUMMING bucket counts (never
percentiles); max = max of the exact per-route maxima; p50/p95/p99 are nearest-rank bucket UPPER bounds (p99 only when
n >= 100), as the writer computes them. G2 new-event transit = source_event_to_ack of the APPLIED outcome; G7
sdk_to_writer and writer_to_redis (= writer_to_ack) over every outcome. Scope = the union of the activation windows,
which must span the load window; the seconds outside the load are reported (window_scope), never subtracted.

Activation end (Codex review S100-5): the configured expiry (expires_unix_ns) is a future deadline, never coverage.
The end of an activation is, in order: the summary's own observed_end_unix_ns (TS trace.rs, export time after the
session drain; the latency aggregates stop at min(expiry, that)), else independently pinned process-end evidence
(--process-end STARTED_NS:FINISHED_NS[:LAST_SEEN_NS], e.g. the G11 probe's Docker State.StartedAt/FinishedAt of the
old writer run and its last poll that saw the old writer: end bounded by [last seen, FinishedAt]), else NOT_MEASURED
(a coverage gap). window_scope reports pre-load / load / post-load seconds separately; the cumulative histogram
cannot be split by phase (distribution_scope says so), so the gated distribution includes pre/post-load applies.

Load slices (TS e35bb316 latency.rs slices_report, summary schema_revision 1.1, block ack_latency_slices schema
ts.native-ack-latency-slices.v1): 60 s wall-clock slices aligned to the Unix epoch, half-open [start, end), membership
by apply completion (latency) / decision clock (integrity). When every scored summary carries an ACTIVE bucketed slice
export with all overflow_*/unanchored_* counters at 0 and a slice exists for every slice intersecting the load window,
G2 is scored from applied_source_event_to_ack.live, G7 from the slices' sdk_to_writer/writer_to_ack and G10 from the
slices' integrity counters, over EVERY slice that intersects the load window (edge slices in full: the scored
population is a superset of the load window, never a subset); distribution_scope = LOAD_SLICES_SUPERSET [from, to).
Catch-up (stream rows without a gateway dispatch clock: resume replay, or without a receive stamp) is never gated and
never dropped: catch_up_report_only per feed|venue gives its rows and applied count/max in scope and over the whole
activation. Any failed condition falls back to the cumulative block with distribution_scope marked
CUMULATIVE_FALLBACK and the reasons listed (slice_fallback_reasons); summaries without slices read exactly as before.

G10 native counters (TS latency.rs apply_integrity, schema ts.native-apply-integrity.v1): per route, counted at the
atomic apply decision for every unit while the trace is configured (not window-gated, fixed memory): decisions,
replay_attempted (a stream row at/below the highest decided offset), duplicate_applied (a stream row APPLIED at/below
the applied high-water: an economic duplicate), ambiguous_after_unconfirmed (APPLIED at/below a lost-reply row) and
epochs. Merged per feed|venue (native_summary.g10); a route whose duplicate first..last span lies wholly outside the
load window is not counted in the window, otherwise its whole count is (conservative). A summary without
apply_integrity is NOT_MEASURED for G10, never zero.
Validation and identity (Codex review SR3): every route row is validated before it is summed (route identity strings;
every gate counter present as an int >= 0, no default to zero; decisions sum == rows; replay/duplicate/ambiguous
bounded by their populations). An invalid row is listed in g10.invalid_routes and named in coverage_gaps, never summed.
Route identity is kept (g10.per_route, key FEED|VENUE|SYMBOL, with its activation and integrity epochs: epoch,
opened_by, first/last offset) so the scorer can require the frozen route roster. `duplicate_applied` is the writer's
offset high-water check (a stream row decided APPLIED at/below the route's prior APPLIED offset, per process and
route across its epochs), not an event-identity duplicate oracle; anchor (non-stream) rows are not checked by it.

Boundary: read-only over the given files; no Redis, Docker or network access; it never enables tracing. Output is JSON
on stdout, or --out (created exclusively).
usage: p184_trace_feed_transit.py FILE_OR_GLOB [...] [--skip-seconds 40] [--from-ns N] [--to-ns N] [--out PATH]
       [--native-summary GLOB] [--process-end STARTED_NS:FINISHED_NS[:LAST_SEEN_NS]]
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
# Stream-return bases that carry the venue clock: Python SDK worker and TS native (Rust) writer (trace.rs:297-301).
STREAM_BASES = ("SDK_STREAM_RETURN", "NATIVE_STREAM_RETURN")
NATIVE_WORKER_PREFIX = "native-"
NATIVE_SUMMARY_SCHEMA = "ts.native-writer-summary.v1"
NATIVE_ACK_SCHEMA = "ts.native-ack-latency.v1"
NATIVE_ACK_SCOPE = "apply_rows_completed_in_activation_window"
NATIVE_OUTCOMES = ("duplicate", "applied", "refreshed", "errors")
NATIVE_G7 = {"sdk_to_writer_ms": "sdk_to_writer", "writer_to_redis_ms": "writer_to_ack"}
NATIVE_INTEGRITY_SCHEMA = "ts.native-apply-integrity.v1"
NATIVE_SLICES_SCHEMA = "ts.native-ack-latency-slices.v1"
SLICE_LOSS_COUNTERS = ("overflow_latency_rows", "overflow_integrity_units", "unanchored_latency_rows",
                       "unanchored_integrity_units")
SLICE_INTEGRITY_COUNTERS = ("rows", "stream_rows", "units", "unconfirmed_units", "unconfirmed_rows",
                            "replay_attempted", "duplicate_applied", "ambiguous_after_unconfirmed")
INTEGRITY_COUNTERS = ("rows", "stream_rows", "units", "unconfirmed_rows", "replay_attempted", "duplicate_applied",
                      "ambiguous_after_unconfirmed", "epochs_total", "epochs_dropped")
# Required on every apply_integrity route row (TS latency.rs RouteIntegrity::report); unconfirmed_units is summed too.
INTEGRITY_REQUIRED = INTEGRITY_COUNTERS + ("unconfirmed_units",)
DECISION_NAMES = ("applied", "refreshed", "duplicate_ignored")
DUPLICATE_APPLIED_BASIS = ("offset high-water check: a stream row decided APPLIED at or below the route's prior APPLIED "
                           "offset high-water (per writer process and route, across its epochs); not an event-identity "
                           "duplicate oracle; anchor (non-stream) rows are not checked")
# Keys summarise() reads; loading only these keeps a full campaign (~250 parts x 4096 records) in memory.
FIELDS = ("feed", "canonical_instrument_id", "outcome", "sdk_receive_basis", "sdk_receive_monotonic_ns",
          "writer_started_monotonic_ns", "redis_reply_monotonic_ns", "writer_started_wall_ns", "redis_reply_wall_ns",
          "source_event_time_ns", "event_id_sha256", "envelope_received_at_ns", "sdk_receive_wall_ns",
          "gateway_dispatch_wall_ns", "gateway_dequeue_wall_ns", "gateway_send_wall_ns")


def gateway_split(r, ingest, sdk_wall):
    """(status, {segment: ms}) for the gateway split of ingest_to_sdk; status MEASURED, NOT_MEASURED or
    OUT_OF_ORDER. The dispatch/dequeue segments are added only when both of those clocks are present too."""
    send = as_int(r.get("gateway_send_wall_ns"))
    if send is None:
        return "NOT_MEASURED", {}
    if not ingest <= send <= sdk_wall:
        return "OUT_OF_ORDER", {}
    parts = {"ingest_to_gateway_send_ms": (send - ingest) / 1e6, "gateway_send_to_sdk_ms": (sdk_wall - send) / 1e6}
    dispatch, dequeue = as_int(r.get("gateway_dispatch_wall_ns")), as_int(r.get("gateway_dequeue_wall_ns"))
    if dispatch is not None and dequeue is not None:
        if not ingest <= dispatch <= dequeue <= send:
            return "OUT_OF_ORDER", {}
        parts.update(ingest_to_gateway_dispatch_ms=(dispatch - ingest) / 1e6,
                     gateway_dispatch_to_dequeue_ms=(dequeue - dispatch) / 1e6,
                     gateway_dequeue_to_send_ms=(send - dequeue) / 1e6)
    return "MEASURED", parts


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
    bases = collections.defaultdict(collections.Counter)
    coverage = collections.defaultdict(collections.Counter)
    segments = collections.defaultdict(lambda: collections.defaultdict(list))
    seen = set()
    duplicates = collections.Counter()
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
        if r.get("sdk_receive_basis") not in STREAM_BASES or event_time is None:
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
            if identity is digest:  # G10: the same canonical event_id APPLIED twice (fallback identities excluded)
                duplicates[(feed, venue)] += 1
            continue
        seen.add((feed, identity))
        value = (reply_wall - event_time) / 1e6
        if value < 0:
            invalid[(feed, venue, "negative_transit")] += 1
            continue
        transit[f"{feed}|{venue}"].append(value)
        bases[f"{feed}|{venue}"][r["sdk_receive_basis"]] += 1
        ingest, sdk_wall = as_int(r.get("envelope_received_at_ns")), as_int(r.get("sdk_receive_wall_ns"))
        if ingest is not None and sdk_wall is not None and event_time <= ingest <= sdk_wall <= reply_wall:
            parts = split[f"{feed}|{venue}"]
            parts["venue_to_ingest_ms"].append((ingest - event_time) / 1e6)
            parts["ingest_to_sdk_ms"].append((sdk_wall - ingest) / 1e6)
            parts["sdk_to_reply_ms"].append((reply_wall - sdk_wall) / 1e6)
            status, gateway = gateway_split(r, ingest, sdk_wall)
            coverage[f"{feed}|{venue}"][status.lower()] += 1
            for name, value in gateway.items():
                parts[name].append(value)
        else:
            invalid[(feed, venue, "split_unavailable")] += 1
    result = {"window": {"from_ns": lower, "to_ns": upper, "seconds": round((upper - lower) / 1e9, 1)},
              "gates": {"transit": TRANSIT_GATES, "g7_p95_le_ms": G7_GATES}, "transit": {}, "g7": {}}
    for key in sorted(transit):
        feed = key.split("|", 1)[0]
        block = quantiles(transit[key])
        block["verdict"] = transit_verdict(block, TRANSIT_GATES[feed])
        block["split"] = {name: quantiles(values) for name, values in sorted(split[key].items())}
        block["gateway_split"] = {name: coverage[key][name] for name in ("measured", "not_measured", "out_of_order")}
        block["stream_basis"] = dict(sorted(bases[key].items()))
        result["transit"][key] = block
    for key in sorted(segments):
        block = {name: quantiles(values) for name, values in sorted(segments[key].items())}
        if "|" not in key:
            block["verdict"] = {name: ("NO_DATA" if not block.get(name, {"n": 0})["n"] else
                                       "INSIDE" if block[name]["p95"] <= limit else "OUTSIDE")
                                for name, limit in G7_GATES.items()}
        result["g7"][key] = block
    result["bar_records"] = sum(1 for r in rows if r.get("feed") == "BAR")
    result["duplicate_applied_event_id"] = {"|".join(k): n for k, n in sorted(duplicates.items())}
    result["outcomes"] = {"|".join(str(p) for p in k): n for k, n in sorted(outcomes.items(), key=str)}
    result["excluded_from_transit"] = {"|".join(k): n for k, n in sorted(excluded.items())}
    result["invalid"] = {"|".join(k): n for k, n in sorted(invalid.items())}
    return result


def writer_kind(worker) -> str:
    return "native" if str(worker or "").startswith(NATIVE_WORKER_PREFIX) else "python"


def files_by_kind(files) -> dict:
    """Per writer kind: file count, stop reasons, export drops and the record start span of each kind."""
    out = {}
    for f in files:
        kind = out.setdefault(writer_kind(f.get("worker")), {"files": 0, "stop_reasons": collections.Counter(),
                                                              "dropped_at_export_total": 0,
                                                              "first_start_ns": None, "last_start_ns": None})
        kind["files"] += 1
        kind["stop_reasons"][f.get("stop_reason")] += 1
        kind["dropped_at_export_total"] += f.get("dropped_at_export") or 0
        for key, pick in (("first_start_ns", min), ("last_start_ns", max)):
            if f.get(key) is not None:
                kind[key] = f[key] if kind.get(key) is None else pick(kind[key], f[key])
    return {k: {**v, "stop_reasons": dict(v["stop_reasons"])} for k, v in sorted(out.items())}


def native_raw_activations(paths) -> list[dict]:
    """(worker, pid, first/last record start) of each native raw file, read from its header and records only."""
    out = []
    for path in paths:
        with open(path) as handle:
            report = json.load(handle)
        if writer_kind(report.get("worker")) != "native":
            continue
        starts = [r.get("writer_started_wall_ns") for r in report.get("records") or []]
        starts = [s for s in starts if as_int(s)]
        out.append({"worker": report.get("worker"), "pid": report.get("pid"),
                    "expires_unix_ns": report.get("expires_unix_ns"), "stop_reason": report.get("stop_reason"),
                    "first_start_ns": min(starts) if starts else None, "last_start_ns": max(starts) if starts else None})
    return out


class Buckets:
    """Merged fixed-bucket histogram: bucket counts are summed, never percentiles (latency.rs report_with_buckets)."""

    def __init__(self, upper_ms):
        self.upper_ms = list(upper_ms)
        self.counts = [0] * len(self.upper_ms)
        self.n = self.missing = self.negative = 0
        self.max_ms = None

    def add(self, metric: dict):
        n = int(metric.get("n") or 0)
        counted = 0
        for index, count in metric.get("sparse_bucket_counts") or []:
            self.counts[int(index)] += int(count)
            counted += int(count)
        if counted != n:
            raise ValueError(f"bucket counts {counted} != n {n}")
        self.n += n
        self.missing += int(metric.get("missing_clocks") or 0)
        self.negative += int(metric.get("negative_clocks") or 0)
        if n and metric.get("max_ms") is not None:
            self.max_ms = metric["max_ms"] if self.max_ms is None else max(self.max_ms, metric["max_ms"])

    def percentile(self, percent: int):
        if not self.n or (percent == 99 and self.n < 100):
            return None
        rank = -(-self.n * percent // 100)
        cumulative = 0
        for index, count in enumerate(self.counts):
            cumulative += count
            if cumulative >= rank:
                return self.upper_ms[index]
        return None

    def block(self) -> dict:
        return {"n": self.n, "p50": self.percentile(50), "p95": self.percentile(95), "p99": self.percentile(99),
                "max": self.max_ms, "missing_clocks": self.missing, "negative_clocks": self.negative,
                "quantile_basis": "BUCKET_UPPER_BOUND"}


def parse_process_end(text: str) -> dict:
    """STARTED_NS:FINISHED_NS[:LAST_SEEN_NS] -> {'started_ns', 'finished_ns', 'last_seen_ns'} (all unix ns)."""
    parts = [int(x) if x else None for x in str(text).split(":")]
    if len(parts) not in (2, 3) or parts[1] is None or (parts[0] is not None and parts[0] >= parts[1]):
        raise ValueError(f"invalid --process-end {text!r}")
    return {"started_ns": parts[0], "finished_ns": parts[1], "last_seen_ns": parts[2] if len(parts) == 3 else None}


def activation_end(report: dict, process_ends=()) -> dict:
    """{'basis', 'end_lower_ns', 'end_upper_ns', 'process_end'?}: when this activation stopped observing.
    The configured expiry only CAPS the aggregate window (aggregates stop counting there); it never proves coverage."""
    started, expires = as_int(report.get("started_unix_ns")), as_int(report.get("expires_unix_ns"))

    def capped(value):
        return min(value, expires) if value is not None and expires is not None else value
    observed = as_int(report.get("observed_end_unix_ns"))
    if observed is not None:
        end = capped(observed)
        return {"basis": "SUMMARY_OBSERVED_END", "end_lower_ns": end, "end_upper_ns": end}
    for evidence in process_ends:
        if started is not None and evidence.get("started_ns") is not None and started < evidence["started_ns"]:
            continue
        if started is not None and started >= evidence["finished_ns"]:
            continue
        seen = evidence.get("last_seen_ns")
        lower = capped(seen) if seen is not None and (started is None or seen >= started) else None
        return {"basis": "PROCESS_END_EVIDENCE", "end_lower_ns": lower,
                "end_upper_ns": capped(evidence["finished_ns"]), "process_end": evidence}
    return {"basis": "NOT_MEASURED", "end_lower_ns": None, "end_upper_ns": None}


def native_window_scope(activations, from_ns, to_ns) -> dict | None:
    """Pre-load / load / post-load seconds of the overlapping activations, from measured ends only."""
    overlapping = [a for a in activations if a.get("overlaps_load")]
    if not overlapping or from_ns is None or to_ns is None:
        return None
    first = min(a["started_unix_ns"] or from_ns for a in overlapping)
    ends = [a.get("end_upper_ns") for a in overlapping]
    known = all(e is not None for e in ends)
    last = max(ends) if known else None
    covered_to = min(to_ns, last) if known else None
    bases = sorted({a.get("end_basis") for a in overlapping})
    return {"basis": "NATIVE_SUMMARY_ACTIVATION_WINDOW (cumulative; buckets cannot be cut to the load window)",
            "end_basis": bases[0] if len(bases) == 1 else bases,
            "activation_from_ns": first, "activation_to_ns": last,
            "activation_to_lower_ns": (min(a["end_lower_ns"] for a in overlapping)
                                       if known and all(a.get("end_lower_ns") is not None for a in overlapping)
                                       else None),
            "configured_expires_unix_ns_not_coverage":
                max((a.get("expires_unix_ns") or 0) for a in overlapping) or None,
            "pre_load_seconds": round(max(0, from_ns - first) / 1e9, 1),
            "load_seconds_covered": round(max(0, covered_to - max(first, from_ns)) / 1e9, 1) if known else None,
            "load_seconds": round((to_ns - from_ns) / 1e9, 1),
            "post_load_seconds": round(max(0, last - to_ns) / 1e9, 1) if known else None,
            "distribution_scope": "PRE_LOAD+LOAD+POST_LOAD_NOT_SEPARABLE (one cumulative histogram per activation)"}


def _unix(report: dict, monotonic):
    """Process monotonic -> unix ns through the summary's own started anchor (None when unavailable)."""
    started_unix, started_mono = as_int(report.get("started_unix_ns")), as_int(report.get("started_monotonic_ns"))
    monotonic = as_int(monotonic)
    if None in (started_unix, started_mono, monotonic) or monotonic < started_mono:
        return None
    return started_unix + (monotonic - started_mono)


def _uint(value) -> bool:
    return type(value) is int and value >= 0


def _decision_problems(value, name) -> list[str]:
    if not isinstance(value, dict):
        return [f"{name} not an object"]
    return [f"{name}.{k} {value.get(k)!r}" for k in DECISION_NAMES if not _uint(value.get(k))]


def validate_integrity_route(item, required=INTEGRITY_REQUIRED, *, epochs=True) -> list[str]:
    """Problems of one apply_integrity route row (or slice cell integrity with required=SLICE_INTEGRITY_COUNTERS and
    epochs=False); [] = valid. Absent counters are a problem, never zero."""
    if not isinstance(item, dict):
        return ["row not an object"]
    problems = []
    route = item.get("route")
    if not isinstance(route, dict) or any(not isinstance(route.get(k), str) or not route.get(k).strip()
                                          for k in ("provider", "symbol", "feed")):
        problems.append("route identity (provider/symbol/feed) missing")
    body = item.get("integrity", item) if not epochs else item
    if not isinstance(body, dict):
        return problems + ["integrity not an object"]
    problems += [f"{name} {body.get(name)!r}" for name in required if not _uint(body.get(name))]
    problems += _decision_problems(body.get("decisions"), "decisions")
    if epochs:
        problems += _decision_problems(body.get("replay_decisions"), "replay_decisions")
        listed = body.get("epochs")
        if not isinstance(listed, list) or any(
                not isinstance(e, dict) or not _uint(e.get("epoch")) or not isinstance(e.get("opened_by"), str)
                for e in listed):
            problems.append("epochs not a list of {epoch, opened_by}")
    if problems:
        return problems
    decided = sum(body["decisions"][k] for k in DECISION_NAMES)
    if decided != body["rows"]:
        problems.append(f"decisions sum {decided} != rows {body['rows']}")
    if body["stream_rows"] > body["rows"]:
        problems.append(f"stream_rows {body['stream_rows']} > rows {body['rows']}")
    if body["replay_attempted"] > body["stream_rows"]:
        problems.append(f"replay_attempted {body['replay_attempted']} > stream_rows {body['stream_rows']}")
    if body["duplicate_applied"] + body["ambiguous_after_unconfirmed"] > body["decisions"]["applied"]:
        problems.append("duplicate_applied + ambiguous_after_unconfirmed > applied decisions")
    return problems


def route_key(route: dict) -> str:
    return f"{route.get('feed')}|{str(route.get('provider')).upper()}|{str(route.get('symbol')).upper()}"


def merge_integrity(reports, from_ns, to_ns) -> dict:
    """native_summary.g10: per feed|venue sums of the validated apply-decision counters of the overlapping activations,
    plus per_route (identity, activation, epochs) and invalid_routes (never summed)."""
    rows, gaps, rejected, per_route, invalid = {}, [], 0, {}, []
    for path, report in reports:
        block = report.get("apply_integrity")
        if not isinstance(block, dict) or block.get("schema") != NATIVE_INTEGRITY_SCHEMA:
            gaps.append(f"native summary without {NATIVE_INTEGRITY_SCHEMA} counters: {path}")
            continue
        if not _uint(block.get("rejected_route_units")) or not isinstance(block.get("routes"), list):
            gaps.append(f"native apply_integrity without typed rejected_route_units/routes: {path}")
            continue
        rejected += block["rejected_route_units"]
        for item in block["routes"]:
            problems = validate_integrity_route(item)
            if problems:
                ident = route_key(item.get("route") or {}) if isinstance(item, dict) and isinstance(
                    item.get("route"), dict) else "?"
                invalid.append({"route": ident, "path": path, "problems": problems[:8]})
                gaps.append(f"invalid apply_integrity route {ident} ({path}): {'; '.join(problems[:3])}")
                continue
            route = item["route"]
            key = f"{route.get('feed')}|{str(route.get('provider')).upper()}"
            entry = per_route.setdefault(route_key(route), {
                **{name: 0 for name in INTEGRITY_REQUIRED},
                "decisions": {k: 0 for k in DECISION_NAMES}, "activations": []})
            for name in INTEGRITY_REQUIRED:
                entry[name] += item[name]
            for outcome in DECISION_NAMES:
                entry["decisions"][outcome] += item["decisions"][outcome]
            entry["activations"].append({
                "worker": report.get("worker"), "pid": report.get("pid"),
                "started_unix_ns": as_int(report.get("started_unix_ns")),
                "start_offset": item.get("start_offset"), "end_offset": item.get("end_offset"),
                "epochs_total": item["epochs_total"], "epochs_dropped": item["epochs_dropped"],
                "epochs": [{k: e.get(k) for k in ("epoch", "opened_by", "first_offset", "last_offset",
                                                    "unconfirmed_rows", "duplicate_applied")}
                           for e in item["epochs"]][:8]})
            row = rows.setdefault(key, {**{name: 0 for name in INTEGRITY_COUNTERS}, "routes": 0,
                                        "decisions": {"applied": 0, "refreshed": 0, "duplicate_ignored": 0},
                                        "replay_decisions": {"applied": 0, "refreshed": 0, "duplicate_ignored": 0},
                                        "duplicate_applied_in_window": 0, "duplicate_spans_unix_ns": []})
            row["routes"] += 1
            for name in INTEGRITY_REQUIRED:
                row[name] = row.get(name, 0) + item[name]
            for name in ("decisions", "replay_decisions"):
                for outcome in row[name]:
                    row[name][outcome] += item[name][outcome]
            count = item["duplicate_applied"]
            if count:
                first = _unix(report, item.get("duplicate_applied_first_monotonic_ns"))
                last = _unix(report, item.get("duplicate_applied_last_monotonic_ns"))
                outside = (first is not None and last is not None and from_ns is not None and to_ns is not None
                           and (last < from_ns or first > to_ns))
                row["duplicate_applied_in_window"] += 0 if outside else count
                row["duplicate_spans_unix_ns"].append([first, last, count])
    if rejected:
        gaps.append(f"{rejected} native apply units of routes beyond max_routes not counted")
    return {"basis": "TS apply_integrity: atomic apply decision counters, whole activation (not window-gated)",
            "in_window_rule": "a route's duplicate count is in the window unless its first..last span is wholly "
                              "outside",
            "duplicate_applied_basis": DUPLICATE_APPLIED_BASIS,
            "per_feed_venue": dict(sorted(rows.items())), "per_route": dict(sorted(per_route.items())),
            "invalid_routes": invalid, "coverage_gaps": gaps}


def slice_plan(valid, from_ns, to_ns):
    """(plan or None, fallback reasons) for scoring from ack_latency_slices (TS latency.rs slices_report).

    None with no reasons = no scored summary carries slices (an older writer): the cumulative path, unchanged. A plan
    needs every scored summary to carry an ACTIVE, bucketed slice export with zero overflow_*/unanchored_* counters,
    and a slice present (in some summary) for every slice that intersects [from_ns, to_ns); the covered range is
    [first slice start, last slice end), a superset of the load window, never a subset."""
    blocks = [(path, report.get("ack_latency_slices")) for path, report, _ in valid]
    if not blocks or all(block is None for _, block in blocks):
        return None, []
    reasons, seconds, present = [], set(), set()
    if from_ns is None or to_ns is None or to_ns <= from_ns:
        reasons.append("no load window to select slices")
    for path, block in blocks:
        if not isinstance(block, dict) or block.get("schema") != NATIVE_SLICES_SCHEMA:
            reasons.append(f"no {NATIVE_SLICES_SCHEMA} block: {path}")
            continue
        if block.get("status") != "ACTIVE":
            reasons.append(f"slices status {block.get('status')!r}: {path}")
        if block.get("buckets") is not True:
            reasons.append(f"slices exported without buckets (summary byte cap): {path}")
        for name in SLICE_LOSS_COUNTERS:
            if int(block.get(name) or 0):
                reasons.append(f"{name}={block.get(name)}: {path}")
        seconds.add(as_int(block.get("slice_seconds")))
        for item in block.get("slices") or []:
            start, end = as_int(item.get("start_unix_ns")), as_int(item.get("end_unix_ns"))
            if start is None or end is None or end - start != (as_int(block.get("slice_seconds")) or 0) * 10**9:
                reasons.append(f"malformed slice [{start}, {end}): {path}")
                continue
            present.add(start)
    if len(seconds) != 1 or not next(iter(seconds)):
        reasons.append(f"slice_seconds not one positive value: {sorted(seconds, key=str)}")
    if reasons:
        return None, reasons
    step = next(iter(seconds)) * 10**9
    starts = list(range(from_ns - from_ns % step, to_ns, step))
    missing = [start for start in starts if start not in present]
    if missing:
        return None, [f"{len(missing)} of {len(starts)} slices intersecting the load window absent "
                      f"(first {missing[0]})"]
    return {"slice_seconds": step // 10**9, "starts": set(starts), "covered_from_ns": starts[0],
            "covered_to_ns": starts[-1] + step}, []


def slice_cells(path, report, plan):
    """(path, feed|venue, cell) of every route cell of the planned slices of one summary."""
    for item in (report.get("ack_latency_slices") or {}).get("slices") or []:
        if as_int(item.get("start_unix_ns")) not in plan["starts"]:
            continue
        for cell in item.get("routes") or []:
            route = cell.get("route") or {}
            yield path, f"{route.get('feed')}|{str(route.get('provider')).upper()}", cell


class CatchUp:
    """Report-only catch-up (resume replay: stream rows the gateway did not dispatch live). Never gated, never
    dropped: in-scope (covered slices) and whole-activation (cumulative per-route catch_up block) kept apart."""

    def __init__(self):
        self.parts = {}

    def _add(self, part, counts, metric):
        row = self.parts.setdefault(part, {"rows": {o: 0 for o in NATIVE_OUTCOMES}, "applied_n": 0,
                                           "applied_max_ms": None, "applied_missing_clocks": 0})
        for outcome in NATIVE_OUTCOMES:
            row["rows"][outcome] += int((counts or {}).get(outcome) or 0)
        metric = metric or {}
        row["applied_n"] += int(metric.get("n") or 0)
        row["applied_missing_clocks"] += int(metric.get("missing_clocks") or 0)
        if metric.get("n") and metric.get("max_ms") is not None:
            row["applied_max_ms"] = (metric["max_ms"] if row["applied_max_ms"] is None
                                     else max(row["applied_max_ms"], metric["max_ms"]))

    def add_cumulative(self, block):
        self._add("whole_activation", block, block.get("applied_source_event_to_ack"))

    def add_slice(self, cell):
        self._add("in_scope", (cell.get("stream") or {}).get("catch_up"),
                  (cell.get("applied_source_event_to_ack") or {}).get("catch_up"))

    def block(self, *, in_scope):
        out = {"verdict": "REPORT_ONLY", "basis": "stream rows without gateway_dispatch_wall_ns (resume replay) or "
                                                  "without a receive stamp; applied source_event_to_ack"}
        if in_scope:
            out["in_scope"] = self.parts.get("in_scope") or self._empty()
        if "whole_activation" in self.parts:
            out["whole_activation"] = self.parts["whole_activation"]
        return out

    @staticmethod
    def _empty():
        return {"rows": {o: 0 for o in NATIVE_OUTCOMES}, "applied_n": 0, "applied_max_ms": None,
                "applied_missing_clocks": 0}


def slice_integrity(g10, valid, plan, scope) -> dict:
    """native_summary.g10 with the decision counters re-summed over the planned slices only (superset of the load
    window). Quantities the slices do not carry (replay_decisions, epochs) stay whole-activation and are named so;
    each row keeps its whole-activation counters under whole_activation."""
    sums, route_sums, invalid, gaps = {}, {}, [], list(g10.get("coverage_gaps") or [])
    for path, report, _ in valid:
        for _, key, cell in slice_cells(path, report, plan):
            problems = validate_integrity_route(cell, SLICE_INTEGRITY_COUNTERS, epochs=False)
            if problems:
                ident = route_key(cell.get("route") or {}) if isinstance(cell.get("route"), dict) else "?"
                invalid.append({"route": ident, "path": path, "slice": True, "problems": problems[:8]})
                gaps.append(f"invalid slice integrity cell {ident} ({path}): {'; '.join(problems[:3])}")
                continue
            integrity = cell["integrity"]
            for target in (sums.setdefault(key, {**{name: 0 for name in SLICE_INTEGRITY_COUNTERS},
                                                 "decisions": {k: 0 for k in DECISION_NAMES}}),
                           route_sums.setdefault(route_key(cell["route"]), {
                               **{name: 0 for name in SLICE_INTEGRITY_COUNTERS},
                               "decisions": {k: 0 for k in DECISION_NAMES}})):
                for name in SLICE_INTEGRITY_COUNTERS:
                    target[name] += integrity[name]
                for outcome in DECISION_NAMES:
                    target["decisions"][outcome] += integrity["decisions"][outcome]
    rows = {}
    for key in sorted(set(g10.get("per_feed_venue") or {}) | set(sums)):
        whole = dict((g10.get("per_feed_venue") or {}).get(key) or {})
        mine = sums.get(key) or {**{name: 0 for name in SLICE_INTEGRITY_COUNTERS},
                                 "decisions": {"applied": 0, "refreshed": 0, "duplicate_ignored": 0}}
        rows[key] = {**whole, **mine, "duplicate_applied_in_window": mine["duplicate_applied"],
                     "counters_scope": scope,
                     "whole_activation_only": ["replay_decisions", "epochs_total", "epochs_dropped",
                                               "duplicate_spans_unix_ns"],
                     "whole_activation": {name: whole.get(name) for name in
                                          SLICE_INTEGRITY_COUNTERS + ("decisions", "duplicate_applied_in_window")
                                          if name in whole}}
    per_route = {}
    for key, whole in (g10.get("per_route") or {}).items():
        mine = route_sums.get(key) or {**{name: 0 for name in SLICE_INTEGRITY_COUNTERS},
                                       "decisions": {k: 0 for k in DECISION_NAMES}}
        per_route[key] = {**whole, **mine, "counters_scope": scope,
                          "whole_activation": {name: whole.get(name) for name in
                                               SLICE_INTEGRITY_COUNTERS + ("decisions",) if name in whole}}
    for key in set(route_sums) - set(per_route):  # a slice route without a whole-activation row: keep, mark
        per_route[key] = {**route_sums[key], "counters_scope": scope, "activations": [],
                          "whole_activation_missing": True}
        gaps.append(f"slice route {key} has no whole-activation apply_integrity row")
    return {**g10, "basis": "TS ack_latency_slices integrity: atomic apply decision counters of the slices "
                            "intersecting the load window",
            "in_window_rule": "sum of the covered slices (edge slices in full: a superset of the load window)",
            "scope": scope, "per_feed_venue": rows, "per_route": dict(sorted(per_route.items())),
            "invalid_routes": list(g10.get("invalid_routes") or []) + invalid, "coverage_gaps": gaps}


def native_summary(paths, *, from_ns=None, to_ns=None, raw_activations=(), process_ends=()) -> dict:
    """G2 TRADE/QUOTE transit and G7 segments of the native writer from its final summary files (see module doc).

    coverage_gaps lists every reason the summaries do not cover [from_ns, to_ns] of the native routes: an unreadable
    or non-final summary, a different histogram, rejected routes, an activation overlapping the load that does not
    span it (restart or expiry inside the load), and a native raw file with records in the load window whose
    activation (worker, pid) has no summary (the process stopped without exporting it)."""
    gaps, activations, upper = [], [], None
    transit, g7, valid = {}, {}, []
    seen = set()
    overlapping_reports = []
    for path in paths:
        try:
            with open(path) as handle:
                report = json.load(handle)
        except (OSError, ValueError) as exc:
            gaps.append(f"native summary unreadable: {path}: {exc.__class__.__name__}")
            continue
        ack = report.get("ack_latency") or {}
        if (report.get("schema") != NATIVE_SUMMARY_SCHEMA or report.get("final") is not True
                or ack.get("schema") != NATIVE_ACK_SCHEMA or ack.get("scope") != NATIVE_ACK_SCOPE
                or ack.get("cumulative") is not True or not ack.get("bucket_upper_bounds_ms")):
            gaps.append(f"native summary not a final {NATIVE_SUMMARY_SCHEMA} with buckets: {path}")
            continue
        started, expires = as_int(report.get("started_unix_ns")), as_int(report.get("expires_unix_ns"))
        end = activation_end(report, process_ends)
        activation = {"worker": report.get("worker"), "pid": report.get("pid"), "started_unix_ns": started,
                      "expires_unix_ns": expires, "expires_basis": "CONFIGURED_DEADLINE_NOT_COVERAGE",
                      "observed_end_unix_ns": as_int(report.get("observed_end_unix_ns")),
                      "last_metric_unix_ns": as_int(report.get("last_metric_unix_ns")),
                      "end_basis": end["basis"], "end_lower_ns": end["end_lower_ns"],
                      "end_upper_ns": end["end_upper_ns"],
                      "routes": len(ack.get("routes") or []),
                      "rejected_route_rows": ack.get("rejected_route_rows"),
                      "not_offered_after_stop": report.get("not_offered_after_stop")}
        if end.get("process_end"):
            activation["process_end"] = end["process_end"]
        # Exclusion needs only an upper bound of the end: the measured one, else the expiry (aggregates stop there).
        hi = end["end_upper_ns"] if end["end_upper_ns"] is not None else expires
        if from_ns is not None and to_ns is not None and started and hi and (hi <= from_ns or started >= to_ns):
            activation["overlaps_load"] = False
            activations.append(activation)
            continue
        activation["overlaps_load"] = True
        activations.append(activation)
        overlapping_reports.append((path, report))
        seen.add((report.get("worker"), report.get("pid")))
        if upper is None:
            upper = list(ack["bucket_upper_bounds_ms"])
        elif list(ack["bucket_upper_bounds_ms"]) != upper:
            gaps.append(f"native summary with a different histogram: {path}")
            continue
        if ack.get("rejected_route_rows"):
            gaps.append(f"{ack['rejected_route_rows']} native rows of routes beyond max_routes not measured ({path})")
        if end["basis"] == "NOT_MEASURED":
            gaps.append(f"native activation {report.get('worker')}/{report.get('pid')} end not measured: no "
                        "observed_end_unix_ns and no process-end evidence (the configured expiry is not coverage)")
        if from_ns is not None and to_ns is not None and not (
                started and started <= from_ns and (end["end_upper_ns"] is None or end["end_upper_ns"] >= to_ns)):
            gaps.append(f"native activation {report.get('worker')}/{report.get('pid')} [{started}, "
                        f"{end['end_upper_ns']}] ({end['basis']}) does not span the load window (restart or trace "
                        "expiry inside the load)")
        valid.append((path, report, ack))
    # Slices are used only when every scored activation carries a complete, overflow-free slice export covering
    # every 60 s slice that intersects the load window; otherwise the cumulative block (as before), marked.
    plan, slice_reasons = slice_plan(valid, from_ns, to_ns)
    catch_up = {}
    for path, report, ack in valid:
        for item in ack.get("routes") or []:
            route = item.get("route") or {}
            feed, venue = str(route.get("feed")), str(route.get("provider")).upper()
            if isinstance(item.get("catch_up"), dict):  # schema_revision 1.1: report-only, whole activation
                catch_up.setdefault(f"{feed}|{venue}", CatchUp()).add_cumulative(item["catch_up"])
            if plan is not None:
                continue
            stream = item.get("stream") or {}
            try:
                block = transit.setdefault(f"{feed}|{venue}", Buckets(upper))
                block.add(((stream.get("applied") or {}).get("metrics") or {}).get("source_event_to_ack") or {})
                for key in (feed, f"{feed}|{venue}"):
                    for name, metric in NATIVE_G7.items():
                        merged = g7.setdefault(key, {}).setdefault(name, Buckets(upper))
                        for outcome in NATIVE_OUTCOMES:
                            merged.add(((stream.get(outcome) or {}).get("metrics") or {}).get(metric) or {})
            except ValueError as exc:
                gaps.append(f"native summary {path} route {feed}|{venue}: {exc}")
        if plan is None:
            continue
        for cell_path, key, cell in slice_cells(path, report, plan):
            feed = key.split("|", 1)[0]
            try:
                transit.setdefault(key, Buckets(upper)).add(
                    (cell.get("applied_source_event_to_ack") or {}).get("live") or {})
                catch_up.setdefault(key, CatchUp()).add_slice(cell)
                for g7_key in (feed, key):
                    for name, metric in NATIVE_G7.items():
                        g7.setdefault(g7_key, {}).setdefault(name, Buckets(upper)).add(cell.get(metric) or {})
            except ValueError as exc:
                gaps.append(f"native summary {cell_path} route {key}: {exc}")
    for raw in raw_activations:
        inside = (raw.get("first_start_ns") is not None and (to_ns is None or raw["first_start_ns"] <= to_ns)
                  and (from_ns is None or (raw.get("last_start_ns") or 0) >= from_ns))
        if inside and (raw.get("worker"), raw.get("pid")) not in seen:
            gaps.append(f"native activation {raw.get('worker')}/{raw.get('pid')} has records in the load window but "
                        "no final summary")
    overlapping = [a for a in activations if a.get("overlaps_load")]
    if not overlapping:
        gaps.append("no native summary overlaps the load window")
    scope = native_window_scope(activations, from_ns, to_ns)
    g10 = merge_integrity(overlapping_reports, from_ns, to_ns) if overlapping_reports else None
    slice_mode = None
    if plan is not None:
        slice_mode = {"mode": "LOAD_SLICES_SUPERSET", "slice_seconds": plan["slice_seconds"],
                      "covered_from_ns": plan["covered_from_ns"], "covered_to_ns": plan["covered_to_ns"],
                      "slices": len(plan["starts"]),
                      "distribution_scope": f"LOAD_SLICES_SUPERSET [{plan['covered_from_ns']}, "
                                            f"{plan['covered_to_ns']}) (every {plan['slice_seconds']} s slice "
                                            "intersecting the load window, edge slices in full)",
                      "pre_load_seconds_in_scope": round((from_ns - plan["covered_from_ns"]) / 1e9, 3),
                      "post_load_seconds_in_scope": round((plan["covered_to_ns"] - to_ns) / 1e9, 3)}
        if g10 is not None:
            g10 = slice_integrity(g10, valid, plan, slice_mode["distribution_scope"])
    elif slice_reasons:
        slice_mode = {"mode": "CUMULATIVE_FALLBACK", "slice_fallback_reasons": slice_reasons,
                      "distribution_scope": "PRE_LOAD+LOAD+POST_LOAD_NOT_SEPARABLE (slices unusable: cumulative "
                                            "fallback)"}
    if scope is not None and slice_mode is not None:
        scope = {**scope, **slice_mode}
        if slice_mode["mode"] == "LOAD_SLICES_SUPERSET":
            scope["basis"] = "NATIVE_SUMMARY ack_latency_slices (60 s epoch slices intersecting the load window)"
    result = {"files": len(paths), "activations": activations, "coverage_gaps": gaps, "window_scope": scope,
              "g10": g10, "transit": {}, "g7": {}}
    if slice_mode is not None:
        result["slice_scope"] = slice_mode
    for key in sorted(transit):
        feed = key.split("|", 1)[0]
        block = transit[key].block()
        block["verdict"] = transit_verdict(block, TRANSIT_GATES[feed]) if feed in TRANSIT_GATES else "NOT_GATED"
        block["basis"] = ("NATIVE_SUMMARY ack_latency_slices applied_source_event_to_ack.live" if plan is not None
                          else "NATIVE_SUMMARY applied.source_event_to_ack")
        if slice_mode is not None:
            block["distribution_scope"] = slice_mode["distribution_scope"]
        if key in catch_up:
            block["catch_up_report_only"] = catch_up[key].block(in_scope=plan is not None)
        result["transit"][key] = block
    for key in sorted(g7):
        block = {name: buckets.block() for name, buckets in sorted(g7[key].items())}
        if "|" not in key:
            block["verdict"] = {name: ("NO_DATA" if not block[name]["n"] else
                                       "INSIDE" if block[name]["p95"] <= limit else "OUTSIDE")
                                for name, limit in G7_GATES.items()}
        if slice_mode is not None:
            block["distribution_scope"] = slice_mode["distribution_scope"]
        result["g7"][key] = block
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("inputs", nargs="+", help="trace files or quoted globs")
    parser.add_argument("--skip-seconds", type=float, default=40.0,
                        help="drop the first N s after the first record (writer start-up); ignored with --from-ns")
    parser.add_argument("--from-ns", type=int)
    parser.add_argument("--to-ns", type=int)
    parser.add_argument("--native-summary", action="append", default=[],
                        help="native writer summary files or quoted globs (summary-native-*-<until>.json)")
    parser.add_argument("--process-end", action="append", default=[],
                        help="STARTED_NS:FINISHED_NS[:LAST_SEEN_NS] of a writer process run (pinned evidence, e.g. "
                             "the G11 probe's Docker StartedAt/FinishedAt and last poll that saw it)")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    process_ends = [parse_process_end(value) for value in args.process_end]
    paths = sorted({path for pattern in args.inputs for path in (glob.glob(pattern) or [pattern])})
    files, records = load(paths, fields=FIELDS)
    report = {"schema": "qdl.p184.trace-feed-transit.v1", "files": len(files), "records": len(records),
              "dropped_at_export_total": sum(f["dropped_at_export"] or 0 for f in files),
              "stop_reasons": dict(collections.Counter(f["stop_reason"] for f in files)),
              "files_by_kind": files_by_kind(files),
              **summarise(records, skip_seconds=args.skip_seconds, from_ns=args.from_ns, to_ns=args.to_ns)}
    if args.native_summary:
        summaries = sorted({p for pattern in args.native_summary for p in glob.glob(pattern)})
        native_raw = [p for p in paths if Path(p).name.startswith("delivery-" + NATIVE_WORKER_PREFIX)]
        window = report.get("window") or {}
        report["native_summary"] = native_summary(
            summaries, from_ns=args.from_ns if args.from_ns is not None else window.get("from_ns"),
            to_ns=args.to_ns if args.to_ns is not None else window.get("to_ns"),
            raw_activations=native_raw_activations(native_raw), process_ends=process_ends)
    text = json.dumps(report, indent=1, sort_keys=True)
    if args.out:
        with open(args.out, "x") as handle:
            handle.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

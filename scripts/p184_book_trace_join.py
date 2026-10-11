"""Bounded, offline BOOK exact-event diagnostics; never delivery certification."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
import os
import re

MAX_FILES = 16
MAX_BYTES = 32 * 1024 * 1024
MAX_LINE_BYTES = 64 * 1024
MAX_ROWS = 20000
MAX_TRACE_ROWS = 4096
BOOK = {"BOOK_SNAPSHOT", "BOOK_DELTA"}
BOUNDARY = "DURABLE_READ_COMMITTED_OBSERVATION_NOT_COMMIT_CLOCK"
CLOCKS = {"source_event_time_ns": "source_event_ns",
          "envelope_received_at_ns": "received_at_ns",
          "envelope_normalized_at_ns": "normalized_at_ns",
          "envelope_published_at_ns": "published_at_ns"}


def uint(value, positive=False):
    if type(value) is not int or not (int(positive) <= value < 2**128):
        raise ValueError("invalid unsigned integer")
    return value


def digest(value):
    if type(value) is not str or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("invalid SHA256")
    return value


def bounded(items, limit):
    for index, item in enumerate(items):
        if index >= limit:
            raise ValueError("row/file limit exceeded")
        if type(item) is not dict:
            raise ValueError("expected object")
        yield item


def canonical_key(row):
    source = row["identity"]["source_id"]
    if type(source) is not str or not 1 <= len(source) <= 256:
        raise ValueError("invalid source ID")
    return (digest(row["event_id_sha256"]), uint(row["offset"]),
            row["feed"].upper(), hashlib.sha256(source.encode()).hexdigest())


def trace_key(row):
    if row.get("sdk_receive_basis") != "SDK_STREAM_RETURN":
        raise ValueError("not a stream receive")
    if row.get("event_id_hash_basis") != "SHA256_PROTOBUF_EVENT_ID_BYTES":
        raise ValueError("wrong event hash basis")
    return (digest(row["event_id_sha256"]), uint(row["watermark_offset"]),
            row["feed"], digest(row["source_id_sha256"]))


def validate(canonical, trace):
    identity = canonical["identity"]
    market = "USD_M" if (identity["venue"], identity["market"]) == ("BINANCE", "USDM") else identity["market"]
    expected = ":".join((identity["venue"], market, identity["native_symbol"]))
    if trace["canonical_instrument_id"] != expected:
        raise ValueError("instrument_identity")
    generation = uint(identity["connection_generation"])
    if uint(trace["source_generation"]) != generation:
        raise ValueError("generation")
    sequence = re.fullmatch(r"g([0-9]{1,39}):([0-9]{1,39})", identity["source_sequence"])
    if (not sequence or int(sequence[1]) != generation
            or uint(trace["native_sequence"]) != int(sequence[2])):
        raise ValueError("native_sequence")
    for target, source in CLOCKS.items():
        if uint(trace[target], True) != uint(canonical[source], True):
            raise ValueError(target)
    if canonical.get("boundary") != BOUNDARY:
        raise ValueError("observer_boundary")
    uint(canonical["partition"])
    if not isinstance(canonical["topic"], str) or not canonical["topic"]:
        raise ValueError("topic")
    uint(canonical["observer_receipt_wall_ns"], True)
    sdk = uint(trace["sdk_receive_monotonic_ns"], True)
    writer = uint(trace["writer_started_monotonic_ns"], True)
    uint(trace["sdk_receive_wall_ns"], True)
    if writer < sdk:
        raise ValueError("monotonic_order")


def metrics(canonical, trace):
    sdk = trace["sdk_receive_monotonic_ns"]
    writer = trace["writer_started_monotonic_ns"]
    reply = uint(trace["redis_reply_monotonic_ns"], True)
    wall = uint(trace["redis_reply_wall_ns"], True)
    if reply < writer:
        raise ValueError("monotonic_order")
    if wall < trace["source_event_time_ns"]:
        raise ValueError("negative_event_to_reply")
    return {"sdk_to_writer_ms": (writer - sdk) / 1e6,
            "writer_to_redis_reply_ms": (reply - writer) / 1e6,
            "sdk_to_redis_reply_ms": (reply - sdk) / 1e6,
            "event_to_redis_reply_ms": (wall - trace["source_event_time_ns"]) / 1e6,
            "observer_to_sdk_signed_ms": (trace["sdk_receive_wall_ns"] -
                                          canonical["observer_receipt_wall_ns"]) / 1e6}


def stats(values):
    values = sorted(values)
    return {"n": len(values), "negative": sum(v < 0 for v in values),
            "min": values[0], "max": values[-1],
            **{name: (values[max(0, math.ceil(len(values) * q) - 1)]
                      if len(values) >= minimum else None)
               for name, q, minimum in (("p50", .5, 1), ("p95", .95, 20), ("p99", .99, 100))}}


def summarize(canonical_rows, trace_reports):
    """Return JSON-safe sampled diagnostics without I/O or input mutation.

    Invalid report structure and resource bounds raise ValueError. Invalid event
    rows are counted and excluded. Duplicate canonical keys are refused, including
    identical rows: trace records cannot disambiguate topic/partition provenance.
    """
    counts = Counter({key: 0 for key in (
        "canonical_observations", "invalid_canonical_rows", "non_book_canonical_rows",
        "trace_records", "non_book_trace_records", "no_exact_join_key",
        "unmatched_trace_records", "ambiguous_trace_records", "validation_failures",
        "exact_validated_matches", "matched_EVAL1_APPLIED", "rejected_records",
        "other_non_success_records", "dropped_at_export",
        "in_flight_at_export_not_recorded")})
    index = defaultdict(list)
    for row in bounded(canonical_rows, MAX_ROWS):
        if row.get("kind") != "canonical_observation":
            continue
        counts["canonical_observations"] += 1
        try:
            key = canonical_key(row)
            if key[2] not in BOOK:
                counts["non_book_canonical_rows"] += 1
                continue
            index[key].append(row)
        except (KeyError, TypeError, AttributeError, ValueError):
            counts["invalid_canonical_rows"] += 1
    joined, failures, categories = [], Counter(), Counter()
    grouped = defaultdict(lambda: defaultdict(list))
    matched = set()
    for report in bounded(trace_reports, MAX_FILES):
        rows = report.get("records")
        if type(rows) is not list or len(rows) > MAX_TRACE_ROWS:
            raise ValueError("invalid trace records")
        if (uint(report["recorded"]) != len(rows)
                or not len(rows) <= uint(report["max_records"]) <= MAX_TRACE_ROWS
                or report["clock_basis"] != {"wall": "UNIX_TIME_NS", "monotonic": "PROCESS_MONOTONIC_NS"}):
            raise ValueError("invalid trace report metadata")
        for field in ("dropped_at_export", "in_flight_at_export_not_recorded"):
            counts[field] += uint(report[field])
        for trace in bounded(rows, MAX_TRACE_ROWS):
            counts["trace_records"] += 1
            if trace.get("feed") not in BOOK:
                counts["non_book_trace_records"] += 1
                continue
            success = (type(trace.get("redis_eval_reply")) is int
                       and trace["redis_eval_reply"] == 1
                       and trace.get("cas_outcome") == "APPLIED"
                       and trace.get("outcome") == "APPLIED" and trace.get("failure") == "NONE")
            rejected = trace.get("cas_outcome") == "REJECTED" or trace.get("outcome") == "REJECTED"
            category = "EVAL1_APPLIED" if success else "REJECTED" if rejected else "OTHER_NON_SUCCESS"
            categories[category] += 1
            if not success:
                counts["rejected_records" if rejected else "other_non_success_records"] += 1
            try:
                key = trace_key(trace)
            except (KeyError, TypeError, ValueError):
                counts["no_exact_join_key"] += 1
                continue
            candidates = index.get(key, [])
            if not candidates:
                counts["unmatched_trace_records"] += 1
                continue
            if len(candidates) != 1:
                counts["ambiguous_trace_records"] += 1
                continue
            canonical = candidates[0]
            try:
                validate(canonical, trace)
                measurements = metrics(canonical, trace) if success else {}
            except (KeyError, TypeError, ValueError) as error:
                counts["validation_failures"] += 1
                failures[str(error)[:96]] += 1
                continue
            counts["exact_validated_matches"] += 1
            matched.add(key)
            if success:
                counts["matched_EVAL1_APPLIED"] += 1
                group = trace["canonical_instrument_id"] + " " + trace["feed"]
                for name, value in measurements.items():
                    grouped[group][name].append(value)
            joined.append({"event_id_sha256": key[0], "watermark_offset": key[1],
                           "feed": key[2], "source_id_sha256": key[3],
                           "canonical_instrument_id": trace["canonical_instrument_id"],
                           "topic": canonical["topic"], "partition": canonical["partition"],
                           "category": category, "metrics_ms": measurements})
    counts["canonical_keys_without_validated_match"] = len(set(index) - matched)
    counts["ambiguous_canonical_keys"] = sum(len(rows) > 1 for rows in index.values())
    invalid = any(counts[key] for key in ("validation_failures", "invalid_canonical_rows",
                                         "ambiguous_canonical_keys", "ambiguous_trace_records"))
    return {"status": "INVALID_EVIDENCE" if invalid else "PARTIAL_SAME_EVENT_MEASUREMENT",
            "counts": dict(counts),
            "categories": dict(categories), "validation_failure_reasons": dict(failures),
            "groups": {g: {"metrics_ms": {k: stats(v) for k, v in m.items()}}
                       for g, m in grouped.items()}, "joined_events": joined,
            "canonical_commit_ns": "NOT_MEASURED", "sdk_ack": "NOT_MEASURED",
            "cursor_persistence": "NOT_MEASURED", "full_delivery": "NOT_ESTABLISHED",
            "notes": ["Monotonic SDK-to-writer/Redis reply; wall event-to-Redis reply.",
                      "Observer-to-SDK is SIGNED durable observation timing, not commit latency.",
                      "Sampled/capped exports: unmatched rows are not proof of loss.",
                      "Success metrics cover only exact validated EVAL1 APPLIED failure NONE attempts."]}


def read_bounded(path):
    with open(path, "rb") as source:
        data = source.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("file byte limit exceeded")
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical", required=True)
    parser.add_argument("--trace", action="append", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    if len(args.trace) > MAX_FILES or len(set(map(os.path.realpath, args.trace))) != len(args.trace):
        parser.error("too many or duplicate trace files")
    try:
        raw = read_bounded(args.canonical)
        lines = raw.splitlines()
        if len(lines) > MAX_ROWS or any(len(line) > MAX_LINE_BYTES for line in lines):
            raise ValueError("canonical line/row limit exceeded")
        # Refuse partial trailing JSON while the collector is still writing.
        canonical = [json.loads(line) for line in lines]
        report = summarize(canonical, (json.loads(read_bounded(p)) for p in args.trace))
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(report, output, allow_nan=False, separators=(",", ":"))
            output.write("\n")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"BOOK trace analysis refused: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Synthetic test-only BOOK trace fixtures; no runtime or provider calls."""
import copy
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "book_trace_join", Path(__file__).resolve().parents[1] / "scripts/p184_book_trace_join.py")
join = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(join)


def fixture():
    canonical = {"kind": "canonical_observation", "topic": "md.canonical.v2",
                 "partition": 3, "offset": 42, "event_id_sha256": "a" * 64,
                 "feed": "book_delta", "source_event_ns": 1000000,
                 "received_at_ns": 2000000, "normalized_at_ns": 3000000,
                 "published_at_ns": 4000000, "observer_receipt_wall_ns": 10000000,
                 "boundary": join.BOUNDARY,
                 "identity": {"venue": "BINANCE", "market": "USDM",
                              "native_symbol": "BTCUSDT", "source_id": "test-only-source",
                              "connection_generation": 7, "source_sequence": "g7:99"}}
    trace = {"canonical_instrument_id": "BINANCE:USD_M:BTCUSDT",
             "source_id_sha256": hashlib.sha256(b"test-only-source").hexdigest(),
             "feed": "BOOK_DELTA", "watermark_offset": 42, "source_generation": 7,
             "native_sequence": 99, "event_id_sha256": "a" * 64,
             "event_id_hash_basis": "SHA256_PROTOBUF_EVENT_ID_BYTES",
             "sdk_receive_basis": "SDK_STREAM_RETURN", "sdk_receive_wall_ns": 9000000,
             "sdk_receive_monotonic_ns": 100, "writer_started_monotonic_ns": 1000100,
             "redis_reply_monotonic_ns": 3000100, "redis_reply_wall_ns": 12000000,
             "redis_eval_reply": 1, "cas_outcome": "APPLIED", "outcome": "APPLIED",
             "failure": "NONE"}
    trace.update({target: canonical[source] for target, source in join.CLOCKS.items()})
    report = {"records": [trace], "recorded": 1, "max_records": 4096,
              "dropped_at_export": 0, "in_flight_at_export_not_recorded": 0,
              "clock_basis": {"wall": "UNIX_TIME_NS", "monotonic": "PROCESS_MONOTONIC_NS"}}
    return canonical, trace, report


class BookTraceTests(unittest.TestCase):
    def test_exact_signed_metrics_and_purity(self):
        c, t, r = fixture()
        before = copy.deepcopy((c, r))
        result = join.summarize([c], [r])
        self.assertEqual((c, r), before)
        self.assertEqual(result["counts"]["matched_EVAL1_APPLIED"], 1)
        self.assertEqual(result["joined_events"][0]["metrics_ms"], {
            "sdk_to_writer_ms": 1, "sdk_to_redis_reply_ms": 3,
            "writer_to_redis_reply_ms": 2,
            "event_to_redis_reply_ms": 11, "observer_to_sdk_signed_ms": -1})
        self.assertEqual(result["sdk_ack"], "NOT_MEASURED")
        self.assertEqual(result["cursor_persistence"], "NOT_MEASURED")
        self.assertEqual(result["full_delivery"], "NOT_ESTABLISHED")
        self.assertNotIn("PASS", result["status"])

    def test_wrong_identity_generation_sequence_and_clocks(self):
        changes = {"canonical_instrument_id": "BINANCE:USD_M:ETHUSDT",
                   "source_generation": 8, "native_sequence": 100,
                   **{key: 123 for key in join.CLOCKS},
                   "sdk_receive_monotonic_ns": 2000100,
                   "redis_reply_monotonic_ns": 1, "redis_reply_wall_ns": 1}
        for field, value in changes.items():
            with self.subTest(field=field):
                c, t, r = fixture()
                t[field] = value
                result = join.summarize([c], [r])
                self.assertEqual(result["counts"]["validation_failures"], 1)
                self.assertEqual(result["status"], "INVALID_EVIDENCE")
                self.assertFalse(result["groups"])

    def test_wrong_source_offset_feed_event_never_fuzzy_joins(self):
        for field, value in (("source_id_sha256", "b" * 64), ("watermark_offset", 43),
                             ("event_id_sha256", "b" * 64), ("feed", "BOOK_SNAPSHOT")):
            with self.subTest(field=field):
                c, t, r = fixture()
                t[field] = value
                result = join.summarize([c], [r])
                self.assertEqual(result["counts"]["unmatched_trace_records"], 1)
                self.assertEqual(result["status"], "PARTIAL_SAME_EVENT_MEASUREMENT")
                self.assertFalse(result["joined_events"])

    def test_ambiguous_partitions_topics_and_duplicates_refused(self):
        for field, value in (("partition", 4), ("topic", "other"), ("partition", 3)):
            with self.subTest(field=field, value=value):
                c, t, r = fixture()
                other = copy.deepcopy(c)
                other[field] = value
                result = join.summarize([c, other], [r])
                self.assertEqual(result["counts"]["ambiguous_trace_records"], 1)
                self.assertEqual(result["status"], "INVALID_EVIDENCE")
                self.assertFalse(result["joined_events"])

    def test_only_eval1_applied_none_succeeds(self):
        for field, value in (("redis_eval_reply", 0), ("redis_eval_reply", True),
                             ("failure", "TIMEOUT"), ("outcome", "DUPLICATE"),
                             ("cas_outcome", "REJECTED")):
            with self.subTest(field=field):
                c, t, r = fixture()
                t[field] = value
                result = join.summarize([c], [r])
                self.assertEqual(result["counts"]["matched_EVAL1_APPLIED"], 0)
                self.assertEqual(result["counts"]["exact_validated_matches"], 1)
                self.assertFalse(result["groups"])

    def test_reject_unmatched_and_export_omissions_separate(self):
        c, t, r = fixture()
        t.update(cas_outcome="REJECTED", outcome="REJECTED", redis_eval_reply=0,
                 watermark_offset=43)
        r.update(dropped_at_export=2, in_flight_at_export_not_recorded=3)
        counts = join.summarize([c], [r])["counts"]
        for key, value in (("rejected_records", 1), ("unmatched_trace_records", 1),
                           ("dropped_at_export", 2), ("in_flight_at_export_not_recorded", 3)):
            self.assertEqual(counts[key], value)

    def test_missing_hash_or_wrong_basis(self):
        for field, value in (("event_id_sha256", "NOT_MEASURED"),
                             ("event_id_hash_basis", "WRONG"),
                             ("sdk_receive_basis", "SDK_QUERY_RETURN"),
                             ("watermark_offset", True)):
            c, t, r = fixture()
            t[field] = value
            self.assertEqual(join.summarize([c], [r])["counts"]["no_exact_join_key"], 1)

    def test_observer_and_native_generation_validation(self):
        for field, value in (("boundary", "COMMIT"), ("observer_receipt_wall_ns", 0)):
            c, t, r = fixture()
            c[field] = value
            self.assertEqual(join.summarize([c], [r])["counts"]["validation_failures"], 1)
        c, t, r = fixture()
        c["identity"]["source_sequence"] = "g8:99"
        self.assertEqual(join.summarize([c], [r])["counts"]["validation_failures"], 1)

    def test_empty_metadata_and_non_book(self):
        result = join.summarize([{"kind": "scope"}], [])
        self.assertFalse(result["groups"])
        c, t, r = fixture()
        t["feed"] = "MARK_INDEX_PRICE"
        self.assertEqual(join.summarize([c], [r])["counts"]["non_book_trace_records"], 1)

    def test_percentile_sample_thresholds(self):
        for n in (1, 19, 20, 99, 100):
            with self.subTest(n=n):
                result = join.stats(list(range(1, n + 1)))
                self.assertEqual(result["p95"], None if n < 20 else math.ceil(n * .95))
                self.assertEqual(result["p99"], None if n < 100 else 99)

    def test_invalid_canonical_evidence_status(self):
        c, t, r = fixture()
        c["event_id_sha256"] = "invalid"
        result = join.summarize([c], [r])
        self.assertEqual(result["counts"]["invalid_canonical_rows"], 1)
        self.assertEqual(result["status"], "INVALID_EVIDENCE")

    def test_writer_reply_percentile_uses_paired_durations(self):
        c, t, r = fixture()
        r["records"] = []
        for i in range(20):
            row = dict(t)
            row["writer_started_monotonic_ns"] = 100 + i * 1000000
            row["redis_reply_monotonic_ns"] = 100 + 20000000
            r["records"].append(row)
        r["recorded"] = 20
        result = join.summarize([c], [r])
        metrics = next(iter(result["groups"].values()))["metrics_ms"]
        self.assertEqual(metrics["writer_to_redis_reply_ms"]["p95"], 19)
        self.assertNotEqual(metrics["writer_to_redis_reply_ms"]["p95"],
                            metrics["sdk_to_redis_reply_ms"]["p95"] -
                            metrics["sdk_to_writer_ms"]["p95"])

    def test_pure_bounds_and_report_metadata(self):
        c, t, r = fixture()
        for name, args in (("MAX_ROWS", ([c, c], [])), ("MAX_FILES", ([], [r, r])),
                           ("MAX_TRACE_ROWS", ([c], [{**r, "records": [t, t]}]))):
            with self.subTest(name=name), patch.object(join, name, 1):
                with self.assertRaises(ValueError):
                    join.summarize(*args)
        r["recorded"] = 2
        with self.assertRaises(ValueError):
            join.summarize([c], [r])

    def test_cli_exclusive_output_and_repeat_trace(self):
        c, t, r = fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical, trace, trace2, output = (root / name for name in ("c", "t", "t2", "o"))
            canonical.write_text(json.dumps(c) + "\n")
            trace.write_text(json.dumps(r))
            trace2.write_text(json.dumps({**r, "records": [], "recorded": 0}))
            args = ["--canonical", str(canonical), "--trace", str(trace),
                    "--trace", str(trace2), "--output", str(output)]
            self.assertEqual(join.main(args), 0)
            original = output.read_bytes()
            with self.assertRaises(SystemExit) as error:
                join.main(args)
            self.assertEqual(error.exception.code, 2)
            self.assertEqual(output.read_bytes(), original)

    def test_cli_malformed_partial_and_byte_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "partial"
            path.write_text('{"kind":')
            with patch.object(join, "MAX_BYTES", 2), self.assertRaises(ValueError):
                join.read_bounded(path)
            with self.assertRaises(SystemExit):
                join.main(["--canonical", str(path), "--trace", str(path),
                           "--output", str(Path(directory) / "out")])
            self.assertFalse((Path(directory) / "out").exists())


if __name__ == "__main__":
    unittest.main()

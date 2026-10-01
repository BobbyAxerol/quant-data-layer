"""Synthetic test-only fixtures; not provider acceptance evidence."""
import json
from pathlib import Path
import tempfile
import unittest

from scripts.probe_exact_market_lineage import Evidence, exact_view, trade_state, exact_join, newer_before_request


class ExactMarketLineageTests(unittest.TestCase):
    def test_exact_view_preserves_flags_and_original_clocks_without_secrets(self):
        row = {"quality": {"flags": ["INDEX_STALE"], "execution_eligible": False},
               "observed_at_ns": 123, "received_at_ns": 124,
               "watermark_offset": 9, "contract": {"connection_generation": 7},
               "cursor": "secret", "payload": {"price": "42"}}
        result = exact_view({"data": row, "cursor": "secret"})
        self.assertEqual(result["quality"], row["quality"])
        self.assertEqual(result["observed_at_ns"], 123)
        self.assertEqual(result["contract"], row["contract"])
        self.assertNotIn("secret", json.dumps(result))
        self.assertNotIn("payload", result)

    def test_missing_raw_never_claims_quiet_from_session(self):
        self.assertEqual(trade_state(raw_count=0, canonical_count=0,
            query_matches=False, coverage_complete=True),
            "NO_TRADE_IN_CAPTURE_WINDOW_NOT_SESSION_PROOF")

    def test_incomplete_capture_fails_closed(self):
        self.assertEqual(trade_state(raw_count=12, canonical_count=12,
            query_matches=True, coverage_complete=False), "UNASSESSED_INCOMPLETE_CAPTURE")

    def test_distinguish_pipeline_stages(self):
        self.assertEqual(trade_state(raw_count=1, canonical_count=0,
            query_matches=False, coverage_complete=True), "RAW_PRESENT_CANONICAL_MISSING_IN_WINDOW")
        self.assertEqual(trade_state(raw_count=1, canonical_count=1,
            query_matches=False, coverage_complete=True), "CANONICAL_PRESENT_QUERY_JOIN_MISSING")
        self.assertEqual(trade_state(raw_count=1, canonical_count=1,
            query_matches=True, coverage_complete=True), "SOURCE_CANONICAL_QUERY_OBSERVED")

    def test_join_and_positive_lag_require_original_identity_and_pre_request_capture(self):
        import copy
        event = dict(instrument_uid="test-only", source_event_time_ns="10",
                     received_at_ns="11", correlation_id="capture", source_session_id="s",
                     connection_generation="7", authority_revision="1")
        selected = dict(event=event, feed="trade", partition=2, offset=4, captured_at_ns=20)
        query = dict(uid="test-only", feed="TRADE", request_started_at_ns=30,
                     exact_view=dict(observed_at_ns=10, received_at_ns=11, watermark_offset=4,
                                     contract=dict(correlation_id="capture")))
        newer = copy.deepcopy(selected)
        newer.update(offset=5, captured_at_ns=29)
        newer["event"]["received_at_ns"] = "12"
        self.assertTrue(exact_join(query, selected))
        query["exact_view"]["watermark_offset"] = 8
        self.assertTrue(exact_join(query, selected))
        query["exact_view"]["watermark_offset"] = 3
        self.assertFalse(exact_join(query, selected))
        query["exact_view"]["watermark_offset"] = 4
        self.assertTrue(newer_before_request(query, selected, newer))
        newer["captured_at_ns"] = 31
        self.assertFalse(newer_before_request(query, selected, newer))
        newer["captured_at_ns"] = 29
        newer["event"]["connection_generation"] = "8"
        self.assertFalse(newer_before_request(query, selected, newer))
        query["exact_view"]["contract"]["correlation_id"] = "different"
        self.assertFalse(exact_join(query, selected))

    def test_bounded_evidence_exclusive_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "test-only.jsonl"
            writer = Evidence(path, max_bytes=200)
            writer.put("test", test_provenance=True)
            with self.assertRaises(RuntimeError):
                writer.put("test", value="x" * 200)
            writer.close()
            self.assertLessEqual(path.stat().st_size, 200)
            with self.assertRaises(FileExistsError):
                Evidence(path)


if __name__ == "__main__":
    unittest.main()

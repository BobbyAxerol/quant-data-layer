"""v2.2.6 class fix: EXECUTION manifest quotas are derived from the declared products and gated offline."""
from copy import deepcopy
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.consumer.quota_model import model_manifest, quota_shortfall
from scripts.check_consumer_quota_model import main as gate_main

ROOT = Path(__file__).resolve().parents[1]
PAPER = ROOT / "consumers/stable/trading-system-paper.yaml"


def _manifest(feeds: dict[str, int], rpm: int, streams: int) -> dict:
    rows = [{"feed": feed, "consumer_grade": "EXECUTION"} for feed, n in feeds.items() for _ in range(n)]
    return {"spec": {"requirements": rows, "quotas": {"requests_per_minute": rpm, "max_streams": streams}}}


# r11 (64 V2 products; its V1-routed VN TRADE row is not counted) and the failed r12 (stage A at 4,500 rpm).
R11 = {"TRADE": 10, "QUOTE": 11, "BAR": 10, "MARK_INDEX_PRICE": 11, "BOOK_SNAPSHOT": 11, "BOOK_DELTA": 11}
STAGE_A = {**R11, "QUOTE": 31, "MARK_INDEX_PRICE": 31}


class ConsumerQuotaModelTests(unittest.TestCase):
    def test_model_is_calibrated_against_the_measured_r11_writer(self):
        model = model_manifest(_manifest(R11, 3000, 77))
        # measured 2,015-2,034 requests/min on the shared quota counter (2026-11-13 08:27-08:29Z)
        self.assertGreaterEqual(model.steady_requests_per_minute, 2034)
        self.assertLessEqual(model.steady_requests_per_minute, int(2034 * 1.15))
        # r11's 3000 / 77 is ~1.5x the MEASURED load; against the conservative model the 1.5x floor is 4000 / 100.
        self.assertEqual((model.required_requests_per_minute, model.required_max_streams), (4000, 100))

    def test_the_failed_r12_quota_is_refused(self):
        short = quota_shortfall(_manifest(STAGE_A, 4500, 150))
        self.assertEqual(set(short["shortfall"]), {"requests_per_minute", "max_streams"})
        self.assertGreater(short["shortfall"]["requests_per_minute"]["required"], 6000)
        self.assertEqual(quota_shortfall(_manifest(STAGE_A, 10_000, 160)), {})

    def test_every_v2_product_holds_one_stream_and_r11_quota_is_exactly_the_floor(self):
        self.assertEqual(model_manifest(_manifest(R11, 3000, 77)).streams, 64)
        self.assertEqual(model_manifest(_manifest(R11, 3000, 77)).required_max_streams, 100)
        model = model_manifest(_manifest(STAGE_A, 10_000, 150))
        self.assertEqual(model.streams, 104)
        self.assertEqual((model.required_requests_per_minute, model.required_max_streams), (10_000, 160))
        self.assertIn("max_streams", quota_shortfall(_manifest(STAGE_A, 10_000, 150))["shortfall"])

    def test_v1_routed_vn_rows_are_not_counted(self):
        doc = _manifest(R11, 3000, 77)
        doc["spec"]["requirements"].append({"feed": "TRADE", "consumer_grade": "EXECUTION", "source_policy_id": "vn_primary_v2"})
        self.assertEqual(model_manifest(doc).streams, 64)

    def test_unknown_feed_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "no request rate"):
            model_manifest(_manifest({"FUNDING_RATE": 1}, 10, 10))

    def test_committed_paper_manifest_passes_the_gate(self):
        doc = yaml.safe_load(PAPER.read_text())
        self.assertEqual(quota_shortfall(doc), {})

    def test_gate_cli_fails_a_short_manifest_and_passes_the_committed_ones(self):
        with tempfile.TemporaryDirectory() as raw:
            short = Path(raw) / "short.yaml"
            doc = yaml.safe_load(PAPER.read_text())
            bad = deepcopy(doc)
            bad["spec"]["quotas"]["requests_per_minute"] = 4500
            short.write_text(yaml.safe_dump(bad))
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(gate_main([str(short)]), 1)
            self.assertEqual(json.loads(out.getvalue())["status"], "FAIL")
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(gate_main([]), 0)
        self.assertEqual(json.loads(out.getvalue())["status"], "PASS")


if __name__ == "__main__":
    unittest.main()

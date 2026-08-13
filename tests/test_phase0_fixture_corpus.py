import json
import unittest
from pathlib import Path


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "phase0"
REQUIRED_SCENARIOS = {
    "trade",
    "closed_candle",
    "duplicate",
    "out_of_order",
    "candle_page",
    "cursor_overlap",
    "cache_regression",
    "book_snapshot",
    "book_keepalive",
    "book_maintenance_reset",
    "book_gap",
    "quote",
    "market_closed",
    "stale",
    "invalid_json_shape",
    "missing_identity",
    "invalid_decimal",
    "unknown_event",
}


class Phase0FixtureCorpusTests(unittest.TestCase):
    def test_manifest_covers_required_provider_and_failure_cases(self):
        manifest = json.loads((FIXTURE_ROOT / "manifest.json").read_text())
        providers = {item["provider"] for item in manifest["fixtures"]}
        scenarios = {scenario for item in manifest["fixtures"] for scenario in item["scenarios"]}

        self.assertEqual(manifest["schema_version"], 1)
        self.assertTrue({"binance", "okx", "dnse_vnstock", "multi"}.issubset(providers))
        self.assertTrue(REQUIRED_SCENARIOS.issubset(scenarios))

    def test_every_manifest_file_is_valid_json(self):
        manifest = json.loads((FIXTURE_ROOT / "manifest.json").read_text())
        for item in manifest["fixtures"]:
            with self.subTest(file=item["file"]):
                payload = json.loads((FIXTURE_ROOT / item["file"]).read_text())
                self.assertIsNotNone(payload)

    def test_okx_book_sequence_has_keepalive_reset_and_real_gap(self):
        frames = json.loads((FIXTURE_ROOT / "okx_events.json").read_text())["book_frames"]
        snapshot, keepalive, reset, gap = frames

        self.assertEqual(snapshot["action"], "snapshot")
        self.assertEqual(keepalive["data"][0]["prevSeqId"], keepalive["data"][0]["seqId"])
        self.assertEqual(reset["data"][0]["prevSeqId"], snapshot["data"][0]["seqId"])
        self.assertLess(reset["data"][0]["seqId"], reset["data"][0]["prevSeqId"])
        self.assertNotEqual(gap["data"][0]["prevSeqId"], reset["data"][0]["seqId"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import copy
import unittest
from pathlib import Path

from scripts.phase90b_bridge_parity import validate_sample, validate_window
from qdl.runtime.canary_source import CanarySourceCatalog


class Phase90BBridgeParityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.binding = CanarySourceCatalog.load(
            Path("config/phase7/canary-sources.yaml")
        ).bindings[0]

    def sample(self):
        row = [
            1_800_000_000_000, "100.10", "101.20", "99.30", "100.40",
            "12.50", 1_800_000_059_999, "1255.00", 42, "6.25", "627.50",
            "0",
        ]
        decimal = lambda value: {"coefficient": "1", "scale": 0, "source_text": value}
        item = {
            "instrument_uid": self.binding.instrument.instrument_uid,
            "instrument_id": self.binding.instrument.instrument_id,
            "feed": "BAR",
            "interval": "1m",
            "payload": {
                "feed": "BAR", "interval": "1m",
                "open_time_ns": row[0] * 1_000_000,
                "close_time_ns": row[6] * 1_000_000,
                "open": decimal(row[1]), "high": decimal(row[2]),
                "low": decimal(row[3]), "close": decimal(row[4]),
                "volume": decimal(row[5]), "trade_count": row[8],
                "lifecycle": "FINAL",
            },
            "source": {
                "source_id": self.binding.source_id,
                "source_role": self.binding.source_role,
                "authoritative": self.binding.authoritative,
            },
            "quality": {
                "policy_id": self.binding.source_policy_id,
                "complete": True,
                "gap_open": False,
                "execution_eligible": False,
            },
        }
        v1 = {
            "provider": "binance", "market": "usdm", "symbol": "BTCUSDT",
            "requested_interval": "1m", "data": [row],
        }
        v2 = {
            "schema": "qdl.marketdata.warmup.v2", "count": 1,
            "watermark_offset": 1, "data": [item],
        }
        return v1, v2

    def test_exact_provider_bar_passes(self):
        v1, v2 = self.sample()
        result = validate_sample(v1, v2, self.binding)
        self.assertEqual(result["count"], 1)
        validate_window(result, result)

    def test_decimal_mismatch_fails(self):
        v1, v2 = self.sample()
        v2["data"][0]["payload"]["close"]["source_text"] = "100.41"
        with self.assertRaisesRegex(AssertionError, "close"):
            validate_sample(v1, v2, self.binding)

    def test_non_final_and_execution_eligible_fail(self):
        v1, v2 = self.sample()
        v2["data"][0]["payload"]["lifecycle"] = "IN_PROGRESS"
        v2["data"][0]["quality"]["execution_eligible"] = True
        with self.assertRaisesRegex(AssertionError, "final"):
            validate_sample(v1, v2, self.binding)

    def test_duplicate_open_time_fails(self):
        v1, v2 = self.sample()
        v2["data"].append(copy.deepcopy(v2["data"][0]))
        v2["count"] = 2
        v2["watermark_offset"] = 2
        with self.assertRaisesRegex(AssertionError, "duplicated"):
            validate_sample(v1, v2, self.binding)

    def test_certification_harness_is_rootless_host_portable(self):
        for name in (
            "scripts/phase73_public_beta_certification.sh",
            "scripts/phase90b_isolated_beta_certification.sh",
        ):
            script = Path(name).read_text()
            self.assertIn('CERT_UID="${QDL_CERT_UID:-$(id -u)}"', script)
            self.assertIn('CERT_GID="${QDL_CERT_GID:-$(id -g)}"', script)
            self.assertNotIn("chown 10001:10001", script)

    def test_window_rejects_regression_and_unbounded_growth(self):
        first = {"watermark_offset": 5, "last_open_time_ns": 100}
        with self.assertRaisesRegex(AssertionError, "outside"):
            validate_window(first, {"watermark_offset": 4, "last_open_time_ns": 100})
        with self.assertRaisesRegex(AssertionError, "outside"):
            validate_window(first, {"watermark_offset": 7, "last_open_time_ns": 100})
        with self.assertRaisesRegex(AssertionError, "backwards"):
            validate_window(first, {"watermark_offset": 6, "last_open_time_ns": 99})


if __name__ == "__main__":
    unittest.main()

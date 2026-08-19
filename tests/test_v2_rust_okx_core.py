from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml

from qdl.canonical.market import canonicalize_okx_bar, canonicalize_okx_bbo
from qdl.canonical.trade import TradeContext
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2


ROOT = Path(__file__).resolve().parents[1]


class V2RustOkxCanonicalParityTest(unittest.TestCase):
    def fixture(self, name: str):
        payload = json.loads(
            (ROOT / "tests/fixtures/phase2" / name).read_text()
        )
        return payload, TradeContext(**payload["context"])

    def test_python_oracle_matches_frozen_okx_bbo_and_bar_bytes(self):
        for fixture_name, golden_name, canonicalize in (
            ("okx_bbo.json", "okx-swap-bbo.bin", canonicalize_okx_bbo),
            ("okx_bar.json", "okx-swap-bar.bin", canonicalize_okx_bar),
        ):
            with self.subTest(fixture=fixture_name):
                fixture, context = self.fixture(fixture_name)
                actual = canonicalize(
                    fixture["raw"], context
                ).SerializeToString(deterministic=True)
                expected = (
                    ROOT / "contracts/golden/phase2" / golden_name
                ).read_bytes()
                self.assertEqual(actual, expected)

    def test_bbo_is_replace_only_quote_and_bar_has_explicit_lifecycle(self):
        fixture, context = self.fixture("okx_bbo.json")
        bbo = canonicalize_okx_bbo(fixture["raw"], context)
        self.assertEqual(bbo.WhichOneof("payload"), "quote")
        self.assertEqual(bbo.quote.level, 1)
        self.assertEqual(bbo.source_sequence, "817263")

        fixture, context = self.fixture("okx_bar.json")
        bar = canonicalize_okx_bar(fixture["raw"], context)
        self.assertEqual(bar.WhichOneof("payload"), "bar")
        self.assertTrue(bar.bar.is_final)
        self.assertEqual(
            bar.bar.lifecycle, market_data_pb2.BAR_LIFECYCLE_FINAL
        )
        self.assertEqual(bar.bar.interval, "1m")
        self.assertIn(
            common_pb2.QUALITY_FLAG_FIELD_MISSING, bar.quality_flags
        )

    def test_malformed_okx_identity_depth_and_confirmation_fail_closed(self):
        fixture, context = self.fixture("okx_bbo.json")
        malformed = json.loads(json.dumps(fixture["raw"]))
        malformed["arg"]["instId"] = "ETH-USDT-SWAP"
        with self.assertRaisesRegex(ValueError, "mismatch"):
            canonicalize_okx_bbo(malformed, context)
        malformed = json.loads(json.dumps(fixture["raw"]))
        malformed["data"][0]["bids"].append(["1", "1", "0", "1"])
        with self.assertRaisesRegex(ValueError, "one bid"):
            canonicalize_okx_bbo(malformed, context)

        fixture, context = self.fixture("okx_bar.json")
        malformed = json.loads(json.dumps(fixture["raw"]))
        malformed["data"][0][8] = "2"
        with self.assertRaisesRegex(ValueError, "confirm"):
            canonicalize_okx_bar(malformed, context)


class V2StableCapabilityMatrixTest(unittest.TestCase):
    def test_binance_okx_and_vn_share_one_truthful_shadow_core(self):
        matrix = yaml.safe_load(
            (ROOT / "config/v2/stable-capabilities.yaml").read_text()
        )
        self.assertEqual(matrix["public_contract_version"], "2.0.0")
        self.assertEqual(matrix["runtime_authority"], "RUST_SHADOW")
        self.assertFalse(matrix["authority_eligible"])
        expected = {"TRADE", "BBO", "BAR"}
        rows = {
            (item["venue"], item["market"]): item
            for item in matrix["capabilities"]
        }
        self.assertEqual(
            set(rows),
            {
                ("BINANCE", "USDM"),
                ("BINANCE", "SPOT"),
                ("OKX", "SWAP"),
                ("OKX", "SPOT"),
                ("HNX", "VN_DERIVATIVES"),
                ("HOSE", "EQUITIES"),
            },
        )
        for key in (("BINANCE", "USDM"), ("BINANCE", "SPOT"), ("OKX", "SWAP"), ("OKX", "SPOT")):
            self.assertEqual(set(rows[key]["feeds"]), expected)
        for key in (("HNX", "VN_DERIVATIVES"), ("HOSE", "EQUITIES")):
            self.assertEqual(set(rows[key]["feeds"]), {"TRADE", "BAR"})
        self.assertTrue(matrix["equal_source_contract"])
        self.assertTrue(
            all(
                item["canonical_core"] == "qdl-rust-realtime-core"
                for item in rows.values()
            )
        )
        self.assertFalse(matrix["capability_gates"]["CRYPTO_L2"]["stable"])
        self.assertFalse(matrix["capability_gates"]["VN_QUOTE"]["stable"])



if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from pathlib import Path

from qdl.certification.phase105_consumer_acceptance import build_release_consumer_acceptance_scope
from qdl.certification.phase105_fallback import (
    PHASE105_PAPER_CONSUMER_ORDER,
    blocked_fallback_identities,
    build_fallback_return_receipt,
    build_v1_fallback_probes,
    validate_v1_fallback_payload,
    validate_v1_provenance,
    validate_v1_runtime_binding,
)
from qdl.consumer import StableReleaseRoutePlan
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]


class Phase105FallbackAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.release = StableReleaseRoutePlan.load(
            ROOT / "config/v2/stable-v2-release-routing.yaml", manifest_root=ROOT
        )
        cls.catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
        cls.acquisition = StableAcquisitionPlan.load(
            ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=cls.catalog
        )
        cls.scope = build_release_consumer_acceptance_scope(
            cls.release, catalog=cls.catalog, acquisition=cls.acquisition
        )
        cls.probes = build_v1_fallback_probes(
            cls.release, catalog=cls.catalog, products=cls.scope.products
        )

    def test_scope_is_exact_and_only_manifest_allowed_binance_products(self) -> None:
        expected = {
            (consumer_id, product.requirement_key)
            for consumer in self.release.consumers
            for product in consumer.products
            for consumer_id in (consumer.consumer_id,)
            if consumer_id in PHASE105_PAPER_CONSUMER_ORDER
            and product.route == "V2_PRIMARY"
            and product.fallback == "V1"
        }
        self.assertEqual({item.identity for item in self.probes}, expected)
        self.assertTrue(self.probes)
        self.assertTrue(all(item.native_symbol in {"BTCUSDT", "ETHUSDT"} for item in self.probes))
        self.assertTrue(all(item.path.startswith("/v1/binance/") for item in self.probes))
        self.assertTrue(all(item.feed in {"TRADE", "BAR"} for item in self.probes))

    def test_blocked_scope_never_appears_in_v1_probe_scope(self) -> None:
        blocked = set(blocked_fallback_identities(self.release))
        self.assertTrue(blocked)
        self.assertFalse(blocked & {item.identity for item in self.probes})
        self.assertTrue(any("alpha.okx" in consumer for consumer, _key in blocked))

    def test_trade_and_final_bar_contracts_are_checked_without_retaining_payload(self) -> None:
        trade = next(item for item in self.probes if item.feed == "TRADE")
        trade_result = validate_v1_fallback_payload(trade, {
            "symbol": trade.native_symbol,
            "market": "binance_usdm",
            "price": "100.25",
            "quantity": "0.5",
            "trade_time": 1_000_000,
        }, now_ms=1_000_500)
        self.assertEqual(trade_result["endpoint_kind"], "BINANCE_TRADE")
        self.assertNotIn("payload", trade_result)
        self.assertEqual(trade_result["source_age_ms"], 500)

        bar = next(item for item in self.probes if item.feed == "BAR")
        bar_result = validate_v1_fallback_payload(bar, {
            "e": "kline",
            "E": 1_000_000,
            "s": bar.native_symbol,
            "k": {
                "t": 940_000,
                "T": 1_000_000,
                "s": bar.native_symbol,
                "i": bar.interval,
                "o": "100",
                "h": "102",
                "l": "99",
                "c": "101",
                "v": "0",
                "x": True,
            },
        }, now_ms=1_000_500)
        self.assertEqual(bar_result["endpoint_kind"], "BINANCE_BAR")
        self.assertNotIn("payload", bar_result)

    def test_invalid_symbol_or_non_final_bar_fails_closed(self) -> None:
        trade = next(item for item in self.probes if item.feed == "TRADE")
        with self.assertRaisesRegex(ValueError, "symbol"):
            validate_v1_fallback_payload(trade, {
                "symbol": "WRONG", "market": "binance_usdm", "price": "1",
                "quantity": "1", "trade_time": 1_000_000,
            }, now_ms=1_000_500)
        bar = next(item for item in self.probes if item.feed == "BAR")
        with self.assertRaisesRegex(ValueError, "finality"):
            validate_v1_fallback_payload(bar, {
                "e": "kline", "E": 1_000_000, "s": bar.native_symbol,
                "k": {"t": 940_000, "T": 1_000_000, "s": bar.native_symbol,
                      "i": bar.interval, "o": "1", "h": "1", "l": "1", "c": "1",
                      "v": "0", "x": False},
            }, now_ms=1_000_500)

    def test_provenance_and_final_receipt_are_exact_and_fail_closed(self) -> None:
        provenance = {
            "schema": "qdl.phase105.v1-fallback-provenance.v1",
            "status": "PASS",
            "image_id": "sha256:" + "a" * 64,
            "source_commit": self.release.v1_fallback.source_commit,
            "source_tree": "b" * 40,
            "dockerfile_sha256": "c" * 64,
            "version": "v1.2.2",
        }
        result = validate_v1_provenance(self.release, provenance)
        self.assertEqual(result["source_commit"], self.release.v1_fallback.source_commit)
        binding = {
            "schema": "qdl.phase105.v1-runtime-binding.v1",
            "status": "PASS",
            "service": "data_layer_service",
            "container_image_id": result["image_id"],
            "container_id_sha256": "d" * 64,
            "v1_provenance_sha256": result["provenance_sha256"],
        }
        self.assertEqual(validate_v1_runtime_binding(result, binding)["image_id"], result["image_id"])
        binding["container_image_id"] = "sha256:" + "e" * 64
        with self.assertRaisesRegex(ValueError, "runtime binding"):
            validate_v1_runtime_binding(result, binding)
        receipt = build_fallback_return_receipt(self.release, self.probes)
        self.assertEqual(receipt["status"], "PASS")
        self.assertEqual(len(receipt["routes"]), len(self.probes))
        provenance["version"] = "wrong"
        with self.assertRaisesRegex(ValueError, "provenance"):
            validate_v1_provenance(self.release, provenance)


if __name__ == "__main__":
    unittest.main()

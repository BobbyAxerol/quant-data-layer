from __future__ import annotations

import time
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
)

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
SPOT_BINDINGS = {
    "binance-spot-btcusdt-trade", "binance-spot-btcusdt-quote",
    "binance-spot-btcusdt-bar-1m", "okx-spot-btcusdt-trade",
    "okx-spot-btcusdt-quote", "okx-spot-btcusdt-bar-1m",
}


class DisabledAcquisitionTests(unittest.TestCase):
    """Program rule 6: an unused feed is disabled, not deleted.

    Until `enabled` existed the acquisition set had to equal the catalog set
    exactly, so the only way to stop acquiring a feed was to remove it from the
    catalog - which removed the capability the rule says to keep.
    """

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH, catalog=self.catalog
        )
        self.authority = stable_authority_record(
            rust_image_digest="e" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION_PATH.read_bytes(),
            effective_at_ns=time.time_ns(),
        )

    def test_the_capability_is_kept_in_both_documents(self):
        catalog_ids = {item.binding_id for item in self.catalog.bindings}
        acquisition_ids = {item.binding_id for item in self.acquisition.bindings}
        self.assertTrue(SPOT_BINDINGS <= catalog_ids)
        self.assertTrue(SPOT_BINDINGS <= acquisition_ids)

    def test_the_disabled_bindings_are_exactly_the_zero_demand_spot_set(self):
        disabled = {
            item.binding_id for item in self.acquisition.bindings if not item.enabled
        }
        self.assertEqual(disabled, SPOT_BINDINGS)

    def test_a_disabled_binding_is_not_configured_into_the_core(self):
        config = self.acquisition.core_config(
            catalog=self.catalog, authority=self.authority, worker_index=1
        )
        configured = {item["source_id"] for item in config["core"]["bindings"]}
        source_ids = {
            binding.source_id for binding in self.catalog.bindings
            if binding.binding_id in SPOT_BINDINGS
        }
        self.assertEqual(configured & source_ids, set())
        enabled_binding_ids = {
            binding.binding_id
            for binding in self.acquisition.bindings
            if binding.enabled
        }
        expected_configured = {
            binding.source_id
            for binding in self.catalog.bindings
            if binding.binding_id in enabled_binding_ids
        }
        self.assertEqual(
            configured,
            expected_configured,
        )

    def test_no_ingestor_role_is_generated_for_a_disabled_market(self):
        roles = self.acquisition.native_ingestor_configs(
            catalog=self.catalog, authority=self.authority
        )
        self.assertNotIn("binance-spot", roles)
        self.assertNotIn("okx-spot", roles)
        self.assertIn("binance-usdm", roles)
        self.assertIn("okx-swap", roles)

    def test_enabled_defaults_to_true_so_existing_bindings_are_unaffected(self):
        enabled = [item for item in self.acquisition.bindings if item.enabled]
        self.assertEqual(
            len(enabled), len(self.acquisition.bindings) - len(SPOT_BINDINGS)
        )
        for item in enabled:
            self.assertNotIn(item.binding_id, SPOT_BINDINGS)


if __name__ == "__main__":
    unittest.main()

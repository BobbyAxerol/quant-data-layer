from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.runtime.execution_l2 import execution_l2_materialization_plan
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
DEMAND = ROOT / "config/v2/stable-crypto-demand.yaml"
EXPECTED_SOURCE_IDS = frozenset({
    "binance-usdm-bnbusdt-book-primary-v2",
    "binance-usdm-btcusdt-book-primary-v2",
    "binance-usdm-dogeusdt-book-primary-v2",
    "binance-usdm-ethusdt-book-primary-v2",
    "binance-usdm-solusdt-book-primary-v2",
    "okx-swap-bnb-usdt-swap-book-primary-v2",
    "okx-swap-btc-usdt-swap-book-primary-v2",
    "okx-swap-doge-usdt-swap-book-primary-v2",
    "okx-swap-eth-usdt-swap-book-primary-v2",
    "okx-swap-sol-usdt-swap-book-primary-v2",
})


class ExecutionL2MaterializationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG)
        self.acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=self.catalog)

    def test_manifest_derives_all_current_execution_books_without_symbol_policy_code(self) -> None:
        plan = execution_l2_materialization_plan(
            demand_path=DEMAND,
            catalog=self.catalog,
            acquisition=self.acquisition,
        )
        self.assertEqual(set(plan.source_ids), EXPECTED_SOURCE_IDS)
        self.assertEqual(len(plan.binding_ids), 20)
        self.assertEqual(plan.materialized_snapshot_interval_ms, 1_000)

        catalog_by_binding = {
            binding.binding_id: binding for binding in self.catalog.bindings
        }
        bindings = {
            catalog_by_binding[binding.binding_id].source_id: binding
            for binding in self.acquisition.bindings
            if binding.binding_id in catalog_by_binding
            and catalog_by_binding[binding.binding_id].source_id in plan.source_ids
        }
        self.assertEqual(set(bindings), EXPECTED_SOURCE_IDS)
        self.assertTrue(all(
            binding.mode == "RUST_NATIVE"
            and binding.sequence_policy == "CONTIGUOUS"
            and binding.l2 is not None
            and binding.l2.depth_per_side == 100
            and binding.l2.snapshot_refresh_seconds == 30
            and binding.l2.materialized_snapshot_interval_ms == 1_000
            for binding in bindings.values()
        ))

    def test_fails_closed_when_an_execution_book_pair_is_incomplete(self) -> None:
        raw = yaml.safe_load(DEMAND.read_text(encoding="utf-8"))
        consumer = next(
            item for item in raw["consumers"]
            if item["consumer_grade"] == "EXECUTION"
        )
        delta_index = next(
            index for index, item in enumerate(consumer["requirements"])
            if item["feed"] == "BOOK_DELTA"
        )
        consumer["requirements"].pop(delta_index)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "demand.yaml"
            path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "complete snapshot/delta pair"):
                execution_l2_materialization_plan(
                    demand_path=path,
                    catalog=self.catalog,
                    acquisition=self.acquisition,
                )

    def test_fails_closed_when_one_execution_book_loses_hot_materialization(self) -> None:
        raw = yaml.safe_load(ACQUISITION.read_text(encoding="utf-8"))
        changed = copy.deepcopy(raw)
        binding = next(
            item for item in changed["bindings"]
            if item["binding_id"] == "okx-swap-sol-usdt-swap-book_snapshot"
        )
        binding["l2"].pop("materialized_snapshot_interval_ms")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acquisition.yaml"
            path.write_text(yaml.safe_dump(changed, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "complete equivalent book pair"):
                StableAcquisitionPlan.load(path, catalog=self.catalog)


if __name__ == "__main__":
    unittest.main()

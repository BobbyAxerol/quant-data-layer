from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import importlib.util
from pathlib import Path
import sys
import unittest

import yaml

from qdl.consumer.universal_release import (
    ConsumerRouteBinding,
    UniversalConsumerClass,
    UniversalReleaseProduct,
    UniversalV1Rollback,
)
from qdl.runtime.stable_catalog import StableSourceCatalog


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase12_materialize_bound_bar_edge.py"
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
SPEC = importlib.util.spec_from_file_location("phase12_bound_bar_edge", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
projection = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = projection
SPEC.loader.exec_module(projection)


def _documents() -> tuple[dict, dict]:
    return (
        yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8")),
        yaml.safe_load(ACQUISITION_PATH.read_text(encoding="utf-8")),
    )


def _binding_for_source_ids(*binding_ids: str) -> ConsumerRouteBinding:
    catalog_raw, _acquisition_raw = _documents()
    catalog = StableSourceCatalog.from_mapping(catalog_raw)
    records = {item.instrument_uid: item for item in catalog.instruments}
    by_id = {item.binding_id: item for item in catalog.bindings}
    products = []
    for index, binding_id in enumerate(binding_ids):
        source = by_id[binding_id]
        instrument = records[source.instrument.instrument_uid]
        products.append(UniversalReleaseProduct(
            consumer_id="phase12-test",
            consumer_class=UniversalConsumerClass.TRADING_SYSTEM,
            requirement_id=f"{index + 1:064x}",
            instrument_uid=instrument.instrument_uid,
            instrument_id=instrument.instrument_id,
            venue=instrument.identity.venue,
            market=instrument.identity.market,
            product_type=instrument.identity.product_type.value,
            native_symbol=instrument.native_symbol,
            feed="BAR",
            interval=source.interval,
            source_policy_id=source.source_policy_id,
            provider_plane="REALTIME",
            max_freshness_ms=source.stale_after_ms,
            require_final_bars=True,
            require_live=True,
            execution_grade=True,
            route="V2_PRIMARY",
            fallback="BLOCKED",
            fallback_rule_id=None,
            blocked_reason="TEST_ONLY",
        ))
    return ConsumerRouteBinding(
        consumer_id="phase12-test",
        consumer_class=UniversalConsumerClass.TRADING_SYSTEM,
        release_revision=1,
        universal_manifest_sha256="a" * 64,
        policy_sha256="b" * 64,
        capability_matrix_sha256="c" * 64,
        capability_matrix_revision=1,
        inventory_sha256="d" * 64,
        v1_rollback=UniversalV1Rollback(
            release_tag="v1", source_commit="e" * 40,
            image_reference="qdl-v1:test", manifest_revision="1",
        ),
        independent_v1_venues=("DNSE",),
        products=tuple(sorted(products, key=lambda item: item.requirement_id)),
    )


def _alpha_binding_for_source_ids(
    consumer_id: str, *binding_ids: str
) -> ConsumerRouteBinding:
    base = _binding_for_source_ids(*binding_ids)
    products = tuple(
        replace(
            item,
            consumer_id=consumer_id,
            consumer_class=UniversalConsumerClass.SINGLE_SYMBOL_ALPHA,
            require_live=False,
            execution_grade=False,
        )
        for item in base.products
    )
    return replace(
        base,
        consumer_id=consumer_id,
        consumer_class=UniversalConsumerClass.SINGLE_SYMBOL_ALPHA,
        products=products,
        binding_sha256=None,
    )


def _retained_projection(*binding_ids: str) -> dict:
    return {
        "schema": "qdl.phase12.bound-bar-edge-projection.v1",
        "status": "MATERIALIZED",
        "consumer_id": "trading-system.paper.stable",
        "binding_ids": list(binding_ids),
        "catalog_sha256": "c" * 64,
        "acquisition_sha256": "d" * 64,
    }


class BoundBarEdgeProjectionTests(unittest.TestCase):
    def test_projects_exact_final_bar_routes_without_mutating_inputs(self) -> None:
        catalog, acquisition = _documents()
        original_catalog = deepcopy(catalog)
        original_acquisition = deepcopy(acquisition)
        binding = _binding_for_source_ids(
            "binance-usdm-btcusdt-bar-1m",
            "okx-swap-eth-usdt-swap-bar-1m",
        )

        result = projection.build_bound_bar_projection(
            binding=binding,
            catalog_document=catalog,
            acquisition_document=acquisition,
        )

        self.assertEqual(catalog, original_catalog)
        self.assertEqual(acquisition, original_acquisition)
        self.assertEqual(result.summary["bar_route_count"], 2)
        self.assertEqual(
            result.summary["binding_ids"],
            ["binance-usdm-btcusdt-bar-1m", "okx-swap-eth-usdt-swap-bar-1m"],
        )
        self.assertEqual(
            {item["binding_id"] for item in result.catalog["bindings"]},
            set(result.summary["binding_ids"]),
        )
        self.assertEqual(
            {item["binding_id"] for item in result.acquisition["bindings"]},
            set(result.summary["binding_ids"]),
        )
        selected = {
            item["binding_id"]: item for item in result.acquisition["bindings"]
        }
        self.assertEqual(
            selected["okx-swap-eth-usdt-swap-bar-1m"]["mode"],
            "PYTHON_REST",
        )
        self.assertIsNone(
            selected["okx-swap-eth-usdt-swap-bar-1m"]["websocket_url"]
        )
        self.assertIsNone(
            selected["okx-swap-eth-usdt-swap-bar-1m"]["business_websocket_url"]
        )

    def test_unions_alpha_bindings_and_keeps_final_bars_without_live_tick_requirement(self) -> None:
        catalog, acquisition = _documents()
        first = _alpha_binding_for_source_ids(
            "alpha.first.binance.paper",
            "binance-usdm-ethusdt-bar-15m",
        )
        duplicate = _alpha_binding_for_source_ids(
            "alpha.second.binance.paper",
            "binance-usdm-ethusdt-bar-15m",
        )
        okx = _alpha_binding_for_source_ids(
            "alpha.first.okx.no-order",
            "okx-swap-eth-usdt-swap-bar-15m",
        )

        result = projection.build_bound_bar_projection_set(
            bindings=(first, duplicate, okx),
            catalog_document=catalog,
            acquisition_document=acquisition,
        )

        self.assertEqual(result.summary["bar_route_count"], 2)
        self.assertEqual(
            result.summary["binding_ids"],
            [
                "binance-usdm-ethusdt-bar-15m",
                "okx-swap-eth-usdt-swap-bar-15m",
            ],
        )
        self.assertEqual(
            result.summary["consumer_ids"],
            [
                "alpha.first.binance.paper",
                "alpha.first.okx.no-order",
                "alpha.second.binance.paper",
            ],
        )
        self.assertNotIn("consumer_id", result.summary)

    def test_retains_active_baseline_when_materializing_alpha_union(self) -> None:
        catalog, acquisition = _documents()
        alpha = _alpha_binding_for_source_ids(
            "alpha.first.binance.paper",
            "binance-usdm-ethusdt-bar-15m",
        )

        result = projection.build_bound_bar_projection_set(
            bindings=(alpha,),
            catalog_document=catalog,
            acquisition_document=acquisition,
            retained_projection=_retained_projection("binance-usdm-btcusdt-bar-1m"),
            retained_projection_sha256="e" * 64,
        )

        self.assertEqual(
            result.summary["binding_ids"],
            ["binance-usdm-btcusdt-bar-1m", "binance-usdm-ethusdt-bar-15m"],
        )
        self.assertEqual(
            result.summary["retained_projection"],
            {
                "consumer_id": "trading-system.paper.stable",
                "binding_ids": ["binance-usdm-btcusdt-bar-1m"],
                "sha256": "e" * 64,
            },
        )

    def test_rejects_retained_route_that_is_missing(self) -> None:
        catalog, acquisition = _documents()
        alpha = _alpha_binding_for_source_ids(
            "alpha.first.binance.paper",
            "binance-usdm-ethusdt-bar-15m",
        )

        with self.assertRaisesRegex(ValueError, "no longer resolves"):
            projection.build_bound_bar_projection_set(
                bindings=(alpha,),
                catalog_document=catalog,
                acquisition_document=acquisition,
                retained_projection=_retained_projection("missing-bar-route"),
            )

    def test_rejects_native_acquisition_outside_declared_okx_final_bar_recovery(self) -> None:
        catalog, acquisition = _documents()
        binding = _binding_for_source_ids("binance-usdm-btcusdt-bar-1m")
        invalid = deepcopy(acquisition)
        for item in invalid["bindings"]:
            if item["binding_id"] == "binance-usdm-btcusdt-bar-1m":
                item["mode"] = "RUST_NATIVE"
                break
        with self.assertRaisesRegex(ValueError, "only permits"):
            projection.build_bound_bar_projection(
                binding=binding,
                catalog_document=catalog,
                acquisition_document=invalid,
            )

    def test_rejects_missing_or_non_final_catalog_mapping(self) -> None:
        catalog, acquisition = _documents()
        binding = _binding_for_source_ids("binance-usdm-btcusdt-bar-1m")
        missing = deepcopy(catalog)
        missing["bindings"] = [
            item for item in missing["bindings"]
            if item["binding_id"] != "binance-usdm-btcusdt-bar-1m"
        ]
        with self.assertRaisesRegex(ValueError, "exactly one catalog binding"):
            projection.build_bound_bar_projection(
                binding=binding,
                catalog_document=missing,
                acquisition_document=acquisition,
            )

        non_final = deepcopy(catalog)
        for item in non_final["bindings"]:
            if item["binding_id"] == "binance-usdm-btcusdt-bar-1m":
                item["quality"]["require_final_bar"] = False
        with self.assertRaisesRegex(ValueError, "non-final catalog binding"):
            projection.build_bound_bar_projection(
                binding=binding,
                catalog_document=non_final,
                acquisition_document=acquisition,
            )

    def test_rejects_acquisition_coverage_mismatch(self) -> None:
        catalog, acquisition = _documents()
        binding = _binding_for_source_ids("binance-usdm-btcusdt-bar-1m")
        bad_acquisition = deepcopy(acquisition)
        bad_acquisition["bindings"] = [
            item for item in bad_acquisition["bindings"]
            if item["binding_id"] != "binance-usdm-btcusdt-bar-1m"
        ]
        with self.assertRaisesRegex(ValueError, "stable acquisition document and strict catalog differ"):
            projection.build_bound_bar_projection(
                binding=binding,
                catalog_document=catalog,
                acquisition_document=bad_acquisition,
            )


if __name__ == "__main__":
    unittest.main()

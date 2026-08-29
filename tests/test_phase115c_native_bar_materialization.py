from __future__ import annotations

from collections import Counter
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import unittest

from qdl.adapters.intervals import BINANCE_USDM_NATIVE_INTERVALS, OKX_NATIVE_INTERVALS
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope, StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase115c_materialize_active_native_bars.py"


def _module():
    spec = importlib.util.spec_from_file_location("phase115c_native_bars", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Phase115CNativeBarMaterializationTests(unittest.TestCase):
    @staticmethod
    def _canonical_demand(payload: dict) -> dict:
        result = deepcopy(payload)
        for consumer in result["consumers"]:
            consumer["requirements"] = sorted(
                consumer["requirements"],
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
            )
        return result

    @staticmethod
    def _before_materialization(
        demand: dict,
        catalog: dict,
        acquisition: dict,
        scope: dict,
    ) -> tuple[dict, dict, dict, dict]:
        """Recover the immediate pre-C3.5 shape from the checked-in result."""
        families = {("BINANCE", "USDM"), ("OKX", "SWAP")}
        before_demand = deepcopy(demand)
        for consumer in before_demand["consumers"]:
            consumer["requirements"] = [
                item for item in consumer["requirements"]
                if not (
                    item["feed"] == "BAR"
                    and (item["venue"], item["market"]) in families
                    and item["interval"] != "1m"
                )
            ]
        before_demand["revision"] -= 1

        instrument_family = {
            item["instrument_uid"]: (item["venue"], item["market"])
            for item in catalog["instruments"]
        }

        def retained(item: dict) -> bool:
            return not (
                item["feed"] == "BAR"
                and instrument_family[item["instrument_uid"]] in families
                and item.get("interval") != "1m"
            )

        before_catalog = deepcopy(catalog)
        before_catalog["bindings"] = [
            item for item in before_catalog["bindings"] if retained(item)
        ]
        before_catalog["catalog_revision"] -= 1
        retained_ids = {item["binding_id"] for item in before_catalog["bindings"]}

        before_acquisition = deepcopy(acquisition)
        before_acquisition["bindings"] = [
            item for item in before_acquisition["bindings"]
            if item["binding_id"] in retained_ids
        ]
        before_acquisition["revision"] -= 1

        before_scope = deepcopy(scope)
        before_scope["binding_ids"] = [
            item for item in before_scope["binding_ids"] if item in retained_ids
        ]
        before_scope["revision"] -= 1
        return before_demand, before_catalog, before_acquisition, before_scope

    @classmethod
    def setUpClass(cls) -> None:
        cls.tool = _module()
        cls.current_demand = cls.tool._load_yaml(
            ROOT / "config/v2/stable-crypto-demand.yaml"
        )
        cls.current_catalog = cls.tool._load_yaml(
            ROOT / "config/v2/stable-source-bindings.yaml"
        )
        cls.current_acquisition = cls.tool._load_yaml(
            ROOT / "config/v2/stable-acquisition-bindings.yaml"
        )
        cls.current_scope = cls.tool._load_yaml(
            ROOT / "config/v2/stable-authority-promotion-scope.yaml"
        )
        cls.current_route = cls.tool._load_yaml(
            ROOT / "config/v2/stable-v2-release-routing.yaml"
        )
        (
            cls.before_demand,
            cls.before_catalog,
            cls.before_acquisition,
            cls.before_scope,
        ) = cls._before_materialization(
            cls.current_demand,
            cls.current_catalog,
            cls.current_acquisition,
            cls.current_scope,
        )
        (
            cls.demand,
            cls.catalog,
            cls.acquisition,
            cls.scope,
            cls.summary,
        ) = cls.tool.build_documents(
            demand=cls.before_demand,
            source_catalog=cls.before_catalog,
            acquisition=cls.before_acquisition,
            promotion_scope=cls.before_scope,
            binance_usdm_capture=ROOT / "config/v2/captures/binance-usdm-exchangeinfo.filtered.json",
            binance_spot_capture=ROOT / "config/v2/captures/binance-spot-exchangeinfo.filtered.json",
            okx_swap_capture=ROOT / "config/v2/captures/okx-instruments-swap.filtered.json",
            okx_spot_capture=ROOT / "config/v2/captures/okx-instruments-spot.filtered.json",
        )
        cls.loaded_catalog = cls.tool._load_temporary(
            cls.catalog, "catalog.yaml", StableSourceCatalog.load
        )
        cls.loaded_acquisition = cls.tool._load_temporary(
            cls.acquisition,
            "acquisition.yaml",
            lambda path: StableAcquisitionPlan.load(path, catalog=cls.loaded_catalog),
        )

    def _intervals(self, venue: str, market: str, symbol: str) -> set[str]:
        return {
            item.interval or ""
            for item in self.loaded_catalog.bindings
            if item.feed.value == "BAR"
            and item.instrument.identity.venue == venue
            and item.instrument.identity.market == market
            and item.instrument.native_symbol == symbol
        }

    def test_exact_active_native_interval_sets_are_materialized(self) -> None:
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"):
            self.assertEqual(
                self._intervals("BINANCE", "USDM", symbol),
                set(BINANCE_USDM_NATIVE_INTERVALS),
            )
        for symbol in (
            "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
            "DOGE-USDT-SWAP", "BNB-USDT-SWAP",
        ):
            self.assertEqual(
                self._intervals("OKX", "SWAP", symbol),
                set(OKX_NATIVE_INTERVALS),
            )
        self.assertEqual(self.summary["demand_additions"], 130)
        self.assertEqual(self.summary["bar_binding_counts"], {
            "binance_usdm": 70, "okx_swap": 70, "dnse": 2,
        })
        self.assertEqual(self.summary["price_binding_counts"], {
            "binance_usdm": 80, "okx_swap": 80,
        })
        self.assertEqual(
            self._canonical_demand(self.demand),
            self._canonical_demand(self.current_demand),
        )
        self.assertEqual(self.catalog, self.current_catalog)
        self.assertEqual(self.acquisition, self.current_acquisition)
        self.assertEqual(self.scope, self.current_scope)

    def test_five_liquid_price_plane_has_exact_trade_quote_counts_without_l2_growth(self) -> None:
        product_counts = Counter(
            (item.instrument.identity.venue, item.instrument.identity.market, item.feed.value)
            for item in self.loaded_catalog.bindings
            if (item.instrument.identity.venue, item.instrument.identity.market)
            in {("BINANCE", "USDM"), ("OKX", "SWAP")}
        )
        self.assertEqual(product_counts[("BINANCE", "USDM", "TRADE")], 5)
        self.assertEqual(product_counts[("BINANCE", "USDM", "QUOTE")], 5)
        self.assertEqual(product_counts[("OKX", "SWAP", "TRADE")], 5)
        self.assertEqual(product_counts[("OKX", "SWAP", "QUOTE")], 5)
        current_l2 = {
            item["binding_id"] for item in self.current_catalog["bindings"]
            if str(item["feed"]).startswith("BOOK_")
        }
        materialized_l2 = {
            item.binding_id for item in self.loaded_catalog.bindings
            if item.feed.value.startswith("BOOK_")
        }
        self.assertEqual(materialized_l2, current_l2)

    def test_every_active_bar_is_rest_owned_with_exact_venue_channel(self) -> None:
        source_by_id = {item.binding_id: item for item in self.loaded_catalog.bindings}
        for item in self.loaded_acquisition.bindings:
            source = source_by_id[item.binding_id]
            family = (source.instrument.identity.venue, source.instrument.identity.market)
            if source.feed.value != "BAR" or family not in {
                ("BINANCE", "USDM"), ("OKX", "SWAP"),
            }:
                continue
            self.assertEqual(item.mode, "PYTHON_REST")
            if family == ("BINANCE", "USDM"):
                self.assertEqual(item.provider_kind, "binance_usdm_rest_bar")
                self.assertEqual(item.native_channel, f"rest-klines/{source.interval}")
            else:
                self.assertEqual(item.provider_kind, "okx_bar")
                self.assertEqual(item.native_channel, f"candle{self.tool.okx_candle_channel(source.interval)[6:]}")

    def test_existing_cursor_identities_and_disabled_spot_are_preserved(self) -> None:
        old = {
            self.tool._source_requirement_key(item): item
            for item in self.current_catalog["bindings"]
        }
        new = {
            self.tool._source_requirement_key(item): item
            for item in self.catalog["bindings"]
        }
        for key, before in old.items():
            if key in new:
                self.assertEqual(new[key]["binding_id"], before["binding_id"])
                self.assertEqual(new[key]["source"]["source_id"], before["source"]["source_id"])
        before_spot = {
            item["binding_id"]: item
            for item in self.current_acquisition["bindings"]
            if "-spot-" in item["binding_id"]
        }
        after_spot = {
            item["binding_id"]: item
            for item in self.acquisition["bindings"]
            if "-spot-" in item["binding_id"]
        }
        self.assertEqual(after_spot, before_spot)
        self.assertTrue(all(item.get("enabled") is False for item in after_spot.values()))

    def test_dnse_remains_one_native_minute_binding_and_scope_covers_new_bars(self) -> None:
        dnse = [
            item for item in self.loaded_catalog.bindings
            if item.feed.value == "BAR" and item.instrument.identity.venue in {"HNX", "HOSE"}
        ]
        self.assertEqual({item.interval for item in dnse}, {"1m"})
        scope = self.tool._load_temporary(
            self.scope,
            "scope.yaml",
            lambda path: AuthorityPromotionScope.load(path, catalog=self.loaded_catalog),
        )
        active_bars = {
            item.binding_id
            for item in self.loaded_catalog.bindings
            if item.feed.value == "BAR"
            and (item.instrument.identity.venue, item.instrument.identity.market)
            in {("BINANCE", "USDM"), ("OKX", "SWAP")}
        }
        self.assertTrue(active_bars.issubset(scope.binding_ids))

    def test_materialization_is_idempotent_after_apply_shape(self) -> None:
        demand, catalog, acquisition, scope, summary = self.tool.build_documents(
            demand=self.demand,
            source_catalog=self.catalog,
            acquisition=self.acquisition,
            promotion_scope=self.scope,
            binance_usdm_capture=ROOT / "config/v2/captures/binance-usdm-exchangeinfo.filtered.json",
            binance_spot_capture=ROOT / "config/v2/captures/binance-spot-exchangeinfo.filtered.json",
            okx_swap_capture=ROOT / "config/v2/captures/okx-instruments-swap.filtered.json",
            okx_spot_capture=ROOT / "config/v2/captures/okx-instruments-spot.filtered.json",
        )
        self.assertEqual(summary["demand_additions"], 0)
        self.assertEqual(demand, self.demand)
        self.assertEqual(catalog, self.catalog)
        self.assertEqual(acquisition, self.acquisition)
        self.assertEqual(scope, self.scope)

    def test_release_route_rebinds_any_changed_artifact_once(self) -> None:
        stale_route = deepcopy(self.current_route)
        stale_route["revision"] -= 1
        stale_route["source_catalog"] = {
            **stale_route["source_catalog"],
            "sha256": "0" * 64,
            "revision": self.summary["catalog_revision"] - 1,
        }
        route = self.tool._update_release_route(
            stale_route, summary=self.summary
        )
        self.assertEqual(
            route["source_catalog"],
            {
                "path": "/app/config/v2/stable-source-bindings.yaml",
                "sha256": self.summary["source_catalog_sha256"],
                "revision": self.summary["catalog_revision"],
            },
        )
        self.assertEqual(route["revision"], stale_route["revision"] + 1)
        _, _, _, _, repeated_summary = self.tool.build_documents(
            demand=self.demand,
            source_catalog=self.catalog,
            acquisition=self.acquisition,
            promotion_scope=self.scope,
            binance_usdm_capture=ROOT / "config/v2/captures/binance-usdm-exchangeinfo.filtered.json",
            binance_spot_capture=ROOT / "config/v2/captures/binance-spot-exchangeinfo.filtered.json",
            okx_swap_capture=ROOT / "config/v2/captures/okx-instruments-swap.filtered.json",
            okx_spot_capture=ROOT / "config/v2/captures/okx-instruments-spot.filtered.json",
        )
        self.assertEqual(
            self.tool._update_release_route(route, summary=repeated_summary), route
        )


if __name__ == "__main__":
    unittest.main()

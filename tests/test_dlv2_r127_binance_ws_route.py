"""R1.27 Phase 1: Binance USD-M WebSocket route groups.

Binance split `fstream.binance.com` into `/public`, `/market` and `/private`
and decommissioned the unrouted base on 2026-04-23. A connection without a
routed path receives the `/public` group only: `/market` subscriptions are
acknowledged and then push nothing. That is how this platform lost every
Binance kline and mark price for five months while `@depth`, `@bookTicker` and
`@trade` - all `/public` - kept working on the same socket.

These tests pin three things: the channel-to-route table, that the catalog
emits both bases for USD-M, and that a config still carrying the decommissioned
base is refused rather than accepted into a silently degraded role.
"""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

import yaml

from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    binance_route_for_channel,
    stable_authority_record,
)


PUBLIC_BASE = "wss://fstream.binance.com/public/ws"
MARKET_BASE = "wss://fstream.binance.com/market/ws"
DECOMMISSIONED_BASE = "wss://fstream.binance.com/ws"

BINANCE_EXCHANGE_INFO = {
    "serverTime": 1000,
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "contractType": "PERPETUAL",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "deliveryDate": 0,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ],
        },
    ],
}


MARK_INDEX_FEED = {
    "feed": "MARK_INDEX_PRICE",
    "interval": None,
    "max_freshness_ms": 2_000,
    "require_live": True,
    # Only OKX carries a separate index instrument; Binance USD-M mark and
    # index arrive on one `@markPrice` frame.
    "index_native_symbol": None,
}


def BOOK_FEED(feed: str) -> dict:  # noqa: N802 - reads as a fixture constructor
    return {
        "feed": feed,
        "interval": None,
        "depth_per_side": 100,
        "max_freshness_ms": 60_000,
        "require_live": True,
    }


def _binance_demand(root: Path, feeds: list[dict]) -> ProductionDemandManifest:
    payload = {
        "schema": "qdl.v2.production-demand.v1",
        "revision": 5,
        "consumers": [
            {
                "consumer_id": "trading-system.execution.v2",
                "consumer_grade": "EXECUTION",
                "requirements": [
                    {
                        "venue": "BINANCE",
                        "market": "USDM",
                        "product_type": "PERPETUAL",
                        "native_symbol": "BTCUSDT",
                        "source_policy_id": "crypto_primary_v2",
                        **feed,
                    }
                    for feed in feeds
                ],
            }
        ],
    }
    path = root / "demand.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return ProductionDemandManifest.load_many([path])


class BinanceRouteTableTests(unittest.TestCase):
    """The channel-to-route table, which the ingestor mirrors in Rust."""

    def test_market_group_channels(self):
        self.assertEqual(binance_route_for_channel("btcusdt@markPrice@1s"), "market")
        self.assertEqual(binance_route_for_channel("btcusdt@kline_1m"), "market")
        self.assertEqual(binance_route_for_channel("ethusdt@kline_15m"), "market")

    def test_public_group_channels(self):
        self.assertEqual(binance_route_for_channel("btcusdt@trade"), "public")
        self.assertEqual(binance_route_for_channel("btcusdt@bookTicker"), "public")
        self.assertEqual(binance_route_for_channel("btcusdt@depth@100ms"), "public")

    def test_an_unknown_channel_has_no_route(self):
        # Refusing is the point: an unrouted channel would otherwise be
        # subscribed on whichever socket happened to be opened first.
        self.assertIsNone(binance_route_for_channel("btcusdt@forceOrder"))
        self.assertIsNone(binance_route_for_channel(""))

    def test_every_channel_the_catalog_emits_for_binance_is_routed(self):
        """Pins this table to the generator rather than to a hand copy.

        The Rust suite pins its own table to `validate_stream` the same way;
        the two cannot share one implementation across the language boundary.
        """

        feeds = [
            {"feed": "TRADE", "interval": None},
            {"feed": "QUOTE", "interval": None},
            MARK_INDEX_FEED,
            BOOK_FEED("BOOK_SNAPSHOT"),
            BOOK_FEED("BOOK_DELTA"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = ProductionCatalogBuilder(
                catalog_revision=8, source_policy_revision=3, authority_revision=11
            ).build(
                demand=_binance_demand(root, feeds),
                binance_usdm=parse_exchange_info(BINANCE_EXCHANGE_INFO, valid_from_ns=99),
                okx_rows=[],
                metadata_provenance={"capture": "a" * 64},
            )
            native = [
                item
                for item in bundle.acquisition_plan["bindings"]
                if item["runtime"] == "BINANCE" and item["mode"] == "RUST_NATIVE"
            ]
            self.assertTrue(native, "the fixture must produce native Binance bindings")
            for item in native:
                self.assertIsNotNone(
                    binance_route_for_channel(item["native_channel"]),
                    f"{item['native_channel']} is emitted but has no routed base",
                )


class BinanceCatalogEmitsBothBasesTests(unittest.TestCase):
    def _native_binance_bindings(self, feeds: list[dict]) -> list[dict]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = ProductionCatalogBuilder(
                catalog_revision=8, source_policy_revision=3, authority_revision=11
            ).build(
                demand=_binance_demand(root, feeds),
                binance_usdm=parse_exchange_info(BINANCE_EXCHANGE_INFO, valid_from_ns=99),
                okx_rows=[],
                metadata_provenance={"capture": "a" * 64},
            )
            return [
                item
                for item in bundle.acquisition_plan["bindings"]
                if item["runtime"] == "BINANCE" and item["mode"] == "RUST_NATIVE"
            ]

    def test_usdm_native_bindings_carry_the_routed_pair(self):
        bindings = self._native_binance_bindings(
            [{"feed": "TRADE", "interval": None}, MARK_INDEX_FEED]
        )
        self.assertTrue(bindings)
        for item in bindings:
            self.assertEqual(item["websocket_url"], PUBLIC_BASE)
            self.assertEqual(item["market_websocket_url"], MARKET_BASE)
            self.assertIsNone(item["business_websocket_url"])

    def test_the_decommissioned_base_is_never_emitted(self):
        bindings = self._native_binance_bindings([{"feed": "TRADE", "interval": None}])
        for item in bindings:
            self.assertNotEqual(item["websocket_url"], DECOMMISSIONED_BASE)
            self.assertNotEqual(item["market_websocket_url"], DECOMMISSIONED_BASE)


class DeployedAcquisitionPlanTests(unittest.TestCase):
    """The plan this repository ships must satisfy the new rule."""

    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1]
        self.catalog = StableSourceCatalog.load(
            self.root / "config/v2/stable-source-bindings.yaml"
        )
        self.acquisition_path = self.root / "config/v2/stable-acquisition-bindings.yaml"

    def test_shipped_plan_loads_and_carries_no_decommissioned_base(self):
        plan = StableAcquisitionPlan.load(self.acquisition_path, catalog=self.catalog)
        binance = [
            item
            for item in plan.bindings
            if item.runtime == "BINANCE" and item.mode == "RUST_NATIVE"
        ]
        self.assertTrue(binance)
        for item in binance:
            self.assertNotEqual(item.websocket_url, DECOMMISSIONED_BASE)
            self.assertIsNotNone(item.market_websocket_url)

    def test_usdm_bindings_use_the_public_control_endpoint(self):
        plan = StableAcquisitionPlan.load(self.acquisition_path, catalog=self.catalog)
        source_by_id = {item.binding_id: item for item in self.catalog.bindings}
        usdm = [
            item
            for item in plan.bindings
            if item.runtime == "BINANCE"
            and item.mode == "RUST_NATIVE"
            and source_by_id[item.binding_id].instrument.identity.market == "USDM"
        ]
        self.assertTrue(usdm)
        for item in usdm:
            self.assertEqual(item.websocket_url, PUBLIC_BASE)
            self.assertEqual(item.market_websocket_url, MARKET_BASE)

    def _reload_with(self, mutate) -> None:
        payload = yaml.safe_load(self.acquisition_path.read_text())
        source_by_id = {item.binding_id: item for item in self.catalog.bindings}
        for binding in payload["bindings"]:
            if binding["runtime"] != "BINANCE" or binding["mode"] != "RUST_NATIVE":
                continue
            if source_by_id[binding["binding_id"]].instrument.identity.market != "USDM":
                continue
            mutate(binding)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acquisition.yaml"
            path.write_text(yaml.safe_dump(payload, sort_keys=False))
            StableAcquisitionPlan.load(path, catalog=self.catalog)

    def test_the_decommissioned_public_base_is_refused(self):
        # The exact configuration this platform ran from 2026-04-23 until
        # 2026-09-18. It must fail loudly, not degrade quietly.
        with self.assertRaises(ValueError) as caught:
            self._reload_with(
                lambda binding: binding.update({"websocket_url": DECOMMISSIONED_BASE})
            )
        self.assertIn("/public/ws", str(caught.exception))

    def test_a_missing_market_base_is_refused(self):
        with self.assertRaises(ValueError):
            self._reload_with(
                lambda binding: binding.update({"market_websocket_url": None})
            )

    def test_the_two_bases_may_not_be_swapped(self):
        with self.assertRaises(ValueError):
            self._reload_with(
                lambda binding: binding.update(
                    {"websocket_url": MARKET_BASE, "market_websocket_url": PUBLIC_BASE}
                )
            )

    def test_a_combined_stream_url_is_refused(self):
        # The ingestor subscribes dynamically and needs the control endpoint.
        with self.assertRaises(ValueError):
            self._reload_with(
                lambda binding: binding.update(
                    {
                        "market_websocket_url": (
                            "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m"
                        )
                    }
                )
            )

    def test_binance_may_not_carry_an_okx_business_service(self):
        with self.assertRaises(ValueError):
            self._reload_with(
                lambda binding: binding.update(
                    {"business_websocket_url": "wss://ws.okx.com:8443/ws/v5/business"}
                )
            )


class OkxIsUnchangedTests(unittest.TestCase):
    """OKX already routes by service and must not acquire Binance's field."""

    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.catalog = StableSourceCatalog.load(
            root / "config/v2/stable-source-bindings.yaml"
        )
        self.acquisition_path = root / "config/v2/stable-acquisition-bindings.yaml"

    def test_okx_keeps_public_and_business_and_declares_no_market_base(self):
        plan = StableAcquisitionPlan.load(self.acquisition_path, catalog=self.catalog)
        okx = [
            item
            for item in plan.bindings
            if item.runtime == "OKX" and item.mode == "RUST_NATIVE"
        ]
        self.assertTrue(okx)
        for item in okx:
            self.assertEqual(item.websocket_url, "wss://ws.okx.com:8443/ws/v5/public")
            self.assertEqual(
                item.business_websocket_url, "wss://ws.okx.com:8443/ws/v5/business"
            )
            self.assertIsNone(item.market_websocket_url)

    def test_okx_may_not_carry_a_binance_market_base(self):
        payload = yaml.safe_load(self.acquisition_path.read_text())
        for binding in payload["bindings"]:
            if binding["runtime"] == "OKX" and binding["mode"] == "RUST_NATIVE":
                binding["market_websocket_url"] = MARKET_BASE
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acquisition.yaml"
            path.write_text(yaml.safe_dump(payload, sort_keys=False))
            with self.assertRaises(ValueError):
                StableAcquisitionPlan.load(path, catalog=self.catalog)


class IngestorConfigCarriesBothBasesTests(unittest.TestCase):
    """The generated native-ingestor config is what the Rust role reads."""

    def test_binance_ingestor_config_declares_the_routed_pair(self):
        root = Path(__file__).resolve().parents[1]
        catalog = StableSourceCatalog.load(root / "config/v2/stable-source-bindings.yaml")
        acquisition_path = root / "config/v2/stable-acquisition-bindings.yaml"
        plan = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
        authority = stable_authority_record(
            rust_image_digest="a" * 64,
            capability_manifest=root / "config/v2/stable-capabilities.yaml",
            contract=root / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=acquisition_path.read_bytes(),
            effective_at_ns=time.time_ns(),
        )
        configs = plan.native_ingestor_configs(catalog=catalog, authority=authority)
        binance = {
            key: value for key, value in configs.items() if key.startswith("binance-")
        }
        self.assertTrue(binance, "the shipped plan must produce a Binance ingestor role")
        for key, value in binance.items():
            self.assertIn("market_websocket_url", value, key)
            self.assertIsNotNone(value["market_websocket_url"], key)
            self.assertNotEqual(value["websocket_url"], DECOMMISSIONED_BASE, key)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from pathlib import Path

from qdl.adapters.binance_spot import parse_spot_exchange_info
from qdl.query.contracts import FeedType
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemand,
    ProductionDemandManifest,
)
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
DEMAND_PATH = ROOT / "config/v2/stable-crypto-demand.yaml"
CRYPTO_VENUES = {"BINANCE", "OKX"}


def _spot_payload(*, contract_type: str | None = None) -> dict:
    symbol = {
        "symbol": "BTCUSDT", "status": "TRADING",
        "baseAsset": "BTC", "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.00001"},
        ],
    }
    if contract_type is not None:
        symbol["contractType"] = contract_type
    return {"serverTime": 1, "symbols": [symbol]}


class BinanceSpotDiscoveryTests(unittest.TestCase):
    """The USD-M parser silently skips every Spot symbol, so Spot needs its own."""

    def test_it_reproduces_the_committed_catalog_record(self):
        discovered = parse_spot_exchange_info(_spot_payload(), valid_from_ns=0)
        record = discovered.records[0]
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        committed = catalog.instrument_for(record.instrument_uid)
        self.assertEqual(committed.identity.instrument_id, "BINANCE.SPOT.SPOT.BTC-USDT")
        self.assertEqual(committed.settlement_asset, record.settlement_asset)
        self.assertEqual(committed.asset_class, record.asset_class)
        self.assertEqual(committed.native_symbol, record.native_symbol)

    def test_spot_identity_rules(self):
        record = parse_spot_exchange_info(_spot_payload(), valid_from_ns=0).records[0]
        self.assertEqual(record.identity.market, "SPOT")
        self.assertEqual(record.identity.product_type.value, "SPOT")
        self.assertIsNone(record.expiry_time_ns)
        self.assertEqual(record.settlement_asset, "USDT")
        self.assertEqual(record.attributes, {})

    def test_a_derivatives_capture_is_refused_not_silently_parsed(self):
        with self.assertRaises(ValueError):
            parse_spot_exchange_info(
                _spot_payload(contract_type="PERPETUAL"), valid_from_ns=0
            )

    def test_a_capture_with_no_active_symbol_fails_closed(self):
        with self.assertRaises(ValueError):
            parse_spot_exchange_info(
                {"serverTime": 1, "symbols": [{"symbol": "X", "status": "BREAK"}]},
                valid_from_ns=0,
            )


class CommittedDemandManifestTests(unittest.TestCase):
    """The catalog is generated, so the demand that produced it must be tracked."""

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.manifest = ProductionDemandManifest.load_many([DEMAND_PATH])

    def _catalog_crypto_keys(self):
        return {
            (
                binding.instrument.identity.venue,
                binding.instrument.identity.market,
                binding.instrument.native_symbol,
                binding.feed.value,
                binding.interval,
            )
            for binding in self.catalog.bindings
            if binding.instrument.identity.venue in CRYPTO_VENUES
        }

    def _demand_keys(self):
        return {
            (item.venue, item.market, item.native_symbol, item.feed.value, item.interval)
            for item in self.manifest.demands
        }

    def test_the_committed_demand_loads(self):
        self.assertTrue(self.manifest.demands)
        self.assertGreaterEqual(self.manifest.revision, 1)

    def test_demand_and_catalog_agree_in_both_directions(self):
        demand = self._demand_keys()
        catalog = self._catalog_crypto_keys()
        self.assertTrue(demand <= catalog)

        # Dated contracts are pre-registered for the shared Rust L2 core, but
        # only the ten five-liquid perpetuals are active execution demand. A
        # new dormant capability must still be an L2 book on a dated leg; no
        # price/bar product may silently fall outside the active inventory.
        dormant = catalog - demand
        self.assertTrue(dormant)
        self.assertTrue(all(
            feed in {FeedType.BOOK_SNAPSHOT.value, FeedType.BOOK_DELTA.value}
            and (market == "FUTURES" or "_" in native_symbol)
            for _venue, market, native_symbol, feed, _interval in dormant
        ))

    def test_every_crypto_family_in_the_catalog_is_expressible(self):
        families = {
            (item.venue, item.market, item.product_type) for item in self.manifest.demands
        }
        self.assertIn(("BINANCE", "SPOT", "SPOT"), families)
        self.assertIn(("BINANCE", "USDM", "PERPETUAL"), families)
        self.assertIn(("OKX", "SPOT", "SPOT"), families)
        self.assertIn(("OKX", "SWAP", "PERPETUAL"), families)

    def test_execution_l2_demand_is_bounded_and_live(self):
        books = [
            item for item in self.manifest.demands
            if item.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}
        ]
        self.assertEqual(len(books), 20)
        self.assertEqual(
            {
                (item.venue, item.market, item.native_symbol)
                for item in books
            },
            {
                ("BINANCE", "USDM", "BTCUSDT"),
                ("BINANCE", "USDM", "ETHUSDT"),
                ("BINANCE", "USDM", "SOLUSDT"),
                ("BINANCE", "USDM", "DOGEUSDT"),
                ("BINANCE", "USDM", "BNBUSDT"),
                ("OKX", "SWAP", "BTC-USDT-SWAP"),
                ("OKX", "SWAP", "ETH-USDT-SWAP"),
                ("OKX", "SWAP", "SOL-USDT-SWAP"),
                ("OKX", "SWAP", "DOGE-USDT-SWAP"),
                ("OKX", "SWAP", "BNB-USDT-SWAP"),
            },
        )
        self.assertTrue(all(item.depth_per_side == 100 for item in books))
        self.assertTrue(all(item.require_live for item in books))
        self.assertEqual(
            {item.feed: item.max_freshness_ms for item in books},
            {
                FeedType.BOOK_SNAPSHOT: 60_000,
                FeedType.BOOK_DELTA: 2_000,
            },
        )


class AcquisitionRecipeTests(unittest.TestCase):
    """A generated acquisition must match the market and interval it was asked for."""

    def _demand(self, market: str, feed: FeedType, interval=None, venue="BINANCE"):
        return ProductionDemand(
            consumer_id="c", consumer_grade=None, venue=venue, market=market,
            product_type="SPOT" if market == "SPOT" else "PERPETUAL",
            native_symbol="BTCUSDT" if venue == "BINANCE" else "BTC-USDT-SWAP",
            feed=feed, interval=interval, source_policy_id="crypto_primary_v2",
        )

    def test_binance_spot_uses_spot_kinds_and_the_spot_endpoint(self):
        result = ProductionCatalogBuilder._acquisition(
            "b", self._demand("SPOT", FeedType.TRADE)
        )
        self.assertEqual(result["provider_kind"], "binance_spot_trade")
        self.assertIn("stream.binance.com", result["websocket_url"])
        self.assertNotIn("fstream", result["websocket_url"])

    def test_binance_usdm_keeps_its_own_kinds_and_endpoint(self):
        result = ProductionCatalogBuilder._acquisition(
            "b", self._demand("USDM", FeedType.TRADE)
        )
        self.assertEqual(result["provider_kind"], "binance_usdm_trade")
        self.assertIn("fstream.binance.com", result["websocket_url"])

    def test_binance_bar_uses_provider_rest_recovery_without_changing_rust_core(self):
        result = ProductionCatalogBuilder._acquisition(
            "b", self._demand("USDM", FeedType.BAR, interval="15m")
        )
        self.assertEqual(result["mode"], "PYTHON_REST")
        self.assertEqual(result["provider_kind"], "binance_usdm_rest_bar")
        self.assertEqual(result["native_channel"], "rest-klines/15m")
        self.assertIsNone(result["websocket_url"])

    def test_okx_swap_bar_uses_native_business_ws_and_preserves_interval(self):
        minute = ProductionCatalogBuilder._acquisition(
            "b", self._demand("SWAP", FeedType.BAR, interval="1m", venue="OKX")
        )
        hour = ProductionCatalogBuilder._acquisition(
            "b", self._demand("SWAP", FeedType.BAR, interval="1h", venue="OKX")
        )
        self.assertEqual(minute["mode"], "RUST_NATIVE")
        self.assertEqual(hour["mode"], "RUST_NATIVE")
        self.assertEqual(minute["provider_kind"], "okx_bar")
        self.assertEqual(minute["websocket_url"], "wss://ws.okx.com:8443/ws/v5/public")
        self.assertEqual(
            minute["business_websocket_url"],
            "wss://ws.okx.com:8443/ws/v5/business",
        )
        self.assertEqual(minute["native_channel"], "candle1m")
        self.assertEqual(hour["native_channel"], "candle1H")


if __name__ == "__main__":
    unittest.main()

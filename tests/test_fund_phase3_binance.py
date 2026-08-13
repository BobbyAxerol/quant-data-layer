from __future__ import annotations

import asyncio
import unittest

from qdl.adapters.binance_usdm import (
    decode_combined_frame,
    parse_exchange_info,
    websocket_url,
)
from qdl.canonical.market import canonicalize_binance_usdm_bar, canonicalize_binance_usdm_bbo
from qdl.canonical.trade import TradeContext
from qdl.ingestion.contracts import ConnectionShard, FeedType, Subscription


def context() -> TradeContext:
    return TradeContext(
        instrument_uid="85ad7cb6-7ebf-5c81-9d82-12c4c10ca85c",
        instrument_id="BINANCE.USDM.PERPETUAL.BTCUSDT", instrument_revision=1,
        venue="BINANCE", market="USDM", product_type="PERPETUAL",
        native_symbol="BTCUSDT", provider="BINANCE_DIRECT",
        source_id="binance-usdm-shadow-1", lease_epoch=4,
        received_at_ns=1_000_000_000, normalized_at_ns=1_000_000_001,
        published_at_ns=1_000_000_002, partition_sequence=5,
        normalizer_version="qdl/2", adapter_version="binance-usdm/1",
        config_revision=3,
    )


class BinanceDiscoveryTests(unittest.TestCase):
    def test_discovery_preserves_exact_filters_and_excludes_non_trading(self):
        payload = {"serverTime": 10, "symbols": [
            {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING",
             "baseAsset": "BTC", "quoteAsset": "USDT", "marginAsset": "USDT",
             "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                         {"filterType": "LOT_SIZE", "stepSize": "0.001"}]},
            {"symbol": "OLDUSDT", "contractType": "PERPETUAL", "status": "CLOSE",
             "filters": []},
        ]}
        result = parse_exchange_info(payload, valid_from_ns=20)
        self.assertEqual([item.native_symbol for item in result.records], ["BTCUSDT"])
        self.assertEqual(result.records[0].price_tick.source_text, "0.10")
        self.assertEqual(result.records[0].quantity_step.source_text, "0.001")

    def test_url_contains_every_demanded_stream_and_rejects_unknown_symbol(self):
        subscriptions = (
            Subscription("BINANCE", "USDM", FeedType.TRADE, "BTCUSDT"),
            Subscription("BINANCE", "USDM", FeedType.BBO, "BTCUSDT"),
            Subscription("BINANCE", "USDM", FeedType.BAR, "BTCUSDT", "1m"),
        )
        shard = ConnectionShard("s", "BINANCE", "USDM", FeedType.TRADE, subscriptions, 1)
        url = websocket_url(shard, {"BTCUSDT"})
        self.assertIn("btcusdt@aggTrade", url)
        self.assertIn("btcusdt@bookTicker", url)
        self.assertIn("btcusdt@kline_1m", url)
        with self.assertRaisesRegex(ValueError, "not active"):
            websocket_url(shard, {"ETHUSDT"})


class BinanceCanonicalTests(unittest.TestCase):
    def test_bbo_exact_decimal_sequence_and_time(self):
        raw = {"e": "bookTicker", "E": 1001, "T": 1000, "u": 99, "s": "BTCUSDT",
               "b": "61234.10", "B": "1.250", "a": "61234.20", "A": "0.500"}
        event = canonicalize_binance_usdm_bbo(raw, context())
        self.assertEqual(event.source_sequence, "99")
        self.assertEqual(event.source_event_time_ns, 1_000_000_000)
        self.assertEqual(event.quote.bid_price.source_text, "61234.10")

    def test_bar_preserves_open_close_and_final_flag(self):
        raw = {"e": "kline", "E": 2000, "s": "BTCUSDT", "k": {
            "t": 1000, "T": 1999, "s": "BTCUSDT", "i": "1m", "o": "10.0",
            "h": "12.0", "l": "9.0", "c": "11.0", "v": "2.50", "n": 4,
            "L": 12, "x": True,
        }}
        event = canonicalize_binance_usdm_bar(raw, context())
        self.assertTrue(event.bar.is_final)
        self.assertEqual(event.bar.open_time_ns, 1_000_000_000)
        self.assertEqual(event.bar.close_time_ns, 1_999_000_000)
        self.assertEqual(event.bar.volume.source_text, "2.50")

    def test_malformed_combined_frame_is_rejected_not_coerced(self):
        with self.assertRaises(ValueError):
            decode_combined_frame('{"stream":"x","data":{}}')


if __name__ == "__main__":
    unittest.main()

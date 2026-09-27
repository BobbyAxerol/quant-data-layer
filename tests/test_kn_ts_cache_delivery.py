import unittest
from scripts.measure_kn_ts_cache_delivery import describe, summary


class CacheDeliveryTests(unittest.TestCase):
    def test_quote_and_nested_watermark(self):
        p = {"canonical_instrument_id": "OKX:SWAP:BTC-USDT-SWAP", "timestamp_ms": 1000,
             "raw": {"v2": {"watermark_offset": 9}}}
        self.assertEqual(describe("events.market.v2.quote.OKX.SWAP.BTC-USDT-SWAP", p),
                         ("cache:market:v2:last_quote:OKX:SWAP:BTC-USDT-SWAP", "QUOTE", 1000, 9))

    def test_bar_close_not_open(self):
        p = {"canonical_instrument_id": "id", "timestamp_ms": 120000, "interval": "1m"}
        self.assertEqual(describe("events.market.v2.bar.V.P.S.1m", p)[2], 180000)
        p["interval"] = "1M"
        with self.assertRaises(ValueError):
            describe("events.market.v2.bar.V.P.S.1M", p)

    def test_execution_event_time_and_small_sample_honesty(self):
        p = {"canonical_instrument_id": "id", "feed": "MARK_INDEX_PRICE", "observed_at_ms": 1000,
             "watermark_offset": 8}
        self.assertEqual(describe("events.market.v2.execution.mark_index_price.V.P.S", p),
                         ("cache:market:v2:execution_mark_index_price:id", "MARK_INDEX_PRICE", 1000, 8))
        self.assertIsNone(summary([1, 2, 3])["p99_ms"])
        self.assertEqual(summary(list(range(1,101)))["p99_ms"], 99)

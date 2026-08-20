from __future__ import annotations

import unittest

from app.stream.feed_builder import build_urls
from qdl.adapters.binance_usdm import BINANCE_USDM_WS_BASE


class BinancePublicStreamEndpointTests(unittest.TestCase):
    def test_usdm_kline_trade_and_bbo_use_official_public_stream_path(self):
        urls = build_urls({"binance_futures": ["BTCUSDT"]}, interval="1m")
        self.assertTrue(urls["binance_futures"])
        self.assertTrue(
            all("fstream.binance.com/public/stream?streams=" in value for value in urls["binance_futures"])
        )
        self.assertEqual(
            BINANCE_USDM_WS_BASE,
            "wss://fstream.binance.com/public/stream?streams=",
        )
        self.assertNotIn("fstream.binance.com/stream?streams=", BINANCE_USDM_WS_BASE)


if __name__ == "__main__":
    unittest.main()

import unittest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from fastapi import HTTPException

from app import main
from app.api import routes_history


class TestCryptoHistoryContract(unittest.TestCase):
    def test_binance_interval_validation(self):
        self.assertEqual(main._normalize_binance_interval("15m"), "15m")
        self.assertEqual(main._normalize_binance_interval("1h"), "1h")
        with self.assertRaises(HTTPException):
            main._normalize_binance_interval("7m")

    def test_okx_interval_mapping(self):
        self.assertEqual(main._normalize_okx_interval("15m"), "15m")
        self.assertEqual(main._normalize_okx_interval("1h"), "1H")
        self.assertEqual(main._normalize_okx_interval("1Dutc"), "1Dutc")
        with self.assertRaises(HTTPException):
            main._normalize_okx_interval("8h")

    def test_okx_symbol_mapping(self):
        self.assertEqual(main._okx_symbol("BTCUSDT"), "BTC-USDT")
        self.assertEqual(main._okx_symbol("ETH-USDT"), "ETH-USDT")
        self.assertEqual(main._okx_symbol("SOLUSDC"), "SOL-USDC")

    def test_binance_market_url_selection(self):
        self.assertEqual(main._binance_kline_urls("spot")[0][0], "spot")
        self.assertEqual(main._binance_kline_urls("usdm")[0][0], "usdm")
        self.assertEqual([row[0] for row in main._binance_kline_urls("auto")], ["usdm", "spot"])
        with self.assertRaises(HTTPException):
            main._binance_kline_urls("coinm")

    def test_daily_preload_schedule_targets_16h_vn(self):
        vn_tz = timezone(timedelta(hours=7))
        before = datetime(2026, 1, 2, 15, 0, tzinfo=vn_tz)
        after = datetime(2026, 1, 2, 17, 0, tzinfo=vn_tz)

        self.assertEqual(main._next_daily_preload_run(before).hour, 16)
        next_after = main._next_daily_preload_run(after)
        self.assertEqual(next_after.hour, 16)
        self.assertGreater(next_after.date(), after.date())

    def test_crypto_batch_history_fetches_symbols_with_bounded_contract(self):
        def fake_fetch(symbol, interval, limit, start_time, end_time, market):
            return {
                "provider": "binance",
                "symbol": symbol,
                "requested_interval": interval,
                "data": [[1760000000000, "1", "2", "0.5", "1.5", "10"]],
            }

        with patch("app.api.routes_history.binance_rest.fetch_klines", side_effect=fake_fetch) as fetch:
            payload = asyncio.run(
                routes_history.post_crypto_ohlcv_batch(
                    "binance",
                    {
                        "symbols": ["btcusdt", "ethusdt", "BTCUSDT"],
                        "interval": "15m",
                        "limit": 2,
                        "concurrency": 2,
                        "market": "usdm",
                    },
                )
            )

        self.assertEqual(payload["requested_count"], 2)
        self.assertEqual(payload["success_count"], 2)
        self.assertEqual(payload["error_count"], 0)
        self.assertEqual(sorted(payload["results"]), ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(fetch.call_count, 2)


if __name__ == "__main__":
    unittest.main()

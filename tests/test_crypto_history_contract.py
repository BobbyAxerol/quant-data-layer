import unittest
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException

from app import main


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


if __name__ == "__main__":
    unittest.main()

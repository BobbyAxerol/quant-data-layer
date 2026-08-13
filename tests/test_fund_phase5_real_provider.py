from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from scripts.phase5_real_provider_smoke import run


class Phase5RealProviderSmokeTests(unittest.TestCase):
    @patch("scripts.phase5_real_provider_smoke.requests.get")
    def test_smoke_is_read_only_and_checks_v1_canonical_value_parity(self, get: Mock):
        health = Mock()
        health.json.return_value = {"status": "ok"}
        trade = Mock()
        trade.json.return_value = {"snapshot": {
            "symbol": "BTCUSDT", "price": 60000.1, "quantity": 0.02,
            "trade_id": 7, "event_time": 1000, "market": "binance_usdm",
            "provider": "binance", "raw": {
                "s": "BTCUSDT", "t": 7, "p": "60000.10", "q": "0.020",
                "E": 1000, "T": 1000, "m": False,
            },
        }}
        history = Mock()
        history.json.return_value = {
            "instId": "BTC-USDT-SWAP", "provider_interval": "1m",
            "data": [["2000", "1", "2", "1", "2", "3"],
                     ["1000", "1", "2", "1", "2", "3"]],
            "cached": False, "stored": False,
        }
        get.side_effect = (health, trade, history)
        result = run("http://data-layer", timeout_seconds=1)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["production_writes"], 0)
        self.assertTrue(all(result["binance"]["canonical_parity"].values()))
        self.assertFalse(result["okx"]["stored"])
        self.assertEqual(get.call_count, 3)


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import main
from app.market_universe import registry
from app.providers.binance import rest as binance_rest
from app.providers.okx import rest as okx_rest


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else []
        self.text = text

    def json(self):
        return self._payload


class TestPhase3ControlPlane(unittest.TestCase):
    def test_phase3_routes_are_registered(self):
        paths = {route.path for route in main.app.routes}

        self.assertIn("/v1/control/universe/configured", paths)
        self.assertIn("/v1/control/universe/active", paths)
        self.assertIn("/v1/control/provider-priority", paths)
        self.assertIn("/v1/control/session-calendar", paths)
        self.assertIn("/v1/fallback/crypto/reference/{symbol}", paths)
        self.assertIn("/v1/fallback/crypto/status/{symbol}", paths)
        self.assertIn("/v1/crypto/ohlcv/{provider}/{symbol}", paths)
        self.assertIn("/v1/preload/{symbol}", paths)

    def test_configured_universe_loads_provider_symbols(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binance_file = root / "symbols.json"
            vn_file = root / "symbols_vn.yaml"
            binance_file.write_text(json.dumps(["btcusdt", "ETHUSDT", "BTCUSDT"]), encoding="utf-8")
            vn_file.write_text("symbols:\n  - fpt\n  - HPG\n  - FPT\n", encoding="utf-8")

            with patch.object(registry, "BINANCE_SYMBOLS_FILE", str(binance_file)), \
                patch.object(registry, "VN_SYMBOLS_FILE", vn_file):
                universe = registry.configured_universe()

        self.assertEqual(universe["binance"]["symbols"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(universe["dnse"]["symbols"], ["FPT", "HPG"])

    def test_provider_priority_contract_is_explicit(self):
        priority = registry.provider_priority()

        self.assertEqual(priority["crypto"]["history"], ["binance", "okx"])
        self.assertEqual(priority["crypto"]["fallback_reference"], ["okx"])
        self.assertEqual(priority["crypto"]["fallback_policy"], "explicit_only")
        self.assertIn("vnstock", priority["vn_stock"]["quote"])

    def test_binance_provider_fetch_uses_interval_and_market(self):
        calls = []

        def fake_get(url, params, timeout):
            calls.append({"url": url, "params": params, "timeout": timeout})
            return _FakeResponse(payload=[[1, "1", "2", "0.5", "1.5", "100"]])

        payload = binance_rest.fetch_klines(
            "BTCUSDT",
            interval="15m",
            limit=2,
            market="spot",
            http_get=fake_get,
        )

        self.assertEqual(payload["provider"], "binance")
        self.assertEqual(payload["market"], "spot")
        self.assertEqual(payload["provider_interval"], "15m")
        self.assertEqual(calls[0]["params"]["interval"], "15m")

    def test_okx_provider_maps_symbol_and_interval(self):
        def fake_get(url, params, timeout):
            return _FakeResponse(payload={"code": "0", "data": [["1", "1", "2", "0.5", "1.5", "100"]]})

        payload = okx_rest.fetch_candles(
            "BTCUSDT",
            interval="1h",
            limit=500,
            http_get=fake_get,
        )

        self.assertEqual(payload["provider"], "okx")
        self.assertEqual(payload["instId"], "BTC-USDT")
        self.assertEqual(payload["provider_interval"], "1H")
        self.assertEqual(payload["params"]["limit"], 300)


if __name__ == "__main__":
    unittest.main()

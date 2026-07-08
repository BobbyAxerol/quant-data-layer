import asyncio
import unittest
from unittest.mock import MagicMock, patch

from app.api import routes_binance_derivatives
from app.providers.binance import derivatives
from app.providers.binance.rest import BinanceProviderError
from app.sdk.client import DataLayerClient


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else []
        self.text = text

    def json(self):
        return self._payload


class TestBinanceDerivativesContract(unittest.TestCase):
    def test_period_validation(self):
        self.assertEqual(derivatives.normalize_period("1d"), "1d")
        with self.assertRaises(ValueError):
            derivatives.normalize_period("7m")

    def test_fetch_metric_history_uses_official_endpoint_and_normalized_payload(self):
        def fake_get(url, params=None, headers=None, timeout=None):
            self.assertTrue(url.endswith("/futures/data/openInterestHist"))
            self.assertEqual(params["symbol"], "BTCUSDT")
            self.assertEqual(params["period"], "1d")
            self.assertEqual(params["limit"], 30)
            return FakeResponse(payload=[{"symbol": "BTCUSDT", "sumOpenInterest": "1", "timestamp": "1"}])

        payload = derivatives.fetch_metric_history(
            "open_interest_hist",
            "btcusdt",
            "1d",
            30,
            http_get=fake_get,
            max_attempts=1,
        )

        self.assertEqual(payload["provider"], "binance")
        self.assertEqual(payload["endpoint"], "open_interest_hist")
        self.assertEqual(payload["symbol"], "BTCUSDT")
        self.assertFalse(payload["stored"])
        self.assertTrue(payload["latest_30_days_only"])

    def test_retry_then_success_on_retryable_status(self):
        calls = {"count": 0}

        def fake_get(url, params=None, headers=None, timeout=None):
            calls["count"] += 1
            if calls["count"] == 1:
                return FakeResponse(status_code=502, text="bad gateway")
            return FakeResponse(payload={"symbol": "BTCUSDT", "openInterest": "10", "time": 1})

        payload = derivatives.fetch_open_interest(
            "BTCUSDT",
            http_get=fake_get,
            max_attempts=2,
            backoff_seconds=0,
        )

        self.assertEqual(calls["count"], 2)
        self.assertEqual(payload["data"]["openInterest"], "10")

    def test_non_retryable_status_raises_provider_error(self):
        def fake_get(url, params=None, headers=None, timeout=None):
            return FakeResponse(status_code=400, text="bad request")

        with self.assertRaises(BinanceProviderError) as ctx:
            derivatives.fetch_depth("BTCUSDT", http_get=fake_get, max_attempts=3)
        self.assertEqual(len(ctx.exception.attempts), 1)

    def test_basis_bundle_is_partial_when_component_fails(self):
        def fake_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/fapi/v1/depth") and params["symbol"] == "BTCUSDT_260925":
                return FakeResponse(status_code=502, text="delivery depth unavailable")
            return FakeResponse(payload=[{"ok": True, "params": params}])

        payload = derivatives.fetch_basis_bundle(
            "BTCUSDT",
            "BTCUSDT_260925",
            pair="BTCUSDT",
            include_depth=True,
            http_get=fake_get,
            max_attempts=1,
        )

        self.assertTrue(payload["partial"])
        self.assertIn("delivery_depth", payload["errors"])
        self.assertIn("perp_klines", payload["components"])
        self.assertEqual(payload["delivery_symbol"], "BTCUSDT_260925")

    def test_route_basis_bundle_requires_symbols(self):
        with self.assertRaises(Exception) as ctx:
            asyncio.run(routes_binance_derivatives.post_basis_bundle({"perp_symbol": "BTCUSDT"}))
        self.assertIn("missing_required_field", str(ctx.exception.detail))

    def test_sdk_basis_bundle_posts_contract_payload(self):
        session = MagicMock()
        redis_client = MagicMock()
        response = MagicMock(status_code=200, text="")
        response.json.return_value = {"kind": "basis_bundle", "components": {}}
        response.raise_for_status.return_value = None
        session.post.return_value = response
        client = DataLayerClient(session=session, redis_client=redis_client)

        payload = client.binance_basis_bundle(
            "btcusdt",
            "btcusdt_260925",
            pair="btcusdt",
            include_depth=False,
        )

        self.assertEqual(payload["kind"], "basis_bundle")
        session.post.assert_called_once()
        _, kwargs = session.post.call_args
        self.assertEqual(kwargs["json"]["perp_symbol"], "BTCUSDT")
        self.assertEqual(kwargs["json"]["delivery_symbol"], "BTCUSDT_260925")
        self.assertFalse(kwargs["json"]["include_depth"])

    def test_route_klines_delegates_to_provider(self):
        with patch("app.api.routes_binance_derivatives.binance_derivatives.fetch_klines") as fetch:
            fetch.return_value = {"endpoint": "klines", "symbol": "BTCUSDT_260925"}
            payload = asyncio.run(
                routes_binance_derivatives.get_derivative_klines(
                    "BTCUSDT_260925",
                    interval="1d",
                    limit=30,
                )
            )
        self.assertEqual(payload["symbol"], "BTCUSDT_260925")
        fetch.assert_called_once_with("BTCUSDT_260925", "1d", 30, None, None)


if __name__ == "__main__":
    unittest.main()

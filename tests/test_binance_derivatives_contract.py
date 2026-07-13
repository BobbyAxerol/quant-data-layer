import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from app.api import routes_binance_derivatives
from app.providers.binance import basis_continuous
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

    def test_continuous_basis_builder_stitches_research_style_contracts(self):
        idx = pd.date_range("2026-01-01", periods=140, freq="1D", tz="UTC")
        perp = pd.DataFrame(
            {
                "symbol": "BTCUSDT",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": [100.0 + i for i in range(len(idx))],
                "volume": 1000.0,
            },
            index=idx,
        )
        first = pd.DataFrame(
            {
                "symbol": "BTCUSDT_260327",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": [101.0 + i for i in range(len(idx))],
                "volume": 100.0,
            },
            index=idx,
        )
        second = pd.DataFrame(
            {
                "symbol": "BTCUSDT_260626",
                "open": 102.0,
                "high": 103.0,
                "low": 101.0,
                "close": [102.0 + i for i in range(len(idx))],
                "volume": 200.0,
            },
            index=idx,
        )
        builder = basis_continuous.ContinuousBasisBuilder()
        with patch.object(builder, "_perp_frame", return_value=perp), patch.object(
            builder,
            "_vision_kline_frame",
            side_effect=lambda symbol, *_args, **_kwargs: first if symbol.endswith("260327") else second,
        ), patch.object(
            builder,
            "_candidate_quarterlies",
            return_value=["BTCUSDT_260327", "BTCUSDT_260626"],
        ):
            frame, meta = builder.build(
                basis_continuous.ContinuousBasisRequest(
                    pair="BTCUSDT",
                    lookback_days=40,
                    buffer_days=2,
                    end_time=datetime(2026, 5, 20, tzinfo=timezone.utc),
                )
            )

        self.assertEqual(len(frame), 40)
        self.assertIn("active_contract", frame)
        self.assertTrue(set(frame["active_contract"]).issubset({"BTCUSDT_260327", "BTCUSDT_260626"}))
        self.assertIn("BTCUSDT_260327", meta["candidate_contracts"])

    def test_route_continuous_basis_bundle_delegates_to_provider(self):
        with patch("app.api.routes_binance_derivatives.basis_continuous.fetch_continuous_basis_bundle") as fetch:
            fetch.return_value = {"kind": "continuous_basis_bundle", "rows": 365}
            payload = asyncio.run(
                routes_binance_derivatives.post_continuous_basis_bundle(
                    {"pair": "btcusdt", "lookback_days": 365, "interval": "1d"}
                )
            )
        self.assertEqual(payload["kind"], "continuous_basis_bundle")
        fetch.assert_called_once()
        self.assertEqual(fetch.call_args.kwargs["pair"], "BTCUSDT")

    def test_sdk_continuous_basis_bundle_posts_contract_payload(self):
        session = MagicMock()
        redis_client = MagicMock()
        response = MagicMock(status_code=200, text="")
        response.json.return_value = {"kind": "continuous_basis_bundle", "rows": 365}
        response.raise_for_status.return_value = None
        session.post.return_value = response
        client = DataLayerClient(session=session, redis_client=redis_client)

        payload = client.binance_continuous_basis_bundle("btcusdt", current_delivery_symbol="btcusdt_260925")

        self.assertEqual(payload["kind"], "continuous_basis_bundle")
        _, kwargs = session.post.call_args
        self.assertEqual(kwargs["json"]["pair"], "BTCUSDT")
        self.assertEqual(kwargs["json"]["current_delivery_symbol"], "BTCUSDT_260925")


if __name__ == "__main__":
    unittest.main()

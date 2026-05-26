import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from app.sdk.client import DataLayerClient


class TestOfficialDataLayerClient(unittest.TestCase):
    def _client(self):
        session = MagicMock()
        redis_client = MagicMock()
        return DataLayerClient(
            base_url="http://data_layer:8100",
            session=session,
            redis_client=redis_client,
        ), session, redis_client

    def test_warmup_ohlcv_routes_vn_to_preload(self):
        client, session, _ = self._client()
        response = MagicMock(status_code=200, text="")
        response.json.return_value = {"symbol": "FPT", "interval": "5m", "data": []}
        response.raise_for_status.return_value = None
        session.get.return_value = response

        payload = client.warmup_ohlcv("vn_stock", "fpt", interval="5m", limit=3, fresh=False)

        self.assertEqual(payload["symbol"], "FPT")
        session.get.assert_called_once_with(
            "http://data_layer:8100/v1/preload/FPT",
            params={"interval": "5m", "limit": 3, "fresh": False},
            timeout=15.0,
        )

    def test_warmup_ohlcv_routes_crypto_to_provider_wrapper(self):
        client, session, _ = self._client()
        response = MagicMock(status_code=200, text="")
        response.json.return_value = {"provider": "okx", "symbol": "BTCUSDT", "data": []}
        response.raise_for_status.return_value = None
        session.get.return_value = response

        payload = client.warmup_ohlcv("crypto", "btcusdt", provider="okx", interval="15m", limit=1)

        self.assertEqual(payload["provider"], "okx")
        session.get.assert_called_once_with(
            "http://data_layer:8100/v1/crypto/ohlcv/okx/BTCUSDT",
            params={"interval": "15m", "limit": 1},
            timeout=15.0,
        )

    def test_stream_helpers_subscribe_to_standard_channels(self):
        client, _, redis_client = self._client()
        pubsub = MagicMock()
        redis_client.pubsub.return_value = pubsub

        client.stream_trades(["BTCUSDT", "ETHUSDT"])
        client.stream_klines("BTCUSDT", interval="1m")
        client.stream_vn_quotes("FPT")

        pubsub.subscribe.assert_any_call("stream:trade:BTCUSDT", "stream:trade:ETHUSDT")
        pubsub.subscribe.assert_any_call("stream:kline:1m:BTCUSDT")
        pubsub.subscribe.assert_any_call("stream:vn:FPT")

    def test_freshness_helper_handles_millisecond_event_time(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        payload = {"event_time": int((now - timedelta(seconds=5)).timestamp() * 1000)}

        freshness = DataLayerClient.validate_freshness(payload, max_age_seconds=10, now=now)

        self.assertTrue(freshness["fresh"])
        self.assertEqual(freshness["reason"], "fresh")

    def test_source_helper_accepts_provider_or_source(self):
        self.assertTrue(DataLayerClient.validate_source({"source": "binance_spot_trade"}, ["binance_spot_trade"]))
        self.assertTrue(DataLayerClient.validate_source({"provider": "okx"}, ["okx"]))
        self.assertFalse(DataLayerClient.validate_source({"provider": "okx"}, ["binance"]))


if __name__ == "__main__":
    unittest.main()


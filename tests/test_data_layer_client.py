import unittest
from unittest.mock import MagicMock, patch

from app.alpha.strategy import DataLayerClient


class TestDataLayerClient(unittest.TestCase):
    @patch("app.sdk.client.redis.Redis")
    @patch("app.sdk.client.requests.Session")
    def test_fetch_preload_requests_expected_endpoint(self, mock_session_cls, mock_redis_cls):
        response = MagicMock()
        response.status_code = 200
        response.text = ""
        response.json.return_value = {
            "symbol": "SSI",
            "count": 1,
            "data": [
                {
                    "time": "2026-01-01T09:00:00",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 10,
                }
            ],
        }
        response.raise_for_status.return_value = None
        session = mock_session_cls.return_value
        session.get.return_value = response

        client = DataLayerClient(base_url="http://data_layer:8100")
        df = client.fetch_preload("SSI", interval="15m")

        session.get.assert_called_once_with(
            "http://data_layer:8100/v1/preload/SSI",
            params={"interval": "15m", "limit": 1000, "fresh": True},
            timeout=15.0,
        )
        self.assertEqual(len(df), 1)
        self.assertEqual(df["close"].iloc[0], 100.5)

    @patch("app.sdk.client.requests.Session")
    @patch("app.sdk.client.redis.Redis")
    def test_get_cached_quote_parses_bytes_from_redis(self, mock_redis, mock_session_cls):
        redis_instance = mock_redis.return_value
        redis_instance.get.return_value = b'{"last": 120.0, "symbol": "SSI"}'

        client = DataLayerClient(base_url="http://data_layer:8100")
        quote = client.get_cached_quote("SSI")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["last"], 120.0)
        redis_instance.get.assert_called_once_with("vn:quote:SSI")


if __name__ == "__main__":
    unittest.main()

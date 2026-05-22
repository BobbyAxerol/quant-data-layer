import unittest
from unittest.mock import MagicMock, patch

from app.alpha.strategy import DataLayerClient


class TestDataLayerClient(unittest.TestCase):
    @patch("app.alpha.strategy.requests.get")
    def test_fetch_preload_requests_expected_endpoint(self, mock_get):
        response = MagicMock()
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
        mock_get.return_value = response

        client = DataLayerClient(base_url="http://data_layer:8100")
        df = client.fetch_preload("SSI")

        mock_get.assert_called_once_with(
            "http://data_layer:8100/v1/preload/SSI",
            params={},
            timeout=15,
        )
        self.assertEqual(len(df), 1)
        self.assertEqual(df["close"].iloc[0], 100.5)

    @patch("app.alpha.strategy.redis.Redis")
    def test_get_cached_quote_parses_bytes_from_redis(self, mock_redis):
        redis_instance = mock_redis.return_value
        redis_instance.get.return_value = b'{"last": 120.0, "symbol": "SSI"}'

        client = DataLayerClient(base_url="http://data_layer:8100")
        quote = client.get_cached_quote("SSI")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["last"], 120.0)
        redis_instance.get.assert_called_once_with("vn:quote:SSI")


if __name__ == "__main__":
    unittest.main()

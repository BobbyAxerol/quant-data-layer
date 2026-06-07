import unittest

from fastapi import HTTPException

from app.api.routes_latest import get_binance_price_last
from app.cache.redis_cache import RedisCache


class FakeCache:
    def __init__(self, *, latest=None, last=None):
        self.latest = latest
        self.last = last
        self.latest_calls = []
        self.last_calls = []

    async def get_binance_price(self, symbol, market="auto"):
        self.latest_calls.append((symbol, market))
        return self.latest

    async def get_binance_price_last(self, symbol, market="auto"):
        self.last_calls.append((symbol, market))
        return self.last


class FakeContext:
    def __init__(self, cache):
        self.redis_cache = cache


class TestLatestPriceLast(unittest.IsolatedAsyncioTestCase):
    def test_binance_market_aliases_normalize_to_internal_namespace(self):
        self.assertEqual(RedisCache._normalize_binance_market("usdm"), "binance_usdm")
        self.assertEqual(RedisCache._normalize_binance_market("spot"), "binance_spot")
        self.assertEqual(RedisCache._normalize_binance_market("USD_M_FUTURES"), "binance_usdm")

    async def test_returns_explicit_last_snapshot_with_live_flag(self):
        payload = {"symbol": "1000SATSUSDT", "price": 0.00001158, "trade_time": 1700000000000}

        cache = FakeCache(latest=None, last=payload)
        result = await get_binance_price_last(
            "1000satsusdt",
            market="usdm",
            ctx=FakeContext(cache),
        )

        self.assertEqual(result["symbol"], "1000SATSUSDT")
        self.assertEqual(result["market"], "usdm")
        self.assertFalse(result["is_live"])
        self.assertEqual(result["snapshot"], payload)
        self.assertEqual(cache.last_calls, [("1000SATSUSDT", "usdm")])
        self.assertEqual(cache.latest_calls, [("1000SATSUSDT", "usdm")])

    async def test_missing_last_snapshot_returns_404(self):
        with self.assertRaises(HTTPException) as raised:
            await get_binance_price_last("missing", ctx=FakeContext(FakeCache()))

        self.assertEqual(raised.exception.status_code, 404)

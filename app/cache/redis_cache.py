"""
Ultra-lean Redis cache manager — upgraded with pipeline batching and orjson.

Stores exactly 1 event per symbol with a 60-second TTL.
Any older data is auto-evicted by Redis.

Upgrade notes (from upgrade-redis-ws-streamming.md):
- orjson for fast serialization (returns bytes, avoids UTF-8 overhead)
- decode_responses=False to stay in bytes end-to-end
- pipeline() to batch SETEX + PUBLISH in 1 roundtrip
"""
import logging
from typing import Optional

import orjson
import redis
import redis.asyncio as redis_async

from app.config import REDIS_HOST, REDIS_PORT, REDIS_DB

logger = logging.getLogger(__name__)

class RedisCache:
    """
    Ultra-lean Redis cache (ASYNC):
    - 1 event max per symbol key (SETEX overwrites previous)
    - 60-second TTL guarantees zero memory bloat
    - Pub/Sub for real-time distribution
    - Pipeline batching: SETEX + PUBLISH in 1 roundtrip
    - orjson bytes serialization
    """

    def __init__(self):
        self.r = redis_async.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=False,
        )
        self.sync_r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=False,
        )
        logger.info(f"RedisCache initialized for {REDIS_HOST}:{REDIS_PORT} db={REDIS_DB}")

    @staticmethod
    def _decode_payload(raw) -> Optional[dict]:
        return orjson.loads(raw) if raw else None

    @staticmethod
    def _vn_last_key(symbol: str) -> str:
        return f"vn:quote:last:{symbol}"

    @staticmethod
    def _last_known_key(key: str) -> Optional[str]:
        if key.startswith("trade:price:"):
            parts = key.split(":")
            if len(parts) == 4:
                _, _, market, symbol = parts
                return f"trade:price:last:{market}:{symbol}"
            return f"trade:price:last:{parts[-1]}"
        if key.startswith("kline:"):
            parts = key.split(":")
            if len(parts) == 3:
                _, interval, symbol = parts
                return f"kline:last:{interval}:{symbol}"
        if key.startswith("vn:quote:") and not key.startswith("vn:quote:last:"):
            return RedisCache._vn_last_key(key.split(":")[-1])
        return None

    @staticmethod
    def _market_trade_key(symbol: str, market: str) -> str:
        return f"trade:price:{market}:{symbol}"

    @staticmethod
    def _market_trade_last_key(symbol: str, market: str) -> str:
        return f"trade:price:last:{market}:{symbol}"

    @staticmethod
    def _normalize_binance_market(market: str) -> str:
        value = str(market or "auto").lower().strip().replace("-", "_")
        aliases = {
            "spot": "binance_spot",
            "binance_spot": "binance_spot",
            "usdm": "binance_usdm",
            "usd_m": "binance_usdm",
            "usd_m_futures": "binance_usdm",
            "futures": "binance_usdm",
            "binance_futures": "binance_usdm",
            "binance_usdm": "binance_usdm",
        }
        return aliases.get(value, value)

    async def init_ping(self):
        try:
            await self.r.ping()
            logger.debug("Async Redis connection verified (ping successful)")
        except redis.ConnectionError as e:
            logger.error(f"Async Redis connection verification FAILED: {e}")
            raise

    # ── Binance Kline ───────────────────────────────────────────

    async def push_binance_kline(self, symbol: str, interval: str, data: dict, ttl: int = 60):
        key = f"kline:{interval}:{symbol}"
        last_key = self._last_known_key(key)
        channel = f"stream:kline:{interval}:{symbol}"
        payload = orjson.dumps(data)
        async with self.r.pipeline(transaction=False) as pipe:
            pipe.setex(key, ttl, payload)
            if last_key:
                pipe.set(last_key, payload)
            pipe.publish(channel, payload)
            await pipe.execute()

    async def get_binance_kline(self, symbol: str, interval: str) -> Optional[dict]:
        key = f"kline:{interval}:{symbol}"
        raw = await self.r.get(key)
        return self._decode_payload(raw)

    async def get_binance_kline_last(self, symbol: str, interval: str) -> Optional[dict]:
        raw = await self.r.get(f"kline:last:{interval}:{symbol}")
        return self._decode_payload(raw)

    async def get_binance_price(self, symbol: str, market: str = "auto") -> Optional[dict]:
        market = self._normalize_binance_market(market)
        if market not in {"", "auto", "legacy"}:
            raw_market = await self.r.get(self._market_trade_key(symbol, market))
            return self._decode_payload(raw_market)
        key = f"trade:price:{symbol}"
        raw = await self.r.get(key)
        return self._decode_payload(raw)

    async def get_binance_price_last(self, symbol: str, market: str = "auto") -> Optional[dict]:
        market = self._normalize_binance_market(market)
        if market not in {"", "auto", "legacy"}:
            raw_market = await self.r.get(self._market_trade_last_key(symbol, market))
            return self._decode_payload(raw_market)
        raw = await self.r.get(f"trade:price:last:{symbol}")
        return self._decode_payload(raw)

    # ── VN Stock Tick / Quote ───────────────────────────────────

    async def push_vn_quote(self, symbol: str, data: dict, ttl: int = 60):
        key = f"vn:quote:{symbol}"
        last_key = self._vn_last_key(symbol)
        channel = f"stream:vn:{symbol}"
        payload = orjson.dumps(data)
        async with self.r.pipeline(transaction=False) as pipe:
            pipe.setex(key, ttl, payload)
            pipe.set(last_key, payload)
            pipe.publish(channel, payload)
            await pipe.execute()

    async def get_vn_quote(self, symbol: str) -> Optional[dict]:
        key = f"vn:quote:{symbol}"
        raw = await self.r.get(key)
        return self._decode_payload(raw)

    async def get_vn_quote_last(self, symbol: str) -> Optional[dict]:
        raw = await self.r.get(self._vn_last_key(symbol))
        return self._decode_payload(raw)

    async def set_vn_quote_last(self, symbol: str, data: dict):
        await self.r.set(self._vn_last_key(symbol), orjson.dumps(data))

    def get_vn_quote_sync(self, symbol: str) -> Optional[dict]:
        key = f"vn:quote:{symbol}"
        raw = self.sync_r.get(key)
        return self._decode_payload(raw)

    def get_vn_quote_last_sync(self, symbol: str) -> Optional[dict]:
        raw = self.sync_r.get(self._vn_last_key(symbol))
        return self._decode_payload(raw)

    def set_vn_quote_last_sync(self, symbol: str, data: dict):
        self.sync_r.set(self._vn_last_key(symbol), orjson.dumps(data))

    # ── Batch pipeline ──────────────────────────────────────────

    async def push_batch(self, items: list, ttl: int = 60):
        if not items:
            return
        async with self.r.pipeline(transaction=False) as pipe:
            for item in items:
                payload = orjson.dumps(item["data"])
                pipe.setex(item["key"], ttl, payload)
                last_key = self._last_known_key(item["key"])
                if last_key:
                    pipe.set(last_key, payload)
                pipe.publish(item["channel"], payload)
            await pipe.execute()

    def push_batch_sync(self, items: list, ttl: int = 60):
        if not items:
            return
        with self.sync_r.pipeline(transaction=False) as pipe:
            for item in items:
                payload = orjson.dumps(item["data"])
                pipe.setex(item["key"], ttl, payload)
                last_key = self._last_known_key(item["key"])
                if last_key:
                    pipe.set(last_key, payload)
                pipe.publish(item["channel"], payload)
            pipe.execute()

    # ── Board helpers ───────────────────────────────────────────

    async def push_vn_board(self, board_data: dict, ttl: int = 60):
        key = b"vn:board:latest"
        payload = orjson.dumps(board_data)
        await self.r.setex(key, ttl, payload)

    async def get_vn_board(self) -> Optional[dict]:
        raw = await self.r.get(b"vn:board:latest")
        return self._decode_payload(raw)

    def push_vn_quote_sync(self, symbol: str, data: dict, ttl: int = 60):
        key = f"vn:quote:{symbol}"
        last_key = self._vn_last_key(symbol)
        channel = f"stream:vn:{symbol}"
        payload = orjson.dumps(data)
        with self.sync_r.pipeline(transaction=False) as pipe:
            pipe.setex(key, ttl, payload)
            pipe.set(last_key, payload)
            pipe.publish(channel, payload)
            pipe.execute()

    async def health_check(self) -> bool:
        try:
            return await self.r.ping()
        except Exception:
            return False

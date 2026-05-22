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
        channel = f"stream:kline:{interval}:{symbol}"
        payload = orjson.dumps(data)
        async with self.r.pipeline(transaction=False) as pipe:
            pipe.setex(key, ttl, payload)
            pipe.publish(channel, payload)
            await pipe.execute()

    async def get_binance_kline(self, symbol: str, interval: str) -> Optional[dict]:
        key = f"kline:{interval}:{symbol}"
        raw = await self.r.get(key)
        return self._decode_payload(raw)

    async def get_binance_price(self, symbol: str) -> Optional[dict]:
        key = f"trade:price:{symbol}"
        raw = await self.r.get(key)
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
                if item["key"].startswith("vn:quote:") and not item["key"].startswith("vn:quote:last:"):
                    symbol = item["key"].split(":")[-1]
                    pipe.set(self._vn_last_key(symbol), payload)
                pipe.publish(item["channel"], payload)
            await pipe.execute()

    def push_batch_sync(self, items: list, ttl: int = 60):
        if not items:
            return
        with self.sync_r.pipeline(transaction=False) as pipe:
            for item in items:
                payload = orjson.dumps(item["data"])
                pipe.setex(item["key"], ttl, payload)
                if item["key"].startswith("vn:quote:") and not item["key"].startswith("vn:quote:last:"):
                    symbol = item["key"].split(":")[-1]
                    pipe.set(self._vn_last_key(symbol), payload)
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

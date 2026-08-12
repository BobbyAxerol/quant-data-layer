from __future__ import annotations

import asyncio
import json
import os
import time
import unittest
import uuid

from redis.asyncio import Redis

from app.history.topup_coordinator import PreloadTopupCoordinator
from app.stream.demand_registry import FeedDemand, FeedDemandRegistry


@unittest.skipUnless(os.getenv("TEST_REDIS_URL"), "TEST_REDIS_URL is required")
class Phase2RedisIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
        self.scope = uuid.uuid4().hex
        self.registry = FeedDemandRegistry(self.redis, prefix=f"test:{self.scope}:feed:demand")

    async def asyncTearDown(self):
        keys = [
            key
            async for key in self.redis.scan_iter(match=f"test:{self.scope}:*")
        ]
        if keys:
            await self.redis.delete(*keys)
        await self.redis.aclose()

    async def test_real_redis_refcount_and_cross_coordinator_singleflight(self):
        demand = FeedDemand("binance_futures_trade", "trade", "BTCUSDT")
        await self.registry.upsert("alpha-a", [demand], ttl_seconds=60)
        await self.registry.upsert("order-recovery", [demand], ttl_seconds=60)

        snapshot = await self.registry.snapshot()
        self.assertEqual(snapshot["lease_count"], 2)
        self.assertEqual(snapshot["items"][0]["refcount"], 2)

        calls: list[str] = []

        def topup(symbol, **_):
            calls.append(symbol)
            time.sleep(0.1)
            return {"needed": True, "symbol": symbol}

        first = PreloadTopupCoordinator(self.redis, topup)
        second = PreloadTopupCoordinator(self.redis, topup)
        results = await asyncio.gather(
            first.run("FPT", interval="1m", max_lag_minutes=5),
            second.run("FPT", interval="1m", max_lag_minutes=5),
        )

        self.assertEqual(calls, ["FPT"])
        self.assertEqual(len(results), 2)
        self.assertEqual(await self.registry.release_owner("alpha-a"), 1)
        self.assertEqual((await self.registry.snapshot())["items"][0]["refcount"], 1)

    async def test_one_thousand_leases_have_bounded_latency_and_memory(self):
        before = int((await self.redis.info("memory"))["used_memory"])
        started = time.monotonic()
        await asyncio.gather(
            *[
                self.registry.upsert(
                    f"alpha-{owner}",
                    [
                        FeedDemand(
                            "binance_futures_trade",
                            "trade",
                            f"TEST{symbol:03d}USDT",
                        )
                        for symbol in range(10)
                    ],
                    ttl_seconds=60,
                )
                for owner in range(100)
            ]
        )
        write_seconds = time.monotonic() - started
        snapshot_started = time.monotonic()
        snapshot = await self.registry.snapshot()
        snapshot_seconds = time.monotonic() - snapshot_started
        after = int((await self.redis.info("memory"))["used_memory"])

        print(json.dumps({
            "lease_count": snapshot["lease_count"],
            "demanded_feed_count": snapshot["demanded_feed_count"],
            "write_seconds": round(write_seconds, 4),
            "snapshot_seconds": round(snapshot_seconds, 4),
            "memory_delta_bytes": after - before,
        }, sort_keys=True))

        self.assertEqual(snapshot["lease_count"], 1000)
        self.assertEqual(snapshot["demanded_feed_count"], 10)
        self.assertLess(write_seconds, 5.0)
        self.assertLess(snapshot_seconds, 2.0)
        self.assertLess(after - before, 5 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()

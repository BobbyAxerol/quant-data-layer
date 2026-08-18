from __future__ import annotations

import asyncio
import fnmatch
import json
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.history.topup_coordinator import PreloadTopupBackoff, PreloadTopupCoordinator
from app.stream.binance_ws import get_usdm_symbols
from app.stream.demand_registry import FeedDemand, FeedDemandRegistry
from app.stream.supervisor import StreamSupervisor


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def set(self, key, value, ex=None, **kwargs):
        self.commands.append((key, value, ex, kwargs))
        return self

    async def execute(self):
        results = []
        for key, value, ex, kwargs in self.commands:
            results.append(await self.redis.set(key, value, ex=ex, **kwargs))
        return results


class FakeRedis:
    def __init__(self):
        self.values = {}

    def pipeline(self, transaction=False):
        return FakePipeline(self)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def get(self, key):
        return self.values.get(key)

    async def mget(self, keys):
        return [self.values.get(key) for key in keys]

    async def exists(self, key):
        return int(key in self.values)

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            removed += int(self.values.pop(key, None) is not None)
        return removed

    async def scan_iter(self, match, count=100):
        for key in list(self.values):
            if fnmatch.fnmatch(key, match):
                yield key

    async def eval(self, script, key_count, key, token):
        if self.values.get(key) == token:
            del self.values[key]
            return 1
        return 0


class DemandRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_registry_aggregates_refcounts_and_releases_only_owner_scope(self):
        redis = FakeRedis()
        registry = FeedDemandRegistry(redis)
        demand = FeedDemand("binance_futures_trade", "trade", "BTCUSDT", reason="position")

        await registry.upsert("alpha-a", [demand], ttl_seconds=120)
        await registry.upsert("order-recovery", [demand], ttl_seconds=120)
        snapshot = await registry.snapshot()

        self.assertEqual(snapshot["lease_count"], 2)
        self.assertEqual(snapshot["demanded_feed_count"], 1)
        self.assertEqual(snapshot["items"][0]["refcount"], 2)
        self.assertEqual(await registry.release_owner("alpha-a"), 1)
        self.assertEqual((await registry.snapshot())["items"][0]["refcount"], 1)


class UsdmStreamUniverseTests(unittest.TestCase):
    @patch("app.stream.binance_ws.requests.get")
    def test_all_trading_contracts_include_delivery_and_exclude_retired(self, get):
        response = get.return_value
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "BTCUSDT_260925", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
                {"symbol": "BTCUSDT_261225", "contractType": "NEXT_QUARTER", "status": "PENDING_TRADING"},
            ]
        }
        with TemporaryDirectory() as directory:
            cache = Path(directory) / "symbols.json"
            symbols = get_usdm_symbols(str(cache), contract_type=None, refresh=True)
            self.assertEqual(symbols, ["BTCUSDT", "BTCUSDT_260925"])
            self.assertEqual(json.loads(cache.read_text()), symbols)

    @patch("app.stream.binance_ws.requests.get")
    def test_perpetual_filter_remains_available_for_legacy_callers(self, get):
        response = get.return_value
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "BTCUSDT_260925", "contractType": "CURRENT_QUARTER", "status": "TRADING"},
            ]
        }
        with TemporaryDirectory() as directory:
            symbols = get_usdm_symbols(
                str(Path(directory) / "symbols.json"),
                contract_type="PERPETUAL",
                refresh=True,
            )
        self.assertEqual(symbols, ["BTCUSDT"])

    @patch("app.stream.binance_ws.os.replace", side_effect=PermissionError("readonly"))
    @patch("app.stream.binance_ws.requests.get")
    def test_cache_write_failure_does_not_retry_or_drop_provider_result(
        self, get, _replace
    ):
        response = get.return_value
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                }
            ]
        }
        with TemporaryDirectory() as directory:
            symbols = get_usdm_symbols(
                str(Path(directory) / "symbols.json"),
                contract_type=None,
                refresh=True,
            )
        self.assertEqual(symbols, ["BTCUSDT"])
        get.assert_called_once()


class TopupCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_topups_call_provider_once(self):
        redis = FakeRedis()
        calls = 0

        def topup(symbol, **kwargs):
            nonlocal calls
            calls += 1
            time.sleep(0.05)
            return {"needed": True, "symbol": symbol}

        coordinator = PreloadTopupCoordinator(redis, topup)
        results = await asyncio.gather(*[
            coordinator.run("FPT", interval="1m", max_lag_minutes=5)
            for _ in range(20)
        ])

        self.assertEqual(calls, 1)
        self.assertEqual(coordinator.provider_fetch_count, 1)
        self.assertEqual(len(results), 20)

    async def test_provider_failure_activates_negative_backoff(self):
        redis = FakeRedis()

        def topup(*_, **__):
            raise RuntimeError("provider down")

        coordinator = PreloadTopupCoordinator(redis, topup)
        with self.assertRaisesRegex(RuntimeError, "provider down"):
            await coordinator.run("FPT", interval="1m", max_lag_minutes=5)
        with self.assertRaises(PreloadTopupBackoff):
            await coordinator.run("FPT", interval="1m", max_lag_minutes=5)


class DemandHealthTests(unittest.TestCase):
    def test_only_missing_demanded_feed_degrades_health(self):
        supervisor = StreamSupervisor(startup_grace_seconds=0)
        shard = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.mark_connected(shard)
        supervisor.mark_message(shard)
        supervisor.expect_feed("binance_spot_kline", "kline", "BROADUSDT", "1m")

        broad = supervisor.snapshot(now=supervisor.started_at + 10)
        demanded = supervisor.snapshot(
            now=supervisor.started_at + 10,
            demanded_feed_keys={"kline:binance_usdm:1m:BTCUSDT"},
        )

        self.assertEqual(broad["status"], "ok")
        self.assertEqual(broad["feeds"]["broad_missing_count"], 1)
        self.assertEqual(demanded["status"], "degraded")
        self.assertEqual(demanded["feeds"]["demanded_missing_count"], 1)

    def test_spot_tick_cannot_satisfy_usdm_execution_demand(self):
        supervisor = StreamSupervisor(startup_grace_seconds=0)
        shard = supervisor.register_shard("binance_spot_trade", "wss://example")
        supervisor.mark_connected(shard)
        supervisor.mark_message(shard)
        supervisor.record_publish({
            "key": "trade:price:binance_spot:BTCUSDT",
            "data": {"source": "binance_spot_trade", "event_time": int(time.time() * 1000)},
        })

        snapshot = supervisor.snapshot(
            now=supervisor.started_at + 10,
            demanded_feed_keys={"trade:binance_usdm:BTCUSDT"},
        )

        self.assertEqual(snapshot["status"], "degraded")
        self.assertEqual(snapshot["feeds"]["demanded_missing_count"], 1)


if __name__ == "__main__":
    unittest.main()

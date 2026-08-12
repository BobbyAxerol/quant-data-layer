import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app.cache.redis_cache import RedisCache
from app.stream.async_live_feed import coalesce_redis_items, get_spot_symbols, redis_publisher_task
from app.stream.supervisor import StreamSupervisor


class FakeRedisCache:
    def __init__(self):
        self.items = []

    async def push_batch(self, items):
        self.items.extend(items)


class TestStreamSupervisor(unittest.TestCase):
    def test_snapshot_reports_missing_then_observed_feed(self):
        supervisor = StreamSupervisor(stale_after_seconds=180)
        supervisor.expect_feed("binance_spot_trade", "trade", "BTCUSDT")

        first = supervisor.snapshot()
        self.assertEqual(first["status"], "not_started")
        self.assertEqual(first["feeds"]["missing_count"], 1)

        shard_id = supervisor.register_shard("binance_spot_trade", "wss://example")
        supervisor.mark_connected(shard_id)
        supervisor.record_publish(
            {
                "key": "trade:price:BTCUSDT",
                "channel": "stream:trade:BTCUSDT",
                "data": {
                    "symbol": "BTCUSDT",
                    "source": "binance_spot_trade",
                    "event_time": 1779683242915,
                },
            }
        )

        observed = supervisor.snapshot()
        self.assertEqual(observed["status"], "ok")
        self.assertEqual(observed["feeds"]["missing_count"], 0)
        self.assertEqual(observed["feeds"]["observed_count"], 1)

    def test_queue_drop_is_warning_by_default(self):
        supervisor = StreamSupervisor()
        shard_id = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.mark_connected(shard_id)
        supervisor.record_queue_drop("binance_spot_kline", shard_id)

        snapshot = supervisor.snapshot()
        self.assertEqual(snapshot["status"], "ok")
        self.assertIn("queue_drop_observed", snapshot["health_warnings"])
        self.assertEqual(snapshot["queue"]["drop_count"], 1)
        self.assertEqual(snapshot["shards"]["items"][0]["queue_drop_count"], 1)

    def test_queue_drop_degrades_strict_health(self):
        supervisor = StreamSupervisor(strict_feed_health=True)
        shard_id = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.mark_connected(shard_id)
        supervisor.record_queue_drop("binance_spot_kline", shard_id)

        snapshot = supervisor.snapshot()
        self.assertEqual(snapshot["status"], "degraded")
        self.assertIn("queue_drop_observed", snapshot["health_warnings"])

    def test_old_queue_drop_does_not_permanently_degrade_health(self):
        supervisor = StreamSupervisor(strict_feed_health=True)
        supervisor.queue_drop_window_seconds = 1
        shard_id = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.mark_connected(shard_id)
        supervisor.record_queue_drop("binance_spot_kline", shard_id)

        snapshot = supervisor.snapshot(now=time.time() + 2)

        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["queue"]["recent_drop_count"], 0)

    def test_missing_trade_feed_is_diagnostic_not_health_failure(self):
        supervisor = StreamSupervisor(startup_grace_seconds=0)
        shard_id = supervisor.register_shard("binance_spot_trade", "wss://example")
        supervisor.expect_feed("binance_spot_trade", "trade", "ILLQUSDT")
        supervisor.mark_connected(shard_id)

        snapshot = supervisor.snapshot(now=supervisor.started_at + 10)
        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["feeds"]["missing_count"], 1)
        self.assertEqual(snapshot["feeds"]["health_missing_count"], 0)

    def test_missing_kline_after_startup_grace_is_warning_by_default(self):
        supervisor = StreamSupervisor(startup_grace_seconds=0)
        shard_id = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.expect_feed("binance_spot_kline", "kline", "BTCUSDT", "1m")
        supervisor.mark_connected(shard_id)

        snapshot = supervisor.snapshot(now=supervisor.started_at + 10)
        self.assertEqual(snapshot["status"], "ok")
        self.assertEqual(snapshot["feeds"]["health_missing_count"], 1)

    def test_missing_kline_after_startup_grace_degrades_strict_health(self):
        supervisor = StreamSupervisor(startup_grace_seconds=0, strict_feed_health=True)
        shard_id = supervisor.register_shard("binance_spot_kline", "wss://example")
        supervisor.expect_feed("binance_spot_kline", "kline", "BTCUSDT", "1m")
        supervisor.mark_connected(shard_id)

        snapshot = supervisor.snapshot(now=supervisor.started_at + 10)
        self.assertEqual(snapshot["status"], "degraded")
        self.assertTrue(snapshot["strict_feed_health"])

    def test_spot_symbol_loader_uses_spot_specific_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "symbols_spot.json"
            path.write_text(json.dumps(["BTCUSDT", "ETHUSDT"]), encoding="utf-8")

            symbols = get_spot_symbols(str(path))

        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    def test_spot_symbol_refresh_falls_back_to_last_good_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "symbols_spot.json"
            path.write_text(json.dumps(["BTCUSDT", "ETHUSDT"]), encoding="utf-8")
            with patch(
                "app.stream.async_live_feed.requests.get",
                side_effect=RuntimeError("exchange info offline"),
            ):
                symbols = get_spot_symbols(str(path), refresh=True)

        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])


class TestRedisLastKnownContract(unittest.TestCase):
    def test_last_known_key_mapping(self):
        self.assertEqual(
            RedisCache._last_known_key("trade:price:BTCUSDT"),
            "trade:price:last:BTCUSDT",
        )
        self.assertEqual(
            RedisCache._last_known_key("kline:1m:ETHUSDT"),
            "kline:last:1m:ETHUSDT",
        )
        self.assertEqual(
            RedisCache._last_known_key("vn:quote:FPT"),
            "vn:quote:last:FPT",
        )
        self.assertIsNone(RedisCache._last_known_key("unknown:key"))


class TestRedisPublisherTask(unittest.IsolatedAsyncioTestCase):
    def test_coalesce_redis_items_keeps_latest_per_key_channel(self):
        items = [
            {"key": "trade:price:BTCUSDT", "channel": "stream:trade:BTCUSDT", "data": {"price": 1}},
            {"key": "trade:price:ETHUSDT", "channel": "stream:trade:ETHUSDT", "data": {"price": 2}},
            {"key": "trade:price:BTCUSDT", "channel": "stream:trade:BTCUSDT", "data": {"price": 3}},
        ]

        result = coalesce_redis_items(items)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"]["price"], 3)
        self.assertEqual(result[1]["data"]["price"], 2)

    async def test_publisher_records_supervisor_metrics(self):
        queue = asyncio.Queue(maxsize=10)
        redis_cache = FakeRedisCache()
        supervisor = StreamSupervisor()
        await queue.put(
            (
                "binance_spot_trade",
                {
                    "s": "BTCUSDT",
                    "p": "77000.0",
                    "q": "0.01",
                    "t": 1,
                    "T": 1779683242915,
                    "m": False,
                },
            )
        )

        task = asyncio.create_task(redis_publisher_task(queue, redis_cache, supervisor=supervisor))
        await asyncio.sleep(0.15)
        task.cancel()
        await task

        self.assertEqual(len(redis_cache.items), 2)
        snapshot = supervisor.snapshot()
        self.assertEqual(snapshot["publisher"]["batch_count"], 1)
        self.assertEqual(snapshot["publisher"]["item_count"], 2)
        keys = {item["key"] for item in redis_cache.items}
        channels = {item["channel"] for item in redis_cache.items}
        self.assertEqual(keys, {"trade:price:binance_spot:BTCUSDT", "trade:price:BTCUSDT"})
        self.assertEqual(channels, {"stream:trade:binance_spot:BTCUSDT", "stream:trade:BTCUSDT"})


if __name__ == "__main__":
    unittest.main()

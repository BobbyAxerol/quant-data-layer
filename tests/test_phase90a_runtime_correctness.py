from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.api.routes_health import health
from app.stream.async_live_feed import (
    StreamBackpressureTimeout,
    _put_provider_item,
    coalesce_redis_items,
    handle_ws,
    provider_items,
    redis_publisher_task,
    valid_provider_frame,
)
from app.stream.kline_recovery import DemandKlineRecovery, KlineRecoveryConfig
from app.stream.supervisor import StreamSupervisor


class FakeDemandRegistry:
    def __init__(self, feed_keys: list[str]):
        self.feed_keys = feed_keys

    async def snapshot(self):
        return {
            "lease_count": len(self.feed_keys),
            "demanded_feed_count": len(self.feed_keys),
            "feed_keys": list(self.feed_keys),
            "items": [{"feed_key": key, "source": "binance_usdm"} for key in self.feed_keys],
        }


class FakeKlineCache:
    def __init__(self, payload=None):
        self.payload = payload

    async def get_binance_kline_last(self, symbol: str, interval: str):
        return self.payload


class RuntimeReadinessTests(unittest.TestCase):
    @staticmethod
    def trade_frame():
        return {
            "e": "trade",
            "E": 120,
            "s": "BTCUSDT",
            "t": 1,
            "p": "60000",
            "q": "0.1",
            "T": 119,
            "m": False,
        }

    @staticmethod
    def kline_frame():
        return {
            "e": "kline",
            "E": 120,
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "i": "1m",
                "t": 60,
                "T": 119,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "10",
                "x": True,
            },
        }

    def test_ack_and_wrong_feed_do_not_count_as_provider_data(self):
        self.assertEqual(provider_items("binance_futures_kline", {"result": None, "id": 1}), [])
        self.assertFalse(valid_provider_frame("binance_futures_kline", self.trade_frame(), "1m"))
        with self.assertRaisesRegex(ValueError, "wrong-feed"):
            provider_items("binance_futures_kline", self.trade_frame(), "1m")

    def test_trade_and_kline_readiness_are_independent(self):
        supervisor = StreamSupervisor(first_frame_timeout_seconds=5, stale_after_seconds=180)
        trade = supervisor.register_shard("binance_futures_trade", "wss://trade")
        kline = supervisor.register_shard("binance_futures_kline", "wss://kline")
        supervisor.mark_connected(trade)
        supervisor.mark_connected(kline)
        supervisor.mark_message(trade)

        snapshot = supervisor.snapshot(now=supervisor.started_at + 10)

        self.assertEqual(snapshot["sources"]["binance_futures_trade"]["status"], "ready")
        self.assertEqual(snapshot["sources"]["binance_futures_kline"]["status"], "unavailable")
        self.assertEqual(snapshot["status"], "degraded")
        self.assertIn("source_data_unavailable", snapshot["health_warnings"])

    def test_both_valid_sources_become_ready(self):
        supervisor = StreamSupervisor(first_frame_timeout_seconds=5, stale_after_seconds=180)
        for source in ("binance_futures_trade", "binance_futures_kline"):
            shard = supervisor.register_shard(source, f"wss://{source}")
            supervisor.mark_connected(shard)
            supervisor.mark_message(shard)

        snapshot = supervisor.snapshot(now=supervisor.started_at + 10)

        self.assertEqual(snapshot["status"], "ok")
        self.assertTrue(all(item["data_ready"] for item in snapshot["sources"].values()))


class FakeWebSocketContext:
    def __init__(self, messages: list[str]):
        self.messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def recv(self):
        if self.messages:
            return self.messages.pop(0)
        await asyncio.Event().wait()


class ReceiverWatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_connected_without_frame_times_out_and_reconnects(self):
        supervisor = StreamSupervisor(first_frame_timeout_seconds=0.01)
        shard = supervisor.register_shard("binance_futures_kline", "wss://kline")
        socket = FakeWebSocketContext([])

        with patch("app.stream.async_live_feed.websocket_connect", return_value=socket), patch(
            "app.stream.async_live_feed.asyncio.sleep", side_effect=asyncio.CancelledError
        ):
            with self.assertRaises(asyncio.CancelledError):
                await handle_ws(
                    "wss://kline",
                    asyncio.Queue(),
                    "binance_futures_kline",
                    shard,
                    supervisor,
                    reconnect_delay=0,
                    first_frame_timeout_seconds=0.01,
                )

        state = supervisor.shards[shard]
        self.assertEqual(state.message_count, 0)
        self.assertEqual(state.data_timeout_count, 1)
        self.assertEqual(state.status, "reconnecting")
        self.assertIn("first_frame timeout", state.last_error)

    async def test_valid_frame_then_idle_timeout_is_observable(self):
        supervisor = StreamSupervisor(first_frame_timeout_seconds=0.01)
        shard = supervisor.register_shard("binance_futures_trade", "wss://trade")
        frame = RuntimeReadinessTests.trade_frame()
        socket = FakeWebSocketContext([json.dumps({"stream": "btcusdt@trade", "data": frame})])
        queue = asyncio.Queue()

        with patch("app.stream.async_live_feed.websocket_connect", return_value=socket), patch(
            "app.stream.async_live_feed.asyncio.sleep", side_effect=asyncio.CancelledError
        ):
            with self.assertRaises(asyncio.CancelledError):
                await handle_ws(
                    "wss://trade",
                    queue,
                    "binance_futures_trade",
                    shard,
                    supervisor,
                    reconnect_delay=0,
                    first_frame_timeout_seconds=0.01,
                    idle_timeout_seconds=0.01,
                )

        state = supervisor.shards[shard]
        self.assertEqual(state.message_count, 1)
        self.assertEqual(state.data_timeout_count, 1)
        self.assertEqual((await queue.get())[1]["s"], "BTCUSDT")
        self.assertIn("idle timeout", state.last_error)


class BackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_queue_never_discards_oldest_event(self):
        queue = asyncio.Queue(maxsize=1)
        await queue.put(("binance_futures_trade", {"t": "first"}))
        supervisor = StreamSupervisor()

        with self.assertRaises(StreamBackpressureTimeout):
            await _put_provider_item(
                queue,
                ("binance_futures_trade", {"t": "second"}),
                supervisor=supervisor,
                timeout_seconds=0.01,
            )

        self.assertEqual((await queue.get())[1]["t"], "first")
        self.assertEqual(supervisor.queue_pressure_count, 1)
        self.assertEqual(supervisor.queue_drop_count, 0)

    def test_recovery_gap_rows_are_not_coalesced_together(self):
        rows = [
            {"key": "kline:1m:BTCUSDT", "channel": "stream:kline:1m:BTCUSDT", "coalesce_id": "recovery:1", "data": {"k": {"t": 1}}},
            {"key": "kline:1m:BTCUSDT", "channel": "stream:kline:1m:BTCUSDT", "coalesce_id": "recovery:2", "data": {"k": {"t": 2}}},
        ]
        self.assertEqual(len(coalesce_redis_items(rows)), 2)


    async def test_publisher_projects_payload_interval_instead_of_stream_default(self):
        class Cache:
            def __init__(self):
                self.items = []

            async def push_batch(self, items):
                self.items.extend(items)

        queue = asyncio.Queue()
        cache = Cache()
        await queue.put(
            (
                "binance_futures_kline",
                {
                    "e": "kline_recovery",
                    "s": "BTCUSDT",
                    "k": {"s": "BTCUSDT", "i": "5m", "t": 300_000, "x": True},
                    "recovery_source": "BINANCE_REST_GAP_FILL",
                },
            )
        )
        task = asyncio.create_task(
            redis_publisher_task(queue, cache, interval="1m", supervisor=StreamSupervisor())
        )
        await asyncio.sleep(0.1)
        task.cancel()
        await task

        self.assertEqual(cache.items[0]["key"], "kline:5m:BTCUSDT")
        self.assertEqual(cache.items[0]["channel"], "stream:kline:5m:BTCUSDT")

class DemandKlineRecoveryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def rows():
        return [
            [0, "1", "2", "0.5", "1.5", "10", 59_999],
            [60_000, "1.5", "2.5", "1", "2", "20", 119_999],
            [120_000, "2", "3", "1.5", "2.5", "30", 179_999],
        ]

    async def test_active_demand_emits_only_latest_fully_closed_bar_on_cold_cache(self):
        queue = asyncio.Queue()
        demands = FakeDemandRegistry(["kline:binance_usdm:1m:BTCUSDT"])
        calls = []

        def fetcher(*args):
            calls.append(args)
            return {"data": self.rows()}

        manager = DemandKlineRecovery(
            queue=queue,
            redis_cache=FakeKlineCache(),
            demand_registry=demands,
            config=KlineRecoveryConfig(),
            fetcher=fetcher,
        )

        result = await manager.poll_once(now_ms=150_000)

        self.assertEqual(result, {"due": 1, "emitted": 1, "failed": 0})
        source, event = await queue.get()
        self.assertEqual(source, "binance_futures_kline")
        self.assertEqual(event["k"]["t"], 60_000)
        self.assertTrue(event["k"]["x"])
        self.assertEqual(event["recovery_source"], "BINANCE_REST_GAP_FILL")
        self.assertEqual(len(calls), 1)
        self.assertEqual(manager.rejected_open_or_invalid_count, 1)

    async def test_existing_final_bar_is_deduplicated(self):
        existing = {"k": {"t": 60_000, "x": True}}
        queue = asyncio.Queue()
        manager = DemandKlineRecovery(
            queue=queue,
            redis_cache=FakeKlineCache(existing),
            demand_registry=FakeDemandRegistry(["kline:binance_usdm:1m:BTCUSDT"]),
            config=KlineRecoveryConfig(),
            fetcher=lambda *args: {"data": self.rows()[:2]},
        )

        result = await manager.poll_once(now_ms=150_000)

        self.assertEqual(result["emitted"], 0)
        self.assertEqual(manager.deduplicated_count, 2)
        self.assertTrue(queue.empty())

    async def test_no_active_demand_makes_no_provider_request_and_clears_schedule(self):
        demands = FakeDemandRegistry(["kline:binance_usdm:1m:BTCUSDT"])
        calls = []
        manager = DemandKlineRecovery(
            queue=asyncio.Queue(),
            redis_cache=FakeKlineCache(),
            demand_registry=demands,
            config=KlineRecoveryConfig(),
            fetcher=lambda *args: calls.append(args) or {"data": self.rows()},
        )
        await manager.poll_once(now_ms=150_000)
        demands.feed_keys = []

        result = await manager.poll_once(now_ms=200_000)

        self.assertEqual(result, {"due": 0, "emitted": 0, "failed": 0})
        self.assertEqual(manager.active_demand_count, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(manager._next_due_ms, {})

    async def test_provider_failure_is_bounded_by_per_feed_backoff(self):
        calls = []

        def failed(*args):
            calls.append(args)
            raise RuntimeError("provider unavailable")

        manager = DemandKlineRecovery(
            queue=asyncio.Queue(),
            redis_cache=FakeKlineCache(),
            demand_registry=FakeDemandRegistry(["kline:binance_usdm:1m:BTCUSDT"]),
            config=KlineRecoveryConfig(poll_seconds=2, max_backoff_seconds=30),
            fetcher=failed,
        )

        first = await manager.poll_once(now_ms=150_000)
        second = await manager.poll_once(now_ms=151_000)

        self.assertEqual(first["failed"], 1)
        self.assertEqual(second["due"], 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(manager.snapshot()["status"], "degraded")


class HealthContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_v1_health_booleans_reflect_matching_source(self):
        supervisor = StreamSupervisor(first_frame_timeout_seconds=1)
        trade = supervisor.register_shard("binance_futures_trade", "wss://trade")
        kline = supervisor.register_shard("binance_futures_kline", "wss://kline")
        supervisor.mark_connected(trade)
        supervisor.mark_connected(kline)
        supervisor.mark_message(trade)
        supervisor.shards[kline].last_connected_at -= 5

        context = SimpleNamespace(
            redis_cache=SimpleNamespace(health_check=lambda: asyncio.sleep(0, result=True)),
            demand_registry=FakeDemandRegistry([]),
            binance_stream_supervisor=supervisor,
            get_dnse_stream_manager=lambda: None,
            get_kline_recovery_manager=lambda: None,
            preload_topup_coordinator=SimpleNamespace(snapshot=lambda: {}),
        )

        result = await health(context)

        self.assertEqual(result["status"], "degraded")
        self.assertTrue(result["binance_trade_stream"])
        self.assertFalse(result["binance_kline_stream"])
        self.assertIn("binance_stream", result)
        self.assertIn("feed_demands", result)


if __name__ == "__main__":
    unittest.main()

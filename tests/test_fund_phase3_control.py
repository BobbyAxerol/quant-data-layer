from __future__ import annotations

import asyncio
import unittest

from qdl.ingestion.contracts import DeliveryPolicy, FeedType, Subscription, plan_shards
from qdl.ingestion.demand import DesiredSubscriptionRegistry
from qdl.ingestion.fencing import FencingGate, InMemoryLeaseStore
from qdl.ingestion.queue import FeedQueue
from qdl.projection.authority import Authority, FeedAuthorityRegistry
from qdl.projection.trade import InMemoryProjectionTarget, ProjectionRecord


class DemandAndShardTests(unittest.TestCase):
    def test_expiring_demand_and_deterministic_shards(self):
        now = [1_000_000_000]
        registry = DesiredSubscriptionRegistry(clock_ns=lambda: now[0])
        trade = Subscription("BINANCE", "USDM", FeedType.TRADE, "BTCUSDT")
        registry.renew("alpha-a", {trade}, ttl_seconds=5)
        first = registry.shards(max_streams=1, config_revision=9)
        second = registry.shards(max_streams=1, config_revision=9)
        self.assertEqual(first, second)
        self.assertEqual(first[0].subscriptions, (trade,))
        now[0] += 5_000_000_000
        self.assertEqual(registry.desired(), set())

    def test_spot_zero_demand_means_no_spot_shard_but_can_enable_without_code_change(self):
        registry = DesiredSubscriptionRegistry()
        self.assertEqual(registry.shards(max_streams=10, config_revision=1), ())
        spot = Subscription("BINANCE", "SPOT", FeedType.BBO, "BTCUSDT")
        registry.renew("consumer", {spot}, ttl_seconds=30)
        self.assertEqual(registry.shards(max_streams=10, config_revision=1)[0].market, "SPOT")

    def test_sharding_never_truncates_requested_subscriptions(self):
        requested = {
            Subscription("BINANCE", "USDM", FeedType.TRADE, f"S{i}USDT")
            for i in range(11)
        }
        shards = plan_shards(requested, max_streams=3, config_revision=1)
        self.assertEqual(len(shards), 4)
        self.assertEqual(sum(len(shard.subscriptions) for shard in shards), 11)


class FencingTests(unittest.TestCase):
    def test_old_owner_is_fenced_after_expiry_and_takeover(self):
        now = [1_000_000_000]
        store = InMemoryLeaseStore(clock_ns=lambda: now[0])
        first = store.acquire("shard-a", "owner-a", 5)
        self.assertIsNotNone(first)
        self.assertIsNone(store.acquire("shard-a", "owner-b", 5))
        now[0] += 5_000_000_001
        second = store.acquire("shard-a", "owner-b", 5)
        self.assertEqual(second.epoch, first.epoch + 1)
        gate = FencingGate()
        self.assertTrue(gate.permits("shard-a", second.epoch))
        self.assertFalse(gate.permits("shard-a", first.epoch))


class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_lossless_queue_backpressures_instead_of_dropping(self):
        queue = FeedQueue[int](capacity=1, policy=DeliveryPolicy.LOSSLESS)
        await queue.put("one", 1)
        blocked = asyncio.create_task(queue.put("two", 2))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())
        self.assertEqual(await queue.get(), 1)
        queue.task_done()
        await blocked
        self.assertEqual(await queue.get(), 2)
        self.assertEqual(queue.stats().rejected, 0)

    async def test_latest_state_coalesces_only_same_pending_key(self):
        queue = FeedQueue[int](capacity=2, policy=DeliveryPolicy.LATEST_STATE)
        await queue.put("btc", 1)
        await queue.put("btc", 2)
        await queue.put("eth", 3)
        self.assertEqual(await queue.get(), 2)
        self.assertEqual(await queue.get(), 3)
        self.assertEqual(queue.stats().coalesced, 1)


class AuthorityTests(unittest.TestCase):
    def test_cutover_and_rollback_are_per_feed_and_fenced(self):
        registry = FeedAuthorityRegistry()
        feed = "BINANCE:USDM:trade:BTCUSDT"
        self.assertFalse(registry.decide(feed_key=feed, shard_id="s", lease_epoch=1).write_legacy)
        registry.set(feed, Authority.CANONICAL)
        self.assertTrue(registry.decide(feed_key=feed, shard_id="s", lease_epoch=2).write_legacy)
        self.assertFalse(registry.decide(feed_key=feed, shard_id="s", lease_epoch=1).accepted)
        registry.set(feed, Authority.LEGACY)
        decision = registry.decide(feed_key=feed, shard_id="s", lease_epoch=2)
        self.assertFalse(decision.write_canonical)
        self.assertFalse(decision.write_legacy)

    def test_projection_target_rejects_stale_epoch_even_with_newer_offset(self):
        target = InMemoryProjectionTarget()
        def record(offset, epoch):
            return ProjectionRecord(
                partition_key="p", offset=offset, event_id_hex=str(offset),
                canonical_key="shadow:qdl:v2:latest:x", canonical_payload=b"x",
                legacy_items=(), shard_id="s", lease_epoch=epoch,
            )
        self.assertTrue(target.apply(record(1, 2)))
        self.assertFalse(target.apply(record(2, 1)))


if __name__ == "__main__":
    unittest.main()

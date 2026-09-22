from __future__ import annotations

import asyncio
import threading
import unittest

from qdl.query.lanes import BoundedReadLane, ReadLanePolicy, ReadLaneRejected
from qdl.query.service import V2QueryService


class ReadLaneTests(unittest.IsolatedAsyncioTestCase):
    def lane(self, **overrides) -> BoundedReadLane:
        values = {
            "max_active": 2,
            "max_pending": 6,
            "max_pending_bytes": 64 * 1024,
            "max_active_per_consumer": 1,
            "max_pending_per_consumer": 2,
            "reserved_consumer_id": "trading-system.paper.stable",
            "reserved_slots": 1,
        }
        values.update(overrides)
        return BoundedReadLane(ReadLanePolicy(**values))

    async def test_reserved_trading_system_request_gets_the_reserved_hot_slot(self):
        lane = self.lane()
        alpha_started = asyncio.Event()
        ts_started = asyncio.Event()
        other_started = asyncio.Event()
        release = asyncio.Event()

        async def block(started):
            started.set()
            await release.wait()
            return "done"

        alpha = asyncio.create_task(
            lane.run(lambda: block(alpha_started), consumer_id="alpha-a", reserved_bytes=1024)
        )
        await asyncio.wait_for(alpha_started.wait(), timeout=1)
        trading_system = asyncio.create_task(
            lane.run(
                lambda: block(ts_started),
                consumer_id="trading-system.paper.stable",
                reserved_bytes=1024,
            )
        )
        await asyncio.wait_for(ts_started.wait(), timeout=1)
        other = asyncio.create_task(
            lane.run(lambda: block(other_started), consumer_id="alpha-b", reserved_bytes=1024)
        )
        await asyncio.sleep(0)
        self.assertFalse(other_started.is_set())
        release.set()
        await asyncio.gather(alpha, trading_system, other)
        self.assertTrue(other_started.is_set())
        self.assertEqual(lane.stats()["pending"], 0)
        self.assertEqual(lane.stats()["active"], 0)

    async def test_one_consumer_cannot_occupy_every_hot_slot(self):
        lane = self.lane(reserved_consumer_id=None, reserved_slots=0)
        first_started = asyncio.Event()
        second_same_started = asyncio.Event()
        other_started = asyncio.Event()
        release = asyncio.Event()

        async def block(started):
            started.set()
            await release.wait()
            return "done"

        first = asyncio.create_task(
            lane.run(lambda: block(first_started), consumer_id="alpha-a", reserved_bytes=1024)
        )
        await asyncio.wait_for(first_started.wait(), timeout=1)
        same = asyncio.create_task(
            lane.run(
                lambda: block(second_same_started),
                consumer_id="alpha-a",
                reserved_bytes=1024,
            )
        )
        other = asyncio.create_task(
            lane.run(lambda: block(other_started), consumer_id="alpha-b", reserved_bytes=1024)
        )
        await asyncio.wait_for(other_started.wait(), timeout=1)
        self.assertFalse(second_same_started.is_set())
        release.set()
        await asyncio.gather(first, same, other)

    async def test_byte_reservation_rejects_before_work_starts(self):
        lane = self.lane(max_pending_bytes=1024)
        called = False

        async def work():
            nonlocal called
            called = True

        with self.assertRaisesRegex(ReadLaneRejected, "byte reservation"):
            await lane.run(work, consumer_id="alpha-a", reserved_bytes=1025)
        self.assertFalse(called)
        self.assertEqual(lane.stats()["rejected_bytes"], 1)
        self.assertEqual(lane.stats()["pending_bytes"], 0)

    async def test_cancelled_waiter_returns_its_count_and_byte_reservation(self):
        lane = self.lane(
            max_active=1,
            max_pending=3,
            reserved_consumer_id=None,
            reserved_slots=0,
        )
        started = asyncio.Event()
        release = asyncio.Event()

        async def active():
            started.set()
            await release.wait()

        first = asyncio.create_task(
            lane.run(active, consumer_id="alpha-a", reserved_bytes=1024)
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        waiting = asyncio.create_task(
            lane.run(lambda: asyncio.sleep(0), consumer_id="alpha-b", reserved_bytes=1024)
        )
        await asyncio.sleep(0)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.assertEqual(lane.stats()["pending"], 1)
        self.assertEqual(lane.stats()["pending_bytes"], 1024)
        release.set()
        await first
        self.assertEqual(lane.stats()["pending"], 0)
        self.assertEqual(lane.stats()["pending_bytes"], 0)


class QueryWorkPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_hot_snapshot_has_a_worker_when_all_cold_workers_are_busy(self):
        class Service(V2QueryService):
            def __init__(self):
                self.snapshot_started = threading.Event()

            def snapshot(self, requirement, *, purpose, request_id=None):
                del requirement, purpose, request_id
                self.snapshot_started.set()
                return "snapshot"

        service = Service()
        release = threading.Event()
        started = [threading.Event() for _ in range(4)]

        def cold(index):
            started[index].set()
            release.wait(timeout=1)
            return index

        cold_tasks = tuple(
            asyncio.create_task(service._query_work_pools_for().cold(cold, index))
            for index in range(4)
        )
        for event in started:
            self.assertTrue(await asyncio.to_thread(event.wait, 1))
        result = await asyncio.wait_for(
            service.snapshot_async(
                object(),
                purpose=object(),
                consumer_id="trading-system.paper.stable",
            ),
            timeout=1,
        )
        self.assertEqual(result, "snapshot")
        self.assertTrue(service.snapshot_started.is_set())
        release.set()
        await asyncio.gather(*cold_tasks)
        await service.close()

    def test_local_history_reservation_is_conservative_and_bounded(self):
        self.assertEqual(V2QueryService._local_history_reservation_bytes(()), 16 * 1024)

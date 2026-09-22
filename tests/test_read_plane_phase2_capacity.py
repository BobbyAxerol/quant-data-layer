from __future__ import annotations

import asyncio
import threading
import unittest

from qdl.query.lanes import BoundedReadLane, ReadLanePolicy, ReadLaneRejected
from qdl.query.service import (
    V2QueryService,
    _hot_reference_lane_policy,
    _hot_snapshot_lane_policy,
)


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

    async def test_one_identity_borrows_idle_worker_then_yields_to_waiting_peer(self):
        lane = self.lane(
            max_pending_per_consumer=4,
            reserved_max_active_per_consumer=2,
            reserved_max_pending_per_consumer=5,
            reserved_pending_slots=1,
            non_reserved_pending_slots=1,
        )
        ts_first_started = asyncio.Event()
        ts_second_started = asyncio.Event()
        alpha_started = asyncio.Event()
        release_first = asyncio.Event()
        release_second = asyncio.Event()
        release_alpha = asyncio.Event()

        async def block(started, release):
            started.set()
            await release.wait()

        first = asyncio.create_task(lane.run(
            lambda: block(ts_first_started, release_first),
            consumer_id="trading-system.paper.stable", reserved_bytes=1024,
        ))
        await asyncio.wait_for(ts_first_started.wait(), timeout=1)
        second = asyncio.create_task(lane.run(
            lambda: block(ts_second_started, release_second),
            consumer_id="trading-system.paper.stable", reserved_bytes=1024,
        ))
        await asyncio.wait_for(ts_second_started.wait(), timeout=1)
        alpha = asyncio.create_task(lane.run(
            lambda: block(alpha_started, release_alpha),
            consumer_id="alpha-a", reserved_bytes=1024,
        ))
        await asyncio.sleep(0)
        self.assertFalse(alpha_started.is_set())
        release_first.set()
        await asyncio.wait_for(alpha_started.wait(), timeout=1)
        release_second.set()
        release_alpha.set()
        await asyncio.gather(first, second, alpha)
        self.assertEqual(lane.stats()["rejected"], 0)

    async def test_pending_class_reserves_admit_a_later_other_class(self):
        policy = ReadLanePolicy(
            max_active=1,
            max_pending=4,
            max_pending_bytes=64 * 1024,
            max_active_per_consumer=1,
            max_pending_per_consumer=4,
            reserved_consumer_id="trading-system.paper.stable",
            reserved_slots=0,
            reserved_max_pending_per_consumer=3,
            reserved_pending_slots=1,
            non_reserved_pending_slots=1,
        )
        lane = BoundedReadLane(policy)
        release = asyncio.Event()
        first_started = asyncio.Event()

        async def block(started=None):
            if started is not None:
                started.set()
            await release.wait()

        alpha_tasks = [asyncio.create_task(lane.run(
            lambda started=first_started if index == 0 else None: block(started),
            consumer_id="alpha-a", reserved_bytes=1024,
        )) for index in range(3)]
        await asyncio.wait_for(first_started.wait(), timeout=1)
        await asyncio.sleep(0)
        trading_system = asyncio.create_task(lane.run(
            block, consumer_id="trading-system.paper.stable", reserved_bytes=1024,
        ))
        for _ in range(20):
            if lane.stats()["pending"] == 4:
                break
            await asyncio.sleep(0)
        self.assertEqual(lane.stats()["pending"], 4)
        with self.assertRaisesRegex(ReadLaneRejected, "another consumer class"):
            await lane.run(block, consumer_id="alpha-b", reserved_bytes=1024)
        release.set()
        await asyncio.gather(*alpha_tasks, trading_system)

        reverse_lane = BoundedReadLane(policy)
        reverse_release = asyncio.Event()

        async def reverse_block():
            await reverse_release.wait()

        ts_tasks = [asyncio.create_task(reverse_lane.run(
            reverse_block, consumer_id="trading-system.paper.stable", reserved_bytes=1024,
        )) for _ in range(3)]
        for _ in range(20):
            if reverse_lane.stats()["pending"] == 3:
                break
            await asyncio.sleep(0)
        with self.assertRaisesRegex(ReadLaneRejected, "another consumer class"):
            await reverse_lane.run(
                reverse_block,
                consumer_id="trading-system.paper.stable",
                reserved_bytes=1024,
            )
        alpha = asyncio.create_task(reverse_lane.run(
            reverse_block, consumer_id="alpha-a", reserved_bytes=1024,
        ))
        for _ in range(20):
            if reverse_lane.stats()["pending"] == 4:
                break
            await asyncio.sleep(0)
        self.assertEqual(reverse_lane.stats()["pending"], 4)
        reverse_release.set()
        await asyncio.gather(*ts_tasks, alpha)

    async def test_normal_identity_stays_bounded_while_ts_burst_is_queued(self):
        lane = BoundedReadLane(_hot_snapshot_lane_policy())
        release = asyncio.Event()

        async def block():
            await release.wait()

        alpha_tasks = [asyncio.create_task(lane.run(
            block, consumer_id="alpha-a", reserved_bytes=16 * 1024,
        )) for _ in range(4)]
        for _ in range(20):
            if lane.stats()["pending"] == 4:
                break
            await asyncio.sleep(0)
        with self.assertRaisesRegex(ReadLaneRejected, "finite pending bound"):
            await lane.run(block, consumer_id="alpha-a", reserved_bytes=16 * 1024)
        release.set()
        await asyncio.gather(*alpha_tasks)

    async def test_reference_lane_uses_the_same_ts_and_alpha_reserves(self):
        lane = BoundedReadLane(_hot_reference_lane_policy())
        release = asyncio.Event()
        started = 0
        all_workers_started = asyncio.Event()

        async def block():
            nonlocal started
            started += 1
            if started == 4:
                all_workers_started.set()
            await release.wait()

        ts_tasks = [asyncio.create_task(lane.run(
            block, consumer_id="trading-system.paper.stable", reserved_bytes=16 * 1024,
        )) for _ in range(12)]
        await asyncio.wait_for(all_workers_started.wait(), timeout=1)
        alpha = asyncio.create_task(lane.run(
            block, consumer_id="alpha-a", reserved_bytes=16 * 1024,
        ))
        for _ in range(40):
            if lane.stats()["pending"] == 13:
                break
            await asyncio.sleep(0)
        self.assertEqual(lane.stats()["pending"], 13)
        self.assertEqual(lane.stats()["rejected"], 0)
        release.set()
        await asyncio.gather(*ts_tasks, alpha)


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

    async def test_ts_hot_slice_burst_is_queued_without_local_admission_reject(self):
        class Service(V2QueryService):
            def __init__(self):
                self.release = threading.Event()
                self.started = threading.Event()

            def snapshot(self, requirement, *, purpose, request_id=None):
                del requirement, purpose, request_id
                self.started.set()
                self.release.wait(timeout=1)
                return "snapshot"

        service = Service()
        tasks = tuple(
            asyncio.create_task(service.snapshot_async(
                object(), purpose=object(), consumer_id="trading-system.paper.stable",
            ))
            for _ in range(10)
        )
        self.assertTrue(await asyncio.to_thread(service.started.wait, 1))
        for _ in range(50):
            if service._hot_snapshot_admission.stats()["pending"] == 10:
                break
            await asyncio.sleep(0)
        self.assertEqual(service._hot_snapshot_admission.stats()["pending"], 10)
        self.assertEqual(service._hot_snapshot_admission.stats()["rejected"], 0)
        service.release.set()
        self.assertEqual(await asyncio.gather(*tasks), ["snapshot"] * 10)
        await service.close()

    def test_local_history_reservation_is_conservative_and_bounded(self):
        self.assertEqual(V2QueryService._local_history_reservation_bytes(()), 16 * 1024)

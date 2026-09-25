"""Cold-read duty cycle (v2.1.1 Phase-3 live evidence, 2026-09-23).

During one 4.1 s cold warmup, QUOTE snapshots on the same replica went from
p50 9.5 ms to p50 156 ms / max 1.44 s with no disk read and no throttling: a
CPU-bound cold thread starved the event loop of the GIL.
"""

from __future__ import annotations

import threading
import time
import unittest
from unittest.mock import patch

from qdl.query import cold_work


class ColdWorkTests(unittest.TestCase):
    def test_a_hot_thread_never_pauses(self):
        with patch.object(cold_work.time, "sleep") as sleep:
            for _ in range(10_000):
                cold_work.cold_yield()
        sleep.assert_not_called()

    def test_a_cold_thread_pauses_once_per_slice(self):
        pauses = []
        real_sleep = time.sleep
        with patch.object(cold_work.time, "sleep", side_effect=lambda s: (pauses.append(s), real_sleep(s))):
            with cold_work.cold_work():
                deadline = time.perf_counter() + 0.05
                while time.perf_counter() < deadline:
                    cold_work.cold_yield()
        self.assertGreaterEqual(len(pauses), 5)
        self.assertTrue(all(value == cold_work.COLD_PAUSE_SECONDS for value in pauses))

    def test_cold_work_lets_another_thread_run(self):
        # The pause is a real sleep, so a competing thread makes progress.
        progress = []
        stop = threading.Event()

        def other():
            while not stop.is_set():
                progress.append(1)
                time.sleep(0.0005)

        thread = threading.Thread(target=other)
        thread.start()
        cold_work.run_cold(lambda: [cold_work.cold_yield() or sum(range(200)) for _ in range(20_000)])
        stop.set()
        thread.join()
        self.assertGreater(len(progress), 5)

    def test_marking_is_scoped_to_the_call(self):
        cold_work.run_cold(lambda: None)
        with patch.object(cold_work.time, "sleep") as sleep:
            cold_work.cold_yield()
        sleep.assert_not_called()


class QueuedLaneWaitTests(unittest.TestCase):
    def test_queued_lane_wait_stays_below_client_timeouts(self):
        from qdl.query.service import _QUEUED_LOCAL_BATCH_MAX_WAIT_MS
        self.assertLessEqual(_QUEUED_LOCAL_BATCH_MAX_WAIT_MS, 10_000)


class CancelledWorkHoldsItsPermitTests(unittest.IsolatedAsyncioTestCase):
    """KN-4 K4-T06: a cancelled/timed-out request never frees its admission
    while its worker thread still runs, and cold work stops at its next slice."""

    async def asyncSetUp(self) -> None:
        import asyncio

        from qdl.query.lanes import BoundedReadLane, ReadLanePolicy
        from qdl.query.service import _QueryWorkPools

        self.asyncio = asyncio
        self.pools = _QueryWorkPools()
        self.addCleanup(self.pools.close)
        self.lane = BoundedReadLane(ReadLanePolicy(
            max_active=1, max_pending=4, max_pending_bytes=1 << 20,
            max_active_per_consumer=1, max_pending_per_consumer=4,
        ))

    async def test_a_cancelled_cold_request_holds_the_lane_until_its_worker_stopped(self):
        started, events = threading.Event(), []

        def materialize():
            started.set()
            try:
                with patch.object(cold_work, "COLD_SLICE_SECONDS", 0.0):
                    deadline = time.perf_counter() + 3.0
                    while time.perf_counter() < deadline:  # a long cold read
                        cold_work.cold_yield()
            finally:
                events.append(("first-stopped", time.perf_counter()))

        def next_batch():
            events.append(("second-started", time.perf_counter()))
            return "second"

        async def first():
            return await self.lane.run(lambda: self.pools.cold(materialize), consumer_id="a", reserved_bytes=1)

        task = self.asyncio.create_task(first())
        await self.asyncio.to_thread(started.wait, 5)
        second = self.asyncio.create_task(self.lane.run(
            lambda: self.pools.cold(next_batch), consumer_id="b", reserved_bytes=1))
        await self.asyncio.sleep(0.05)
        self.assertEqual(self.lane.stats()["active"], 1)
        cancelled_at = time.perf_counter()
        task.cancel()
        with self.assertRaises(self.asyncio.CancelledError):
            await task
        self.assertEqual(await second, "second")
        order = [name for name, _at in events]
        self.assertEqual(order, ["first-stopped", "second-started"],
                         "the next batch starts only after the cancelled worker stopped")
        self.assertLess(events[0][1] - cancelled_at, 0.5, "cold work stops at its next slice")
        self.assertEqual(self.lane.stats()["active"], 0)

    async def test_a_non_cooperative_worker_keeps_its_caller_until_it_returns(self):
        release, finished = threading.Event(), []

        def blocking():
            release.wait(5)
            finished.append(time.perf_counter())

        task = self.asyncio.create_task(self.lane.run(
            lambda: self.pools.hot(blocking), consumer_id="a", reserved_bytes=1))
        await self.asyncio.sleep(0.05)
        task.cancel()
        await self.asyncio.sleep(0.05)
        task.cancel()  # a second cancellation does not abandon the thread
        await self.asyncio.sleep(0.05)
        self.assertFalse(task.done(), "the caller waits for its thread")
        self.assertEqual(self.lane.stats()["active"], 1)
        release.set()
        with self.assertRaises(self.asyncio.CancelledError):
            await task
        self.assertEqual(len(finished), 1)
        self.assertEqual(self.lane.stats()["active"], 0)

    async def test_the_warmup_render_holds_its_lease_through_cancellation(self):
        import importlib

        router = importlib.import_module("qdl.api_v2.router")
        started, release, done = threading.Event(), threading.Event(), []

        def build():
            started.set()
            release.wait(5)
            done.append(1)
            raise RuntimeError("never rendered: the request is gone")

        task = self.asyncio.create_task(router._warmup_json_off_loop(build))
        await self.asyncio.to_thread(started.wait, 5)
        task.cancel()
        await self.asyncio.sleep(0.05)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaises(self.asyncio.CancelledError):
            await task
        self.assertEqual(done, [1])


if __name__ == "__main__":
    unittest.main()

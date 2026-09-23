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


if __name__ == "__main__":
    unittest.main()

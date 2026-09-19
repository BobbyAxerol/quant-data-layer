"""R1.31: a projector that cannot close its broker must not stop projecting.

On 2026-09-19 two of the three projectors went silent mid-recovery, both
immediately after logging `generation failed; reconnecting attempt=5`.
`retry_max_seconds` is 5.0, so a supervisor in backoff logs every five seconds;
eleven minutes of silence was not backoff. The next statement after that log
line is the `finally` block, whose only await was an unbounded
`asyncio.to_thread(broker.close)`.

The consequence is worse than a stopped role, because the role stays `Up` and
reports nothing: Kafka rebalanced all six `md.canonical.v2` partitions onto the
one surviving projector, lag reached 47,258, and 33 of 188 spool partitions fell
outside their own declared `stale_after_ms`.

These tests drive the real supervisor with a broker whose `close` blocks, and
assert it keeps going.
"""

from __future__ import annotations

import asyncio
import threading
import unittest

from qdl.runtime.stable_projector import supervise_stable_projector


class _Engine:
    def __init__(self, fail_with: Exception | None) -> None:
        self._fail_with = fail_with

    async def run_once(self, *, timeout_seconds: float) -> bool:
        if self._fail_with is not None:
            raise self._fail_with
        return True


class _Broker:
    """A broker whose close blocks for longer than any test will wait."""

    def __init__(self, *, block: bool) -> None:
        self.block = block
        self.close_started = threading.Event()
        self.release = threading.Event()
        self.closed = False

    def close(self) -> None:
        self.close_started.set()
        if self.block:
            # Never released by the test; this is the hung Kafka client. The
            # wait is bounded only so the suite ends - the supervisor must not
            # depend on it ending.
            self.release.wait(timeout=3)
        self.closed = True


class ProjectorCloseTimeoutTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.brokers: list[_Broker] = []
        self.generations = 0

    def _factory(self, *, block: bool, error: Exception):
        def factory():
            self.generations += 1
            broker = _Broker(block=block)
            self.brokers.append(broker)
            return broker, _Engine(error)

        return factory

    async def _run(self, *, block: bool, generations: int, close_timeout: float):
        stop = {"value": False}
        slept: list[float] = []

        async def sleep(delay: float) -> None:
            slept.append(delay)
            if self.generations >= generations:
                stop["value"] = True

        await asyncio.wait_for(
            supervise_stable_projector(
                broker_factory=self._factory(block=block, error=RuntimeError("409")),
                should_stop=lambda: stop["value"],
                on_broker=lambda _broker: None,
                sleep=sleep,
                close_timeout_seconds=close_timeout,
            ),
            timeout=15,
        )
        return slept

    async def test_a_blocking_close_no_longer_stops_the_supervisor(self) -> None:
        """The defect: before the timeout this call never returned."""
        slept = await self._run(block=True, generations=3, close_timeout=0.2)
        self.assertGreaterEqual(self.generations, 3)
        self.assertGreaterEqual(len(slept), 3)

    async def test_the_blocked_close_really_was_entered(self) -> None:
        await self._run(block=True, generations=2, close_timeout=0.2)
        for broker in self.brokers:
            self.assertTrue(broker.close_started.is_set())
            self.assertFalse(broker.closed, "close must still be blocked, not completed")

    async def test_each_generation_builds_its_own_broker(self) -> None:
        """An abandoned broker is never reused by the next generation."""
        await self._run(block=True, generations=3, close_timeout=0.2)
        self.assertEqual(len({id(b) for b in self.brokers}), len(self.brokers))

    async def test_a_close_that_returns_is_still_awaited_normally(self) -> None:
        await self._run(block=False, generations=2, close_timeout=5.0)
        for broker in self.brokers:
            self.assertTrue(broker.closed)

    async def test_backoff_is_still_capped_so_silence_stays_diagnostic(self) -> None:
        """A supervisor that is alive logs at least every `retry_max_seconds`."""
        slept = await self._run(block=True, generations=6, close_timeout=0.2)
        self.assertTrue(slept)
        self.assertLessEqual(max(slept), 5.0)

    async def test_a_non_positive_close_timeout_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            await supervise_stable_projector(
                broker_factory=self._factory(block=False, error=RuntimeError("x")),
                should_stop=lambda: True,
                on_broker=lambda _broker: None,
                close_timeout_seconds=0.0,
            )


if __name__ == "__main__":
    unittest.main()

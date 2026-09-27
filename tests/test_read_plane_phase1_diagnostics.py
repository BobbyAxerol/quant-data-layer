from __future__ import annotations

import asyncio
import threading
import unittest

from qdl.domain.instrument import InstrumentRegistry
from qdl.query import (
    CanonicalErrorCode,
    EntitlementPolicy,
    InstrumentQuery,
    QueryProblem,
    QueryServiceError,
    V2QueryService,
)
from qdl.query.results import MemoryMarketDataBackend, QueryBackendError


class _BlockingGapBackend:
    """Test-only bounded backend; no provider, spool, or runtime is touched."""

    def __init__(self) -> None:
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled = False

    def open_gaps_bounded(self, *, cancelled):
        self.calls += 1
        self.started.set()
        while not self.release.wait(0.005):
            if cancelled():
                self.cancelled = True
                raise QueryBackendError(QueryProblem(
                    CanonicalErrorCode.PARTIAL_RESULT,
                    "global gap diagnostic was cancelled",
                    True,
                    retry_after_ms=1_000,
                ))
        return ()

    def open_gaps(self):
        raise AssertionError("bounded diagnostic path must be selected")


class _FailingGapBackend:
    def open_gaps_bounded(self, *, cancelled):
        del cancelled
        raise QueryBackendError(QueryProblem(
            CanonicalErrorCode.PARTIAL_RESULT,
            "global gap diagnostic exceeded its work deadline",
            True,
            retry_after_ms=1_000,
        ))


def _service(backend) -> V2QueryService:
    return V2QueryService(
        instruments=InstrumentQuery(InstrumentRegistry()),
        backend=backend,
        entitlements=EntitlementPolicy(()),
    )


class ReadPlanePhase1DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_waiter_does_not_cancel_coalesced_bounded_worker(self):
        backend = _BlockingGapBackend()
        service = _service(backend)
        first = asyncio.create_task(service.open_gaps_async())
        await asyncio.wait_for(asyncio.to_thread(backend.started.wait), timeout=1)
        second = asyncio.create_task(service.open_gaps_async())
        await asyncio.sleep(0)

        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertEqual(backend.calls, 1)
        self.assertFalse(backend.cancelled)

        backend.release.set()
        self.assertEqual(await asyncio.wait_for(second, timeout=1), ())
        self.assertEqual(backend.calls, 1)
        self.assertIsNone(service._gap_scan_task)

    async def test_typed_incomplete_scan_is_preserved_at_query_boundary(self):
        service = _service(_FailingGapBackend())
        with self.assertRaises(QueryServiceError) as error:
            await service.open_gaps_async()
        self.assertEqual(error.exception.problem.code, CanonicalErrorCode.PARTIAL_RESULT)
        self.assertTrue(error.exception.problem.retryable)
        self.assertEqual(error.exception.problem.retry_after_ms, 1_000)

    async def test_worker_shutdown_cancels_the_bounded_scan_without_a_stuck_task(self):
        backend = _BlockingGapBackend()
        service = _service(backend)
        waiter = asyncio.create_task(service.open_gaps_async())
        await asyncio.wait_for(asyncio.to_thread(backend.started.wait), timeout=1)
        worker = service._gap_scan_task
        self.assertIsNotNone(worker)
        worker.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        for _ in range(20):
            if backend.cancelled:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(backend.cancelled)
        self.assertIsNone(service._gap_scan_task)

    async def test_memory_backend_cancellation_is_typed_not_an_empty_success(self):
        service = _service(MemoryMarketDataBackend())
        with self.assertRaises(QueryBackendError) as error:
            service.backend.open_gaps_bounded(cancelled=lambda: True)
        self.assertEqual(error.exception.problem.code, CanonicalErrorCode.PARTIAL_RESULT)


if __name__ == "__main__":
    unittest.main()

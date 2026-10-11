"""Opt-in replica redirect for a busy large-warmup lane (P18.4 G5, stage 100f).

Two 2,500-row cold warmups that landed on one Query replica ran one after the
other on its single-active large-warmup lane (3.5 s and 7.0 s) while the other
replica was idle. A client that sends ``X-QDL-Replica-Redirect: 1`` is refused
at once with retryable RATE_LIMITED / REPLICA_LANE_BUSY instead of queuing;
without the header (older clients) the request still queues.
"""

from __future__ import annotations

import asyncio
import importlib
import threading
from types import SimpleNamespace
import unittest

from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    CanonicalErrorCode,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    QueryProblem,
    V2QueryService,
)
from qdl.query.lanes import BoundedReadLane, ReadLaneBusy, ReadLaneRejected
from qdl.query.service import (
    REPLICA_LANE_BUSY_REASON,
    QueryServiceError,
    _LocalBatchAdmission,
    _queued_local_batch_lane_policy,
)
from qdl.warmup import WarmupSpecification
from qdl.warmup.executor import WarmupExecution

# ``qdl.api_v2.router`` the attribute is the APIRouter; the module holds the dependencies.
router_module = importlib.import_module("qdl.api_v2.router")


class LaneRefuseIfQueuedTests(unittest.IsolatedAsyncioTestCase):
    async def test_idle_lane_runs_busy_lane_refuses_without_reserving(self):
        lane = BoundedReadLane(_queued_local_batch_lane_policy())
        self.assertEqual(
            await lane.run(_ok, consumer_id="a", reserved_bytes=1, refuse_if_queued=True), "ok"
        )
        hold = asyncio.Event()
        first = asyncio.create_task(lane.run(hold.wait, consumer_id="a", reserved_bytes=1))
        while lane.stats()["active"] < 1:
            await asyncio.sleep(0)
        with self.assertRaises(ReadLaneBusy):
            await lane.run(_ok, consumer_id="b", reserved_bytes=1, refuse_if_queued=True)
        stats = lane.stats()
        self.assertEqual((stats["pending"], stats["pending_bytes"]), (1, 1))
        self.assertEqual((stats["busy_refusals"], stats["rejected"]), (1, 0))
        # Without the flag the same request queues and completes (unchanged).
        queued = asyncio.create_task(lane.run(_ok, consumer_id="b", reserved_bytes=1))
        while lane.stats()["pending"] < 2:
            await asyncio.sleep(0)
        hold.set()
        self.assertEqual(await queued, "ok")
        await first

    async def test_full_queue_on_a_busy_lane_is_busy_not_a_plain_refusal(self):
        lane = BoundedReadLane(_queued_local_batch_lane_policy())
        hold = asyncio.Event()
        held = [asyncio.create_task(lane.run(hold.wait, consumer_id="a", reserved_bytes=1))
                for _ in range(8)]
        while lane.stats()["pending"] < 8:
            await asyncio.sleep(0)
        with self.assertRaises(ReadLaneBusy):
            await lane.run(_ok, consumer_id="a", reserved_bytes=1, refuse_if_queued=True)
        with self.assertRaises(ReadLaneRejected) as plain:
            await lane.run(_ok, consumer_id="a", reserved_bytes=1)
        self.assertNotIsInstance(plain.exception, ReadLaneBusy)
        hold.set()
        await asyncio.gather(*held)


async def _ok():
    return "ok"


def _service(*, small_lane: bool = True):
    class Backend:
        def __init__(self):
            self.large_started = threading.Event()
            self.gate = threading.Event()
            self.calls = 0

        @staticmethod
        def warmup_is_local(_requirement):
            return True

        def history_many(self, requirements):
            self.calls += 1
            if any(_rows(item) > 2 for item in requirements):
                self.large_started.set()
                self.gate.wait(5)
            return {requirement: "history" for requirement in requirements}

    class Executor:
        @staticmethod
        def stats():
            return {}

        async def execute(self, items, *, work, identity, provider, deadline_ms):
            del identity, provider, deadline_ms
            return tuple(
                [WarmupExecution(item, await work(item), None, 1, False, 0.0) for item in items]
            )

    class Service(V2QueryService):
        def __init__(self):
            self.backend = Backend()
            self.instruments = SimpleNamespace()
            self.warmup_executor = Executor()
            self.last_batch_evidence = {}
            self._small_local_warmup_lane = small_lane
            self._queued_local_batch_lane = True
            self._local_batch_admission = _LocalBatchAdmission(
                policy=_queued_local_batch_lane_policy()
            )

        def _warmup_from_history(self, requirement, history, *, purpose, request_id):
            del requirement, purpose, request_id
            return history

    return Service()


def _requirement(uid: str, rows: int):
    return DataRequirement(
        instrument_uid=uid, feed=FeedType.BAR, consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="crypto_primary_v2", interval="1m",
        warmup=WarmupSpecification.for_rows(rows, deadline_ms=5_000),
    )


def _rows(requirement) -> int:
    specification = requirement.warmup_specification
    return 1 if specification is None or specification.rows is None else specification.rows


async def _warm(service, consumer, *requirements, **kwargs):
    async def completion(result):
        return result
    return await service.warmup_batch_completed_async(
        BatchRequirement(consumer_id=consumer, requirements=requirements),
        purpose=AccessPurpose.INTERNAL_ALPHA, completion=completion, **kwargs,
    )


class ServiceLaneBusyRedirectTests(unittest.IsolatedAsyncioTestCase):
    async def _hold_large(self, service):
        large = asyncio.create_task(_warm(service, "alpha.okx", _requirement("okx", 2500)))
        await asyncio.wait_for(asyncio.to_thread(service.backend.large_started.wait, 2), 3)
        return large

    async def test_opt_in_large_warmup_is_refused_at_once_typed_and_retryable(self):
        service = _service()
        large = await self._hold_large(service)
        with self.assertRaises(QueryServiceError) as raised:
            await asyncio.wait_for(
                _warm(service, "alpha.binance", _requirement("bin", 2500), redirect_when_busy=True), 1
            )
        error = raised.exception
        self.assertEqual(error.problem.code, CanonicalErrorCode.RATE_LIMITED)
        self.assertTrue(error.problem.retryable)
        self.assertEqual(error.problem.retry_after_ms, 0)
        self.assertEqual(error.reason, REPLICA_LANE_BUSY_REASON)
        self.assertTrue(error.problem.detail.startswith("REPLICA_LANE_BUSY:"))
        self.assertEqual(error.instrument_uid, "bin")
        self.assertEqual(service.backend.calls, 1)  # nothing read for the refused one
        lane = service._local_batch_admission_for((_requirement("bin", 2500),))
        self.assertEqual(lane.stats()["pending"], 1)
        service.backend.gate.set()
        self.assertEqual([item.status for item in (await large).results], ["OK"])

    async def test_without_opt_in_the_request_queues_as_before(self):
        service = _service()
        large = await self._hold_large(service)
        queued = asyncio.create_task(_warm(service, "alpha.binance", _requirement("bin", 2500)))
        lane = service._local_batch_admission_for((_requirement("bin", 2500),))
        while lane.stats()["pending"] < 2:
            await asyncio.sleep(0)
        service.backend.gate.set()
        results = await asyncio.gather(large, queued)
        self.assertEqual([r.results[0].status for r in results], ["OK", "OK"])
        self.assertEqual(lane.stats()["busy_refusals"], 0)

    async def test_opt_in_on_an_idle_lane_and_small_warmups_are_served(self):
        service = _service()
        idle = await _warm(service, "alpha.binance", _requirement("bin", 2500), redirect_when_busy=True)
        self.assertEqual([item.status for item in idle.results], ["OK"])
        service.backend.large_started.clear()
        large = await self._hold_large(service)
        small = await asyncio.wait_for(
            _warm(service, "alpha.binance", _requirement("handoff", 1), redirect_when_busy=True), 2
        )
        self.assertEqual([item.status for item in small.results], ["OK"])
        service.backend.gate.set()
        await large


class _Access(SimpleNamespace):
    def __getattr__(self, name):
        if name.startswith("require_"):
            return lambda *args, **kwargs: None
        raise AttributeError(name)


class _RecordingService:
    """Records the opt-in the router passes; refuses like Query when it is set."""

    def __init__(self):
        self.calls: list[dict] = []

    @staticmethod
    def request_id():
        return "req-1"

    async def warmup_batch_completed_async(self, batch, *, purpose, completion, **kwargs):
        del purpose, completion
        self.calls.append(kwargs)
        if kwargs.get("redirect_when_busy"):
            raise QueryServiceError(
                QueryProblem(CanonicalErrorCode.RATE_LIMITED,
                             "REPLICA_LANE_BUSY: this replica's large-warmup lane is busy", True, 0),
                request_id="req-1", instrument_uid=batch.requirements[0].instrument_uid,
                reason=REPLICA_LANE_BUSY_REASON,
            )
        return JSONResponse({"queued": True})


class RouterReplicaRedirectHeaderTests(unittest.TestCase):
    def setUp(self):
        self.service = _RecordingService()
        app = create_v2_app(self.service)  # type: ignore[arg-type]
        app.dependency_overrides[router_module._data_access] = lambda: _Access(consumer_id="alpha")
        self.client = TestClient(app)

    def _get(self, headers):
        return self.client.get(
            "/v2/market-data/inst-1/warmup",
            params={"feed": "BAR", "source_policy_id": "crypto_primary_v2",
                    "interval": "1m", "limit": 2500},
            headers={"X-QDL-Purpose": "INTERNAL_ALPHA", **headers},
        )

    def test_header_one_refuses_with_typed_reason_and_retry_after_zero(self):
        response = self._get({"X-QDL-Replica-Redirect": "1"})
        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.headers["X-QDL-Refusal-Reason"], "REPLICA_LANE_BUSY")
        self.assertEqual(response.headers["Retry-After"], "0")
        body = response.json()
        self.assertEqual((body["code"], body["retryable"], body["retry_after_ms"]),
                         ("RATE_LIMITED", True, 0))
        self.assertEqual(self.service.calls, [{"redirect_when_busy": True}])

    def test_absent_zero_or_other_values_queue_exactly_as_before(self):
        for headers in ({}, {"X-QDL-Replica-Redirect": "0"}, {"X-QDL-Replica-Redirect": "yes"}):
            response = self._get(headers)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn("X-QDL-Refusal-Reason", response.headers)
        self.assertEqual(self.service.calls, [{}, {}, {}])

    def test_ordinary_refusal_carries_no_reason_header(self):
        async def refuse(batch, *, purpose, completion, **kwargs):
            raise QueryServiceError(
                QueryProblem(CanonicalErrorCode.RATE_LIMITED, "read lane is full", True),
                request_id="req-2",
            )
        self.service.warmup_batch_completed_async = refuse
        response = self._get({"X-QDL-Replica-Redirect": "1"})
        self.assertEqual(response.status_code, 429)
        self.assertNotIn("X-QDL-Refusal-Reason", response.headers)
        self.assertNotIn("Retry-After", response.headers)


if __name__ == "__main__":
    unittest.main()

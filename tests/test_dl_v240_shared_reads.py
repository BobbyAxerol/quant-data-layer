"""DL v2.4.0 S4/S5/S6: direct in-process MARK/INDEX view call, key-level latest-view singleflight, and the
session-fair warmup queue with warmup singleflight (offline, no runtime).

The S6 cases are the multi-member/concurrency proof the brief asks for: one tier identity fronting 100
sessions, many concurrent waiters, a Trading System request in the middle of a storm, identical warmups
sharing one materialization, and abandoned waiters."""

from __future__ import annotations

import asyncio
import threading
import time
import unittest
from dataclasses import replace
from types import SimpleNamespace

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
from qdl.query import QueryBackendError
from qdl.query.lanes import FairLanePolicy, FairWarmupLane, ReadLaneAtCapacity, ReadLaneRejected
from qdl.query.service import (
    _LocalBatchAdmission,
    _queued_local_batch_lane_policy,
    _small_local_warmup_lane_policy,
    _warmup_flight_key,
)
from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend
from qdl.security import request_cost
from qdl.warmup import WarmupSpecification
from qdl.warmup.executor import WarmupExecution
from tests import test_execution_mark_index_live_view as live
from tests.test_execution_mark_index_live_view import NOW_NS, _envelope, _hydration_stored

TS = "trading-system.paper.stable"


# --------------------------------------------------------------------- S6 lane

class FairWarmupLaneTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_storm_of_100_sessions_is_served_with_no_refusal(self):
        lane = FairWarmupLane(_queued_local_batch_lane_policy())
        served: list[str] = []

        async def session(index: int):
            async def work():
                await asyncio.sleep(0.002)
                served.append(f"s{index}")
                return index
            return await lane.run(work, consumer_id="alpha.okx.paper.stable", fairness_key=f"alpha.okx#s{index}",
                                  reserved_bytes=2_500 * 1024, wait_timeout_ms=20_000)

        results = await asyncio.gather(*(session(i) for i in range(100)))
        self.assertEqual(sorted(results), list(range(100)))
        stats = lane.stats()
        self.assertEqual((stats["rejected"], stats["queue_wait_timeouts"], stats["active"], stats["pending"]),
                         (0, 0, 0, 0))
        self.assertEqual(served, [f"s{i}" for i in range(100)])  # FIFO across sessions

    async def test_a_trading_system_request_jumps_the_storm(self):
        lane = FairWarmupLane(_queued_local_batch_lane_policy())
        order: list[str] = []
        gate = asyncio.Event()

        async def run(name, consumer, key=None):
            async def work():
                if name == "first":
                    await gate.wait()
                order.append(name)
            await lane.run(work, consumer_id=consumer, fairness_key=key, reserved_bytes=1, wait_timeout_ms=10_000)

        tasks = [asyncio.create_task(run("first", "alpha", "alpha#0"))]
        await asyncio.sleep(0)
        tasks += [asyncio.create_task(run(f"a{i}", "alpha", f"alpha#{i}")) for i in range(1, 20)]
        await asyncio.sleep(0)
        tasks.append(asyncio.create_task(run("ts", TS, "ignored-for-ts")))
        while lane.stats()["waiting"] < 20:
            await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*tasks)
        self.assertEqual(order[:2], ["first", "ts"])

    async def test_session_fair_rounds_serve_every_first_request_before_a_second(self):
        lane = FairWarmupLane(_queued_local_batch_lane_policy())
        order: list[str] = []
        gate = asyncio.Event()

        async def run(name, key):
            async def work():
                if name == "hold":
                    await gate.wait()
                order.append(name)
            await lane.run(work, consumer_id="alpha", fairness_key=key, reserved_bytes=1, wait_timeout_ms=10_000)

        tasks = [asyncio.create_task(run("hold", "alpha#hold"))]
        await asyncio.sleep(0)
        tasks += [asyncio.create_task(run(f"A{i}", "alpha#A")) for i in range(1, 4)]
        await asyncio.sleep(0)
        tasks += [asyncio.create_task(run("B1", "alpha#B")), asyncio.create_task(run("C1", "alpha#C"))]
        while lane.stats()["waiting"] < 5:
            await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*tasks)
        self.assertEqual(order, ["hold", "A1", "B1", "C1", "A2", "A3"])

    async def test_a_waiter_waits_up_to_its_deadline_and_a_refusal_carries_a_position_hint(self):
        lane = FairWarmupLane(_queued_local_batch_lane_policy())
        gate = asyncio.Event()
        holder = asyncio.create_task(lane.run(gate.wait, consumer_id="a", reserved_bytes=1))
        await asyncio.sleep(0)
        started = time.monotonic()
        with self.assertRaises(ReadLaneRejected) as late:
            await lane.run(_ok, consumer_id="b", reserved_bytes=1, wait_timeout_ms=300)
        self.assertGreaterEqual(time.monotonic() - started, 0.29)
        self.assertNotIsInstance(late.exception, ReadLaneAtCapacity)
        self.assertTrue(250 <= late.exception.retry_after_ms <= 2_000)
        gate.set()
        await holder
        self.assertEqual(lane.stats()["pending"], 0)

    async def test_full_bounds_are_typed_capacity_refusals_with_hints(self):
        policy = replace(_queued_local_batch_lane_policy(), max_pending=8, max_pending_per_consumer=6,
                         max_pending_per_session=2, reserved_pending_slots=2)
        lane = FairWarmupLane(policy)
        gate = asyncio.Event()
        held = [asyncio.create_task(lane.run(gate.wait, consumer_id="a", fairness_key="a#1", reserved_bytes=1))
                for _ in range(2)]
        await asyncio.sleep(0)
        with self.assertRaises(ReadLaneAtCapacity) as session_full:
            await lane.run(_ok, consumer_id="a", fairness_key="a#1", reserved_bytes=1)
        self.assertIn("session", str(session_full.exception))
        held += [asyncio.create_task(lane.run(gate.wait, consumer_id="a", reserved_bytes=1)) for _ in range(4)]
        await asyncio.sleep(0)
        with self.assertRaises(ReadLaneAtCapacity):
            await lane.run(_ok, consumer_id="a", fairness_key="a#2", reserved_bytes=1)  # identity 6/6
        # The non-reserved class cannot take the last 2 slots; the TS identity can.
        with self.assertRaises(ReadLaneAtCapacity) as full:
            await lane.run(_ok, consumer_id="b", reserved_bytes=1)
        self.assertEqual(full.exception.retry_after_ms, 2_000)
        held.append(asyncio.create_task(lane.run(_ok, consumer_id=TS, reserved_bytes=1)))
        await asyncio.sleep(0)
        self.assertEqual(lane.stats()["pending"], 7)
        gate.set()
        await asyncio.gather(*held)

    async def test_an_abandoned_waiter_is_withdrawn_before_any_work(self):
        policy = replace(_queued_local_batch_lane_policy(), abandon_poll_ms=50)
        lane = FairWarmupLane(policy)
        gate = asyncio.Event()
        holder = asyncio.create_task(lane.run(gate.wait, consumer_id="a", reserved_bytes=1))
        await asyncio.sleep(0)
        gone = asyncio.Event()
        ran = []

        async def abandoned():
            return gone.is_set()

        async def work():
            ran.append(1)

        waiter = asyncio.create_task(lane.run(work, consumer_id="b", reserved_bytes=1, wait_timeout_ms=10_000,
                                              abandoned=abandoned))
        await asyncio.sleep(0.06)
        gone.set()
        with self.assertRaises(ReadLaneRejected):
            await waiter
        self.assertEqual((lane.stats()["abandoned"], lane.stats()["waiting"]), (1, 0))
        gate.set()
        await holder
        self.assertEqual(ran, [])

    async def test_a_cancelled_waiter_and_a_cancelled_runner_release_cleanly(self):
        lane = FairWarmupLane(_small_local_warmup_lane_policy())
        gate = asyncio.Event()
        runners = [asyncio.create_task(lane.run(gate.wait, consumer_id="a", fairness_key=f"a#{i}",
                                                reserved_bytes=1)) for i in range(2)]
        waiter = asyncio.create_task(lane.run(_ok, consumer_id="a", fairness_key="a#9", reserved_bytes=1))
        await asyncio.sleep(0)
        waiter.cancel()
        runners[0].cancel()
        await asyncio.gather(waiter, runners[0], return_exceptions=True)
        gate.set()
        await runners[1]
        self.assertEqual({k: lane.stats()[k] for k in ("active", "pending", "pending_bytes")},
                         {"active": 0, "pending": 0, "pending_bytes": 0})


async def _ok():
    return "ok"


class HotLaneSessionFairnessTests(unittest.IsolatedAsyncioTestCase):
    """Coordinator review: tier identities put ~100 alpha sessions on ONE identity; the hot lanes must not serialise them."""

    async def test_100_sessions_on_one_identity_run_up_to_the_lane_cap_with_no_refusal(self):
        from qdl.query.service import _hot_snapshot_fair_policy

        lane = FairWarmupLane(_hot_snapshot_fair_policy())
        gate = asyncio.Event()
        running = []

        async def read(i):
            async def work():
                running.append(i)
                await gate.wait()
            await lane.run(work, consumer_id="alpha.okx.paper.stable", fairness_key=f"alpha#s{i}",
                           reserved_bytes=16 * 1024)

        tasks = [asyncio.create_task(read(i)) for i in range(100)]
        while lane.stats()["pending"] < 100:
            await asyncio.sleep(0)
        self.assertEqual(lane.stats()["active"], 2)  # the lane cap (2 hot threads), not 1 per identity
        self.assertEqual(lane.stats()["rejected"], 0)
        gate.set()
        await asyncio.gather(*tasks)
        self.assertEqual(sorted(running), list(range(100)))

    async def test_without_a_session_header_one_identity_still_runs_one_at_a_time(self):
        from qdl.query.service import _hot_snapshot_fair_policy

        lane = FairWarmupLane(_hot_snapshot_fair_policy())
        gate = asyncio.Event()
        tasks = [asyncio.create_task(lane.run(gate.wait, consumer_id="alpha", reserved_bytes=1)) for _ in range(5)]
        while lane.stats()["pending"] < 5:
            await asyncio.sleep(0)
        self.assertEqual(lane.stats()["active"], 1)
        gate.set()
        await asyncio.gather(*tasks)

    def test_the_hot_lane_wait_ends_before_the_ts_writer_query_deadline(self):
        from qdl.query.service import _hot_lane_wait_ms

        self.assertEqual(_hot_lane_wait_ms(), 7_500)          # < TS query_deadline_ms 10 000
        self.assertEqual(_hot_lane_wait_ms(20_000), 7_500)
        self.assertEqual(_hot_lane_wait_ms(2_000), 1_500)
        self.assertGreaterEqual(_hot_lane_wait_ms(1), 1)

    async def test_an_expired_hot_wait_is_a_rate_limited_refusal_with_a_hint(self):
        service = V2QueryService.__new__(V2QueryService)
        release = threading.Event()

        def snapshot(requirement, *, purpose, request_id=None):
            release.wait(5)
            return "snapshot"

        service.snapshot = snapshot
        from unittest.mock import patch

        with patch("qdl.query.service._hot_lane_wait_ms", return_value=200):
            busy = [asyncio.create_task(service.snapshot_async(
                SimpleNamespace(instrument_uid="u"), purpose=object(), consumer_id="alpha",
                fairness_key=f"alpha#{i}")) for i in range(2)]
            await asyncio.sleep(0.05)
            from qdl.query import QueryServiceError

            with self.assertRaises(QueryServiceError) as late:
                await service.snapshot_async(SimpleNamespace(instrument_uid="u"), purpose=object(),
                                             consumer_id="alpha", fairness_key="alpha#3")
        self.assertEqual(late.exception.problem.code, CanonicalErrorCode.RATE_LIMITED)
        self.assertTrue(250 <= late.exception.problem.retry_after_ms <= 2_000)
        release.set()
        self.assertEqual(await asyncio.gather(*busy), ["snapshot", "snapshot"])
        await service.close()

    async def test_one_noisy_session_is_throttled_alone_and_ts_keeps_priority(self):
        from qdl.query.service import _hot_reference_fair_policy

        lane = FairWarmupLane(_hot_reference_fair_policy())
        gate = asyncio.Event()
        noisy = [asyncio.create_task(lane.run(gate.wait, consumer_id="alpha", fairness_key="alpha#noisy",
                                              reserved_bytes=1)) for _ in range(20)]
        others = [asyncio.create_task(lane.run(gate.wait, consumer_id="alpha", fairness_key=f"alpha#q{i}",
                                               reserved_bytes=1)) for i in range(50)]
        ts = asyncio.create_task(lane.run(gate.wait, consumer_id=TS, reserved_bytes=1))
        for _ in range(200):  # the 12 beyond the session's 8 are refused at once; 8 wait on the gate
            if sum(task.done() for task in noisy) == 12:
                break
            await asyncio.sleep(0)
        refused = [task.exception() for task in noisy if task.done()]
        self.assertEqual(len(refused), 12)
        self.assertTrue(all(isinstance(r, ReadLaneAtCapacity) and "session" in str(r) for r in refused))
        gate.set()
        self.assertTrue(all(not isinstance(r, Exception) for r in await asyncio.gather(*others, ts)))
        await asyncio.gather(*(task for task in noisy if not task.done()))


# --------------------------------------------------------------- S6 service

def _requirement(uid: str, rows: int, *, deadline_ms: int = 20_000):
    return DataRequirement(
        instrument_uid=uid, feed=FeedType.BAR, consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="crypto_primary_v2", interval="1m",
        warmup=WarmupSpecification.for_rows(rows, deadline_ms=deadline_ms),
    )


def _service(*, read_seconds: float = 0.0, fail: bool = False):
    class Backend:
        def __init__(self):
            self.calls = 0
            self.items = 0
            self.lock = threading.Lock()

        @staticmethod
        def warmup_is_local(_requirement):
            return True

        def history_many(self, requirements):
            with self.lock:
                self.calls += 1
                self.items += len(requirements)
            if read_seconds:
                time.sleep(read_seconds)
            if fail:
                return {item: QueryBackendError(QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "nr", True, 1_000))
                        for item in requirements}
            return {item: ("history", item.instrument_uid) for item in requirements}

    class Executor:
        @staticmethod
        def stats():
            return {}

        async def execute(self, items, *, work, identity, provider, deadline_ms):
            del identity, provider, deadline_ms
            out = []
            for item in items:
                try:
                    out.append(WarmupExecution(item, await work(item), None, 1, False, 0.0))
                except Exception as error:  # noqa: BLE001 - per-item outcome
                    out.append(WarmupExecution(item, None, error, 1, False, 0.0))
            return tuple(out)

    class Service(V2QueryService):
        def __init__(self):
            self.backend = Backend()
            self.instruments = SimpleNamespace()
            self.warmup_executor = Executor()
            self.last_batch_evidence = {}
            self._small_local_warmup_lane = True
            self._queued_local_batch_lane = True
            self._local_batch_admission = _LocalBatchAdmission(policy=_queued_local_batch_lane_policy())
            self._small_local_warmup_admission = _LocalBatchAdmission(policy=_small_local_warmup_lane_policy())

        def _warmup_from_history(self, requirement, history, *, purpose, request_id):
            del purpose, request_id
            return (requirement.instrument_uid, history)

    return Service()


async def _warm(service, consumer, *requirements, session=None):
    async def completion(result):
        return result
    return await service.warmup_batch_completed_async(
        BatchRequirement(consumer_id=consumer, requirements=requirements),
        purpose=AccessPurpose.INTERNAL_ALPHA, completion=completion,
        fairness_key=None if session is None else f"{consumer}#{session}",
    )


class WarmupServiceStormTests(unittest.IsolatedAsyncioTestCase):
    async def test_identical_warmups_share_one_materialization(self):
        service = _service(read_seconds=0.05)
        requirement = _requirement("okx-btc", 2_500)
        results = await asyncio.gather(*(
            _warm(service, "alpha.okx.paper.stable", replace(requirement, warmup=replace(
                requirement.warmup, deadline_ms=10_000 + i)), session=f"s{i}")
            for i in range(50)))
        self.assertTrue(all(r.results[0].status == "OK" for r in results))
        self.assertEqual(service.backend.calls, 1)
        flights = service._warmup_flights_for().stats()
        self.assertEqual((flights["joined"], flights["led"], flights["in_flight"]), (49, 1, 0))
        # A request after the flight landed reads fresh data.
        await _warm(service, "alpha.okx.paper.stable", requirement)
        self.assertEqual(service.backend.calls, 2)

    async def test_a_shared_typed_refusal_is_a_fresh_exception_per_follower(self):
        service = _service(read_seconds=0.05, fail=True)
        requirement = _requirement("okx-btc", 2_500)
        results = await asyncio.gather(*(_warm(service, "alpha", requirement, session=f"s{i}") for i in range(5)))
        problems = [r.results[0].problem for r in results]
        self.assertTrue(all(p.code is CanonicalErrorCode.DATA_NOT_READY for p in problems))
        self.assertEqual(service.backend.calls, 1)

    async def test_restart_storm_100_sessions_distinct_warmups_all_served(self):
        service = _service(read_seconds=0.002)
        results = await asyncio.gather(*(
            _warm(service, "alpha.okx.paper.stable", _requirement(f"uid-{i % 40}", 2_500), session=f"s{i}")
            for i in range(100)))
        self.assertTrue(all(r.results[0].status == "OK" for r in results))
        lane = service._local_batch_admission.stats()
        self.assertEqual((lane["rejected"], lane["queue_wait_timeouts"]), (0, 0))
        self.assertLessEqual(service.backend.calls, 100)

    async def test_no_cross_wait_between_overlapping_batches(self):
        service = _service(read_seconds=0.02)
        a, b, c = (_requirement(uid, 2_500) for uid in ("a", "b", "c"))
        first, second = await asyncio.wait_for(asyncio.gather(
            _warm(service, "alpha", a, b, session="x"), _warm(service, "alpha", b, c, session="y")), 5)
        self.assertEqual([i.status for i in first.results + second.results], ["OK"] * 4)

    async def test_the_cost_notes_mark_followers_as_shared(self):
        service = _service(read_seconds=0.05)
        requirement = _requirement("okx-btc", 2_500)

        async def costed(index):
            cost, token = request_cost.begin()
            try:
                await _warm(service, "alpha", requirement, session=f"s{index}")
            finally:
                request_cost.end(token)
            return cost

        costs = await asyncio.gather(*(costed(i) for i in range(4)))
        self.assertEqual(sum(not c.free() for c in costs), 1)
        self.assertEqual(sum(c.free() for c in costs), 3)

    def test_the_queue_wait_never_outlives_the_client(self):
        from qdl.query.service import _fair_queue_wait_ms

        self.assertEqual(_fair_queue_wait_ms(20_000), 19_000)  # SDK 2.8.0 waits 22 s
        self.assertEqual(_fair_queue_wait_ms(120_000), 59_000)  # capped under the 90 s request deadline
        self.assertEqual(_fair_queue_wait_ms(100), 75)
        self.assertGreaterEqual(_fair_queue_wait_ms(1), 1)

    def test_the_flight_key_ignores_only_the_deadline(self):
        base = _requirement("x", 100, deadline_ms=5_000)
        self.assertEqual(_warmup_flight_key(base), _warmup_flight_key(_requirement("x", 100, deadline_ms=9_000)))
        self.assertNotEqual(_warmup_flight_key(base), _warmup_flight_key(_requirement("x", 101)))
        self.assertNotEqual(_warmup_flight_key(base), _warmup_flight_key(replace(base, max_freshness_ms=1)))


# ------------------------------------------------------------------- S5 view

class _SharedViewBackend:
    """Only the attributes ``_shared_view`` uses."""

    _shared_view = KnMarketCacheQueryBackend._shared_view
    _shared_outcome = staticmethod(KnMarketCacheQueryBackend._shared_outcome)

    def __init__(self, ttl_ms: int, *, read_seconds: float = 0.0, problem: QueryProblem | None = None):
        self._shared_view_ttl_ns = ttl_ms * 1_000_000
        self._shared_views = {}
        self._shared_view_locks = {}
        self._shared_view_guard = threading.Lock()
        self.shared_view_stats = {"reads": 0, "hits": 0}
        self.reads = 0
        self._read_seconds = read_seconds
        self._problem = problem
        self._lock = threading.Lock()

    def _view(self, binding, *, last):
        with self._lock:
            self.reads += 1
            number = self.reads
        if self._read_seconds:
            time.sleep(self._read_seconds)
        if self._problem is not None:
            raise QueryBackendError(self._problem)
        return SimpleNamespace(binding=binding.binding_id, last=last, number=number)


class SharedLatestViewTests(unittest.TestCase):
    binding = SimpleNamespace(binding_id="okx-btc-quote")

    def test_concurrent_readers_share_one_market_cache_read(self):
        backend = _SharedViewBackend(20, read_seconds=0.005)
        views, barrier = [], threading.Barrier(16)

        def reader():
            barrier.wait()
            views.append(backend._shared_view(self.binding, 1))

        threads = [threading.Thread(target=reader) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(backend.reads, 1)
        self.assertEqual({id(v) for v in views}, {id(views[0])})
        self.assertEqual(backend.shared_view_stats, {"reads": 1, "hits": 15})

    def test_the_view_expires_after_its_ttl_and_keys_are_separate(self):
        backend = _SharedViewBackend(5)
        first = backend._shared_view(self.binding, 1)
        self.assertIs(backend._shared_view(self.binding, 1), first)
        self.assertIsNot(backend._shared_view(self.binding, 2), first)
        time.sleep(0.006)
        self.assertIsNot(backend._shared_view(self.binding, 1), first)
        self.assertEqual(backend.reads, 3)

    def test_a_typed_refusal_is_shared_as_a_fresh_exception(self):
        problem = QueryProblem(CanonicalErrorCode.DEPENDENCY_UNAVAILABLE, "market cache: down", True, 500)
        backend = _SharedViewBackend(20, problem=problem)
        errors = []
        for _ in range(3):
            with self.assertRaises(QueryBackendError) as raised:
                backend._shared_view(self.binding, 1)
            errors.append(raised.exception)
        self.assertEqual(backend.reads, 1)
        self.assertEqual(len({id(e) for e in errors}), 3)
        self.assertTrue(all(e.problem == problem for e in errors))

    def test_ttl_zero_reads_every_time(self):
        backend = _SharedViewBackend(0)
        backend._shared_view(self.binding, 1)
        backend._shared_view(self.binding, 1)
        self.assertEqual(backend.reads, 2)

    def test_cost_notes_mark_hits_as_shared(self):
        backend = _SharedViewBackend(20)
        costs = []
        for _ in range(2):
            cost, token = request_cost.begin()
            backend._shared_view(self.binding, 1)
            request_cost.end(token)
            costs.append(cost)
        self.assertEqual([(c.work, c.shared_hits) for c in costs], [(1, 0), (0, 1)])

    def test_the_backup_path_never_reads_the_shared_memo(self):
        # rc4 precedent: _hot_candidate(binding, primary) reads hot_client.latest directly.
        import inspect

        source = inspect.getsource(KnMarketCacheQueryBackend._hot_candidate)
        self.assertIn("self._hot_latest(binding) if shared else self.hot_client.latest(", source)
        self.assertNotIn("_shared_view", source)

    def test_the_constructor_bounds_the_ttl(self):
        with self.assertRaises(ValueError):
            KnMarketCacheQueryBackend.__init__(object.__new__(KnMarketCacheQueryBackend), None, None,
                                               schema_digest="x", topic_id="t", shared_view_ttl_ms=21)


# ------------------------------------------------------------------- S4 MARK

class DirectMarkIndexReaderEquivalenceTests(unittest.IsolatedAsyncioTestCase):
    """The direct view call returns exactly what the ASGI hop returned, OK and refused."""

    setUp = live.LocalAlphaMarkIndexReaderTests.setUp
    _request = live.LocalAlphaMarkIndexReaderTests._request

    async def _both(self, **kwargs):
        from qdl.reference.local_mark_index import InProcessViewClient, reader_for_view

        direct, hop = reader_for_view(self.view), reader_for_view(self.view, direct=False)
        self.assertIsInstance(direct.client, InProcessViewClient)
        a = await direct.fetch(self._request(), source_policy_id="crypto_liquid_v2", **kwargs)
        b = await hop.fetch(self._request(), source_policy_id="crypto_liquid_v2", **kwargs)
        return a, b

    def _same(self, a, b):
        self.assertEqual(a.status, b.status)
        self.assertEqual(a.error_code, b.error_code)
        self.assertEqual(a.observations, b.observations)
        self.assertEqual(a.lineage, b.lineage)
        self.assertEqual(a.coverage, b.coverage)

    async def test_ok_and_refusals_are_identical(self):
        empty = await self._both(max_freshness_ms=60_000)
        self._same(*empty)
        self.assertNotEqual(empty[0].status.value, "OK")
        envelope = _envelope(self.binding, sequence=1, generation=2)
        self.spool.rows.append(_hydration_stored(self.binding, envelope, offset=10))
        ok = await self._both(max_freshness_ms=60_000)
        self._same(*ok)
        self.assertEqual(ok[0].status.value, "OK")
        from unittest.mock import patch

        with patch("qdl.runtime.execution_mark_index.time.time_ns", return_value=NOW_NS + 5_000_000_000):
            strict = await self._both(max_freshness_ms=2_000)
        self._same(*strict)
        self.assertNotEqual(strict[0].status.value, "OK")

    async def test_the_direct_client_still_checks_the_signature_and_schema(self):
        from qdl.reference.local_mark_index import InProcessViewClient

        client = InProcessViewClient(gateway=SimpleNamespace(assert_active=lambda epoch=None: 1),
                                     view=self.view, secret=b"k" * 32)
        unsigned = await client.post("http://localhost/x", content=b"{}", headers={})
        self.assertEqual((unsigned.status_code, unsigned.json()["detail"]),
                         (401, "invalid stable read signature"))
        from qdl.runtime.internal_auth import stable_hmac_signature

        bad = await client.post("http://localhost/x", content=b'{"schema":"x"}',
                                headers={"X-QDL-Stable-Signature": stable_hmac_signature(b"k" * 32, b'{"schema":"x"}')})
        self.assertEqual(bad.status_code, 400)


if __name__ == "__main__":
    unittest.main()

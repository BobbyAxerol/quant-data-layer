"""IDR-084 MARK hot lookups are shared per binding (P18.4 D3 stage 100, HOT_READER_BUSY)."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace

from qdl.query import FeedType
from qdl.runtime.kn_hot_view import HotViewGap, HotViewUnavailable
from qdl.runtime.kn_query_backend import MARK_HOT_MEMO_NS, KnMarketCacheQueryBackend


class _Hot:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def latest(self, binding, key, **_kwargs):
        self.calls += 1
        outcome = self.outcomes[min(self.calls, len(self.outcomes)) - 1]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _backend(hot, clock):
    backend = KnMarketCacheQueryBackend.__new__(KnMarketCacheQueryBackend)
    backend.hot_client = hot
    backend.product_key = lambda binding: binding.binding_id
    backend._monotonic_ns = lambda: clock[0]
    backend._mark_hot_memo = {}
    backend._mark_hot_locks = {}
    backend._mark_hot_locks_guard = threading.Lock()
    return backend


MARK = SimpleNamespace(binding_id="mark-a", feed=FeedType.MARK_INDEX_PRICE)
MARK_B = SimpleNamespace(binding_id="mark-b", feed=FeedType.MARK_INDEX_PRICE)
QUOTE = SimpleNamespace(binding_id="quote-a", feed=FeedType.QUOTE)


class MarkHotMemoTests(unittest.TestCase):
    def test_mark_reads_inside_the_window_share_one_lookup(self):
        clock = [1_000]
        hot = _Hot(["v1", "v2"])
        backend = _backend(hot, clock)
        self.assertEqual(backend._hot_latest(MARK), "v1")
        clock[0] += MARK_HOT_MEMO_NS - 1
        self.assertEqual(backend._hot_latest(MARK), "v1")
        self.assertEqual(hot.calls, 1)
        clock[0] += 1
        self.assertEqual(backend._hot_latest(MARK), "v2")
        self.assertEqual(hot.calls, 2)

    def test_bindings_do_not_share(self):
        clock = [0]
        hot = _Hot(["a", "b"])
        backend = _backend(hot, clock)
        self.assertEqual(backend._hot_latest(MARK), "a")
        self.assertEqual(backend._hot_latest(MARK_B), "b")
        self.assertEqual(hot.calls, 2)

    def test_other_feeds_always_ask(self):
        clock = [0]
        hot = _Hot(["q1", "q2"])
        backend = _backend(hot, clock)
        self.assertEqual(backend._hot_latest(QUOTE), "q1")
        self.assertEqual(backend._hot_latest(QUOTE), "q2")
        self.assertEqual(hot.calls, 2)

    def test_a_refusal_is_shared_and_raised_fresh(self):
        clock = [0]
        hot = _Hot([HotViewUnavailable("HOT_READER_BUSY")])
        backend = _backend(hot, clock)
        errors = []
        for _ in range(2):
            with self.assertRaises(HotViewUnavailable) as caught:
                backend._hot_latest(MARK)
            errors.append(caught.exception)
        self.assertEqual(hot.calls, 1)
        self.assertEqual([str(e) for e in errors], ["HOT_READER_BUSY", "HOT_READER_BUSY"])
        self.assertIsNot(errors[0], errors[1])

    def test_a_gap_keeps_its_type(self):
        clock = [0]
        backend = _backend(_Hot([HotViewGap("HOT_RECORD_GAPPED")]), clock)
        for _ in range(2):
            with self.assertRaises(HotViewGap):
                backend._hot_latest(MARK)

    def test_concurrent_readers_make_one_lookup(self):
        clock = [0]
        gate = threading.Event()

        class SlowHot(_Hot):
            def latest(self, binding, key, **kwargs):
                gate.wait(1)
                return super().latest(binding, key, **kwargs)

        hot = SlowHot(["v"])
        backend = _backend(hot, clock)
        results = []
        threads = [threading.Thread(target=lambda: results.append(backend._hot_latest(MARK))) for _ in range(8)]
        for thread in threads:
            thread.start()
        gate.set()
        for thread in threads:
            thread.join(2)
        self.assertEqual(results, ["v"] * 8)
        self.assertEqual(hot.calls, 1)


class BackupPathNeverSharesTests(unittest.TestCase):
    """rc4 regression (2026-10-10 04:30Z): the _latest_view backup path must ask the hot reader every time."""

    def _candidate_backend(self, hot, clock):
        backend = _backend(hot, clock)
        record = SimpleNamespace(source_offset=5, canonical=b"x")
        view = SimpleNamespace(boundary=SimpleNamespace(topic_id="t", partition=0, offset=5), rows=(record,))
        hot.outcomes = [view] * 10
        backend._parsed = lambda binding, candidate: ("parsed",)
        backend._validate_records = lambda binding, records: None
        backend._hot_monotonic = lambda binding, candidate, record=False: True
        return backend

    def test_backup_path_asks_every_time(self):
        clock = [0]
        hot = _Hot([])
        backend = self._candidate_backend(hot, clock)
        backend._hot_candidate(MARK)
        backend._hot_candidate(MARK)
        self.assertEqual(hot.calls, 2)

    def test_opportunistic_path_shares(self):
        clock = [0]
        hot = _Hot([])
        backend = self._candidate_backend(hot, clock)
        backend._hot_candidate(MARK, shared=True)
        backend._hot_candidate(MARK, shared=True)
        self.assertEqual(hot.calls, 1)


class RealOrderingChecksTests(unittest.TestCase):
    """The shared/direct split through the REAL ordering gates (nothing mocked inside the backend).

    Real canonical MARK/INDEX golden records (retimed with their Rust lineage
    kept whole, as in tests/test_kn_query_backend.py) in ProductView and
    CanonicalHotView objects; the cache reader is a Redis-free stand-in that
    returns one ProductView, as in tests/test_kn_hot_view.py
    HotQuerySelectionTests. ``_hot_candidate``'s boundary/offset comparisons,
    ``_hot_monotonic``/``_hot_boundaries``, ``_parsed``, ``_validate_records``
    and the quality oracle all run unmodified. Records, in canonical order:
    C10 (record 10, boundary 20) < H1 (30, 40) < H2 (50, 60) < H3 (70, 80)."""

    OFFSETS = {"C10": (10, 20), "H1": (30, 40), "H2": (50, 60), "H3": (70, 80)}

    @classmethod
    def setUpClass(cls):
        from pathlib import Path

        from qdl.marketdata.v2 import market_data_pb2
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from tests.test_kn_bar_legacy_import import golden_records
        from tests.test_kn_query_backend import binding_of

        cls.catalog = StableSourceCatalog.load(
            Path(__file__).resolve().parents[1] / "config/v2/stable-source-bindings.yaml")
        marks = (
            (binding_of(cls.catalog, pk, payload), payload)
            for pk, payload in golden_records(bar=False)
            if market_data_pb2.EventEnvelope.FromString(payload).WhichOneof("payload") == "mark_index_price"
        )
        cls.binding, cls.golden = next(
            (binding, payload) for binding, payload in marks
            if binding.authoritative and binding.source_role == "PRIMARY"
        )

    def setUp(self):
        import time as _time

        from qdl.marketdata.v2 import market_data_pb2
        from qdl.query import ConsumerGrade, DataRequirement
        from qdl.runtime.kn_bar_readback import binding_product_key
        from tests.test_kn_query_backend import retimed_mark_index

        self.now = _time.time_ns()
        self.mono = [10_000_000_000]
        self.lpk = binding_product_key(self.binding, "paper")

        def payload(age_ms):
            env = market_data_pb2.EventEnvelope.FromString(
                retimed_mark_index(self.golden, now_ns=self.now, age_ms=age_ms))
            env.normalized_at_ns = env.received_at_ns + 1
            env.published_at_ns = env.received_at_ns + 2
            return env.SerializeToString(deterministic=True)

        self.payloads = {"C10": payload(900), "H1": payload(400), "H2": payload(200), "H3": payload(100)}
        self.primary = self.cache("C10")
        self.reply = self.hot("H1")
        self.calls = 0
        self.requirement = DataRequirement(
            instrument_uid=self.binding.instrument.instrument_uid, feed=self.binding.feed,
            consumer_grade=ConsumerGrade.EXECUTION, source_policy_id=self.binding.source_policy_id,
            max_freshness_ms=2000,
        )

    def cache(self, name):
        from qdl.runtime.kn_market_cache import CacheRow, ProductView, SourceBoundary

        record, mark = self.OFFSETS[name]
        return ProductView(self.lpk, 1, 1, (CacheRow(self.payloads[name], record),), SourceBoundary("topic", 0, mark))

    def hot(self, name):
        from qdl.runtime.kn_hot_view import CanonicalHotView
        from qdl.runtime.kn_market_cache import CacheRow, SourceBoundary

        record, mark = self.OFFSETS[name]
        return CanonicalHotView(self.lpk, (CacheRow(self.payloads[name], record),), SourceBoundary("topic", 0, mark))

    def backend(self, *, rc4=False):
        from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend

        def primary(key):
            if isinstance(self.primary, Exception):
                raise self.primary
            return self.primary

        def latest(binding, key, **_kwargs):
            self.calls += 1
            if isinstance(self.reply, Exception):
                raise self.reply
            return self.reply

        backend = KnMarketCacheQueryBackend(
            SimpleNamespace(environment="paper", latest=primary), self.catalog, schema_digest="a" * 64,
            topic_id="topic", clock_ns=lambda: self.now, monotonic_ns=lambda: self.mono[0],
            hot_client=SimpleNamespace(latest=latest),
        )
        if rc4:
            # rc4 (before ec89a31): every _hot_candidate caller read through the shared memo.
            real = backend._hot_candidate
            backend._hot_candidate = lambda binding, primary=None, *, shared=False: real(binding, primary, shared=True)
        return backend

    def name_of(self, payload):
        return next(name for name, value in self.payloads.items() if value == payload)

    def served(self, backend):
        """Which record the Query latest path (_latest_view) served, by its cursor boundary."""
        from qdl.runtime.kn_query_backend import parse_placeholder

        mark = parse_placeholder(backend.latest(self.requirement).cursor)[2]
        return next(name for name, (_record, boundary) in self.OFFSETS.items() if boundary == mark)

    def recorded(self, backend):
        return backend._hot_boundaries[self.binding.binding_id][1]

    def fill_memo_with_h1(self, backend):
        # IDR-084: cache C10 is older than hot H1 -> H1 served, memo holds H1, H1 recorded.
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H1", 1))
        self.assertEqual(self.recorded(backend), 30)

    # (c) IDR-084 preserved ------------------------------------------------

    def test_c_newer_hot_record_inside_the_window_is_used_by_the_idr084_branch(self):
        backend = self.backend()
        # The valid, usable cache view C10 is replaced by the newer committed H1 (one lookup).
        self.assertEqual(self.served(backend), "C10")  # the Query path alone keeps a usable cache view
        self.fill_memo_with_h1(backend)
        # The cache catches up to exactly H1; inside the window the shared H1 passes the real
        # gates (equal offset, equal bytes) and is not newer, so the cache view stays.
        self.primary = self.cache("H1")
        self.reply = self.hot("H2")
        self.mono[0] += MARK_HOT_MEMO_NS - 1
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H1", 1))
        # Window over: a fresh lookup on the same IDR-084 branch, and the newer H2 wins.
        self.mono[0] += 1
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H2", 2))
        self.assertEqual(self.recorded(backend), 50)
        # A cache view behind the recorded record never reaches the shared branch: inside the
        # window it takes the direct backup (a fresh lookup) and gets the newest H3.
        self.primary = self.cache("C10")
        self.reply = self.hot("H3")
        self.mono[0] += 1
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H3", 3))

    # (a) primary advances past the shared memo -----------------------------

    def test_a_primary_advances_past_the_shared_memo(self):
        backend = self.backend()
        self.fill_memo_with_h1(backend)
        # The cache catches up to H2 while the memo (10 ms old) still holds H1; hot is now at H3.
        self.primary = self.cache("H2")
        self.reply = self.hot("H3")
        self.mono[0] += 10_000_000
        # The real offset comparison refuses the shared H1 against primary H2 ...
        with self.assertRaisesRegex(HotViewUnavailable, "HOT_BACKUP_BEHIND_PRIMARY"):
            backend._hot_candidate(self.binding, self.primary, shared=True)
        # ... so the IDR-084 branch keeps the cache view: never H1, never a refusal.
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload)), ("OK", "H2"))
        self.assertEqual(self.calls, 1, "memo hit, no hot lookup")
        self.assertEqual(self.recorded(backend), 50)
        # MARK-view backup (prefer_hot) inside the window: a fresh lookup, H3 accepted.
        stored, state = backend.latest_stored_event(self.binding, prefer_hot=True)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H3", 2))
        # _latest_view backup: primary H2 is now behind the recorded H3 -> fresh lookup, H3 accepted.
        self.assertEqual(self.served(backend), "H3")
        self.assertEqual(self.calls, 3)
        self.assertEqual(self.recorded(backend), 70)
        # The memo was never refreshed by the backup reads and still holds H1 inside its window.
        taken_ns, memo = backend._mark_hot_memo[self.binding.binding_id]
        self.assertLess(self.mono[0] - taken_ns, MARK_HOT_MEMO_NS)
        self.assertEqual(memo.rows[0].canonical, self.payloads["H1"])

    # (b) the rc4 failure ---------------------------------------------------

    def _cache_outage_sequence(self, backend):
        """Memo H1; the cache serves H2 (recorded); then a cache read fails while hot is at H3."""
        from qdl.runtime.kn_market_cache import KnCacheError

        self.fill_memo_with_h1(backend)
        self.primary = self.cache("H2")
        self.mono[0] += 10_000_000
        self.assertEqual(self.served(backend), "H2")  # usable and monotonic: no hot lookup
        self.assertEqual(self.recorded(backend), 50)
        self.primary = KnCacheError("cache read timed out")
        self.reply = self.hot("H3")
        self.mono[0] += 10_000_000
        return self.served(backend)

    def test_b_rc4_shared_backup_refuses_on_a_cache_outage(self):
        from qdl.query.contracts import CanonicalErrorCode
        from qdl.query.results import QueryBackendError

        backend = self.backend(rc4=True)
        with self.assertRaises(QueryBackendError) as refused:
            self._cache_outage_sequence(backend)
        self.assertIs(refused.exception.problem.code, CanonicalErrorCode.DEPENDENCY_UNAVAILABLE)
        self.assertEqual(self.calls, 1, "the rc4 backup never asked the hot reader")
        # Why: the real monotonic gate refuses the stale shared H1 behind the recorded H2.
        self.assertFalse(backend._hot_monotonic(self.binding, self.hot("H1")))
        with self.assertRaisesRegex(HotViewUnavailable, "HOT_BACKUP_BEHIND_RETURNED_VIEW"):
            backend._hot_candidate(self.binding, None, shared=True)

    def test_b_rc5_direct_backup_serves_the_fresh_record_on_a_cache_outage(self):
        backend = self.backend()
        self.assertEqual(self._cache_outage_sequence(backend), "H3")
        self.assertEqual(self.calls, 2)
        self.assertEqual(self.recorded(backend), 70)

    def test_b_stale_record_on_the_backup_path_is_refused_primary_behind(self):
        """The backup called with a stale record (the memo forced onto it) vs a fresh lookup."""
        from qdl.query.contracts import CanonicalErrorCode
        from qdl.query.results import QueryBackendError

        rc4, rc5 = self.backend(rc4=True), self.backend()
        for backend in (rc4, rc5):
            self.calls = 0
            self.reply = self.hot("H1")
            self.fill_memo_with_h1(backend)
            # Any read records H2 (here the cache served it), then the cache view is C10 again
            # (another replica's older generation); hot is at H2; the memo window is still open.
            backend._hot_monotonic(self.binding, self.cache("H2"), record=True)
            self.reply = self.hot("H2")
            self.mono[0] += 10_000_000
        with self.assertRaises(QueryBackendError) as refused:
            self.served(rc4)
        self.assertIs(refused.exception.problem.code, CanonicalErrorCode.DATA_NOT_READY)
        self.assertIn("HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND: HOT_BACKUP_BEHIND_RETURNED_VIEW",
                      str(refused.exception))
        self.assertEqual(rc4.latest_stored_event(self.binding, prefer_hot=True), (None, "BACKUP_UNAVAILABLE"))
        self.assertEqual(self.served(rc5), "H2")
        stored, state = rc5.latest_stored_event(self.binding, prefer_hot=True)
        self.assertEqual((state, self.name_of(stored.event.payload)), ("OK", "H2"))

    # (d) Astra R1: a hard hot refusal is never downgraded to the older cache view -------------

    def view_read(self, backend):
        """The execution MARK view over this backend (the TS at-use path), at the fixture clock."""
        import asyncio

        from qdl.reference.local_mark_index import CacheRefreshingMarkIndexView

        view = CacheRefreshingMarkIndexView.from_catalog(self.catalog).attach_cache(
            backend=backend, relax_to_alpha=False)
        return asyncio.run(view.read(
            instrument_uid=self.binding.instrument.instrument_uid,
            instrument_revision=self.binding.instrument.metadata_revision,
            source_policy_id=self.binding.source_policy_id, max_freshness_ms=2000, gateway_epoch=1,
            now_ns=self.now))

    def hot_reply(self, name, *, topic="topic", partition=0, payload=None):
        from dataclasses import replace

        hot = self.hot(name)
        hot = replace(hot, boundary=replace(hot.boundary, topic_id=topic, partition=partition))
        if payload is not None:
            hot = replace(hot, rows=(replace(hot.rows[0], canonical=payload),))
        return hot

    def test_d_hard_refusals_are_not_downgraded_to_the_cache_view(self):
        """Before the fix every row below except the payload conflict returned (C10, OK)."""
        cases = {
            "gap": (HotViewGap("HOT_RECORD_GAPPED"), "INTEGRITY"),
            "other partition": (lambda: self.hot_reply("H1", partition=1), "FENCED"),
            # Unreachable through CanonicalHotClient (its reply check refuses another topic as the transient
            # HOT_REPLY_INVALID, see test_d_real_client_*), still a hard refusal at the backend gate.
            "other topic": (lambda: self.hot_reply("H1", topic="different-topic"), "FENCED"),
            "same offset, other bytes": (lambda: self.hot_reply("C10", payload=self.payloads["H1"]), "INTEGRITY"),
            "lineage invalid": (lambda: self.hot_reply("H1", payload=b"\x0a\x01x"), "INTEGRITY"),
        }
        for name, (reply, state) in cases.items():
            with self.subTest(name):
                self.setUp()
                self.reply = reply() if callable(reply) else reply
                backend = self.backend()
                self.assertEqual(backend.latest_stored_event(self.binding), (None, state))
                self.assertEqual(self.calls, 1)
                self.assertNotIn(self.binding.binding_id, backend._hot_boundaries, "nothing recorded")
                # The TS at-use path: refused with the layer's state, no second (prefer_hot) attempt that could
                # heal it, and inside the memo window the shared refusal still holds.
                result = self.view_read(backend)
                self.assertIsNone(result.record)
                self.assertEqual(result.reason, f"MARKET_CACHE_{state}")
                self.assertEqual(self.calls, 1)

    def test_d_transient_outcomes_keep_the_valid_cache_view(self):
        for name in ("HOT_READER_BUSY", "HOT_READ_CAPACITY", "HOT_READ_DEADLINE", "HOT_READER_CATCHING_UP",
                     "HOT_BROKER_UNCONFIRMED", "HOT_RECORD_MISSING", "HOT_REPLY_INVALID",
                     "HOT_AUTHORITY_OR_PROTOCOL_REFUSED"):
            with self.subTest(name):
                self.setUp()
                self.reply = HotViewUnavailable(name)
                backend = self.backend()
                stored, state = backend.latest_stored_event(self.binding)
                self.assertEqual((state, self.name_of(stored.event.payload)), ("OK", "C10"))
                self.assertEqual(self.recorded(backend), 10)
                result = self.view_read(backend)
                self.assertIsNotNone(result.record, result.reason)
                self.assertEqual(result.record.canonical, self.payloads["C10"])
        # A hot record older than the cache view is not newer: the cache view stands (HOT_BACKUP_BEHIND_PRIMARY).
        self.setUp()
        self.primary, self.reply = self.cache("H1"), self.hot("C10")
        stored, state = self.backend().latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload)), ("OK", "H1"))

    def test_d_recovery_after_a_gap_refusal(self):
        backend = self.backend()
        self.reply = HotViewGap("HOT_RECORD_GAPPED")
        self.assertEqual(backend.latest_stored_event(self.binding), (None, "INTEGRITY"))
        # Inside the window the shared gap still refuses, even though the cache view did not change.
        self.reply = self.hot("H2")
        self.mono[0] += MARK_HOT_MEMO_NS - 1
        self.assertEqual(backend.latest_stored_event(self.binding), (None, "INTEGRITY"))
        self.assertEqual(self.calls, 1)
        # Window over: the newer clean committed record is served through every gate.
        self.mono[0] += 1
        stored, state = backend.latest_stored_event(self.binding)
        self.assertEqual((state, self.name_of(stored.event.payload), self.calls), ("OK", "H2", 2))
        self.assertEqual(self.recorded(backend), 50)
        # The backup path (prefer_hot) keeps its existing hard mapping for the same refusal.
        self.reply = HotViewGap("HOT_RECORD_GAPPED")
        self.assertEqual(backend.latest_stored_event(self.binding, prefer_hot=True), (None, "INTEGRITY"))

    def test_d_real_client_refuses_another_topic_as_a_transient_reply_error(self):
        """Through CanonicalHotClient a reply for another canonical topic never reaches the backend gate: the
        client's identity check (kn_hot_view.py _decode; expectation source_topic_id is Query's own topic,
        stable.py hot_client_from_environment) turns it into HOT_REPLY_INVALID. A reply that cannot be attributed
        to this Query's authority says nothing about the cache view, which stands. The same reply for the right
        topic is taken (newer)."""
        import base64
        import json

        from google.protobuf.wrappers_pb2 import BytesValue

        from qdl.runtime.kn_hot_view import SCHEMA, CanonicalHotClient

        def client(topic):
            body = dict(schema=SCHEMA, binding_id=self.binding.binding_id, product_key=self.lpk.encode(),
                        physical_key=self.binding.partition_key, source_topic_id=topic, source_partition=0,
                        source_offset=40, record_offset=30,
                        canonical=base64.b64encode(self.payloads["H1"]).decode())
            return CanonicalHotClient(calls=(lambda *a, **k: BytesValue(value=json.dumps(body).encode()),),
                                      expectation={"source_topic_id": "topic"}, secret=b"x" * 32)

        for topic, expected in (("different-topic", "C10"), ("topic", "H1")):
            with self.subTest(topic):
                self.setUp()
                backend = self.backend()
                backend.hot_client = client(topic)
                stored, state = backend.latest_stored_event(self.binding)
                self.assertEqual((state, self.name_of(stored.event.payload)), ("OK", expected))


MARK_BINDING = SimpleNamespace(binding_id="mark-a", feed=FeedType.MARK_INDEX_PRICE, interval=None)


class MarkHotBudgetTests(unittest.TestCase):
    """Astra R3: one budget bounds the wait for a binding's lookup slot plus the lookup itself."""

    def test_a_waiter_behind_a_slow_leader_is_refused_within_its_budget_and_not_memoised(self):
        import time as _time

        clock = [0]
        release = threading.Event()
        entered = threading.Event()

        class SlowHot(_Hot):
            timeout_seconds = 0.05

            def latest(self, binding, key, **kwargs):
                entered.set()
                release.wait(2)
                return super().latest(binding, key, **kwargs)

        hot = SlowHot(["v1", "v2"])
        backend = _backend(hot, clock)
        leader_result = []
        leader = threading.Thread(target=lambda: leader_result.append(backend._hot_latest(MARK)))
        leader.start()
        self.assertTrue(entered.wait(2))
        started = _time.monotonic()
        with self.assertRaisesRegex(HotViewUnavailable, "HOT_READ_DEADLINE"):
            backend._hot_latest(MARK)
        waited = _time.monotonic() - started
        self.assertGreaterEqual(waited, 0.04)
        self.assertLess(waited, 1.0, "bounded by the 50 ms budget, not by the leader")
        release.set()
        leader.join(2)
        self.assertEqual(leader_result, ["v1"])
        self.assertEqual(hot.calls, 1)
        # The waiter's own deadline was not memoised: the next reader inside the window gets the leader's value.
        self.assertEqual(backend._hot_latest(MARK), "v1")
        self.assertEqual(hot.calls, 1)

    def test_the_lookup_gets_only_the_remaining_budget(self):
        clock = [0]
        seen = []

        class BudgetHot(_Hot):
            timeout_seconds = 0.2

            def latest(self, binding, key, **kwargs):
                seen.append(kwargs.get("timeout_seconds"))
                return super().latest(binding, key, **kwargs)

        backend = _backend(BudgetHot(["v"]), clock)
        self.assertEqual(backend._hot_latest(MARK), "v")
        self.assertEqual(len(seen), 1)
        self.assertGreater(seen[0], 0)
        self.assertLessEqual(seen[0], 0.2)

    def test_an_unavailable_outcome_expires_and_recovers(self):
        clock = [0]
        hot = _Hot([HotViewUnavailable("HOT_READER_BUSY"), "v"])
        backend = _backend(hot, clock)
        with self.assertRaises(HotViewUnavailable):
            backend._hot_latest(MARK)
        clock[0] += MARK_HOT_MEMO_NS
        self.assertEqual(backend._hot_latest(MARK), "v")
        self.assertEqual(hot.calls, 2)

    def test_other_feeds_keep_the_direct_call(self):
        clock = [0]
        seen = []

        class Direct(_Hot):
            def latest(self, binding, key, **kwargs):
                seen.append(kwargs)
                return super().latest(binding, key)

        backend = _backend(Direct(["q"]), clock)
        self.assertEqual(backend._hot_latest(QUOTE), "q")
        self.assertEqual(seen, [{}])


class HotClientBudgetTests(unittest.TestCase):
    """CanonicalHotClient.latest(timeout_seconds=...) can only shorten its configured deadline."""

    def client(self, call, timeout=0.25):
        from qdl.runtime.kn_hot_view import CanonicalHotClient

        return CanonicalHotClient(calls=(call,), expectation={}, secret=b"x" * 32, timeout_seconds=timeout)

    def test_an_exhausted_budget_never_calls_the_reader(self):
        def call(*args, **kwargs):
            raise AssertionError("no RPC without budget")

        client = self.client(call)
        self.assertEqual(client.timeout_seconds, 0.25)
        for remaining in (0, -0.1):
            with self.assertRaisesRegex(HotViewUnavailable, "HOT_READ_DEADLINE"):
                client.latest(MARK_BINDING, "k", timeout_seconds=remaining)
        self.assertTrue(client._slots.acquire(False))
        client._slots.release()

    def test_the_rpc_timeout_is_the_smaller_budget(self):
        import grpc

        timeouts = []

        class Busy(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.RESOURCE_EXHAUSTED

            def details(self):
                return "HOT_READER_BUSY"

        def call(body, *, timeout, metadata, wait_for_ready):
            timeouts.append(timeout)
            raise Busy()

        client = self.client(call)
        for budget in (0.05, 5.0, None):
            with self.assertRaisesRegex(HotViewUnavailable, "HOT_READER_BUSY"):
                client.latest(MARK_BINDING, "k", timeout_seconds=budget)
        self.assertLessEqual(timeouts[0], 0.05)
        self.assertLessEqual(timeouts[1], 0.25)
        self.assertGreater(timeouts[1], 0.05)
        self.assertLessEqual(timeouts[2], 0.25)
        self.assertGreater(timeouts[2], 0.05)


if __name__ == "__main__":
    unittest.main()

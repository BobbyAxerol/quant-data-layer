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

    def latest(self, binding, key):
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
            def latest(self, binding, key):
                gate.wait(1)
                return super().latest(binding, key)

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


if __name__ == "__main__":
    unittest.main()

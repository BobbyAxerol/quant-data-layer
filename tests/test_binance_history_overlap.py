"""Test-only synthetic OHLCV; provider-observed 3d transition timestamps."""
from dataclasses import dataclass, replace
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from qdl.adapters.binance.bar_edge import fetch_closed_bar_history_raw_envelopes
from qdl.adapters.intervals import BarHistoryGapError, BarHistoryOverlapError
from qdl.query import ConsumerGrade, CoverageStatus, DataRequirement, FeedType, HistoryResult
from qdl.query.contracts import CanonicalErrorCode, evaluate_requirement
from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
import tests.test_kn_history_fill as fixtures

DURATION = 259_200_000
START = 1_692_144_000_000


def row(open_ms):
    value = fixtures.ShortProviderHistoryTests._row(open_ms)
    value[6] = open_ms + DURATION - 1
    return value


def binding():
    return replace(fixtures.ShortProviderHistoryTests()._binding(),
                   native_symbol="ETHUSDT", interval="3d")


def provider(rows):
    def fetch(_symbol, *, end_time, limit, **kwargs):
        return {"data": [value for value in rows if value[0] <= end_time][-limit:]}
    return Mock(side_effect=fetch)


def overlapping_rows():
    return [row(START - 86_400_000 * 2)] + [row(START + i * DURATION) for i in range(3)]


def fetch(fetcher, **kwargs):
    return fetch_closed_bar_history_raw_envelopes(
        binding(), now_ms=START + 3 * DURATION + 1, attempts=1,
        fetcher=fetcher, test_provenance=True, **kwargs,
    )


class SuffixOutcomeTests(unittest.TestCase):
    def test_same_page_suffix_raises_and_preserves_exact_native_rows(self):
        rows = overlapping_rows()
        call = provider(rows)
        with self.assertRaises(BarHistoryOverlapError) as caught:
            fetch(call, limit=10000, allow_short=True)
        error = caught.exception
        self.assertEqual(error.requested_rows, 10000)
        self.assertEqual(error.older_prefix_status, "UNCERTIFIED_PROVIDER_OVERLAP")
        self.assertEqual([json.loads(v.raw_frame_bytes)["row"] for v in error.recent_envelopes], rows[1:])
        self.assertEqual(call.call_count, 1, "do not fetch an older page or probe exhaustion after overlap")
        self.assertEqual(len(fetch(provider(rows), limit=3)), 3)

    def test_cross_page_overlap_retains_all_verified_newer_rows(self):
        call = provider(overlapping_rows())
        with patch("qdl.adapters.binance.bar_edge._HISTORY_PAGE_ROWS", 3):
            with self.assertRaises(BarHistoryOverlapError) as caught:
                fetch(call, limit=10000, allow_short=True)
        self.assertEqual(len(caught.exception.recent_envelopes), 3)
        self.assertEqual(call.call_count, 2)

    def test_only_newest_contiguous_segment_can_be_materialized(self):
        rows = [row(START - 86_400_000 * 4), row(START - 86_400_000 * 2)] + overlapping_rows()[1:]
        with self.assertRaises(BarHistoryOverlapError) as caught:
            fetch(provider(rows), limit=5)
        self.assertEqual(caught.exception.previous, START - 86_400_000 * 2)
        self.assertEqual(len(caught.exception.recent_envelopes), 3)

    def test_off_grid_stale_and_missing_suffix_never_offer_materialization(self):
        for rows in (
            [row(value[0] - 1) for value in overlapping_rows()],
            overlapping_rows()[:-1],
            [row(START - DURATION * 2), row(START), row(START + 2 * DURATION)],
        ):
            with self.subTest(opens=[r[0] for r in rows]):
                with self.assertRaises(BarHistoryGapError) as caught:
                    fetch(provider(rows), limit=len(rows))
                self.assertNotIsInstance(caught.exception, BarHistoryOverlapError)

    def test_invalid_close_rejected_before_any_suffix_is_offered(self):
        rows = overlapping_rows()
        rows[-1][6] -= 1
        with self.assertRaises(ValueError):
            fetch(provider(rows), limit=4)


class SuffixMaterializationTests(unittest.TestCase):
    def edge(self):
        fixture = fixtures.EdgeFillTests()
        edge = fixture._edge(demand=None, gate=None)
        source = edge.history_bindings[0][0]
        source.interval = "3d"
        acquisition = SimpleNamespace(runtime="BINANCE")
        edge.history_bindings = ((source, acquisition),)
        edge._history_short = {"b": (1, 0), "c": (1, 0)}
        edge._settled_observed_ms = lambda: START + 3 * DURATION + 1
        edge._assert_repair_writer_current = lambda: None
        edge._assert_canonical_cache_identity = lambda: None
        edge._persist_state = Mock()
        edge._record_serving = Mock()
        edge._publish_history = StableBinanceBarEdge._publish_history.__get__(edge)
        durable = {START + 2 * DURATION}
        edge._durable_final_bar_opens = lambda _source, opens: frozenset(durable & opens)
        emitted = []

        def publish(values):
            values = tuple(values)
            emitted.extend(values)
            durable.update(json.loads(value.raw_frame_bytes)["row"][0] for value in values)
            return tuple(object() for _ in values)

        edge.publisher = SimpleNamespace(publish_many=Mock(side_effect=publish))
        call = provider(overlapping_rows())
        edge._fetch_history = lambda _source, _acquisition, rows, **kwargs: fetch(
            call, limit=rows, allow_short=True)
        return edge, call, durable, emitted

    def test_bootstrap_materializes_recent_three_but_never_completes_deep_history(self):
        edge, call, durable, emitted = self.edge()
        self.assertEqual(edge.bootstrap_history(), 2)
        self.assertEqual(durable, {START + i * DURATION for i in range(3)})
        self.assertEqual(len(emitted), 2, "existing newest bar is not republished")
        self.assertTrue(edge._live_history_ready("a"))
        self.assertEqual(edge._last_open_ms["a"], START + 2 * DURATION)
        self.assertFalse(edge._history_bootstrapped)
        self.assertNotIn("a", edge._history_short)
        self.assertEqual(edge._history_suspended["a"]["status"], "UNCERTIFIED_PROVIDER_OVERLAP")
        for now in (31, 3601, 86400):
            edge.clock = lambda: now
            edge.bootstrap_history()
        self.assertEqual(call.call_count, 1)
        self.assertEqual(edge.publisher.publish_many.call_count, 1)
        self.assertEqual(edge._history_loop_delay(86400), 1)
        edge.clock = lambda: 86401
        self.assertEqual(edge.bootstrap_history(), 0)
        self.assertEqual(call.call_count, 2)
        self.assertFalse(edge._history_bootstrapped)
        edge.canonical_cache_id = "test-cache"
        edge.connection_generation = 1
        edge._state_identity_payload = lambda **kwargs: {"schema": kwargs["schema"]}
        self.assertEqual(edge._state_payload()["last_open_ms"], {}, "restart cannot certify suffix as full history")

    def test_next_final_bar_advances_while_deep_history_remains_suspended(self):
        edge, call, durable, emitted = self.edge()
        edge.bootstrap_history()
        edge.bindings = edge.history_bindings
        edge.okx_bindings = ()
        edge._rest_fallback_active = True
        edge.max_catchup_rows = 10000
        edge._binding_is_due = lambda *args, **kwargs: True
        latest = fetch_closed_bar_history_raw_envelopes(
            binding(), now_ms=START + 4 * DURATION + 1, limit=1, attempts=1,
            fetcher=provider([row(START + 3 * DURATION)]), test_provenance=True)[0]
        edge._fetch_latest = Mock(return_value=latest)
        edge._settled_observed_ms = lambda: START + 4 * DURATION + 1
        self.assertEqual(edge.run_cycle(), 1)
        self.assertEqual(edge._last_open_ms["a"], START + 3 * DURATION)
        self.assertIn(START + 3 * DURATION, durable)
        self.assertFalse(edge._history_bootstrapped)
        self.assertIn("a", edge._history_retry)
        self.assertIn("a", edge._history_suspended)
        self.assertEqual(call.call_count, 1)

    def test_empty_recheck_cannot_certify_previously_rejected_history(self):
        edge, call, durable, emitted = self.edge()
        edge.bootstrap_history()
        edge.clock = lambda: 86401
        edge._fetch_history = lambda *args, **kwargs: ()
        self.assertEqual(edge.bootstrap_history(), 0)
        self.assertFalse(edge._history_bootstrapped)
        self.assertIn("a", edge._history_suspended)
        self.assertIn("a", edge._history_retry)
        self.assertNotIn("a", edge._history_short)

    def test_sink_failure_reuses_pending_provider_bytes_without_refetch(self):
        edge, call, durable, emitted = self.edge()
        publish = edge.publisher.publish_many.side_effect
        edge.publisher.publish_many.side_effect = OSError("test-only sink failure")
        with self.assertRaises(OSError):
            edge.bootstrap_history()
        self.assertNotIn("a", edge._last_open_ms)
        self.assertFalse(edge._history_bootstrapped)
        self.assertEqual(call.call_count, 1)
        edge.clock = lambda: 31
        edge.publisher.publish_many.side_effect = publish
        self.assertEqual(edge.bootstrap_history(), 2)
        self.assertEqual(call.call_count, 1)
        self.assertEqual(len(durable), 3)
        self.assertFalse(edge._history_suffix_pending)

    def test_verified_provider_correction_can_complete_without_rewinding_live(self):
        edge, call, durable, emitted = self.edge()
        edge.bootstrap_history()
        edge.clock = lambda: 86401
        edge._last_open_ms["a"] = START + 3 * DURATION
        edge._fetch_history = lambda *args, **kwargs: fetch(provider(overlapping_rows()), limit=3)
        edge.bootstrap_history()
        self.assertTrue(edge._history_bootstrapped)
        self.assertFalse(edge._history_suspended)
        self.assertFalse(edge._history_retry)
        self.assertEqual(edge._last_open_ms["a"], START + 3 * DURATION)


@dataclass(frozen=True)
class Item:
    snapshot_id: str = "test-only"


class SuffixQueryCoverageTests(unittest.TestCase):
    def result(self, requested, *, venue="BINANCE", interval="3d", start_ns=None):
        backend = KnMarketCacheQueryBackend.__new__(KnMarketCacheQueryBackend)
        source = SimpleNamespace(feed=FeedType.BAR, interval=interval,
                                 instrument=SimpleNamespace(identity=SimpleNamespace(venue=venue)))
        backend.catalog = SimpleNamespace(binding_for=lambda _: source)
        backend._requested_window = lambda _: (requested, start_ns, 4, None)
        records = tuple(SimpleNamespace(envelope=SimpleNamespace(bar=SimpleNamespace(open_time_ns=i))) for i in (1, 2, 3))
        view = SimpleNamespace(boundary=SimpleNamespace(offset=3))
        backend._history_view = lambda *args: (view, records)
        backend._history_from_records = lambda *args, **kwargs: HistoryResult(
            items=tuple(Item() for _ in records), coverage=CoverageStatus.FULL,
            snapshot_id="test-only", stream_cursor="test-only", watermark_offset=3, data_as_of_ns=3)
        with patch("qdl.runtime.kn_query_backend.view_snapshot_id", return_value="test-only"), patch(
                "qdl.runtime.kn_query_backend.source_placeholder", return_value="test-only"):
            return backend.history_with_envelopes(None)[0]

    def test_exact_recent_request_full_but_uncertified_older_prefix_is_partial(self):
        self.assertEqual(self.result(3).coverage, CoverageStatus.FULL)
        result = self.result(10000)
        self.assertEqual(result.coverage, CoverageStatus.PARTIAL)
        requirement = DataRequirement(instrument_uid="test-only", feed=FeedType.BAR,
                                      interval="3d", consumer_grade=ConsumerGrade.ALPHA,
                                      source_policy_id="test-only", warmup_limit=10000)
        problem = evaluate_requirement(requirement, coverage=result.coverage, entitled=True,
                                       available=True, fresh=True, authoritative=True, gap_open=False)
        self.assertEqual(problem.code, CanonicalErrorCode.PARTIAL_RESULT)
        self.assertEqual(self.result(10000, venue="OKX").coverage, CoverageStatus.FULL)
        self.assertEqual(self.result(10000, interval="1m").coverage, CoverageStatus.FULL)
        self.assertEqual(self.result(10000, start_ns=1).coverage, CoverageStatus.FULL)

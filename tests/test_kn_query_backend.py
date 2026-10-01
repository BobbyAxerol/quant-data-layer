"""KN-4 K4.1 (D25-D28): Query backend and cursor v3 issuer over the market cache.

The cache is populated in the exact layout of ``rust/qdl-projector/src/cache.rs``
+ ``apply.lua`` (``ptr``, ``l``, ``bm``, ``b``, ``src`` and the ``ckpt``
source-watermark fields of KN-4 slice 1) with values encoded by the shared
state codec from real canonical records (the codec golden) and synthetic
derivations marked ``k36-test``. The reads use the backend's real Lua
scripts, so every case needs ``QDL_KN_TEST_REDIS`` = URL of a disposable
Redis; each case writes only keys under a unique ``kn3:k4q<run>:`` prefix and
deletes exactly those keys. Without it the class is skipped loudly (the
kn-native integration job sets it). A cross-check against a cache written by
the Rust stage B itself is the shadow flow's (K4-T01/T03), not this file's.
"""
from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from dataclasses import replace
from pathlib import Path

from qdl.adapters.intervals import canonical_interval_ms
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.kn_state_codec import encode_bar_row, encode_latest_value, state_partition
from qdl.projection.state_contract import MAX_OFFSET
from qdl.query import ConsumerGrade, DataRequirement, FeedType
from qdl.common.v1 import common_pb2
from qdl.query.contracts import BarRevisionPolicy, CanonicalErrorCode
from qdl.query.results import QueryBackendError
from qdl.replay.cursor_v3 import CursorV3Expectation, SignedCursorV3Codec, requirement_digest
from qdl.runtime.kn_bar_readback import BUCKET_OPENS, binding_product_key
from qdl.runtime.kn_market_cache import KnCacheViewChanged, KnMarketCacheReader
from qdl.runtime.kn_query_backend import (
    KnCursorSettings,
    KnCursorV3Issuer,
    KnMarketCacheQueryBackend,
    parse_placeholder,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import StableSpoolQueryBackend
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from tests.test_kn_bar_legacy_import import derived_bar, golden_records, open_ms_of

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
EPOCH = 3
TOPIC_ID = "ljfjPYApRpWQd79McfTtZg"
DIGEST = "a" * 64
KEYS = {"k4-test": bytes(range(32))}


def requirement(binding, rows: int = 0) -> DataRequirement:
    return DataRequirement(
        instrument_uid=binding.instrument.instrument_uid,
        feed=binding.feed,
        interval=binding.interval,
        consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id=binding.source_policy_id,
        warmup_limit=rows,
    )


def binding_of(catalog, partition_key: str, canonical: bytes):
    payload = market_data_pb2.EventEnvelope.FromString(canonical).WhichOneof("payload")
    return next(
        item for item in catalog.bindings
        if item.partition_key == partition_key and item.feed.value.lower() == payload
    )


def retimed_mark_index(payload: bytes, *, now_ns: int, age_ms: int, **changes) -> bytes:
    """A real MARK/INDEX pair moved in time with its Rust lineage kept whole:
    both components confirmed at ``now - age + 1 ms``, source values at
    ``now - age`` (captures unchanged, so the capture digest still holds)."""

    envelope = market_data_pb2.EventEnvelope.FromString(payload)
    fields = envelope.source_sequence.split(":")
    source_ms = (now_ns - age_ms * 1_000_000) // 1_000_000
    received_ns = now_ns - age_ms * 1_000_000 + 1_000_000
    fields[0:4] = [str(source_ms), str(source_ms), str(received_ns), str(received_ns)]
    envelope.source_sequence = ":".join(fields)
    envelope.source_event_time_ns = source_ms * 1_000_000
    envelope.received_at_ns = received_ns
    for name, value in changes.items():
        setattr(envelope, name, value)
    return envelope.SerializeToString(deterministic=True)


@unittest.skipUnless(os.environ.get("QDL_KN_TEST_REDIS"),
                     "needs QDL_KN_TEST_REDIS = URL of a disposable Redis (kn-native integration job)")
class KnQueryBackendRedisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.bar_pk, cls.real_bar = next(
            (pk, payload) for pk, payload in golden_records(bar=True)
            if pk.endswith("okx-swap-doge-usdt-swap-bar-1h-primary-v2")
        )
        cls.bar_binding = binding_of(cls.catalog, cls.bar_pk, cls.real_bar)
        cls.latest_records = [
            (binding_of(cls.catalog, pk, payload), payload)
            for pk, payload in golden_records(bar=False)
            if market_data_pb2.EventEnvelope.FromString(payload).WhichOneof("payload")
            in {"trade", "quote", "mark_index_price"}
        ]

    def setUp(self) -> None:
        import redis

        self.client = redis.Redis.from_url(os.environ["QDL_KN_TEST_REDIS"])
        self.environment = f"k4q{uuid.uuid4().hex[:8]}"
        self.prefix = f"kn3:{self.environment}:"
        self.reader = KnMarketCacheReader(self.client, self.environment)
        interval_ms = canonical_interval_ms(self.bar_binding.interval)
        self.interval_ms = interval_ms
        self.base = open_ms_of(self.real_bar)
        self.now_ns = (self.base + interval_ms) * 1_000_000 + 5_000_000_000

    def tearDown(self) -> None:
        keys = list(self.client.scan_iter(match=f"{self.prefix}*", count=1000))
        if keys:
            self.client.delete(*keys)
        self.client.close()

    # ------------------------------------------------------------ fixtures

    def lpk(self, binding):
        return binding_product_key(binding, self.environment)

    def backend(self, **kwargs) -> KnMarketCacheQueryBackend:
        return KnMarketCacheQueryBackend(
            self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID,
            clock_ns=lambda: self.now_ns, **kwargs,
        )

    def put_bars(self, binding, payloads, *, generation=7, fence=2, offsets=None, source=(TOPIC_ID, 3),
                 mark=5_000, floor=None):
        lpk = self.lpk(binding)
        interval_ms = canonical_interval_ms(binding.interval)
        self.client.hset(f"{self.prefix}ptr:{lpk.encode()}", mapping={"ready": generation, "fence": fence})
        opens = []
        for index, payload in enumerate(payloads):
            open_ms = open_ms_of(payload)
            opens.append(open_ms)
            offset = MAX_OFFSET if offsets is None else offsets[index]
            key = f"{self.prefix}b:{generation}:{lpk.encode()}:{open_ms // (BUCKET_OPENS * interval_ms)}"
            self.client.hset(key, str(open_ms), encode_bar_row(payload, lpk, offset, EPOCH))
        meta = {"first": min(opens), "last": max(opens), "rows": len(opens)}
        if floor is not None:
            meta["floor"] = floor
        self.client.hset(f"{self.prefix}bm:{generation}:{lpk.encode()}", mapping=meta)
        if source is not None:
            self.client.hset(f"{self.prefix}src:{lpk.encode()}", mapping={"t": source[0], "p": source[1]})
            q = state_partition(lpk, 6)
            self.client.hset(f"{self.prefix}ckpt:md.bars.v2:{q}", f"s|{source[0]}|{source[1]}", mark)
        return lpk

    def put_latest(self, binding, payload, *, generation=4, offset=50, partition=1, mark=None,
                   topic_id=TOPIC_ID):
        lpk = self.lpk(binding)
        self.client.hset(f"{self.prefix}ptr:{lpk.encode()}", mapping={"ready": generation, "fence": 1})
        self.client.hset(f"{self.prefix}l:{generation}:{lpk.encode()}", mapping={
            "v": encode_latest_value(payload, offset, EPOCH), "t": topic_id, "p": partition, "o": offset,
        })
        if mark is not None:
            q = state_partition(lpk, 6)
            self.client.hset(f"{self.prefix}ckpt:md.latest.v2:{q}", f"s|{topic_id}|{partition}", mark)
        return lpk

    def history_rows(self, count: int) -> list[bytes]:
        return [self.real_bar, *(derived_bar(self.real_bar, -shift) for shift in range(1, count))]

    def spool_backend(self, payloads) -> StableSpoolQueryBackend:
        directory = tempfile.TemporaryDirectory(prefix="k4q-spool-")
        self.addCleanup(directory.cleanup)
        spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(directory.name) / "spool.sqlite3", max_records=50_000, max_payload_bytes=200_000_000,
            max_event_bytes=64_000, max_storage_bytes=400_000_000, min_free_disk_bytes=0,
            max_partition_records=20_000))
        self.addCleanup(spool.close)
        events = [
            DurableEvent(stream=self.bar_binding.canonical_stream, partition_key=self.bar_binding.partition_key,
                         event_id=bytes(market_data_pb2.EventEnvelope.FromString(payload).event_id),
                         payload=payload, accepted_at_ns=1_000 + index)
            for index, payload in enumerate(sorted(payloads, key=open_ms_of))
        ]
        spool.append_many(events)
        return StableSpoolQueryBackend(spool, self.catalog, schema_digest=DIGEST, clock_ns=lambda: self.now_ns)

    def put_diagnostic_index(self, binding, payloads, generation=7):
        lpk = self.lpk(binding)
        step = canonical_interval_ms(binding.interval)
        for payload in payloads:
            opened = open_ms_of(payload)
            envelope = market_data_pb2.EventEnvelope.FromString(payload)
            flag = ("G" + envelope.source_sequence
                    if common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE in envelope.quality_flags else "N")
            key = f"{self.prefix}bd:{generation}:{lpk.encode()}:{opened // (BUCKET_OPENS * step)}"
            self.client.hset(key, str(opened), flag)

    def summarize_buckets(self, binding, payloads, generation=7):
        source = (ROOT / "rust/qdl-projector/src/apply.lua").read_text()
        function = source.split("-- BEGIN diagnostic_summary", 1)[1].split("-- END diagnostic_summary", 1)[0]
        function = function[function.index("local function diagnostic_summary"):]
        lpk = self.lpk(binding)
        step = canonical_interval_ms(binding.interval)
        for bucket in {open_ms_of(p) // (BUCKET_OPENS * step) for p in payloads}:
            suffix = f"{generation}:{lpk.encode()}:{bucket}"
            self.client.eval(function + "\nreturn diagnostic_summary(KEYS[1],KEYS[2],KEYS[3],ARGV[1])", 3,
                self.prefix + "b:" + suffix, self.prefix + "bd:" + suffix,
                self.prefix + "bs:" + suffix, binding.interval)

    def test_coverage_reports_missing_generation_retained_window_and_exclusions(self):
        backend = self.backend()
        report = backend.open_gaps_bounded()
        self.assertEqual(len(report.unavailable), len(self.catalog.bindings))
        self.assertEqual(tuple(report), ())
        self.assertFalse(report.coverage_document()["materialization_complete"])
        rows = self.history_rows(4)
        self.put_bars(self.bar_binding, rows)
        self.put_diagnostic_index(self.bar_binding, rows)
        report = backend.open_gaps_bounded()
        item = next(c for c in report.coverage if c["binding_id"] == self.bar_binding.binding_id)
        self.assertEqual(item["state"], "SCANNED")
        self.assertEqual(item["retained_rows"], 4)
        self.assertEqual(item["first_open_ns"], min(open_ms_of(r) for r in rows)*1_000_000)
        self.assertEqual(report.coverage_document()["trailing_coverage"], "NOT_ASSESSED")
        self.assertIsNone(report.coverage_document()["history_complete"])
        excluded = self.backend(diagnostic_exclusions={self.bar_binding.binding_id: "ACQUISITION_DISABLED"})
        item = next(c for c in excluded.open_gaps_bounded().coverage
                    if c["binding_id"] == self.bar_binding.binding_id)
        self.assertEqual(item["state"], "EXCLUDED")
        with self.assertRaisesRegex(ValueError, "catalog bindings"):
            self.backend(diagnostic_exclusions={"not-a-binding": "disabled"})

    def test_materialized_summary_matches_oracle_floor_and_missing_index(self):
        rows = self.history_rows(350)
        rows.pop(140)
        env = market_data_pb2.EventEnvelope.FromString(rows[20])
        env.quality_flags.append(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE)
        env.source_sequence = "summary-sequence"
        rows[20] = env.SerializeToString()
        lpk = self.put_bars(self.bar_binding, rows)
        self.put_diagnostic_index(self.bar_binding, rows)
        expected = self.backend().open_gaps()
        self.summarize_buckets(self.bar_binding, rows)
        self.assertEqual(self.backend().open_gaps(), expected)
        for limit in (1, 112, 240, 350):
            _, entries = self.reader.bar_diagnostics(lpk, self.interval_ms, last=limit, check_budget=lambda: None)
            self.assertEqual([o for o, _ in entries], sorted(open_ms_of(p) for p in rows)[-limit:])
        floor = open_ms_of(rows[142])
        self.client.hset(f"{self.prefix}bm:7:{lpk.encode()}", "floor", floor)
        _, entries = self.reader.bar_diagnostics(lpk, self.interval_ms, last=350, check_budget=lambda: None)
        self.assertEqual([o for o, _ in entries], sorted(open_ms_of(p) for p in rows if open_ms_of(p) >= floor))
        opened = max(open_ms_of(p) for p in rows)
        key = f"{self.prefix}bd:7:{lpk.encode()}:{opened // (BUCKET_OPENS * self.interval_ms)}"
        self.client.hdel(key, str(opened))
        from qdl.runtime.kn_market_cache import KnCacheIntegrityError
        with self.assertRaises(KnCacheIntegrityError):
            self.reader.bar_diagnostics(lpk, self.interval_ms, last=350, check_budget=lambda: None)

    def test_indexed_diagnostic_matches_verified_scan_without_decoding(self):
        from unittest.mock import patch
        rows = self.history_rows(300)
        rows.pop(110)
        self.put_bars(self.bar_binding, rows)
        backend = self.backend()
        expected = backend.open_gaps()
        self.assertEqual(len(expected), 1)
        self.put_diagnostic_index(self.bar_binding, rows)
        with patch("qdl.runtime.kn_market_cache.decode_bar_row", side_effect=AssertionError("decoded BAR")):
            self.assertEqual(backend.open_gaps(), expected)

    def test_diagnostic_ranges_preserve_trimming_and_cross_bucket_gaps(self):
        rows = self.history_rows(350)
        rows.pop(140)
        rows.pop(141)
        lpk = self.put_bars(self.bar_binding, rows)
        self.put_diagnostic_index(self.bar_binding, rows)
        for limit in (1, 17, 112, 240, 350):
            boundary, entries = self.reader.bar_diagnostics(lpk, self.interval_ms,
                last=limit, check_budget=lambda: None)
            self.assertEqual([o for o, _ in entries], sorted(open_ms_of(x) for x in rows)[-limit:])
        opened = open_ms_of(rows[-1])
        key = f"{self.prefix}bd:7:{lpk.encode()}:{opened // (BUCKET_OPENS * self.interval_ms)}"
        self.client.hset(key, str(opened), "INVALID")
        from qdl.runtime.kn_market_cache import KnCacheIntegrityError
        with self.assertRaises(KnCacheIntegrityError):
            self.reader.bar_diagnostics(lpk, self.interval_ms, last=350, check_budget=lambda: None)

    def test_indexed_diagnostic_sequence_revision_floor_and_legacy_fallback(self):
        rows = self.history_rows(5)
        env = market_data_pb2.EventEnvelope.FromString(rows[1])
        env.quality_flags.append(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE)
        env.source_sequence = "gap-123"
        rows[1] = env.SerializeToString()
        lpk = self.put_bars(self.bar_binding, rows)
        expected = self.backend().open_gaps()
        self.put_diagnostic_index(self.bar_binding, rows)
        self.assertEqual(self.backend().open_gaps(), expected)
        self.assertEqual(len(expected), 1)
        # A partial index (including count-preserving wrong opens) never hides gaps.
        opened = open_ms_of(rows[1])
        key = f"{self.prefix}bd:7:{lpk.encode()}:{opened // (BUCKET_OPENS * self.interval_ms)}"
        self.client.hdel(key, str(opened))
        self.client.hset(key, str(opened + 1), "N")
        self.assertEqual(self.backend().open_gaps(), expected)
        self.client.hdel(key, str(opened + 1))
        self.put_diagnostic_index(self.bar_binding, rows)
        self.client.hset(f"{self.prefix}bm:7:{lpk.encode()}", "floor", self.base)
        self.assertEqual(self.backend().open_gaps(), ())

    def test_indexed_diagnostic_generation_and_deadline_fail_closed(self):
        from unittest.mock import patch
        from qdl.runtime.kn_market_cache import KnCacheViewChanged
        rows = self.history_rows(200)
        lpk = self.put_bars(self.bar_binding, rows)
        self.put_diagnostic_index(self.bar_binding, rows)
        original = self.reader._bar_head
        count = 0
        def changing(*args):
            nonlocal count
            count += 1
            head = original(*args)
            return head[0], count, *head[2:]
        with patch.object(self.reader, "_bar_head", side_effect=changing):
            with self.assertRaises(KnCacheViewChanged):
                self.reader.bar_diagnostics(lpk, self.interval_ms, last=200, check_budget=lambda: None)
        def cancel():
            raise RuntimeError("cancel diagnostic")
        with self.assertRaisesRegex(RuntimeError, "cancel diagnostic"):
            self.reader.bar_diagnostics(lpk, self.interval_ms, last=200, check_budget=cancel)

    def test_bar_hash_valid_but_wrong_open_is_rejected(self):
        from qdl.runtime.kn_market_cache import KnCacheIntegrityError
        lpk = self.put_bars(self.bar_binding, [self.real_bar])
        bucket = self.base // (BUCKET_OPENS * self.interval_ms)
        key = self.reader.bucket_key(7, lpk, bucket)
        self.client.hset(key, str(self.base), encode_bar_row(
            derived_bar(self.real_bar, -1), lpk, MAX_OFFSET, EPOCH))
        with self.assertRaises(KnCacheIntegrityError):
            self.reader.bars(lpk, self.interval_ms, last=1)

    def test_diagnostic_deadline_is_checked_inside_bucket_decode(self):
        from unittest.mock import patch
        from qdl.runtime.stable_source import _GapDiagnosticIncomplete
        import qdl.runtime.kn_market_cache as cache
        lpk = self.put_bars(self.bar_binding, self.history_rows(200))
        checks = 0
        def budget():
            nonlocal checks
            checks += 1
            if checks == 5:
                raise _GapDiagnosticIncomplete("test deadline")
        with patch.object(cache, "decode_bar_row", wraps=cache.decode_bar_row) as decode:
            with self.assertRaises(_GapDiagnosticIncomplete):
                self.reader.bars(lpk, self.interval_ms, last=200, check_budget=budget)
            self.assertLessEqual(decode.call_count, 2)

    def test_cancelled_cold_bucket_read_stops_before_redis_io(self):
        import threading
        from unittest.mock import patch
        from qdl.query.cold_work import _run_cancellable, ColdWorkCancelled
        lpk = self.put_bars(self.bar_binding, [self.real_bar])
        cancelled = threading.Event()
        cancelled.set()
        with patch.object(self.client, "pipeline", wraps=self.client.pipeline) as pipeline:
            with self.assertRaises(ColdWorkCancelled):
                _run_cancellable(cancelled, True, self.reader.bars, lpk, self.interval_ms, last=1)
            pipeline.assert_not_called()

    # ------------------------------------------------------------ K4-T01/T03

    def test_bar_history_matches_the_spool_semantics_at_the_view_boundary(self):
        payloads = self.history_rows(130)  # crosses the 112-open bucket boundary
        self.put_bars(self.bar_binding, payloads, offsets=[1_000 + index for index in range(130)], mark=4_321)
        req = requirement(self.bar_binding, 100)
        cache = self.backend().history(req)
        spool = self.spool_backend(payloads).history(req)
        self.assertEqual(len(cache.items), 100)
        self.assertEqual(cache.coverage, spool.coverage)
        self.assertEqual(cache.data_as_of_ns, spool.data_as_of_ns)
        for ours, theirs in zip(cache.items, spool.items, strict=True):
            self.assertEqual(ours.payload, theirs.payload)
            self.assertEqual(ours.quality, theirs.quality)
            self.assertEqual((ours.source, ours.contract, ours.bar_lifecycle), (theirs.source, theirs.contract,
                                                                                 theirs.bar_lifecycle))
            self.assertEqual(ours.watermark_offset, 4_321, "every item carries the view boundary")
        self.assertEqual(cache.watermark_offset, 4_321)
        self.assertEqual(parse_placeholder(cache.stream_cursor), (TOPIC_ID, 3, 4_321))
        self.assertTrue(cache.snapshot_id.startswith("qdl-v2-"))
        # Latest (snapshot) of the same product: the newest open, same boundary.
        item = self.backend().latest(requirement(self.bar_binding))
        self.assertEqual(item.payload, spool.items[-1].payload)
        self.assertEqual(parse_placeholder(item.cursor), (TOPIC_ID, 3, 4_321))

    def test_binance_3d_suffix_does_not_certify_the_uncaptured_older_prefix(self):
        from qdl.query import CoverageStatus
        from qdl.query.contracts import evaluate_requirement

        pk, real_bar = next((pk, raw) for pk, raw in golden_records(bar=True)
                            if pk.endswith("binance-usdm-dogeusdt-bar-3d-primary-v2"))
        binding = binding_of(self.catalog, pk, real_bar)
        rows = [derived_bar(real_bar, -shift) for shift in range(3)]
        self.now_ns = (open_ms_of(real_bar) + canonical_interval_ms("3d")) * 1_000_000 + 5_000_000_000
        self.put_bars(binding, rows)
        backend = self.backend()
        recent = backend.history(requirement(binding, 3))
        self.assertEqual(len(recent.items), 3)
        self.assertEqual(recent.coverage, CoverageStatus.FULL)
        full_request = requirement(binding, 10000)
        incomplete = backend.history(full_request)
        self.assertEqual(len(incomplete.items), 3)
        self.assertEqual(incomplete.coverage, CoverageStatus.PARTIAL)
        problem = evaluate_requirement(full_request, coverage=incomplete.coverage,
                                       entitled=True, available=True, fresh=True,
                                       authoritative=True, gap_open=False)
        self.assertEqual(problem.code, CanonicalErrorCode.PARTIAL_RESULT)

    def test_a_time_range_reads_exactly_its_opens(self):
        payloads = self.history_rows(300)
        self.put_bars(self.bar_binding, payloads, offsets=[10 + index for index in range(300)], mark=400)
        view = self.reader.bars(self.lpk(self.bar_binding), self.interval_ms,
                                start_ms=self.base - 120 * self.interval_ms,
                                end_ms=self.base - 20 * self.interval_ms)
        opens = [row.open_ms for row in view.rows]
        self.assertEqual(opens, [self.base - shift * self.interval_ms for shift in range(120, 20, -1)])

    def test_last_rows_walk_below_missing_opens_and_never_below_the_floor(self):
        payloads = [self.real_bar, *(derived_bar(self.real_bar, -shift) for shift in range(400, 700))]
        floor = self.base - 650 * self.interval_ms
        kept = [payload for payload in payloads if open_ms_of(payload) >= floor]
        self.put_bars(self.bar_binding, kept, offsets=[5] * len(kept), mark=9, floor=floor)
        # A stray row below the floor (never served) in the floor's bucket.
        lpk = self.lpk(self.bar_binding)
        stray = derived_bar(self.real_bar, -651)
        self.client.hset(
            f"{self.prefix}b:7:{lpk.encode()}:{open_ms_of(stray) // (BUCKET_OPENS * self.interval_ms)}",
            str(open_ms_of(stray)), encode_bar_row(stray, lpk, 5, EPOCH),
        )
        view = self.reader.bars(lpk, self.interval_ms, last=200)
        self.assertEqual(len(view.rows), 200)
        self.assertEqual(view.rows[-1].open_ms, self.base)
        self.assertEqual(view.rows[-2].open_ms, self.base - 400 * self.interval_ms, "walked past the hole")
        everything = self.reader.bars(lpk, self.interval_ms, last=10_000)
        self.assertEqual(len(everything.rows), len(kept))
        self.assertEqual(min(row.open_ms for row in everything.rows), floor, "nothing below the floor")

    def test_legacy_rows_have_no_canonical_offset_and_the_boundary_is_the_watermark(self):
        self.put_bars(self.bar_binding, self.history_rows(5), mark=77)
        view = self.reader.bars(self.lpk(self.bar_binding), self.interval_ms, last=5)
        self.assertEqual({row.source_offset for row in view.rows}, {None})
        self.assertEqual(view.boundary.offset, 77)
        self.assertFalse(view.changed)

    # ------------------------------------------------------------ latest

    def test_latest_boundary_is_the_max_of_its_offset_and_the_watermark(self):
        binding, payload = self.latest_records[0]
        self.put_latest(binding, payload, offset=50, mark=40)
        self.assertEqual(self.reader.latest(self.lpk(binding)).boundary.offset, 50)
        self.put_latest(binding, payload, offset=50, mark=70)
        self.assertEqual(self.reader.latest(self.lpk(binding)).boundary.offset, 70)
        item = self.backend().latest(requirement(binding))
        self.assertEqual(item.watermark_offset, 70)
        self.assertEqual(parse_placeholder(item.cursor), (TOPIC_ID, 1, 70))
        history = self.backend().history(requirement(binding, 1000))
        self.assertEqual(len(history.items), 1, "a latest-state product keeps one record")
        self.assertEqual(history.watermark_offset, 70)

    def test_every_latest_feed_of_the_golden_projects_like_the_spool(self):
        for binding, payload in self.latest_records:
            with self.subTest(binding=binding.binding_id):
                self.put_latest(binding, payload, offset=0, mark=None)
                item = self.backend().latest(requirement(binding))
                self.assertIsNotNone(item)
                self.assertEqual(item.feed, binding.feed)
                self.assertEqual(item.watermark_offset, 0, "offset zero is a valid boundary")

    # ------------------------------------------------------------ typed outcomes

    def test_not_ready_outcomes_are_typed_and_never_empty(self):
        binding, payload = self.latest_records[0]
        backend = self.backend()
        self.assertIsNone(backend.latest(requirement(binding)), "no pointer: DATA_NOT_READY")
        self.assertIsNone(backend.history(requirement(self.bar_binding, 10)))
        # Legacy rows only: no canonical coordinate yet.
        self.put_bars(self.bar_binding, self.history_rows(3), source=None)
        with self.assertRaises(QueryBackendError) as caught:
            backend.history(requirement(self.bar_binding, 3))
        self.assertEqual(caught.exception.problem.code, CanonicalErrorCode.DATA_NOT_READY)
        self.assertIn("SOURCE_BOUNDARY_UNKNOWN", caught.exception.problem.detail)
        self.assertTrue(caught.exception.problem.retryable)
        # Another canonical topic generation is never signed.
        self.put_latest(binding, payload, topic_id="otherTopicIdAAAAAAAAAA", mark=3)
        with self.assertRaises(QueryBackendError) as caught:
            backend.latest(requirement(binding))
        self.assertIn("SOURCE_TOPIC_GENERATION", caught.exception.problem.detail)

    def test_a_corrupt_row_fails_closed(self):
        lpk = self.put_bars(self.bar_binding, self.history_rows(3), offsets=[1, 2, 3], mark=3)
        key = f"{self.prefix}b:7:{lpk.encode()}:{self.base // (BUCKET_OPENS * self.interval_ms)}"
        row = bytearray(self.client.hget(key, str(self.base)))
        row[-1] ^= 0xFF
        self.client.hset(key, str(self.base), bytes(row))
        with self.assertRaises(QueryBackendError) as caught:
            self.backend().history(requirement(self.bar_binding, 3))
        self.assertEqual(caught.exception.problem.code, CanonicalErrorCode.INTERNAL_ERROR)
        self.assertFalse(caught.exception.problem.retryable)

    def test_a_generation_change_during_the_read_is_retried_then_refused(self):
        lpk = self.put_bars(self.bar_binding, self.history_rows(10), offsets=list(range(10)), mark=20)
        pointer = f"{self.prefix}ptr:{lpk.encode()}"
        original = self.reader._fetch

        def swap_once(*args, **kwargs):
            original(*args, **kwargs)
            if not getattr(swap_once, "done", False):
                swap_once.done = True
                self.client.hincrby(pointer, "fence", 1)

        self.reader._fetch = swap_once
        view = self.reader.bars(lpk, self.interval_ms, last=10)
        self.assertEqual(view.fence, 3, "second attempt at the new fence")

        def swap_always(*args, **kwargs):
            original(*args, **kwargs)
            self.client.hincrby(pointer, "fence", 1)

        self.reader._fetch = swap_always
        with self.assertRaises(KnCacheViewChanged):
            self.reader.bars(lpk, self.interval_ms, last=10)
        with self.assertRaises(QueryBackendError) as caught:
            self.backend().history(requirement(self.bar_binding, 10))
        self.assertEqual(caught.exception.problem.code, CanonicalErrorCode.DEPENDENCY_UNAVAILABLE)
        self.assertTrue(caught.exception.problem.retryable)

    def test_a_row_above_the_watermark_marks_the_view_changed(self):
        # The product changed after the head read: its re-delivery is a duplicate.
        lpk = self.put_bars(self.bar_binding, self.history_rows(4), offsets=[1, 2, 3, 99], mark=50)
        view = self.reader.bars(lpk, self.interval_ms, last=4)
        self.assertTrue(view.changed)
        self.assertEqual(view.boundary.offset, 50)

    def test_reads_never_write(self):
        lpk = self.put_bars(self.bar_binding, self.history_rows(130), offsets=list(range(130)), mark=200)
        binding, payload = self.latest_records[0]
        self.put_latest(binding, payload, mark=60)
        keys = sorted(self.client.scan_iter(match=f"{self.prefix}*", count=1000))
        before = {key: self.client.dump(key) for key in keys}
        backend = self.backend()
        backend.history(requirement(self.bar_binding, 100))
        backend.latest(requirement(binding))
        backend.history_many((requirement(self.bar_binding, 5), requirement(binding)))
        backend.open_gaps()
        after = {key: self.client.dump(key) for key in sorted(self.client.scan_iter(match=f"{self.prefix}*"))}
        self.assertEqual(after, before)
        self.assertEqual(lpk.feed, "BAR")

    # ------------------------------------------------------------ batch / gaps

    def test_history_many_keeps_per_item_outcomes(self):
        self.put_bars(self.bar_binding, self.history_rows(20), offsets=list(range(20)), mark=30)
        binding, _payload = self.latest_records[0]
        results = self.backend().history_many((requirement(self.bar_binding, 20), requirement(binding)))
        self.assertEqual(len(results[requirement(self.bar_binding, 20)].items), 20)
        self.assertIsNone(results[requirement(binding)], "absent product: DATA_NOT_READY per item")

    def test_gap_diagnostic_reports_a_missing_open(self):
        payloads = [payload for index, payload in enumerate(self.history_rows(50)) if index != 7]
        self.put_bars(self.bar_binding, payloads, offsets=list(range(49)), mark=60)
        gaps = self.backend().open_gaps()
        missing = [gap for gap in gaps if gap.observed_sequence == "MISSING"]
        self.assertEqual([gap.expected_sequence for gap in missing],
                         [str((self.base - 7 * self.interval_ms) * 1_000_000)])

    # ------------------------------------------------------------ issuer (D28)

    def test_the_issuer_signs_a_cursor_the_stream_expectation_accepts(self):
        self.put_bars(self.bar_binding, self.history_rows(10), offsets=list(range(10)), mark=12_345)
        settings = KnCursorSettings(KEYS, "k4-test", "paper", TOPIC_ID, 1, "kn4-route-a", 600)
        issuer = KnCursorV3Issuer(settings, self.catalog, clock_ns=lambda: 1_000)
        req = requirement(self.bar_binding, 10)
        history = issuer.bind_history(req, self.backend().history(req), consumer_id="alpha.binance.paper.stable")
        self.assertEqual({item.cursor for item in history.items}, {history.stream_cursor})
        codec = SignedCursorV3Codec(KEYS, active_key_id="k4-test")
        claims = codec.verify(
            history.stream_cursor, consumer_id="alpha.binance.paper.stable", environment="paper",
            requirement_digest_value=requirement_digest(req),
            expected=CursorV3Expectation(
                environment="paper", stream=self.catalog.canonical_stream, source_topic_id=TOPIC_ID,
                partition_plan_epoch=1, source_policy_revision=self.catalog.source_policy_revision,
                catalog_revision=self.catalog.catalog_revision, route_generation="kn4-route-a",
            ),
            now_ns=2_000,
        )
        self.assertEqual((claims.source_partition, claims.source_offset), (3, 12_345))
        self.assertEqual(claims.product_key, binding_product_key(self.bar_binding, "paper").encode())
        self.assertEqual(claims.snapshot_id, history.snapshot_id)
        self.assertEqual(claims.expires_at_ns - claims.issued_at_ns, 600 * 1_000_000_000)
        # Nothing but a backend coordinate is ever signed.
        with self.assertRaises(ValueError):
            issuer.bind_history(req, replace(history, stream_cursor="CONSUMER_CURSOR_PENDING"),
                                consumer_id="alpha.binance.paper.stable")
        with self.assertRaises(ValueError):
            issuer.bind_history(req, replace(history, stream_cursor="kn3-source:otherTopic:3:1"),
                                consumer_id="alpha.binance.paper.stable")

    # ------------------------------------------------------------ HTTP (router + issuer)

    def test_the_http_warmup_and_snapshot_carry_a_signed_v3_cursor_and_no_placeholder(self):
        import time as _time

        from fastapi.testclient import TestClient

        from qdl.api_v2 import create_v2_app
        from qdl.consumer import ConsumerManifestLoader
        from qdl.runtime.stable_source import build_stable_query_stack
        from tests.phase7_support import make_identity, make_token, manifest_mapping

        interval_ms = self.interval_ms
        closed_open = (_time.time_ns() // 1_000_000 // interval_ms - 1) * interval_ms
        anchor = (closed_open - self.base) // interval_ms
        payloads = [derived_bar(self.real_bar, anchor - shift) for shift in range(120)]
        self.put_bars(self.bar_binding, payloads, offsets=list(range(120)), mark=777)
        settings = KnCursorSettings(KEYS, "k4-test", "paper", TOPIC_ID, 1, "kn4-route-a", 600)
        backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID)
        service, _backend, issuer = build_stable_query_stack(
            spool=None, catalog=self.catalog, schema_digest=DIGEST, handoff=None, cursor_ttl_seconds=600,
            backend=backend, issuer=KnCursorV3Issuer(settings, self.catalog),
        )
        consumer_id, subject = "k4-http", "spiffe://qdl/paper/k4-http"
        manifest = ConsumerManifestLoader.from_mapping(manifest_mapping(
            consumer_id=consumer_id, subject=subject,
            instrument_uid=self.bar_binding.instrument.instrument_uid, feed="BAR",
            interval=self.bar_binding.interval, source_policy_id=self.bar_binding.source_policy_id,
        ))
        client = TestClient(
            create_v2_app(service, identity_service=make_identity(manifest), cursor_issuer=issuer),
            raise_server_exceptions=False,
        )
        client.headers.update({"Authorization": f"Bearer {make_token(subject)}",
                               "X-QDL-Consumer-ID": consumer_id, "X-QDL-Purpose": "INTERNAL_ALPHA"})
        params = {"feed": "BAR", "interval": self.bar_binding.interval,
                  "source_policy_id": self.bar_binding.source_policy_id, "limit": 100,
                  "bar_revision_policy": "EMIT_REVISIONS"}
        uid = self.bar_binding.instrument.instrument_uid
        warmup = client.get(f"/v2/market-data/{uid}/warmup", params=params)
        self.assertEqual(warmup.status_code, 200, warmup.text)
        self.assertNotIn("kn3-source", warmup.text)
        body = warmup.json()
        self.assertEqual(body["watermark_offset"], 777)
        self.assertEqual(len(body["data"]), 100)
        codec = SignedCursorV3Codec(KEYS, active_key_id="k4-test")
        expected = CursorV3Expectation(
            environment="paper", stream=self.catalog.canonical_stream, source_topic_id=TOPIC_ID,
            partition_plan_epoch=1, source_policy_revision=self.catalog.source_policy_revision,
            catalog_revision=self.catalog.catalog_revision, route_generation="kn4-route-a",
        )
        requirement_value = DataRequirement(
            instrument_uid=uid, feed=FeedType.BAR, interval=self.bar_binding.interval,
            consumer_grade=ConsumerGrade.ALPHA, source_policy_id=self.bar_binding.source_policy_id,
            max_freshness_ms=None, bar_revision_policy=BarRevisionPolicy.EMIT_REVISIONS,
        )
        claims = codec.verify(
            body["stream_cursor"], consumer_id=consumer_id, environment="paper",
            requirement_digest_value=requirement_digest(requirement_value), expected=expected,
            now_ns=_time.time_ns(),
        )
        self.assertEqual((claims.source_partition, claims.source_offset), (3, 777))
        snapshot = client.get(f"/v2/market-data/{uid}/snapshot", params={
            key: value for key, value in params.items() if key != "limit"})
        self.assertEqual(snapshot.status_code, 200, snapshot.text)
        self.assertNotIn("kn3-source", snapshot.text)
        item = snapshot.json()["data"]
        self.assertEqual(item["watermark_offset"], 777)
        self.assertEqual(
            codec.verify(item["cursor"], consumer_id=consumer_id, environment="paper",
                         requirement_digest_value=requirement_digest(requirement_value), expected=expected,
                         now_ns=_time.time_ns()).source_offset,
            777,
        )

    # ------------------------------------------------------------ D30 row cache

    def test_row_cache_reuses_static_views_byte_identically_and_never_a_verdict(self):
        import importlib

        from qdl.query.service import WarmupResult

        router = importlib.import_module("qdl.api_v2.router")
        payloads = self.history_rows(40)
        self.put_bars(self.bar_binding, payloads, offsets=list(range(40)), mark=90)
        binding, latest_payload = self.latest_records[0]
        self.put_latest(binding, latest_payload, mark=12)
        clock = {"now": self.now_ns}
        backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST,
                                            topic_id=TOPIC_ID, clock_ns=lambda: clock["now"])
        req = requirement(self.bar_binding, 40)
        first = backend.history(req)
        self.assertEqual(backend.rows.stats()["misses"], 40, "one derivation per row")
        second = backend.history(req)
        self.assertEqual(backend.rows.stats()["misses"], 40, "the second read reuses every row")
        self.assertEqual(second, first)
        self.assertTrue(all(item.render_key for item in second.items))
        # A later read at a later time rebuilds quality: no cached verdict.
        clock["now"] += 3_600_000_000_000
        later = backend.history(req)
        self.assertNotEqual([item.quality for item in later.items], [item.quality for item in first.items])
        self.assertEqual([item.payload for item in later.items], [item.payload for item in first.items])

        def render(history) -> bytes:
            body = router._render_warmup_chunked(router._warmup(WarmupResult("req-1", history)))
            # D39: the per-chunk renderer the endpoint uses is byte-identical.
            self.assertEqual(router._render_warmup_result(WarmupResult("req-1", history)), body)
            return body

        def stripped(history):
            return replace(history, items=tuple(replace(item, render_key=None) for item in history.items))

        # Cached row derivations render exactly like fresh ones.
        self.assertEqual(render(later), render(stripped(later)))
        fresh = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID,
                                          clock_ns=lambda: clock["now"], row_cache_entries=0).history(req)
        self.assertEqual(render(later), render(fresh))
        for other_binding, payload in self.latest_records:
            self.put_latest(other_binding, payload, mark=5)
            item = backend.latest(requirement(other_binding))
            item = backend.latest(requirement(other_binding))  # the cached derivation
            self.assertEqual(
                router._market_item(item).model_dump(mode="json", by_alias=True),
                router._market_item(replace(item, render_key=None)).model_dump(mode="json", by_alias=True),
            )

    # ------------------------------------------------------------ D31 MARK/INDEX

    def test_execution_mark_index_comes_from_the_cache_with_execution_bounds(self):
        import asyncio
        import time as _time

        from qdl.reference.local_mark_index import CacheRefreshingMarkIndexView
        from qdl.runtime.session_liveness import StableSessionLivenessReader
        from qdl.runtime.stable_deployment import StableAcquisitionPlan

        binding, payload = next(
            (binding, payload) for binding, payload in self.latest_records
            if binding.feed is FeedType.MARK_INDEX_PRICE and binding.authoritative
            and binding.source_role == "PRIMARY"
        )
        acquisition = StableAcquisitionPlan.load(ROOT / "config/v2/stable-acquisition-bindings.yaml",
                                                 catalog=self.catalog)
        backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID)
        directory = tempfile.TemporaryDirectory(prefix="k4q-session-")
        self.addCleanup(directory.cleanup)

        def put(age_ms: int) -> None:
            self.put_latest(binding, retimed_mark_index(payload, now_ns=_time.time_ns(), age_ms=age_ms),
                            offset=age_ms, mark=age_ms)

        def read(view):
            return asyncio.run(view.read(
                instrument_uid=binding.instrument.instrument_uid,
                instrument_revision=binding.instrument.metadata_revision,
                source_policy_id=binding.source_policy_id, max_freshness_ms=300_000, gateway_epoch=1,
            ))

        execution = CacheRefreshingMarkIndexView.from_catalog(
            self.catalog, acquisition=acquisition,
            session_liveness_reader=StableSessionLivenessReader(directory.name),
        ).attach_cache(backend=backend, relax_to_alpha=False)
        alpha = CacheRefreshingMarkIndexView.from_catalog(self.catalog).attach_cache(backend=backend)
        put(200)
        fresh = read(execution)
        self.assertIsNotNone(fresh.record, fresh.reason)
        self.assertEqual(fresh.record.canonical, self.reader.latest(self.lpk(binding)).rows[0].canonical)
        put(5_000)
        stale = read(CacheRefreshingMarkIndexView.from_catalog(
            self.catalog, acquisition=acquisition,
            session_liveness_reader=StableSessionLivenessReader(directory.name),
        ).attach_cache(backend=backend, relax_to_alpha=False))
        self.assertIsNone(stale.record)
        self.assertEqual(stale.reason, "STALE", "the execution horizon is the binding's own")
        self.assertIsNotNone(read(alpha).record, "alpha keeps its relaxed horizon")

    def test_the_mark_index_view_never_answers_a_cache_failure_with_a_remembered_price(self):
        """D36: good read -> cache error / integrity / lost state / generation
        change / broken lineage -> read again."""
        import asyncio
        import time as _time

        from qdl.reference.local_mark_index import CacheRefreshingMarkIndexView
        from qdl.runtime.kn_market_cache import KnCacheError
        from qdl.runtime.session_liveness import StableSessionLivenessReader
        from qdl.runtime.stable_deployment import StableAcquisitionPlan

        binding, payload = next(
            (binding, payload) for binding, payload in self.latest_records
            if binding.feed is FeedType.MARK_INDEX_PRICE and binding.authoritative
            and binding.source_role == "PRIMARY"
        )
        lpk = self.lpk(binding)
        acquisition = StableAcquisitionPlan.load(ROOT / "config/v2/stable-acquisition-bindings.yaml",
                                                 catalog=self.catalog)
        directory = tempfile.TemporaryDirectory(prefix="k4q-session-")
        self.addCleanup(directory.cleanup)
        backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID)
        view = CacheRefreshingMarkIndexView.from_catalog(
            self.catalog, acquisition=acquisition,
            session_liveness_reader=StableSessionLivenessReader(directory.name),
        ).attach_cache(backend=backend, relax_to_alpha=False)

        def fresh(age_ms=100, **changes):
            return retimed_mark_index(payload, now_ns=_time.time_ns(), age_ms=age_ms, **changes)

        def read():
            return asyncio.run(view.read(
                instrument_uid=binding.instrument.instrument_uid,
                instrument_revision=binding.instrument.metadata_revision,
                source_policy_id=binding.source_policy_id, max_freshness_ms=300_000, gateway_epoch=1,
            ))

        good = fresh()
        self.put_latest(binding, good, generation=4, offset=10, mark=10)
        self.assertIsNotNone(read().record)
        # 1. The cache cannot answer: nothing remembered is served.
        original = self.reader.latest
        self.reader.latest = lambda lpk: (_ for _ in ()).throw(KnCacheError("down"))
        outcome = read()
        self.assertEqual((outcome.record, outcome.reason), (None, "MARKET_CACHE_UNAVAILABLE"))
        self.reader.latest = original
        self.assertIsNotNone(read().record, "recovers on the next good read")
        # 2. Integrity: a value that fails its trailer check.
        key = f"{self.prefix}l:4:{lpk.encode()}"
        value = bytearray(self.client.hget(key, "v"))
        value[-1] ^= 0xFF
        self.client.hset(key, "v", bytes(value))
        self.assertEqual(read().reason, "MARKET_CACHE_INTEGRITY")
        # 3. The product lost its state (no pointer).
        self.put_latest(binding, fresh(), generation=4, offset=11, mark=11)
        self.assertIsNotNone(read().record)
        self.client.delete(f"{self.prefix}ptr:{lpk.encode()}")
        self.assertEqual(read().reason, "MARKET_CACHE_NOT_READY")
        # 4. Another canonical topic generation is fenced, not served.
        self.put_latest(binding, fresh(), generation=5, offset=12, mark=12, topic_id="otherTopicIdAAAAAAAAAA")
        self.assertEqual(read().reason, "MARKET_CACHE_FENCED")
        # 5. A new READY generation (rebuild) serves its own record.
        newer = fresh(age_ms=50)
        self.put_latest(binding, newer, generation=6, offset=13, mark=13)
        served = read()
        self.assertEqual(served.record.canonical, newer)
        # 6. Broken lineage (another source id) never serves the old price.
        self.put_latest(binding, fresh(age_ms=10, source_id="not-the-binding-source"), generation=6,
                        offset=14, mark=14)
        self.assertEqual(read().reason, "LINEAGE_INVALID")
        # 7. D35: the right identity but pair lineage that does not hold (the
        # envelope confirmation is not the oldest component's) is refused too.
        self.put_latest(binding, fresh(age_ms=5), generation=6, offset=15, mark=15)
        self.assertIsNotNone(read().record)
        envelope = market_data_pb2.EventEnvelope.FromString(fresh(age_ms=4))
        envelope.received_at_ns += 1
        self.put_latest(binding, envelope.SerializeToString(deterministic=True), generation=6, offset=16, mark=16)
        self.assertEqual(read().reason, "LINEAGE_INVALID")

    def test_mark_index_hot_backup_keeps_component_quality_and_monotonic_return(self):
        import asyncio
        import time as _time
        from types import SimpleNamespace
        from qdl.reference.local_mark_index import CacheRefreshingMarkIndexView
        from qdl.runtime.kn_hot_view import CanonicalHotView, HotViewUnavailable
        from qdl.runtime.kn_market_cache import CacheRow, SourceBoundary

        exercised = set()
        for binding, payload in self.latest_records:
            if binding.feed is not FeedType.MARK_INDEX_PRICE or not binding.authoritative or binding.source_role != "PRIMARY":
                continue
            with self.subTest(binding=binding.binding_id):
                exercised.add(binding.instrument.identity.venue)
                now = _time.time_ns()
                def timed(age):
                    env = market_data_pb2.EventEnvelope.FromString(retimed_mark_index(payload, now_ns=now, age_ms=age))
                    env.normalized_at_ns = env.received_at_ns + 1
                    env.published_at_ns = env.received_at_ns + 2
                    return env.SerializeToString(deterministic=True)
                old, fresh = timed(10000), timed(100)
                self.put_latest(binding, old, offset=10, mark=20)
                candidate = CanonicalHotView(self.lpk(binding), (CacheRow(fresh, 30),), SourceBoundary(TOPIC_ID, 0, 40))
                # Keep the primary's actual source partition for an exact comparison.
                primary = self.reader.latest(self.lpk(binding))
                candidate = replace(candidate, boundary=replace(candidate.boundary, partition=primary.boundary.partition))
                replies = [candidate]
                def hot(*args):
                    if isinstance(replies[0], Exception): raise replies[0]
                    return replies[0]
                backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST,
                    topic_id=TOPIC_ID, hot_client=SimpleNamespace(latest=hot))
                view = CacheRefreshingMarkIndexView.from_catalog(self.catalog).attach_cache(backend=backend, relax_to_alpha=False)
                def read():
                    return asyncio.run(view.read(instrument_uid=binding.instrument.instrument_uid,
                        instrument_revision=binding.instrument.metadata_revision,source_policy_id=binding.source_policy_id,
                        max_freshness_ms=2000,gateway_epoch=1,now_ns=now))
                result = read()
                self.assertIsNotNone(result.record, result.reason)
                self.assertEqual(result.record.canonical, fresh)
                self.assertEqual(result.record.spool_watermark_offset, 40)
                replies[0] = HotViewUnavailable("test outage")
                self.assertIsNone(read().record, "must not return the old cached pair")
                self.put_latest(binding, fresh, offset=30, mark=40)
                self.assertIsNotNone(read().record, "primary catches up without backup")
                # A fresh envelope with an old component pair still fails unchanged oracle.
                now += 10_000_000_000
                replies[0] = candidate
                self.assertIsNone(read().record)
        self.assertGreaterEqual(len(exercised), 2, "both Binance and OKX fixtures required")

    # ------------------------------------------------------------ D29 read view

    def test_the_stream_read_view_runs_the_python_oracle_on_one_view(self):
        import base64
        import json as _json

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from qdl.query.v2 import query_pb2
        from qdl.runtime.internal_auth import stable_hmac_signature
        from qdl.runtime.kn_read_view import READ_VIEW_PATH, READ_VIEW_SCHEMA, install_kn_read_view
        from qdl.runtime.stable_source import StableConsumerCursorIssuer, StableGrpcSnapshotLoader, build_stable_query_stack
        from qdl_sdk.models import DataRequirement as SdkRequirement, Feed, Grade

        payloads = self.history_rows(30)
        self.put_bars(self.bar_binding, payloads, offsets=list(range(30)), mark=31)
        settings = KnCursorSettings(KEYS, "k4-test", "paper", TOPIC_ID, 1, "kn4-route-a", 600)
        backend = KnMarketCacheQueryBackend(self.reader, self.catalog, schema_digest=DIGEST, topic_id=TOPIC_ID,
                                            clock_ns=lambda: self.now_ns)
        issuer = KnCursorV3Issuer(settings, self.catalog)
        service, _backend, _issuer = build_stable_query_stack(
            spool=None, catalog=self.catalog, schema_digest=DIGEST, handoff=None, cursor_ttl_seconds=600,
            backend=backend, issuer=issuer,
        )
        secret = bytes(range(32, 64))
        app = FastAPI()
        install_kn_read_view(app, service=service, backend=backend, issuer=issuer, secret=secret)
        client = TestClient(app, raise_server_exceptions=False)

        def call(kind: str, requirement, *, sign=True, consumer="alpha.binance.paper.stable"):
            body = _json.dumps({"schema": READ_VIEW_SCHEMA, "kind": kind, "consumer_id": consumer,
                                "requirement": base64.b64encode(requirement.to_proto().SerializeToString()).decode()})
            headers = {"X-QDL-Stable-Signature": stable_hmac_signature(secret if sign else b"x" * 32, body.encode())}
            return client.post(READ_VIEW_PATH, content=body, headers=headers)

        sdk = SdkRequirement(
            instrument_uid=self.bar_binding.instrument.instrument_uid, feed=Feed.BAR,
            consumer_grade=Grade.ALPHA, source_policy_id=self.bar_binding.source_policy_id,
            interval=self.bar_binding.interval, warmup_limit=30,
            stale_policy=__import__("qdl_sdk.models", fromlist=["StalePolicy"]).StalePolicy.OBSERVE,
        )
        reply = call("SNAPSHOT", sdk)
        self.assertEqual(reply.status_code, 200, reply.text)
        snapshot = query_pb2.GetSnapshotResponse.FromString(reply.content)
        self.assertEqual(snapshot.watermark_offset, 31)
        self.assertEqual([event.SerializeToString(deterministic=True) for event in snapshot.events],
                         sorted(payloads, key=open_ms_of))
        # Oracle parity: the spool loader's events for the same records.
        spool_backend = self.spool_backend(payloads)
        spool_service, _b, _i = build_stable_query_stack(
            spool=spool_backend.spool, catalog=self.catalog, schema_digest=DIGEST, handoff=None,
            cursor_ttl_seconds=600, backend=spool_backend,
            issuer=type("NoCursor", (), {"bind_history": staticmethod(lambda r, h, consumer_id: h)})(),
        )
        from qdl.stream.grpc_service import requirement_from_proto
        oracle = StableGrpcSnapshotLoader(service=spool_service, backend=spool_backend,
                                          issuer=type("NoCursor", (), {"bind_history": staticmethod(
                                              lambda r, h, consumer_id: h)})()).load(
            requirement_from_proto(sdk.to_proto()), consumer_id="alpha.binance.paper.stable")
        self.assertEqual(list(snapshot.events), list(oracle.events))
        self.assertEqual(snapshot.data_as_of_ns, oracle.data_as_of_ns)
        # The cursor is a v3 cursor at the view boundary.
        self.assertEqual(SignedCursorV3Codec(KEYS, active_key_id="k4-test").verify(
            snapshot.stream_cursor, consumer_id="alpha.binance.paper.stable", environment="paper",
            requirement_digest_value=requirement_digest(requirement_from_proto(sdk.to_proto())),
            expected=CursorV3Expectation(
                environment="paper", stream=self.catalog.canonical_stream, source_topic_id=TOPIC_ID,
                partition_plan_epoch=1, source_policy_revision=self.catalog.source_policy_revision,
                catalog_revision=self.catalog.catalog_revision, route_generation="kn4-route-a"),
            now_ns=__import__("time").time_ns()).source_offset, 31)
        status = call("STATUS", sdk)
        self.assertEqual(status.status_code, 200, status.text)
        feed_status = query_pb2.GetFeedStatusResponse.FromString(status.content)
        expected = service.status(requirement_from_proto(sdk.to_proto()))
        self.assertEqual((feed_status.state, feed_status.policy_id, list(feed_status.flags)),
                         (expected.state, expected.policy_id, list(expected.flags)))
        # Typed refusals.
        self.assertEqual(call("STATUS", sdk, sign=False).status_code, 401)
        absent = next(b for b in self.catalog.bindings if b.feed is FeedType.TRADE)
        not_ready = call("STATUS", SdkRequirement(
            instrument_uid=absent.instrument.instrument_uid, feed=Feed.TRADE, consumer_grade=Grade.ALPHA,
            source_policy_id=absent.source_policy_id))
        self.assertEqual((not_ready.status_code, not_ready.json()["code"]), (409, "DATA_NOT_READY"))
        bad = client.post(READ_VIEW_PATH, content=b"{}", headers={
            "X-QDL-Stable-Signature": stable_hmac_signature(secret, b"{}")})
        self.assertEqual(bad.status_code, 400)


if __name__ == "__main__":
    unittest.main()

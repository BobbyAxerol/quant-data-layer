from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from qdl.history import BarRecord, reconcile_history_live
from qdl.replay import (
    DeterministicReplayEngine,
    GapFreeHandoff,
    ReplayGapError,
    SignedHandoffCursorCodec,
)
from qdl.runtime.stable import build_stable_handoff
from qdl.transport import Cursor, CursorExpired, DurableEvent, SQLiteDurableSpool, SpoolConfig


STREAM = "md.canonical.v2.trade"
PARTITION = "uid/trade/binance"


def event(index: int, *, accepted_at_ns: int | None = None) -> DurableEvent:
    return DurableEvent(
        stream=STREAM,
        partition_key=PARTITION,
        event_id=index.to_bytes(16, "big"),
        payload=f'{{"index":{index}}}'.encode(),
        accepted_at_ns=accepted_at_ns or (1_000_000_000 + index),
        content_type="application/json",
    )


def canonicalize(raw: DurableEvent) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2.normalized",
        partition_key=raw.partition_key,
        event_id=hashlib.sha256(raw.event_id + b"canonical").digest()[:16],
        payload=raw.payload.upper(),
        accepted_at_ns=raw.accepted_at_ns,
        content_type="application/json",
        headers={"raw_event_id": raw.event_id.hex()},
    )


def bar(index: int, *, revision: int = 0, close: str | None = None) -> BarRecord:
    return BarRecord(
        instrument_uid="uid", instrument_id="TEST.MARKET.EQUITY.X",
        instrument_revision=1, source_id="provider", source_role="PRIMARY",
        interval="1m", open_time_ns=1_000_000_000 + index * 60_000_000_000,
        close_time_ns=61_000_000_000 + index * 60_000_000_000,
        open="1", high="3", low="1", close=close or "2", volume="10",
        trade_count=1, is_final=True, revision=revision,
        origin="VENUE_NATIVE" if revision == 0 else "RECONCILED",
        normalizer_version="phase4", config_revision=1,
        raw_lineage_json=f'{{"row":{index}}}',
    )


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.now = [10_000_000_000]
        clock = lambda: self.now[0]
        self.spool = SQLiteDurableSpool(
            SpoolConfig(
                path=Path(self.temp.name) / "replay.sqlite3",
                min_free_disk_bytes=0,
                replay_retention_seconds=1,
                maintenance_interval_seconds=1,
            ),
            clock_ns=clock,
        )
        self.clock = clock
        self.codec = SignedHandoffCursorCodec(
            {"2026-08": b"x" * 32}, active_key_id="2026-08", clock_ns=clock
        )
        self.handoff = GapFreeHandoff(self.spool, self.codec, clock_ns=clock)

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def grant(self, watermark=None, ttl=60):
        watermark = watermark or self.handoff.capture_watermark(
            stream=STREAM, partition_key=PARTITION
        )
        return self.handoff.issue(
            consumer_id="alpha-a", snapshot_id="snapshot-a",
            snapshot_watermark=watermark, ttl_seconds=ttl,
        )

    def test_cold_start_overlap_duplicate_and_late_event_have_exactly_once_offsets(self):
        grant = self.grant()
        first = self.spool.append(event(1))
        duplicate = self.spool.append(event(1))
        late = self.spool.append(event(2, accepted_at_ns=1))
        rows = self.handoff.replay(
            token=grant.token, consumer_id="alpha-a",
            stream=STREAM, partition_key=PARTITION,
        )
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(first.cursor.offset, 1)
        self.assertEqual([row.cursor.offset for row in rows], [1, 2])
        self.assertEqual(rows[1].event.accepted_at_ns, 1)

    def test_reconnect_resumes_after_durable_checkpoint(self):
        grant = self.grant()
        self.spool.append_many([event(1), event(2)])
        first_replay = self.handoff.replay(
            token=grant.token, consumer_id="alpha-a",
            stream=STREAM, partition_key=PARTITION,
        )
        self.handoff.acknowledge(consumer_id="alpha-a", cursor=first_replay[0].cursor)
        second_replay = self.handoff.replay(
            token=grant.token, consumer_id="alpha-a",
            stream=STREAM, partition_key=PARTITION,
        )
        self.assertEqual([row.cursor.offset for row in second_replay], [2])

    def test_expired_tampered_and_wrong_scope_tokens_fail_closed(self):
        grant = self.grant(ttl=1)
        with self.assertRaisesRegex(ValueError, "signature mismatch"):
            self.handoff.replay(
                token=grant.token[:-1] + ("A" if grant.token[-1] != "A" else "B"),
                consumer_id="alpha-a", stream=STREAM, partition_key=PARTITION,
            )
        with self.assertRaisesRegex(ValueError, "scope mismatch"):
            self.handoff.replay(
                token=grant.token, consumer_id="alpha-b",
                stream=STREAM, partition_key=PARTITION,
            )
        self.now[0] += 1_000_000_000
        with self.assertRaises(CursorExpired):
            self.handoff.replay(
                token=grant.token, consumer_id="alpha-a",
                stream=STREAM, partition_key=PARTITION,
            )

    def test_compaction_boundary_requires_new_snapshot(self):
        grant = self.grant(ttl=60)
        self.spool.append(event(1))
        self.now[0] += 2_000_000_000
        self.spool.append(event(2))
        with self.assertRaises(CursorExpired):
            self.handoff.replay(
                token=grant.token, consumer_id="alpha-a",
                stream=STREAM, partition_key=PARTITION,
            )

    def test_missing_durable_offset_is_detected(self):
        grant = self.grant()
        self.spool.append_many([event(1), event(2), event(3)])
        self.spool._connection.execute(
            "DELETE FROM events WHERE stream = ? AND partition_key = ? AND logical_offset = 2",
            (STREAM, PARTITION),
        )
        with self.assertRaisesRegex(ReplayGapError, "expected durable offset 2"):
            self.handoff.replay(
                token=grant.token, consumer_id="alpha-a",
                stream=STREAM, partition_key=PARTITION,
            )

    def test_generation_bound_cursor_expires_legacy_and_cross_generation_tokens(self):
        legacy_grant = self.grant()
        generation_a = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"2026-08": b"x" * 32},
                active_key_id="2026-08",
                generation_id="cache-generation-a",
                clock_ns=self.clock,
            ),
            clock_ns=self.clock,
        )
        grant_a = generation_a.issue(
            consumer_id="alpha-a",
            snapshot_id="snapshot-a",
            snapshot_watermark=generation_a.capture_watermark(
                stream=STREAM, partition_key=PARTITION
            ),
            ttl_seconds=60,
        )
        restarted_same_generation = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"2026-08": b"x" * 32},
                active_key_id="2026-08",
                generation_id="cache-generation-a",
                clock_ns=self.clock,
            ),
            clock_ns=self.clock,
        )
        self.assertEqual(
            restarted_same_generation.replay(
                token=grant_a.token,
                consumer_id="alpha-a",
                stream=STREAM,
                partition_key=PARTITION,
            ),
            [],
        )
        self.spool.append(event(1))
        advanced = restarted_same_generation.advance_token(
            token=grant_a.token,
            consumer_id="alpha-a",
            cursor=Cursor(STREAM, PARTITION, 1),
            ttl_seconds=60,
        )
        self.assertEqual(
            restarted_same_generation.resolve_scope(
                token=advanced.token, consumer_id="alpha-a"
            ).watermark_offset,
            1,
        )

        with self.assertRaisesRegex(CursorExpired, "previous cache generation"):
            restarted_same_generation.replay(
                token=legacy_grant.token,
                consumer_id="alpha-a",
                stream=STREAM,
                partition_key=PARTITION,
            )
        generation_b = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"2026-08": b"x" * 32},
                active_key_id="2026-08",
                generation_id="cache-generation-b",
                clock_ns=self.clock,
            ),
            clock_ns=self.clock,
        )
        with self.assertRaisesRegex(CursorExpired, "previous cache generation"):
            generation_b.replay(
                token=grant_a.token,
                consumer_id="alpha-a",
                stream=STREAM,
                partition_key=PARTITION,
            )

    def test_stable_handoff_binds_the_durable_spool_cache_identity(self):
        handoff = build_stable_handoff(
            SimpleNamespace(
                cursor_keys={"2026-08": b"x" * 32},
                active_cursor_key_id="2026-08",
                cursor_ttl_seconds=60,
            ),
            self.spool,
        )
        grant = handoff.issue(
            consumer_id="alpha-a",
            snapshot_id="snapshot-a",
            snapshot_watermark=handoff.capture_watermark(
                stream=STREAM, partition_key=PARTITION
            ),
            ttl_seconds=60,
        )
        peer = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"2026-08": b"x" * 32},
                active_key_id="2026-08",
                generation_id=self.spool.cache_id,
                clock_ns=self.clock,
            ),
            clock_ns=self.clock,
        )
        self.assertEqual(
            peer.resolve_scope(token=grant.token, consumer_id="alpha-a").watermark_offset,
            0,
        )

    def test_generation_bound_cursor_keeps_signature_and_scope_fail_closed(self):
        handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"2026-08": b"x" * 32},
                active_key_id="2026-08",
                generation_id="cache-generation-a",
                clock_ns=self.clock,
            ),
            clock_ns=self.clock,
        )
        grant = handoff.issue(
            consumer_id="alpha-a",
            snapshot_id="snapshot-a",
            snapshot_watermark=handoff.capture_watermark(
                stream=STREAM, partition_key=PARTITION
            ),
            ttl_seconds=60,
        )
        with self.assertRaisesRegex(ValueError, "signature mismatch"):
            handoff.replay(
                token=grant.token[:-1] + ("A" if grant.token[-1] != "A" else "B"),
                consumer_id="alpha-a",
                stream=STREAM,
                partition_key=PARTITION,
            )
        with self.assertRaisesRegex(ValueError, "consumer scope mismatch"):
            handoff.replay(
                token=grant.token,
                consumer_id="alpha-b",
                stream=STREAM,
                partition_key=PARTITION,
            )


class DeterministicReplayTests(unittest.TestCase):
    def test_repeated_raw_replay_has_identical_versioned_checksums(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "spool.sqlite3", min_free_disk_bytes=0
            ))
            try:
                spool.append_many([event(1), event(2), event(3)])
                records = spool.read(stream=STREAM, partition_key=PARTITION)
                engine = DeterministicReplayEngine()
                first = engine.replay(
                    records, canonicalizer=canonicalize,
                    normalizer_version="normalizer@abc", config_revision=7,
                    source_revision="adapter@def",
                )
                second = engine.replay(
                    records, canonicalizer=canonicalize,
                    normalizer_version="normalizer@abc", config_revision=7,
                    source_revision="adapter@def",
                )
                changed_config = engine.replay(
                    records, canonicalizer=canonicalize,
                    normalizer_version="normalizer@abc", config_revision=8,
                    source_revision="adapter@def",
                )
                self.assertEqual(first, second)
                self.assertEqual(first.event_count, 3)
                self.assertNotEqual(first.run_checksum, changed_config.run_checksum)
                self.assertEqual(first.canonical_checksum, changed_config.canonical_checksum)
            finally:
                spool.close()


class HistoryLiveReconciliationTests(unittest.TestCase):
    def test_overlap_revision_and_calendar_supplied_gap_are_explicit(self):
        historical = [bar(0), bar(1)]
        live = [bar(1, revision=1, close="2.5"), bar(3)]
        report = reconcile_history_live(
            historical, live,
            expected_open_times_ns=tuple(bar(i).open_time_ns for i in range(4)),
        )
        self.assertEqual(report.row_count, 3)
        self.assertEqual(report.overlap_count, 1)
        self.assertEqual(report.revised_count, 1)
        self.assertEqual(report.expected_gap_open_times_ns, (bar(2).open_time_ns,))

    def test_same_revision_conflict_fails(self):
        with self.assertRaisesRegex(ValueError, "same revision"):
            reconcile_history_live(
                [bar(0)], [bar(0, close="2.5")],
                expected_open_times_ns=(bar(0).open_time_ns,),
            )


if __name__ == "__main__":
    unittest.main()

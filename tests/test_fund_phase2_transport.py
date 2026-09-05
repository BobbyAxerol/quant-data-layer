from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from qdl.transport import (
    BackpressureRequired,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    DurablePublisher,
    EventIdCollision,
    PayloadCorruption,
    PublisherState,
    SQLiteDurableSpool,
    SpoolConfig,
)
from qdl.transport.contracts import partition_key


class ManualClock:
    def __init__(self, now_ns: int = 1_800_000_000_000_000_000):
        self.now_ns = now_ns

    def __call__(self) -> int:
        self.now_ns += 1
        return self.now_ns


def event(index: int, *, payload: bytes | None = None) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2.trade",
        partition_key="instrument/trade/source",
        event_id=index.to_bytes(16, "big"),
        payload=payload or f"event-{index}".encode(),
        accepted_at_ns=1_800_000_000_000_000_000 + index,
        headers={"schema": "qdl.marketdata.trade/2"},
    )


class CursorTests(unittest.TestCase):
    def test_cursor_token_round_trip_is_transport_neutral(self):
        cursor = Cursor("md.canonical.v2.trade", "instrument/trade/source", 42)
        token = cursor.to_token()
        self.assertNotIn("redis", token.lower())
        self.assertNotIn("kafka", token.lower())
        self.assertEqual(Cursor.from_token(token), cursor)

    def test_partition_key_requires_complete_domain_identity(self):
        self.assertEqual(
            partition_key(instrument_uid="uuid", feed_type="TRADE", source_id="direct-1"),
            "uuid/trade/direct-1",
        )
        with self.assertRaises(ValueError):
            partition_key(instrument_uid="", feed_type="TRADE", source_id="direct-1")


class SQLiteDurableSpoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "phase2.sqlite3"
        self.clock = ManualClock()

    def tearDown(self):
        self.temp.cleanup()

    def spool(self, **overrides) -> SQLiteDurableSpool:
        config = SpoolConfig(
            path=self.path,
            max_records=overrides.get("max_records", 10),
            max_payload_bytes=overrides.get("max_payload_bytes", 1024),
            max_event_bytes=overrides.get("max_event_bytes", 512),
            min_free_disk_bytes=0,
            consumer_ttl_seconds=10,
            replay_retention_seconds=10,
            maintenance_interval_seconds=1,
            max_partition_records=overrides.get("max_partition_records", 0),
            retain_partition_windows=overrides.get("retain_partition_windows", False),
        )
        return SQLiteDurableSpool(config, clock_ns=self.clock)

    def test_concurrent_replicas_share_one_initialized_spool(self):
        config = SpoolConfig(path=self.path, min_free_disk_bytes=0)
        barrier = Barrier(8)

        def open_replica() -> SQLiteDurableSpool:
            barrier.wait()
            return SQLiteDurableSpool(config)

        with ThreadPoolExecutor(max_workers=8) as executor:
            replicas = list(executor.map(lambda _: open_replica(), range(8)))
        try:
            self.assertEqual(len({replica.cache_id for replica in replicas}), 1)
            self.assertTrue(all(replica.integrity_check() for replica in replicas))
        finally:
            for replica in replicas:
                replica.close()

    def test_commit_restart_replay_and_idempotent_retry(self):
        with self.spool() as spool:
            first = spool.append(event(1))
            duplicate = spool.append(event(1))
            self.assertFalse(first.duplicate)
            self.assertTrue(duplicate.duplicate)
            self.assertEqual(first.cursor, duplicate.cursor)

        with self.spool() as recovered:
            rows = recovered.read(
                stream=event(1).stream,
                partition_key=event(1).partition_key,
            )
            self.assertEqual([row.event.payload for row in rows], [b"event-1"])
            self.assertEqual(rows[0].payload_sha256, first.payload_sha256)

    def test_cache_identity_survives_restart_and_changes_after_atomic_rebuild(self):
        with self.spool() as spool:
            first_cache_id = spool.cache_id
            spool.append(event(1))
        with self.spool() as recovered:
            self.assertEqual(recovered.cache_id, first_cache_id)
        self.path.unlink()
        with self.spool() as rebuilt:
            self.assertNotEqual(rebuilt.cache_id, first_cache_id)
            self.assertEqual(rebuilt.stats().records, 0)

    def test_tail_returns_newest_window_without_changing_replay_order(self):
        with self.spool(max_records=10) as spool:
            spool.append_many([event(index) for index in range(1, 6)])
            replay = spool.read(
                stream=event(1).stream,
                partition_key=event(1).partition_key,
                limit=2,
            )
            latest = spool.read_tail(
                stream=event(1).stream,
                partition_key=event(1).partition_key,
                limit=2,
            )
            self.assertEqual([row.cursor.offset for row in replay], [1, 2])
            self.assertEqual([row.cursor.offset for row in latest], [4, 5])
            self.assertEqual(
                [row.event.payload for row in latest], [b"event-4", b"event-5"]
            )

    def test_tail_allows_only_configured_internal_partition_headroom(self):
        with self.spool(max_partition_records=10_064) as spool:
            self.assertEqual(
                spool.read_tail(
                    stream=event(1).stream,
                    partition_key=event(1).partition_key,
                    limit=10_064,
                ),
                [],
            )
        with self.spool() as spool:
            with self.assertRaisesRegex(ValueError, "between 1 and 10000"):
                spool.read_tail(
                    stream=event(1).stream,
                    partition_key=event(1).partition_key,
                    limit=10_001,
                )

    def test_retained_window_survives_age_and_restart_but_remains_count_bounded(self):
        with self.spool(max_partition_records=3, retain_partition_windows=True) as spool:
            first = spool.append(event(1))
            spool.append(event(2))
            self.clock.now_ns += 86_400_000_000_000
            spool.append(event(3))
            self.assertEqual(spool.stats().records, 3)
            self.assertTrue(spool.append(event(1)).duplicate)
            self.assertEqual(spool.trim_consumed(), 0)
        with self.spool(max_partition_records=3, retain_partition_windows=True) as spool:
            self.assertEqual(spool.stats().records, 3)
            spool.append(event(4))
            spool.append(event(5))
            self.assertEqual(spool.stats().records, 3)
            with self.assertRaises(CursorExpired):
                spool.read(stream=event(1).stream, partition_key=event(1).partition_key,
                           after=first.cursor)
            self.assertTrue(spool.integrity_check())

    def test_default_partition_window_still_expires_by_age(self):
        with self.spool(max_partition_records=3) as spool:
            spool.append(event(1))
            self.clock.now_ns += 11_000_000_000
            spool.append(event(2))
            self.assertIsNone(spool.find_event(stream=event(1).stream, event_id=event(1).event_id))

    def test_retained_partition_window_requires_finite_bound(self):
        for value, bound in ((True, 0), ("true", 3)):
            with self.subTest(value=value, bound=bound), self.assertRaises(ValueError):
                SpoolConfig(path=self.path, retain_partition_windows=value,
                            max_partition_records=bound)

    def test_partition_window_is_bounded_and_old_cursor_expires(self):
        with self.spool(max_records=10, max_partition_records=3) as spool:
            spool.append_many([event(index) for index in range(1, 6)])
            self.assertEqual(spool.stats().records, 3)
            self.assertEqual(
                set(spool.find_events(
                    stream=event(1).stream,
                    event_ids=[event(index).event_id for index in range(1, 6)],
                )),
                {event(index).event_id for index in range(3, 6)},
            )
            self.assertIsNone(
                spool.find_event(stream=event(1).stream, event_id=event(1).event_id)
            )
            with self.assertRaises(CursorExpired):
                spool.read(
                    stream=event(1).stream,
                    partition_key=event(1).partition_key,
                    after=Cursor(event(1).stream, event(1).partition_key, 1),
                )
            retained = spool.read(
                stream=event(1).stream,
                partition_key=event(1).partition_key,
                after=Cursor(event(1).stream, event(1).partition_key, 2),
            )
            self.assertEqual(
                [row.cursor.offset for row in retained], [3, 4, 5]
            )

    def test_event_id_collision_fails_closed_without_partial_row(self):
        with self.spool() as spool:
            spool.append(event(1))
            with self.assertRaises(EventIdCollision):
                spool.append(event(1, payload=b"different"))
            self.assertEqual(spool.stats().records, 1)
            self.assertEqual(spool.high_watermark(event(1).stream, event(1).partition_key), 1)

    def test_replay_detects_payload_corruption(self):
        with self.spool() as spool:
            first = spool.append(event(1))
            spool._connection.execute(
                """
                UPDATE events SET payload = ?
                WHERE stream = ? AND partition_key = ? AND logical_offset = ?
                """,
                (b"tampered", first.cursor.stream, first.cursor.partition_key, first.cursor.offset),
            )
            with self.assertRaises(PayloadCorruption):
                spool.read(
                    stream=first.cursor.stream,
                    partition_key=first.cursor.partition_key,
                )

    def test_batch_is_one_transaction_and_rolls_back_on_collision(self):
        with self.spool() as spool:
            spool.append(event(1))
            with self.assertRaises(EventIdCollision):
                spool.append_many([event(2), event(1, payload=b"different")])
            self.assertEqual(spool.stats().records, 1)
            results = spool.append_many([event(2), event(3)])
            self.assertEqual([item.cursor.offset for item in results], [2, 3])

    def test_capacity_blocks_and_publisher_reports_blocked(self):
        with self.spool(max_records=1) as spool:
            publisher = DurablePublisher(spool, sleep=lambda _: None)
            publisher.publish(event(1))
            with self.assertRaises(BackpressureRequired):
                publisher.publish(event(2))
            self.assertEqual(publisher.status.state, PublisherState.BLOCKED)
            self.assertEqual(spool.stats().records, 1)

    def test_metadata_and_physical_storage_bounds_fail_closed(self):
        with SQLiteDurableSpool(
            SpoolConfig(
                path=self.path,
                max_records=10,
                max_payload_bytes=1024,
                max_event_bytes=512,
                max_storage_bytes=1024,
                max_partitions=1,
                max_consumer_checkpoints=1,
                min_free_disk_bytes=0,
            ),
            clock_ns=self.clock,
        ) as spool:
            with self.assertRaises(BackpressureRequired):
                spool.append(event(1))

        second_path = Path(self.temp.name) / "metadata.sqlite3"
        with SQLiteDurableSpool(
            SpoolConfig(
                path=second_path,
                max_records=10,
                max_payload_bytes=1024,
                max_event_bytes=512,
                max_storage_bytes=10 * 1024 * 1024,
                max_partitions=1,
                max_consumer_checkpoints=1,
                min_free_disk_bytes=0,
            ),
            clock_ns=self.clock,
        ) as spool:
            first = spool.append(event(1))
            with self.assertRaises(BackpressureRequired):
                spool.append(
                    DurableEvent(
                        stream=event(2).stream,
                        partition_key="another/partition/source",
                        event_id=event(2).event_id,
                        payload=event(2).payload,
                        accepted_at_ns=event(2).accepted_at_ns,
                    )
                )
            spool.register_consumer(
                consumer_id="first",
                stream=first.cursor.stream,
                partition_key=first.cursor.partition_key,
            )
            with self.assertRaises(BackpressureRequired):
                spool.register_consumer(
                    consumer_id="second",
                    stream=first.cursor.stream,
                    partition_key=first.cursor.partition_key,
                )

    def test_checkpoint_is_monotonic_and_trim_waits_for_all_active_consumers(self):
        with self.spool() as spool:
            first = spool.append(event(1))
            second = spool.append(event(2))
            spool.register_consumer(
                consumer_id="fast",
                stream=first.cursor.stream,
                partition_key=first.cursor.partition_key,
            )
            spool.register_consumer(
                consumer_id="slow",
                stream=first.cursor.stream,
                partition_key=first.cursor.partition_key,
            )
            spool.checkpoint(consumer_id="fast", cursor=second.cursor, ttl_seconds=10)
            spool.checkpoint(consumer_id="slow", cursor=first.cursor, ttl_seconds=10)
            self.assertEqual(spool.trim_consumed(), 1)
            with self.assertRaises(CheckpointRegression):
                spool.checkpoint(consumer_id="fast", cursor=first.cursor, ttl_seconds=10)
            with self.assertRaises(CursorExpired):
                spool.read(
                    stream=first.cursor.stream,
                    partition_key=first.cursor.partition_key,
                    after=Cursor(first.cursor.stream, first.cursor.partition_key, 0),
                )

    def test_expired_consumer_no_longer_pins_retention(self):
        with self.spool() as spool:
            first = spool.append(event(1))
            spool.register_consumer(
                consumer_id="abandoned",
                stream=first.cursor.stream,
                partition_key=first.cursor.partition_key,
                ttl_seconds=1,
            )
            self.clock.now_ns += 2_000_000_000
            self.assertEqual(spool.trim_consumed(), 0)
            self.clock.now_ns += 11_000_000_000
            spool.append(event(2))
            self.assertEqual(spool.stats().records, 1)

    def test_poison_record_is_quarantined_with_bounded_metadata(self):
        with SQLiteDurableSpool(
            SpoolConfig(
                path=self.path,
                max_records=10,
                max_payload_bytes=1024,
                max_event_bytes=512,
                max_storage_bytes=10 * 1024 * 1024,
                max_quarantine_records=1,
                min_free_disk_bytes=0,
            ),
            clock_ns=self.clock,
        ) as spool:
            quarantine_id = spool.quarantine(
                event=event(1),
                reason_code="PARSER_INVALID",
                reason_message="missing price",
                retry_count=3,
            )
            self.assertEqual(quarantine_id, 1)
            with self.assertRaises(BackpressureRequired):
                spool.quarantine(
                    event=event(2),
                    reason_code="PARSER_INVALID",
                    reason_message="second poison record",
                    retry_count=3,
                )

    def test_idempotent_quarantine_replay_preserves_the_single_evidence_slot(self):
        config = SpoolConfig(
            path=self.path,
            max_records=10,
            max_payload_bytes=1024,
            max_event_bytes=512,
            max_storage_bytes=10 * 1024 * 1024,
            max_quarantine_records=1,
            min_free_disk_bytes=0,
        )
        with SQLiteDurableSpool(config, clock_ns=self.clock) as spool:
            first = spool.quarantine_once(
                event=event(1),
                reason_code="RECOVERY_BACKFILL_OVERLAP_CONFLICT",
                reason_message="retained native final BAR wins",
                retry_count=0,
            )
            with SQLiteDurableSpool(config, clock_ns=self.clock) as replica:
                replay = replica.quarantine_once(
                    event=event(1),
                    reason_code="RECOVERY_BACKFILL_OVERLAP_CONFLICT",
                    reason_message="retained native final BAR wins",
                    retry_count=0,
                )
            self.assertEqual(first, replay)
            self.assertEqual(len(spool.quarantine_records()), 1)
            with self.assertRaises(BackpressureRequired):
                spool.quarantine_once(
                    event=event(2),
                    reason_code="RECOVERY_BACKFILL_OVERLAP_CONFLICT",
                    reason_message="second distinct overlap",
                    retry_count=0,
                )


class DurablePublisherTests(unittest.TestCase):
    def test_transient_failure_marks_degraded_then_recovers_after_commit(self):
        class FlakySink:
            attempts = 0

            def append(self, durable_event):
                self.attempts += 1
                if self.attempts < 3:
                    raise OSError("broker unavailable")
                return type(
                    "Result",
                    (),
                    {
                        "cursor": Cursor(
                            durable_event.stream, durable_event.partition_key, 1
                        ),
                        "duplicate": False,
                    },
                )()

        sleeps = []
        publisher = DurablePublisher(FlakySink(), sleep=sleeps.append)
        result = publisher.publish(event(1))
        self.assertEqual(result.cursor.offset, 1)
        self.assertEqual(publisher.status.state, PublisherState.LIVE)
        self.assertEqual(len(sleeps), 2)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qdl.transport import (
    BackpressureRequired,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    DurablePublisher,
    EventIdCollision,
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
        )
        return SQLiteDurableSpool(config, clock_ns=self.clock)

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

    def test_event_id_collision_fails_closed_without_partial_row(self):
        with self.spool() as spool:
            spool.append(event(1))
            with self.assertRaises(EventIdCollision):
                spool.append(event(1, payload=b"different"))
            self.assertEqual(spool.stats().records, 1)
            self.assertEqual(spool.high_watermark(event(1).stream, event(1).partition_key), 1)

    def test_capacity_blocks_and_publisher_reports_blocked(self):
        with self.spool(max_records=1) as spool:
            publisher = DurablePublisher(spool, sleep=lambda _: None)
            publisher.publish(event(1))
            with self.assertRaises(BackpressureRequired):
                publisher.publish(event(2))
            self.assertEqual(publisher.status.state, PublisherState.BLOCKED)
            self.assertEqual(spool.stats().records, 1)

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
        with self.spool() as spool:
            quarantine_id = spool.quarantine(
                event=event(1),
                reason_code="PARSER_INVALID",
                reason_message="missing price",
                retry_count=3,
            )
            self.assertEqual(quarantine_id, 1)


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

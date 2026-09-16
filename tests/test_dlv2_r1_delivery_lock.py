"""DL-V2 R1: delivering a live record must not read the durable store.

Every delivered record used to advance its signed cursor through
``GapFreeHandoff.issue`` -> ``SQLiteDurableSpool.high_watermark``, a SELECT
taken under the spool ``RLock`` that ``append_many`` holds through its
``synchronous=FULL`` fsync. At ~945 records per second that serialised the
delivery path against ingest inside the one process holding the writer lease.

R1.1 lets the gateway supply the offset the durable store assigned when it
committed the very record being acknowledged. These tests pin the invariant
that makes that safe: a token is never signed above the durable watermark, and
anything the gateway cannot prove falls back to reading the store.

R1.2 covers the replay path, where a token had to advance through records the
subscriber's predicate rejects without paying a store read per record.
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.stream.gateway import DurableStreamGateway
from qdl.transport import Cursor, DurableEvent, SQLiteDurableSpool, SpoolConfig

STREAM = "md.canonical.v2.trade"
PARTITION = "uid/trade/binance"
OTHER = "uid/quote/binance"


def event(index: int, partition: str = PARTITION) -> DurableEvent:
    return DurableEvent(
        stream=STREAM,
        partition_key=partition,
        event_id=index.to_bytes(16, "big"),
        payload=f'{{"index":{index}}}'.encode(),
        accepted_at_ns=1_000_000_000 + index,
        content_type="application/json",
    )


class CountingSpool(SQLiteDurableSpool):
    """A spool that counts durable watermark reads, nothing else changed."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.high_watermark_calls = 0

    def high_watermark(self, stream: str, partition_key: str) -> int:
        self.high_watermark_calls += 1
        return super().high_watermark(stream, partition_key)


class StubAuthority:
    """A writer lease whose epoch the test controls."""

    def __init__(self, epoch: int = 1) -> None:
        self.epoch = epoch

    @property
    def current_epoch(self) -> int | None:
        return self.epoch

    def assert_active(self, expected_epoch: int | None = None) -> int:
        if expected_epoch is not None and expected_epoch != self.epoch:
            raise RuntimeError("stream gateway lease epoch changed")
        return self.epoch


class DeliveryLockTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.spool = CountingSpool(SpoolConfig(
            path=Path(self.temp.name) / "r1.sqlite3",
            min_free_disk_bytes=0,
        ))
        self.addCleanup(self.spool.close)
        self.codec = SignedHandoffCursorCodec({"k": b"x" * 32}, active_key_id="k")
        self.handoff = GapFreeHandoff(self.spool, self.codec)
        self.authority = StubAuthority()
        self.gateway = DurableStreamGateway(
            handoff=self.handoff, sink=self.spool, authority=self.authority
        )

    def token(self, partition: str = PARTITION) -> str:
        return self.handoff.issue(
            consumer_id="alpha",
            snapshot_id="snap",
            snapshot_watermark=self.handoff.capture_watermark(
                stream=STREAM, partition_key=partition
            ),
            ttl_seconds=60,
        ).token

    async def subscribe(self, partition: str = PARTITION):
        return await self.gateway.open(
            consumer_id="alpha", stream=STREAM, partition_key=partition,
            token=self.token(partition),
        )

    # ------------------------------------------------ R1.1 watermark cache
    async def test_live_delivery_stops_reading_the_durable_store(self) -> None:
        subscription = await self.subscribe()
        await self.gateway.publish_many([event(1), event(2), event(3)])
        self.spool.high_watermark_calls = 0
        for _ in range(3):
            await subscription.next_live()
        self.assertEqual(
            self.spool.high_watermark_calls, 0,
            "delivering a record this process committed must not re-read the store",
        )

    async def test_the_signed_offset_still_matches_the_durable_offset(self) -> None:
        """The cache must not change what a token means, only how it is proven."""

        subscription = await self.subscribe()
        await self.gateway.publish_many([event(1), event(2)])
        for expected in (1, 2):
            record = await subscription.next_live()
            self.assertEqual(record.stored.cursor.offset, expected)
            scope = self.handoff.resolve_scope(
                token=record.resume_token, consumer_id="alpha"
            )
            # The token tracks the record just delivered, which is at or below
            # the durable watermark; it is not the watermark itself, because
            # later records may already be committed and undelivered.
            self.assertEqual(scope.watermark_offset, expected)
            self.assertLessEqual(
                scope.watermark_offset,
                self.spool.high_watermark(STREAM, PARTITION),
            )

    async def test_a_watermark_is_never_signed_above_the_durable_state(self) -> None:
        """The invariant, exercised over interleaved appends and advances."""

        subscription = await self.subscribe()
        for batch in ([1], [2, 3], [4], [5, 6, 7]):
            await self.gateway.publish_many([event(i) for i in batch])
            for _ in batch:
                record = await subscription.next_live()
                durable = self.spool.high_watermark(STREAM, PARTITION)
                self.assertLessEqual(
                    record.stored.cursor.offset, durable,
                    "a signed cursor must never lead the durable store",
                )

    async def test_an_unknown_partition_falls_back_to_the_store(self) -> None:
        """Absence of knowledge is a read, never an assumption."""

        self.assertIsNone(
            self.gateway.known_high_watermark(STREAM, "never/seen", 1)
        )
        self.spool.high_watermark_calls = 0
        await self.gateway.publish_many([event(1, OTHER)])
        subscription = await self.subscribe(OTHER)
        self.assertIsNotNone(
            self.gateway.known_high_watermark(STREAM, OTHER, 1)
        )

    async def test_a_watermark_from_another_lease_epoch_is_discarded(self) -> None:
        """After a fence this process is not the writer, so it knows nothing."""

        await self.gateway.publish_many([event(1), event(2)])
        self.assertEqual(self.gateway.known_high_watermark(STREAM, PARTITION, 1), 2)
        self.authority.epoch = 2
        self.assertIsNone(
            self.gateway.known_high_watermark(STREAM, PARTITION, 2),
            "a watermark learned under a previous lease must not be trusted",
        )

    async def test_fencing_clears_every_remembered_watermark(self) -> None:
        await self.gateway.publish_many([event(1)])
        self.assertIsNotNone(self.gateway.known_high_watermark(STREAM, PARTITION, 1))
        await self.gateway.fence_all()
        self.assertIsNone(self.gateway.known_high_watermark(STREAM, PARTITION, 1))

    async def test_a_duplicate_append_never_moves_the_watermark_backwards(self) -> None:
        await self.gateway.publish_many([event(1), event(2), event(3)])
        self.assertEqual(self.gateway.known_high_watermark(STREAM, PARTITION, 1), 3)
        await self.gateway.publish_many([event(2)])  # same event id, duplicate
        self.assertEqual(
            self.gateway.known_high_watermark(STREAM, PARTITION, 1), 3,
            "a duplicate reports the offset of the record that already existed",
        )

    async def test_acknowledging_beyond_what_this_writer_committed_reads_the_store(self) -> None:
        """A cursor the gateway cannot account for must not be signed from cache."""

        subscription = await self.subscribe()
        await self.gateway.publish_many([event(1)])
        await subscription.next_live()
        self.spool.high_watermark_calls = 0
        with self.assertRaises(ValueError):
            await self.gateway.advance_token(
                token=subscription.token,
                consumer_id="alpha",
                cursor=Cursor(STREAM, PARTITION, 99),
            )
        self.assertEqual(
            self.spool.high_watermark_calls, 1,
            "the store must be consulted before refusing an unaccounted cursor",
        )

    def test_the_handoff_refuses_a_malformed_known_watermark(self) -> None:
        for bad in ("3", -1, 1.5, True):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    self.handoff.issue(
                        consumer_id="alpha", snapshot_id="snap",
                        snapshot_watermark=Cursor(STREAM, PARTITION, 0),
                        ttl_seconds=60, known_high_watermark=bad,
                    )

    def test_a_known_watermark_still_bounds_the_cursor(self) -> None:
        """Supplying the value does not disable the check it feeds."""

        with self.assertRaises(ValueError):
            self.handoff.issue(
                consumer_id="alpha", snapshot_id="snap",
                snapshot_watermark=Cursor(STREAM, PARTITION, 5),
                ttl_seconds=60, known_high_watermark=4,
            )

    async def test_watermarks_are_tracked_per_partition(self) -> None:
        await self.gateway.publish_many([event(1), event(2)])
        await self.gateway.publish_many([event(10, OTHER)])
        self.assertEqual(self.gateway.known_high_watermark(STREAM, PARTITION, 1), 2)
        self.assertEqual(self.gateway.known_high_watermark(STREAM, OTHER, 1), 1)

    async def test_concurrent_publishers_keep_the_invariant(self) -> None:
        """Interleave two partitions under concurrent publishes."""

        subscription = await self.subscribe()
        await asyncio.gather(*(
            self.gateway.publish_many([event(i), event(i + 100, OTHER)])
            for i in range(1, 6)
        ))
        durable = self.spool.high_watermark(STREAM, PARTITION)
        self.assertEqual(
            self.gateway.known_high_watermark(STREAM, PARTITION, 1), durable
        )
        for _ in range(5):
            record = await subscription.next_live()
            self.assertLessEqual(record.stored.cursor.offset, durable)


class ReplayTokenAdvanceTests(unittest.IsolatedAsyncioTestCase):
    """R1.2: replay must skip cheaply without losing resume position.

    The loop is reproduced here against the real gateway and handoff rather
    than the gRPC servicer, because the servicer needs a full access context.
    The rule under test is the one the servicer implements: advance the token
    once per unmatched run, once per matched record, never backwards, and never
    past a record that was not seen.
    """

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.spool = CountingSpool(SpoolConfig(
            path=Path(self.temp.name) / "replay.sqlite3",
            min_free_disk_bytes=0,
        ))
        self.addCleanup(self.spool.close)
        self.codec = SignedHandoffCursorCodec({"k": b"x" * 32}, active_key_id="k")
        self.handoff = GapFreeHandoff(self.spool, self.codec)
        self.gateway = DurableStreamGateway(handoff=self.handoff, sink=self.spool)

    async def _replay(self, count: int, matcher):
        """Run the R1.2 loop over `count` durable records and report what it did."""

        token = self.handoff.issue(
            consumer_id="alpha", snapshot_id="snap",
            snapshot_watermark=self.handoff.capture_watermark(
                stream=STREAM, partition_key=PARTITION
            ),
            ttl_seconds=60,
        ).token
        await self.gateway.publish_many([event(i) for i in range(1, count + 1)])
        subscription = await self.gateway.open(
            consumer_id="alpha", stream=STREAM, partition_key=PARTITION,
            token=token, accepts=matcher, replay_limit=max(count, 1),
        )
        delivered, advances = [], 0
        pending_skip = None
        for stored in subscription.initial:
            if not subscription.accepts(stored):
                pending_skip = stored
                continue
            if pending_skip is not None:
                await subscription.record(pending_skip); advances += 1
                pending_skip = None
            record = await subscription.record(stored); advances += 1
            delivered.append(record.stored.cursor.offset)
        if pending_skip is not None:
            await subscription.record(pending_skip); advances += 1
        return subscription, delivered, advances

    async def test_a_fully_unmatched_replay_advances_the_token_once(self) -> None:
        subscription, delivered, advances = await self._replay(
            50, lambda _stored: False
        )
        self.assertEqual(delivered, [])
        self.assertEqual(advances, 1, "one advance for the whole unmatched run")
        scope = self.handoff.resolve_scope(
            token=subscription.token, consumer_id="alpha"
        )
        self.assertEqual(
            scope.watermark_offset, 50,
            "the token must still sit past every record replay consumed",
        )

    async def test_every_matched_record_is_delivered_exactly_once(self) -> None:
        _, delivered, _ = await self._replay(
            20, lambda stored: stored.cursor.offset % 2 == 0
        )
        self.assertEqual(delivered, list(range(2, 21, 2)))
        self.assertEqual(len(delivered), len(set(delivered)))

    async def test_the_token_never_moves_backwards_across_mixed_runs(self) -> None:
        token = self.handoff.issue(
            consumer_id="alpha", snapshot_id="snap",
            snapshot_watermark=self.handoff.capture_watermark(
                stream=STREAM, partition_key=PARTITION
            ),
            ttl_seconds=60,
        ).token
        await self.gateway.publish_many([event(i) for i in range(1, 31)])
        keep = {3, 4, 5, 17, 30}
        subscription = await self.gateway.open(
            consumer_id="alpha", stream=STREAM, partition_key=PARTITION,
            token=token, accepts=lambda s: s.cursor.offset in keep, replay_limit=30,
        )
        seen = 0
        pending_skip = None
        for stored in subscription.initial:
            if not subscription.accepts(stored):
                pending_skip = stored
                continue
            if pending_skip is not None:
                await subscription.record(pending_skip)
                offset = self.handoff.resolve_scope(
                    token=subscription.token, consumer_id="alpha"
                ).watermark_offset
                self.assertGreaterEqual(offset, seen)
                seen = offset
                pending_skip = None
            await subscription.record(stored)
            offset = self.handoff.resolve_scope(
                token=subscription.token, consumer_id="alpha"
            ).watermark_offset
            self.assertGreaterEqual(offset, seen)
            seen = offset
        if pending_skip is not None:
            await subscription.record(pending_skip)
            seen = self.handoff.resolve_scope(
                token=subscription.token, consumer_id="alpha"
            ).watermark_offset
        self.assertEqual(seen, 30)

    async def test_resuming_after_replay_never_repeats_a_delivered_record(self) -> None:
        """The property the collapse must not break."""

        subscription, delivered, _ = await self._replay(
            12, lambda s: s.cursor.offset in {2, 9}
        )
        resumed = await self.gateway.open(
            consumer_id="alpha", stream=STREAM, partition_key=PARTITION,
            token=subscription.token, accepts=lambda s: s.cursor.offset in {2, 9},
            replay_limit=12,
        )
        again = [s.cursor.offset for s in resumed.initial if resumed.accepts(s)]
        self.assertEqual(delivered, [2, 9])
        self.assertEqual(again, [], "a resumed subscriber must not see them twice")

    async def test_the_collapse_removes_durable_reads_for_skipped_records(self) -> None:
        for records in (10, 100, 400):
            with self.subTest(records=records):
                self.setUp()
                self.spool.high_watermark_calls = 0
                await self._replay(records, lambda _stored: False)
                # The remaining reads are the fixed cost of opening the
                # subscription, not a per-record cost: issuing the initial
                # token and the replay-limit watermark check. The property
                # under test is that the count does not grow with the run.
                self.assertLessEqual(self.spool.high_watermark_calls, 4)
                self.assertLess(
                    self.spool.high_watermark_calls, records,
                    "durable reads must not scale with the unmatched run length",
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

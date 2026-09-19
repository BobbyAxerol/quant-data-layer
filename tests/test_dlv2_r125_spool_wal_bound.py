"""DL-V2 R1.25: the durable spool keeps its WAL inside the declared bound.

On 2026-09-17 the spool's WAL grew to 922 MB while the main file held 2148 MB,
reaching the 3 GiB physical bound with 7,720 bytes to spare. Every canonical
write then failed closed for two hours. The frames had already been
checkpointed - PASSIVE recycles a WAL but never shrinks the file - so the
outage was the file, not the retained data. These tests pin both halves of the
repair: routine retention remains nonblocking, and the physical bound reclaims
before it refuses a write.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qdl.transport.contracts import (
    BackpressureRequired,
    DurableEvent,
    FINAL_BAR_CLOSE_TIME_NS_HEADER,
)
from qdl.transport.sqlite_spool import (
    JOURNAL_SIZE_LIMIT_BYTES,
    SpoolConfig,
    SQLiteDurableSpool,
)


def _event(index: int, payload: bytes) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2",
        partition_key="OKX/SWAP/BTC-USDT-SWAP/candle1m",
        event_id=index.to_bytes(16, "big"),
        payload=payload,
        accepted_at_ns=1_700_000_000_000_000_000 + index,
    )


def _bar_event(index: int, close_time_ns: int) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2",
        partition_key="OKX/SWAP/BTC-USDT-SWAP/bar/1m",
        event_id=index.to_bytes(16, "big"),
        payload=f"final-bar-{index}".encode(),
        accepted_at_ns=1_700_000_000_000_000_000 + index,
        headers={FINAL_BAR_CLOSE_TIME_NS_HEADER: str(close_time_ns)},
    )


class SpoolWalBoundTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "cache.sqlite3"
        self.addCleanup(self.temp.cleanup)

    def _spool(self, **overrides) -> SQLiteDurableSpool:
        config = SpoolConfig(
            path=self.path,
            max_records=10_000,
            max_payload_bytes=64 * 1024 * 1024,
            max_storage_bytes=32 * 1024 * 1024,
            min_free_disk_bytes=0,
            maintenance_interval_seconds=1,
            **overrides,
        )
        spool = SQLiteDurableSpool(config)
        self.addCleanup(spool.close)
        return spool

    def test_journal_size_limit_is_declared_on_the_connection(self):
        spool = self._spool()
        limit = spool._connection.execute("PRAGMA journal_size_limit").fetchone()[0]
        self.assertEqual(int(limit), JOURNAL_SIZE_LIMIT_BYTES)

    def test_final_bar_watermark_is_atomic_max_and_survives_reopen(self):
        spool = self._spool()
        partition = "OKX/SWAP/BTC-USDT-SWAP/bar/1m"
        other_partition = "OKX/SWAP/ETH-USDT-SWAP/bar/1m"
        self.assertIsNone(
            spool.final_bar_watermark(
                stream="md.canonical.v2", partition_key=partition
            )
        )
        spool.append_many((_bar_event(1, 300), _bar_event(2, 200)))
        self.assertEqual(
            spool.final_bar_watermark(
                stream="md.canonical.v2", partition_key=partition
            ),
            300,
        )
        self.assertEqual(
            spool.seed_final_bar_watermark(
                stream="md.canonical.v2", partition_key=partition, close_time_ns=250
            ),
            300,
        )
        spool.append(DurableEvent(
            stream="md.canonical.v2",
            partition_key=other_partition,
            event_id=b"o" * 16,
            payload=b"final-bar-other-partition",
            accepted_at_ns=2,
            headers={FINAL_BAR_CLOSE_TIME_NS_HEADER: "900"},
        ))
        self.assertEqual(
            spool.final_bar_watermark(
                stream="md.canonical.v2", partition_key=other_partition
            ),
            900,
        )
        self.assertEqual(
            spool.final_bar_watermark(
                stream="md.canonical.v2", partition_key=partition
            ),
            300,
        )
        spool.close()
        reopened = self._spool()
        self.assertEqual(
            reopened.final_bar_watermark(
                stream="md.canonical.v2", partition_key=partition
            ),
            300,
        )

    def test_duplicate_event_can_hydrate_legacy_final_bar_watermark(self):
        spool = self._spool()
        legacy = DurableEvent(
            stream="md.canonical.v2",
            partition_key="OKX/SWAP/BTC-USDT-SWAP/bar/1m",
            event_id=b"l" * 16,
            payload=b"legacy-final-bar",
            accepted_at_ns=1,
        )
        spool.append(legacy)
        self.assertIsNone(
            spool.final_bar_watermark(
                stream=legacy.stream, partition_key=legacy.partition_key
            )
        )
        hydrated = DurableEvent(
            stream=legacy.stream,
            partition_key=legacy.partition_key,
            event_id=legacy.event_id,
            payload=legacy.payload,
            accepted_at_ns=legacy.accepted_at_ns,
            headers={FINAL_BAR_CLOSE_TIME_NS_HEADER: "400"},
        )
        result = spool.append(hydrated)
        self.assertTrue(result.duplicate)
        self.assertEqual(
            spool.final_bar_watermark(
                stream=legacy.stream, partition_key=legacy.partition_key
            ),
            400,
        )

    def test_legacy_hydration_is_reused_after_a_second_spool_opens(self):
        first = self._spool()
        partition = "OKX/SWAP/BTC-USDT-SWAP/bar/1m"
        calls = []
        self.assertEqual(
            first.hydrate_final_bar_watermark(
                stream="md.canonical.v2",
                partition_key=partition,
                legacy_lookup=lambda: calls.append("first") or 300,
            ),
            300,
        )
        self.assertEqual(calls, ["first"])

        second = SQLiteDurableSpool(first.config)
        self.addCleanup(second.close)
        self.assertEqual(
            second.hydrate_final_bar_watermark(
                stream="md.canonical.v2",
                partition_key=partition,
                legacy_lookup=lambda: self.fail("second spool must not rescan legacy tail"),
            ),
            300,
        )

    def test_malformed_final_bar_watermark_rolls_back_the_event(self):
        spool = self._spool()
        malformed = DurableEvent(
            stream="md.canonical.v2",
            partition_key="OKX/SWAP/BTC-USDT-SWAP/bar/1m",
            event_id=b"m" * 16,
            payload=b"malformed-final-bar",
            accepted_at_ns=1,
            headers={FINAL_BAR_CLOSE_TIME_NS_HEADER: "-1"},
        )
        with self.assertRaisesRegex(ValueError, "final BAR watermark header"):
            spool.append(malformed)
        self.assertEqual(
            spool.read_tail(
                stream=malformed.stream, partition_key=malformed.partition_key, limit=1
            ),
            [],
        )

    def test_truncate_checkpoint_reclaims_the_wal_file(self):
        spool = self._spool()
        spool.append_many([_event(index, b"p" * 4096) for index in range(200)])
        self.assertGreater(spool._wal_bytes(), 0)
        self.assertTrue(spool._checkpoint_wal_truncate_locked())
        self.assertEqual(spool._wal_bytes(), 0)

    def test_wal_bytes_reports_zero_without_a_wal_file(self):
        spool = self._spool()
        spool._checkpoint_wal_truncate_locked()
        self.assertEqual(spool._wal_bytes(), 0)

    def test_routine_maintenance_never_runs_blocking_truncate(self):
        spool = self._spool()
        with patch.object(
            spool, "_wal_bytes", return_value=JOURNAL_SIZE_LIMIT_BYTES + 1
        ), patch.object(
            spool, "_checkpoint_wal_passive_locked", return_value=False
        ) as passive, patch.object(
            spool, "_checkpoint_wal_truncate_locked", return_value=True
        ) as truncate:
            spool.append(_event(1, b"p" * 4096))

        passive.assert_called_once_with()
        truncate.assert_not_called()

    def test_storage_bytes_counts_every_physical_file(self):
        spool = self._spool()
        spool.append_many([_event(index, b"p" * 2048) for index in range(50)])
        total = sum(
            path.stat().st_size
            for path in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm"))
            if path.exists()
        )
        self.assertEqual(spool.storage_bytes(), total)

    def test_physical_bound_reclaims_before_it_refuses_a_write(self):
        spool = self._spool()
        reclaimed = []
        original = spool._checkpoint_wal_truncate_locked

        def record():
            reclaimed.append(True)
            return original()

        spool._checkpoint_wal_truncate_locked = record
        object.__setattr__(spool.config, "max_storage_bytes", spool.storage_bytes() + 1)
        with self.assertRaises(BackpressureRequired):
            spool.append_many([_event(999, b"p" * 8192)])
        self.assertTrue(reclaimed, "the bound must attempt a reclaim before failing")

    def test_bound_still_fails_closed_when_nothing_can_be_reclaimed(self):
        spool = self._spool()
        object.__setattr__(spool.config, "max_storage_bytes", 1024)
        with self.assertRaises(BackpressureRequired) as raised:
            spool.append_many([_event(1, b"p" * 4096)])
        self.assertIn("physical storage bound", str(raised.exception))


if __name__ == "__main__":
    unittest.main()


class SpoolPhysicalBoundMatchesRetentionTests(unittest.TestCase):
    """The physical ceiling must not be tighter than the retention it carries.

    On 2026-09-17 the spool failed every write closed at a 3 GiB ceiling while
    still inside its own row policy: 1,301,097 rows occupied 1,114 MB of payload
    in a 2,312 MB file, and max_records allows 1,841,712. A bound that refuses
    writes before retention has trimmed anything is not a safety bound, it is an
    outage waiting for a backlog.
    """

    def test_the_physical_bound_carries_the_rows_max_records_allows(self):
        from qdl.runtime.stable_capacity import STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW

        # Measured on the live cache, payload only, before index and page
        # overhead roughly doubles it on disk.
        measured_payload_bytes_per_row = 1114 * 1024 * 1024 / 1_301_097
        physical_partitions = 183
        max_records = physical_partitions * STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW
        on_disk = max_records * measured_payload_bytes_per_row * 2
        self.assertGreater(6 * 1024**3, on_disk + JOURNAL_SIZE_LIMIT_BYTES)
        self.assertLess(3 * 1024**3, on_disk)

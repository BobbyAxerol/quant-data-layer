"""DL-V2 R1.25: the durable spool keeps its WAL inside the declared bound.

On 2026-09-17 the spool's WAL grew to 922 MB while the main file held 2148 MB,
reaching the 3 GiB physical bound with 7,720 bytes to spare. Every canonical
write then failed closed for two hours. The frames had already been
checkpointed - PASSIVE recycles a WAL but never shrinks the file - so the
outage was the file, not the retained data. These tests pin both halves of the
repair: retention work reclaims a WAL that outgrew journal_size_limit, and the
physical bound reclaims before it refuses a write.
"""

import tempfile
import unittest
from pathlib import Path

from qdl.transport.contracts import BackpressureRequired, DurableEvent
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

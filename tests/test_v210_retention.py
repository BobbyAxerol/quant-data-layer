from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from qdl.transport import Cursor, CursorExpired, DurableEvent, SQLiteDurableSpool, SpoolConfig


def event(index: int, partition: str = "symbol/trade/source") -> DurableEvent:
    return DurableEvent(
        "canonical", partition, index.to_bytes(16, "big"),
        f"test-only-retention-{index}".encode(), 1_800_000_000_000_000_000 + index,
    )


class LegacySpool(SQLiteDurableSpool):
    def _retained_threshold_locked(self, stream, partition_key, limit):
        row = self._connection.execute(
            "SELECT logical_offset FROM events WHERE stream=? AND partition_key=? "
            "ORDER BY logical_offset DESC LIMIT 1 OFFSET ?",
            (stream, partition_key, limit - 1),
        ).fetchone()
        return int(row[0]) if row else None


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def config(self, name="spool", limit=5, retain=True):
        return SpoolConfig(
            path=Path(self.temp.name) / f"{name}.sqlite3",
            max_records=30_000, max_payload_bytes=50_000_000,
            max_partition_records=limit, retain_partition_windows=retain,
            min_free_disk_bytes=0,
        )

    def assert_rows(self, spool, offsets, partition="symbol/trade/source"):
        rows = spool.read_tail(stream="canonical", partition_key=partition, limit=100)
        self.assertEqual([row.cursor.offset for row in rows], offsets)
        actual = spool._connection.execute(
            "SELECT COUNT(*),COALESCE(SUM(LENGTH(payload)),0) FROM events"
        ).fetchone()
        stats = spool.stats()
        self.assertEqual((stats.records, stats.payload_bytes), tuple(actual))
        self.assertTrue(spool.integrity_check())

    def delete_test_rows(self, config, offsets):
        # Deliberately sparse legacy/corruption fixture in this test's DB only.
        with sqlite3.connect(config.path) as connection:
            connection.executemany(
                "DELETE FROM events WHERE stream='canonical' AND logical_offset=?",
                [(value,) for value in offsets],
            )
            connection.execute(
                "UPDATE spool_state SET event_records=(SELECT COUNT(*) FROM events), "
                "payload_bytes=(SELECT COALESCE(SUM(LENGTH(payload)),0) FROM events)"
            )

    def test_dense_duplicate_reopen_and_cursor_expiration(self):
        config = self.config()
        with SQLiteDurableSpool(config) as spool:
            spool.append_many([event(i) for i in range(1, 13)])
            self.assert_rows(spool, [8, 9, 10, 11, 12])
            self.assertTrue(spool.append(event(12)).duplicate)
            self.assert_rows(spool, [8, 9, 10, 11, 12])
            with self.assertRaises(CursorExpired):
                spool.read(stream="canonical", partition_key=event(1).partition_key,
                           after=Cursor("canonical", event(1).partition_key, 1))
        with SQLiteDurableSpool(config) as spool:
            spool.append(event(13))
            self.assert_rows(spool, [9, 10, 11, 12, 13])

    def test_foreign_commit_invalidates_dense_proof_and_keeps_sparse_rows(self):
        config = self.config()
        with SQLiteDurableSpool(config) as spool:
            spool.append_many([event(i) for i in range(1, 6)])
            self.delete_test_rows(config, [3])
            spool.append(event(6))
            self.assert_rows(spool, [1, 2, 4, 5, 6])
            spool.append(event(7))
            self.assert_rows(spool, [2, 4, 5, 6, 7])
            spool.append(event(8))
            self.assert_rows(spool, [4, 5, 6, 7, 8])

    def test_sparse_legacy_reopen_matches_original_oracle(self):
        config = self.config()
        with SQLiteDurableSpool(config) as spool:
            spool.append_many([event(i) for i in range(1, 6)])
        self.delete_test_rows(config, [2, 4])
        with SQLiteDurableSpool(config) as spool:
            spool.append_many([event(i) for i in range(6, 9)])
            self.assert_rows(spool, [3, 5, 6, 7, 8])

    def test_rollback_does_not_retain_uncommitted_dense_certificate(self):
        config = self.config()
        with SQLiteDurableSpool(config) as spool:
            spool.append_many([event(i) for i in range(1, 6)])
            with patch.object(spool, "_decrement_usage_locked", side_effect=RuntimeError("injected")):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    spool.append(event(6))
            self.assertFalse(spool._dense_retained_partitions)
            self.assert_rows(spool, [1, 2, 3, 4, 5])
            spool.append(event(6))
            self.assert_rows(spool, [2, 3, 4, 5, 6])

    def test_independent_partition_isolation_and_large_batch(self):
        with SQLiteDurableSpool(self.config()) as spool:
            spool.append_many([event(i, "binance/trade/a") for i in range(1, 13)]
                              + [event(100 + i, "okx/trade/b") for i in range(1, 4)])
            self.assert_rows(spool, [8, 9, 10, 11, 12], "binance/trade/a")
            self.assert_rows(spool, [1, 2, 3], "okx/trade/b")

    def test_two_writer_connections_preserve_newest_five(self):
        config = self.config()
        with SQLiteDurableSpool(config) as a, SQLiteDurableSpool(config) as b:
            def write(pair):
                writer, index = pair
                return writer.append(event(index)).cursor.offset
            with ThreadPoolExecutor(max_workers=2) as pool:
                offsets = list(pool.map(write, [(a if i % 2 else b, i) for i in range(1, 31)]))
            self.assertEqual(sorted(offsets), list(range(1, 31)))
            self.assert_rows(a, [26, 27, 28, 29, 30])
            a.append(event(31))
            self.assert_rows(b, [27, 28, 29, 30, 31])

    def test_limit_one_and_empty_partition(self):
        with SQLiteDurableSpool(self.config(limit=1)) as spool:
            self.assertIsNone(spool._retained_threshold_locked("canonical", "missing", 1))
            spool.append_many([event(1), event(2)])
            self.assert_rows(spool, [2])

    def test_age_retained_path_does_not_use_dense_certificate(self):
        with SQLiteDurableSpool(self.config(retain=False)) as spool:
            spool.append_many([event(i) for i in range(1, 8)])
            self.assert_rows(spool, [3, 4, 5, 6, 7])
            self.assertFalse(spool._dense_retained_partitions)

    def test_steady_window_avoids_linear_sqlite_vm_work_with_identical_rows(self):
        work = {}
        for name, kind in (("legacy", LegacySpool), ("candidate", SQLiteDurableSpool)):
            with kind(self.config(name=name, limit=12_064)) as spool:
                for start in range(1, 12_065, 512):
                    spool.append_many([event(i) for i in range(start, min(start + 512, 12_065))])
                ticks = [0]
                def progress():
                    ticks[0] += 1
                    return 0
                spool._connection.set_progress_handler(progress, 100)
                spool.append(event(12_065))
                spool._connection.set_progress_handler(None, 0)
                work[name] = ticks[0]
                self.assertEqual(spool.stats().records, 12_064)
                self.assertEqual(spool.read_tail(stream="canonical", partition_key=event(1).partition_key,
                                                limit=1)[0].event.event_id, event(12_065).event_id)
        self.assertLess(work["candidate"], work["legacy"] // 5, work)


if __name__ == "__main__":
    unittest.main()

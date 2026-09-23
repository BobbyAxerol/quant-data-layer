"""Hot single-statement spool reads do not queue behind a batch visit.

``visit_tails`` keeps its connection lock while the caller materializes one
physical tail, which for a 5,000-row BAR warmup takes seconds. On 2026-09-23
(v2.1.1 Phase-3 stage 5) every hot latest read on that Query replica waited
behind it. ``read_tail`` and ``high_watermark`` now use their own query-only
connection; these tests pin that they return promptly, see committed data and
never see a write that has not committed.
"""

from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import time
import unittest

from qdl.transport import DurableEvent, SQLiteDurableSpool
from qdl.transport.sqlite_spool import SpoolConfig


def _event(index: int) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2", partition_key="p1",
        event_id=index.to_bytes(16, "big"), payload=b"x" * 32, accepted_at_ns=1_000 + index,
    )


class HotReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "spool.sqlite3", max_records=1_000,
            max_payload_bytes=8 << 20, max_storage_bytes=16 << 20, min_free_disk_bytes=0,
        ))
        self.spool.append_many([_event(index) for index in range(1, 6)])

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def test_hot_reads_return_while_a_batch_visit_holds_its_lock(self):
        inside = threading.Event()
        timings: dict[str, float] = {}

        def slow_visit(_key, rows):
            inside.set()
            time.sleep(1.5)  # a caller materializing thousands of rows

        visitor = threading.Thread(target=lambda: self.spool.visit_tails(
            requests=[("md.canonical.v2", "p1", 5)], visit=slow_visit))
        visitor.start()
        self.assertTrue(inside.wait(5))
        started = time.monotonic()
        rows = self.spool.read_tail(stream="md.canonical.v2", partition_key="p1", limit=5)
        high = self.spool.high_watermark("md.canonical.v2", "p1")
        timings["hot"] = time.monotonic() - started
        visitor.join()
        self.assertLess(timings["hot"], 0.5)
        self.assertEqual([item.cursor.offset for item in rows], [1, 2, 3, 4, 5])
        self.assertEqual(high, 5)

    def test_hot_reads_see_new_commits_and_never_an_open_write(self):
        self.spool.append_many([_event(6)])
        self.assertEqual(self.spool.high_watermark("md.canonical.v2", "p1"), 6)
        with self.spool._lock:
            self.spool._connection.execute("BEGIN IMMEDIATE")
            self.spool._connection.execute(
                "UPDATE partitions SET next_offset = 99 WHERE stream = ? AND partition_key = ?",
                ("md.canonical.v2", "p1"),
            )
            # Uncommitted on the writer connection: the hot reader must not see it.
            self.assertEqual(self.spool.high_watermark("md.canonical.v2", "p1"), 6)
            self.spool._connection.execute("ROLLBACK")
        self.assertEqual(len(self.spool.read_tail(stream="md.canonical.v2", partition_key="p1", limit=10)), 6)

    def test_close_releases_the_hot_connection(self):
        self.spool.read_tail(stream="md.canonical.v2", partition_key="p1", limit=1)
        self.assertIsNotNone(self.spool._hot_connection)
        self.spool.close()
        self.assertIsNone(self.spool._hot_connection)


if __name__ == "__main__":
    unittest.main()

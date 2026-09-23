"""Stream ingest spans (v2.1.1 Phase-3, 2026-09-23): the writer must say where time goes."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from qdl.runtime import stable_ingest
from qdl.transport import DurableEvent, SQLiteDurableSpool
from qdl.transport.sqlite_spool import SpoolConfig


class _Gateway:
    # Same protocol as ``DurableStreamGateway.subscriber_count``: a property.
    subscriber_count = 7


class IngestSpanTests(unittest.TestCase):
    def test_spool_counts_lock_wait_and_hold_per_append(self):
        with tempfile.TemporaryDirectory() as root:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(root) / "s.sqlite3", max_records=100, max_payload_bytes=8 << 20,
                max_storage_bytes=16 << 20, min_free_disk_bytes=0))
            try:
                spool.append_many([DurableEvent(stream="s", partition_key="p", event_id=i.to_bytes(16, "big"),
                                                payload=b"x", accepted_at_ns=1) for i in range(1, 4)])
                timing = spool.append_timing
                self.assertEqual((timing["calls"], timing["rows"]), (1, 3))
                self.assertGreater(timing["hold_ns"], 0)
            finally:
                spool.close()

    def test_one_line_per_ten_seconds_splits_the_request(self):
        spans = stable_ingest._IngestSpans()

        class Spool:
            append_timing = {"calls": 4, "rows": 40, "wait_ns": 8_000_000, "hold_ns": 40_000_000,
                             "max_wait_ns": 3_000_000, "max_hold_ns": 15_000_000}

        spans.observe(events=40, decode_ns=5_000_000, raw_lookup_ns=1_000_000, view_pre_ns=1_000_000,
                      publish_ns=60_000_000, view_post_ns=1_000_000, total_ns=70_000_000)
        with self.assertLogs(stable_ingest.logger, "INFO") as logs:
            with patch.object(stable_ingest.time, "monotonic_ns", return_value=spans._reported_at_ns + 11_000_000_000):
                spans.maybe_report(spool=Spool(), gateway=_Gateway())
        line = logs.output[0]
        self.assertIn("qdl_stable_ingest_spans requests=1", line)
        self.assertIn("publish_ms=mean:60.0,max:60.0", line)
        self.assertIn("append_calls=4 lock_wait_ms=mean:2.0,max:3.0 lock_hold_ms=mean:10.0,max:15.0", line)
        self.assertIn("subscribers=7", line)
        self.assertEqual(spans._requests, 0)

    def test_report_reads_the_real_gateway_subscriber_count(self):
        # KN-1 K1.2: the fake above once declared ``subscriber_count`` as a
        # method while the real gateway exposes a property, so the report
        # raised ``TypeError`` on the stream writer. Use the real collaborator.
        from qdl.replay.handoff import GapFreeHandoff, SignedHandoffCursorCodec
        from qdl.stream.gateway import DurableStreamGateway

        with tempfile.TemporaryDirectory() as root:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(root) / "s.sqlite3", max_records=100, max_payload_bytes=8 << 20,
                max_storage_bytes=16 << 20, min_free_disk_bytes=0))
            try:
                handoff = GapFreeHandoff(spool, SignedHandoffCursorCodec({"k": b"x" * 32}, active_key_id="k"))
                gateway = DurableStreamGateway(handoff=handoff, sink=spool)
                spans = stable_ingest._IngestSpans()
                spans.observe(events=1, decode_ns=1, raw_lookup_ns=1, view_pre_ns=1,
                              publish_ns=1, view_post_ns=1, total_ns=6)
                with self.assertLogs(stable_ingest.logger, "INFO") as logs:
                    with patch.object(stable_ingest.time, "monotonic_ns",
                                      return_value=spans._reported_at_ns + 11_000_000_000):
                        spans.maybe_report(spool=spool, gateway=gateway)
                self.assertIn("subscribers=0", logs.output[0])
            finally:
                spool.close()


if __name__ == "__main__":
    unittest.main()

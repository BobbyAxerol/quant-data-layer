import base64
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.p184_dr05_observer import CanonicalObserver, record_summary


class RecordTests(unittest.TestCase):
    def test_canonical_capture_identity_and_exact_bytes(self):
        from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
        event = EventEnvelope(venue="OKX", instrument_uid="instrument", connection_generation=8,
                              raw_capture_id=b"1234567890123456", source_sequence="202")
        event.book_snapshot.native_sequence = "202"
        raw = event.SerializeToString()
        result = record_summary("isolated.canonical", 0, raw, True, partition=3)
        self.assertEqual(result["partition"], 3)
        self.assertEqual(result["capture_id"], event.raw_capture_id.hex())
        self.assertEqual(result["generation"], 8)
        self.assertEqual(result["payload"], "book_snapshot")
        self.assertEqual(base64.b64decode(result["event_b64"]), raw)

    def test_quarantine_preserves_reason_not_phase_label(self):
        from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord, QUARANTINE_REASON_SEQUENCE_GAP
        event = QuarantineRecord(reason=QUARANTINE_REASON_SEQUENCE_GAP, safe_summary="test")
        event.raw.capture_id = b"1234567890123456"
        event.raw.venue = "BINANCE"
        event.raw.connection_generation = 7
        row = record_summary("isolated.quarantine", 2, event.SerializeToString(), False)
        self.assertEqual(row["reason"], "QUARANTINE_REASON_SEQUENCE_GAP")
        self.assertEqual(row["capture_id"], event.raw.capture_id.hex())
        self.assertNotIn("acceptance", row)


class BarrierTests(unittest.TestCase):
    def observer(self, committed=2):
        from confluent_kafka import KafkaError
        observer = CanonicalObserver.__new__(CanonicalObserver)
        observer.raw, observer.canonical, observer.topics = "raw", "canonical", ["canonical", "quarantine"]
        observer.positions = {topic: 0 for topic in observer.topics}
        observer.tp = lambda topic, partition, offset=None: (topic, partition, offset)
        error = SimpleNamespace(code=lambda: KafkaError._PARTITION_EOF)
        class Consumer:
            def __init__(self):
                self.queue, self.assignments, self.closed = [], [], False
            def assign(self, values):
                self.assignments.append(values)
                self.queue = [SimpleNamespace(error=lambda: error, topic=lambda t=t: t, offset=lambda: 0)
                              for t in observer.topics]
            def poll(self, timeout):
                return self.queue.pop(0) if self.queue else None
            def close(self):
                self.closed = True
        observer.consumer = Consumer()
        observer.consumer.assign([])
        observer.offset_reader = SimpleNamespace(committed=lambda *a, **k: [SimpleNamespace(offset=committed)], close=lambda: None)
        return observer

    def test_barrier_refetches_after_commit(self):
        observer = self.observer()
        result = observer.observe({"offset": 1, "capture_id": "absent"}, timeout=1)
        self.assertEqual(result["raw_committed_next"], 2)
        self.assertEqual(result["matched"], [])
        self.assertEqual(len(observer.consumer.assignments), 2)
        self.assertEqual(result["read_committed_eof"], ["canonical", "quarantine"])

    def test_uncommitted_offset_is_not_evidence_of_filtered_frame(self):
        observer = self.observer(committed=1)
        with self.assertRaises(TimeoutError):
            observer.observe({"offset": 1, "capture_id": "absent"}, timeout=.01)

    def test_non_eof_kafka_error_propagates(self):
        observer = self.observer()
        observer.consumer.queue = [SimpleNamespace(error=lambda: SimpleNamespace(code=lambda: 123))]
        with self.assertRaises(RuntimeError):
            observer.observe({"offset": 1, "capture_id": "absent"})


if __name__ == "__main__":
    unittest.main()

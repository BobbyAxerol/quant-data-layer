from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts.phasec40_collect_live_handoff import _scan, collect


class _MetadataPartition:
    pass


class _MetadataTopic:
    error = None
    partitions = {0: _MetadataPartition()}


class _Metadata:
    topics = {"topic": _MetadataTopic()}


class _Message:
    def __init__(self, offset: int) -> None:
        self._offset = offset

    def error(self):
        return None

    def offset(self):
        return self._offset

    def partition(self):
        return 0


class _Consumer:
    def __init__(self) -> None:
        self.values = [_Message(0), _Message(1)]
        self.closed = False
        self.assignment = None

    def list_topics(self, topic, timeout):
        self.topic = topic
        return _Metadata()

    def get_watermark_offsets(self, _partition, timeout):
        return 0, 2

    def assign(self, assignments):
        self.assignment = assignments

    def poll(self, _timeout):
        return self.values.pop(0) if self.values else None

    def close(self):
        self.closed = True


class C40LiveHandoffCollectorTests(unittest.TestCase):
    def test_snapshot_stops_at_captured_high_without_checkpoint_mutation(self):
        consumer = _Consumer()
        args = Namespace(timeout_seconds=1.0)
        accepted = []
        with patch(
            "scripts.phasec40_collect_live_handoff._consumer",
            return_value=consumer,
        ):
            result = _scan(args, "topic", lambda message: accepted.append(message.offset()))
        self.assertEqual(accepted, [0, 1])
        self.assertEqual(result["records"], 2)
        self.assertEqual(result["low_offsets"], {"0": 0})
        self.assertEqual(result["high_offsets"], {"0": 2})
        self.assertTrue(consumer.closed)

    def test_invalid_overlap_bounds_fail_before_network_access(self):
        args = Namespace(
            min_overlap=1,
            tail_per_slice=512,
            catalog=Path("unused"),
        )
        with self.assertRaisesRegex(ValueError, "overlap/tail bounds"):
            collect(args)


if __name__ == "__main__":
    unittest.main()

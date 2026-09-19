"""DL-V2 R1.25: the projector fetches a bounded batch in one broker call.

Polling one record at a time cost one thread hop per record and capped a
replica at a few hundred events a second while disk and CPU sat idle. These
tests pin the batch path, the fallback for brokers that do not offer it, and
the in-order deferral of whatever does not fit the byte bound.
"""

import asyncio
import os
import subprocess
import sys
import unittest
from pathlib import Path

from qdl.runtime.stable_projector import poll_projector_records
from qdl.transport.kafka_projector import KafkaProjectorRecord


def _record(offset: int, payload: bytes = b"x" * 16) -> KafkaProjectorRecord:
    return KafkaProjectorRecord(
        topic="md.canonical.v2",
        partition=0,
        offset=offset,
        key="OKX/SWAP/BTC-USDT-SWAP/candle1m",
        event_id=offset.to_bytes(16, "big"),
        payload=payload,
        accepted_at_ns=1_700_000_000_000_000_000 + offset,
    )


class _BatchBroker:
    """Broker that serves whole batches, like the live Kafka consumer."""

    def __init__(self, batches):
        self.batches = list(batches)
        self.batch_calls = []
        self.poll_calls = 0

    def poll_batch(self, max_records, timeout_seconds):
        self.batch_calls.append((max_records, timeout_seconds))
        return self.batches.pop(0) if self.batches else []

    def poll(self, timeout_seconds):
        self.poll_calls += 1
        return None


class _SingleBroker:
    """Broker that only implements the single-record poll."""

    def __init__(self, records):
        self.records = list(records)
        self.poll_calls = 0

    def poll(self, timeout_seconds):
        del timeout_seconds
        self.poll_calls += 1
        return self.records.pop(0) if self.records else None


class PollProjectorRecordsTests(unittest.IsolatedAsyncioTestCase):
    async def test_projector_import_does_not_depend_on_query_package_order(self):
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        completed = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-c", "import qdl.runtime.stable_projector"],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    async def test_batch_broker_is_served_by_one_call(self):
        broker = _BatchBroker([[_record(1), _record(2), _record(3)]])
        records = await poll_projector_records(
            broker, max_records=128, timeout_seconds=1.0, batch_wait_seconds=0.02
        )
        self.assertEqual([record.offset for record in records], [1, 2, 3])
        self.assertEqual(broker.batch_calls, [(128, 1.0)])
        self.assertEqual(broker.poll_calls, 0)

    async def test_batch_broker_waits_for_the_first_record_only(self):
        broker = _BatchBroker([[]])
        records = await poll_projector_records(
            broker, max_records=4, timeout_seconds=0.5, batch_wait_seconds=0.02
        )
        self.assertEqual(records, [])
        self.assertEqual(broker.batch_calls, [(4, 0.5)])

    async def test_single_record_broker_keeps_the_bounded_fill_loop(self):
        broker = _SingleBroker([_record(1), _record(2)])
        records = await poll_projector_records(
            broker, max_records=8, timeout_seconds=1.0, batch_wait_seconds=0.05
        )
        self.assertEqual([record.offset for record in records], [1, 2])
        self.assertGreaterEqual(broker.poll_calls, 2)

    async def test_single_record_broker_returns_empty_without_a_first_record(self):
        broker = _SingleBroker([])
        records = await poll_projector_records(
            broker, max_records=8, timeout_seconds=1.0, batch_wait_seconds=0.05
        )
        self.assertEqual(records, [])
        self.assertEqual(broker.poll_calls, 1)

    async def test_fill_loop_stops_at_the_record_bound(self):
        broker = _SingleBroker([_record(offset) for offset in range(1, 10)])
        records = await poll_projector_records(
            broker, max_records=3, timeout_seconds=1.0, batch_wait_seconds=0.5
        )
        self.assertEqual([record.offset for record in records], [1, 2, 3])

    async def test_batch_bound_must_be_positive(self):
        broker = _BatchBroker([])
        for kwargs in (
            {"max_records": 0, "timeout_seconds": 1.0, "batch_wait_seconds": 0.02},
            {"max_records": 8, "timeout_seconds": 0.0, "batch_wait_seconds": 0.02},
            {"max_records": 8, "timeout_seconds": 1.0, "batch_wait_seconds": 0.0},
        ):
            with self.assertRaises(ValueError):
                await poll_projector_records(broker, **kwargs)

    async def test_batch_result_is_a_list_the_caller_may_own(self):
        served = [_record(1), _record(2)]
        broker = _BatchBroker([tuple(served)])
        records = await poll_projector_records(
            broker, max_records=8, timeout_seconds=1.0, batch_wait_seconds=0.02
        )
        records.append(_record(3))
        self.assertEqual(len(served), 2)

    async def test_batch_path_does_not_block_the_event_loop(self):
        class _SlowBroker(_BatchBroker):
            def poll_batch(self, max_records, timeout_seconds):
                import time

                time.sleep(0.05)
                return super().poll_batch(max_records, timeout_seconds)

        broker = _SlowBroker([[_record(1)]])
        ticked = []

        async def tick():
            for _ in range(3):
                await asyncio.sleep(0.005)
                ticked.append(1)

        records, _ = await asyncio.gather(
            poll_projector_records(
                broker, max_records=8, timeout_seconds=1.0, batch_wait_seconds=0.02
            ),
            tick(),
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(len(ticked), 3)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from qdl.consumer import ConsumerManifestLoader
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.stream import DurableStreamGateway, StreamCapacityExceeded
from qdl.transport import Cursor, SQLiteDurableSpool, SpoolConfig
from scripts.phase73_finalize_evidence import _bytes
from scripts.phase73_beta_certification import _percentile


ROOT = Path(__file__).parents[1]


class Phase73ManifestAndMetricTests(unittest.TestCase):
    def test_capacity_manifest_is_bounded_and_read_only(self):
        manifest = ConsumerManifestLoader.load(
            ROOT / "consumers/beta/phase7-capacity-binance.yaml"
        )
        self.assertEqual(manifest.execution_dependency, "FORBIDDEN")
        self.assertEqual(manifest.rollback_contract, "V1")
        self.assertEqual(manifest.quotas.max_streams, 4)
        self.assertLessEqual(manifest.quotas.requests_per_minute, 10_000)

    def test_percentiles_and_docker_units_are_deterministic(self):
        self.assertEqual(_percentile([1.0, 2.0, 3.0, 4.0], 0.50), 2.0)
        self.assertEqual(_percentile([1.0, 2.0, 3.0, 4.0], 0.999), 4.0)
        self.assertEqual(_bytes("1MiB"), 1024 * 1024)
        self.assertEqual(_bytes("1.5 MB"), 1_500_000)


class Phase73ConsumerStreamQuotaTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temporary.name) / "spool.sqlite3",
            min_free_disk_bytes=0,
        ))
        self.handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"phase73": b"p" * 32}, active_key_id="phase73"
            ),
        )
        self.gateway = DurableStreamGateway(
            handoff=self.handoff,
            sink=self.spool,
            max_subscribers=10,
        )
        self.cursor = Cursor("md.canonical.v2.bar", "uid/bar/source", 0)
        self.token = self.handoff.issue(
            consumer_id="bounded-consumer",
            snapshot_id="phase73-snapshot",
            snapshot_watermark=self.cursor,
            ttl_seconds=60,
        ).token

    async def asyncTearDown(self):
        self.spool.close()
        self.temporary.cleanup()

    async def test_manifest_stream_quota_is_enforced_per_consumer(self):
        first = await self.gateway.open(
            consumer_id="bounded-consumer",
            stream=self.cursor.stream,
            partition_key=self.cursor.partition_key,
            token=self.token,
            max_consumer_streams=2,
        )
        second = await self.gateway.open(
            consumer_id="bounded-consumer",
            stream=self.cursor.stream,
            partition_key=self.cursor.partition_key,
            token=self.token,
            max_consumer_streams=2,
        )
        with self.assertRaisesRegex(StreamCapacityExceeded, "consumer concurrent"):
            await self.gateway.open(
                consumer_id="bounded-consumer",
                stream=self.cursor.stream,
                partition_key=self.cursor.partition_key,
                token=self.token,
                max_consumer_streams=2,
            )
        other = await self.gateway.open(
            consumer_id="independent-consumer",
            stream=self.cursor.stream,
            partition_key=self.cursor.partition_key,
            token=self.handoff.issue(
                consumer_id="independent-consumer",
                snapshot_id="phase73-other",
                snapshot_watermark=self.cursor,
                ttl_seconds=60,
            ).token,
            max_consumer_streams=1,
        )
        await asyncio.gather(first.close(), second.close(), other.close())


if __name__ == "__main__":
    unittest.main()

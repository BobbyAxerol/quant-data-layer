from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import redis

from qdl.canonical.trade import (
    TradeContext,
    canonical_event,
    canonicalize_binance_usdm_trade,
    raw_trade_event,
)
from qdl.projection import RedisProjectionTarget, TradeProjector
from qdl.transport import SQLiteDurableSpool, SpoolConfig


REDIS_URL = os.getenv("QDL_PHASE2_REDIS_URL")


@unittest.skipUnless(REDIS_URL, "set QDL_PHASE2_REDIS_URL for isolated Redis integration")
class RedisReplayIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.redis = redis.Redis.from_url(REDIS_URL, decode_responses=False)
        self.redis.flushdb()
        self.temp = tempfile.TemporaryDirectory()
        self.spool = SQLiteDurableSpool(
            SpoolConfig(
                path=Path(self.temp.name) / "redis-replay.sqlite3",
                max_records=100,
                max_payload_bytes=1_000_000,
                max_event_bytes=100_000,
                min_free_disk_bytes=0,
            )
        )

    def tearDown(self):
        self.redis.flushdb()
        self.spool.close()
        self.temp.cleanup()

    def test_flush_replay_rebuilds_identical_latest_and_legacy_projection(self):
        fixture = json.loads(
            (
                Path(__file__).parent
                / "fixtures/phase2/binance_usdm_trade.json"
            ).read_text()
        )
        context = TradeContext(**fixture["context"])
        raw = raw_trade_event(
            fixture["raw"], context=context, accepted_at_ns=context.received_at_ns
        )
        self.spool.append(raw)
        envelope = canonicalize_binance_usdm_trade(fixture["raw"], context)
        canonical = canonical_event(
            envelope,
            accepted_at_ns=context.normalized_at_ns,
            raw_event=raw,
        )
        canonical_result = self.spool.append(canonical)
        stored = self.spool.read(
            stream=canonical.stream,
            partition_key=canonical_result.cursor.partition_key,
        )[0]
        target = RedisProjectionTarget(self.redis)
        projector = TradeProjector(
            target,
            raw_resolver=lambda stream, event_id: (
                found.event.payload
                if (found := self.spool.find_event(stream=stream, event_id=event_id))
                else None
            ),
        )
        self.assertTrue(projector.project(stored))
        expected_checksum = target.checksum()
        self.assertFalse(projector.project(stored))
        self.redis.flushdb()
        self.assertTrue(projector.project(stored))
        self.assertEqual(target.checksum(), expected_checksum)
        legacy = self.redis.get(
            "shadow:qdl:v2:legacy:trade:price:binance_usdm:BTCUSDT"
        )
        self.assertEqual(json.loads(legacy)["raw"], fixture["raw"])


if __name__ == "__main__":
    unittest.main()

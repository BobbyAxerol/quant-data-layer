from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.canonical.market import canonicalize_binance_usdm_bar
from qdl.canonical.trade import TradeContext, canonical_event, raw_market_event
from qdl.pipeline.shadow import ShadowCanonicalPipeline
from qdl.projection.market import MarketProjector
from qdl.projection.trade import InMemoryProjectionTarget
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig


class MarketProjectionTests(unittest.TestCase):
    def test_bar_projection_is_raw_referenced_idempotent_and_v1_shaped(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "phase3.sqlite", max_records=20,
                max_payload_bytes=100_000, max_event_bytes=20_000,
                min_free_disk_bytes=0,
            ))
            context = TradeContext(
                instrument_uid="uid", instrument_id="BINANCE.USDM.PERPETUAL.BTCUSDT",
                instrument_revision=1, venue="BINANCE", market="USDM",
                product_type="PERPETUAL", native_symbol="BTCUSDT",
                provider="BINANCE_DIRECT", source_id="shard-1", lease_epoch=2,
                received_at_ns=10, normalized_at_ns=11, published_at_ns=12,
                partition_sequence=1, normalizer_version="qdl/2",
                adapter_version="binance/1", config_revision=1,
            )
            raw = {"e": "kline", "E": 2000, "s": "BTCUSDT", "k": {
                "t": 1000, "T": 1999, "s": "BTCUSDT", "i": "1m",
                "o": "10", "h": "12", "l": "9", "c": "11", "v": "5",
                "n": 2, "L": 4, "x": True,
            }}
            raw_event = raw_market_event(raw, context=context, feed_type="bar", accepted_at_ns=10)
            pipeline = ShadowCanonicalPipeline(
                spool, consumer_id="bar-normalizer",
                canonicalizer=lambda event: canonical_event(
                    canonicalize_binance_usdm_bar(json.loads(event.payload), context),
                    accepted_at_ns=11, raw_event=event,
                ),
            )
            _, result = pipeline.accept(raw_event)
            stored = spool.read(stream="md.canonical.v2.bar", partition_key=result.cursor.partition_key)[0]
            target = InMemoryProjectionTarget()
            projector = MarketProjector(
                target,
                raw_resolver=lambda stream, event_id: (
                    found.event.payload
                    if (found := spool.find_event(stream=stream, event_id=event_id))
                    else None
                ),
            )
            self.assertTrue(projector.project(stored))
            self.assertFalse(projector.project(stored))
            legacy = next(value for key, value in target.latest.items() if ":legacy:kline:1m:" in key)
            self.assertEqual(json.loads(legacy), raw)
            spool.close()


if __name__ == "__main__":
    unittest.main()

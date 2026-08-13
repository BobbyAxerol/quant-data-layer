from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.canonical.trade import (
    TradeContext,
    canonical_event,
    canonicalize_binance_usdm_trade,
    canonicalize_okx_trade,
    raw_trade_event,
)
from qdl.pipeline import ShadowCanonicalPipeline
from qdl.projection import InMemoryProjectionTarget, TradeProjector
from qdl.transport import SQLiteDurableSpool, SpoolConfig


BINANCE_RAW = {
    "E": 1_786_352_400_124,
    "T": 1_786_352_400_123,
    "a": 9_876_543_210_123_456_789,
    "e": "aggTrade",
    "m": False,
    "p": "61234.10",
    "q": "0.125",
    "s": "BTCUSDT",
}


def context(*, venue: str = "BINANCE") -> TradeContext:
    if venue == "OKX":
        return TradeContext(
            instrument_uid="23533297-11a8-5290-a800-c7f584706cd6",
            instrument_id="OKX.SWAP.PERPETUAL.BTC-USDT",
            instrument_revision=2,
            venue="OKX",
            market="SWAP",
            product_type="PERPETUAL",
            native_symbol="BTC-USDT-SWAP",
            provider="OKX_DIRECT",
            source_id="okx-public-trades-001",
            lease_epoch=7,
            received_at_ns=1_786_352_400_124_000_000,
            normalized_at_ns=1_786_352_400_124_100_000,
            published_at_ns=1_786_352_400_124_200_000,
            partition_sequence=88,
            normalizer_version="qdl-normalizer/2.0.0",
            adapter_version="okx-json/1.0.0",
            config_revision=11,
            correlation_id="phase2-okx-trade",
        )
    return TradeContext(
        instrument_uid="85ad7cb6-7ebf-5c81-9d82-12c4c10ca85c",
        instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
        instrument_revision=7,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        provider="BINANCE_DIRECT",
        source_id="binance-usdm-trade-shadow-001",
        lease_epoch=42,
        received_at_ns=1_786_352_400_123_456_000,
        normalized_at_ns=1_786_352_400_123_500_000,
        published_at_ns=1_786_352_400_123_700_000,
        partition_sequence=1234,
        normalizer_version="qdl-normalizer/2.0.0",
        adapter_version="binance-json/1.0.0",
        config_revision=9,
        correlation_id="phase2-binance-trade",
    )


class CanonicalTradeTests(unittest.TestCase):
    def test_binance_exact_fields_and_repeat_replay_bytes(self):
        first = canonicalize_binance_usdm_trade(BINANCE_RAW, context())
        second = canonicalize_binance_usdm_trade(dict(BINANCE_RAW), context())
        self.assertEqual(first.SerializeToString(deterministic=True), second.SerializeToString(deterministic=True))
        self.assertEqual(first.trade.price.source_text, "61234.10")
        self.assertEqual(first.trade.price.mantissa, 6_123_410)
        self.assertEqual(first.trade.quantity.mantissa, 125)
        self.assertEqual(first.source_event_time_ns, 1_786_352_400_123_000_000)

    def test_okx_trade_uses_provider_aggressor_side_and_exact_inst_id(self):
        raw = {
            "instId": "BTC-USDT-SWAP",
            "px": "61235.20",
            "side": "sell",
            "sz": "1.50",
            "tradeId": "998877665544332211",
            "ts": "1786352400125",
        }
        envelope = canonicalize_okx_trade(raw, context(venue="OKX"))
        self.assertEqual(envelope.native_symbol, "BTC-USDT-SWAP")
        self.assertEqual(envelope.trade.price.source_text, "61235.20")
        self.assertEqual(envelope.trade.aggressor_side, 2)


class ShadowPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "shadow.sqlite3"
        self.spool = SQLiteDurableSpool(
            SpoolConfig(
                path=self.path,
                max_records=100,
                max_payload_bytes=1_000_000,
                max_event_bytes=100_000,
                min_free_disk_bytes=0,
            )
        )
        self.context = context()

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def canonicalizer(self, raw_event):
        raw = json.loads(raw_event.payload)
        envelope = canonicalize_binance_usdm_trade(raw, self.context)
        return canonical_event(envelope, accepted_at_ns=self.context.normalized_at_ns)

    def test_crash_after_raw_commit_is_recovered_without_duplicate(self):
        raw = raw_trade_event(
            BINANCE_RAW,
            context=self.context,
            accepted_at_ns=self.context.received_at_ns,
        )
        raw_result = self.spool.append(raw)
        pipeline = ShadowCanonicalPipeline(
            self.spool,
            consumer_id="canonicalizer-v1",
            canonicalizer=self.canonicalizer,
        )
        self.assertEqual(
            pipeline.drain(stream=raw.stream, partition_key=raw.partition_key), 1
        )
        self.assertEqual(
            pipeline.drain(stream=raw.stream, partition_key=raw.partition_key), 0
        )
        checkpoint = self.spool.get_checkpoint(
            consumer_id="canonicalizer-v1",
            stream=raw.stream,
            partition_key=raw.partition_key,
        )
        self.assertEqual(checkpoint, raw_result.cursor)

    def test_replay_rebuilds_identical_canonical_and_legacy_state(self):
        raw = raw_trade_event(
            BINANCE_RAW,
            context=self.context,
            accepted_at_ns=self.context.received_at_ns,
        )
        pipeline = ShadowCanonicalPipeline(
            self.spool,
            consumer_id="canonicalizer-v1",
            canonicalizer=self.canonicalizer,
        )
        _, canonical_result = pipeline.accept(raw)
        rows = self.spool.read(
            stream="md.canonical.v2.trade",
            partition_key=canonical_result.cursor.partition_key,
        )
        target = InMemoryProjectionTarget()
        projector = TradeProjector(target)
        self.assertTrue(projector.project(rows[0]))
        first_checksum = target.checksum()
        self.assertFalse(projector.project(rows[0]))
        target.clear()
        self.assertTrue(projector.project(rows[0]))
        self.assertEqual(target.checksum(), first_checksum)
        legacy = next(value for key, value in target.latest.items() if ":legacy:trade:price:binance_usdm:" in key)
        parsed = json.loads(legacy)
        self.assertEqual(parsed["price"], 61234.1)
        self.assertEqual(parsed["raw"]["p"], "61234.10")


if __name__ == "__main__":
    unittest.main()

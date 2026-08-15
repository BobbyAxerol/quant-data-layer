from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

from qdl.certification import AdapterEvidence, certify_adapter
from qdl.data_quality.engine import FeedKey, FeedQualityLedger, FeedQualityState, Observation
from qdl.domain.capabilities import (
    binance_usdm_capabilities,
    dnse_capabilities,
    okx_global_capabilities,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    OptionType,
    ProductType,
)
from qdl.history import AtomicParquetCatalog, BarRecord, LocalObjectStore
from qdl.ingestion.fencing import FencingGate, InMemoryLeaseStore
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.simulator import DeterministicVenueSimulator, Scenario
from qdl.simulator.okx import BookState, OkxBookSimulator
from qdl.stream import DurableStreamGateway, SlowConsumer
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl.transport.publisher import DurablePublisher


ROOT = Path(__file__).resolve().parents[1]


def _event(index: int) -> DurableEvent:
    return DurableEvent(
        "md.canonical.v2.trade",
        "instrument/trade/source",
        index.to_bytes(16, "big"),
        json.dumps({"sequence": index}, separators=(",", ":")).encode(),
        time.time_ns(),
    )


def _evidence(**overrides) -> AdapterEvidence:
    values = {name: True for name in AdapterEvidence.__dataclass_fields__}
    values.update(overrides)
    return AdapterEvidence(**values)


class _TransientSink:
    def __init__(self, delegate, failures: int):
        self.delegate = delegate
        self.failures = failures

    def append(self, event):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("injected broker partition")
        return self.delegate.append(event)


class DurableRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_restart_preserves_every_acknowledged_event(self):
        with tempfile.TemporaryDirectory() as directory:
            config = SpoolConfig(
                path=Path(directory) / "durable.sqlite3",
                min_free_disk_bytes=0,
                maintenance_interval_seconds=3600,
            )
            first = SQLiteDurableSpool(config)
            for index in range(1, 6):
                first.append(_event(index))
            first.close()

            recovered = SQLiteDurableSpool(config)
            records = recovered.read(
                stream="md.canonical.v2.trade",
                partition_key="instrument/trade/source",
                limit=10,
            )
            self.assertEqual([item.cursor.offset for item in records], [1, 2, 3, 4, 5])
            self.assertEqual([json.loads(item.event.payload)["sequence"] for item in records], [1, 2, 3, 4, 5])
            recovered.close()

    async def test_transient_broker_partition_retries_before_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "publisher.sqlite3", min_free_disk_bytes=0,
            ))
            sleeps = []
            publisher = DurablePublisher(
                _TransientSink(spool, failures=2), max_attempts=3, sleep=sleeps.append
            )
            result = publisher.publish(_event(1))
            self.assertEqual(result.cursor.offset, 1)
            self.assertEqual(len(sleeps), 2)
            self.assertEqual(spool.stats().records, 1)
            spool.close()

    async def test_slow_consumer_isolated_and_can_replay_from_durable_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "gateway.sqlite3", min_free_disk_bytes=0,
            ))
            codec = SignedHandoffCursorCodec({"active": b"x" * 32}, active_key_id="active")
            handoff = GapFreeHandoff(spool, codec)
            watermark = handoff.capture_watermark(
                stream="md.canonical.v2.trade", partition_key="instrument/trade/source"
            )
            grant = handoff.issue(
                consumer_id="slow-alpha",
                snapshot_id="snapshot-1",
                snapshot_watermark=watermark,
                ttl_seconds=3600,
            )
            gateway = DurableStreamGateway(
                handoff=handoff, sink=spool, max_buffer_events=1
            )
            subscription = await gateway.open(
                consumer_id="slow-alpha",
                stream=watermark.stream,
                partition_key=watermark.partition_key,
                token=grant.token,
                max_buffer_events=1,
            )
            await gateway.publish(_event(1))
            await gateway.publish(_event(2))
            with self.assertRaises(SlowConsumer):
                await subscription.next_live()
            replayed = handoff.replay(
                token=grant.token,
                consumer_id="slow-alpha",
                stream=watermark.stream,
                partition_key=watermark.partition_key,
            )
            self.assertEqual([item.cursor.offset for item in replayed], [1, 2])
            await subscription.close()
            spool.close()


class HistoricalAndOwnershipChaosTests(unittest.TestCase):
    @staticmethod
    def bar() -> BarRecord:
        return BarRecord(
            instrument_uid="instrument-1",
            instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
            instrument_revision=1,
            source_id="binance.usdm.public",
            source_role="PRIMARY",
            interval="1m",
            open_time_ns=1_000_000_000,
            close_time_ns=61_000_000_000,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="10",
            trade_count=5,
            is_final=True,
            revision=1,
            origin="NATIVE",
            normalizer_version="phase6",
            config_revision=1,
            raw_lineage_json='{"event_id":"1"}',
        )

    def test_object_store_crash_never_advances_snapshot_head(self):
        with tempfile.TemporaryDirectory() as directory:
            catalog = AtomicParquetCatalog(LocalObjectStore(Path(directory)))
            with self.assertRaisesRegex(RuntimeError, "after immutable"):
                catalog.commit(
                    "bars", [self.bar()],
                    source_cursor_start="1", source_cursor_end="1",
                    normalizer_version="phase6", config_revision=1,
                    expected_parent_snapshot_id=None, crash_at="after_data",
                )
            self.assertIsNone(catalog.current("bars"))
            self.assertEqual(len(catalog.orphan_objects("bars")), 1)
            self.assertEqual(catalog.purge_orphans("bars", confirm_dataset_id="bars"), 1)

    def test_expired_owner_is_fenced_after_failover(self):
        now = [1_000_000_000]
        leases = InMemoryLeaseStore(clock_ns=lambda: now[0])
        old = leases.acquire("binance-usdm-0", "owner-a", 1)
        self.assertIsNotNone(old)
        now[0] += 2_000_000_000
        new = leases.acquire("binance-usdm-0", "owner-b", 1)
        self.assertGreater(new.epoch, old.epoch)
        gate = FencingGate()
        self.assertTrue(gate.permits(new.shard_id, new.epoch))
        self.assertFalse(gate.permits(old.shard_id, old.epoch))


class VenueAndQualityChaosTests(unittest.TestCase):
    def test_simulated_gap_duplicate_malformed_and_burst_are_deterministic(self):
        simulator = DeterministicVenueSimulator({"price": "100"})
        self.assertEqual([item.sequence for item in simulator.frames(Scenario.GAP)], [1, 3])
        self.assertEqual([item.sequence for item in simulator.frames(Scenario.DUPLICATE)], [1, 1, 2, 3])
        self.assertEqual(len(simulator.frames(Scenario.BURST, count=10)), 100)
        self.assertIsInstance(simulator.frames(Scenario.MALFORMED)[0].payload, bytes)

    def test_quality_ledger_blocks_gap_and_recovers_after_snapshot(self):
        key = FeedKey("binance.usdm.public", "instrument", "trade")
        ledger = FeedQualityLedger()
        first = ledger.observe(Observation(key, b"a" * 16, 100, 100, 1))
        self.assertTrue(first.executable)
        gapped = ledger.observe(Observation(key, b"b" * 16, 102, 102, 3))
        self.assertEqual(gapped.state, FeedQualityState.GAPPED)
        self.assertFalse(gapped.executable)
        ledger.begin_resync(key)
        ledger.complete_resync(key, snapshot_sequence=3, source_time_ns=102, completed_at_ns=103)
        self.assertEqual(ledger.snapshot(key)["state"], "LIVE")

    def test_okx_make_before_break_generation_discards_old_socket(self):
        simulator = OkxBookSimulator()
        simulator.apply({"kind": "connect", "generation": 1})
        simulator.apply({"kind": "book", "generation": 1, "action": "snapshot", "seq_id": 10})
        self.assertEqual(simulator.state, BookState.LIVE)
        simulator.apply({"kind": "maintenance", "generation": 1})
        simulator.apply({"kind": "connect", "generation": 2})
        simulator.apply({"kind": "book", "generation": 2, "action": "snapshot", "seq_id": 20})
        old = simulator.apply({"kind": "book", "generation": 1, "action": "update", "prev_seq_id": 10, "seq_id": 11})
        self.assertFalse(old.accepted)
        self.assertEqual(old.state, BookState.LIVE)


class MultiVenueCertificationTests(unittest.TestCase):
    def test_binance_core_can_pass_while_okx_tier_product_stays_blocked(self):
        binance = certify_adapter(
            binance_usdm_capabilities(), feed="trade", evidence=_evidence(),
            evidence_prefix="phase6/binance-usdm-trade",
        )
        self.assertTrue(binance.report.production_eligible)
        okx_json = certify_adapter(
            okx_global_capabilities("SWAP"), feed="trade", evidence=_evidence(),
            evidence_prefix="phase6/okx-swap-json-trade",
        )
        self.assertTrue(okx_json.report.production_eligible)
        okx_sbe = certify_adapter(
            okx_global_capabilities("SWAP"), feed="sbe_trade", evidence=_evidence(),
            evidence_prefix="phase6/okx-sbe-not-entitled",
        )
        self.assertFalse(okx_sbe.report.production_eligible)
        self.assertEqual(okx_sbe.report.blockers[0].gate_id, "capability")

    def test_dnse_unsupported_l2_does_not_invalidate_bar_capability(self):
        dnse_bar = certify_adapter(
            dnse_capabilities(), feed="bar", evidence=_evidence(),
            evidence_prefix="phase6/dnse-bar",
        )
        dnse_l2 = certify_adapter(
            dnse_capabilities(), feed="l2", evidence=_evidence(),
            evidence_prefix="phase6/dnse-l2-not-supported",
        )
        self.assertTrue(dnse_bar.report.production_eligible)
        self.assertFalse(dnse_l2.report.production_eligible)

    def test_option_identity_has_no_crypto_spot_assumption(self):
        fixture = json.loads(
            (ROOT / "tests/fixtures/phase3/deribit_option_book.json").read_text()
        )
        identity = InstrumentIdentity.create(
            venue=fixture["venue"], market=fixture["market"],
            product_type=ProductType.OPTION,
            canonical_symbol="BTC-USD-20260630-60000-C",
        )
        record = InstrumentRecord(
            identity=identity,
            metadata_revision=1,
            asset_class=AssetClass.OPTION,
            native_symbol=fixture["native_symbol"],
            base_asset="BTC",
            quote_asset="USD",
            settlement_asset="BTC",
            price_tick=CanonicalDecimal.from_text("0.0001"),
            quantity_step=CanonicalDecimal.from_text("0.1"),
            contract_multiplier=CanonicalDecimal.from_text("1"),
            session_calendar_id="CRYPTO_24X7",
            expiry_time_ns=1_782_777_600_000_000_000,
            strike_price=CanonicalDecimal.from_text("60000"),
            option_type=OptionType.CALL,
            underlying_instrument_uid="underlying-btc-index",
        )
        self.assertEqual(record.identity.product_type, ProductType.OPTION)
        self.assertEqual(record.option_type, OptionType.CALL)
        self.assertEqual(str(record.strike_price.as_decimal()), "60000")
        self.assertEqual(fixture["prev_change_id"], "1000")
        self.assertEqual(fixture["change_id"], "1001")
        self.assertEqual(fixture["provenance"], "TEST_SYNTHETIC_EXTENSION_FIXTURE")


if __name__ == "__main__":
    unittest.main()

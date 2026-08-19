from __future__ import annotations

import hashlib
import json
import os
import tempfile
from urllib.parse import urlsplit
import unittest
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml
from fastapi import FastAPI

from qdl.canonical.market import (
    canonicalize_binance_usdm_bbo,
    canonicalize_binance_usdm_rest_bar,
    canonicalize_dnse_bar,
    canonicalize_okx_bbo,
)
from qdl.canonical.trade import (
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_dnse_trade,
)
from qdl.common.v1 import common_pb2
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.query import ConsumerGrade, DataRequirement, FeedType
from qdl.projection.stable import (
    InMemoryStableProjectionTarget,
    ProjectionFenced,
    RedisStableProjectionTarget,
    StableCompatibilityProjector,
    StableProjectionItem,
    StableProjectionRecord,
)
from qdl.raw.capture import bind_capture_context, capture_exact_frame
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable import StableRuntimeConfig
from qdl.runtime.stable_ingest import StableHttpCanonicalSink, install_stable_canonical_ingest
from qdl.runtime.stable_projector import LocalStableCanonicalSink, StableProjectorEngine
from qdl.runtime.stable_source import (
    StableConsumerCursorIssuer,
    StableSpoolQueryBackend,
)
from qdl.stream import DurableStreamGateway
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl.transport.kafka_projector import KafkaProjectorRecord


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
FIXTURE_PATH = ROOT / "tests/fixtures/phase2"


def _canonicalizer(name: str):
    if "trade" in name:
        return canonicalize_dnse_trade if name.startswith("dnse") else canonicalize_binance_usdm_trade
    if "bbo" in name:
        return canonicalize_okx_bbo if name.startswith("okx") else canonicalize_binance_usdm_bbo
    if name.startswith("dnse"):
        return canonicalize_dnse_bar
    return canonicalize_binance_usdm_rest_bar


def _stable_event(catalog: StableSourceCatalog, fixture_name: str, binding_id: str):
    fixture = json.loads((FIXTURE_PATH / fixture_name).read_text())
    binding = next(item for item in catalog.bindings if item.binding_id == binding_id)
    raw_bytes = json.dumps(
        fixture["raw"], sort_keys=True, separators=(",", ":")
    ).encode()
    context = dict(fixture["context"])
    context.update({
        "instrument_uid": binding.instrument.instrument_uid,
        "instrument_id": binding.instrument.instrument_id,
        "instrument_revision": binding.instrument.metadata_revision,
        "venue": binding.instrument.identity.venue,
        "market": binding.instrument.identity.market,
        "product_type": binding.instrument.identity.product_type.value,
        "native_symbol": binding.instrument.native_symbol,
        "provider": binding.provider,
        "source_id": binding.source_id,
        "source_role": binding.source_role,
        "adapter_version": binding.adapter_version,
        "normalizer_version": binding.normalizer_version,
        "authority_revision": catalog.authority_revision,
        "source_session_id": f"phase-b-{binding.binding_id}-session-1",
        "connection_generation": 1,
        "partition_plan_epoch": 1,
        "lease_epoch": 1,
        "partition_sequence": 1,
        "raw_capture_id": hashlib.sha256(raw_bytes).digest()[:16],
        "raw_frame_sha256": hashlib.sha256(raw_bytes).digest(),
    })
    context["normalized_at_ns"] = int(context["received_at_ns"]) + 1
    context["published_at_ns"] = int(context["received_at_ns"]) + 2
    return _canonicalizer(fixture_name)(fixture["raw"], TradeContext(**context))


def _stable_pair(catalog: StableSourceCatalog, fixture_name: str, binding_id: str):
    fixture = json.loads((FIXTURE_PATH / fixture_name).read_text())
    binding = next(item for item in catalog.bindings if item.binding_id == binding_id)
    raw_bytes = json.dumps(
        fixture["raw"], sort_keys=True, separators=(",", ":")
    ).encode()
    received_at_ns = int(fixture["context"]["received_at_ns"])
    raw = capture_exact_frame(
        provider=binding.provider,
        venue=binding.instrument.identity.venue,
        market=binding.instrument.identity.market,
        product_type=binding.instrument.identity.product_type.value,
        native_symbol=binding.instrument.native_symbol,
        native_channel=f"phase-b/{binding.feed.value.lower()}",
        subscription_id=binding.binding_id,
        source_session_id=f"phase-b-{binding.binding_id}-session-1",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=catalog.authority_revision,
        partition_plan_epoch=1,
        received_at_ns=received_at_ns,
        raw_frame_bytes=raw_bytes,
        adapter_version=binding.adapter_version,
        config_revision=1,
        instrument_catalog_revision=catalog.catalog_revision,
        correlation_id=f"phase-b-{binding.binding_id}",
        test_provenance=True,
    )
    context = dict(fixture["context"])
    context.update({
        "instrument_uid": binding.instrument.instrument_uid,
        "instrument_id": binding.instrument.instrument_id,
        "instrument_revision": binding.instrument.metadata_revision,
        "venue": binding.instrument.identity.venue,
        "market": binding.instrument.identity.market,
        "product_type": binding.instrument.identity.product_type.value,
        "native_symbol": binding.instrument.native_symbol,
        "provider": binding.provider,
        "source_id": binding.source_id,
        "source_role": binding.source_role,
        "adapter_version": binding.adapter_version,
        "normalizer_version": binding.normalizer_version,
        "lease_epoch": 1,
        "partition_sequence": 1,
        "normalized_at_ns": received_at_ns + 1,
        "published_at_ns": received_at_ns + 2,
    })
    event = _canonicalizer(fixture_name)(
        fixture["raw"], bind_capture_context(TradeContext(**context), raw)
    )
    return binding, raw, event


class _Broker:
    def __init__(self, *, fail_once_offset=None):
        self.checkpoints = []
        self.fail_once_offset = fail_once_offset

    def poll(self, timeout_seconds):
        del timeout_seconds
        return None

    def checkpoint(self, record):
        if self.fail_once_offset == (record.topic, record.partition, record.offset):
            self.fail_once_offset = None
            raise RuntimeError("injected checkpoint failure")
        self.checkpoints.append((record.topic, record.partition, record.offset))

    def close(self):
        return None


def _broker_records(binding, raw, event, *, raw_offset=0, canonical_offset=0):
    raw_topic = "qdl.stable.raw.phase-b.v1"
    canonical_topic = "qdl.stable.canonical.phase-b.v2"
    raw_record = KafkaProjectorRecord(
        topic=raw_topic, partition=0, offset=raw_offset,
        key=f"{raw.venue}/{raw.market}/{raw.native_symbol}/{raw.native_channel}",
        event_id=bytes(raw.capture_id),
        payload=raw.SerializeToString(deterministic=True),
        accepted_at_ns=raw.received_at_ns,
    )
    canonical_record = KafkaProjectorRecord(
        topic=canonical_topic, partition=0, offset=canonical_offset,
        key=binding.partition_key, event_id=bytes(event.event_id),
        payload=event.SerializeToString(deterministic=True),
        accepted_at_ns=event.received_at_ns,
    )
    return raw_topic, canonical_topic, raw_record, canonical_record


def _append(spool, catalog, event):
    binding = catalog.binding_for_envelope(event)
    durable = DurableEvent(
        stream=catalog.canonical_stream,
        partition_key=binding.partition_key,
        event_id=bytes(event.event_id),
        payload=event.SerializeToString(deterministic=True),
        accepted_at_ns=event.received_at_ns,
        headers={
            "raw_stream": "md.raw.v1.phase-b",
            "raw_event_id": event.raw_capture_id.hex(),
        },
    )
    return spool.append(durable)


def _requirement(binding, *, grade=ConsumerGrade.ALPHA, warmup=1):
    return DataRequirement(
        instrument_uid=binding.instrument.instrument_uid,
        feed=binding.feed,
        interval=binding.interval,
        consumer_grade=grade,
        source_policy_id=binding.source_policy_id,
        warmup_limit=warmup,
    )


class StableCatalogContractTests(unittest.TestCase):
    def test_catalog_covers_equal_source_baseline_with_deterministic_identity(self):
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.assertEqual(len(catalog.bindings), 16)
        self.assertEqual(
            {(item.instrument.identity.venue, item.feed.value) for item in catalog.bindings},
            {
                ("BINANCE", "TRADE"), ("BINANCE", "QUOTE"), ("BINANCE", "BAR"),
                ("OKX", "TRADE"), ("OKX", "QUOTE"), ("OKX", "BAR"),
                ("HNX", "TRADE"), ("HNX", "BAR"),
                ("HOSE", "TRADE"), ("HOSE", "BAR"),
            },
        )
        for binding in catalog.bindings:
            recreated = InstrumentIdentity.create(
                venue=binding.instrument.identity.venue,
                market=binding.instrument.identity.market,
                product_type=binding.instrument.identity.product_type,
                canonical_symbol=binding.instrument.identity.canonical_symbol,
            )
            self.assertEqual(recreated.instrument_uid, binding.instrument.instrument_uid)

    def test_catalog_fails_closed_on_uid_unknown_fields_and_incomplete_lineage(self):
        payload = yaml.safe_load(CATALOG_PATH.read_text())
        payload["instruments"][0]["instrument_uid"] = "not-deterministic"
        with tempfile.TemporaryDirectory() as temp:
            candidate = Path(temp) / "catalog.yaml"
            candidate.write_text(yaml.safe_dump(payload, sort_keys=False))
            with self.assertRaisesRegex(ValueError, "not deterministic"):
                StableSourceCatalog.load(candidate)
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        event = _stable_event(
            catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        event.raw_capture_id = b""
        with self.assertRaisesRegex(ValueError, "provenance is incomplete"):
            catalog.binding_for_envelope(event)

    def test_continuous_future_is_explicit_and_dated_future_still_requires_expiry(self):
        identity = InstrumentIdentity.create(
            venue="HNX", market="VN_DERIVATIVES",
            product_type=ProductType.FUTURE, canonical_symbol="VN30F1M",
        )
        continuous = InstrumentRecord(
            identity=identity, metadata_revision=1, asset_class=AssetClass.DERIVATIVE,
            native_symbol="VN30F1M", base_asset="VN30", quote_asset="VND",
            settlement_asset="VND", price_tick=CanonicalDecimal.from_text("0.1"),
            quantity_step=CanonicalDecimal.from_text("1"),
            contract_multiplier=CanonicalDecimal.from_text("100000"),
            session_calendar_id="VN_DERIVATIVES",
            attributes={"continuous_series": "true"},
        )
        self.assertEqual(continuous.attributes["continuous_series"], "true")
        with self.assertRaisesRegex(ValueError, "dated futures require"):
            InstrumentRecord(
                identity=identity, metadata_revision=1,
                asset_class=AssetClass.DERIVATIVE, native_symbol="VN30F2608",
                base_asset="VN30", quote_asset="VND", settlement_asset="VND",
                price_tick=CanonicalDecimal.from_text("0.1"),
                quantity_step=CanonicalDecimal.from_text("1"),
                contract_multiplier=CanonicalDecimal.from_text("100000"),
                session_calendar_id="VN_DERIVATIVES",
            )


class StableQueryContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "stable.sqlite3",
            max_records=1000,
            max_payload_bytes=8 * 1024 * 1024,
            max_storage_bytes=16 * 1024 * 1024,
            min_free_disk_bytes=0,
        ))

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def test_trade_quote_bar_units_and_quality_are_typed(self):
        cases = (
            ("binance_usdm_trade.json", "binance-usdm-btcusdt-trade", "BASE_ASSET"),
            ("okx_bbo.json", "okx-swap-btcusdt-quote", "CONTRACT"),
            ("dnse_derivative_bar.json", "dnse-vn30f1m-bar-1m", "CONTRACT"),
        )
        for fixture, binding_id, unit in cases:
            with self.subTest(fixture=fixture):
                binding = next(x for x in self.catalog.bindings if x.binding_id == binding_id)
                event = _stable_event(self.catalog, fixture, binding_id)
                _append(self.spool, self.catalog, event)
                observed = (
                    event.bar.close_time_ns
                    if event.WhichOneof("payload") == "bar"
                    else event.source_event_time_ns
                )
                backend = StableSpoolQueryBackend(
                    self.spool, self.catalog, schema_digest="a" * 64,
                    clock_ns=lambda observed=observed: observed + 1_000_000,
                )
                item = backend.latest(_requirement(binding))
                self.assertIsNotNone(item)
                self.assertEqual(item.payload.get("quantity_unit") or item.payload.get("volume_unit"), unit)
                self.assertEqual(item.contract.contract_version, "2.0.0")
                expected_state = "MARKET_CLOSED" if binding.instrument.identity.venue == "HNX" else "LIVE"
                self.assertEqual(item.quality.state, expected_state)
                self.assertEqual(item.quality.execution_eligible, expected_state == "LIVE")

    def test_crypto_gap_blocks_but_vn_sparse_minutes_are_not_fabricated(self):
        crypto = next(x for x in self.catalog.bindings if x.binding_id == "binance-usdm-btcusdt-bar-1m")
        first = _stable_event(self.catalog, "binance_usdm_rest_bar.json", crypto.binding_id)
        second = type(first)()
        second.CopyFrom(first)
        second.bar.open_time_ns += 120 * 1_000_000_000
        second.bar.close_time_ns += 120 * 1_000_000_000
        second.source_event_time_ns += 120 * 1_000_000_000
        second.source_sequence = "gap-second"
        second.event_id = hashlib.sha256(b"phase-b-gap-second").digest()[:16]
        _append(self.spool, self.catalog, first)
        _append(self.spool, self.catalog, second)
        backend = StableSpoolQueryBackend(
            self.spool, self.catalog, schema_digest="b" * 64,
            clock_ns=lambda: second.bar.close_time_ns + 1_000_000,
        )
        self.assertTrue(backend.latest(_requirement(crypto, warmup=2)).quality.gap_open)

        vn = next(x for x in self.catalog.bindings if x.binding_id == "dnse-vn30f1m-bar-1m")
        vn_event = _stable_event(self.catalog, "dnse_derivative_bar.json", vn.binding_id)
        _append(self.spool, self.catalog, vn_event)
        closed_ns = int(datetime(2026, 8, 22, 3, tzinfo=timezone.utc).timestamp() * 1e9)
        backend = StableSpoolQueryBackend(
            self.spool, self.catalog, schema_digest="c" * 64,
            clock_ns=lambda: closed_ns,
        )
        item = backend.latest(_requirement(vn))
        self.assertEqual(item.quality.state, "MARKET_CLOSED")
        self.assertFalse(item.quality.gap_open)
        self.assertFalse(item.quality.execution_eligible)

    def test_history_cursor_is_signed_and_bound_to_consumer(self):
        binding = next(x for x in self.catalog.bindings if x.binding_id == "binance-usdm-btcusdt-trade")
        event = _stable_event(self.catalog, "binance_usdm_trade.json", binding.binding_id)
        _append(self.spool, self.catalog, event)
        backend = StableSpoolQueryBackend(
            self.spool, self.catalog, schema_digest="d" * 64,
            clock_ns=lambda: event.source_event_time_ns + 1_000_000,
        )
        handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec({"stable-k1": b"s" * 32}, active_key_id="stable-k1"),
            checkpoint_ttl_seconds=3600,
        )
        issuer = StableConsumerCursorIssuer(handoff, self.catalog, ttl_seconds=3600)
        requirement = _requirement(binding)
        history = issuer.bind_history(
            requirement, backend.history(requirement), consumer_id="alpha-binance-paper",
        )
        replay = handoff.replay(
            token=history.stream_cursor,
            consumer_id="alpha-binance-paper",
            stream=binding.canonical_stream,
            partition_key=binding.partition_key,
            limit=10,
        )
        self.assertEqual(replay, [])
        with self.assertRaises(ValueError):
            handoff.replay(
                token=history.stream_cursor,
                consumer_id="different-consumer",
                stream=binding.canonical_stream,
                partition_key=binding.partition_key,
                limit=10,
            )


class StableProjectorRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "projector.sqlite3",
            max_records=1000,
            max_payload_bytes=8 * 1024 * 1024,
            max_storage_bytes=16 * 1024 * 1024,
            min_free_disk_bytes=0,
        ))
        handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec({"k": b"k" * 32}, active_key_id="k"),
            checkpoint_ttl_seconds=3600,
        )
        self.gateway = DurableStreamGateway(handoff=handoff, sink=self.spool)

    async def asyncTearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def engine(self, broker, target, raw_topic, canonical_topic):
        return StableProjectorEngine(
            broker=broker, spool=self.spool, catalog=self.catalog,
            canonical_topic=canonical_topic, raw_topics=(raw_topic,),
            sink=LocalStableCanonicalSink(self.gateway, self.spool),
            projector=StableCompatibilityProjector(self.catalog), target=target,
            max_pending_records=10, max_pending_bytes=1024 * 1024,
        )

    async def test_canonical_before_raw_waits_and_checkpoints_after_all_downstreams(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept(canonical_record)
        self.assertEqual(engine.stats.pending_canonical, 1)
        self.assertEqual(broker.checkpoints, [])
        await engine.accept(raw_record)
        self.assertEqual(engine.stats.pending_canonical, 0)
        self.assertEqual(
            broker.checkpoints,
            [(raw_topic, 0, 0), (canonical_topic, 0, 0)],
        )
        self.assertIn("trade:price:BTCUSDT", target.latest)
        self.assertIn("trade:price:binance_usdm:BTCUSDT", target.latest)
        self.assertEqual(len(target.publications), 2)

    async def test_projector_batches_durability_projection_and_checkpoints_in_order(self):
        first_binding, first_raw, first_event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        second_binding, second_raw, second_event = _stable_pair(
            self.catalog, "okx_bbo.json", "okx-swap-btcusdt-quote"
        )
        raw_topic, canonical_topic, raw_first, canonical_first = _broker_records(
            first_binding, first_raw, first_event, raw_offset=0, canonical_offset=0
        )
        _, _, raw_second, canonical_second = _broker_records(
            second_binding, second_raw, second_event, raw_offset=1, canonical_offset=1
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        append_calls = []
        original_append_many = self.spool.append_many

        def tracked_append_many(events):
            append_calls.append(len(events))
            return original_append_many(events)

        projection_calls = []
        original_apply_many = target.apply_many

        def tracked_apply_many(records):
            projection_calls.append(len(records))
            return original_apply_many(records)

        self.spool.append_many = tracked_append_many
        target.apply_many = tracked_apply_many
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many(
            (raw_first, raw_second, canonical_first, canonical_second)
        )

        self.assertEqual(append_calls, [2, 2])
        self.assertEqual(projection_calls, [2])
        self.assertEqual(
            broker.checkpoints,
            [
                (raw_topic, 0, 0),
                (raw_topic, 0, 1),
                (canonical_topic, 0, 0),
                (canonical_topic, 0, 1),
            ],
        )
        self.assertEqual(engine.stats.raw_committed, 2)
        self.assertEqual(engine.stats.canonical_committed, 2)
        self.assertEqual(engine.stats.pending_canonical, 0)

    async def test_batch_projection_failure_replays_without_premature_checkpoint(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()

        def fail_projection(_records):
            raise RuntimeError("injected batch projection failure")

        target.apply_many = fail_projection
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        with self.assertRaisesRegex(RuntimeError, "injected batch projection"):
            await engine.accept_many((raw_record, canonical_record))
        self.assertEqual(broker.checkpoints, [(raw_topic, 0, 0)])

        recovered_broker = _Broker()
        recovered_target = InMemoryStableProjectionTarget()
        recovered = self.engine(
            recovered_broker, recovered_target, raw_topic, canonical_topic
        )
        await recovered.accept(canonical_record)
        self.assertEqual(
            recovered_broker.checkpoints, [(canonical_topic, 0, 0)]
        )
        self.assertEqual(recovered.stats.canonical_committed, 1)
        self.assertEqual(recovered.stats.pending_canonical, 0)

    async def test_cross_replica_raw_cache_wakes_waiting_canonical_on_idle_poll(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept(canonical_record)
        self.assertEqual(engine.stats.pending_canonical, 1)

        # Simulate another projector replica durably storing/checkpointing raw.
        self.spool.append(DurableEvent(
            stream=raw_topic,
            partition_key=raw_record.key,
            event_id=raw_record.event_id,
            payload=raw_record.payload,
            accepted_at_ns=raw_record.accepted_at_ns,
            headers={"kafka_partition": "1", "kafka_offset": "7"},
        ))
        processed = await engine.run_once(timeout_seconds=0.01)
        self.assertFalse(processed)
        self.assertEqual(engine.stats.pending_canonical, 0)
        self.assertEqual(broker.checkpoints, [(canonical_topic, 0, 0)])
        self.assertEqual(len(target.publications), 2)

    async def test_checkpoint_failure_replays_idempotently_without_duplicate_publication(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, first = _broker_records(
            binding, raw, event
        )
        broker = _Broker(fail_once_offset=(canonical_topic, 0, 0))
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept(raw_record)
        with self.assertRaisesRegex(RuntimeError, "injected checkpoint"):
            await engine.accept(first)
        self.assertEqual(len(target.publications), 2)
        replay = KafkaProjectorRecord(
            topic=canonical_topic, partition=0, offset=1, key=first.key,
            event_id=first.event_id, payload=first.payload,
            accepted_at_ns=first.accepted_at_ns,
        )
        await engine.accept(replay)
        self.assertEqual(len(target.publications), 2)
        self.assertEqual(engine.stats.duplicate_projections, 2)
        self.assertEqual(
            broker.checkpoints[-2:],
            [(canonical_topic, 0, 0), (canonical_topic, 0, 1)],
        )

    async def test_redis_loss_rebuilds_from_kafka_replay_and_spool_raw_lineage(self):
        binding, raw, event = _stable_pair(
            self.catalog, "okx_bbo.json", "okx-swap-btcusdt-quote"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        first_target = InMemoryStableProjectionTarget()
        await self.engine(_Broker(), first_target, raw_topic, canonical_topic).accept(raw_record)
        await self.engine(_Broker(), first_target, raw_topic, canonical_topic).accept(canonical_record)
        rebuilt_target = InMemoryStableProjectionTarget()
        rebuilt = self.engine(_Broker(), rebuilt_target, raw_topic, canonical_topic)
        await rebuilt.accept(canonical_record)
        canonical_keys = [key for key in rebuilt_target.latest if key.startswith("qdl:stable:v2:")]
        self.assertEqual(len(canonical_keys), 1)
        self.assertEqual(rebuilt_target.publications, [])

    async def test_alias_policy_prevents_spot_usdm_generic_mixed_writer(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_spot_trade.json", "binance-spot-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        target = InMemoryStableProjectionTarget()
        engine = self.engine(_Broker(), target, raw_topic, canonical_topic)
        await engine.accept(raw_record)
        await engine.accept(canonical_record)
        self.assertIn("trade:price:binance_spot:BTCUSDT", target.latest)
        self.assertNotIn("trade:price:BTCUSDT", target.latest)
        self.assertEqual(
            [channel for channel, _ in target.publications],
            ["stream:trade:binance_spot:BTCUSDT"],
        )

    async def test_signed_http_sink_commits_to_active_gateway_idempotently(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, _canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        await StableProjectorEngine(
            broker=_Broker(), spool=self.spool, catalog=self.catalog,
            canonical_topic="unused-canonical", raw_topics=(raw_topic,),
            sink=LocalStableCanonicalSink(self.gateway, self.spool),
            projector=StableCompatibilityProjector(self.catalog),
            target=InMemoryStableProjectionTarget(),
        ).accept(raw_record)
        app = FastAPI()
        secret = b"phase-b-stable-ingest-secret-32b"
        install_stable_canonical_ingest(
            app, gateway=self.gateway, catalog=self.catalog,
            spool=self.spool, secret=secret,
        )
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        )
        sink = StableHttpCanonicalSink(
            ("http://localhost",), secret, self.spool, client=client
        )
        durable = DurableEvent(
            stream=self.catalog.canonical_stream,
            partition_key=binding.partition_key,
            event_id=canonical_record.event_id,
            payload=canonical_record.payload,
            accepted_at_ns=canonical_record.accepted_at_ns,
            headers={
                "raw_stream": raw_topic,
                "raw_event_id": raw_record.event_id.hex(),
            },
        )
        try:
            first = await sink.publish(durable)
            second = await sink.publish(durable)
            self.assertEqual(first.cursor, second.cursor)
            rejected = await client.post(
                "/internal/v2/canonical/events", content=b"{}",
                headers={"X-QDL-Stable-Signature": "sha256=bad"},
            )
            self.assertEqual(rejected.status_code, 401)
        finally:
            await sink.close()
            await client.aclose()

    async def test_stale_projection_epoch_fails_without_broker_checkpoint(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        target.lease_epochs[binding.source_id] = event.lease_epoch + 1
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept(raw_record)
        with self.assertRaises(ProjectionFenced):
            await engine.accept(canonical_record)
        self.assertEqual(broker.checkpoints, [(raw_topic, 0, 0)])



class StableRuntimeBoundaryTests(unittest.TestCase):
    def environment(self, root):
        return {
            "QDL_ENVIRONMENT": "paper",
            "QDL_CONFIG_REVISION": "phase-b-test-1",
            "QDL_STABLE_AUTHORITY_MODE": "RUST_SHADOW",
            "QDL_STABLE_AUTHORITY_REVISION": "1",
            "QDL_STABLE_SCHEMA_DIGEST": "e" * 64,
            "QDL_STABLE_STATE_DIR": str(root / "state"),
            "QDL_STABLE_DURABLE_STATE_DIR": str(root / "durable"),
            "QDL_STABLE_AUDIT_PATH": str(root / "state" / "audit.jsonl"),
            "QDL_STABLE_CONSUMER_MANIFESTS": str(root / "consumer.yaml"),
            "QDL_STABLE_SOURCE_BINDINGS": str(CATALOG_PATH),
            "QDL_STABLE_INTERNAL_INGEST_SECRET": "i" * 32,
            "QDL_STABLE_REDIS_URL": "redis://qdl-stable-redis:6379/0",
            "QDL_STABLE_REDIS_PREFIX": "qdl:stable:v2:paper:test",
            "QDL_STABLE_CONSUMER_GROUP": "qdl-stable-test",
            "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps({"stable-k1": "c" * 32}),
            "QDL_STABLE_CURSOR_ACTIVE_KEY_ID": "stable-k1",
        }

    def test_projector_sink_accepts_only_declared_internal_stream_roles(self):
        secret = b"phase-b-stable-ingest-secret-32b"
        client = object()
        sink = StableHttpCanonicalSink(
            ("http://stream_v2_active:8200", "http://stream_v2_passive:8200"),
            secret, object(), client=client,
        )
        self.assertIs(sink.client, client)
        with self.assertRaisesRegex(ValueError, "configuration is invalid"):
            StableHttpCanonicalSink(
                ("https://stream_v2_active:8200",), secret, object(), client=client
            )
        with self.assertRaisesRegex(ValueError, "configuration is invalid"):
            StableHttpCanonicalSink(
                ("http://external.example:8200",), secret, object(), client=client
            )

    def test_query_role_is_isolated_and_projector_dependencies_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            values = self.environment(root)
            config = StableRuntimeConfig.from_environment("query_v2", values)
            manifest = config.public_manifest()
            self.assertEqual(manifest["contract_version"], "2.0.0")
            self.assertEqual(manifest["authority"], "RUST_SHADOW")
            self.assertFalse(manifest["writes_current_v1_redis"])
            with self.assertRaisesRegex(ValueError, "Kafka/stream dependencies"):
                StableRuntimeConfig.from_environment("projector_v2", values)
            values["QDL_STABLE_AUTHORITY_MODE"] = "PRIMARY"
            with self.assertRaisesRegex(ValueError, "must remain RUST_SHADOW"):
                StableRuntimeConfig.from_environment("query_v2", values)


@unittest.skipUnless(os.getenv("QDL_PHASEB_REDIS_URL"), "isolated Redis is not configured")
class StableRedisProjectionIntegrationTests(unittest.TestCase):
    namespace = "qdl:test:phaseb:stable:v2"
    symbol = "PHASEBTESTBTCUSDT"

    def test_atomic_ttl_publication_duplicate_and_fencing(self):
        import redis

        url = os.environ["QDL_PHASEB_REDIS_URL"]
        if urlsplit(url).hostname != "qdl-phaseb-redis-test":
            self.fail("Phase B Redis integration test requires its named disposable container")
        client = redis.Redis.from_url(url)
        current = f"trade:price:{self.symbol}"
        last = f"trade:price:last:{self.symbol}"
        channel = f"stream:trade:{self.symbol}"
        scoped = [current, last, *client.scan_iter(match=f"{self.namespace}:*")]
        if scoped:
            client.delete(*scoped)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(channel)
        pubsub.get_message(timeout=1)
        try:
            record = StableProjectionRecord(
                partition_key="phase-b/test/partition",
                offset=1,
                event_id_hex="ab" * 16,
                shard_id="phase-b-test-shard",
                lease_epoch=2,
                items=(
                    StableProjectionItem(f"{self.namespace}:latest:test", b"canonical"),
                    StableProjectionItem(current, b"legacy", 60),
                    StableProjectionItem(last, b"legacy"),
                ),
                publications=((channel, b"legacy"),),
            )
            target = RedisStableProjectionTarget(
                client, namespace=self.namespace, dedicated_database=True
            )
            self.assertTrue(target.apply(record))
            self.assertGreater(client.ttl(current), 0)
            self.assertEqual(client.ttl(last), -1)
            message = pubsub.get_message(timeout=1)
            self.assertIsNotNone(message)
            self.assertEqual(message["channel"].decode(), channel)
            self.assertFalse(target.apply(record))
            self.assertIsNone(pubsub.get_message(timeout=0.2))
            fenced = StableProjectionRecord(
                partition_key=record.partition_key,
                offset=2,
                event_id_hex=record.event_id_hex,
                shard_id=record.shard_id,
                lease_epoch=1,
                items=record.items,
                publications=record.publications,
            )
            with self.assertRaises(ProjectionFenced):
                target.apply(fenced)
        finally:
            pubsub.close()
            scoped = [current, last, *client.scan_iter(match=f"{self.namespace}:*")]
            if scoped:
                client.delete(*scoped)
            client.close()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import base64
from collections import deque
import hashlib
import hmac
import json
import os
import tempfile
from types import SimpleNamespace
from urllib.parse import urlsplit
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

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
from qdl.marketdata.v2 import market_data_pb2
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.query import (
    AccessPurpose,
    ConsumerGrade,
    DataRequirement,
    InstrumentQuery,
    QueryServiceError,
    StalePolicy,
    V2QueryService,
)
from qdl.projection.stable import (
    InMemoryStableProjectionTarget,
    ProjectionCacheMismatch,
    ProjectionFenced,
    RedisStableProjectionTarget,
    StableCompatibilityProjector,
    StableProjectionItem,
    StableProjectionRecord,
)
from qdl.raw.capture import bind_capture_context, capture_exact_frame
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_capacity import (
    STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
)
from qdl.runtime.stable import (
    StableRuntimeConfig,
    stable_request_bounds,
    stable_grpc_server_credentials,
    stable_uvicorn_tls,
)
from qdl.runtime.stable_ingest import StableHttpCanonicalSink, install_stable_canonical_ingest
from qdl.runtime.stable_projector import (
    LocalStableCanonicalSink,
    StableProjectorEngine,
    supervise_stable_projector,
)
from qdl.runtime.stable_source import (
    StableCatalogCursorScopeValidator,
    StableConsumerCursorIssuer,
    StableSpoolQueryBackend,
)
from qdl.runtime.session_liveness import StableSessionLivenessReader
from qdl.stream import DurableStreamGateway
from qdl.transport import BackpressureRequired, DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl.transport.kafka_projector import KafkaProjectorRecord
from qdl.warmup import WarmupSpecification, WarmupTimeRange


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
        self.flow_control = []
        self.checkpoint_batches = []

    def poll(self, timeout_seconds):
        del timeout_seconds
        return None

    def checkpoint(self, record):
        if self.fail_once_offset == (record.topic, record.partition, record.offset):
            self.fail_once_offset = None
            raise RuntimeError("injected checkpoint failure")
        self.checkpoints.append((record.topic, record.partition, record.offset))

    def checkpoint_many(self, records):
        values = tuple(records)
        self.checkpoint_batches.append(len(values))
        for record in values:
            self.checkpoint(record)

    def pause_canonical(self):
        self.flow_control.append("pause")

    def resume_canonical(self):
        self.flow_control.append("resume")

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
    return _append_unvalidated(spool, binding, event)


def _append_unvalidated(spool, binding, event):
    """Persist a controlled legacy-row fixture without catalog admission.

    Production ingestion always resolves the envelope through the catalog. This
    helper exists solely to model old retained rows whose lineage was valid at
    the time they were written but has since been retired.
    """
    durable = DurableEvent(
        stream=binding.canonical_stream,
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
    def test_catalog_from_mapping_matches_strict_file_loader_without_io(self):
        payload = yaml.safe_load(CATALOG_PATH.read_text())
        from_mapping = StableSourceCatalog.from_mapping(payload)
        from_file = StableSourceCatalog.load(CATALOG_PATH)
        self.assertEqual(from_mapping.instruments, from_file.instruments)
        self.assertEqual(from_mapping.bindings, from_file.bindings)
        payload["bindings"][0]["unknown"] = "not-allowed"
        with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
            StableSourceCatalog.from_mapping(payload)

    def test_catalog_covers_equal_source_baseline_with_deterministic_identity(self):
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        # The fixed non-crypto capability plane has 10 rows. Each of the five
        # liquid Binance USD-M and five OKX Swap instruments contributes TRADE,
        # QUOTE and every provider-native BAR interval. C3.6 adds the declared
        # 18 physical L2 books as 36 snapshot/delta logical bindings.
        from qdl.adapters.intervals import (
            BINANCE_USDM_NATIVE_INTERVALS,
            OKX_NATIVE_INTERVALS,
        )
        baseline = (
            10
            + 5 * (2 + len(BINANCE_USDM_NATIVE_INTERVALS))
            + 5 * (2 + len(OKX_NATIVE_INTERVALS))
        )
        l2_bindings = [
            item for item in catalog.bindings
            if item.feed.value in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
        ]
        self.assertEqual(len(l2_bindings), 36)
        self.assertEqual(len(catalog.bindings), baseline + len(l2_bindings))
        self.assertEqual(
            {(item.instrument.identity.venue, item.feed.value) for item in catalog.bindings},
            {
                ("BINANCE", "TRADE"), ("BINANCE", "QUOTE"), ("BINANCE", "BAR"),
                ("BINANCE", "BOOK_SNAPSHOT"), ("BINANCE", "BOOK_DELTA"),
                ("OKX", "TRADE"), ("OKX", "QUOTE"), ("OKX", "BAR"),
                ("OKX", "BOOK_SNAPSHOT"), ("OKX", "BOOK_DELTA"),
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

    def test_catalog_accepts_only_declared_historical_instrument_revision(self):
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        binding = next(
            item for item in catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-trade"
        )
        self.assertEqual(binding.instrument.metadata_revision, 2)
        self.assertEqual(binding.historical_metadata_revisions, (1,))
        current = _stable_event(
            catalog, "binance_usdm_trade.json", binding.binding_id
        )
        self.assertIs(catalog.binding_for_envelope(current), binding)
        historical = market_data_pb2.EventEnvelope()
        historical.CopyFrom(current)
        historical.instrument_revision = 1
        self.assertIs(catalog.binding_for_envelope(historical), binding)
        future = market_data_pb2.EventEnvelope()
        future.CopyFrom(current)
        future.instrument_revision = 3
        with self.assertRaisesRegex(ValueError, "identity/lineage"):
            catalog.binding_for_envelope(future)

    def test_catalog_rejects_implicit_or_current_historical_revision(self):
        payload = yaml.safe_load(CATALOG_PATH.read_text())
        btc = next(
            item for item in payload["instruments"]
            if item["instrument_id"] == "BINANCE.USDM.PERPETUAL.BTC-USDT"
        )
        btc["historical_metadata_revisions"] = [2]
        with self.assertRaisesRegex(ValueError, "historical instrument revisions"):
            StableSourceCatalog.from_mapping(payload)
        btc["historical_metadata_revisions"] = ["1"]
        with self.assertRaisesRegex(ValueError, "historical instrument revisions"):
            StableSourceCatalog.from_mapping(payload)

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
            max_partition_records=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
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
                self.assertEqual(item.observed_at_ns, observed)
                self.assertEqual(item.received_at_ns, event.received_at_ns)
                self.assertEqual(item.source.provider, event.provider)
                self.assertEqual(item.source.source_id, event.source_id)
                self.assertGreaterEqual(item.quality.freshness_ms, 0)
                self.assertEqual(item.contract.contract_version, "2.0.0")
                expected_state = "MARKET_CLOSED" if binding.instrument.identity.venue == "HNX" else "LIVE"
                self.assertEqual(item.quality.state, expected_state)
                self.assertEqual(item.quality.execution_eligible, expected_state == "LIVE")

    def test_materialized_bar_history_data_as_of_is_the_closed_boundary(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        event = _stable_event(
            self.catalog, "binance_usdm_rest_bar.json", binding.binding_id
        )
        _append(self.spool, self.catalog, event)
        backend = StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="a" * 64,
            clock_ns=lambda: event.bar.close_time_ns + 1_000_000,
        )
        history = backend.history(_requirement(binding))
        self.assertIsNotNone(history)
        self.assertEqual(history.data_as_of_ns, event.bar.close_time_ns)

    def test_late_bar_backfill_keeps_market_order_and_fences_max_offset(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        newer = _stable_event(
            self.catalog, "binance_usdm_rest_bar.json", binding.binding_id
        )
        newer.event_id = hashlib.sha256(b"phase103-newer-bar").digest()[:16]
        newer.raw_capture_id = hashlib.sha256(b"phase103-newer-raw").digest()[:16]

        older = market_data_pb2.EventEnvelope()
        older.CopyFrom(newer)
        interval_ns = 60_000_000_000
        older.event_id = hashlib.sha256(b"phase103-older-bar").digest()[:16]
        older.raw_capture_id = hashlib.sha256(b"phase103-older-raw").digest()[:16]
        older.bar.open_time_ns -= interval_ns
        older.bar.close_time_ns -= interval_ns
        older.source_event_time_ns -= interval_ns
        older.received_at_ns -= interval_ns
        older.normalized_at_ns -= interval_ns
        older.published_at_ns -= interval_ns
        older.source_sequence = "phase103-late-history"
        older.partition_sequence += 1
        older.correlation_id = "phase103-late-history"

        # A real provider history bootstrap may reach durable storage after a
        # newer live BAR. Append order must fence replay, not reorder market time.
        self.assertEqual(_append(self.spool, self.catalog, newer).cursor.offset, 1)
        self.assertEqual(_append(self.spool, self.catalog, older).cursor.offset, 2)
        backend = StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="a" * 64,
            clock_ns=lambda: newer.bar.close_time_ns + 1_000_000,
        )

        history = backend.history(_requirement(binding, warmup=2))
        self.assertIsNotNone(history)
        self.assertEqual(
            [item.payload["open_time_ns"] for item in history.items],
            [older.bar.open_time_ns, newer.bar.open_time_ns],
        )
        self.assertEqual(history.watermark_offset, 2)
        self.assertEqual(history.data_as_of_ns, newer.bar.close_time_ns)
        self.assertEqual(
            backend.latest(_requirement(binding)).payload["open_time_ns"],
            newer.bar.open_time_ns,
        )

    def test_row_warmup_selects_current_market_tail_after_late_backfill(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        template = _stable_event(
            self.catalog, "binance_usdm_rest_bar.json", binding.binding_id
        )
        minute_ns = 60 * 1_000_000_000

        def event_at(index: int):
            event = type(template)()
            event.CopyFrom(template)
            event.event_id = hashlib.sha256(
                f"phase-b-market-tail-{index}".encode()
            ).digest()[:16]
            event.raw_capture_id = hashlib.sha256(
                f"phase-b-market-tail-raw-{index}".encode()
            ).digest()[:16]
            event.bar.open_time_ns = template.bar.open_time_ns + index * minute_ns
            event.bar.close_time_ns = template.bar.close_time_ns + index * minute_ns
            event.source_event_time_ns = event.bar.close_time_ns
            event.received_at_ns = event.bar.close_time_ns + 1
            event.normalized_at_ns = event.received_at_ns + 1
            event.published_at_ns = event.received_at_ns + 2
            event.source_sequence = f"phase-b-market-tail-{index}"
            event.partition_sequence = index + 1
            event.correlation_id = f"phase-b-market-tail-{index}"
            return event

        # Live rows may arrive first. A later bounded provider repair is valid
        # market history even though its durable logical offsets are newer.
        for index in range(6, 11):
            _append(self.spool, self.catalog, event_at(index))
        for index in range(0, 6):
            event = event_at(index)
            if index < 2:
                event.adapter_version = "binance-rest/2.0.0"
                _append_unvalidated(self.spool, binding, event)
            else:
                _append(self.spool, self.catalog, event)
        for index in range(11, 13):
            _append(self.spool, self.catalog, event_at(index))

        backend = StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="b" * 64,
            clock_ns=lambda: event_at(12).bar.close_time_ns + 1,
        )
        requirement = _requirement(binding, warmup=5)
        history = backend.history(requirement)
        self.assertIsNotNone(history)
        self.assertEqual(history.coverage.value, "FULL")
        expected_opens = [event_at(index).bar.open_time_ns for index in range(8, 13)]
        self.assertEqual(
            [item.payload["open_time_ns"] for item in history.items], expected_opens
        )
        self.assertEqual(
            [
                market_data_pb2.EventEnvelope.FromString(item.event.payload).bar.open_time_ns
                for item in backend.stored_events(requirement)
            ],
            expected_opens,
        )
        self.assertFalse(backend.latest(requirement).quality.gap_open)

        with tempfile.TemporaryDirectory() as directory:
            gap_spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "market-tail-gap.sqlite3",
                min_free_disk_bytes=0,
                max_partition_records=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
            ))
            try:
                for index in (6, 7, 9, 10, 11):
                    _append(gap_spool, self.catalog, event_at(index))
                gapped = StableSpoolQueryBackend(
                    gap_spool,
                    self.catalog,
                    schema_digest="c" * 64,
                    clock_ns=lambda: event_at(11).bar.close_time_ns + 1,
                ).history(requirement)
                self.assertIsNotNone(gapped)
                self.assertEqual(gapped.coverage.value, "PARTIAL")
            finally:
                gap_spool.close()

        with tempfile.TemporaryDirectory() as directory:
            invalid_spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "market-tail-lineage.sqlite3",
                min_free_disk_bytes=0,
                max_partition_records=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
            ))
            try:
                for index in range(8, 13):
                    event = event_at(index)
                    if index == 12:
                        event.adapter_version = "binance-rest/2.0.0"
                        _append_unvalidated(invalid_spool, binding, event)
                    else:
                        _append(invalid_spool, self.catalog, event)
                invalid = StableSpoolQueryBackend(
                    invalid_spool,
                    self.catalog,
                    schema_digest="d" * 64,
                    clock_ns=lambda: event_at(12).bar.close_time_ns + 1,
                )
                with self.assertRaisesRegex(ValueError, "identity/lineage"):
                    invalid.history(requirement)
            finally:
                invalid_spool.close()

    def test_public_bar_warmup_scans_physical_tail_before_market_selection(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        template = _stable_event(
            self.catalog, "binance_usdm_rest_bar.json", binding.binding_id
        )
        minute_ns = 60 * 1_000_000_000

        def stored(index: int, offset: int):
            event = type(template)()
            event.CopyFrom(template)
            event.event_id = hashlib.sha256(
                f"phase-b-physical-tail-{index}".encode()
            ).digest()[:16]
            event.raw_capture_id = hashlib.sha256(
                f"phase-b-physical-tail-raw-{index}".encode()
            ).digest()[:16]
            event.bar.open_time_ns = template.bar.open_time_ns + index * minute_ns
            event.bar.close_time_ns = template.bar.close_time_ns + index * minute_ns
            event.source_event_time_ns = event.bar.close_time_ns
            event.received_at_ns = event.bar.close_time_ns + 1
            event.normalized_at_ns = event.received_at_ns + 1
            event.published_at_ns = event.received_at_ns + 2
            event.source_sequence = f"phase-b-physical-tail-{index}"
            event.partition_sequence = offset
            event.correlation_id = f"phase-b-physical-tail-{index}"
            return SimpleNamespace(
                event=SimpleNamespace(
                    payload=event.SerializeToString(), event_id=bytes(event.event_id)
                ),
                cursor=SimpleNamespace(
                    stream=binding.canonical_stream,
                    partition_key=binding.partition_key,
                    offset=offset,
                ),
            )

        # The first logical records simulate authentic late repairs.  Once the
        # cache has grown, a public-size append-tail scan would evict precisely
        # these recent market-time bars and manufacture a false warmup gap.
        repaired_indices = tuple(range(9_900, 9_964))
        repaired_set = set(repaired_indices)
        remaining_indices = tuple(
            index
            for index in range(-64, 10_000)
            if index not in repaired_set
        )
        rows = tuple(
            stored(index, offset)
            for offset, index in enumerate(
                (*repaired_indices, *remaining_indices), start=1
            )
        )
        self.assertEqual(len(rows), STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW)

        class PhysicalTailSpool:
            def __init__(self, values):
                self.values = values
                self.limits = []

            def read_tail(self, *, stream, partition_key, limit):
                self.limits.append((stream, partition_key, limit))
                return tuple(self.values[-limit:])

        spool = PhysicalTailSpool(rows)
        backend = StableSpoolQueryBackend(
            spool,
            self.catalog,
            schema_digest="e" * 64,
            clock_ns=lambda: template.bar.close_time_ns + 10_001 * minute_ns,
        )
        history = backend.history(_requirement(binding, warmup=700))
        self.assertIsNotNone(history)
        self.assertEqual(history.coverage.value, "FULL")
        self.assertEqual(
            [item.payload["open_time_ns"] for item in history.items],
            [
                template.bar.open_time_ns + index * minute_ns
                for index in range(9_300, 10_000)
            ],
        )
        self.assertFalse(backend.latest(_requirement(binding, warmup=700)).quality.gap_open)
        self.assertTrue(spool.limits)
        self.assertTrue(all(
            limit == STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW
            for _stream, _partition, limit in spool.limits
        ))

    def test_vn_time_range_accepts_lunch_break_but_rejects_missing_session_bar(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "dnse-vn30f1m-bar-1m"
        )
        start_ns = int(
            datetime(2026, 8, 24, 4, 28, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        afternoon_ns = int(
            datetime(2026, 8, 24, 6, 0, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        end_ns = afternoon_ns + 60_000_000_000

        def event_at(open_ns, index):
            event = market_data_pb2.EventEnvelope()
            event.CopyFrom(_stable_event(
                self.catalog, "dnse_derivative_bar.json", binding.binding_id
            ))
            event.event_id = hashlib.sha256(f"vn-session-{index}".encode()).digest()[:16]
            event.raw_capture_id = hashlib.sha256(
                f"vn-raw-{index}".encode()
            ).digest()[:16]
            event.bar.open_time_ns = open_ns
            event.bar.close_time_ns = open_ns + 60_000_000_000 - 1_000_000
            event.source_event_time_ns = event.bar.close_time_ns
            event.received_at_ns = event.bar.close_time_ns + 1_000_000
            event.normalized_at_ns = event.received_at_ns + 1
            event.published_at_ns = event.received_at_ns + 2
            event.source_sequence = str(index)
            event.partition_sequence = index
            event.correlation_id = f"vn-session-{index}"
            return event

        for index, open_ns in enumerate(
            (start_ns, start_ns + 60_000_000_000, afternoon_ns), start=1
        ):
            _append(self.spool, self.catalog, event_at(open_ns, index))
        requirement = DataRequirement(
            instrument_uid=binding.instrument.instrument_uid,
            feed=binding.feed,
            interval=binding.interval,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id=binding.source_policy_id,
            warmup=WarmupSpecification(
                time_range=WarmupTimeRange(start_ns, end_ns)
            ),
        )
        backend = StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="a" * 64,
            clock_ns=lambda: end_ns + 1_000_000,
        )
        complete = backend.history(requirement)
        self.assertEqual(complete.coverage.value, "FULL")
        self.assertEqual(len(complete.items), 3)
        self.assertEqual(backend.open_gaps(), ())

        with tempfile.TemporaryDirectory() as directory:
            incomplete_spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "incomplete.sqlite3",
                min_free_disk_bytes=0,
                max_partition_records=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
            ))
            try:
                _append(incomplete_spool, self.catalog, event_at(start_ns, 1))
                _append(incomplete_spool, self.catalog, event_at(afternoon_ns, 3))
                incomplete = StableSpoolQueryBackend(
                    incomplete_spool,
                    self.catalog,
                    schema_digest="a" * 64,
                    clock_ns=lambda: end_ns + 1_000_000,
                )
                result = incomplete.history(requirement)
                self.assertEqual(result.coverage.value, "PARTIAL")
                self.assertEqual(len(incomplete.open_gaps()), 1)
            finally:
                incomplete_spool.close()

    def test_query_reads_are_bounded_by_feed_and_requested_history(self):
        trade = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-trade"
        )
        bar = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        trade_event = _stable_event(
            self.catalog, "binance_usdm_trade.json", trade.binding_id
        )
        bar_event = _stable_event(
            self.catalog, "binance_usdm_rest_bar.json", bar.binding_id
        )
        _append(self.spool, self.catalog, trade_event)
        _append(self.spool, self.catalog, bar_event)
        backend = StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="e" * 64,
            clock_ns=lambda: max(
                trade_event.source_event_time_ns, bar_event.bar.close_time_ns
            ) + 1_000_000,
        )
        observed_limits = []
        read_tail = self.spool.read_tail

        def tracked_read_tail(**kwargs):
            observed_limits.append(kwargs["limit"])
            return read_tail(**kwargs)

        self.spool.read_tail = tracked_read_tail
        self.assertIsNotNone(backend.latest(_requirement(trade)))
        self.assertIsNotNone(backend.history(_requirement(trade, warmup=1)))
        self.assertIsNotNone(backend.latest(_requirement(bar)))
        self.assertEqual(
            observed_limits,
            [1, 1, STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW],
        )

    def test_latest_uses_newest_tail_after_partition_exceeds_query_window(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-trade"
        )
        template = _stable_event(
            self.catalog, "binance_usdm_trade.json", binding.binding_id
        )
        base_time = template.source_event_time_ns
        tail_path = Path(self.temp.name) / "stable-tail.sqlite3"
        with SQLiteDurableSpool(SpoolConfig(
            path=tail_path,
            max_records=10_010,
            max_payload_bytes=32 * 1024 * 1024,
            max_storage_bytes=48 * 1024 * 1024,
            min_free_disk_bytes=0,
        )) as spool:
            pending = []
            for index in range(10_001):
                event = type(template)()
                event.CopyFrom(template)
                event.event_id = hashlib.sha256(
                    f"phase-b-tail-{index}".encode()
                ).digest()[:16]
                event.source_sequence = str(index + 1)
                event.partition_sequence = index + 1
                event.source_event_time_ns = base_time + index * 1_000_000
                event.received_at_ns = event.source_event_time_ns + 1
                event.normalized_at_ns = event.source_event_time_ns + 2
                event.published_at_ns = event.source_event_time_ns + 3
                pending.append(DurableEvent(
                    stream=self.catalog.canonical_stream,
                    partition_key=binding.partition_key,
                    event_id=bytes(event.event_id),
                    payload=event.SerializeToString(deterministic=True),
                    accepted_at_ns=event.received_at_ns,
                    headers={"schema": "qdl.marketdata.trade/2"},
                ))
                if len(pending) == 1000:
                    spool.append_many(pending)
                    pending = []
            spool.append_many(pending)
            expected_time = base_time + 10_000 * 1_000_000
            backend = StableSpoolQueryBackend(
                spool,
                self.catalog,
                schema_digest="f" * 64,
                clock_ns=lambda: expected_time + 1_000_000,
            )
            latest = backend.latest(_requirement(binding))
            self.assertIsNotNone(latest)
            self.assertEqual(latest.observed_at_ns, expected_time)
            self.assertEqual(latest.watermark_offset, 10_001)
            self.assertEqual(latest.quality.state, "LIVE")

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


class StableSessionLivenessQualityTests(unittest.TestCase):
    """Deterministic quiet-feed evidence; no provider connection is opened."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "stable.sqlite3",
            max_records=100,
            max_payload_bytes=2 * 1024 * 1024,
            max_storage_bytes=8 * 1024 * 1024,
            min_free_disk_bytes=0,
        ))
        self.binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-trade"
        )
        self.event = _stable_event(
            self.catalog, "binance_usdm_trade.json", self.binding.binding_id
        )
        self.event.config_revision = 7
        _append(self.spool, self.catalog, self.event)
        self.now_ns = self.event.source_event_time_ns + 5_000_000_000
        self.root = Path(self.temp.name) / "session-liveness"

    def tearDown(self):
        self.spool.close()
        self.temp.cleanup()

    def _requirement(self, *, policy=StalePolicy.OBSERVE):
        return DataRequirement(
            instrument_uid=self.binding.instrument.instrument_uid,
            feed=self.binding.feed,
            interval=self.binding.interval,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id=self.binding.source_policy_id,
            max_freshness_ms=3_000,
            event_recency_policy=policy,
            max_session_liveness_ms=45_000,
            stale_policy=StalePolicy.BLOCK,
        )

    def _write_session(
        self,
        *,
        state="LIVE",
        generation=None,
        revision=7,
        age_ms=1,
    ):
        directory = self.root / "binance-usdm"
        directory.mkdir(parents=True, exist_ok=True)
        transport_at_ns = self.now_ns - age_ms * 1_000_000
        (directory / "trade-lane.json").write_text(
            json.dumps({
                "schema": "qdl.provider-session-liveness.v1",
                "source_session_id": self.event.source_session_id,
                "connection_generation": (
                    self.event.connection_generation
                    if generation is None
                    else generation
                ),
                "state": state,
                "last_transport_at_ns": transport_at_ns,
                "updated_at_ns": transport_at_ns,
                "config_revision": revision,
            }),
            encoding="utf-8",
        )

    def _backend(self):
        return StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="e" * 64,
            config_revision=7,
            session_liveness_root=str(self.root),
            clock_ns=lambda: self.now_ns,
        )

    def test_quiet_but_connected_trade_is_visible_and_non_executable(self):
        self._write_session()
        requirement = self._requirement()
        backend = self._backend()
        item = backend.latest(requirement)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item.quality.state, "LIVE")
        self.assertGreater(item.quality.freshness_ms, 3_000)
        self.assertEqual(item.quality.event_recency_state, "STALE")
        self.assertEqual(item.quality.provider_session_state, "LIVE")
        self.assertEqual(item.quality.provider_session_liveness_ms, 1)
        self.assertFalse(item.quality.execution_eligible)
        self.assertIn("LAST_EVENT_STALE", item.quality.flags)

        service = V2QueryService(
            instruments=InstrumentQuery(self.catalog.instrument_registry()),
            backend=backend,
            entitlements=self.catalog.entitlements(),
            clock_ns=lambda: self.now_ns,
        )
        result = service.snapshot(
            requirement,
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertFalse(result.item.quality.execution_eligible)

    def test_disconnected_expired_and_reconnected_session_evidence_fail_closed(self):
        requirement = self._requirement()
        cases = (
            ("DISCONNECTED", None, 7, 1, "DISCONNECTED"),
            ("LIVE", None, 7, 45_001, "STALE"),
            ("LIVE", None, 6, 1, "UNKNOWN"),
            ("LIVE", 2, 7, 1, "UNKNOWN"),
        )
        for state, generation, revision, age_ms, expected in cases:
            with self.subTest(
                state=state,
                generation=generation,
                revision=revision,
                age_ms=age_ms,
            ):
                self._write_session(
                    state=state,
                    generation=generation,
                    revision=revision,
                    age_ms=age_ms,
                )
                item = self._backend().latest(requirement)
                self.assertIsNotNone(item)
                assert item is not None
                self.assertEqual(item.quality.state, "STALE")
                self.assertEqual(item.quality.provider_session_state, expected)
        self._write_session()
        raw = self.root / "binance-usdm" / "trade-lane.json"
        raw.write_text("{malformed", encoding="utf-8")
        status = StableSessionLivenessReader(self.root).status(
            venue="BINANCE",
            market="USDM",
            source_session_id=self.event.source_session_id,
            connection_generation=self.event.connection_generation,
            config_revision=7,
            now_ns=self.now_ns,
        )
        self.assertEqual(status.state, "UNKNOWN")
        self.assertEqual(status.flags, ("SOURCE_SESSION_MALFORMED",))

    def test_block_policy_remains_strict_with_a_live_session(self):
        self._write_session()
        requirement = self._requirement(policy=StalePolicy.BLOCK)
        backend = self._backend()
        item = backend.latest(requirement)
        self.assertIsNotNone(item)
        assert item is not None
        self.assertEqual(item.quality.event_recency_state, "STALE")
        self.assertEqual(item.quality.provider_session_state, "LIVE")
        self.assertEqual(item.quality.state, "STALE")
        service = V2QueryService(
            instruments=InstrumentQuery(self.catalog.instrument_registry()),
            backend=backend,
            entitlements=self.catalog.entitlements(),
            clock_ns=lambda: self.now_ns,
        )
        with self.assertRaises(QueryServiceError) as raised:
            service.snapshot(requirement, purpose=AccessPurpose.INTERNAL_EXECUTION)
        self.assertEqual(raised.exception.problem.code.value, "DATA_STALE")


class StableCursorScopeValidatorTests(unittest.TestCase):
    def setUp(self):
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.binding = next(
            item for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-trade"
        )
        self.requirement = _requirement(self.binding)
        self.validator = StableCatalogCursorScopeValidator(self.catalog)

    def test_unified_canonical_stream_matches_exact_catalog_binding(self):
        self.validator.validate(
            self.requirement,
            stream=self.binding.canonical_stream,
            partition_key=self.binding.partition_key,
        )

    def test_wrong_stream_partition_and_policy_fail_closed(self):
        invalid_scopes = (
            (f"{self.binding.canonical_stream}.trade", self.binding.partition_key),
            (self.binding.canonical_stream, self.binding.partition_key + "-other"),
            (
                self.binding.canonical_stream,
                self.binding.partition_key.replace("/trade/", "/quote/"),
            ),
        )
        for stream, partition_key in invalid_scopes:
            with self.subTest(stream=stream, partition_key=partition_key):
                with self.assertRaisesRegex(ValueError, "stable binding"):
                    self.validator.validate(
                        self.requirement, stream=stream, partition_key=partition_key
                    )

        wrong_policy = DataRequirement(
            instrument_uid=self.requirement.instrument_uid,
            feed=self.requirement.feed,
            interval=self.requirement.interval,
            consumer_grade=self.requirement.consumer_grade,
            source_policy_id="wrong-policy",
            warmup_limit=self.requirement.warmup_limit,
        )
        with self.assertRaisesRegex(ValueError, "no matching stable binding"):
            self.validator.validate(
                wrong_policy,
                stream=self.binding.canonical_stream,
                partition_key=self.binding.partition_key,
            )


class StableProjectionAllowlistTests(unittest.TestCase):
    namespace = "qdl:test:phaseb:stable:v2"

    def target(self):
        # _command is pure validation/serialization; avoid a Redis dependency.
        target = object.__new__(RedisStableProjectionTarget)
        target._namespace = self.namespace
        target._cache_id = "ab" * 16
        return target

    def record(
        self,
        key: str,
        *,
        publications: tuple[tuple[str, bytes], ...] = (),
    ) -> StableProjectionRecord:
        return StableProjectionRecord(
            partition_key="phase-b/allowlist/partition",
            offset=1,
            event_id_hex="cd" * 16,
            shard_id="phase-b-allowlist",
            lease_epoch=1,
            items=(StableProjectionItem(key, b"payload"),),
            publications=publications,
        )

    def test_every_catalog_legacy_bar_interval_is_admitted(self):
        catalog = StableSourceCatalog.load(CATALOG_PATH)
        bindings = tuple(
            binding for binding in catalog.bindings
            if binding.v1_compatibility == "BINANCE_BAR_GENERIC"
        )
        self.assertTrue(bindings)
        self.assertEqual(
            {binding.interval[-1] for binding in bindings if binding.interval},
            {"m", "h", "d", "w"},
        )
        target = self.target()
        for binding in bindings:
            with self.subTest(binding=binding.binding_id):
                current = f"kline:{binding.interval}:{binding.instrument.native_symbol}"
                last = f"kline:last:{binding.interval}:{binding.instrument.native_symbol}"
                target._command(self.record(current))
                target._command(self.record(last))
                target._command(self.record(
                    current,
                    publications=((
                        f"stream:kline:{binding.interval}:{binding.instrument.native_symbol}",
                        b"payload",
                    ),),
                ))

    def test_undeclared_suffix_and_unscoped_key_or_channel_remain_rejected(self):
        target = self.target()
        for key in ("kline:1M:BTCUSDT", "kline:1w:BTCUSDT:extra", "foreign:key"):
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "escapes its allowlist"):
                    target._command(self.record(key))
        for channel in (
            "stream:kline:1M:BTCUSDT",
            "stream:kline:1w:BTCUSDT:extra",
            "foreign:channel",
        ):
            with self.subTest(channel=channel):
                with self.assertRaisesRegex(ValueError, "escapes its allowlist"):
                    target._command(self.record(
                        "kline:1m:BTCUSDT",
                        publications=((channel, b"payload"),),
                    ))


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

    def _native_backfill_overlap(self):
        binding, raw, event = _stable_pair(
            self.catalog,
            "binance_usdm_rest_bar.json",
            "binance-usdm-btcusdt-bar-1m",
        )
        raw_topic, canonical_topic, raw_record, original_record = _broker_records(
            binding, raw, event
        )
        native = type(event)()
        native.CopyFrom(event)
        native.bar.origin = common_pb2.BAR_ORIGIN_VENUE_NATIVE
        native.canonical_payload_hash = hashlib.sha256(
            native.bar.SerializeToString(deterministic=True)
        ).digest()
        native_record = KafkaProjectorRecord(
            topic=canonical_topic,
            partition=0,
            offset=0,
            key=binding.partition_key,
            event_id=bytes(native.event_id),
            payload=native.SerializeToString(deterministic=True),
            accepted_at_ns=original_record.accepted_at_ns,
        )
        backfill = type(native)()
        backfill.CopyFrom(native)
        backfill.bar.origin = common_pb2.BAR_ORIGIN_BACKFILLED
        backfill.bar.close.mantissa += 1
        backfill.bar.close.source_text = str(
            CanonicalDecimal(
                backfill.bar.close.mantissa,
                backfill.bar.close.scale,
                backfill.bar.close.source_text,
            ).as_decimal()
        )
        backfill.canonical_payload_hash = hashlib.sha256(
            backfill.bar.SerializeToString(deterministic=True)
        ).digest()
        return (
            binding,
            raw_topic,
            canonical_topic,
            raw_record,
            native,
            native_record,
            backfill,
            original_record.accepted_at_ns,
        )

    @staticmethod
    def _canonical_record(
        *,
        topic: str,
        key: str,
        envelope: market_data_pb2.EventEnvelope,
        offset: int,
        accepted_at_ns: int,
    ) -> KafkaProjectorRecord:
        return KafkaProjectorRecord(
            topic=topic,
            partition=0,
            offset=offset,
            key=key,
            event_id=bytes(envelope.event_id),
            payload=envelope.SerializeToString(deterministic=True),
            accepted_at_ns=accepted_at_ns,
        )

    def test_projector_round_robins_coowned_partition_backlog_without_reordering(self):
        def record(partition: int, offset: int) -> KafkaProjectorRecord:
            return KafkaProjectorRecord(
                topic="qdl.stable.canonical.phase-b.v2",
                partition=partition,
                offset=offset,
                key=f"fixture/{partition}",
                event_id=(partition.to_bytes(1) + offset.to_bytes(15, "big")),
                payload=b"fixture",
                accepted_at_ns=1,
            )

        queues = {
            ("qdl.stable.canonical.phase-b.v2", 2): deque(
                record(2, offset) for offset in (0, 1, 2)
            ),
            ("qdl.stable.canonical.phase-b.v2", 3): deque(
                record(3, offset) for offset in (40, 41)
            ),
        }

        selected = StableProjectorEngine._round_robin_candidates(queues, 5)

        self.assertEqual(
            [(record.partition, record.offset) for _partition, record in selected],
            [(2, 0), (3, 40), (2, 1), (3, 41), (2, 2)],
        )
        self.assertEqual(
            [record.offset for partition, record in selected if partition[1] == 2],
            [0, 1, 2],
        )
        self.assertEqual(
            [record.offset for partition, record in selected if partition[1] == 3],
            [40, 41],
        )
        self.assertEqual(
            [
                record.offset
                for record in queues[("qdl.stable.canonical.phase-b.v2", 2)]
            ],
            [0, 1, 2],
        )
        self.assertEqual(
            [
                record.offset
                for record in queues[("qdl.stable.canonical.phase-b.v2", 3)]
            ],
            [40, 41],
        )

    async def test_supervisor_recreates_poisoned_generation_with_bounded_backoff(self):
        stopped = [False]
        sleeps = []
        active = []
        brokers = []
        generations = ["fail", "recover"]

        class Broker:
            def __init__(self, fail_close):
                self.fail_close = fail_close
                self.closed = 0

            def close(self):
                self.closed += 1
                if self.fail_close:
                    raise RuntimeError("injected poisoned close")

        class Engine:
            def __init__(self, outcome):
                self.outcome = outcome

            async def run_once(self, timeout_seconds):
                self.assert_timeout = timeout_seconds
                if self.outcome == "fail":
                    raise RuntimeError("injected asynchronous checkpoint failure")
                stopped[0] = True
                return True

        def factory():
            outcome = generations.pop(0)
            broker = Broker(outcome == "fail")
            brokers.append(broker)
            return broker, Engine(outcome)

        async def sleep(delay):
            sleeps.append(delay)

        await supervise_stable_projector(
            broker_factory=factory,
            should_stop=lambda: stopped[0],
            on_broker=active.append,
            sleep=sleep,
        )
        self.assertEqual(len(brokers), 2)
        self.assertEqual([broker.closed for broker in brokers], [1, 1])
        self.assertEqual(sleeps, [0.25])
        self.assertEqual(active, [brokers[0], None, brokers[1], None])

    async def test_run_once_defers_a_polled_record_at_the_batch_byte_bound(self):
        class Record:
            def __init__(self, payload):
                self.payload = payload

        class PollingBroker(_Broker):
            def __init__(self, records):
                super().__init__()
                self.records = list(records)

            def poll(self, timeout_seconds):
                del timeout_seconds
                return self.records.pop(0) if self.records else None

        first = Record(b"aaa")
        second = Record(b"bbb")
        broker = PollingBroker((first, second))
        engine = StableProjectorEngine(
            broker=broker,
            spool=self.spool,
            catalog=self.catalog,
            canonical_topic="qdl.stable.canonical.phase-b.v2",
            raw_topics=(),
            sink=LocalStableCanonicalSink(self.gateway, self.spool),
            projector=StableCompatibilityProjector(self.catalog),
            target=InMemoryStableProjectionTarget(),
            max_pending_records=10,
            max_pending_bytes=1024,
            max_batch_records=4,
            max_batch_bytes=4,
        )
        accepted = []

        async def accept_many(records):
            accepted.append(tuple(records))

        engine.accept_many = accept_many
        self.assertTrue(await engine.run_once(timeout_seconds=0.01))
        self.assertEqual(accepted, [(first,)])
        self.assertIs(engine._deferred_record, second)
        self.assertTrue(await engine.run_once(timeout_seconds=0.01))
        self.assertEqual(accepted, [(first,), (second,)])
        self.assertIsNone(engine._deferred_record)

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

    async def test_embedded_raw_lineage_projects_without_raw_cache_rows(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        inline = KafkaProjectorRecord(
            topic=canonical_record.topic,
            partition=canonical_record.partition,
            offset=canonical_record.offset,
            key=canonical_record.key,
            event_id=canonical_record.event_id,
            payload=canonical_record.payload,
            accepted_at_ns=canonical_record.accepted_at_ns,
            raw_provider_envelope=raw_record.payload,
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = StableProjectorEngine(
            broker=broker,
            spool=self.spool,
            catalog=self.catalog,
            canonical_topic=canonical_topic,
            raw_topics=(),
            sink=LocalStableCanonicalSink(self.gateway, self.spool),
            projector=StableCompatibilityProjector(self.catalog),
            target=target,
            max_pending_records=10,
            max_pending_bytes=1024 * 1024,
        )
        await engine.accept(inline)
        self.assertEqual(engine.stats.raw_committed, 0)
        self.assertEqual(engine.stats.canonical_committed, 1)
        self.assertIsNone(
            self.spool.find_event(stream=raw_topic, event_id=raw_record.event_id)
        )
        self.assertEqual(
            broker.checkpoints, [(canonical_topic, 0, canonical_record.offset)]
        )
        self.assertIn("trade:price:BTCUSDT", target.latest)

    async def test_missing_embedded_lineage_fails_closed_without_raw_subscription(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        _raw_topic, canonical_topic, _raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        engine = StableProjectorEngine(
            broker=broker,
            spool=self.spool,
            catalog=self.catalog,
            canonical_topic=canonical_topic,
            raw_topics=(),
            sink=LocalStableCanonicalSink(self.gateway, self.spool),
            projector=StableCompatibilityProjector(self.catalog),
            target=InMemoryStableProjectionTarget(),
            max_pending_records=10,
            max_pending_bytes=1024 * 1024,
        )
        with self.assertRaisesRegex(ValueError, "missing private Kafka raw lineage"):
            await engine.accept(canonical_record)
        self.assertEqual(broker.checkpoints, [])

    async def test_canonical_backpressure_pauses_before_hard_bound_and_resumes_after_raw(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        waiting = tuple(
            KafkaProjectorRecord(
                topic=canonical_record.topic,
                partition=canonical_record.partition,
                offset=offset,
                key=canonical_record.key,
                event_id=canonical_record.event_id,
                payload=canonical_record.payload,
                accepted_at_ns=canonical_record.accepted_at_ns + offset,
            )
            for offset in range(8)
        )

        await engine.accept_many(waiting)
        self.assertEqual(engine.stats.pending_canonical, 8)
        self.assertEqual(broker.flow_control, ["pause"])
        self.assertEqual(broker.checkpoints, [])

        await engine.accept(raw_record)
        self.assertEqual(engine.stats.pending_canonical, 0)
        self.assertEqual(broker.flow_control, ["pause", "resume"])
        self.assertEqual(
            broker.checkpoints,
            [(raw_topic, 0, 0)]
            + [(canonical_topic, 0, offset) for offset in range(8)],
        )

    async def test_canonical_backpressure_keeps_hard_bound_when_pause_is_violated(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, _raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        engine = self.engine(
            broker, InMemoryStableProjectionTarget(), raw_topic, canonical_topic
        )
        records = tuple(
            KafkaProjectorRecord(
                topic=canonical_record.topic,
                partition=canonical_record.partition,
                offset=offset,
                key=canonical_record.key,
                event_id=canonical_record.event_id,
                payload=canonical_record.payload,
                accepted_at_ns=canonical_record.accepted_at_ns + offset,
            )
            for offset in range(11)
        )

        await engine.accept_many(records[:10])
        with self.assertRaisesRegex(RuntimeError, "buffer exhausted"):
            await engine.accept(records[10])
        self.assertEqual(engine.stats.pending_canonical, 10)
        self.assertEqual(broker.flow_control, ["pause"])
        self.assertEqual(broker.checkpoints, [])

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
        self.assertEqual(broker.checkpoint_batches, [2, 2])
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

    async def test_provenance_only_semantic_duplicate_is_checkpointed_without_fanout(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, first = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((raw_record, first))
        publication_count = len(target.publications)

        replayed = type(event)()
        replayed.CopyFrom(event)
        replayed.partition_sequence += 99
        replay = KafkaProjectorRecord(
            topic=canonical_topic,
            partition=0,
            offset=1,
            key=first.key,
            event_id=first.event_id,
            payload=replayed.SerializeToString(deterministic=True),
            accepted_at_ns=first.accepted_at_ns + 1,
        )
        await engine.accept(replay)

        self.assertEqual(len(target.publications), publication_count)
        self.assertEqual(engine.stats.canonical_committed, 2)
        self.assertEqual(engine.stats.duplicate_projections, 1)
        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 1))

    async def test_bar_decimal_spelling_and_origin_are_semantic_duplicates(self):
        binding, raw, event = _stable_pair(
            self.catalog,
            "binance_usdm_rest_bar.json",
            "binance-usdm-btcusdt-bar-1m",
        )
        raw_topic, canonical_topic, raw_record, first = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((raw_record, first))
        publication_count = len(target.publications)

        replayed = type(event)()
        replayed.CopyFrom(event)
        replayed.bar.low.mantissa *= 10
        replayed.bar.low.scale += 1
        replayed.bar.low.source_text += "0"
        replayed.bar.origin = (
            common_pb2.BAR_ORIGIN_BACKFILLED
            if event.bar.origin != common_pb2.BAR_ORIGIN_BACKFILLED
            else common_pb2.BAR_ORIGIN_VENUE_NATIVE
        )
        replayed.canonical_payload_hash = hashlib.sha256(
            replayed.bar.SerializeToString(deterministic=True)
        ).digest()
        duplicate = KafkaProjectorRecord(
            topic=canonical_topic,
            partition=0,
            offset=1,
            key=first.key,
            event_id=first.event_id,
            payload=replayed.SerializeToString(deterministic=True),
            accepted_at_ns=first.accepted_at_ns + 1,
        )
        await engine.accept(duplicate)

        self.assertEqual(len(target.publications), publication_count)
        self.assertEqual(engine.stats.duplicate_projections, 1)
        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 1))

    async def test_bar_numeric_change_remains_a_hard_collision(self):
        binding, raw, event = _stable_pair(
            self.catalog,
            "binance_usdm_rest_bar.json",
            "binance-usdm-btcusdt-bar-1m",
        )
        raw_topic, canonical_topic, raw_record, first = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((raw_record, first))

        changed = type(event)()
        changed.CopyFrom(event)
        changed.bar.close.mantissa += 1
        changed.bar.close.source_text = str(
            CanonicalDecimal(
                changed.bar.close.mantissa,
                changed.bar.close.scale,
                changed.bar.close.source_text,
            ).as_decimal()
        )
        changed.canonical_payload_hash = hashlib.sha256(
            changed.bar.SerializeToString(deterministic=True)
        ).digest()
        conflicting = KafkaProjectorRecord(
            topic=canonical_topic,
            partition=0,
            offset=1,
            key=first.key,
            event_id=first.event_id,
            payload=changed.SerializeToString(deterministic=True),
            accepted_at_ns=first.accepted_at_ns + 1,
        )
        with self.assertRaisesRegex(RuntimeError, "different market semantics"):
            await engine.accept(conflicting)

        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 0))
        self.assertEqual(engine.stats.canonical_committed, 1)

    async def test_native_backfill_recovery_overlap_is_terminalized_once_before_checkpoint(self):
        (
            binding,
            raw_topic,
            canonical_topic,
            raw_record,
            native,
            native_record,
            backfill,
            accepted_at_ns,
        ) = self._native_backfill_overlap()
        quarantine_counts_at_checkpoint = []
        spool = self.spool

        class AuditingBroker(_Broker):
            def checkpoint(self, record):
                if record.topic == canonical_topic and record.offset == 1:
                    quarantine_counts_at_checkpoint.append(
                        len(spool.quarantine_records())
                    )
                super().checkpoint(record)

        broker = AuditingBroker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((raw_record, native_record))
        latest_before = dict(target.latest)
        publications_before = list(target.publications)
        candidate_record = self._canonical_record(
            topic=canonical_topic,
            key=binding.partition_key,
            envelope=backfill,
            offset=1,
            accepted_at_ns=accepted_at_ns + 1,
        )

        await engine.accept(candidate_record)

        self.assertEqual(quarantine_counts_at_checkpoint, [1])
        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 1))
        self.assertEqual(target.latest, latest_before)
        self.assertEqual(target.publications, publications_before)
        retained = self.spool.find_event(
            stream=self.catalog.canonical_stream,
            event_id=bytes(native.event_id),
        )
        self.assertIsNotNone(retained)
        retained_envelope = market_data_pb2.EventEnvelope.FromString(
            retained.event.payload
        )
        self.assertEqual(
            retained_envelope.bar.origin, common_pb2.BAR_ORIGIN_VENUE_NATIVE
        )
        quarantine = self.spool.quarantine_records()
        self.assertEqual(len(quarantine), 1)
        self.assertEqual(
            quarantine[0]["reason_code"], "RECOVERY_BACKFILL_OVERLAP_CONFLICT"
        )

        replay_broker = _Broker()
        replay_target = InMemoryStableProjectionTarget()
        replay = self.engine(
            replay_broker, replay_target, raw_topic, canonical_topic
        )
        await replay.accept(candidate_record)
        self.assertEqual(replay_broker.checkpoints, [(canonical_topic, 0, 1)])
        self.assertEqual(replay_target.publications, [])
        self.assertEqual(len(self.spool.quarantine_records()), 1)

    async def test_native_backfill_recovery_overlap_rejects_every_nonmatching_boundary(self):
        (
            binding,
            raw_topic,
            canonical_topic,
            raw_record,
            _native,
            native_record,
            backfill,
            accepted_at_ns,
        ) = self._native_backfill_overlap()
        initial = self.engine(
            _Broker(), InMemoryStableProjectionTarget(), raw_topic, canonical_topic
        )
        await initial.accept_many((raw_record, native_record))

        def hashed_copy():
            candidate = type(backfill)()
            candidate.CopyFrom(backfill)
            return candidate

        def rehash(candidate):
            candidate.canonical_payload_hash = hashlib.sha256(
                candidate.bar.SerializeToString(deterministic=True)
            ).digest()

        cases = []
        native_to_native = hashed_copy()
        native_to_native.bar.origin = common_pb2.BAR_ORIGIN_VENUE_NATIVE
        rehash(native_to_native)
        cases.append((
            "native_to_native", native_to_native, binding.partition_key, RuntimeError
        ))

        reconciled = hashed_copy()
        reconciled.bar.origin = common_pb2.BAR_ORIGIN_RECONCILED
        rehash(reconciled)
        cases.append(("reconciled", reconciled, binding.partition_key, RuntimeError))

        revised = hashed_copy()
        revised.bar.revision = 1
        rehash(revised)
        cases.append(("revised", revised, binding.partition_key, RuntimeError))

        superseded = hashed_copy()
        superseded.bar.supersedes_event_id = b"r" * 16
        rehash(superseded)
        cases.append(("supersedes", superseded, binding.partition_key, RuntimeError))

        changed_sequence = hashed_copy()
        changed_sequence.source_sequence = "unexpected-recovery-sequence"
        cases.append((
            "source_sequence", changed_sequence, binding.partition_key, RuntimeError
        ))

        missing_lineage = hashed_copy()
        missing_lineage.ClearField("raw_capture_id")
        cases.append(("missing_lineage", missing_lineage, binding.partition_key, ValueError))

        invalid_hash = hashed_copy()
        invalid_hash.bar.close.mantissa += 1
        invalid_hash.bar.close.source_text = str(
            CanonicalDecimal(
                invalid_hash.bar.close.mantissa,
                invalid_hash.bar.close.scale,
                invalid_hash.bar.close.source_text,
            ).as_decimal()
        )
        cases.append(("invalid_hash", invalid_hash, binding.partition_key, RuntimeError))

        cross_binding = hashed_copy()
        cases.append(("cross_binding", cross_binding, "wrong-binding", ValueError))

        for name, candidate, key, expected_error in cases:
            with self.subTest(name=name):
                broker = _Broker()
                engine = self.engine(
                    broker, InMemoryStableProjectionTarget(), raw_topic, canonical_topic
                )
                record = self._canonical_record(
                    topic=canonical_topic,
                    key=key,
                    envelope=candidate,
                    offset=1,
                    accepted_at_ns=accepted_at_ns + 1,
                )
                with self.assertRaises(expected_error):
                    await engine.accept(record)
                self.assertEqual(broker.checkpoints, [])
                self.assertEqual(self.spool.quarantine_records(), [])

    async def test_late_historical_bar_repairs_cache_without_latest_regression(self):
        binding = next(
            item
            for item in self.catalog.bindings
            if item.binding_id == "binance-usdm-btcusdt-bar-1m"
        )
        fixture = json.loads(
            (FIXTURE_PATH / "binance_usdm_rest_bar.json").read_text()
        )

        def pair(open_time_ms):
            raw_payload = json.loads(json.dumps(fixture["raw"]))
            raw_payload["row"][0] = open_time_ms
            raw_payload["row"][6] = open_time_ms + 59_999
            raw_bytes = json.dumps(
                raw_payload, sort_keys=True, separators=(",", ":")
            ).encode()
            received_at_ns = (open_time_ms + 60_001) * 1_000_000
            raw = capture_exact_frame(
                provider=binding.provider,
                venue=binding.instrument.identity.venue,
                market=binding.instrument.identity.market,
                product_type=binding.instrument.identity.product_type.value,
                native_symbol=binding.instrument.native_symbol,
                native_channel="rest-klines/1m",
                subscription_id=binding.source_id,
                source_session_id="phase-b-late-bar-session",
                connection_generation=1,
                lease_epoch=1,
                authority_revision=self.catalog.authority_revision,
                partition_plan_epoch=1,
                received_at_ns=received_at_ns,
                raw_frame_bytes=raw_bytes,
                adapter_version=binding.adapter_version,
                config_revision=1,
                instrument_catalog_revision=self.catalog.catalog_revision,
                correlation_id=f"phase-b-late-bar-{open_time_ms}",
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
            event = canonicalize_binance_usdm_rest_bar(
                raw_payload, bind_capture_context(TradeContext(**context), raw)
            )
            return raw, event

        older_raw, older_event = pair(1786352340000)
        later_raw, later_event = pair(1786352400000)
        raw_topic, canonical_topic, later_raw_record, later_record = _broker_records(
            binding, later_raw, later_event
        )
        _raw_topic, _canonical_topic, older_raw_record, older_record = _broker_records(
            binding, older_raw, older_event, raw_offset=1, canonical_offset=1
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((later_raw_record, later_record))
        latest_before = dict(target.latest)

        await engine.accept_many((older_raw_record, older_record))

        self.assertEqual(target.latest, latest_before)
        stored = self.spool.read_tail(
            stream=self.catalog.canonical_stream,
            partition_key=binding.partition_key,
            limit=10,
        )
        self.assertEqual(len(stored), 2)
        opens = sorted(
            market_data_pb2.EventEnvelope.FromString(
                item.event.payload
            ).bar.open_time_ns
            for item in stored
        )
        self.assertEqual(
            opens,
            [1786352340000 * 1_000_000, 1786352400000 * 1_000_000],
        )
        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 1))

    async def test_same_event_id_with_changed_market_semantics_fails_closed(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, canonical_topic, raw_record, first = _broker_records(
            binding, raw, event
        )
        broker = _Broker()
        target = InMemoryStableProjectionTarget()
        engine = self.engine(broker, target, raw_topic, canonical_topic)
        await engine.accept_many((raw_record, first))

        changed = type(event)()
        changed.CopyFrom(event)
        changed.trade.aggressor_side = (
            common_pb2.AGGRESSOR_SIDE_SELL
            if event.trade.aggressor_side == common_pb2.AGGRESSOR_SIDE_BUY
            else common_pb2.AGGRESSOR_SIDE_BUY
        )
        changed.canonical_payload_hash = hashlib.sha256(
            changed.trade.SerializeToString(deterministic=True)
        ).digest()
        conflicting = KafkaProjectorRecord(
            topic=canonical_topic,
            partition=0,
            offset=1,
            key=first.key,
            event_id=first.event_id,
            payload=changed.SerializeToString(deterministic=True),
            accepted_at_ns=first.accepted_at_ns + 1,
        )
        with self.assertRaisesRegex(
            RuntimeError, "different market semantics"
        ):
            await engine.accept(conflicting)

        self.assertEqual(broker.checkpoints[-1], (canonical_topic, 0, 0))
        self.assertEqual(engine.stats.canonical_committed, 1)

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
                "raw_provider_envelope": base64.b64encode(
                    raw_record.payload
                ).decode("ascii"),
            },
        )
        try:
            first = await sink.publish(durable)
            second = await sink.publish(durable)
            self.assertEqual(first.cursor, second.cursor)
            historical_envelope = market_data_pb2.EventEnvelope()
            historical_envelope.CopyFrom(event)
            historical_envelope.instrument_revision = 1
            historical_envelope.event_id = hashlib.sha256(
                bytes(event.event_id) + b":declared-historical-revision"
            ).digest()[:16]
            historical = DurableEvent(
                stream=self.catalog.canonical_stream,
                partition_key=binding.partition_key,
                event_id=bytes(historical_envelope.event_id),
                payload=historical_envelope.SerializeToString(deterministic=True),
                accepted_at_ns=canonical_record.accepted_at_ns,
                headers=durable.headers,
            )
            historical_stored = await sink.publish(historical)
            self.assertNotEqual(historical_stored.cursor, first.cursor)
            tampered = DurableEvent(
                stream=durable.stream,
                partition_key=durable.partition_key,
                event_id=durable.event_id,
                payload=durable.payload,
                accepted_at_ns=durable.accepted_at_ns,
                headers={
                    **durable.headers,
                    "raw_provider_envelope": base64.b64encode(
                        b"malformed-private-lineage"
                    ).decode("ascii"),
                },
            )
            with self.assertRaisesRegex(
                RuntimeError,
                "http_status=422 detail=Error parsing message with type",
            ):
                await sink.publish(tampered)
            rejected = await client.post(
                "/internal/v2/canonical/events", content=b"{}",
                headers={"X-QDL-Stable-Signature": "sha256=bad"},
            )
            self.assertEqual(rejected.status_code, 401)
        finally:
            await sink.close()
            await client.aclose()

    async def test_canonical_ingest_maps_shared_spool_capacity_to_typed_503(self):
        binding, raw, event = _stable_pair(
            self.catalog, "binance_usdm_trade.json", "binance-usdm-btcusdt-trade"
        )
        raw_topic, _canonical_topic, raw_record, canonical_record = _broker_records(
            binding, raw, event
        )
        app = FastAPI()
        secret = b"phase-b-stable-capacity-secret-32"
        install_stable_canonical_ingest(
            app, gateway=self.gateway, catalog=self.catalog,
            spool=self.spool, secret=secret,
        )
        body = json.dumps({
            "schema": "qdl.v2.stable-canonical-ingest.v1",
            "batch_id": "00000000-0000-4000-8000-000000000001",
            "events": [{
                "canonical": base64.b64encode(canonical_record.payload).decode("ascii"),
                "raw_stream": raw_topic,
                "raw_event_id": raw_record.event_id.hex(),
                "raw_provider_envelope": base64.b64encode(raw_record.payload).decode("ascii"),
            }],
        }, sort_keys=True, separators=(",", ":")).encode()
        signature = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        )
        try:
            with patch.object(
                self.gateway,
                "publish_many",
                side_effect=BackpressureRequired("bridge max_records exhausted"),
            ):
                response = await client.post(
                    "/internal/v2/canonical/events", content=body,
                    headers={"X-QDL-Stable-Signature": signature},
                )
            self.assertEqual(response.status_code, 503)
            self.assertEqual(
                response.json(),
                {"detail": "stable canonical cache capacity temporarily unavailable"},
            )
        finally:
            await client.aclose()

    async def test_signed_http_sink_chunks_by_exact_request_bytes(self):
        durable = tuple(
            DurableEvent(
                stream=self.catalog.canonical_stream,
                partition_key="stable-byte-chunk-test",
                event_id=bytes([index]) * 16,
                payload=(f"canonical-{index}".encode() * 128),
                accepted_at_ns=1,
                headers={
                    "raw_stream": "md.raw.byte-chunk-test",
                    "raw_event_id": bytes([index + 10]).hex() * 16,
                    "raw_provider_envelope": "a" * 2_000,
                },
            )
            for index in range(1, 4)
        )
        stored = tuple(self.spool.append(event) for event in durable)
        expected = {
            base64.b64encode(event.payload).decode(): (event, append)
            for event, append in zip(durable, stored, strict=True)
        }
        observed_bodies = []
        observed_event_ids = []

        async def handler(request: httpx.Request) -> httpx.Response:
            observed_bodies.append(request.content)
            payload = json.loads(request.content)
            received = [expected[item["canonical"]] for item in payload["events"]]
            observed_event_ids.extend(event.event_id for event, _ in received)
            return httpx.Response(200, json={
                "schema": "qdl.v2.stable-canonical-ingest-result.v1",
                "results": [
                    {
                        "event_id": event.event_id.hex(),
                        "offset": append.cursor.offset,
                    }
                    for event, append in received
                ],
            })

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://localhost"
        )
        sink = StableHttpCanonicalSink(
            ("http://localhost",), b"s" * 32, self.spool,
            max_request_bytes=5_000, client=client,
        )
        try:
            result = await sink.publish_many(durable)
            self.assertEqual(
                tuple(item.cursor for item in result),
                tuple(item.cursor for item in stored),
            )
            self.assertEqual(
                tuple(item.event.event_id for item in result),
                tuple(event.event_id for event in durable),
            )
            self.assertGreater(len(observed_bodies), 1)
            self.assertTrue(all(len(body) <= 5_000 for body in observed_bodies))
            self.assertEqual(
                sum(len(json.loads(body)["events"]) for body in observed_bodies), 3
            )
            self.assertEqual(
                observed_event_ids, [event.event_id for event in durable]
            )
            with self.assertRaisesRegex(
                ValueError, "event exceeds request byte bound"
            ):
                await StableHttpCanonicalSink(
                    ("http://localhost",), b"s" * 32, self.spool,
                    max_request_bytes=256, client=client,
                ).publish(durable[0])

            async def reject(request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    413,
                    json={"detail": "x" * 256},
                    request=request,
                )

            rejected_client = httpx.AsyncClient(
                transport=httpx.MockTransport(reject), base_url="http://localhost"
            )
            rejected_sink = StableHttpCanonicalSink(
                ("http://localhost",), b"s" * 32, self.spool,
                max_request_bytes=5_000, client=rejected_client,
            )
            try:
                with self.assertRaisesRegex(
                    RuntimeError,
                    "http_status=413 detail=<redacted>",
                ):
                    await rejected_sink.publish(durable[0])
            finally:
                await rejected_sink.close()
                await rejected_client.aclose()
        finally:
            await sink.close()
            await client.aclose()

    async def test_signed_http_sink_reports_fenced_and_backpressure_gateways(self):
        durable = DurableEvent(
            stream=self.catalog.canonical_stream,
            partition_key="stable-gateway-diagnostics",
            event_id=b"g" * 16,
            payload=b"canonical-diagnostics",
            accepted_at_ns=1,
            headers={
                "raw_stream": "md.raw.gateway-diagnostics",
                "raw_event_id": (b"r" * 16).hex(),
            },
        )
        responses = iter((
            httpx.Response(409, json={"detail": "stable gateway is not active"}),
            httpx.Response(
                503,
                json={"detail": "stable canonical cache capacity temporarily unavailable"},
            ),
        ))

        async def reject(_request: httpx.Request) -> httpx.Response:
            return next(responses)

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(reject), base_url="http://localhost"
        )
        sink = StableHttpCanonicalSink(
            ("http://stream_v2_active:8200", "http://stream_v2_passive:8200"),
            b"s" * 32,
            self.spool,
            client=client,
        )
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "statuses=409:stable gateway is not active,503:stable canonical cache capacity temporarily unavailable",
            ):
                await sink.publish(durable)
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
        for name in ("ca.crt", "workload.crt", "workload.key"):
            (root / name).write_text("test", encoding="utf-8")
        runtime = root / "runtime"
        runtime.mkdir()
        (runtime / "authority.json").write_text(json.dumps({
            "schema": "qdl.authority-record.v1",
            "slice_id": "qdl-v2-stable-multivenue-test",
            "revision": 1,
            "mode": "RUST_SHADOW",
            "candidate_image_digest": "sha256:" + "a" * 64,
            "capability_manifest_digest": "b" * 64,
            "contract_digest": "c" * 64,
            "partition_plan_digest": "d" * 64,
            "public_write_allowed": False,
            "legacy_write_allowed": False,
            "approved_by": "stable-runtime-boundary-test",
            "effective_at_ns": 1,
        }), encoding="utf-8")
        return {
            "QDL_ENVIRONMENT": "paper",
            "QDL_CONFIG_REVISION": "phase-b-test-1",
            "QDL_STABLE_AUTHORITY_MODE": "RUST_SHADOW",
            "QDL_STABLE_AUTHORITY_REVISION": "1",
            "QDL_STABLE_RUNTIME_DIR": str(runtime),
            "QDL_STABLE_SCHEMA_DIGEST": "e" * 64,
            "QDL_STABLE_STATE_DIR": str(root / "state"),
            "QDL_STABLE_DURABLE_STATE_DIR": str(root / "durable"),
            "QDL_STABLE_AUDIT_PATH": str(root / "state" / "audit.jsonl"),
            "QDL_STABLE_CONSUMER_MANIFESTS": str(root / "consumer.yaml"),
            "QDL_STABLE_SOURCE_BINDINGS": str(CATALOG_PATH),
            "QDL_STABLE_TLS_CA_FILE": str(root / "ca.crt"),
            "QDL_STABLE_TLS_CERT_FILE": str(root / "workload.crt"),
            "QDL_STABLE_TLS_KEY_FILE": str(root / "workload.key"),
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
        secure = StableHttpCanonicalSink(
            ("https://stream_v2_active:8200",), secret, object(), client=client
        )
        self.assertIs(secure.client, client)
        with self.assertRaisesRegex(ValueError, "workload TLS context"):
            StableHttpCanonicalSink(
                ("https://stream_v2_active:8200",), secret, object()
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
            self.assertEqual(config.request_deadline_seconds, 10.0)
            self.assertEqual(
                config.session_liveness_dir,
                root / "state" / "session-liveness",
            )
            values["QDL_STABLE_REQUEST_DEADLINE_SECONDS"] = "90"
            configured = StableRuntimeConfig.from_environment("query_v2", values)
            self.assertEqual(configured.request_deadline_seconds, 90.0)
            bounds = stable_request_bounds(configured)
            self.assertEqual(bounds.request_deadline_seconds, 90.0)
            self.assertEqual(bounds.max_request_bytes, configured.max_request_bytes)
            self.assertEqual(
                bounds.max_concurrent_requests, configured.max_concurrent_requests
            )
            values["QDL_STABLE_REQUEST_DEADLINE_SECONDS"] = "121"
            with self.assertRaisesRegex(ValueError, "request deadline"):
                StableRuntimeConfig.from_environment("query_v2", values)
            values["QDL_STABLE_REQUEST_DEADLINE_SECONDS"] = "90"
            manifest = config.public_manifest()
            self.assertEqual(manifest["contract_version"], "2.0.0")
            self.assertEqual(manifest["authority"], "RUST_SHADOW")
            self.assertFalse(manifest["writes_current_v1_redis"])
            with self.assertRaisesRegex(ValueError, "Kafka/stream dependencies"):
                StableRuntimeConfig.from_environment("projector_v2", values)
            values.update({
                "QDL_STABLE_KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092",
                "QDL_STABLE_KAFKA_CLIENT_ID": "canonical-only-projector",
                "QDL_STABLE_KAFKA_CANONICAL_TOPIC": "md.canonical.v2",
                "QDL_STABLE_KAFKA_CERT_ROOT": str(root),
                "QDL_STABLE_STREAM_INGEST_URLS_JSON": '["https://stream_v2_active:8200"]',
            })
            projector = StableRuntimeConfig.from_environment("projector_v2", values)
            self.assertEqual(projector.kafka_raw_topics, ())
            values.update({
                "QDL_STABLE_MAX_PENDING_RECORDS": "2048",
                "QDL_STABLE_MAX_PENDING_BYTES": "33554432",
                "QDL_STABLE_PROJECTOR_MAX_BATCH_RECORDS": "1000",
                "QDL_STABLE_PROJECTOR_MAX_BATCH_BYTES": "8388608",
            })
            bounded_projector = StableRuntimeConfig.from_environment(
                "projector_v2", values
            )
            self.assertEqual(bounded_projector.max_pending_records, 2048)
            self.assertEqual(bounded_projector.max_pending_bytes, 33_554_432)
            self.assertEqual(bounded_projector.projector_max_batch_records, 1000)
            self.assertEqual(bounded_projector.projector_max_batch_bytes, 8_388_608)
            values["QDL_STABLE_PROJECTOR_MAX_BATCH_RECORDS"] = "1001"
            with self.assertRaisesRegex(ValueError, "projector batch bound"):
                StableRuntimeConfig.from_environment("projector_v2", values)
            values["QDL_STABLE_PROJECTOR_MAX_BATCH_RECORDS"] = "1000"
            values["QDL_STABLE_MAX_PENDING_RECORDS"] = "64"
            with self.assertRaisesRegex(ValueError, "pending records"):
                StableRuntimeConfig.from_environment("projector_v2", values)
            values["QDL_STABLE_MAX_PENDING_RECORDS"] = "2048"
            values["QDL_STABLE_PROJECTOR_MAX_BATCH_BYTES"] = "33554433"
            with self.assertRaisesRegex(ValueError, "batch byte bound"):
                StableRuntimeConfig.from_environment("projector_v2", values)
            values["QDL_STABLE_PROJECTOR_MAX_BATCH_BYTES"] = "8388608"
            authority_path = Path(values["QDL_STABLE_RUNTIME_DIR"]) / "authority.json"
            primary = json.loads(authority_path.read_text(encoding="utf-8"))
            primary.update({"mode": "RUST_PRIMARY", "revision": 2})
            authority_path.write_text(json.dumps(primary), encoding="utf-8")
            values.update({
                "QDL_STABLE_AUTHORITY_MODE": "RUST_PRIMARY",
                "QDL_STABLE_AUTHORITY_REVISION": "2",
            })
            primary_config = StableRuntimeConfig.from_environment("query_v2", values)
            self.assertEqual(primary_config.authority_mode, "RUST_PRIMARY")
            self.assertEqual(primary_config.authority_revision, 2)
            values["QDL_STABLE_AUTHORITY_MODE"] = "RUST_SHADOW"
            with self.assertRaisesRegex(ValueError, "differs from generated record"):
                StableRuntimeConfig.from_environment("query_v2", values)

    def test_provider_admission_url_is_limited_to_private_rust_core(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            values = self.environment(root)
            values["QDL_STABLE_PROVIDER_ADMISSION_URL"] = "http://rust_core:8300"
            config = StableRuntimeConfig.from_environment("query_v2", values)
            self.assertEqual(config.provider_admission_url, "http://rust_core:8300")
            values["QDL_STABLE_PROVIDER_ADMISSION_URL"] = "http://provider.example:8300"
            with self.assertRaisesRegex(ValueError, "private rust_core"):
                StableRuntimeConfig.from_environment("query_v2", values)

    def test_query_client_trust_may_be_additive_without_changing_server_trust(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            values = self.environment(root)
            client_ca = root / "external-client-ca-bundle.crt"
            client_ca.write_text("test", encoding="utf-8")
            values["QDL_STABLE_TLS_CLIENT_CA_FILE"] = str(client_ca)
            config = StableRuntimeConfig.from_environment("query_v2", values)
            self.assertEqual(config.tls_ca_path, root / "ca.crt")
            self.assertEqual(config.tls_client_authority_path, client_ca)
            self.assertEqual(
                stable_uvicorn_tls(config)["ssl_ca_certs"], str(client_ca)
            )
            with patch("qdl.runtime.stable.grpc.ssl_server_credentials") as factory:
                stable_grpc_server_credentials(config)
            self.assertEqual(
                factory.call_args.kwargs["root_certificates"], client_ca.read_bytes()
            )


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
            cache_id = "12" * 16
            target.bind_cache(cache_id, initialize_if_missing=True)
            self.assertTrue(target.cache_is_bound())
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

            mismatched = RedisStableProjectionTarget(
                client, namespace=self.namespace, dedicated_database=True
            )
            with self.assertRaises(ProjectionCacheMismatch):
                mismatched.bind_cache("34" * 16, initialize_if_missing=False)

            client.delete(target.cache_identity_key)
            self.assertFalse(target.cache_is_bound())
            changed = StableProjectionRecord(
                partition_key=record.partition_key,
                offset=3,
                event_id_hex="cd" * 16,
                shard_id=record.shard_id,
                lease_epoch=2,
                items=record.items,
                publications=record.publications,
            )
            with self.assertRaises(ProjectionCacheMismatch):
                target.apply(changed)
            with self.assertRaises(ProjectionCacheMismatch):
                target.bind_cache(cache_id, initialize_if_missing=False)
        finally:
            pubsub.close()
            scoped = [current, last, *client.scan_iter(match=f"{self.namespace}:*")]
            if scoped:
                client.delete(*scoped)
            client.close()


if __name__ == "__main__":
    unittest.main()

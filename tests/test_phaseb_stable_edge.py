from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import yaml

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
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_source import (
    StableConsumerCursorIssuer,
    StableSpoolQueryBackend,
)
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig


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


if __name__ == "__main__":
    unittest.main()

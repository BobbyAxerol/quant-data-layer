"""DL v2.3.0 slice 1: DERIVED_FINAL bars aggregated from finer final bars.

Covers, without any service:

* the interval anchors used for grouping (`bar_bucket_open_ms`): Monday weeks
  on both venues, the Binance Monday 3d grid, OKX Unix 2d/3d, UTC 12h, and
  calendar months refused;
* warmup resample (`resample_final_bars`) on those anchors and its parity with
  OKX native candles from the committed public capture
  (`tests/fixtures/okx_derived_bar_parity_ldo_20261112.json`), including the
  zero-volume rule that capture exposed;
* the `DERIVED_FINAL` acquisition schema, plan chain validation and the
  `derived_bar` core mapping;
* the bar edge deriver against an in-process market cache (`FakeRedis` in the
  exact `apply.lua` layout): publish, idempotent skip, native row kept, wait,
  gap after grace, gap healed, revision superseding, and the raw record the
  Rust core consumes (shared golden `tests/fixtures/derived_bar_record_v1.json`).

The Rust side of the same fixture and golden is
`rust/qdl-core/src/derived_bar.rs` (tests) and the core routing tests in
`rust/qdl-realtime-core/src/lib.rs`.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.adapters.intervals import bar_bucket_open_ms, canonical_interval_ms
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.query import FeedType
from qdl.query.lifecycle import BarLifecycle
from qdl.query.results import ContractMetadata, MarketDataItem, QualityMetadata, SourceMetadata
from qdl.runtime import derived_bars
from qdl.runtime.kn_bar_readback import DurableBarRow, KnBarReadback
from qdl.runtime.stable_bar_edge import StableBinanceBarEdge
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    StableDerivedBarAcquisition,
)
from qdl.runtime.stable_source import bar_item_fields
from qdl.warmup.handoff import resample_final_bars
from tests.test_kn_bar_readback import FakeRedis, build_partitions, put_rows

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
FIXTURE = ROOT / "tests/fixtures/okx_derived_bar_parity_ldo_20261112.json"
RECORD_GOLDEN = ROOT / "tests/fixtures/derived_bar_record_v1.json"
DAY = 86_400_000
MONDAY = 4 * DAY  # 1970-01-05, the first Monday after the Unix epoch
ENVIRONMENT = "paper"
OKX_NATIVE = {
    "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "2h": "2H",
    "4h": "4H", "6h": "6Hutc", "12h": "12Hutc", "1d": "1Dutc",
}


class IntervalAnchorTests(unittest.TestCase):
    def test_weeks_open_on_monday_on_both_venues(self):
        monday = 1_794_182_400_000  # 2026-11-09 00:00Z, a Monday
        self.assertEqual((monday // DAY + 3) % 7, 0)
        for provider in ("OKX", "BINANCE", None):
            self.assertEqual(bar_bucket_open_ms("1w", monday, provider=provider), monday)
            self.assertEqual(bar_bucket_open_ms("1w", monday + 6 * DAY + 1, provider=provider), monday)
            self.assertEqual(bar_bucket_open_ms("1w", monday - 1, provider=provider), monday - 7 * DAY)
        # The Unix grid would have put this week on a Thursday.
        self.assertNotEqual(monday // (7 * DAY) * 7 * DAY, monday)

    def test_multi_day_and_half_day_anchors(self):
        start = 1_794_441_600_000  # 2026-11-12 00:00Z
        self.assertEqual(bar_bucket_open_ms("2d", start, provider="OKX") % (2 * DAY), 0)
        self.assertEqual(bar_bucket_open_ms("3d", start, provider="OKX") % (3 * DAY), 0)
        binance_3d = bar_bucket_open_ms("3d", start, provider="BINANCE")
        self.assertEqual((binance_3d - MONDAY) % (3 * DAY), 0)
        self.assertNotEqual(binance_3d % (3 * DAY), 0)
        self.assertEqual(bar_bucket_open_ms("12h", start + 13 * 3_600_000, provider="OKX"),
                         start + 12 * 3_600_000)

    def test_calendar_month_is_not_an_interval(self):
        with self.assertRaises(ValueError):
            bar_bucket_open_ms("1M", 1_794_441_600_000)
        with self.assertRaises(ValueError):
            bar_bucket_open_ms("1h", -1)


def _okx_item(row: list[str], interval: str) -> MarketDataItem:
    open_ms = int(row[0])
    step = canonical_interval_ms(interval)
    return MarketDataItem(
        instrument_uid="uid-ldo",
        instrument_id="OKX:SWAP:PERPETUAL:LDO-USDT",
        instrument_revision=1,
        feed=FeedType.BAR,
        interval=interval,
        observed_at_ns=(open_ms + step) * 1_000_000 - 1,
        payload={
            "open_time_ns": open_ms * 1_000_000,
            "close_time_ns": (open_ms + step - 1) * 1_000_000,
            "open": row[1], "high": row[2], "low": row[3], "close": row[4],
            "volume": row[5], "volume_unit": "CONTRACT", "base_volume": row[6],
            "quote_volume": row[7], "contract_volume": row[5], "trade_count": 0,
            "origin": "VENUE_NATIVE", "is_final": True,
        },
        source=SourceMetadata("OKX", "OKX_DIRECT", "source", "PRIMARY", True),
        quality=QualityMetadata("LIVE", 0, False, True, True, "crypto_primary_v2"),
        contract=ContractMetadata("a" * 64, "2.0.0", "normalizer", "adapter", 1, 1, 1, 1, "c"),
        revision=0,
        bar_lifecycle=BarLifecycle.FINAL,
    )


def _same(item: MarketDataItem, native: list[str]) -> bool:
    fields = ("open", "high", "low", "close", "volume", "base_volume", "quote_volume")
    return all(
        Decimal(item.payload[name]) == Decimal(native[index])
        for index, name in enumerate(fields, start=1)
    )


class WarmupResampleParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.doc = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_fixture_is_a_bounded_public_capture(self):
        self.assertEqual(self.doc["schema"], "qdl.test.okx-derived-bar-parity.v1")
        self.assertEqual(len(self.doc["candles"]["1m"]), 1_440)
        self.assertTrue(all(row[8] == "1" for rows in self.doc["candles"].values() for row in rows))
        zero = sum(1 for row in self.doc["candles"]["1m"] if Decimal(row[5]) == 0)
        self.assertGreater(zero, 0, "the capture must exercise zero-volume minutes")

    def test_every_intraday_native_interval_equals_the_resampled_minutes(self):
        minutes = tuple(_okx_item(row, "1m") for row in self.doc["candles"]["1m"])
        compared = 0
        for interval, native_bar in OKX_NATIVE.items():
            derived = {item.payload["open_time_ns"] // 1_000_000: item
                       for item in resample_final_bars(minutes, target_interval=interval)}
            for native in self.doc["candles"][native_bar]:
                item = derived[int(native[0])]
                self.assertTrue(_same(item, native), (interval, native, item.payload))
                self.assertEqual(item.resample_lineage.constituent_count,
                                 canonical_interval_ms(interval) // 60_000)
                compared += 1
        self.assertEqual(compared, 961)

    def test_calendar_bars_from_days_use_unix_and_monday_anchors(self):
        days = tuple(_okx_item(row, "1d") for row in self.doc["candles"]["1Dutc_calendar"])
        compared = 0
        for interval, native_bar in (("2d", "2Dutc"), ("3d", "3Dutc"), ("1w", "1Wutc")):
            derived = {item.payload["open_time_ns"] // 1_000_000: item
                       for item in resample_final_bars(days, target_interval=interval)}
            for native in self.doc["candles"][native_bar]:
                self.assertTrue(_same(derived[int(native[0])], native), (interval, native))
                compared += 1
            # No partial bucket at either edge of the window is ever emitted.
            self.assertEqual(len(derived), len(self.doc["candles"][native_bar]))
        self.assertEqual(compared, 27)
        weeks = resample_final_bars(days, target_interval="1w")
        self.assertTrue(all((item.payload["open_time_ns"] // 1_000_000 - MONDAY) % (7 * DAY) == 0
                            for item in weeks))

    def test_zero_volume_minutes_carry_no_price(self):
        rows = [r for r in self.doc["candles"]["1m"] if r[0] == "1794451680000"]
        start = self.doc["candles"]["1m"].index(rows[0])
        minutes = tuple(_okx_item(row, "1m") for row in self.doc["candles"]["1m"][start:start + 3])
        self.assertEqual(Decimal(minutes[0].payload["volume"]), 0)
        (bar,) = resample_final_bars(minutes, target_interval="3m")
        native = next(r for r in self.doc["candles"]["3m"] if r[0] == "1794451680000")
        self.assertTrue(_same(bar, native))
        self.assertNotEqual(bar.payload["open"], minutes[0].payload["open"])

    def test_revision_is_the_sum_of_constituent_revisions(self):
        minutes = [_okx_item(row, "1m") for row in self.doc["candles"]["1m"][:3]]
        minutes[1] = replace(minutes[1], revision=1)
        minutes[2] = replace(minutes[2], revision=2)
        (bar,) = resample_final_bars(minutes, target_interval="3m")
        self.assertEqual(bar.revision, 3)


class AcquisitionSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.plan = StableAcquisitionPlan.load(ACQUISITION_PATH, catalog=cls.catalog)
        cls.sources = {item.binding_id: item for item in cls.catalog.bindings}
        cls.by_id = {item.binding_id: item for item in cls.plan.bindings}

    def derived(self, binding_id: str, base: str):
        return replace(
            self.by_id[binding_id], mode="DERIVED_FINAL", websocket_url=None,
            business_websocket_url=None, market_websocket_url=None,
            derived_from=StableDerivedBarAcquisition(interval=base),
        )

    def plan_with(self, *items):
        replaced = {item.binding_id: item for item in items}
        return replace(self.plan, bindings=tuple(replaced.get(item.binding_id, item)
                                                 for item in self.plan.bindings))

    def test_valid_okx_derivation_and_refusals(self):
        binding = self.derived("okx-swap-eth-usdt-swap-bar-1h", "30m")
        binding.validate(self.sources[binding.binding_id])
        cases = {
            "finer": replace(binding, derived_from=StableDerivedBarAcquisition("2h")),
            "divide": replace(binding, derived_from=StableDerivedBarAcquisition("7m")),
            "channel": replace(binding, native_channel="candle1m"),
            "socket": replace(binding, websocket_url="wss://ws.okx.com/ws/v5/public"),
            "derived_from": replace(binding, derived_from=None),
        }
        for name, case in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                case.validate(self.sources[binding.binding_id])
        week = self.derived("okx-swap-eth-usdt-swap-bar-1w", "2d")
        with self.assertRaises(ValueError):
            week.validate(self.sources[week.binding_id])
        native = replace(self.by_id["okx-swap-eth-usdt-swap-bar-1h"],
                         derived_from=StableDerivedBarAcquisition("30m"))
        with self.assertRaises(ValueError):
            native.validate(self.sources[native.binding_id])

    def test_chain_parents_and_core_mapping(self):
        plan = self.plan_with(
            self.derived("okx-swap-eth-usdt-swap-bar-1h", "30m"),
            self.derived("okx-swap-eth-usdt-swap-bar-1w", "1d"),
            self.derived("okx-swap-eth-usdt-swap-bar-1d", "12h"),
        )
        parents = plan.derived_parents(self.sources)
        self.assertEqual(parents["okx-swap-eth-usdt-swap-bar-1h"], "okx-swap-eth-usdt-swap-bar-30m")
        self.assertEqual(parents["okx-swap-eth-usdt-swap-bar-1w"], "okx-swap-eth-usdt-swap-bar-1d")
        authority = _authority()
        core = plan.core_config(catalog=self.catalog, authority=authority,
                                binding_ids=frozenset(parents))["core"]["bindings"]
        mapping = {item["source_id"]: item.get("derived_bar") for item in core}
        week = mapping[self.sources["okx-swap-eth-usdt-swap-bar-1w"].source_id]
        self.assertEqual(week, {
            "interval": "1w", "base_interval": "1d",
            "base_source_id": self.sources["okx-swap-eth-usdt-swap-bar-1d"].source_id,
            "anchor_ms": MONDAY,
        })
        hour = next(item for item in core
                    if item["source_id"] == self.sources["okx-swap-eth-usdt-swap-bar-1h"].source_id)
        self.assertEqual((hour["native_channel"], hour["provider_kind"]), ("candle1H", "okx_bar"))
        # Derived bindings are never subscribed by an ingestor.
        ingestors = plan.native_ingestor_configs(catalog=self.catalog, authority=authority)
        subscribed = {item["subscription_id"] for value in ingestors.values() for item in value["bindings"]}
        self.assertFalse(subscribed & {self.sources[key].source_id for key in parents})

    def test_enabled_child_needs_an_enabled_parent(self):
        plan = self.plan_with(
            self.derived("okx-swap-eth-usdt-swap-bar-1h", "30m"),
            replace(self.by_id["okx-swap-eth-usdt-swap-bar-30m"], enabled=False),
        )
        with self.assertRaisesRegex(ValueError, "disabled parent"):
            plan.derived_parents(self.sources)

    def test_existing_plan_identity_digest_is_unchanged(self):
        from dataclasses import asdict

        bindings = []
        for item in sorted(self.plan.bindings, key=lambda value: value.binding_id):
            payload = asdict(item)
            for name in self.plan.TRANSPORT_FIELDS + ("derived_from",):
                payload.pop(name)
            bindings.append(payload)
        legacy = hashlib.sha256(json.dumps({
            "schema": self.plan.schema, "revision": self.plan.revision,
            "topics": [self.plan.raw_topic, self.plan.canonical_topic, self.plan.quarantine_topic],
            "bindings": bindings,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(self.plan.identity_digest(), legacy)

    def test_yaml_loader_reads_derived_from(self):
        document = yaml.safe_load(ACQUISITION_PATH.read_text(encoding="utf-8"))
        for row in document["bindings"]:
            if row["binding_id"] == "okx-swap-eth-usdt-swap-bar-1h":
                row.update(mode="DERIVED_FINAL", websocket_url=None, business_websocket_url=None,
                           derived_from={"interval": "30m"})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "acquisition.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            plan = StableAcquisitionPlan.load(path, catalog=self.catalog)
            row = next(item for item in plan.bindings if item.binding_id == "okx-swap-eth-usdt-swap-bar-1h")
            self.assertEqual(row.derived_from.interval, "30m")
            for bad in ({"interval": "30m", "x": 1}, "30m"):
                for item in document["bindings"]:
                    if item["binding_id"] == "okx-swap-eth-usdt-swap-bar-1h":
                        item["derived_from"] = bad
                path.write_text(yaml.safe_dump(document), encoding="utf-8")
                with self.assertRaises(ValueError):
                    StableAcquisitionPlan.load(path, catalog=self.catalog)


def _authority() -> dict:
    from qdl.runtime.stable_deployment import stable_authority_record

    return stable_authority_record(
        rust_image_digest="a" * 64,
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=ACQUISITION_PATH.read_bytes(),
        effective_at_ns=1,
    )


def _envelope(source, open_ms: int, row, *, revision: int = 0,
              origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE, event_id: bytes | None = None) -> bytes:
    """A canonical BAR of ``source``'s full identity (the cache row body)."""

    identity = source.instrument.identity
    step = canonical_interval_ms(source.interval)
    envelope = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.bar", schema_major=2,
        event_id=event_id or hashlib.sha256(f"{source.source_id}:{open_ms}:{revision}".encode()).digest()[:16],
        instrument_uid=source.instrument.instrument_uid, instrument_id=source.instrument.instrument_id,
        instrument_revision=source.instrument.metadata_revision, venue=identity.venue,
        market=identity.market, product_type=identity.product_type.value,
        native_symbol=source.instrument.native_symbol, provider=source.provider,
        source_id=source.source_id,
        source_role=getattr(common_pb2, f"SOURCE_ROLE_{source.source_role}"),
        source_event_time_ns=open_ms * 1_000_000, source_sequence=f"{open_ms}:1",
        quality_flags=[common_pb2.QUALITY_FLAG_FIELD_MISSING],
    )
    bar = envelope.bar
    bar.interval = source.interval
    bar.open_time_ns = open_ms * 1_000_000
    bar.close_time_ns = (open_ms + step - 1) * 1_000_000
    for name, text in zip(("open", "high", "low", "close", "volume"), row[1:6]):
        getattr(bar, name).CopyFrom(_decimal(text))
    bar.base_volume.CopyFrom(_decimal(row[6]))
    bar.quote_volume.CopyFrom(_decimal(row[7]))
    bar.contract_volume.CopyFrom(_decimal(row[5]))
    bar.is_final = True
    bar.revision = revision
    bar.origin = origin
    bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_FINAL
    bar.volume_unit = common_pb2.QUANTITY_UNIT_CONTRACT
    return envelope.SerializeToString()


def _decimal(text: str) -> common_pb2.DecimalValue:
    whole, _, fraction = text.partition(".")
    return common_pb2.DecimalValue(mantissa=int(whole + fraction), scale=len(fraction), source_text=text)


class _Publisher:
    def __init__(self) -> None:
        self.published: list[raw_provider_pb2.RawProviderEnvelope] = []

    def publish_many(self, values):
        values = list(values)
        self.published.extend(values)
        return tuple(range(len(values)))


class DeriverTests(unittest.TestCase):
    """`run_derived_cycle` on a lightweight edge over the real catalog."""

    PARENT = "okx-swap-eth-usdt-swap-bar-1m"
    CHILD = "okx-swap-eth-usdt-swap-bar-3m"
    START = 1_794_441_600_000

    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.plan = StableAcquisitionPlan.load(ACQUISITION_PATH, catalog=cls.catalog)
        cls.sources = {item.binding_id: item for item in cls.catalog.bindings}
        cls.minutes = json.loads(FIXTURE.read_text(encoding="utf-8"))["candles"]["1m"]

    def setUp(self) -> None:
        self.redis = FakeRedis()
        build_partitions(self.redis, ENVIRONMENT)
        self.parent = self.sources[self.PARENT]
        self.child = self.sources[self.CHILD]
        acquisition = next(item for item in self.plan.bindings if item.binding_id == self.CHILD)
        self.acquisition = replace(
            acquisition, mode="DERIVED_FINAL", websocket_url=None, business_websocket_url=None,
            derived_from=StableDerivedBarAcquisition("1m"),
        )
        self.acquisition.validate(self.child)
        self.publisher = _Publisher()
        self.now = (self.START + 6 * 60_000) / 1000 + 1.0
        edge = StableBinanceBarEdge.__new__(StableBinanceBarEdge)
        edge.bar_readback = KnBarReadback(self.redis, ENVIRONMENT)
        edge.derived_bindings = ((self.child, self.acquisition),)
        edge._derived_parent = {self.CHILD: self.parent}
        edge._derived_active = True
        edge._derived_next_at = {}
        edge._derived_gaps = {}
        edge.derived_published = 0
        edge.derived_gaps_recorded = 0
        edge._history_bootstrap_active = True
        edge._last_open_ms = {self.CHILD: self.START - 180_000, self.PARENT: self.START + 5 * 60_000}
        edge.max_catchup_rows = 1000
        edge.publisher = self.publisher
        edge.authority = {"revision": 7}
        edge._authority_revision = 7
        edge.connection_generation = 11
        edge.acquisition = self.plan
        edge.catalog = self.catalog
        edge.clock = lambda: self.now
        edge.persisted = 0
        edge._persist_state = lambda: setattr(edge, "persisted", edge.persisted + 1)
        self.edge = edge
        self.parent_rows: list[bytes] = []

    def put_minutes(self, indexes, *, revision: int = 0):
        rows = [_envelope(self.parent, int(self.minutes[i][0]), self.minutes[i], revision=revision)
                for i in indexes]
        put_rows(self.redis, ENVIRONMENT, self.parent, 1, rows)

    def published_records(self):
        return [json.loads(item.raw_frame_bytes) for item in self.publisher.published]

    def test_publishes_complete_buckets_and_moves_the_watermark(self):
        self.put_minutes(range(6))
        self.assertEqual(self.edge.run_derived_cycle(), 2)
        records = self.published_records()
        self.assertEqual([item["open_time_ms"] for item in records], [self.START, self.START + 180_000])
        first = self.publisher.published[0]
        self.assertEqual(first.native_channel, "candle3m")
        self.assertEqual(first.subscription_id, self.child.source_id)
        self.assertEqual(first.transport_protocol, raw_provider_pb2.TRANSPORT_PROTOCOL_FILE_REPLAY)
        self.assertEqual(first.capture_boundary, raw_provider_pb2.CAPTURE_BOUNDARY_REPLAY_BYTES)
        self.assertEqual(records[0]["base_source_id"], self.parent.source_id)
        self.assertEqual(len(records[0]["constituents"]), 3)
        self.assertEqual(self.edge._last_open_ms[self.CHILD], self.START + 180_000)
        self.assertEqual(self.edge.persisted, 1)

    def test_waits_then_records_a_gap_then_heals_it(self):
        self.put_minutes([0, 2, 3, 4, 5])
        self.now = (self.START + 3 * 60_000) / 1000 + 10.0  # inside the grace
        self.assertEqual(self.edge.run_derived_cycle(), 0)
        self.assertEqual(self.edge._last_open_ms[self.CHILD], self.START - 180_000)
        self.now = (self.START + 6 * 60_000) / 1000 + 121.0
        self.edge._derived_next_at.clear()
        self.assertEqual(self.edge.run_derived_cycle(), 1, "the later complete bucket publishes")
        self.assertIn(self.START, self.edge._derived_gaps[self.CHILD])
        self.assertEqual(self.edge.derived_gaps_recorded, 1)
        self.assertEqual([item["open_time_ms"] for item in self.published_records()], [self.START + 180_000])
        # Native gap repair fills the minute; the recorded gap is derived.
        self.put_minutes([1])
        self.now += 31.0
        self.edge._derived_next_at.clear()
        self.assertEqual(self.edge.run_derived_cycle(), 1)
        self.assertEqual(self.published_records()[-1]["open_time_ms"], self.START)
        self.assertEqual(self.edge._derived_gaps[self.CHILD], {})

    def test_never_replaces_durable_rows_except_by_a_higher_aggregated_revision(self):
        self.put_minutes(range(6))
        native = _envelope(self.child, self.START, ["0", "1", "1", "1", "1", "1", "1", "1"])
        aggregated = _envelope(self.child, self.START + 180_000, ["0", "1", "1", "1", "1", "1", "1", "1"],
                               origin=common_pb2.BAR_ORIGIN_AGGREGATED, event_id=b"A" * 16)
        put_rows(self.redis, ENVIRONMENT, self.child, 1, [native, aggregated])
        self.assertEqual(self.edge.run_derived_cycle(), 0)
        self.assertEqual(self.edge._last_open_ms[self.CHILD], self.START + 180_000)
        # A constituent correction raises the revision: the aggregated row is
        # superseded, the venue history row is still kept.
        self.put_minutes([4], revision=1)
        self.edge._last_open_ms[self.CHILD] = self.START - 180_000
        self.edge._derived_next_at.clear()
        self.assertEqual(self.edge.run_derived_cycle(), 1)
        (record,) = self.published_records()
        self.assertEqual(record["open_time_ms"], self.START + 180_000)
        self.assertEqual(record["supersedes_event_id"], (b"A" * 16).hex())

    def test_parent_not_bootstrapped_defers_without_gap(self):
        self.edge._last_open_ms.pop(self.PARENT)
        self.edge.bar_readback = KnBarReadback(self.redis, ENVIRONMENT)
        self.now += 600
        self.assertEqual(self.edge.run_derived_cycle(), 0)
        self.assertEqual(self.edge._derived_gaps.get(self.CHILD, {}), {})
        self.assertGreater(self.edge._derived_next_at[self.CHILD], self.now)

    def test_record_bytes_match_the_shared_golden(self):
        rows = tuple(
            DurableBarRow(
                market_data_pb2.EventEnvelope.FromString(
                    _envelope(self.parent, int(self.minutes[i][0]), self.minutes[i],
                              event_id=bytes([i + 1]) * 16)),
                100 + i,
            )
            for i in range(3)
        )
        frame = derived_bars.derived_record_bytes(
            interval="3m", base_interval="1m", base_source_id=self.parent.source_id,
            native_symbol=self.child.instrument.native_symbol, open_ms=self.START, constituents=rows,
        )
        if os.environ.get("QDL_REGENERATE_DERIVED_GOLDEN") == "1":  # pragma: no cover
            RECORD_GOLDEN.write_bytes(frame + b"\n")
        self.assertEqual(frame + b"\n", RECORD_GOLDEN.read_bytes())
        self.assertEqual(json.loads(frame)["schema"], derived_bars.DERIVED_BAR_SCHEMA)

    def test_aggregated_row_keeps_its_lineage_through_the_cache_codec(self):
        payload = _envelope(self.child, self.START, self.minutes[0],
                            origin=common_pb2.BAR_ORIGIN_AGGREGATED)
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
        envelope.bar.aggregation.base_interval = "1m"
        envelope.bar.aggregation.constituent_count = 3
        put_rows(self.redis, ENVIRONMENT, self.child, 1, [envelope.SerializeToString()])
        rows = self.edge.bar_readback.final_bar_rows(self.child, frozenset({self.START}))
        self.assertEqual(rows[self.START].envelope.bar.aggregation.constituent_count, 3)

    def test_query_item_exposes_the_canonical_lineage(self):
        envelope = market_data_pb2.EventEnvelope.FromString(
            _envelope(self.child, self.START, self.minutes[0], origin=common_pb2.BAR_ORIGIN_AGGREGATED))
        envelope.bar.aggregation.base_interval = "1m"
        envelope.bar.aggregation.constituent_count = 3
        envelope.bar.aggregation.constituent_sha256 = b"\x01" * 32
        envelope.bar.aggregation.first_constituent_offset = 5
        envelope.bar.aggregation.last_constituent_offset = 9
        fields = bar_item_fields(envelope)
        self.assertEqual(fields["payload"]["origin"], "AGGREGATED")
        lineage = fields["resample_lineage"]
        self.assertEqual((lineage.base_interval, lineage.constituent_count), ("1m", 3))
        self.assertEqual((lineage.first_watermark, lineage.last_watermark), (5, 9))
        self.assertIsNone(bar_item_fields(market_data_pb2.EventEnvelope.FromString(
            _envelope(self.child, self.START, self.minutes[0])))["resample_lineage"])


if __name__ == "__main__":
    unittest.main()

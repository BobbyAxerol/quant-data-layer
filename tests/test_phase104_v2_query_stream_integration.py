"""Phase 10.4-D source-only V2 query/stream integration evidence.

All provider-like payloads in this module are explicit synthetic test
provenance.  The test proves contract/identity/replay behavior only; it never
opens a provider socket or writes to a shared runtime.
"""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
import unittest
from pathlib import Path

from qdl.api_v2.router import _market_item
from qdl.canonical.book import canonicalize_okx_book
from qdl.canonical.reference import (
    ReferenceProjectionError,
    canonicalize_reference_observation,
)
from qdl.canonical.trade import TradeContext
from qdl.common.v1 import common_pb2
from qdl.domain.capabilities import CapabilityAvailability, FeedCapability
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.l2 import (
    BookConfig,
    BookIdentity,
    BookLevelInput,
    ChecksumEvidence,
    ChecksumPolicy,
    L2BookReference,
    SequencePolicy,
    SnapshotOrigin,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    ConsumerGrade,
    DataRequirement as DomainRequirement,
    DataProduct,
    FeedType,
    InstrumentQuery,
    V2QueryService,
)
from qdl.reference.contracts import (
    BasisSeries,
    LongShortKind,
    ReferenceBatchResult,
    ReferenceCoverage,
    ReferenceField,
    ReferenceLineage,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
)
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_source import (
    StableConsumerCursorIssuer,
    StableGrpcSnapshotLoader,
    StableSpoolQueryBackend,
)
from qdl.stream import DurableStreamGateway, SlowConsumer
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl_sdk import (
    DataRequirement as SdkRequirement,
    Feed,
    GapPolicy,
    Grade,
    RecoveryPolicy,
    StalePolicy,
    StreamEvent,
    market_data_view_from_stream,
)


NOW = 1_800_000_100_000_000_000
STREAM = "md.canonical.phase104d.v2"


def _record(
    *, venue: str, market: str, native_symbol: str, base: str
) -> InstrumentRecord:
    identity = InstrumentIdentity.create(
        venue=venue,
        market=market,
        product_type=ProductType.PERPETUAL,
        canonical_symbol=f"{base}-USDT",
    )
    return InstrumentRecord(
        identity=identity,
        metadata_revision=7,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=native_symbol,
        base_asset=base,
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


def _binding(
    record: InstrumentRecord,
    feed: FeedType,
    *,
    provider: str,
    source_id: str,
    source_role: str,
    interval: str | None = None,
    authoritative: bool = False,
    stale_after_ms: int = 60_000,
    freshness_basis: str = "SOURCE_EVENT",
) -> StableSourceBinding:
    return StableSourceBinding(
        binding_id=f"phase104d-{record.native_symbol.lower()}-{feed.value.lower()}-{interval or 'latest'}",
        instrument=record,
        provider=provider,
        source_id=source_id,
        source_role=source_role,
        source_policy_id=f"phase104d-{source_id.lower()}",
        authoritative=authoritative,
        adapter_version=f"phase104d/{source_role.lower()}",
        normalizer_version="phase104d-core/1",
        feed=feed,
        interval=interval,
        stale_after_ms=stale_after_ms,
        require_final_bar=False,
        continuous_calendar=True,
        v1_compatibility="NONE",
        canonical_stream=STREAM,
        freshness_basis=freshness_basis,
    )


def _context(binding: StableSourceBinding, *, sequence: int) -> TradeContext:
    raw = hashlib.sha256(f"phase104d-raw-{binding.binding_id}-{sequence}".encode()).digest()
    return TradeContext(
        instrument_uid=binding.instrument.instrument_uid,
        instrument_id=binding.instrument.instrument_id,
        instrument_revision=binding.instrument.metadata_revision,
        venue=binding.instrument.identity.venue,
        market=binding.instrument.identity.market,
        product_type=binding.instrument.identity.product_type.value,
        native_symbol=binding.instrument.native_symbol,
        provider=binding.provider,
        source_id=binding.source_id,
        source_role=binding.source_role,
        lease_epoch=1,
        received_at_ns=NOW + sequence * 1_000_000,
        normalized_at_ns=NOW + sequence * 1_000_000 + 1,
        published_at_ns=NOW + sequence * 1_000_000 + 2,
        partition_sequence=sequence,
        normalizer_version=binding.normalizer_version,
        adapter_version=binding.adapter_version,
        config_revision=1,
        correlation_id=f"phase104d-{binding.binding_id}-{sequence}",
        source_session_id=f"phase104d-session-{binding.source_id}",
        connection_generation=1,
        authority_revision=3,
        partition_plan_epoch=1,
        raw_capture_id=raw[:16],
        raw_frame_sha256=raw,
    )


def _field(name: str, value: str, unit: str) -> ReferenceField:
    return ReferenceField(name, CanonicalDecimal.from_text(value), unit)


def _reference_result(
    *,
    binding: StableSourceBinding,
    product: ReferenceProduct,
    fields: tuple[ReferenceField, ...],
    labels: tuple[tuple[str, str], ...] = (),
    interval: str | None = None,
    sequence: int,
) -> tuple[ReferenceBatchResult, ReferenceObservation, TradeContext]:
    start_ms = (NOW // 1_000_000) - 10_000
    history = product in {
        ReferenceProduct.FUNDING_RATE,
        ReferenceProduct.LONG_SHORT_RATIO,
        ReferenceProduct.TAKER_FLOW,
        ReferenceProduct.BASIS,
    }
    request = ReferenceRequest(
        instrument=binding.instrument,
        product=product,
        start_ms=start_ms if history else None,
        end_ms=start_ms + 5_000 if history else None,
        interval=interval,
        long_short_kind=(
            LongShortKind.GLOBAL_ACCOUNT
            if product is ReferenceProduct.LONG_SHORT_RATIO
            else None
        ),
        # ReferenceRequest requires its neutral NATIVE selector for every
        # non-basis product; only the contract selector itself is BASIS-only.
        basis_series=BasisSeries.NATIVE,
        basis_contract_type=(
            "CURRENT_QUARTER" if product is ReferenceProduct.BASIS else None
        ),
    )
    observed_at_ns = NOW + sequence * 100_000
    observation = ReferenceObservation(
        instrument_uid=binding.instrument.instrument_uid,
        instrument_revision=binding.instrument.metadata_revision,
        product=product,
        observed_at_ns=observed_at_ns,
        fields=fields,
        labels=labels,
    )
    capability = FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True)
    lineage = ReferenceLineage(
        provider=binding.provider,
        provider_endpoint="TEST_SYNTHETIC_PHASE104D",
        source_role="REFERENCE",
        adapter_version=binding.adapter_version,
        capability_name="reference_data",
    )
    coverage = ReferenceCoverage(
        requested_start_ms=start_ms if history else None,
        requested_end_ms=start_ms + 5_000 if history else None,
        observed_min_ms=observed_at_ns // 1_000_000,
        observed_max_ms=observed_at_ns // 1_000_000,
        complete_left=True,
        complete_right=True,
        truncated=False,
        terminal_reason="TEST_SYNTHETIC_PHASE104D",
    )
    result = ReferenceBatchResult(
        request=request,
        status=ReferenceStatus.OK,
        capability=capability,
        lineage=(lineage,),
        coverage=coverage,
        received_at_ns=NOW + sequence * 1_000_000,
        observations=(observation,),
    )
    return result, observation, _context(binding, sequence=sequence)


def _requirement(binding: StableSourceBinding, *, warmup: int = 1) -> DomainRequirement:
    return DomainRequirement(
        instrument_uid=binding.instrument.instrument_uid,
        feed=binding.feed,
        interval=binding.interval,
        consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id=binding.source_policy_id,
        warmup_limit=warmup,
    )


def _durable(binding: StableSourceBinding, event) -> DurableEvent:
    return DurableEvent(
        stream=binding.canonical_stream,
        partition_key=binding.partition_key,
        event_id=bytes(event.event_id),
        payload=event.SerializeToString(deterministic=True),
        accepted_at_ns=event.received_at_ns,
        headers={"provenance": "TEST_ONLY_SYNTHETIC_PHASE104D"},
    )


class Phase104V2QueryStreamIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.binance_btc = _record(
            venue="BINANCE", market="USDM", native_symbol="BTCUSDT", base="BTC"
        )
        self.binance_eth = _record(
            venue="BINANCE", market="USDM", native_symbol="ETHUSDT", base="ETH"
        )
        self.okx_btc = _record(
            venue="OKX", market="SWAP", native_symbol="BTC-USDT-SWAP", base="BTC"
        )
        self.bindings = (
            _binding(self.binance_btc, FeedType.FUNDING_RATE, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE"),
            _binding(self.binance_eth, FeedType.FUNDING_RATE, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE"),
            _binding(self.binance_btc, FeedType.OPEN_INTEREST, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE", interval="1h"),
            _binding(self.binance_btc, FeedType.MARK_INDEX_PRICE, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE"),
            _binding(self.binance_btc, FeedType.LONG_SHORT_RATIO, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE", interval="1h"),
            _binding(self.binance_btc, FeedType.TAKER_FLOW, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE", interval="1h"),
            _binding(self.binance_btc, FeedType.BASIS, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE", interval="1h"),
            _binding(self.binance_btc, FeedType.CONTRACT_METADATA, provider="BINANCE_DIRECT", source_id="binance-reference", source_role="REFERENCE"),
            _binding(self.okx_btc, FeedType.BOOK_SNAPSHOT, provider="OKX_DIRECT", source_id="okx-l2", source_role="PRIMARY", authoritative=True),
            _binding(self.okx_btc, FeedType.BOOK_DELTA, provider="OKX_DIRECT", source_id="okx-l2", source_role="PRIMARY", authoritative=True),
        )
        self.catalog = StableSourceCatalog(
            canonical_stream=STREAM,
            bindings=self.bindings,
            catalog_revision=7,
            source_policy_revision=3,
            authority_revision=3,
            instruments=(self.binance_btc, self.binance_eth, self.okx_btc),
        )
        self.spool = SQLiteDurableSpool(
            SpoolConfig(
                path=Path(self.temp.name) / "phase104d.sqlite3",
                max_records=1000,
                max_payload_bytes=4 * 1024 * 1024,
                max_storage_bytes=8 * 1024 * 1024,
                min_free_disk_bytes=0,
            )
        )

    def tearDown(self) -> None:
        self.spool.close()
        self.temp.cleanup()

    def _binding_for(self, feed: FeedType, *, record: InstrumentRecord | None = None):
        record = record or self.binance_btc
        return next(
            item
            for item in self.bindings
            if item.feed is feed and item.instrument.instrument_uid == record.instrument_uid
        )

    def _append(self, binding: StableSourceBinding, event) -> None:
        self.assertEqual(self.catalog.binding_for_envelope(event).binding_id, binding.binding_id)
        self.spool.append(_durable(binding, event))

    def _backend(self) -> StableSpoolQueryBackend:
        return StableSpoolQueryBackend(
            self.spool,
            self.catalog,
            schema_digest="d" * 64,
            clock_ns=lambda: NOW + 10_000_000,
        )

    def test_reference_products_materialize_with_exact_units_and_interval(self):
        cases = (
            (FeedType.FUNDING_RATE, ReferenceProduct.FUNDING_RATE, (_field("funding_rate", "0.0001", "DIMENSIONLESS_RATE"),), (), None, {"rate": "0.0001"}),
            (FeedType.OPEN_INTEREST, ReferenceProduct.OPEN_INTEREST, (_field("open_interest_contracts", "123.450", "CONTRACTS"), _field("open_interest_quote_notional", "4567.890", "QUOTE_NOTIONAL")), (), "1h", {"quantity": "123.450", "quantity_unit": "CONTRACT", "notional": "4567.890", "sampling_interval": "1h"}),
            (FeedType.MARK_INDEX_PRICE, ReferenceProduct.MARK_INDEX_PRICE, (_field("mark_price", "60123.40", "QUOTE_PRICE"), _field("index_price", "60120.10", "QUOTE_PRICE")), (), None, {"mark_price": "60123.40", "index_price": "60120.10"}),
            (FeedType.LONG_SHORT_RATIO, ReferenceProduct.LONG_SHORT_RATIO, (_field("long_account_ratio", "0.55", "RATIO"), _field("short_account_ratio", "0.45", "RATIO"), _field("long_short_ratio", "1.222222", "RATIO")), (("ratio_kind", "GLOBAL_ACCOUNT"),), "1h", {"population": "GLOBAL_ACCOUNT", "sampling_interval": "1h", "value_unit": "RATIO"}),
            (FeedType.TAKER_FLOW, ReferenceProduct.TAKER_FLOW, (_field("buy_volume", "12.500", "BASE_ASSET_QUANTITY"), _field("sell_volume", "8.250", "BASE_ASSET_QUANTITY"), _field("buy_sell_ratio", "1.515151", "RATIO")), (), "1h", {"buy_volume": "12.500", "sell_volume": "8.250", "quantity_unit": "BASE_ASSET"}),
            (FeedType.BASIS, ReferenceProduct.BASIS, (_field("basis", "12.340", "QUOTE_PRICE"), _field("annualized_basis_rate", "0.1200", "DIMENSIONLESS_RATE")), (), "1h", {"kind": "PROVIDER_NATIVE", "basis": "12.340", "basis_unit": "PRICE", "annualized_basis": "0.1200"}),
            (FeedType.CONTRACT_METADATA, ReferenceProduct.CONTRACT_METADATA, (_field("price_tick", "0.10", "QUOTE_PRICE"), _field("quantity_step", "0.001", "CONTRACTS")), (("contract_type", "PERPETUAL"),), None, {"contract_kind": "PERPETUAL", "settlement_asset": "USDT", "contract_multiplier": "1", "price_tick": "0.10", "quantity_step": "0.001"}),
        )
        for sequence, (feed, product, fields, labels, interval, expected) in enumerate(cases, start=1):
            with self.subTest(feed=feed.value):
                binding = self._binding_for(feed)
                result, observation, context = _reference_result(
                    binding=binding,
                    product=product,
                    fields=fields,
                    labels=labels,
                    interval=interval,
                    sequence=sequence,
                )
                self._append(
                    binding,
                    canonicalize_reference_observation(
                        result=result, observation=observation, context=context
                    ),
                )
                item = self._backend().latest(_requirement(binding))
                self.assertIsNotNone(item)
                self.assertEqual(item.feed, feed)
                self.assertEqual(item.instrument_uid, self.binance_btc.instrument_uid)
                self.assertEqual(item.interval, interval)
                self.assertEqual(item.source.source_role, "REFERENCE")
                self.assertFalse(item.quality.execution_eligible)
                self.assertEqual(item.contract.adapter_version, binding.adapter_version)
                for key, value in expected.items():
                    self.assertEqual(item.payload[key], value)
                api_item = _market_item(item)
                self.assertEqual(api_item.feed, feed.value)
                self.assertEqual(api_item.payload.feed.value, feed.value)

    def test_mark_index_confirmation_freshness_preserves_source_time_and_blocks_expiry(self):
        """A repeated provider value is usable only while its WS confirmation is fresh."""
        binding = _binding(
            self.okx_btc,
            FeedType.MARK_INDEX_PRICE,
            provider="OKX_DIRECT",
            source_id="okx-mark-index-primary",
            source_role="PRIMARY",
            authoritative=True,
            stale_after_ms=2_000,
            freshness_basis="PROVIDER_CONFIRMATION",
        )
        catalog = StableSourceCatalog(
            canonical_stream=STREAM,
            bindings=(binding,),
            catalog_revision=8,
            source_policy_revision=4,
            authority_revision=4,
            instruments=(self.okx_btc,),
        )
        # The generic reference canonicalizer intentionally accepts REFERENCE
        # only.  The live Rust path emits the same payload as PRIMARY, so build
        # valid fixture bytes first, then set the proven runtime lineage.
        reference_binding = _binding(
            self.okx_btc,
            FeedType.MARK_INDEX_PRICE,
            provider="OKX_DIRECT",
            source_id=binding.source_id,
            source_role="REFERENCE",
        )
        result, observation, context = _reference_result(
            binding=reference_binding,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            fields=(
                _field("mark_price", "60123.40", "QUOTE_PRICE"),
                _field("index_price", "60120.10", "QUOTE_PRICE"),
            ),
            sequence=71,
        )
        current = canonicalize_reference_observation(
            result=result, observation=observation, context=context
        )
        current.source_role = common_pb2.SOURCE_ROLE_PRIMARY
        current.adapter_version = binding.adapter_version
        current.normalizer_version = binding.normalizer_version
        current.authority_revision = catalog.authority_revision
        # Keep value time for lineage. A current authenticated provider frame,
        # rather than the REST age of an unchanged index, proves freshness.
        current.source_event_time_ns = NOW - 10_000_000_000
        current.received_at_ns = NOW - 1_000_000_000
        current.normalized_at_ns = current.received_at_ns
        current.published_at_ns = current.received_at_ns
        self.spool.append(_durable(binding, current))
        backend = StableSpoolQueryBackend(
            self.spool,
            catalog,
            schema_digest="e" * 64,
            clock_ns=lambda: NOW,
        )
        requirement = _requirement(binding)
        fresh = backend.latest(requirement)
        self.assertIsNotNone(fresh)
        assert fresh is not None
        self.assertEqual(fresh.observed_at_ns, NOW - 10_000_000_000)
        self.assertEqual(fresh.quality.state, "LIVE")
        self.assertTrue(fresh.quality.execution_eligible)
        self.assertIn("FRESHNESS_BASIS_PROVIDER_CONFIRMATION", fresh.quality.flags)
        self.assertIn("SOURCE_VALUE_TIMESTAMP_OLD", fresh.quality.flags)

        expired = market_data_pb2.EventEnvelope()
        expired.CopyFrom(current)
        expired.event_id = hashlib.sha256(b"phase104d-mark-index-expired").digest()[:16]
        expired.received_at_ns = NOW - 2_001_000_000
        expired.normalized_at_ns = expired.received_at_ns
        expired.published_at_ns = expired.received_at_ns
        self.spool.append(_durable(binding, expired))
        stale = backend.latest(requirement)
        self.assertIsNotNone(stale)
        assert stale is not None
        self.assertEqual(stale.quality.state, "STALE")
        self.assertFalse(stale.quality.execution_eligible)
        self.assertIn("LAST_EVENT_STALE", stale.quality.flags)

    def test_source_scoped_entitlements_coalesce_bound_products(self):
        entitlements = self.catalog.entitlements()
        binance = entitlements.authorize(
            source_id="binance-reference",
            purpose=AccessPurpose.INTERNAL_ALPHA,
            product=DataProduct.CANONICAL_HISTORY,
            at_ns=NOW,
        )
        self.assertTrue(binance.allowed)
        self.assertEqual(binance.reason_code, "ALLOWED")
        self.assertFalse(
            entitlements.authorize(
                source_id="binance-reference",
                purpose=AccessPurpose.INTERNAL_EXECUTION,
                product=DataProduct.CANONICAL_SNAPSHOT,
                at_ns=NOW,
            ).allowed
        )
        self.assertTrue(
            entitlements.authorize(
                source_id="okx-l2",
                purpose=AccessPurpose.INTERNAL_EXECUTION,
                product=DataProduct.CANONICAL_SNAPSHOT,
                at_ns=NOW,
            ).allowed
        )

    def test_valid_zero_reference_values_are_not_converted_to_missing(self):
        cases = (
            (
                FeedType.OPEN_INTEREST,
                ReferenceProduct.OPEN_INTEREST,
                (_field("open_interest_contracts", "0", "CONTRACTS"),),
                (),
                "1h",
                "quantity",
            ),
            (
                FeedType.TAKER_FLOW,
                ReferenceProduct.TAKER_FLOW,
                (
                    _field("buy_volume", "0", "BASE_ASSET_QUANTITY"),
                    _field("sell_volume", "8.250", "BASE_ASSET_QUANTITY"),
                    _field("buy_sell_ratio", "0", "RATIO"),
                ),
                (),
                "1h",
                "buy_volume",
            ),
        )
        for sequence, (feed, product, fields, labels, interval, field) in enumerate(
            cases,
            start=40,
        ):
            with self.subTest(feed=feed.value):
                binding = self._binding_for(feed)
                result, observation, context = _reference_result(
                    binding=binding,
                    product=product,
                    fields=fields,
                    labels=labels,
                    interval=interval,
                    sequence=sequence,
                )
                self._append(
                    binding,
                    canonicalize_reference_observation(
                        result=result,
                        observation=observation,
                        context=context,
                    ),
                )
                item = self._backend().latest(_requirement(binding))
                self.assertIsNotNone(item)
                self.assertEqual(item.payload[field], "0")

    async def test_multi_symbol_batch_and_sdk_stream_do_not_cross_mix(self):
        btc = self._binding_for(FeedType.FUNDING_RATE, record=self.binance_btc)
        eth = self._binding_for(FeedType.FUNDING_RATE, record=self.binance_eth)
        for sequence, binding, rate in ((10, btc, "0.0001"), (11, eth, "0.0002")):
            result, observation, context = _reference_result(
                binding=binding,
                product=ReferenceProduct.FUNDING_RATE,
                fields=(_field("funding_rate", rate, "DIMENSIONLESS_RATE"),),
                sequence=sequence,
            )
            self._append(
                binding,
                canonicalize_reference_observation(
                    result=result, observation=observation, context=context
                ),
            )
        backend = self._backend()
        service = V2QueryService(
            instruments=InstrumentQuery(self.catalog.instrument_registry()),
            backend=backend,
            entitlements=self.catalog.entitlements(),
            clock_ns=lambda: NOW + 10_000_000,
        )
        batch = await service.warmup_batch_async(
            BatchRequirement(
                consumer_id="phase104d-alpha",
                requirements=(_requirement(btc), _requirement(eth)),
            ),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        self.assertEqual([item.status for item in batch.results], ["OK", "OK"])
        btc_item = batch.results[0].result.history.items[0]
        eth_item = batch.results[1].result.history.items[0]
        self.assertEqual(btc_item.instrument_uid, self.binance_btc.instrument_uid)
        self.assertEqual(eth_item.instrument_uid, self.binance_eth.instrument_uid)
        self.assertEqual(btc_item.payload["rate"], "0.0001")
        self.assertEqual(eth_item.payload["rate"], "0.0002")

        handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec({"phase104d": b"d" * 32}, active_key_id="phase104d"),
        )
        loader = StableGrpcSnapshotLoader(
            service=service,
            backend=backend,
            issuer=StableConsumerCursorIssuer(handoff, self.catalog, ttl_seconds=60),
        )
        snapshot = loader.load(_requirement(btc), consumer_id="phase104d-alpha")
        self.assertEqual(len(snapshot.events), 1)
        template = _market_item(btc_item)
        view = market_data_view_from_stream(
            StreamEvent(
                logical_offset=1,
                resume_token=snapshot.stream_cursor,
                event=snapshot.events[0],
            ),
            template=template,
            requirement=SdkRequirement(
                self.binance_btc.instrument_uid,
                Feed.FUNDING_RATE,
                Grade.ALPHA,
                btc.source_policy_id,
                stale_policy=StalePolicy.BLOCK,
                gap_policy=GapPolicy.BLOCK,
                recovery=RecoveryPolicy.FRESH_SNAPSHOT,
            ),
            now_ns=NOW + 10_000_000,
        )
        self.assertEqual(view.instrument_uid, self.binance_btc.instrument_uid)
        self.assertEqual(view.payload.rate.source_text, "0.0001")
        self.assertFalse(view.quality.execution_eligible)

    def test_book_ready_gap_recovery_and_delta_snapshot_anchor(self):
        identity = BookIdentity("OKX_SWAP_BOOKS", self.okx_btc.instrument_uid, "books")
        core = L2BookReference(
            BookConfig(
                identity=identity,
                sequence_policy=SequencePolicy.PREVIOUS_SEQUENCE,
                checksum_policy=ChecksumPolicy.IGNORE,
                view_depth_per_side=2,
            )
        )
        levels = [
            BookLevelInput("BID", "60000.1", "2"),
            BookLevelInput("ASK", "60000.2", "3"),
        ]
        self.assertEqual(
            core.apply_snapshot(
                identity=identity,
                generation=1,
                sequence_end=42,
                checksum=ChecksumEvidence.NOT_PROVIDED,
                origin=SnapshotOrigin.WEBSOCKET,
                levels=levels,
            ),
            "SNAPSHOT_APPLIED",
        )
        self.assertIsNotNone(core.view())
        self.assertEqual(
            core.apply_delta(
                identity=identity,
                generation=1,
                sequence_start=None,
                previous_sequence=99,
                sequence_end=100,
                checksum=ChecksumEvidence.NOT_PROVIDED,
                levels=[],
            ),
            "SEQUENCE_GAP",
        )
        self.assertIsNone(core.view())
        self.assertEqual(core.request_resync(2), "RESYNC_REQUESTED")
        self.assertIsNone(core.view())

        snapshot_binding = self._binding_for(FeedType.BOOK_SNAPSHOT, record=self.okx_btc)
        delta_binding = self._binding_for(FeedType.BOOK_DELTA, record=self.okx_btc)
        snapshot_frame = {
            "arg": {"instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "seqId": "42", "ts": str(NOW // 1_000_000),
                "bids": [["60000.1", "2", "0", "1"]],
                "asks": [["60000.2", "3", "0", "1"]],
                "checksum": "0",
            }],
        }
        snapshot = canonicalize_okx_book(snapshot_frame, _context(snapshot_binding, sequence=20))
        self._append(snapshot_binding, snapshot)
        update = {
            "arg": {"instId": "BTC-USDT-SWAP"},
            "action": "update",
            "data": [{
                "prevSeqId": "42", "seqId": "43", "ts": str(NOW // 1_000_000 + 1),
                "bids": [["60000.1", "1", "0", "1"]], "asks": [], "checksum": "0",
            }],
        }
        with self.assertRaisesRegex(ValueError, "snapshot sequence"):
            canonicalize_okx_book(update, _context(delta_binding, sequence=21))
        delta = canonicalize_okx_book(
            update, _context(delta_binding, sequence=21), snapshot_sequence="42"
        )
        self._append(delta_binding, delta)
        self.assertEqual(delta.book_delta.snapshot_sequence, "42")
        ready_snapshot = self._backend().latest(_requirement(snapshot_binding))
        ready_delta = self._backend().latest(_requirement(delta_binding))
        self.assertEqual(ready_snapshot.payload["native_sequence"], "42")
        self.assertEqual(ready_delta.payload["snapshot_sequence"], "42")
        self.assertEqual(ready_snapshot.quality.state, "LIVE")
        self.assertTrue(ready_snapshot.quality.complete)
        self.assertTrue(ready_snapshot.quality.execution_eligible)

        unverified = market_data_pb2.EventEnvelope()
        unverified.CopyFrom(snapshot)
        unverified.event_id = b"unverified-book!"
        unverified.source_event_time_ns += 1
        unverified.book_snapshot.book_generation = 0
        unverified.book_snapshot.sequence_verified = False
        self._append(snapshot_binding, unverified)
        blocked = self._backend().latest(_requirement(snapshot_binding))
        self.assertEqual(blocked.quality.state, "SYNCING")
        self.assertFalse(blocked.quality.complete)
        self.assertFalse(blocked.quality.execution_eligible)
        self.assertIn("BOOK_SEQUENCE_UNVERIFIED", blocked.quality.flags)

    def test_unrepresentable_reference_values_fail_closed(self):
        taker_binding = self._binding_for(FeedType.TAKER_FLOW)
        result, observation, context = _reference_result(
            binding=taker_binding,
            product=ReferenceProduct.TAKER_FLOW,
            fields=(
                _field("buy_volume", "10", "PROVIDER_NATIVE_VOLUME"),
                _field("sell_volume", "8", "PROVIDER_NATIVE_VOLUME"),
                _field("buy_sell_ratio", "1.25", "RATIO"),
            ),
            interval="1h",
            sequence=30,
        )
        with self.assertRaisesRegex(ReferenceProjectionError, "not representable"):
            canonicalize_reference_observation(
                result=result, observation=observation, context=context
            )
        mark_binding = self._binding_for(FeedType.MARK_INDEX_PRICE)
        result, observation, context = _reference_result(
            binding=mark_binding,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            fields=(_field("mark_price", "60000", "QUOTE_PRICE"),),
            sequence=31,
        )
        with self.assertRaisesRegex(ReferenceProjectionError, "index_price"):
            canonicalize_reference_observation(
                result=result, observation=observation, context=context
            )

    async def test_slow_book_subscription_cannot_block_bar_peer(self):
        handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec({"phase104d": b"s" * 32}, active_key_id="phase104d"),
        )
        gateway = DurableStreamGateway(
            handoff=handoff, sink=self.spool, max_buffer_events=1
        )
        book = self._binding_for(FeedType.BOOK_SNAPSHOT, record=self.okx_btc)
        bar_partition = f"{self.binance_btc.instrument_uid}/bar/binance-primary"
        trade_partition = f"{self.binance_btc.instrument_uid}/trade/binance-primary"
        book_token = handoff.issue(
            consumer_id="slow-book",
            snapshot_id="phase104d-book",
            snapshot_watermark=handoff.capture_watermark(
                stream=STREAM,
                partition_key=book.partition_key,
            ),
            ttl_seconds=60,
        ).token
        bar_token = handoff.issue(
            consumer_id="bar-peer",
            snapshot_id="phase104d-bar",
            snapshot_watermark=handoff.capture_watermark(
                stream=STREAM,
                partition_key=bar_partition,
            ),
            ttl_seconds=60,
        ).token
        trade_token = handoff.issue(
            consumer_id="trade-peer",
            snapshot_id="phase104d-trade",
            snapshot_watermark=handoff.capture_watermark(
                stream=STREAM,
                partition_key=trade_partition,
            ),
            ttl_seconds=60,
        ).token
        slow = await gateway.open(
            consumer_id="slow-book", stream=STREAM, partition_key=book.partition_key,
            token=book_token, max_buffer_events=1,
        )
        peer = await gateway.open(
            consumer_id="bar-peer", stream=STREAM, partition_key=bar_partition,
            token=bar_token, max_buffer_events=1,
        )
        trade_peer = await gateway.open(
            consumer_id="trade-peer", stream=STREAM, partition_key=trade_partition,
            token=trade_token, max_buffer_events=1,
        )
        for index in (1, 2):
            await gateway.publish(
                DurableEvent(
                    STREAM, book.partition_key, index.to_bytes(16, "big"),
                    b"phase104d-book", NOW + index,
                )
            )
        with self.assertRaises(SlowConsumer):
            await slow.next_live()
        bar_event = DurableEvent(
            STREAM, bar_partition, b"b" * 16, b"phase104d-bar", NOW + 3
        )
        trade_event = DurableEvent(
            STREAM, trade_partition, b"t" * 16, b"phase104d-trade", NOW + 4
        )
        await gateway.publish(bar_event)
        await gateway.publish(trade_event)
        self.assertEqual((await peer.next_live()).stored.event.event_id, b"b" * 16)
        self.assertEqual((await trade_peer.next_live()).stored.event.event_id, b"t" * 16)
        await slow.close()
        await peer.close()
        await trade_peer.close()


if __name__ == "__main__":
    unittest.main()

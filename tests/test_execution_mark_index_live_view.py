"""Execution MARK/INDEX live-view contract tests.

All market values in this file are deterministic test provenance.  They prove
the private current-state boundary, not provider latency or a real market read.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import httpx
from fastapi import FastAPI

from qdl.common.v1 import common_pb2
from qdl.domain.capabilities import CapabilityAvailability, FeedCapability
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.query import (
    AccessPurpose,
    ConsumerGrade,
    DataProduct,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    InstrumentQuery,
    MemoryMarketDataBackend,
    V2QueryService,
)
from qdl.query.contracts import StalePolicy
from qdl.query.reference import ReferenceBatchRequirement, ReferenceDataRequirement
from qdl.reference.batch import ReferenceBatch
from qdl.reference.contracts import (
    MarkIndexKind,
    ReferenceBatchResult,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceLineage,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
    decimal_field,
)
from qdl.reference.execution_live import HttpExecutionMarkIndexReader
from qdl.runtime.execution_mark_index import (
    ExecutionMarkIndexQuietPolicy,
    ExecutionMarkIndexLiveView,
    install_execution_mark_index_read,
)
from qdl.runtime.internal_auth import stable_hmac_signature
from qdl.runtime.lease import GatewayFenced
from qdl.runtime.stable_catalog import StableSourceBinding
from qdl.runtime.session_liveness import StableSessionLivenessReader
from qdl.transport import Cursor, DurableEvent, StoredEvent


NOW_NS = 1_800_000_000_000_000_000
SECRET = b"execution-mark-index-live-view-test-secret"
STREAM = "md.canonical.execution-mark-index-test.v2"
LIVE_ENDPOINT = "qdl://stable-stream/internal/v2/execution/mark-index/latest"


def _record(*, venue: str, market: str, native_symbol: str, base: str) -> InstrumentRecord:
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


def _binding(record: InstrumentRecord, *, source_policy_id: str = "crypto_liquid_v2"):
    provider = "BINANCE_DIRECT" if record.identity.venue == "BINANCE" else "OKX_DIRECT"
    return StableSourceBinding(
        binding_id=f"execution-mark-index-{record.native_symbol.lower()}",
        instrument=record,
        provider=provider,
        source_id=provider,
        source_role="PRIMARY",
        source_policy_id=source_policy_id,
        authoritative=True,
        adapter_version="execution-mark-index-test/1",
        normalizer_version="execution-mark-index-core-test/1",
        feed=FeedType.MARK_INDEX_PRICE,
        interval=None,
        stale_after_ms=2_000,
        require_final_bar=False,
        continuous_calendar=True,
        v1_compatibility="NONE",
        canonical_stream=STREAM,
        freshness_basis="PROVIDER_CONFIRMATION",
    )


def _envelope(
    binding: StableSourceBinding,
    *,
    sequence: int,
    generation: int = 1,
    received_at_ns: int = NOW_NS,
    quality_flags: tuple[int, ...] = (),
) -> market_data_pb2.EventEnvelope:
    raw = hashlib.sha256(
        f"execution-mark-index:{binding.instrument.instrument_uid}:{sequence}:{generation}".encode()
    ).digest()
    record = binding.instrument
    envelope = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.v2",
        schema_major=2,
        schema_minor=0,
        event_id=raw[:16],
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=record.metadata_revision,
        venue=record.identity.venue,
        market=record.identity.market,
        product_type=record.identity.product_type.value,
        native_symbol=record.native_symbol,
        provider=binding.provider,
        source_id=binding.source_id,
        source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        lease_epoch=5,
        source_event_time_ns=received_at_ns - 1_000_000,
        received_at_ns=received_at_ns,
        normalized_at_ns=received_at_ns + 1,
        published_at_ns=received_at_ns + 2,
        source_sequence=f"test:{generation}:{sequence}",
        partition_sequence=sequence,
        normalizer_version=binding.normalizer_version,
        adapter_version=binding.adapter_version,
        raw_capture_id=raw[:16],
        raw_payload_hash=raw,
        correlation_id=raw.hex(),
        config_revision=11,
        source_session_id="execution-mark-index-test-session",
        connection_generation=generation,
        authority_revision=3,
        partition_plan_epoch=1,
    )
    envelope.quality_flags.extend(quality_flags)
    envelope.mark_index_price.mark_price.source_text = "101.25"
    envelope.mark_index_price.index_price.source_text = "101.00"
    envelope.canonical_payload_hash = hashlib.sha256(
        envelope.mark_index_price.SerializeToString(deterministic=True)
    ).digest()
    return envelope


def _paired_envelope(
    binding: StableSourceBinding,
    *,
    sequence: int,
    generation: int = 1,
    mark_received_at_ns: int,
    index_received_at_ns: int,
) -> market_data_pb2.EventEnvelope:
    """Build deterministic Rust-shaped pair lineage for quiet-contract tests."""

    envelope = _envelope(
        binding,
        sequence=sequence,
        generation=generation,
        received_at_ns=min(mark_received_at_ns, index_received_at_ns),
    )
    mark_capture = hashlib.sha256(
        f"mark:{binding.instrument.instrument_uid}:{sequence}".encode()
    ).digest()[:16]
    index_capture = hashlib.sha256(
        f"index:{binding.instrument.instrument_uid}:{sequence}".encode()
    ).digest()[:16]
    source_times = (
        mark_received_at_ns // 1_000_000,
        index_received_at_ns // 1_000_000,
    )
    envelope.source_event_time_ns = min(source_times) * 1_000_000
    envelope.received_at_ns = min(mark_received_at_ns, index_received_at_ns)
    envelope.normalized_at_ns = envelope.received_at_ns + 1
    envelope.published_at_ns = envelope.received_at_ns + 2
    envelope.source_sequence = ":".join(
        str(value)
        for value in (
            *source_times,
            mark_received_at_ns,
            index_received_at_ns,
            mark_capture.hex(),
            index_capture.hex(),
        )
    )
    capture = hashlib.sha256()
    capture.update(b"qdl-mark-index-capture-v1")
    capture.update(mark_capture)
    capture.update(index_capture)
    envelope.raw_capture_id = capture.digest()[:16]
    envelope.raw_payload_hash = hashlib.sha256(
        b"paired-mark-index-test" + mark_capture + index_capture
    ).digest()
    return envelope


def _write_session(
    root: Path,
    envelope: market_data_pb2.EventEnvelope,
    *,
    state: str = "LIVE",
    generation: int | None = None,
    config_revision: int | None = None,
    last_transport_at_ns: int = NOW_NS - 1_000_000,
) -> None:
    directory = root / f"{envelope.venue.lower()}-{envelope.market.lower()}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "source.json").write_text(
        json.dumps({
            "schema": "qdl.provider-session-liveness.v1",
            "source_session_id": envelope.source_session_id,
            "connection_generation": (
                envelope.connection_generation if generation is None else generation
            ),
            "state": state,
            "last_transport_at_ns": last_transport_at_ns,
            "updated_at_ns": last_transport_at_ns,
            "config_revision": (
                envelope.config_revision
                if config_revision is None else config_revision
            ),
        }),
        encoding="utf-8",
    )


def _stored(envelope: market_data_pb2.EventEnvelope, *, offset: int) -> StoredEvent:
    payload = envelope.SerializeToString(deterministic=True)
    event = DurableEvent(
        stream=STREAM,
        partition_key=f"execution/{envelope.instrument_uid}",
        event_id=bytes(envelope.event_id),
        payload=payload,
        accepted_at_ns=envelope.received_at_ns,
    )
    return StoredEvent(
        event=event,
        cursor=Cursor(STREAM, event.partition_key, offset),
        committed_at_ns=envelope.received_at_ns,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _hydration_stored(
    binding: StableSourceBinding,
    envelope: market_data_pb2.EventEnvelope,
    *,
    offset: int,
) -> StoredEvent:
    """One durable canonical row in the exact physical partition under test."""

    payload = envelope.SerializeToString(deterministic=True)
    event = DurableEvent(
        stream=STREAM,
        partition_key=binding.partition_key,
        event_id=bytes(envelope.event_id),
        payload=payload,
        accepted_at_ns=envelope.received_at_ns,
    )
    return StoredEvent(
        event=event,
        cursor=Cursor(STREAM, event.partition_key, offset),
        committed_at_ns=envelope.received_at_ns,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )


class _Gateway:
    def __init__(self, epoch: int = 5) -> None:
        self.epoch = epoch
        self.fenced = False

    def assert_active(self, expected_epoch: int | None = None) -> int:
        if self.fenced or (expected_epoch is not None and expected_epoch != self.epoch):
            raise GatewayFenced("test gateway fenced")
        return self.epoch


class _FallbackReferenceAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, request, *, capability, received_at_ns):
        del capability
        self.calls += 1
        fields = tuple(
            field
            for field in (
                decimal_field("mark_price", "101.25", "QUOTE_PRICE"),
                decimal_field("index_price", "101.00", "QUOTE_PRICE"),
            )
            if field is not None
        )
        return ReferenceFetch(
            observations=(ReferenceObservation(
                instrument_uid=request.instrument.instrument_uid,
                instrument_revision=request.instrument.metadata_revision,
                product=request.product,
                observed_at_ns=NOW_NS,
                fields=fields,
            ),),
            lineage=(ReferenceLineage(
                provider="BINANCE_DIRECT",
                provider_endpoint="TEST_REFERENCE_REST_ADAPTER",
                source_role="REFERENCE",
                adapter_version="execution-mark-index-test/1",
                capability_name="mark_index_price",
            ),),
            coverage=ReferenceCoverage(
                requested_start_ms=None,
                requested_end_ms=None,
                observed_min_ms=NOW_NS // 1_000_000,
                observed_max_ms=NOW_NS // 1_000_000,
                complete_left=True,
                complete_right=True,
                truncated=False,
                terminal_reason="TEST_CURRENT",
            ),
        )


class _LiveReader:
    def __init__(
        self,
        *,
        status: ReferenceStatus = ReferenceStatus.OK,
        source_event_time_ns: int = NOW_NS,
        provider_confirmation_ns: int = NOW_NS,
        freshness_basis: str = "SOURCE_EVENT",
    ) -> None:
        self.status = status
        self.calls = 0
        self.calls_by_policy: list[tuple[str, int, int | None]] = []
        self.calls_by_recency: list[tuple[StalePolicy, int | None]] = []
        self.source_event_time_ns = source_event_time_ns
        self.provider_confirmation_ns = provider_confirmation_ns
        self.freshness_basis = freshness_basis

    async def fetch(
        self,
        request,
        *,
        max_freshness_ms,
        source_policy_id,
        event_recency_policy=StalePolicy.BLOCK,
        max_session_liveness_ms=None,
        deadline_ms=None,
    ):
        self.calls += 1
        self.calls_by_policy.append((source_policy_id, max_freshness_ms, deadline_ms))
        self.calls_by_recency.append((event_recency_policy, max_session_liveness_ms))
        capability = FeedCapability(CapabilityAvailability.AVAILABLE, snapshot=True)
        lineage = ReferenceLineage(
            provider="BINANCE_DIRECT",
            provider_endpoint=LIVE_ENDPOINT,
            source_role="REFERENCE",
            adapter_version="execution-mark-index-live-test/1",
            capability_name="mark_index_price",
        )
        if self.status is not ReferenceStatus.OK:
            return ReferenceBatchResult(
                request=request,
                status=self.status,
                capability=capability,
                lineage=(lineage,),
                coverage=ReferenceCoverage(
                    requested_start_ms=None,
                    requested_end_ms=None,
                    observed_min_ms=None,
                    observed_max_ms=None,
                    complete_left=False,
                    complete_right=False,
                    truncated=False,
                    terminal_reason="LIVE_VIEW_STALE",
                ),
                received_at_ns=self.provider_confirmation_ns,
                error_code="LIVE_VIEW_STALE",
                error_detail="test stale live view",
            )
        fields = tuple(
            field
            for field in (
                decimal_field("mark_price", "101.25", "QUOTE_PRICE"),
                decimal_field("index_price", "101.00", "QUOTE_PRICE"),
            )
            if field is not None
        )
        labels = [
            ("freshness_basis", self.freshness_basis),
            ("provider_confirmation_ns", str(self.provider_confirmation_ns)),
        ]
        if event_recency_policy is StalePolicy.OBSERVE:
            labels.extend((
                ("event_recency_policy", "OBSERVE"),
                ("recency_mode", "COMPONENT_SESSION_LIVE"),
                ("provider_session_state", "LIVE"),
                ("provider_session_liveness_ms", "1"),
                ("provider_session_checked_at_ns", str(NOW_NS)),
                ("component_mark_received_at_ns", str(NOW_NS - 10_000_000_000)),
                ("component_index_received_at_ns", str(NOW_NS - 60_000_000_000)),
                ("component_mark_quiet_after_ms", "15000"),
                ("component_index_quiet_after_ms", "70000"),
            ))
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.OK,
            capability=capability,
            lineage=(lineage,),
            coverage=ReferenceCoverage(
                requested_start_ms=None,
                requested_end_ms=None,
                observed_min_ms=NOW_NS // 1_000_000,
                observed_max_ms=NOW_NS // 1_000_000,
                complete_left=True,
                complete_right=True,
                truncated=False,
                terminal_reason="LIVE_EXECUTION_VIEW",
            ),
            received_at_ns=self.provider_confirmation_ns,
            observations=(ReferenceObservation(
                instrument_uid=request.instrument.instrument_uid,
                instrument_revision=request.instrument.metadata_revision,
                product=request.product,
                observed_at_ns=self.source_event_time_ns,
                fields=fields,
                labels=tuple(labels),
            ),),
        )

    def stats(self) -> dict[str, int]:
        return {"calls": self.calls, "successes": self.calls if self.status is ReferenceStatus.OK else 0,
                "failures": 0 if self.status is ReferenceStatus.OK else self.calls}


class StableCatalogImportTests(unittest.TestCase):
    def test_direct_catalog_import_does_not_form_a_runtime_cycle(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from qdl.runtime.stable_catalog import StableSourceCatalog; "
                "assert StableSourceCatalog.__name__ == 'StableSourceCatalog'",
            ],
            cwd=Path(__file__).resolve().parents[1],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class ExecutionMarkIndexLiveViewTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.record = _record(
            venue="BINANCE", market="USDM", native_symbol="BTCUSDT", base="BTC"
        )
        self.binding = _binding(self.record)
        self.view = ExecutionMarkIndexLiveView(frozenset({self.record.instrument_uid}))

    async def test_generation_gap_fence_and_exact_identity_are_fail_closed(self):
        first = _envelope(self.binding, sequence=1, generation=2)
        await self.view.remember(
            binding=self.binding, envelope=first, stored=_stored(first, offset=10), gateway_epoch=5
        )
        ready = await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertIsNotNone(ready.record)

        delayed_old_generation = _envelope(
            self.binding, sequence=2, generation=1, received_at_ns=NOW_NS + 1_000_000
        )
        await self.view.remember(
            binding=self.binding,
            envelope=delayed_old_generation,
            stored=_stored(delayed_old_generation, offset=11),
            gateway_epoch=5,
        )
        unchanged = await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(unchanged.record.connection_generation, 2)

        gap = _envelope(
            self.binding,
            sequence=3,
            generation=2,
            received_at_ns=NOW_NS + 2_000_000,
            quality_flags=(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE,),
        )
        await self.view.remember(
            binding=self.binding, envelope=gap, stored=_stored(gap, offset=12), gateway_epoch=5
        )
        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=7,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "GAP_OR_RESYNC",
        )
        same_generation = _envelope(
            self.binding, sequence=4, generation=2, received_at_ns=NOW_NS + 3_000_000
        )
        await self.view.remember(
            binding=self.binding,
            envelope=same_generation,
            stored=_stored(same_generation, offset=13),
            gateway_epoch=5,
        )
        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=7,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "GAP_OR_RESYNC",
        )
        recovered = _envelope(
            self.binding, sequence=5, generation=3, received_at_ns=NOW_NS + 4_000_000
        )
        await self.view.remember(
            binding=self.binding,
            envelope=recovered,
            stored=_stored(recovered, offset=14),
            gateway_epoch=5,
        )
        self.assertIsNotNone((await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )).record)

        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=8,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "IDENTITY_MISMATCH",
        )
        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=7,
                source_policy_id="different-policy",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "SOURCE_POLICY_MISMATCH",
        )
        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=7,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 3_000_000_000,
            )).reason,
            "STALE",
        )
        await self.view.fence_all()
        self.assertEqual(
            (await self.view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=7,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "NOT_READY",
        )

    async def test_durable_hydration_restores_only_the_exact_latest_binding(self):
        envelope = _envelope(self.binding, sequence=11, generation=4)
        stored = _hydration_stored(self.binding, envelope, offset=44)

        class _Spool:
            def __init__(self):
                self.calls = []

            def read_tail(self, *, stream, partition_key, limit):
                self.calls.append((stream, partition_key, limit))
                return [stored]

        view = ExecutionMarkIndexLiveView(
            frozenset({self.record.instrument_uid}),
            bindings={self.record.instrument_uid: self.binding},
        )
        restored = await view.hydrate_from_spool(
            spool=_Spool(), canonical_stream=STREAM, gateway_epoch=9
        )
        self.assertEqual(restored, 1)
        result = await view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=self.record.metadata_revision,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=9,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(result.record.delivery_stage, "SPOOL_CONFIRMED")
        self.assertEqual(result.record.spool_watermark_offset, 44)

    async def test_durable_hydration_preserves_gap_and_identity_fences(self):
        gap = _envelope(
            self.binding,
            sequence=12,
            generation=4,
            quality_flags=(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE,),
        )
        stored = _hydration_stored(self.binding, gap, offset=45)

        class _Spool:
            def read_tail(self, *, stream, partition_key, limit):
                return [stored]

        view = ExecutionMarkIndexLiveView(
            frozenset({self.record.instrument_uid}),
            bindings={self.record.instrument_uid: self.binding},
        )
        self.assertEqual(
            await view.hydrate_from_spool(
                spool=_Spool(), canonical_stream=STREAM, gateway_epoch=9
            ),
            1,
        )
        self.assertEqual(
            (await view.read(
                instrument_uid=self.record.instrument_uid,
                instrument_revision=self.record.metadata_revision,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=9,
                now_ns=NOW_NS + 500_000_000,
            )).reason,
            "GAP_OR_RESYNC",
        )

    async def test_durable_hydration_never_overwrites_a_newer_live_record(self):
        durable = _envelope(
            self.binding,
            sequence=13,
            generation=4,
            received_at_ns=NOW_NS - 100_000_000,
        )
        stored = _hydration_stored(self.binding, durable, offset=46)

        class _Spool:
            def read_tail(self, *, stream, partition_key, limit):
                return [stored]

        view = ExecutionMarkIndexLiveView(
            frozenset({self.record.instrument_uid}),
            bindings={self.record.instrument_uid: self.binding},
        )
        await view.hydrate_from_spool(
            spool=_Spool(), canonical_stream=STREAM, gateway_epoch=9
        )
        current = _envelope(
            self.binding,
            sequence=14,
            generation=4,
            received_at_ns=NOW_NS,
        )
        await view.remember(
            binding=self.binding,
            envelope=current,
            stored=_hydration_stored(self.binding, current, offset=47),
            gateway_epoch=9,
        )
        await view.hydrate_from_spool(
            spool=_Spool(), canonical_stream=STREAM, gateway_epoch=9
        )
        result = await view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=self.record.metadata_revision,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=9,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(result.record.event_id, bytes(current.event_id))

    async def test_quiet_pair_is_session_bound_and_preserves_component_lineage(self):
        """Only a declared, healthy paired provider session may admit quiet data."""

        record = _record(
            venue="OKX", market="SWAP", native_symbol="DOGE-USDT-SWAP", base="DOGE"
        )
        binding = _binding(record)
        envelope = _paired_envelope(
            binding,
            sequence=1,
            generation=3,
            mark_received_at_ns=NOW_NS - 10_000_000_000,
            index_received_at_ns=NOW_NS - 60_000_000_000,
        )
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            view = ExecutionMarkIndexLiveView(
                frozenset({record.instrument_uid}),
                quiet_policies={
                    record.instrument_uid: ExecutionMarkIndexQuietPolicy((
                        ("MARK", 15_000), ("INDEX", 70_000),
                    )),
                },
                session_liveness_reader=StableSessionLivenessReader(root),
            )
            await view.remember(
                binding=binding,
                envelope=envelope,
                stored=_stored(envelope, offset=31),
                gateway_epoch=5,
            )
            _write_session(root, envelope)
            quiet = await view.read(
                instrument_uid=record.instrument_uid,
                instrument_revision=record.metadata_revision,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                event_recency_policy=StalePolicy.OBSERVE,
                max_session_liveness_ms=45_000,
                now_ns=NOW_NS,
            )
            self.assertIsNotNone(quiet.record)
            self.assertEqual(quiet.recency_mode, "COMPONENT_SESSION_LIVE")
            self.assertEqual(
                quiet.record.source_event_time_ns,
                envelope.source_event_time_ns,
            )
            self.assertEqual(
                dict(quiet.component_receipts_ns),
                {"MARK": NOW_NS - 10_000_000_000, "INDEX": NOW_NS - 60_000_000_000},
            )

            strict = await view.read(
                instrument_uid=record.instrument_uid,
                instrument_revision=record.metadata_revision,
                source_policy_id="crypto_liquid_v2",
                max_freshness_ms=2_000,
                gateway_epoch=5,
                now_ns=NOW_NS,
            )
            self.assertEqual(strict.reason, "STALE")

            _write_session(root, envelope, state="DISCONNECTED")
            self.assertEqual(
                (await view.read(
                    instrument_uid=record.instrument_uid,
                    instrument_revision=record.metadata_revision,
                    source_policy_id="crypto_liquid_v2",
                    max_freshness_ms=2_000,
                    gateway_epoch=5,
                    event_recency_policy=StalePolicy.OBSERVE,
                    max_session_liveness_ms=45_000,
                    now_ns=NOW_NS,
                )).reason,
                "SESSION_STATE",
            )

            _write_session(root, envelope, generation=4)
            self.assertEqual(
                (await view.read(
                    instrument_uid=record.instrument_uid,
                    instrument_revision=record.metadata_revision,
                    source_policy_id="crypto_liquid_v2",
                    max_freshness_ms=2_000,
                    gateway_epoch=5,
                    event_recency_policy=StalePolicy.OBSERVE,
                    max_session_liveness_ms=45_000,
                    now_ns=NOW_NS,
                )).reason,
                "SESSION_STATE",
            )

            _write_session(root, envelope)
            expired_mark = _paired_envelope(
                binding,
                sequence=2,
                generation=3,
                mark_received_at_ns=NOW_NS - 16_000_000_000,
                index_received_at_ns=NOW_NS - 60_000_000_000,
            )
            await view.remember(
                binding=binding,
                envelope=expired_mark,
                stored=_stored(expired_mark, offset=32),
                gateway_epoch=5,
            )
            self.assertEqual(
                (await view.read(
                    instrument_uid=record.instrument_uid,
                    instrument_revision=record.metadata_revision,
                    source_policy_id="crypto_liquid_v2",
                    max_freshness_ms=2_000,
                    gateway_epoch=5,
                    event_recency_policy=StalePolicy.OBSERVE,
                    max_session_liveness_ms=45_000,
                    now_ns=NOW_NS,
                )).reason,
                "COMPONENT_STALE",
            )

    async def test_pre_spool_record_is_readable_then_withdrawn_or_promoted_exactly(self):
        first = _envelope(self.binding, sequence=1, generation=2)
        await self.view.remember(
            binding=self.binding, envelope=first, stored=None, gateway_epoch=5
        )
        pre_spool = await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(pre_spool.record.delivery_stage, "CANONICAL_READ_COMMITTED")
        self.assertIsNone(pre_spool.record.spool_watermark_offset)

        # A failed append can only withdraw its own current unconfirmed record;
        # a later record must not disappear with it.
        later = _envelope(
            self.binding, sequence=2, generation=2, received_at_ns=NOW_NS + 1_000_000
        )
        await self.view.remember(
            binding=self.binding, envelope=later, stored=None, gateway_epoch=5
        )
        await self.view.withdraw(
            instrument_uid=self.record.instrument_uid,
            event_id=bytes(first.event_id),
            gateway_epoch=5,
        )
        retained = await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(retained.record.event_id, bytes(later.event_id))

        await self.view.remember(
            binding=self.binding,
            envelope=later,
            stored=_stored(later, offset=12),
            gateway_epoch=5,
        )
        confirmed = await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )
        self.assertEqual(confirmed.record.delivery_stage, "SPOOL_CONFIRMED")
        self.assertEqual(confirmed.record.spool_watermark_offset, 12)
        await self.view.withdraw(
            instrument_uid=self.record.instrument_uid,
            event_id=bytes(later.event_id),
            gateway_epoch=5,
        )
        self.assertIsNotNone((await self.view.read(
            instrument_uid=self.record.instrument_uid,
            instrument_revision=7,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
            gateway_epoch=5,
            now_ns=NOW_NS + 500_000_000,
        )).record)

    async def test_private_endpoint_and_reader_preserve_pair_and_never_call_venue(self):
        envelope = _envelope(self.binding, sequence=1)
        await self.view.remember(
            binding=self.binding, envelope=envelope, stored=None, gateway_epoch=5
        )
        gateway = _Gateway()
        app = FastAPI()
        install_execution_mark_index_read(app, gateway=gateway, view=self.view, secret=SECRET)
        body = json.dumps(
            {
                "schema": "qdl.v2.execution-mark-index-read.v1",
                "instrument_uid": self.record.instrument_uid,
                "instrument_revision": 7,
                "source_policy_id": "crypto_liquid_v2",
                "max_freshness_ms": 2_000,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            denied = await client.post("/internal/v2/execution/mark-index/latest", content=body)
            self.assertEqual(denied.status_code, 401)
            response = await client.post(
                "/internal/v2/execution/mark-index/latest",
                content=body,
                headers={"X-QDL-Stable-Signature": stable_hmac_signature(SECRET, body)},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["schema"], "qdl.v2.execution-mark-index-view.v2")
        self.assertEqual(payload["delivery_stage"], "CANONICAL_READ_COMMITTED")
        self.assertIsNone(payload["spool_watermark_offset"])
        self.assertEqual(
            response.headers["X-QDL-Execution-Freshness-Basis"],
            "PROVIDER_CONFIRMATION",
        )
        self.assertEqual(
            market_data_pb2.EventEnvelope.FromString(base64.b64decode(payload["canonical"])).instrument_uid,
            self.record.instrument_uid,
        )

        venue_calls = []

        async def stream_handler(request: httpx.Request) -> httpx.Response:
            venue_calls.append(str(request.url))
            self.assertEqual(request.url.host, "stream_v2_active")
            return httpx.Response(
                200,
                json=payload,
                headers={"X-QDL-Execution-Freshness-Basis": "PROVIDER_CONFIRMATION"},
                request=request,
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(stream_handler))
        reader = HttpExecutionMarkIndexReader(
            ("https://stream_v2_active:8200",), SECRET, client=client
        )
        try:
            result = await reader.fetch(
                ReferenceRequest(
                    self.record, ReferenceProduct.MARK_INDEX_PRICE, mark_index_kind=MarkIndexKind.BOTH
                ),
                max_freshness_ms=2_000,
                source_policy_id="crypto_liquid_v2",
            )
        finally:
            await client.aclose()
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual({field.name for field in result.observations[0].fields}, {"mark_price", "index_price"})
        self.assertEqual(result.lineage[0].provider_endpoint, LIVE_ENDPOINT)
        self.assertEqual(
            dict(result.observations[0].labels)["freshness_basis"],
            "PROVIDER_CONFIRMATION",
        )
        self.assertEqual(len(venue_calls), 1)

    async def test_reader_accepts_both_venues_but_rejects_cross_venue_identity(self):
        records = (
            _record(
                venue="BINANCE", market="USDM", native_symbol="SOLUSDT", base="SOL"
            ),
            _record(
                venue="OKX", market="SWAP", native_symbol="SOL-USDT-SWAP", base="SOL"
            ),
        )
        for sequence, record in enumerate(records, start=1):
            with self.subTest(venue=record.identity.venue):
                binding = _binding(record)
                envelope = _envelope(binding, sequence=sequence)
                payload = {
                    "schema": "qdl.v2.execution-mark-index-view.v2",
                    "lease_epoch": 5,
                    "spool_watermark_offset": sequence,
                    "delivery_stage": "SPOOL_CONFIRMED",
                    "canonical": base64.b64encode(
                        envelope.SerializeToString(deterministic=True)
                    ).decode("ascii"),
                }

                async def handler(request: httpx.Request) -> httpx.Response:
                    return httpx.Response(200, json=payload, request=request)

                client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
                reader = HttpExecutionMarkIndexReader(
                    ("https://stream_v2_active:8200",), SECRET, client=client
                )
                try:
                    result = await reader.fetch(
                        ReferenceRequest(
                            record,
                            ReferenceProduct.MARK_INDEX_PRICE,
                            mark_index_kind=MarkIndexKind.BOTH,
                        ),
                        max_freshness_ms=2_000,
                        source_policy_id="crypto_liquid_v2",
                    )
                finally:
                    await client.aclose()
                self.assertEqual(result.status, ReferenceStatus.OK)
                self.assertEqual(
                    result.observations[0].instrument_uid, record.instrument_uid
                )

        expected = records[0]
        wrong = _envelope(_binding(records[1]), sequence=99)
        payload = {
            "schema": "qdl.v2.execution-mark-index-view.v2",
            "lease_epoch": 5,
            "spool_watermark_offset": 99,
            "delivery_stage": "SPOOL_CONFIRMED",
            "canonical": base64.b64encode(
                wrong.SerializeToString(deterministic=True)
            ).decode("ascii"),
        }

        async def wrong_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload, request=request)

        client = httpx.AsyncClient(transport=httpx.MockTransport(wrong_handler))
        reader = HttpExecutionMarkIndexReader(
            ("https://stream_v2_active:8200",), SECRET, client=client
        )
        try:
            result = await reader.fetch(
                ReferenceRequest(expected, ReferenceProduct.MARK_INDEX_PRICE),
                max_freshness_ms=2_000,
                source_policy_id="crypto_liquid_v2",
            )
        finally:
            await client.aclose()
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "LIVE_VIEW_PROTOCOL")

    async def test_reader_rejects_missing_session_provenance_for_quiet_request(self):
        envelope = _envelope(self.binding, sequence=98)
        envelope.source_session_id = ""
        payload = {
            "schema": "qdl.v2.execution-mark-index-view.v2",
            "lease_epoch": 5,
            "spool_watermark_offset": 98,
            "delivery_stage": "SPOOL_CONFIRMED",
            "canonical": base64.b64encode(
                envelope.SerializeToString(deterministic=True)
            ).decode("ascii"),
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=payload,
                headers={
                    "X-QDL-Execution-Freshness-Basis": "PROVIDER_CONFIRMATION",
                    "X-QDL-Execution-Recency-Mode": "COMPONENT_SESSION_LIVE",
                    "X-QDL-Execution-Session-State": "LIVE",
                    "X-QDL-Execution-Session-Liveness-Ms": "1",
                    "X-QDL-Execution-Session-Checked-At-Ns": str(NOW_NS),
                    "X-QDL-Execution-Component-Receipts-Ns": (
                        f"INDEX={NOW_NS - 1},MARK={NOW_NS - 1}"
                    ),
                    "X-QDL-Execution-Component-Quiet-After-Ms": (
                        "INDEX=70000,MARK=15000"
                    ),
                },
                request=request,
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        reader = HttpExecutionMarkIndexReader(
            ("https://stream_v2_active:8200",), SECRET, client=client
        )
        try:
            result = await reader.fetch(
                ReferenceRequest(self.record, ReferenceProduct.MARK_INDEX_PRICE),
                max_freshness_ms=2_000,
                source_policy_id="crypto_liquid_v2",
                event_recency_policy=StalePolicy.OBSERVE,
                max_session_liveness_ms=45_000,
            )
        finally:
            await client.aclose()
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "LIVE_VIEW_PROTOCOL")

    async def test_reader_uses_one_deadline_across_active_passive_urls(self):
        envelope = _envelope(self.binding, sequence=50)
        payload = {
            "schema": "qdl.v2.execution-mark-index-view.v2",
            "lease_epoch": 5,
            "spool_watermark_offset": 50,
            "delivery_stage": "SPOOL_CONFIRMED",
            "canonical": base64.b64encode(
                envelope.SerializeToString(deterministic=True)
            ).decode("ascii"),
        }
        calls = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.host)
            if request.url.host == "stream_v2_active":
                await asyncio.sleep(0.05)
            return httpx.Response(200, json=payload, request=request)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        reader = HttpExecutionMarkIndexReader(
            (
                "https://stream_v2_active:8200",
                "https://stream_v2_passive:8200",
            ),
            SECRET,
            timeout_seconds=1.0,
            client=client,
        )
        try:
            result = await reader.fetch(
                ReferenceRequest(self.record, ReferenceProduct.MARK_INDEX_PRICE),
                max_freshness_ms=2_000,
                source_policy_id="crypto_liquid_v2",
                deadline_ms=10,
            )
        finally:
            await client.aclose()
        self.assertEqual(result.status, ReferenceStatus.ERROR)
        self.assertEqual(result.error_code, "LIVE_VIEW_UNAVAILABLE")
        self.assertEqual(calls, ["stream_v2_active"])

    async def test_reader_fails_over_within_one_deadline(self):
        envelope = _envelope(self.binding, sequence=51)
        payload = {
            "schema": "qdl.v2.execution-mark-index-view.v2",
            "lease_epoch": 5,
            "spool_watermark_offset": 51,
            "delivery_stage": "SPOOL_CONFIRMED",
            "canonical": base64.b64encode(
                envelope.SerializeToString(deterministic=True)
            ).decode("ascii"),
        }
        calls = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.host)
            if request.url.host == "stream_v2_active":
                return httpx.Response(
                    409,
                    json={"detail": "execution MARK/INDEX gateway fenced"},
                    request=request,
                )
            return httpx.Response(
                200,
                json=payload,
                headers={"X-QDL-Execution-Freshness-Basis": "PROVIDER_CONFIRMATION"},
                request=request,
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        reader = HttpExecutionMarkIndexReader(
            (
                "https://stream_v2_active:8200",
                "https://stream_v2_passive:8200",
            ),
            SECRET,
            timeout_seconds=1.0,
            client=client,
        )
        try:
            result = await reader.fetch(
                ReferenceRequest(self.record, ReferenceProduct.MARK_INDEX_PRICE),
                max_freshness_ms=2_000,
                source_policy_id="crypto_liquid_v2",
                deadline_ms=100,
            )
        finally:
            await client.aclose()
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(calls, ["stream_v2_active", "stream_v2_passive"])


class ExecutionMarkIndexQueryRoutingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.record = _record(
            venue="BINANCE", market="USDM", native_symbol="BTCUSDT", base="BTC"
        )
        registry = InstrumentRegistry()
        registry.register(self.record, [])
        self.fallback = _FallbackReferenceAdapter()
        self.entitlements = EntitlementPolicy((EntitlementGrant(
            source_id="BINANCE_DIRECT",
            license_revision="execution-mark-index-live-test",
            purposes=frozenset({AccessPurpose.INTERNAL_EXECUTION, AccessPurpose.INTERNAL_ALPHA}),
            products=frozenset({DataProduct.CANONICAL_SNAPSHOT}),
            valid_from_ns=0,
        ),))
        self.common = {
            "instruments": InstrumentQuery(registry),
            "backend": MemoryMarketDataBackend(),
            "entitlements": self.entitlements,
            "reference_batch": ReferenceBatch({("BINANCE", "USDM"): self.fallback}, clock_ns=lambda: NOW_NS),
            "reference_source_id": lambda _record: "BINANCE_DIRECT",
            "clock_ns": lambda: NOW_NS,
        }

    def _execution_requirement(
        self,
        *,
        source_policy_id: str = "crypto_liquid_v2",
        max_freshness_ms: int = 2_000,
        event_recency_policy: StalePolicy | None = None,
        max_session_liveness_ms: int | None = None,
        deadline_ms: int = 20_000,
    ) -> ReferenceDataRequirement:
        return ReferenceDataRequirement(
            instrument_uid=self.record.instrument_uid,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id=source_policy_id,
            limit=1,
            page_size=1,
            max_pages=1,
            max_freshness_ms=max_freshness_ms,
            event_recency_policy=event_recency_policy,
            max_session_liveness_ms=max_session_liveness_ms,
            deadline_ms=deadline_ms,
        )

    async def test_execution_uses_live_view_but_alpha_reference_keeps_existing_adapter(self):
        live = _LiveReader()
        service = V2QueryService(**self.common, execution_mark_index_reader=live)
        execution = await service.reference_data_batch_async(
            ReferenceBatchRequirement("execution-reader", (self._execution_requirement(),)),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertFalse(execution.partial)
        self.assertEqual(live.calls, 1)
        self.assertEqual(self.fallback.calls, 0)
        self.assertEqual(
            execution.results[0].result.lineage[0].provider_endpoint, LIVE_ENDPOINT
        )

        alpha_requirement = ReferenceDataRequirement(
            instrument_uid=self.record.instrument_uid,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_liquid_v2",
            max_freshness_ms=2_000,
        )
        alpha = await service.reference_data_batch_async(
            ReferenceBatchRequirement("alpha-reader", (alpha_requirement,)),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        self.assertFalse(alpha.partial)
        self.assertEqual(live.calls, 1)
        self.assertEqual(self.fallback.calls, 1)
        self.assertEqual(
            alpha.results[0].result.lineage[0].provider_endpoint,
            "TEST_REFERENCE_REST_ADAPTER",
        )

    async def test_execution_live_view_stale_is_typed_and_never_falls_back_to_rest(self):
        live = _LiveReader(status=ReferenceStatus.ERROR)
        service = V2QueryService(**self.common, execution_mark_index_reader=live)
        result = await service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader", (self._execution_requirement(),), require_all=False
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertTrue(result.partial)
        self.assertEqual(result.results[0].problem.code.value, "DATA_STALE")
        self.assertEqual(live.calls, 1)
        self.assertEqual(self.fallback.calls, 0)

    async def test_provider_confirmation_freshness_is_explicit_and_source_event_remains_lineage(self):
        clock = {"ns": NOW_NS + 1_500_000_000}
        requirement = self._execution_requirement()
        service = V2QueryService(
            **{**self.common, "clock_ns": lambda: clock["ns"]},
            execution_mark_index_reader=_LiveReader(
                source_event_time_ns=NOW_NS - 3_000_000_000,
                provider_confirmation_ns=NOW_NS,
                freshness_basis="PROVIDER_CONFIRMATION",
            ),
        )
        accepted = await service.reference_data_batch_async(
            ReferenceBatchRequirement("execution-reader", (requirement,)),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertFalse(accepted.partial)
        labels = dict(accepted.results[0].result.observations[0].labels)
        self.assertEqual(labels["freshness_basis"], "PROVIDER_CONFIRMATION")
        self.assertEqual(labels["provider_confirmation_ns"], str(NOW_NS))
        self.assertEqual(
            accepted.results[0].result.observations[0].observed_at_ns,
            NOW_NS - 3_000_000_000,
        )

        source_event_service = V2QueryService(
            **{**self.common, "clock_ns": lambda: clock["ns"]},
            execution_mark_index_reader=_LiveReader(
                source_event_time_ns=NOW_NS - 3_000_000_000,
                provider_confirmation_ns=NOW_NS,
                freshness_basis="SOURCE_EVENT",
            ),
        )
        rejected = await source_event_service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader", (requirement,), require_all=False
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertTrue(rejected.partial)
        self.assertEqual(rejected.results[0].problem.code.value, "DATA_STALE")

    async def test_execution_live_snapshot_that_ages_before_assembly_is_never_ok(self):
        clock = {"ns": NOW_NS}

        class AgingReader(_LiveReader):
            async def fetch(self, *args, **kwargs):
                result = await super().fetch(*args, **kwargs)
                clock["ns"] += 2_001_000_000
                return result

        service = V2QueryService(
            **{**self.common, "clock_ns": lambda: clock["ns"]},
            execution_mark_index_reader=AgingReader(),
        )
        result = await service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader",
                (self._execution_requirement(),),
                require_all=False,
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertTrue(result.partial)
        self.assertEqual(result.results[0].problem.code.value, "DATA_STALE")
        self.assertEqual(self.fallback.calls, 0)

    async def test_quiet_execution_mark_index_rechecks_component_and_session_evidence(self):
        clock = {"ns": NOW_NS}
        requirement = self._execution_requirement(
            event_recency_policy=StalePolicy.OBSERVE,
            max_session_liveness_ms=45_000,
        )
        service = V2QueryService(
            **{**self.common, "clock_ns": lambda: clock["ns"]},
            execution_mark_index_reader=_LiveReader(
                source_event_time_ns=NOW_NS - 60_000_000_000,
                provider_confirmation_ns=NOW_NS - 60_000_000_000,
                freshness_basis="PROVIDER_CONFIRMATION",
            ),
        )
        accepted = await service.reference_data_batch_async(
            ReferenceBatchRequirement("execution-reader", (requirement,)),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertFalse(accepted.partial)
        labels = dict(accepted.results[0].result.observations[0].labels)
        self.assertEqual(labels["recency_mode"], "COMPONENT_SESSION_LIVE")
        self.assertEqual(labels["component_index_quiet_after_ms"], "70000")

        clock["ns"] += 70_001_000_000
        stale = await service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader", (requirement,), require_all=False,
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        )
        self.assertTrue(stale.partial)
        self.assertEqual(stale.results[0].problem.code.value, "DATA_STALE")
        self.assertEqual(self.fallback.calls, 0)

    async def test_execution_live_singleflight_isolated_by_policy_freshness_and_deadline(self):
        class BlockingReader(_LiveReader):
            def __init__(self):
                super().__init__()
                self.entered = 0
                self.started = asyncio.Event()
                self.release = asyncio.Event()

            async def fetch(self, *args, **kwargs):
                self.entered += 1
                if self.entered == 2:
                    self.started.set()
                await self.release.wait()
                return await super().fetch(*args, **kwargs)

        live = BlockingReader()
        service = V2QueryService(**self.common, execution_mark_index_reader=live)
        first = asyncio.create_task(service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader-a",
                (self._execution_requirement(source_policy_id="policy-a"),),
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        ))
        second = asyncio.create_task(service.reference_data_batch_async(
            ReferenceBatchRequirement(
                "execution-reader-b",
                (self._execution_requirement(
                    source_policy_id="policy-b",
                    max_freshness_ms=1_500,
                    deadline_ms=1_500,
                ),),
            ),
            purpose=AccessPurpose.INTERNAL_EXECUTION,
        ))
        await asyncio.wait_for(live.started.wait(), timeout=0.2)
        live.release.set()
        left, right = await asyncio.gather(first, second)
        self.assertFalse(left.partial)
        self.assertFalse(right.partial)
        self.assertEqual(live.calls, 2)
        self.assertEqual(
            set(live.calls_by_policy),
            {("policy-a", 2_000, 2_000), ("policy-b", 1_500, 1_500)},
        )

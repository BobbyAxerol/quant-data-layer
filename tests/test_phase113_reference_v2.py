"""Phase 11.3 source-only contract and scheduler acceptance.

Every provider-like value here is deterministic test provenance.  These tests
exercise the public V2 boundary, provider-neutral reference scheduler and SDK
without opening a provider socket, starting a role, or writing shared state.
"""

from __future__ import annotations

import asyncio
import time
import unittest

from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.domain.capabilities import CapabilityAvailability, FeedCapability
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
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
from qdl.query.reference import ReferenceBatchRequirement, ReferenceDataRequirement
from qdl.reference.batch import ReferenceBatch, ReferenceBatchPolicy
from qdl.reference.contracts import (
    BasisSeries,
    LongShortKind,
    ReferenceCoverage,
    ReferenceFetch,
    ReferenceLineage,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceUnavailable,
    decimal_field,
)
from qdl_sdk import AsyncDataLayerClient, Grade
from qdl_sdk.errors import ContinuityError
from qdl_sdk.reference import ReferenceRequirement
from tests.phase7_support import make_identity, make_token, manifest_mapping


NOW_NS = 1_800_000_000_000_000_000


def record(*, venue: str, market: str, symbol: str, base: str) -> InstrumentRecord:
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
        native_symbol=symbol,
        base_asset=base,
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


def coverage(request: ReferenceRequest) -> ReferenceCoverage:
    return ReferenceCoverage(
        requested_start_ms=request.start_ms,
        requested_end_ms=request.end_ms,
        observed_min_ms=request.start_ms,
        observed_max_ms=request.end_ms,
        complete_left=True,
        complete_right=True,
        truncated=False,
        terminal_reason="TEST_FULL_COVERAGE",
    )


class FixtureReferenceAdapter:
    def __init__(self, *, delay_seconds: float = 0.0, observed_at_ns: int = NOW_NS):
        self.delay_seconds = delay_seconds
        self.observed_at_ns = observed_at_ns
        self.calls = 0
        self.active = 0
        self.max_active = 0

    async def fetch(self, request, *, capability, received_at_ns):
        del capability, received_at_ns
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            if request.product is ReferenceProduct.BASIS and request.instrument.identity.venue == "OKX":
                raise ReferenceUnavailable("fixture has no same-venue OKX basis source")
            field = decimal_field("funding_rate", "0.000125", "DIMENSIONLESS_RATE")
            assert field is not None
            observation = ReferenceObservation(
                instrument_uid=request.instrument.instrument_uid,
                instrument_revision=request.instrument.metadata_revision,
                product=request.product,
                observed_at_ns=self.observed_at_ns,
                fields=(field,),
                labels=(("native_symbol", request.instrument.native_symbol),),
            )
            lineage = ReferenceLineage(
                provider=f"{request.instrument.identity.venue}_DIRECT",
                provider_endpoint="TEST_REFERENCE_FIXTURE",
                source_role="REFERENCE",
                adapter_version="phase113-test/1",
                capability_name=request.product.value.lower(),
            )
            return ReferenceFetch((observation,), (lineage,), coverage(request))
        finally:
            self.active -= 1


def fixture_batch(*, observed_at_ns: int = NOW_NS, policy=None) -> ReferenceBatch:
    return ReferenceBatch(
        {
            ("BINANCE", "USDM"): FixtureReferenceAdapter(observed_at_ns=observed_at_ns),
            ("OKX", "SWAP"): FixtureReferenceAdapter(observed_at_ns=observed_at_ns),
        },
        policy=policy or ReferenceBatchPolicy(),
        clock_ns=lambda: NOW_NS,
    )


def grants() -> EntitlementPolicy:
    return EntitlementPolicy(
        tuple(
            EntitlementGrant(
                source_id=source_id,
                license_revision="phase113-test",
                purposes=frozenset({AccessPurpose.INTERNAL_ALPHA, AccessPurpose.INTERNAL_RESEARCH}),
                products=frozenset({DataProduct.CANONICAL_HISTORY, DataProduct.CANONICAL_SNAPSHOT}),
                valid_from_ns=0,
            )
            for source_id in ("BINANCE_DIRECT", "OKX_DIRECT")
        )
    )


class ReferenceRequirementAndSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.binance = record(venue="BINANCE", market="USDM", symbol="BTCUSDT", base="BTC")
        self.okx = record(venue="OKX", market="SWAP", symbol="ETH-USDT-SWAP", base="ETH")

    def test_reference_requirement_is_manifest_bounded_and_execution_forbidden(self):
        requirement = ReferenceDataRequirement(
            instrument_uid=self.binance.instrument_uid,
            product=ReferenceProduct.FUNDING_RATE,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="reference_primary_v2",
            start_time_ns=NOW_NS - 86_400_000_000_000,
            end_time_ns=NOW_NS,
            limit=365,
        )
        gate = requirement.data_requirement
        self.assertEqual(gate.feed, FeedType.FUNDING_RATE)
        self.assertEqual(gate.warmup_limit, 365)
        self.assertFalse(gate.require_final_bars)
        with self.assertRaisesRegex(ValueError, "execution-grade"):
            ReferenceDataRequirement(
                instrument_uid=self.binance.instrument_uid,
                product=ReferenceProduct.FUNDING_RATE,
                consumer_grade=ConsumerGrade.EXECUTION,
                source_policy_id="reference_primary_v2",
                start_time_ns=NOW_NS - 1_000_000,
                end_time_ns=NOW_NS,
            )

    async def test_provider_lane_caps_same_venue_and_timeout_is_typed(self):
        adapter = FixtureReferenceAdapter(delay_seconds=0.02)
        batch = ReferenceBatch(
            {("BINANCE", "USDM"): adapter},
            policy=ReferenceBatchPolicy(
                max_concurrency=2,
                max_provider_concurrency=1,
                request_timeout_seconds=0.2,
            ),
            clock_ns=lambda: NOW_NS,
        )
        second = record(venue="BINANCE", market="USDM", symbol="ETHUSDT", base="ETH")
        requests = (
            ReferenceRequest(
                self.binance,
                ReferenceProduct.FUNDING_RATE,
                start_ms=NOW_NS // 1_000_000 - 10_000,
                end_ms=NOW_NS // 1_000_000,
            ),
            ReferenceRequest(
                second,
                ReferenceProduct.FUNDING_RATE,
                start_ms=NOW_NS // 1_000_000 - 10_000,
                end_ms=NOW_NS // 1_000_000,
            ),
        )
        results = await batch.fetch(requests)
        self.assertTrue(all(item.status.value == "OK" for item in results))
        self.assertEqual(adapter.max_active, 1)
        self.assertEqual(batch.stats()["source_calls"], 2)

        timeout_adapter = FixtureReferenceAdapter(delay_seconds=0.2)
        timeout_batch = ReferenceBatch(
            {("BINANCE", "USDM"): timeout_adapter},
            policy=ReferenceBatchPolicy(request_timeout_seconds=0.1),
            clock_ns=lambda: NOW_NS,
        )
        timed_out = await timeout_batch.fetch_one(requests[0])
        self.assertEqual(timed_out.status.value, "ERROR")
        self.assertEqual(timed_out.error_code, "PROVIDER_TIMEOUT")
        self.assertEqual(timeout_batch.stats()["provider_timeouts"], 1)


class ReferenceServiceAndApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.binance = record(venue="BINANCE", market="USDM", symbol="BTCUSDT", base="BTC")
        self.okx = record(venue="OKX", market="SWAP", symbol="ETH-USDT-SWAP", base="ETH")
        registry = InstrumentRegistry()
        registry.register(self.binance, [])
        registry.register(self.okx, [])
        self.service = V2QueryService(
            instruments=InstrumentQuery(registry),
            backend=MemoryMarketDataBackend(),
            entitlements=grants(),
            reference_batch=fixture_batch(),
            reference_source_id=lambda item: f"{item.identity.venue}_DIRECT",
            clock_ns=lambda: NOW_NS,
        )

    def funding_requirement(self, instrument_uid: str, *, freshness_ms: int | None = None):
        return ReferenceDataRequirement(
            instrument_uid=instrument_uid,
            product=ReferenceProduct.FUNDING_RATE,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="reference_primary_v2",
            start_time_ns=NOW_NS - 86_400_000_000_000,
            end_time_ns=NOW_NS,
            limit=10,
            max_freshness_ms=freshness_ms,
        )

    async def test_multi_venue_service_keeps_identity_coverage_and_missing_typed(self):
        batch = ReferenceBatchRequirement(
            "phase113-alpha",
            (
                self.funding_requirement(self.binance.instrument_uid),
                self.funding_requirement(self.okx.instrument_uid),
            ),
        )
        result = await self.service.reference_data_batch_async(
            batch, purpose=AccessPurpose.INTERNAL_ALPHA
        )
        self.assertFalse(result.partial)
        self.assertEqual(result.success_count, 2)
        self.assertEqual(
            [item.result.request.instrument.instrument_uid for item in result.results],
            [self.binance.instrument_uid, self.okx.instrument_uid],
        )
        self.assertEqual(self.service.last_reference_batch_evidence["item_count"], 2)

        unavailable = ReferenceDataRequirement(
            instrument_uid=self.okx.instrument_uid,
            product=ReferenceProduct.BASIS,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="reference_primary_v2",
            start_time_ns=NOW_NS - 86_400_000_000_000,
            end_time_ns=NOW_NS,
            interval="1d",
            basis_series=BasisSeries.CONTINUOUS,
            basis_contract_type="CURRENT_QUARTER",
        )
        partial = await self.service.reference_data_batch_async(
            ReferenceBatchRequirement("phase113-alpha", (unavailable,), require_all=False),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        self.assertTrue(partial.partial)
        self.assertEqual(partial.results[0].problem.code.value, "UNSUPPORTED_FEED")
        self.assertEqual(partial.results[0].result.status.value, "UNAVAILABLE")

    async def test_stale_reference_result_fails_closed(self):
        registry = InstrumentRegistry()
        registry.register(self.binance, [])
        stale = V2QueryService(
            instruments=InstrumentQuery(registry),
            backend=MemoryMarketDataBackend(),
            entitlements=grants(),
            reference_batch=fixture_batch(observed_at_ns=NOW_NS - 2_000_000_000),
            reference_source_id=lambda item: f"{item.identity.venue}_DIRECT",
            clock_ns=lambda: NOW_NS,
        )
        result = await stale.reference_data_batch_async(
            ReferenceBatchRequirement(
                "phase113-alpha",
                (self.funding_requirement(self.binance.instrument_uid, freshness_ms=100),),
                require_all=False,
            ),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        self.assertEqual(result.results[0].problem.code.value, "DATA_STALE")

    async def test_rest_api_checks_manifest_and_serializes_exact_decimals(self):
        consumer_id = "phase113-api-alpha"
        subject = "spiffe://qdl/paper/phase113-api-alpha"
        payload = manifest_mapping(
            consumer_id=consumer_id,
            subject=subject,
            instrument_uid=self.binance.instrument_uid,
            feed="FUNDING_RATE",
            interval=None,
            source_policy_id="reference_primary_v2",
        )
        requirement = payload["spec"]["requirements"][0]
        requirement.update({"warmup_limit": 500, "require_final_bars": False})
        identity = make_identity(__import__("qdl.consumer", fromlist=["ConsumerManifestLoader"]).ConsumerManifestLoader.from_mapping(payload))
        client = TestClient(create_v2_app(self.service, identity_service=identity))
        response = client.post(
            "/v2/market-data/reference:batch",
            headers={
                "Authorization": f"Bearer {make_token(subject)}",
                "X-QDL-Consumer-ID": consumer_id,
                "X-QDL-Purpose": "INTERNAL_ALPHA",
            },
            json={
                "consumer_id": consumer_id,
                "require_all": True,
                "requirements": [
                    {
                        "instrument_uid": self.binance.instrument_uid,
                        "product": "FUNDING_RATE",
                        "consumer_grade": "ALPHA",
                        "source_policy_id": "reference_primary_v2",
                        "start_time_ns": NOW_NS - 86_400_000_000_000,
                        "end_time_ns": NOW_NS,
                        "limit": 10,
                    }
                ],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        item = response.json()["results"][0]
        self.assertEqual(item["data"]["status"], "OK")
        self.assertEqual(item["data"]["observations"][0]["fields"][0]["value"], {
            "coefficient": "125",
            "scale": 6,
            "source_text": "0.000125",
        })
        denied = client.post(
            "/v2/market-data/reference:batch",
            headers={
                "Authorization": f"Bearer {make_token(subject)}",
                "X-QDL-Consumer-ID": consumer_id,
                "X-QDL-Purpose": "INTERNAL_ALPHA",
            },
            json={
                "consumer_id": consumer_id,
                "requirements": [
                    {
                        "instrument_uid": self.binance.instrument_uid,
                        "product": "FUNDING_RATE",
                        "consumer_grade": "EXECUTION",
                        "source_policy_id": "reference_primary_v2",
                        "start_time_ns": NOW_NS - 86_400_000_000_000,
                        "end_time_ns": NOW_NS,
                    }
                ],
            },
        )
        self.assertEqual(denied.status_code, 400)
        self.assertEqual(denied.json()["code"], "INVALID_ARGUMENT")


class _NoopStreamTransport:
    async def close(self):
        return None


class _ReferenceTransport:
    def __init__(self, *, corrupt: bool = False):
        self.calls: list[int] = []
        self.corrupt = corrupt

    async def reference_batch(self, requirements, *, consumer_id, require_all):
        del consumer_id, require_all
        self.calls.append(len(requirements))
        results = []
        for index, requirement in enumerate(requirements):
            uid = "wrong-uid" if self.corrupt and index == 0 else requirement.instrument_uid
            results.append({
                "instrument_uid": uid,
                "product": requirement.product.value,
                "status": "OK",
                "data": {
                    "instrument_uid": uid,
                    "product": requirement.product.value,
                    "status": "OK",
                    "lineage": [{
                        "provider": "TEST_DIRECT",
                        "provider_endpoint": "TEST",
                        "source_role": "REFERENCE",
                        "adapter_version": "phase113-test/1",
                        "capability_name": "funding_rate",
                    }],
                    "coverage": {
                        "requested_start_ms": 1,
                        "requested_end_ms": 2,
                        "observed_min_ms": 1,
                        "observed_max_ms": 2,
                        "complete_left": True,
                        "complete_right": True,
                        "truncated": False,
                        "terminal_reason": "TEST",
                    },
                    "received_at_ns": NOW_NS,
                    "observations": [{
                        "instrument_uid": uid,
                        "instrument_revision": 1,
                        "product": requirement.product.value,
                        "observed_at_ns": NOW_NS,
                        "fields": [{
                            "name": "funding_rate",
                            "value": {"coefficient": "1", "scale": 4, "source_text": "0.0001"},
                            "unit": "DIMENSIONLESS_RATE",
                        }],
                        "labels": {},
                    }],
                    "cache_hit": False,
                    "coalesced": False,
                },
                "problem": None,
            })
        return {
            "schema": "qdl.reference.batch.v2",
            "request_id": f"phase113-{len(self.calls)}",
            "partial": False,
            "success_count": len(results),
            "error_count": 0,
            "results": results,
        }

    async def close(self):
        return None


class ReferenceSdkTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def requirements(count: int):
        return [
            ReferenceRequirement(
                instrument_uid=f"phase113-{index}",
                product="FUNDING_RATE",
                consumer_grade=Grade.ALPHA,
                source_policy_id="reference_primary_v2",
                start_time_ns=1_000_000,
                end_time_ns=2_000_000,
                limit=10,
            )
            for index in range(count)
        ]

    async def test_sdk_chunks_typed_reference_batches_without_cross_mixing(self):
        transport = _ReferenceTransport()
        client = AsyncDataLayerClient(
            query_transport=transport,
            stream_transport=_NoopStreamTransport(),
            consumer_id="phase113-sdk-alpha",
        )
        result = await client.reference_batch(self.requirements(101))
        self.assertEqual(transport.calls, [100, 1])
        self.assertEqual(len(result.results), 101)

    async def test_sdk_rejects_reference_identity_corruption(self):
        client = AsyncDataLayerClient(
            query_transport=_ReferenceTransport(corrupt=True),
            stream_transport=_NoopStreamTransport(),
            consumer_id="phase113-sdk-alpha",
        )
        with self.assertRaises(ContinuityError) as captured:
            await client.reference_batch(self.requirements(1))
        self.assertEqual(captured.exception.code, "CONFLICT")


if __name__ == "__main__":
    unittest.main()

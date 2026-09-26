from __future__ import annotations

import asyncio
import importlib
import time
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.api_v2.models import BatchResponse, WarmupResponse
from qdl.consumer import ConsumerManifestLoader
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from tests.phase7_support import make_identity, make_token, manifest_mapping
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    CanonicalErrorCode,
    ConsumerGrade,
    ContractMetadata,
    CoverageStatus,
    DataProduct,
    DataRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    GapRecord,
    HistoryResult,
    InstrumentQuery,
    MarketDataItem,
    MemoryMarketDataBackend,
    QualityMetadata,
    QueryProblem,
    QueryServiceError,
    SourceMetadata,
    V2QueryService,
    WarmupSpecification,
    WarmupTimeRange,
)


router_module = importlib.import_module("qdl.api_v2.router")


def contract() -> ContractMetadata:
    return ContractMetadata(
        schema_digest="a" * 64,
        contract_version="2.0.0-beta.1",
        normalizer_version="phase7-test",
        adapter_version="fixture-v1",
        instrument_catalog_revision=1,
        source_policy_revision=1,
        authority_revision=1,
        config_revision=1,
        correlation_id="phase7-api-test",
    )


def record(venue: str, market: str, symbol: str) -> InstrumentRecord:
    identity = InstrumentIdentity.create(
        venue=venue,
        market=market,
        product_type=ProductType.PERPETUAL,
        canonical_symbol=symbol,
    )
    return InstrumentRecord(
        identity=identity,
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol="BTCUSDT" if venue == "BINANCE" else "BTC-USDT-SWAP",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


class Phase5ApiTests(unittest.TestCase):
    def setUp(self):
        now = time.time_ns()
        self.registry = InstrumentRegistry()
        self.binance = record("BINANCE", "USDM", "BTC-USDT")
        self.okx = record("OKX", "SWAP", "BTC-USDT")
        self.registry.register(self.binance, [])
        self.registry.register(self.okx, [])
        self.backend = MemoryMarketDataBackend()
        self.requirement = DataRequirement(
            instrument_uid=self.binance.instrument_uid,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="alpha_crypto_primary_v1",
            interval="1m",
            warmup_limit=2,
            max_freshness_ms=10_000,
        )
        source = SourceMetadata("BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True)
        quality = QualityMetadata("LIVE", 10, False, True, True, "alpha_crypto_primary_v1")
        bars = tuple(
            MarketDataItem(
                instrument_uid=self.binance.instrument_uid,
                instrument_id=self.binance.instrument_id,
                instrument_revision=1,
                feed=FeedType.BAR,
                interval="1m",
                observed_at_ns=now - (1 - index) * 60_000_000_000,
                revision=0,
                payload={
                    "open_time_ns": now - (2 - index) * 60_000_000_000,
                    "close_time_ns": now - (1 - index) * 60_000_000_000,
                    "open": str(60_000 + index),
                    "high": str(60_001 + index),
                    "low": str(59_999 + index),
                    "close": str(60_000 + index),
                    "volume": "12.5",
                    "volume_unit": "BASE_ASSET",
                    "trade_count": 10,
                    "origin": "VENUE_NATIVE",
                    "is_final": True,
                },
                source=source,
                quality=quality,
                contract=contract(),
                cursor=f"cursor-{index + 1}",
                snapshot_id="snapshot-2",
                watermark_offset=index + 1,
                bar_lifecycle=BarLifecycle.FINAL,
            )
            for index in range(2)
        )
        self.backend.put_latest(self.requirement, bars[-1])
        self.backend.put_history(
            self.requirement,
            HistoryResult(
                bars, CoverageStatus.FULL, "snapshot-2", "signed-cursor-2", 2, now
            ),
        )
        self.backend.put_gap(GapRecord(
            "gap-1", self.okx.instrument_uid, FeedType.TRADE, "OKX_DIRECT",
            "100", "102", now,
        ))
        grants = tuple(
            EntitlementGrant(
                source_id=source_id,
                license_revision="public-market-data-v1",
                purposes=frozenset({
                    AccessPurpose.INTERNAL_ALPHA,
                    AccessPurpose.INTERNAL_EXECUTION,
                    AccessPurpose.INTERNAL_RESEARCH,
                }),
                products=frozenset({
                    DataProduct.CANONICAL_SNAPSHOT,
                    DataProduct.CANONICAL_HISTORY,
                }),
                valid_from_ns=0,
            )
            for source_id in ("BINANCE_DIRECT", "OKX_DIRECT")
        )
        self.service = V2QueryService(
            instruments=InstrumentQuery(self.registry),
            backend=self.backend,
            entitlements=EntitlementPolicy(grants),
        )
        self.consumer_id = "phase5-api-shadow"
        self.subject = "spiffe://qdl/paper/phase5-api-shadow"
        manifest_payload = manifest_mapping(
            consumer_id=self.consumer_id,
            subject=self.subject,
            instrument_uid=self.binance.instrument_uid,
            source_policy_id="alpha_crypto_primary_v1",
        )
        base_requirement = manifest_payload["spec"]["requirements"][0]
        manifest_payload["spec"]["purposes"] = [
            "INTERNAL_ALPHA", "INTERNAL_EXECUTION"
        ]
        manifest_payload["spec"]["requirements"] = [
            base_requirement,
            {**base_requirement, "instrument_uid": self.okx.instrument_uid},
            {**base_requirement, "consumer_grade": "EXECUTION"},
            {**base_requirement, "source_policy_id": "alpha_crypto_reference_v1"},
            {
                **base_requirement,
                "consumer_grade": "EXECUTION",
                "source_policy_id": "alpha_crypto_reference_v1",
            },
        ]
        self.manifest = ConsumerManifestLoader.from_mapping(manifest_payload)
        self.identity = make_identity(self.manifest)
        self.client = TestClient(
            create_v2_app(self.service, identity_service=self.identity),
            raise_server_exceptions=False,
        )
        self.client.headers.update(self.headers())

    def headers(self, purpose: str = "INTERNAL_ALPHA"):
        return {
            "Authorization": f"Bearer {make_token(self.subject)}",
            "X-QDL-Consumer-ID": self.consumer_id,
            "X-QDL-Purpose": purpose,
        }

    def params(self, **overrides):
        values = {
            "feed": "BAR",
            "interval": "1m",
            "source_policy_id": "alpha_crypto_primary_v1",
            "consumer_grade": "ALPHA",
            "max_freshness_ms": 10000,
        }
        values.update(overrides)
        return values

    def test_instruments_are_provider_neutral_and_cursor_paginated(self):
        first = self.client.get("/v2/instruments", params={"limit": 1})
        self.assertEqual(first.status_code, 200)
        payload = first.json()
        self.assertEqual(len(payload["items"]), 1)
        self.assertIsNotNone(payload["next_cursor"])
        second = self.client.get(
            "/v2/instruments", params={"limit": 1, "cursor": payload["next_cursor"]}
        )
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(second.json()["items"]), 1)
        by_uid = self.client.get(f"/v2/instruments/{self.okx.instrument_uid}")
        self.assertEqual(by_uid.json()["venue"], "OKX")
        self.assertNotIn("provider", by_uid.request.url.path)

    def test_snapshot_warmup_history_status_gaps_and_readiness(self):
        snapshot = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            params=self.params(),
        )
        self.assertEqual(snapshot.status_code, 200, snapshot.text)
        self.assertEqual(snapshot.json()["data"]["source"]["provider"], "BINANCE_DIRECT")
        for route in ("warmup", "history"):
            response = self.client.get(
                f"/v2/market-data/{self.binance.instrument_uid}/{route}",
                params=self.params(limit=2),
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["count"], 2)
            self.assertEqual(response.json()["stream_cursor"], "signed-cursor-2")
        status = self.client.get(
            f"/v2/feeds/{self.binance.instrument_uid}/status", params=self.params()
        )
        self.assertEqual(status.json()["quality"]["state"], "LIVE")
        gaps = self.client.get("/v2/data-quality/gaps").json()["items"]
        self.assertEqual(gaps[0]["source_id"], "OKX_DIRECT")
        self.assertEqual(self.client.get("/v2/system/readiness").json()["authority"], "V1")

    def test_gap_diagnostic_preserves_typed_incomplete_result(self):
        async def incomplete_scan():
            raise QueryServiceError(
                QueryProblem(
                    CanonicalErrorCode.PARTIAL_RESULT,
                    "global gap diagnostic exceeded its work deadline",
                    True,
                    retry_after_ms=1_000,
                ),
                request_id="phase1-gap-diagnostic",
            )

        with patch.object(self.service, "open_gaps_async", new=incomplete_scan):
            response = self.client.get("/v2/data-quality/gaps")
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["code"], "PARTIAL_RESULT")
        self.assertTrue(response.json()["retryable"])

    def test_query_routes_pass_authenticated_identity_to_owned_read_lanes(self):
        calls = []
        snapshot_original = self.service.snapshot_async
        status_original = self.service.status_async
        readiness_original = self.service.readiness_async

        async def record_snapshot(*args, **kwargs):
            calls.append(("snapshot", kwargs["consumer_id"]))
            return await snapshot_original(*args, **kwargs)

        async def record_status(*args, **kwargs):
            calls.append(("status", kwargs["consumer_id"]))
            return await status_original(*args, **kwargs)

        async def record_readiness(*args, **kwargs):
            calls.append(("readiness", args[0].consumer_id))
            return await readiness_original(*args, **kwargs)

        requirement = {
            "instrument_uid": self.binance.instrument_uid,
            "feed": "BAR",
            "consumer_grade": "ALPHA",
            "source_policy_id": "alpha_crypto_primary_v1",
            "interval": "1m",
            "max_freshness_ms": 10_000,
        }
        with (
            patch.object(self.service, "snapshot_async", new=record_snapshot),
            patch.object(self.service, "status_async", new=record_status),
            patch.object(self.service, "readiness_async", new=record_readiness),
        ):
            snapshot = self.client.get(
                f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
                params=self.params(),
            )
            status = self.client.get(
                f"/v2/feeds/{self.binance.instrument_uid}/status",
                params=self.params(),
            )
            readiness = self.client.post(
                "/v2/system/readiness:check",
                json={
                    "consumer_id": self.consumer_id,
                    "requirements": [requirement],
                },
            )

        self.assertEqual(snapshot.status_code, 200, snapshot.text)
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(readiness.status_code, 200, readiness.text)
        self.assertEqual(
            calls,
            [
                ("snapshot", self.consumer_id),
                ("status", self.consumer_id),
                ("readiness", self.consumer_id),
            ],
        )

    def test_batch_partial_semantics_and_execution_fail_closed(self):
        missing = self.requirement.__dict__ | {
            "instrument_uid": self.okx.instrument_uid,
            "consumer_grade": "ALPHA",
            "feed": "BAR",
            "require_full_coverage": False,
            "stale_policy": "OBSERVE",
            "gap_policy": "OBSERVE",
            "recovery": "SNAPSHOT_AND_REPLAY",
            "bar_revision_policy": "LATEST",
        }
        existing = self.requirement.__dict__ | {
            "consumer_grade": "ALPHA",
            "feed": "BAR",
            "stale_policy": "BLOCK",
            "gap_policy": "BLOCK",
            "recovery": "SNAPSHOT_AND_REPLAY",
            "bar_revision_policy": "LATEST",
        }
        response = self.client.post(
            "/v2/market-data/warmup:batch",
            json={
                "consumer_id": self.consumer_id,
                "require_all": False,
                "requirements": [existing, missing],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertTrue(payload["partial"])
        self.assertEqual((payload["success_count"], payload["error_count"]), (1, 1))
        self.assertEqual(payload["results"][1]["status"], "DATA_NOT_READY")

        invalid_execution = {**existing, "consumer_grade": "EXECUTION", "gap_policy": "OBSERVE"}
        denied = self.client.post(
            "/v2/system/readiness:check",
            headers={"X-QDL-Purpose": "INTERNAL_EXECUTION"},
            json={
                "consumer_id": self.consumer_id,
                "requirements": [invalid_execution],
            },
        )
        self.assertEqual(denied.status_code, 400)
        self.assertEqual(denied.headers["content-type"], "application/problem+json")
        self.assertEqual(denied.json()["code"], "INVALID_ARGUMENT")

    def test_batch_completion_returns_the_validated_public_json_contract(self):
        requirement = self.requirement.__dict__ | {
            "consumer_grade": "ALPHA",
            "feed": "BAR",
            "stale_policy": "BLOCK",
            "gap_policy": "BLOCK",
            "recovery": "SNAPSHOT_AND_REPLAY",
            "bar_revision_policy": "LATEST",
        }
        original = self.service.warmup_batch_completed_async
        completed = []

        async def instrumented(*args, **kwargs):
            response = await original(*args, **kwargs)
            completed.append(response)
            return response

        with patch.object(self.service, "warmup_batch_completed_async", instrumented):
            response = self.client.post(
                "/v2/market-data/warmup:batch",
                json={
                    "consumer_id": self.consumer_id,
                    "require_all": True,
                    "requirements": [requirement],
                },
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].media_type, "application/json")
        validated = BatchResponse.model_validate_json(response.content)
        self.assertEqual(
            validated.model_dump(mode="json", by_alias=True),
            response.json(),
        )

    def test_single_warmup_renders_inside_the_local_lease_like_the_batch(self):
        original = self.service.warmup_batch_completed_async
        completed = []

        async def instrumented(*args, **kwargs):
            response = await original(*args, **kwargs)
            completed.append(response)
            return response

        with patch.object(self.service, "warmup_batch_completed_async", instrumented):
            response = self.client.get(
                f"/v2/market-data/{self.binance.instrument_uid}/warmup",
                params=self.params(limit=2),
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(completed), 1)
        # The completion returned the rendered response, so rendering happened
        # while the local lease was still held.
        self.assertEqual(completed[0].media_type, "application/json")
        validated = WarmupResponse.model_validate_json(response.content)
        self.assertEqual(validated.count, 2)
        self.assertEqual(validated.model_dump(mode="json", by_alias=True), response.json())

    def test_large_response_rendering_leaves_the_event_loop_serving(self):
        router_module = importlib.import_module("qdl.api_v2.router")
        response = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/warmup",
            params=self.params(limit=2),
        )
        model = WarmupResponse.model_validate_json(response.content)

        def slow_build():
            time.sleep(0.3)  # stands in for seconds of Pydantic work on 5,000 rows
            return model

        async def scenario():
            ticks = 0
            done = asyncio.Event()

            async def ticker():
                nonlocal ticks
                while not done.is_set():
                    await asyncio.sleep(0.01)
                    ticks += 1

            task = asyncio.create_task(ticker())
            rendered = await router_module._json_off_loop(slow_build)
            done.set()
            await task
            return ticks, rendered

        ticks, rendered = asyncio.run(scenario())
        self.assertGreaterEqual(ticks, 15)
        self.assertEqual(
            rendered.body,
            JSONResponse(content=model.model_dump(mode="json", by_alias=True)).body,
        )

    def test_chunked_warmup_rendering_is_byte_identical_to_the_full_render(self):
        router_module = importlib.import_module("qdl.api_v2.router")
        response = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/warmup",
            params=self.params(limit=2),
        )
        self.assertEqual(response.status_code, 200, response.text)
        model = WarmupResponse.model_validate_json(response.content)
        # The served body is exactly what the chunked renderer produces.
        self.assertEqual(response.content, router_module._render_warmup_chunked(model))
        for rows in (601, 250, 0):  # across chunk boundaries, one exact chunk, empty
            sized = model.model_copy(update={"data": (model.data * 301)[:rows], "count": rows})
            self.assertEqual(
                router_module._render_warmup_chunked(sized),
                JSONResponse(content=sized.model_dump(mode="json", by_alias=True)).body,
            )

    def test_stale_and_unentitled_sources_return_stable_problem_details(self):
        stale_requirement = DataRequirement(
            **{**self.requirement.__dict__, "consumer_grade": ConsumerGrade.EXECUTION}
        )
        stale = MarketDataItem(
            **{
                **self.backend.latest(self.requirement).__dict__,
                "quality": QualityMetadata(
                    "STALE", 20_000, False, True, False, "alpha_crypto_primary_v1"
                ),
            }
        )
        self.backend.put_latest(stale_requirement, stale)
        response = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            headers={"X-QDL-Purpose": "INTERNAL_EXECUTION"},
            params=self.params(consumer_grade="EXECUTION"),
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["code"], "DATA_STALE")
        self.assertEqual(response.json()["quality_state"], "STALE")
        # KN-4 review: the quality the refusal was decided on travels with it.
        diagnostics = response.json()["diagnostics"]
        self.assertEqual(
            {key: diagnostics[key] for key in (
                "state", "freshness_ms", "execution_eligible", "gap_open", "complete",
                "provider_session_state", "watermark_offset", "source_id")},
            {"state": "STALE", "freshness_ms": 20_000, "execution_eligible": False,
             "gap_open": False, "complete": True, "provider_session_state": "NOT_APPLICABLE",
             "watermark_offset": stale.watermark_offset, "source_id": stale.source.source_id},
        )
        self.assertGreater(diagnostics["evaluated_at_ns"], 0)
        self.assertEqual(diagnostics["observed_at_ns"], stale.observed_at_ns)

        denied_service = V2QueryService(
            instruments=InstrumentQuery(self.registry),
            backend=self.backend,
            entitlements=EntitlementPolicy(()),
        )
        denied_client = TestClient(
            create_v2_app(denied_service, identity_service=self.identity),
            raise_server_exceptions=False,
        )
        denied_client.headers.update(self.headers())
        denied = denied_client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/warmup",
            params=self.params(limit=2),
        )
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.json()["code"], "SOURCE_NOT_ALLOWED")

    def test_execution_gap_is_not_misreported_as_non_authoritative(self):
        execution_requirement = DataRequirement(
            **{**self.requirement.__dict__, "consumer_grade": ConsumerGrade.EXECUTION}
        )
        current = self.backend.latest(self.requirement)
        self.backend.put_latest(
            execution_requirement,
            MarketDataItem(
                **{
                    **current.__dict__,
                    "quality": QualityMetadata(
                        "GAPPED", 10, True, False, False,
                        "alpha_crypto_primary_v1",
                    ),
                }
            ),
        )
        response = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            headers={"X-QDL-Purpose": "INTERNAL_EXECUTION"},
            params=self.params(consumer_grade="EXECUTION"),
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["code"], "OPEN_SEQUENCE_GAP")

    def test_market_closed_history_is_available_but_not_execution_eligible(self):
        current = self.backend.latest(self.requirement)
        self.backend.put_latest(
            self.requirement,
            MarketDataItem(
                **{
                    **current.__dict__,
                    "quality": QualityMetadata(
                        "MARKET_CLOSED", 86_400_000, False, True, False,
                        "alpha_crypto_primary_v1", ("MARKET_CLOSED",),
                    ),
                }
            ),
        )
        response = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            params=self.params(max_freshness_ms=500),
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["quality"]["state"], "MARKET_CLOSED")
        self.assertFalse(response.json()["data"]["quality"]["execution_eligible"])

        execution_requirement = DataRequirement(
            **{**self.requirement.__dict__, "consumer_grade": ConsumerGrade.EXECUTION}
        )
        self.backend.put_latest(
            execution_requirement, self.backend.latest(self.requirement)
        )
        blocked = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            headers=self.headers("INTERNAL_EXECUTION"),
            params=self.params(consumer_grade="EXECUTION", max_freshness_ms=500),
        )
        self.assertEqual(blocked.status_code, 503)
        self.assertEqual(blocked.json()["code"], "DATA_NOT_READY")
        self.assertEqual(blocked.json()["quality_state"], "MARKET_CLOSED")

    def test_single_query_preserves_manifest_freshness_and_final_bar_policy(self):
        current = self.backend.latest(self.requirement)
        self.backend.put_latest(
            self.requirement,
            MarketDataItem(
                **{
                    **current.__dict__,
                    "payload": {**current.payload, "is_final": False},
                    "bar_lifecycle": BarLifecycle.IN_PROGRESS,
                    "quality": QualityMetadata(
                        "STALE", 20_000, False, True, False,
                        "alpha_crypto_primary_v1",
                    ),
                }
            ),
        )
        allowed = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            params=self.params(
                stale_policy="OBSERVE",
                require_final_bars=False,
            ),
        )
        self.assertEqual(allowed.status_code, 200, allowed.text)
        blocked = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            params=self.params(
                stale_policy="OBSERVE",
                require_final_bars=True,
            ),
        )
        self.assertEqual(blocked.status_code, 503)
        self.assertEqual(blocked.json()["code"], "DATA_NOT_READY")

    def test_approved_reference_fallback_is_alpha_visible_but_execution_blocked(self):
        fallback_requirement = DataRequirement(
            instrument_uid=self.binance.instrument_uid,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="alpha_crypto_reference_v1",
            interval="1m",
            max_freshness_ms=10_000,
        )
        current = self.backend.latest(self.requirement)
        fallback = MarketDataItem(
            **{
                **current.__dict__,
                "source": SourceMetadata(
                    "OKX", "OKX_DIRECT", "OKX_DIRECT", "REFERENCE", False
                ),
                "quality": QualityMetadata(
                    "LIVE", 20, False, True, False,
                    "alpha_crypto_reference_v1", ("FALLBACK_ACTIVE",),
                ),
            }
        )
        self.backend.put_latest(fallback_requirement, fallback)
        alpha = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            params=self.params(source_policy_id="alpha_crypto_reference_v1"),
        )
        self.assertEqual(alpha.status_code, 200, alpha.text)
        self.assertEqual(alpha.json()["data"]["source"]["source_role"], "REFERENCE")
        self.assertIn("FALLBACK_ACTIVE", alpha.json()["data"]["quality"]["flags"])

        execution = self.client.get(
            f"/v2/market-data/{self.binance.instrument_uid}/snapshot",
            headers={"X-QDL-Purpose": "INTERNAL_EXECUTION"},
            params=self.params(
                source_policy_id="alpha_crypto_reference_v1",
                consumer_grade="EXECUTION",
            ),
        )
        self.assertEqual(execution.status_code, 503)
        self.assertEqual(execution.json()["code"], "SOURCE_NON_AUTHORITATIVE")

    def test_query_service_time_range_is_aligned_and_bounded_before_materialization(self):
        minute_ns = 60_000_000_000
        start_ns = minute_ns
        cases = (
            (start_ns + 10_001 * minute_ns, "public row bound"),
            (start_ns + 2 * minute_ns + 1, "not aligned"),
        )
        for end_ns, detail in cases:
            with self.subTest(detail=detail):
                requirement = DataRequirement(
                    instrument_uid=self.binance.instrument_uid,
                    feed=FeedType.BAR,
                    consumer_grade=ConsumerGrade.ALPHA,
                    source_policy_id="alpha_crypto_primary_v1",
                    interval="1m",
                    max_freshness_ms=10_000,
                    warmup=WarmupSpecification(
                        time_range=WarmupTimeRange(start_ns, end_ns)
                    ),
                )
                with self.assertRaises(QueryServiceError) as rejected:
                    self.service.warmup(
                        requirement,
                        purpose=AccessPurpose.INTERNAL_ALPHA,
                    )
                self.assertEqual(rejected.exception.problem.code.value, "INVALID_ARGUMENT")
                self.assertIn(detail, rejected.exception.problem.detail)


if __name__ == "__main__":
    unittest.main()

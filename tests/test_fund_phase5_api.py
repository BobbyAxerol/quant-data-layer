from __future__ import annotations

import importlib
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
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

    def test_sync_query_routes_use_the_existing_thread_boundary(self):
        calls = []
        original = router_module.asyncio.to_thread

        async def record(callable_, *args, **kwargs):
            calls.append(callable_.__name__)
            return await original(callable_, *args, **kwargs)

        requirement = {
            "instrument_uid": self.binance.instrument_uid,
            "feed": "BAR",
            "consumer_grade": "ALPHA",
            "source_policy_id": "alpha_crypto_primary_v1",
            "interval": "1m",
            "max_freshness_ms": 10_000,
        }
        with patch.object(router_module.asyncio, "to_thread", new=record):
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
        self.assertEqual(calls, ["snapshot", "status", "readiness"])

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

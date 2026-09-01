from __future__ import annotations

import asyncio
import time
import unittest
from types import SimpleNamespace

import grpc
from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.api_v2.models import MarketDataView
from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from qdl.ingestion.contracts import DeliveryPolicy, FeedType as IngestFeed, delivery_policy
from qdl.ingestion.queue import FeedQueue
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    ConsumerGrade,
    ContractMetadata,
    DataProduct,
    DataRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    InstrumentQuery,
    MarketDataItem,
    MemoryMarketDataBackend,
    QualityMetadata,
    SourceMetadata,
    V2QueryService,
)
from qdl.query.v2 import query_pb2
from qdl.security import (
    DataPlaneAccessError,
    DataPlaneIdentityService,
    DataPlanePermission,
    DataPlaneSecurityConfig,
)
from qdl.security.grpc import GrpcDataPlaneInterceptor
from qdl.stream import GrpcMarketDataService, create_grpc_server
from qdl.stream.grpc_service import requirement_from_proto
from qdl_sdk import (
    BarRevisionPolicy as SdkBarRevisionPolicy,
    DataRequirement as SdkRequirement,
    Feed,
    GapPolicy as SdkGapPolicy,
    Grade,
    RecoveryPolicy as SdkRecoveryPolicy,
    StalePolicy as SdkStalePolicy,
)
from tests.phase7_support import (
    TEST_KEY_ID,
    TEST_SECRET,
    auth_headers,
    make_identity,
    make_manifest,
    make_token,
)


def record() -> InstrumentRecord:
    identity = InstrumentIdentity.create(
        venue="BINANCE",
        market="USDM",
        product_type=ProductType.PERPETUAL,
        canonical_symbol="BTC-USDT",
    )
    return InstrumentRecord(
        identity=identity,
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


class Phase7Fixture:
    def __init__(self) -> None:
        self.record = record()
        self.consumer_id = "phase7.alpha"
        self.subject = "spiffe://qdl/paper/phase7-alpha"
        self.manifest = make_manifest(
            consumer_id=self.consumer_id,
            subject=self.subject,
            instrument_uid=self.record.instrument_uid,
        )
        self.identity = make_identity(self.manifest)
        self.requirement = DataRequirement(
            instrument_uid=self.record.instrument_uid,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="alpha_binance_v1",
            interval="1m",
            warmup_limit=0,
            max_freshness_ms=10_000,
        )
        registry = InstrumentRegistry()
        registry.register(self.record, [])
        backend = MemoryMarketDataBackend()
        now = time.time_ns()
        item = MarketDataItem(
            instrument_uid=self.record.instrument_uid,
            instrument_id=self.record.instrument_id,
            instrument_revision=1,
            feed=FeedType.BAR,
            observed_at_ns=now,
            payload={
                "open_time_ns": now - 60_000_000_000,
                "close_time_ns": now,
                "open": "60000.10",
                "high": "60100.20",
                "low": "59900.30",
                "close": "60050.40",
                "volume": "12.500",
                "volume_unit": "BASE_ASSET",
                "trade_count": 42,
                "origin": "VENUE_NATIVE",
                "is_final": True,
            },
            source=SourceMetadata(
                "BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True
            ),
            quality=QualityMetadata(
                "LIVE", 1, False, True, True, "alpha_binance_v1"
            ),
            contract=ContractMetadata(
                schema_digest="7" * 64,
                contract_version="2.0.0-beta.1",
                normalizer_version="phase7-test",
                adapter_version="binance-fixture-v1",
                instrument_catalog_revision=1,
                source_policy_revision=1,
                authority_revision=1,
                config_revision=1,
                correlation_id="phase7-contract-test",
            ),
            interval="1m",
            cursor="signed-phase7-cursor",
            snapshot_id="immutable-phase7-snapshot",
            watermark_offset=7,
            bar_lifecycle=BarLifecycle.FINAL,
        )
        backend.put_latest(self.requirement, item)
        self.service = V2QueryService(
            instruments=InstrumentQuery(registry),
            backend=backend,
            entitlements=EntitlementPolicy((EntitlementGrant(
                "BINANCE_DIRECT",
                "public-v1",
                frozenset({AccessPurpose.INTERNAL_ALPHA}),
                frozenset({DataProduct.CANONICAL_SNAPSHOT}),
                0,
            ),)),
        )

    def headers(self):
        return auth_headers(consumer_id=self.consumer_id, subject=self.subject)


class Phase7RestAndContractTests(unittest.TestCase):
    def setUp(self):
        self.fixture = Phase7Fixture()
        self.client = TestClient(
            create_v2_app(
                self.fixture.service,
                identity_service=self.fixture.identity,
            ),
            raise_server_exceptions=False,
        )

    def params(self):
        return {
            "feed": "BAR",
            "interval": "1m",
            "source_policy_id": "alpha_binance_v1",
            "consumer_grade": "ALPHA",
            "max_freshness_ms": 10_000,
        }

    def test_rest_is_application_authenticated_and_consumer_bound(self):
        route = f"/v2/market-data/{self.fixture.record.instrument_uid}/snapshot"
        self.assertEqual(self.client.get(route, params=self.params()).status_code, 401)
        wrong = self.fixture.headers() | {"X-QDL-Consumer-ID": "other-consumer"}
        denied = self.client.get(route, params=self.params(), headers=wrong)
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.json()["code"], "CONSUMER_MISMATCH")

        response = self.client.get(route, params=self.params(), headers=self.fixture.headers())
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()["data"]
        self.assertEqual(payload["payload"]["feed"], "BAR")
        self.assertEqual(payload["payload"]["close"]["coefficient"], "6005040")
        self.assertEqual(payload["payload"]["close"]["scale"], 2)
        self.assertEqual(payload["payload"]["lifecycle"], "FINAL")
        self.assertEqual(payload["contract"]["schema_digest"], "7" * 64)
        self.assertFalse(payload["quality"]["execution_eligible"])

    def test_typed_contract_rejects_provider_fallthrough_and_feed_mismatch(self):
        response = self.client.get(
            f"/v2/market-data/{self.fixture.record.instrument_uid}/snapshot",
            params=self.params(),
            headers=self.fixture.headers(),
        ).json()["data"]
        response["payload"]["provider_native_secret"] = "must-not-leak"
        with self.assertRaisesRegex(ValueError, "Extra inputs"):
            MarketDataView.model_validate(response)
        response["payload"].pop("provider_native_secret")
        response["feed"] = "TRADE"
        with self.assertRaises(ValueError):
            MarketDataView.model_validate(response)

    def test_every_public_feed_has_a_closed_discriminated_payload(self):
        decimal = {"coefficient": "1", "scale": 0, "source_text": "1"}
        level = {
            "side": "BID", "price": decimal, "quantity": decimal,
            "quantity_unit": "BASE_ASSET",
        }
        payloads = {
            "TRADE": {
                "native_trade_id": "trade-1", "price": decimal,
                "quantity": decimal, "quantity_unit": "BASE_ASSET",
                "aggressor_side": "BUY", "identity_kind": "NATIVE",
            },
            "QUOTE": {
                "bid_price": decimal, "bid_quantity": decimal,
                "ask_price": decimal, "ask_quantity": decimal,
                "quantity_unit": "BASE_ASSET",
            },
            "BAR": {
                "interval": "1m", "open_time_ns": 1, "close_time_ns": 2,
                "open": decimal, "high": decimal, "low": decimal,
                "close": decimal, "volume": decimal, "volume_unit": "BASE_ASSET",
                "lifecycle": "FINAL",
                "revision": 0, "origin": "VENUE_NATIVE",
            },
            "BOOK_SNAPSHOT": {
                "native_sequence": "1", "levels": [level], "depth": 1,
            },
            "BOOK_DELTA": {
                "native_sequence_start": "1", "native_sequence_end": "2",
                "snapshot_sequence": "0", "updates": [level],
            },
            "FUNDING_RATE": {"rate": decimal, "funding_time_ns": 1},
            "OPEN_INTEREST": {
                "quantity": decimal, "quantity_unit": "CONTRACT",
                "notional": decimal,
            },
            "MARK_INDEX_PRICE": {"mark_price": decimal, "index_price": decimal},
            "LONG_SHORT_RATIO": {
                "population": "GLOBAL_ACCOUNT", "sampling_interval": "1h",
                "long_value": decimal, "short_value": decimal,
                "long_short_ratio": decimal, "value_unit": "RATIO",
            },
            "TAKER_FLOW": {
                "sampling_interval": "1h", "buy_volume": decimal,
                "sell_volume": decimal, "buy_sell_ratio": decimal,
                "quantity_unit": "BASE_ASSET",
            },
            "BASIS": {
                "kind": "PROVIDER_NATIVE", "sampling_interval": "1h",
                "basis": decimal, "basis_unit": "PRICE",
            },
            "CONTRACT_METADATA": {
                "contract_kind": "PERPETUAL", "settlement_asset": "USDT",
                "contract_multiplier": decimal, "price_tick": decimal,
                "quantity_step": decimal,
            },
            "TICKER": {
                "last_price": decimal, "volume_24h": decimal,
                "volume_24h_unit": "BASE_ASSET",
            },
        }
        base = {
            "instrument_uid": self.fixture.record.instrument_uid,
            "instrument_id": self.fixture.record.instrument_id,
            "instrument_revision": 1,
            "observed_at_ns": 2,
            "revision": 0,
            "source": {
                "venue": "BINANCE", "provider": "BINANCE_DIRECT",
                "source_id": "BINANCE_DIRECT", "source_role": "PRIMARY",
                "authoritative": True,
            },
            "quality": {
                "state": "LIVE", "freshness_ms": 1, "gap_open": False,
                "complete": True, "execution_eligible": False,
                "policy_id": "alpha_binance_v1", "flags": [],
            },
            "contract": {
                "schema_digest": "7" * 64, "contract_version": "2.0.0-beta.1",
                "normalizer_version": "phase7", "adapter_version": "fixture-v1",
                "instrument_catalog_revision": 1, "source_policy_revision": 1,
                "authority_revision": 1, "config_revision": 1,
                "correlation_id": "phase7-all-feeds",
            },
        }
        for feed, payload in payloads.items():
            with self.subTest(feed=feed):
                result = MarketDataView.model_validate({
                    **base,
                    "feed": feed,
                    "interval": "1m" if feed == "BAR" else "1h" if feed in {"LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"} else None,
                    "payload": {"feed": feed, **payload},
                })
                self.assertEqual(result.feed.value, feed)

    def test_token_audience_environment_time_and_purpose_fail_closed(self):
        identity = self.fixture.identity
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(
                make_token(self.fixture.subject, audience="wrong-audience"),
                consumer_id=self.fixture.consumer_id,
            )
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(
                make_token(self.fixture.subject, environment="live"),
                consumer_id=self.fixture.consumer_id,
            )
        now = int(time.time())
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(
                make_token(
                    self.fixture.subject,
                    issued_at=now - 600,
                    expires_at=now - 300,
                ),
                consumer_id=self.fixture.consumer_id,
            )
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(
                make_token(self.fixture.subject, not_before=now + 60),
                consumer_id=self.fixture.consumer_id,
            )
        access = identity.authenticate(
            make_token(self.fixture.subject), consumer_id=self.fixture.consumer_id
        )
        with self.assertRaises(DataPlaneAccessError):
            access.require_purpose(AccessPurpose.INTERNAL_EXECUTION)
        with self.assertRaises(DataPlaneAccessError):
            identity.authenticate(
                make_token(self.fixture.subject, manifest_revision=2),
                consumer_id=self.fixture.consumer_id,
            )

        scoped = identity.authenticate(
            make_token(self.fixture.subject, roles=("auditor",)),
            consumer_id=self.fixture.consumer_id,
        )
        with self.assertRaises(DataPlaneAccessError):
            scoped.require_permission(DataPlanePermission.SNAPSHOT_READ)
        access.require_stream_buffer(access.manifest.quotas.max_buffer_events)
        with self.assertRaises(DataPlaneAccessError):
            access.require_stream_buffer(access.manifest.quotas.max_buffer_events + 1)

    def test_signing_key_is_bound_to_its_registered_workload_subject(self):
        other = make_manifest(
            consumer_id="phase7.other",
            subject="spiffe://qdl/paper/phase7-other",
            instrument_uid=self.fixture.record.instrument_uid,
        )
        identity = DataPlaneIdentityService(
            DataPlaneSecurityConfig(
                environment="paper",
                issuer="https://identity.qdl.test",
                audience="qdl-v2-beta",
                keys_by_id={TEST_KEY_ID: TEST_SECRET},
                algorithms=("HS256",),
                subjects_by_key_id={TEST_KEY_ID: self.fixture.subject},
            ),
            ConsumerManifestRegistry((self.fixture.manifest, other)),
        )
        self.assertEqual(
            identity.authenticate(
                make_token(self.fixture.subject),
                consumer_id=self.fixture.consumer_id,
            ).consumer_id,
            self.fixture.consumer_id,
        )
        with self.assertRaisesRegex(DataPlaneAccessError, "signing key"):
            identity.authenticate(
                make_token(other.subject),
                consumer_id=other.consumer_id,
            )

    def test_key_subject_bindings_must_cover_the_keyring_exactly(self):
        with self.assertRaisesRegex(ValueError, "cover exactly"):
            DataPlaneSecurityConfig(
                environment="paper",
                issuer="https://identity.qdl.test",
                audience="qdl-v2-beta",
                keys_by_id={TEST_KEY_ID: TEST_SECRET},
                algorithms=("HS256",),
                subjects_by_key_id={},
            )

    def test_manifest_rejects_unknown_permission_at_registration(self):
        from tests.phase7_support import manifest_mapping

        payload = manifest_mapping(
            consumer_id="phase7.invalid-permission",
            subject="spiffe://qdl/paper/invalid-permission",
            instrument_uid=self.fixture.record.instrument_uid,
        )
        payload["spec"]["permissions"].append("provider-admin:write")
        with self.assertRaisesRegex(ValueError, "unknown data-plane permission"):
            ConsumerManifestLoader.from_mapping(payload)

    def test_key_rotation_unknown_kid_and_error_redaction(self):
        rotated_secret = b"phase7-rotated-secret-material-32b"
        config = self.fixture.identity.config
        rotated_identity = type(self.fixture.identity)(
            type(config)(
                environment=config.environment,
                issuer=config.issuer,
                audience=config.audience,
                keys_by_id={
                    TEST_KEY_ID: TEST_SECRET,
                    "phase7-rotated": rotated_secret,
                },
                algorithms=config.algorithms,
                max_token_lifetime_seconds=config.max_token_lifetime_seconds,
            ),
            self.fixture.identity.manifests,
        )
        for token in (
            make_token(self.fixture.subject),
            make_token(
                self.fixture.subject,
                key_id="phase7-rotated",
                secret=rotated_secret,
            ),
        ):
            self.assertEqual(
                rotated_identity.authenticate(
                    token, consumer_id=self.fixture.consumer_id
                ).consumer_id,
                self.fixture.consumer_id,
            )

        untrusted = make_token(self.fixture.subject, key_id="unknown-key")
        response = TestClient(
            create_v2_app(self.fixture.service, identity_service=rotated_identity),
            raise_server_exceptions=False,
        ).get(
            f"/v2/market-data/{self.fixture.record.instrument_uid}/snapshot",
            params=self.params(),
            headers={
                "Authorization": f"Bearer {untrusted}",
                "X-QDL-Consumer-ID": self.fixture.consumer_id,
                "X-QDL-Purpose": "INTERNAL_ALPHA",
            },
        )
        self.assertEqual(response.status_code, 401)
        rendered = response.text
        self.assertNotIn(untrusted, rendered)
        self.assertNotIn("unknown-key", rendered)
        self.assertNotIn(TEST_SECRET.decode(), rendered)

    def test_malformed_manifest_revision_is_a_closed_auth_failure(self):
        route = f"/v2/market-data/{self.fixture.record.instrument_uid}/snapshot"
        headers = self.fixture.headers() | {
            "Authorization": "Bearer "
            + make_token(self.fixture.subject, manifest_revision="not-an-integer")
        }
        response = self.client.get(route, params=self.params(), headers=headers)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["code"], "UNAUTHENTICATED")

    def test_bar_delivery_policy_never_coalesces_final_or_revision(self):
        self.assertEqual(
            delivery_policy(IngestFeed.BAR, bar_lifecycle=BarLifecycle.IN_PROGRESS),
            DeliveryPolicy.LIFECYCLE_COALESCE,
        )
        for lifecycle in (
            BarLifecycle.FINAL,
            BarLifecycle.REVISED,
            BarLifecycle.CANCELLED,
        ):
            self.assertEqual(
                delivery_policy(IngestFeed.BAR, bar_lifecycle=lifecycle),
                DeliveryPolicy.LOSSLESS,
            )

    async def _bar_queue_lifecycle(self):
        queue = FeedQueue[str](capacity=4, policy=DeliveryPolicy.LOSSLESS)
        await queue.put(
            "BTC-USDT:1m:100",
            "in-progress-1",
            policy=DeliveryPolicy.LIFECYCLE_COALESCE,
        )
        await queue.put(
            "BTC-USDT:1m:100",
            "in-progress-2",
            policy=DeliveryPolicy.LIFECYCLE_COALESCE,
        )
        await queue.put(
            "BTC-USDT:1m:100",
            "final",
            policy=DeliveryPolicy.LOSSLESS,
        )
        values = (await queue.get(), await queue.get())
        return values, queue.stats()

    def test_bar_queue_coalesces_only_in_progress_and_preserves_final(self):
        values, stats = asyncio.run(self._bar_queue_lifecycle())
        self.assertEqual(values, ("in-progress-2", "final"))
        self.assertEqual(stats.enqueued, 2)
        self.assertEqual(stats.coalesced, 1)

    def test_sdk_requirement_uses_typed_proto_enums_and_legacy_only_fails(self):
        sdk = SdkRequirement(
            self.fixture.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            stale_policy=SdkStalePolicy.BLOCK,
            gap_policy=SdkGapPolicy.BLOCK,
            recovery=SdkRecoveryPolicy.SNAPSHOT_AND_REPLAY,
            bar_revision_policy=SdkBarRevisionPolicy.EMIT_REVISIONS,
        )
        message = sdk.to_proto()
        self.assertEqual(message.feed_type, query_pb2.FEED_TYPE_BAR)
        self.assertEqual(requirement_from_proto(message).feed, FeedType.BAR)
        with self.assertRaisesRegex(ValueError, "UNSPECIFIED"):
            requirement_from_proto(query_pb2.DataRequirement(
                instrument_uid=self.fixture.record.instrument_uid,
                feed="BAR",
                consumer_grade="ALPHA",
                source_policy_id="alpha_binance_v1",
            ))

    def test_sdk_requirement_preserves_event_recency_and_session_liveness(self):
        sdk = SdkRequirement(
            self.fixture.record.instrument_uid,
            Feed.TRADE,
            Grade.EXECUTION,
            "alpha_binance_v1",
            max_freshness_ms=3_000,
            event_recency_policy=SdkStalePolicy.OBSERVE,
            max_session_liveness_ms=45_000,
            stale_policy=SdkStalePolicy.BLOCK,
            gap_policy=SdkGapPolicy.BLOCK,
            recovery=SdkRecoveryPolicy.SNAPSHOT_AND_REPLAY,
            bar_revision_policy=SdkBarRevisionPolicy.LATEST,
        )
        message = sdk.to_proto()
        self.assertEqual(message.event_recency_policy, query_pb2.STALE_POLICY_OBSERVE)
        self.assertEqual(message.max_session_liveness_ms, 45_000)
        decoded = requirement_from_proto(message)
        self.assertEqual(decoded.event_recency_policy.value, "OBSERVE")
        self.assertEqual(decoded.max_session_liveness_ms, 45_000)


class Phase7GrpcIdentityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = Phase7Fixture()
        service = GrpcMarketDataService(
            gateway=None,
            query_service=self.fixture.service,
            snapshot_loader=None,
        )
        self.server = create_grpc_server(
            service,
            identity_service=self.fixture.identity,
        )
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{self.port}")
        self.call = self.channel.unary_unary(
            "/qdl.query.v2.MarketDataStreamService/GetFeedStatus",
            request_serializer=query_pb2.GetFeedStatusRequest.SerializeToString,
            response_deserializer=query_pb2.GetFeedStatusResponse.FromString,
        )
        requirement = SdkRequirement(
            self.fixture.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
        )
        self.request = query_pb2.GetFeedStatusRequest(
            consumer_id=self.fixture.consumer_id,
            requirement=requirement.to_proto(),
        )

    async def asyncTearDown(self):
        await self.channel.close()
        await self.server.stop(grace=0)

    def metadata(
        self,
        *,
        consumer_id: str | None = None,
        token: str | None = None,
        purpose: str = "INTERNAL_ALPHA",
    ):
        return (
            ("authorization", f"Bearer {token or make_token(self.fixture.subject)}"),
            ("x-qdl-consumer-id", consumer_id or self.fixture.consumer_id),
            ("x-qdl-purpose", purpose),
        )

    async def test_grpc_interceptor_matches_rest_identity_decision(self):
        response = await self.call(self.request, metadata=self.metadata())
        self.assertEqual(response.state, "LIVE")
        with self.assertRaises(grpc.aio.AioRpcError) as missing:
            await self.call(self.request)
        self.assertEqual(missing.exception.code(), grpc.StatusCode.UNAUTHENTICATED)
        with self.assertRaises(grpc.aio.AioRpcError) as mismatch:
            await self.call(self.request, metadata=self.metadata(consumer_id="other"))
        self.assertEqual(mismatch.exception.code(), grpc.StatusCode.PERMISSION_DENIED)

        for token in (
            make_token(self.fixture.subject, audience="wrong-audience"),
            make_token(self.fixture.subject, environment="live"),
            make_token(self.fixture.subject, manifest_revision=2),
        ):
            with self.assertRaises(grpc.aio.AioRpcError) as denied:
                await self.call(self.request, metadata=self.metadata(token=token))
            self.assertEqual(denied.exception.code(), grpc.StatusCode.UNAUTHENTICATED)

        with self.assertRaises(grpc.aio.AioRpcError) as purpose:
            await self.call(
                self.request,
                metadata=self.metadata(purpose="INTERNAL_EXECUTION"),
            )
        self.assertEqual(purpose.exception.code(), grpc.StatusCode.PERMISSION_DENIED)


class GrpcStreamQuotaAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def test_unary_stream_authenticates_once_for_many_responses(self):
        class Access:
            def require_purpose(self, _purpose):
                return None

        class Identity:
            def __init__(self):
                self.calls = 0

            def authenticate(self, *_args, **_kwargs):
                self.calls += 1
                return Access()

        class Context:
            async def abort(self, *_args):
                raise AssertionError("valid stream metadata must not abort")

        async def behavior(_request, _context):
            for index in range(100):
                yield index

        identity = Identity()
        interceptor = GrpcDataPlaneInterceptor(identity)
        handler = grpc.unary_stream_rpc_method_handler(behavior)

        async def continuation(_details):
            return handler

        details = SimpleNamespace(invocation_metadata=(
            ("authorization", "Bearer token"),
            ("x-qdl-consumer-id", "consumer"),
            ("x-qdl-purpose", "INTERNAL_EXECUTION"),
        ))
        wrapped = await interceptor.intercept_service(continuation, details)
        values = [
            value
            async for value in wrapped.unary_stream(None, Context())
        ]
        self.assertEqual(values, list(range(100)))
        self.assertEqual(identity.calls, 1)


if __name__ == "__main__":
    unittest.main()

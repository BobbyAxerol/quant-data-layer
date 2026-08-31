from __future__ import annotations

import asyncio
from dataclasses import replace
import json
import tempfile
import time
import unittest
from pathlib import Path

import grpc
import httpx

from qdl.api_v2 import create_v2_app
from qdl.canonical.market import canonicalize_binance_usdm_bar
from qdl.canonical.trade import TradeContext, canonical_event, raw_market_event
from qdl.common.v1 import common_pb2
from qdl.consumer import (
    ConsumerManifestLoader,
    ConsumerMigrationRegistry,
    ManifestShadowConsumer,
    MigrationState,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.pipeline import ShadowCanonicalPipeline
from qdl.projection import InMemoryProjectionTarget, MarketProjector
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    ConsumerGrade,
    ContractMetadata,
    CoverageStatus,
    DataProduct,
    DataRequirement as DomainRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    HistoryResult,
    InstrumentQuery,
    MarketDataItem,
    MemoryMarketDataBackend,
    QualityMetadata,
    SourceMetadata,
    V2QueryService,
)
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.stream import DurableStreamGateway, GrpcMarketDataService, GrpcSnapshot, create_grpc_server
from qdl.transport import Cursor, DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl_sdk import (
    AsyncDataLayerClient,
    Feed,
    Grade,
    DataRequirement as SdkRequirement,
    GrpcStreamTransport,
    RestQueryTransport,
    StaticBearerCredential,
)
from tests.phase7_support import make_identity, make_manifest, make_token


ROOT = Path(__file__).resolve().parents[1]


def _record(venue: str, market: str, native_symbol: str) -> InstrumentRecord:
    identity = InstrumentIdentity.create(
        venue=venue, market=market, product_type=ProductType.PERPETUAL,
        canonical_symbol="BTC-USDT",
    )
    return InstrumentRecord(
        identity=identity, metadata_revision=1, asset_class=AssetClass.DERIVATIVE,
        native_symbol=native_symbol, base_asset="BTC", quote_asset="USDT",
        settlement_asset="USDT", price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


def _envelope(record: InstrumentRecord, feed: FeedType, source_id: str):
    common = dict(
        schema_name=f"qdl.marketdata.{feed.value.lower()}", schema_major=2,
        event_id=b"e" * 16, instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id, instrument_revision=1,
        venue=record.identity.venue, market=record.identity.market,
        product_type="PERPETUAL", native_symbol=record.native_symbol,
        provider=f"{record.identity.venue}_DIRECT", source_id=source_id,
        source_role=1, lease_epoch=1, source_event_time_ns=time.time_ns(),
        received_at_ns=time.time_ns(), normalized_at_ns=time.time_ns(),
        published_at_ns=time.time_ns(), source_sequence="1", partition_sequence=1,
        normalizer_version="phase5-e2e", adapter_version="fixture-v1",
        config_revision=1,
    )
    if feed is FeedType.BAR:
        return market_data_pb2.EventEnvelope(**common, bar=market_data_pb2.Bar(
            interval="1m", open_time_ns=1, close_time_ns=2, is_final=True, revision=1,
            lifecycle=market_data_pb2.BAR_LIFECYCLE_REVISED,
            supersedes_event_id=b"previous",
        ))
    return market_data_pb2.EventEnvelope(**common, trade=market_data_pb2.Trade(
        native_trade_id="1", is_buyer_maker=False,
    ))


class _SnapshotLoader:
    def __init__(self, token: str):
        self.token = token

    def load(self, requirement, *, consumer_id):
        del requirement, consumer_id
        return GrpcSnapshot("request", "snapshot", self.token, time.time_ns(), 0, ())


def _contract(correlation_id: str) -> ContractMetadata:
    return ContractMetadata(
        "5" * 64, "2.0.0-beta.1", "phase7-test", "fixture-v1",
        1, 1, 1, 1, correlation_id,
    )


def _payload(feed: FeedType, now: int) -> dict:
    if feed is FeedType.BAR:
        return {
            "open_time_ns": now - 60_000_000_000,
            "close_time_ns": now,
            "open": "60000", "high": "60100", "low": "59900",
            "close": "60050", "volume": "10", "volume_unit": "BASE_ASSET",
            "trade_count": 5,
            "origin": "VENUE_NATIVE", "is_final": True,
        }
    return {
        "native_trade_id": "trade-1",
        "price": "60050",
        "quantity": "0.01",
        "quantity_unit": "BASE_ASSET",
        "aggressor_side": "BUY",
        "identity_kind": "NATIVE",
        "is_block_trade": False,
        "is_buyer_maker": False,
    }


class Phase5EndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def _observe_manifest(self, manifest_name: str, record: InstrumentRecord, feed: FeedType):
        manifest = ConsumerManifestLoader.load(ROOT / "consumers/shadow" / manifest_name)
        registry = ConsumerMigrationRegistry()
        registry.register(manifest, reason="Phase 5 test registration")
        migration = registry.transition(
            manifest.consumer_id, MigrationState.SHADOW, owner=manifest.owner,
            reason="isolated E2E shadow observation",
        )
        source_id = f"{record.identity.venue}_DIRECT"
        envelope = _envelope(record, feed, source_id)
        stream = f"md.canonical.v2.{feed.value.lower()}"
        partition = f"{record.instrument_uid}/{feed.value.lower()}/{source_id}"
        event = DurableEvent(
            stream, partition, bytes(envelope.event_id), envelope.SerializeToString(), time.time_ns()
        )

        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "stream.sqlite3", min_free_disk_bytes=0,
            ))
            handoff = GapFreeHandoff(
                spool, SignedHandoffCursorCodec({"phase5": b"p" * 32}, active_key_id="phase5")
            )
            token = handoff.issue(
                consumer_id=manifest.consumer_id, snapshot_id="snapshot",
                snapshot_watermark=Cursor(stream, partition, 0), ttl_seconds=3600,
            ).token
            gateway = DurableStreamGateway(handoff=handoff, sink=spool)
            instruments = InstrumentRegistry()
            instruments.register(record, [])
            backend = MemoryMarketDataBackend()
            domain = manifest.requirements[0]
            quality = QualityMetadata(
                "LIVE", 1, False, True, True, domain.source_policy_id
            )
            now = time.time_ns()
            item = MarketDataItem(
                record.instrument_uid, record.instrument_id, 1, feed, now,
                _payload(feed, now),
                SourceMetadata(record.identity.venue, source_id, source_id, "PRIMARY", True),
                quality, _contract("phase5-manifest-e2e"),
                interval=domain.interval, cursor=token, snapshot_id="snapshot",
                watermark_offset=0,
                bar_lifecycle=(BarLifecycle.FINAL if feed is FeedType.BAR else None),
            )
            backend.put_latest(domain, item)
            if domain.warmup_limit:
                interval_ns = 60_000_000_000
                history = tuple(
                    replace(
                        item,
                        observed_at_ns=now - (domain.warmup_limit - index - 1) * interval_ns,
                        payload={
                            **item.payload,
                            "open_time_ns": now
                            - (domain.warmup_limit - index) * interval_ns,
                            "close_time_ns": now
                            - (domain.warmup_limit - index - 1) * interval_ns,
                        },
                    )
                    for index in range(domain.warmup_limit)
                )
                backend.put_history(domain, HistoryResult(
                    history,
                    CoverageStatus.FULL,
                    "snapshot",
                    token,
                    0,
                    time.time_ns(),
                ))
            service = V2QueryService(
                instruments=InstrumentQuery(instruments), backend=backend,
                entitlements=EntitlementPolicy((EntitlementGrant(
                    source_id, "public-v1",
                    frozenset({AccessPurpose.INTERNAL_ALPHA, AccessPurpose.INTERNAL_EXECUTION}),
                    frozenset({DataProduct.CANONICAL_SNAPSHOT, DataProduct.CANONICAL_HISTORY}),
                    0,
                ),)),
            )
            identity = make_identity(manifest)
            credential = StaticBearerCredential(make_token(manifest.subject))
            http_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_v2_app(
                    service, identity_service=identity
                )),
                base_url="http://phase5-shadow",
            )
            grpc_server = create_grpc_server(GrpcMarketDataService(
                gateway=gateway, query_service=service, snapshot_loader=_SnapshotLoader(token),
            ), identity_service=identity)
            port = grpc_server.add_insecure_port("127.0.0.1:0")
            await grpc_server.start()
            client = AsyncDataLayerClient(
                query_transport=RestQueryTransport(
                    "http://phase5-shadow", client=http_client,
                    credential_provider=credential,
                ),
                stream_transport=GrpcStreamTransport(
                    f"127.0.0.1:{port}", allow_insecure_loopback=True,
                    credential_provider=credential,
                ),
                consumer_id=manifest.consumer_id,
            )
            consumer = ManifestShadowConsumer(
                manifest=manifest, migration=migration, client=client,
            )
            task = asyncio.create_task(consumer.observe_once())
            for _ in range(100):
                if gateway.subscriber_count == 1:
                    break
                await asyncio.sleep(0.005)
            self.assertEqual(gateway.subscriber_count, 1)
            await gateway.publish(event)
            observation = await asyncio.wait_for(task, timeout=2)
            self.assertEqual(observation.consumer_id, manifest.consumer_id)
            self.assertEqual(observation.instrument_uid, record.instrument_uid)
            self.assertEqual(observation.feed, feed.value)
            await client.close()
            await http_client.aclose()
            await grpc_server.stop(grace=0)
            spool.close()

    async def test_reference_alpha_and_execution_consumers_use_v2_without_venue_access(self):
        await self._observe_manifest(
            "alpha-okx-reference.yaml", _record("OKX", "SWAP", "BTC-USDT-SWAP"),
            FeedType.BAR,
        )
        await self._observe_manifest(
            "trading-system-binance-execution.yaml",
            _record("BINANCE", "USDM", "BTCUSDT"), FeedType.TRADE,
        )

    async def test_provider_fixture_reaches_v1_projection_and_v2_stream_without_divergence(self):
        fixture = json.loads((ROOT / "tests/fixtures/phase2/binance_usdm_bar.json").read_text())
        context = TradeContext(**fixture["context"])
        raw = fixture["raw"]
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "full-e2e.sqlite3", min_free_disk_bytes=0,
            ))
            raw_event = raw_market_event(raw, context=context, feed_type="bar", accepted_at_ns=1)
            _, result = ShadowCanonicalPipeline(
                spool, consumer_id="phase5-full-e2e",
                canonicalizer=lambda event: canonical_event(
                    canonicalize_binance_usdm_bar(json.loads(event.payload), context),
                    accepted_at_ns=2, raw_event=event,
                ),
            ).accept(raw_event)
            stored = spool.read(
                stream="md.canonical.v2.bar", partition_key=result.cursor.partition_key,
            )[0]
            target = InMemoryProjectionTarget()
            self.assertTrue(MarketProjector(
                target,
                raw_resolver=lambda stream, event_id: (
                    found.event.payload
                    if (found := spool.find_event(stream=stream, event_id=event_id)) else None
                ),
            ).project(stored))
            legacy = json.loads(next(
                value for key, value in target.latest.items() if ":legacy:kline:1m:" in key
            ))

            codec = SignedHandoffCursorCodec({"phase5": b"e" * 32}, active_key_id="phase5")
            handoff = GapFreeHandoff(spool, codec)
            token = handoff.issue(
                consumer_id="alpha-binance-e2e", snapshot_id="snapshot",
                snapshot_watermark=Cursor(
                    "md.canonical.v2.bar", result.cursor.partition_key, 0
                ), ttl_seconds=3600,
            ).token
            gateway = DurableStreamGateway(handoff=handoff, sink=spool)
            # The fixture predates canonical UUIDv5 identity standardization; preserve
            # its exact identity through this compatibility E2E path.
            record = InstrumentRecord(
                identity=InstrumentIdentity(
                    context.instrument_uid, context.instrument_id, "BINANCE", "USDM",
                    ProductType.PERPETUAL, "BTC-USDT",
                ), metadata_revision=context.instrument_revision,
                asset_class=AssetClass.DERIVATIVE, native_symbol="BTCUSDT",
                base_asset="BTC", quote_asset="USDT", settlement_asset="USDT",
                price_tick=CanonicalDecimal.from_text("0.1"),
                quantity_step=CanonicalDecimal.from_text("0.001"),
                contract_multiplier=CanonicalDecimal.from_text("1"),
                session_calendar_id="CRYPTO_24X7",
            )
            registry = InstrumentRegistry()
            registry.register(record, [])
            requirement = DomainRequirement(
                context.instrument_uid, FeedType.BAR, ConsumerGrade.ALPHA,
                "alpha_binance_v1", interval="1m", warmup_limit=1,
            )
            canonical = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
            quality = QualityMetadata("LIVE", 1, False, True, True, "alpha_binance_v1")
            item = MarketDataItem(
                context.instrument_uid, context.instrument_id, context.instrument_revision,
                FeedType.BAR, canonical.source_event_time_ns,
                {
                    "open_time_ns": canonical.bar.open_time_ns,
                    "close_time_ns": canonical.bar.close_time_ns,
                    "open": canonical.bar.open.source_text,
                    "high": canonical.bar.high.source_text,
                    "low": canonical.bar.low.source_text,
                    "close": canonical.bar.close.source_text,
                    "volume": canonical.bar.volume.source_text,
                    "volume_unit": common_pb2.QuantityUnit.Name(
                        canonical.bar.volume_unit
                    ).removeprefix("QUANTITY_UNIT_"),
                    "trade_count": canonical.bar.trade_count,
                    "origin": "VENUE_NATIVE",
                    "is_final": canonical.bar.is_final,
                },
                SourceMetadata("BINANCE", "BINANCE_DIRECT", context.source_id, "PRIMARY", True),
                quality, _contract("phase5-provider-e2e"),
                interval="1m", cursor=token, snapshot_id="snapshot",
                watermark_offset=0,
                bar_lifecycle=BarLifecycle.FINAL,
            )
            backend = MemoryMarketDataBackend()
            backend.put_latest(requirement, item)
            backend.put_history(requirement, HistoryResult(
                (item,), CoverageStatus.FULL, "snapshot", token, 0, time.time_ns()
            ))
            service = V2QueryService(
                instruments=InstrumentQuery(registry), backend=backend,
                entitlements=EntitlementPolicy((EntitlementGrant(
                    context.source_id, "public-v1", frozenset({AccessPurpose.INTERNAL_ALPHA}),
                    frozenset({DataProduct.CANONICAL_HISTORY, DataProduct.CANONICAL_SNAPSHOT}), 0,
                ),)),
            )
            manifest = make_manifest(
                consumer_id="alpha-binance-e2e",
                subject="spiffe://qdl/paper/alpha-binance-e2e",
                instrument_uid=context.instrument_uid,
                source_policy_id="alpha_binance_v1",
            )
            identity = make_identity(manifest)
            credential = StaticBearerCredential(make_token(manifest.subject))
            http_client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_v2_app(
                    service, identity_service=identity
                )),
                base_url="http://phase5-full-e2e",
            )
            server = create_grpc_server(GrpcMarketDataService(
                gateway=gateway, query_service=service, snapshot_loader=_SnapshotLoader(token),
            ), identity_service=identity)
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            client = AsyncDataLayerClient(
                query_transport=RestQueryTransport(
                    "http://phase5-full-e2e", client=http_client,
                    credential_provider=credential,
                ),
                stream_transport=GrpcStreamTransport(
                    f"127.0.0.1:{port}", allow_insecure_loopback=True,
                    credential_provider=credential,
                ),
                consumer_id="alpha-binance-e2e",
            )
            sdk_requirement = SdkRequirement(
                context.instrument_uid, Feed.BAR, Grade.ALPHA,
                "alpha_binance_v1",
                interval="1m", warmup_limit=1,
            )
            async with client.warmup_then_stream(sdk_requirement) as session:
                delivered = await asyncio.wait_for(session.__anext__(), timeout=2)
                while not hasattr(delivered, "event"):
                    delivered = await asyncio.wait_for(session.__anext__(), timeout=2)
                self.assertEqual(delivered.event.bar.close.source_text, legacy["k"]["c"])
                self.assertEqual(delivered.event.bar.is_final, legacy["k"]["x"])
            await client.close()
            await http_client.aclose()
            await server.stop(grace=0)
            spool.close()


class Phase5ProjectionParityTests(unittest.TestCase):
    def test_provider_bar_fixture_has_exact_canonical_and_v1_semantic_parity(self):
        fixture = json.loads((ROOT / "tests/fixtures/phase2/binance_usdm_bar.json").read_text())
        context = TradeContext(**fixture["context"])
        raw = fixture["raw"]
        with tempfile.TemporaryDirectory() as directory:
            spool = SQLiteDurableSpool(SpoolConfig(
                path=Path(directory) / "e2e.sqlite3", min_free_disk_bytes=0,
            ))
            raw_event = raw_market_event(raw, context=context, feed_type="bar", accepted_at_ns=1)
            pipeline = ShadowCanonicalPipeline(
                spool, consumer_id="phase5-e2e-canonicalizer",
                canonicalizer=lambda event: canonical_event(
                    canonicalize_binance_usdm_bar(json.loads(event.payload), context),
                    accepted_at_ns=2, raw_event=event,
                ),
            )
            _, canonical_result = pipeline.accept(raw_event)
            stored = spool.read(
                stream="md.canonical.v2.bar",
                partition_key=canonical_result.cursor.partition_key,
            )[0]
            target = InMemoryProjectionTarget()
            projector = MarketProjector(
                target,
                raw_resolver=lambda stream, event_id: (
                    row.event.payload
                    if (row := spool.find_event(stream=stream, event_id=event_id)) else None
                ),
            )
            self.assertTrue(projector.project(stored))
            canonical = market_data_pb2.EventEnvelope.FromString(stored.event.payload).bar
            legacy = json.loads(next(
                value for key, value in target.latest.items() if ":legacy:kline:1m:" in key
            ))["k"]
            self.assertEqual(canonical.open.source_text, legacy["o"])
            self.assertEqual(canonical.high.source_text, legacy["h"])
            self.assertEqual(canonical.low.source_text, legacy["l"])
            self.assertEqual(canonical.close.source_text, legacy["c"])
            self.assertEqual(canonical.volume.source_text, legacy["v"])
            self.assertEqual(canonical.is_final, legacy["x"])
            self.assertEqual(spool.stats().records, 2)
            spool.close()


if __name__ == "__main__":
    unittest.main()

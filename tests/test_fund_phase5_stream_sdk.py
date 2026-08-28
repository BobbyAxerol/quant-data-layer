from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

import grpc

from qdl.common.v1 import common_pb2
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
    BarLifecycle,
    ConsumerGrade,
    ContractMetadata,
    DataProduct,
    DataRequirement as DomainRequirement,
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
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.runtime import GatewayFenced
from qdl.consumer import UsageTelemetry
from qdl.stream import (
    DurableStreamGateway,
    GrpcMarketDataService,
    GrpcSnapshot,
    SlowConsumer,
    create_grpc_server,
)
from qdl.transport import Cursor, DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl_sdk import (
    AsyncDataLayerClient,
    BarRevisionPolicy as SdkBarRevisionPolicy,
    DataLayerClientV2,
    DataRequirement,
    Feed,
    GapPolicy as SdkGapPolicy,
    Grade,
    GrpcStreamTransport,
    MemoryCursorStore,
    RecoveryPolicy as SdkRecoveryPolicy,
    StalePolicy as SdkStalePolicy,
    StaticBearerCredential,
)
from qdl_sdk.cursor import CursorCheckpoint, FileCursorStore
from qdl_sdk.errors import ContinuityError, CursorExpiredError, DataLayerError, SlowConsumerError
from qdl_sdk.models import ControlEvent, StreamEvent
from qdl_sdk.v1_facade import V1CompatibilityFacade
from qdl.consumer import ConsumerManifestLoader
from tests.phase7_support import make_identity, make_token, manifest_mapping


STREAM = "md.canonical.v2.bar"
BOOK_STREAM = "md.canonical.v2.book_snapshot"


def instrument() -> InstrumentRecord:
    identity = InstrumentIdentity.create(
        venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
        canonical_symbol="BTC-USDT",
    )
    return InstrumentRecord(
        identity=identity, metadata_revision=1, asset_class=AssetClass.DERIVATIVE,
        native_symbol="BTCUSDT", base_asset="BTC", quote_asset="USDT",
        settlement_asset="USDT", price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )


def envelope(record: InstrumentRecord, index: int, *, revision: int = 0) -> market_data_pb2.EventEnvelope:
    bar = market_data_pb2.Bar(
        interval="1m",
        open_time_ns=1_000_000_000,
        close_time_ns=61_000_000_000,
        is_final=True,
        revision=revision,
        lifecycle=(
            market_data_pb2.BAR_LIFECYCLE_REVISED
            if revision
            else market_data_pb2.BAR_LIFECYCLE_FINAL
        ),
    )
    if revision:
        bar.supersedes_event_id = b"previous"
    return market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.bar",
        schema_major=2,
        event_id=index.to_bytes(16, "big"),
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=1,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        provider="BINANCE_DIRECT",
        source_id="BINANCE_DIRECT",
        source_role=1,
        lease_epoch=1,
        source_event_time_ns=1_000_000_000 + index,
        received_at_ns=1_000_000_100 + index,
        normalized_at_ns=1_000_000_200 + index,
        published_at_ns=1_000_000_300 + index,
        source_sequence=str(index),
        partition_sequence=index,
        normalizer_version="phase5-test",
        adapter_version="fixture-v1",
        config_revision=1,
        bar=bar,
    )


def durable(record: InstrumentRecord, index: int, *, revision: int = 0) -> DurableEvent:
    message = envelope(record, index, revision=revision)
    return DurableEvent(
        STREAM,
        f"{record.instrument_uid}/bar/BINANCE_DIRECT",
        index.to_bytes(16, "big"),
        message.SerializeToString(),
        1_000_000_000 + index,
    )


def book_envelope(
    record: InstrumentRecord, index: int, *, snapshot: bool,
) -> market_data_pb2.EventEnvelope:
    result = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.book",
        schema_major=2,
        event_id=index.to_bytes(16, "big"),
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=1,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        provider="BINANCE_DIRECT",
        source_id="BINANCE_DIRECT",
        source_role=1,
        lease_epoch=1,
        source_event_time_ns=1_000_000_000 + index,
        received_at_ns=1_000_000_100 + index,
        normalized_at_ns=1_000_000_200 + index,
        published_at_ns=1_000_000_300 + index,
        source_sequence=str(index),
        partition_sequence=index,
        normalizer_version="phase5-test",
        adapter_version="fixture-v1",
        config_revision=1,
    )
    level = market_data_pb2.BookLevel(
        side=common_pb2.BOOK_SIDE_BID,
        price=common_pb2.DecimalValue(mantissa=100, scale=0, source_text="100"),
        quantity=common_pb2.DecimalValue(mantissa=1, scale=0, source_text="1"),
        quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
    )
    if snapshot:
        result.book_snapshot.CopyFrom(market_data_pb2.OrderBookSnapshot(
            native_sequence=str(index), levels=[level], depth=1,
            book_generation=1, sequence_verified=True,
        ))
    else:
        result.book_delta.CopyFrom(market_data_pb2.OrderBookDelta(
            native_sequence_start=str(index), native_sequence_end=str(index),
            snapshot_sequence="1", updates=[level], book_generation=1,
            sequence_verified=True,
        ))
    return result


def durable_book(record: InstrumentRecord, index: int, *, snapshot: bool) -> DurableEvent:
    message = book_envelope(record, index, snapshot=snapshot)
    return DurableEvent(
        BOOK_STREAM,
        f"{record.instrument_uid}/book_snapshot/BINANCE_DIRECT",
        bytes(message.event_id),
        message.SerializeToString(),
        1_000_000_000 + index,
    )


class FakeQueryTransport:
    def __init__(self, token: str, *, watermark: int = 0):
        self.token = token
        self.watermark = watermark
        self.calls = 0

    def row(self, requirement):
        decimal = {"coefficient": "1", "scale": 0, "source_text": "1"}
        return {
            "instrument_uid": requirement.instrument_uid,
            "instrument_id": "binance:usdm:perpetual:BTC-USDT",
            "instrument_revision": 1,
            "feed": requirement.feed.value,
            "interval": requirement.interval,
            "observed_at_ns": 61_000_000_000,
            "revision": 0,
            "payload": {
                "feed": "BAR",
                "interval": requirement.interval,
                "open_time_ns": 1_000_000_000,
                "close_time_ns": 61_000_000_000,
                "open": decimal,
                "high": decimal,
                "low": decimal,
                "close": decimal,
                "volume": decimal,
                "volume_unit": "BASE_ASSET",
                "trade_count": 1,
                "lifecycle": "FINAL",
                "revision": 0,
                "origin": "VENUE_NATIVE",
            },
            "source": {
                "venue": "BINANCE",
                "provider": "BINANCE_DIRECT",
                "source_id": "BINANCE_DIRECT",
                "source_role": "PRIMARY",
                "authoritative": True,
            },
            "quality": {
                "state": "LIVE",
                "gap_open": False,
                "execution_eligible": True,
                "complete": True,
                "freshness_ms": 1,
                "policy_id": requirement.source_policy_id,
                "flags": [],
            },
            "contract": {
                "schema_digest": "5" * 64,
                "contract_version": "2.0.0-beta.1",
                "normalizer_version": "phase7-test",
                "adapter_version": "fixture-v1",
                "instrument_catalog_revision": 1,
                "source_policy_revision": 1,
                "authority_revision": 1,
                "config_revision": 1,
                "correlation_id": "phase5-sdk-fixture",
            },
            "snapshot_id": "snapshot",
            "cursor": self.token,
            "watermark_offset": self.watermark,
        }

    async def warmup(self, requirement, *, consumer_id):
        self.calls += 1
        return {
            "schema": "qdl.marketdata.warmup.v2",
            "request_id": "request",
            "snapshot_id": "snapshot",
            "data_as_of_ns": 61_000_000_000,
            "stream_cursor": self.token,
            "watermark_offset": self.watermark,
            "coverage": "FULL",
            "count": 1,
            "data": [self.row(requirement)],
        }

    async def snapshot(self, requirement, *, consumer_id):
        self.calls += 1
        return {
            "request_id": "request",
            "data": self.row(requirement),
        }

    async def close(self):
        return None


class SnapshotLoader:
    def __init__(self, record, token):
        self.record = record
        self.token = token

    def load(self, requirement, *, consumer_id):
        return GrpcSnapshot(
            "request", "snapshot", self.token, 1_000_000_000, 0,
            (envelope(self.record, 1),),
        )


class ScriptedIterator:
    def __init__(self, values):
        self.values = list(values)
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.values:
            raise StopAsyncIteration
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value

    async def aclose(self):
        self.close_calls += 1


class ScriptedStreamTransport:
    def __init__(self, scripts):
        self.scripts = list(scripts)
        self.tokens = []
        self.iterators = []

    def subscribe(self, requirement, **kwargs):
        del requirement
        self.tokens.append(kwargs["cursor_token"])
        iterator = ScriptedIterator(self.scripts.pop(0))
        self.iterators.append(iterator)
        return iterator

    async def close(self):
        return None


class Phase5StreamSdkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.record = instrument()
        self.partition = f"{self.record.instrument_uid}/bar/BINANCE_DIRECT"
        self.spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(self.temp.name) / "spool.sqlite3", min_free_disk_bytes=0,
        ))
        self.codec = SignedHandoffCursorCodec(
            {"phase5": b"s" * 32}, active_key_id="phase5"
        )
        self.handoff = GapFreeHandoff(self.spool, self.codec)
        self.gateway = DurableStreamGateway(
            handoff=self.handoff,
            sink=self.spool,
            max_buffer_events=2,
        )
        self.token = self.handoff.issue(
            consumer_id="alpha-shadow",
            snapshot_id="snapshot-0",
            snapshot_watermark=Cursor(STREAM, self.partition, 0),
            ttl_seconds=3600,
        ).token
        self.consumer_id = "alpha-shadow"
        self.subject = "spiffe://qdl/paper/alpha-shadow"
        manifest_payload = manifest_mapping(
            consumer_id=self.consumer_id,
            subject=self.subject,
            instrument_uid=self.record.instrument_uid,
            source_policy_id="alpha_binance_v1",
        )
        manifest_payload["spec"]["requirements"].append({
            **manifest_payload["spec"]["requirements"][0],
            "feed": "TRADE",
            "interval": None,
        })
        self.manifest = ConsumerManifestLoader.from_mapping(manifest_payload)
        self.identity = make_identity(self.manifest)
        self.credential = StaticBearerCredential(make_token(self.subject))

    async def asyncTearDown(self):
        self.spool.close()
        self.temp.cleanup()

    async def test_durable_first_duplicate_replay_and_resume(self):
        subscription = await self.gateway.open(
            consumer_id="alpha-shadow", stream=STREAM, partition_key=self.partition,
            token=self.token,
        )
        first = await self.gateway.publish(durable(self.record, 1))
        duplicate = await self.gateway.publish(durable(self.record, 1))
        self.assertIsNotNone(first)
        self.assertIsNone(duplicate)
        live = await subscription.next_live()
        self.assertEqual(live.stored.cursor.offset, 1)
        await subscription.close()

        restarted = await self.gateway.open(
            consumer_id="alpha-shadow", stream=STREAM, partition_key=self.partition,
            token=live.resume_token,
        )
        self.assertEqual(restarted.initial, ())
        await self.gateway.publish(durable(self.record, 2))
        self.assertEqual((await restarted.next_live()).stored.cursor.offset, 2)
        await restarted.close()

    async def test_grpc_filters_interleaved_book_feeds_before_buffering(self):
        partition = f"{self.record.instrument_uid}/book_snapshot/BINANCE_DIRECT"
        token = self.handoff.issue(
            consumer_id=self.consumer_id,
            snapshot_id="book-snapshot-0",
            snapshot_watermark=Cursor(BOOK_STREAM, partition, 0),
            ttl_seconds=3600,
        ).token
        manifest_payload = manifest_mapping(
            consumer_id=self.consumer_id,
            subject=self.subject,
            instrument_uid=self.record.instrument_uid,
            feed="BOOK_SNAPSHOT",
            interval=None,
            source_policy_id="alpha_binance_v1",
        )
        manifest_payload["spec"]["requirements"][0].update({
            "require_final_bars": False,
            "bar_revision_policy": "LATEST",
        })
        identity = make_identity(ConsumerManifestLoader.from_mapping(manifest_payload))
        await self.gateway.publish(durable_book(self.record, 1, snapshot=False))
        await self.gateway.publish(durable_book(self.record, 2, snapshot=True))
        server = create_grpc_server(
            GrpcMarketDataService(
                gateway=self.gateway,
                query_service=None,
                snapshot_loader=SnapshotLoader(self.record, token),
            ),
            identity_service=identity,
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}", allow_insecure_loopback=True,
            credential_provider=StaticBearerCredential(make_token(self.subject)),
        )
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BOOK_SNAPSHOT, Grade.ALPHA,
            "alpha_binance_v1", warmup_limit=1,
        )
        events = transport.subscribe(
            requirement, consumer_id=self.consumer_id, cursor_token=token,
            max_buffer_events=1,
        ).__aiter__()
        try:
            self.assertEqual((await events.__anext__()).code, "REPLAYING")
            first = await events.__anext__()
            self.assertEqual(first.logical_offset, 2)
            self.assertEqual(first.event.WhichOneof("payload"), "book_snapshot")
            self.assertEqual((await events.__anext__()).code, "LIVE")
            await self.gateway.publish(durable_book(self.record, 3, snapshot=False))
            await self.gateway.publish(durable_book(self.record, 4, snapshot=True))
            second = await events.__anext__()
            self.assertEqual(second.logical_offset, 4)
            self.assertEqual(second.event.WhichOneof("payload"), "book_snapshot")
        finally:
            await events.aclose()
            await transport.close()
            await server.stop(grace=0)

    async def test_slow_consumer_is_disconnected_without_durable_loss_or_peer_block(self):
        slow = await self.gateway.open(
            consumer_id="alpha-shadow", stream=STREAM, partition_key=self.partition,
            token=self.token, max_buffer_events=1,
        )
        peer = await self.gateway.open(
            consumer_id="peer-shadow", stream=STREAM, partition_key=self.partition,
            token=self.handoff.issue(
                consumer_id="peer-shadow", snapshot_id="snapshot-0",
                snapshot_watermark=Cursor(STREAM, self.partition, 0), ttl_seconds=3600,
            ).token,
        )
        await self.gateway.publish(durable(self.record, 1))
        await self.gateway.publish(durable(self.record, 2))
        with self.assertRaises(SlowConsumer):
            await slow.next_live()
        self.assertEqual((await peer.next_live()).stored.cursor.offset, 1)
        self.assertEqual((await peer.next_live()).stored.cursor.offset, 2)
        self.assertEqual(self.spool.high_watermark(STREAM, self.partition), 2)
        await slow.close()
        await peer.close()

    async def test_grpc_multitarget_fails_over_from_standby_without_gap(self):
        class StandbyAuthority:
            current_epoch = None

            def __init__(self):
                self.attempts = 0

            def assert_active(self, expected_epoch=None):
                del expected_epoch
                self.attempts += 1
                raise GatewayFenced("test standby")

        standby_authority = StandbyAuthority()
        standby_gateway = DurableStreamGateway(
            handoff=self.handoff,
            sink=self.spool,
            max_buffer_events=2,
            authority=standby_authority,
        )
        standby_server = create_grpc_server(
            GrpcMarketDataService(
                gateway=standby_gateway,
                query_service=None,
                snapshot_loader=SnapshotLoader(self.record, self.token),
            ),
            identity_service=self.identity,
        )
        active_server = create_grpc_server(
            GrpcMarketDataService(
                gateway=self.gateway,
                query_service=None,
                snapshot_loader=SnapshotLoader(self.record, self.token),
            ),
            identity_service=self.identity,
        )
        standby_port = standby_server.add_insecure_port("127.0.0.1:0")
        active_port = active_server.add_insecure_port("127.0.0.1:0")
        await standby_server.start()
        await active_server.start()
        await self.gateway.publish(durable(self.record, 1))
        transport = GrpcStreamTransport(
            (f"127.0.0.1:{standby_port}", f"127.0.0.1:{active_port}"),
            allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            warmup_limit=1,
        )
        events = transport.subscribe(
            requirement,
            consumer_id="alpha-shadow",
            cursor_token=self.token,
            max_buffer_events=2,
        ).__aiter__()
        try:
            self.assertEqual((await events.__anext__()).code, "REPLAYING")
            first = await events.__anext__()
            self.assertEqual(first.logical_offset, 1)
            self.assertEqual((await events.__anext__()).code, "LIVE")
            await self.gateway.publish(durable(self.record, 2))
            second = await events.__anext__()
            self.assertEqual(second.logical_offset, 2)
            self.assertEqual(
                transport.target,
                f"127.0.0.1:{active_port}",
            )
            self.assertEqual(standby_authority.attempts, 1)
        finally:
            await transport.close()
            await standby_server.stop(grace=0)
            await active_server.stop(grace=0)

    async def test_grpc_multitarget_fails_after_each_standby_once(self):
        class StandbyAuthority:
            current_epoch = None

            def __init__(self):
                self.attempts = 0

            def assert_active(self, expected_epoch=None):
                del expected_epoch
                self.attempts += 1
                raise GatewayFenced("test standby")

        authorities = (StandbyAuthority(), StandbyAuthority())
        servers = []
        ports = []
        for authority in authorities:
            gateway = DurableStreamGateway(
                handoff=self.handoff,
                sink=self.spool,
                max_buffer_events=2,
                authority=authority,
            )
            server = create_grpc_server(
                GrpcMarketDataService(
                    gateway=gateway,
                    query_service=None,
                    snapshot_loader=SnapshotLoader(self.record, self.token),
                ),
                identity_service=self.identity,
            )
            ports.append(server.add_insecure_port("127.0.0.1:0"))
            servers.append(server)
            await server.start()
        transport = GrpcStreamTransport(
            tuple(f"127.0.0.1:{port}" for port in ports),
            allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            warmup_limit=1,
        )
        events = transport.subscribe(
            requirement,
            consumer_id="alpha-shadow",
            cursor_token=self.token,
            max_buffer_events=2,
        ).__aiter__()
        try:
            with self.assertRaises(DataLayerError) as raised:
                await events.__anext__()
            self.assertEqual(raised.exception.code, "DEPENDENCY_UNAVAILABLE")
            self.assertTrue(raised.exception.retryable)
            self.assertEqual(tuple(item.attempts for item in authorities), (1, 1))
        finally:
            await transport.close()
            for server in servers:
                await server.stop(grace=0)

    async def test_grpc_emits_backpressure_control_before_slow_consumer_disconnect(self):
        grpc_service = GrpcMarketDataService(
            gateway=self.gateway,
            query_service=None,
            snapshot_loader=SnapshotLoader(self.record, self.token),
        )
        server = create_grpc_server(grpc_service, identity_service=self.identity)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}", allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        events = transport.subscribe(
            requirement, consumer_id="alpha-shadow", cursor_token=self.token,
            max_buffer_events=1,
        ).__aiter__()
        try:
            self.assertEqual((await events.__anext__()).code, "REPLAYING")
            self.assertEqual((await events.__anext__()).code, "LIVE")
            await self.gateway.publish(durable(self.record, 1))
            await self.gateway.publish(durable(self.record, 2))
            # With async durable I/O the transport may have accepted the first
            # valid event before backpressure is observed. The invariant is
            # that RATE_LIMITED is explicit and every committed event remains
            # replayable, not that scheduler timing hides the first event.
            response = await events.__anext__()
            if not hasattr(response, "code"):
                response = await events.__anext__()
            self.assertEqual(response.code, "RATE_LIMITED")
            with self.assertRaises(SlowConsumerError):
                await events.__anext__()
        finally:
            await transport.close()
            await server.stop(grace=0)

    async def test_grpc_stream_close_has_no_cross_context_finalizer_error(self):
        service = GrpcMarketDataService(
            gateway=self.gateway,
            query_service=None,
            snapshot_loader=SnapshotLoader(self.record, self.token),
        )
        server = create_grpc_server(service, identity_service=self.identity)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA,
            "alpha_binance_v1", interval="1m", warmup_limit=1,
        )
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        unhandled = []
        loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
        events = transport.subscribe(
            requirement, consumer_id="alpha-shadow", cursor_token=self.token,
            max_buffer_events=1,
        ).__aiter__()
        try:
            self.assertEqual((await events.__anext__()).code, "REPLAYING")
            await events.aclose()
            await asyncio.sleep(0.05)
            self.assertFalse(
                [
                    item for item in unhandled
                    if "different Context" in str(item.get("exception", ""))
                ]
            )
        finally:
            loop.set_exception_handler(previous_handler)
            await transport.close()
            await server.stop(grace=0)

    async def test_grpc_iterator_close_releases_server_subscription_capacity(self):
        gateway = DurableStreamGateway(
            handoff=self.handoff,
            sink=self.spool,
            max_subscribers=10,
            max_buffer_events=2,
        )
        server = create_grpc_server(
            GrpcMarketDataService(
                gateway=gateway,
                query_service=None,
                snapshot_loader=SnapshotLoader(self.record, self.token),
            ),
            identity_service=self.identity,
        )
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            warmup_limit=1,
        )
        streams = []
        replacement = None
        try:
            for _ in range(10):
                stream = transport.subscribe(
                    requirement,
                    consumer_id="alpha-shadow",
                    cursor_token=self.token,
                    max_buffer_events=1,
                ).__aiter__()
                self.assertEqual((await stream.__anext__()).code, "REPLAYING")
                streams.append(stream)
            await streams[0].aclose()
            await asyncio.sleep(0.05)
            replacement = transport.subscribe(
                requirement,
                consumer_id="alpha-shadow",
                cursor_token=self.token,
                max_buffer_events=1,
            ).__aiter__()
            self.assertEqual((await replacement.__anext__()).code, "REPLAYING")
        finally:
            if replacement is not None:
                await replacement.aclose()
            for stream in streams:
                await stream.aclose()
            await transport.close()
            await server.stop(grace=0)

    async def test_real_grpc_sdk_handoff_ack_restart_and_bar_revisions(self):
        registry = InstrumentRegistry()
        registry.register(self.record, [])
        backend = MemoryMarketDataBackend()
        domain_requirement = DomainRequirement(
            self.record.instrument_uid, FeedType.BAR, ConsumerGrade.ALPHA,
            "alpha_binance_v1", interval="1m", warmup_limit=1,
        )
        quality = QualityMetadata("LIVE", 1, False, True, True, "alpha_binance_v1")
        now = time.time_ns()
        backend.put_latest(domain_requirement, MarketDataItem(
            self.record.instrument_uid, self.record.instrument_id, 1, FeedType.BAR,
            now, {
                "open_time_ns": now - 60_000_000_000,
                "close_time_ns": now,
                "open": "60000", "high": "60100", "low": "59900",
                "close": "60050", "volume": "10", "volume_unit": "BASE_ASSET",
                "trade_count": 5,
                "origin": "VENUE_NATIVE", "is_final": True,
            },
            SourceMetadata("BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True),
            quality,
            ContractMetadata(
                "5" * 64, "2.0.0-beta.1", "phase7-test", "fixture-v1",
                1, 1, 1, 1, "phase5-stream-sdk",
            ),
            interval="1m", cursor=self.token, snapshot_id="snapshot-0",
            bar_lifecycle=BarLifecycle.FINAL,
        ))
        service = V2QueryService(
            instruments=InstrumentQuery(registry), backend=backend,
            entitlements=EntitlementPolicy((EntitlementGrant(
                "BINANCE_DIRECT", "public-v1",
                frozenset({AccessPurpose.INTERNAL_ALPHA}),
                frozenset({DataProduct.CANONICAL_HISTORY, DataProduct.CANONICAL_SNAPSHOT}),
                0,
            ),)),
        )
        grpc_service = GrpcMarketDataService(
            gateway=self.gateway,
            query_service=service,
            snapshot_loader=SnapshotLoader(self.record, self.token),
        )
        server = create_grpc_server(grpc_service, identity_service=self.identity)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}", allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        query = FakeQueryTransport(self.token)
        cursor_store = MemoryCursorStore()
        sdk_requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        client = AsyncDataLayerClient(
            query_transport=query,
            stream_transport=transport,
            consumer_id="alpha-shadow",
            cursor_store=cursor_store,
            max_buffer_events=2,
        )
        try:
            async with client.warmup_then_stream(sdk_requirement) as session:
                await self.gateway.publish(durable(self.record, 1, revision=0))
                controls = []
                first = await session.__anext__()
                while isinstance(first, ControlEvent):
                    controls.append(first.code)
                    first = await session.__anext__()
                self.assertIsInstance(first, StreamEvent)
                self.assertIn("REPLAYING", controls)
                session.acknowledge(first)
                await self.gateway.publish(durable(self.record, 2, revision=1))
                revised = await session.__anext__()
                while isinstance(revised, ControlEvent):
                    controls.append(revised.code)
                    revised = await session.__anext__()
                self.assertEqual(revised.event.bar.revision, 1)
                self.assertIn("LIVE", controls)
                session.acknowledge(revised)
            self.assertEqual(next(iter(cursor_store._items.values())).offset, 2)

            async with client.warmup_then_stream(
                sdk_requirement,
                resume_restored_state=True,
            ) as restarted:
                await self.gateway.publish(durable(self.record, 3))
                resumed = await restarted.__anext__()
                while isinstance(resumed, ControlEvent):
                    resumed = await restarted.__anext__()
                self.assertEqual(resumed.logical_offset, 3)
        finally:
            await client.close()
            await server.stop(grace=0)

    async def test_file_cursor_is_atomic_and_v1_facade_preserves_delegation(self):
        path = Path(self.temp.name) / "state/cursors.json"
        store = FileCursorStore(path)
        from qdl_sdk.cursor import CursorCheckpoint

        store.save("a", CursorCheckpoint("signed", 7))
        self.assertEqual(store.load("a").offset, 7)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaisesRegex(ValueError, "backwards"):
            store.save("a", CursorCheckpoint("older", 6))
        store.replace("a", CursorCheckpoint("new-snapshot", 2))
        self.assertEqual(store.load("a"), CursorCheckpoint("new-snapshot", 2))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

        class Legacy:
            def latest_trade(self, provider, symbol, **kwargs):
                return {"provider": provider, "symbol": symbol, **kwargs}
            def warmup_ohlcv(self, provider, symbol, **kwargs):
                return {"rows": kwargs["limit"]}

        facade = V1CompatibilityFacade(Legacy())
        self.assertEqual(facade.latest_trade("binance", "BTCUSDT")["symbol"], "BTCUSDT")
        self.assertEqual(facade.warmup_ohlcv("binance", "BTCUSDT", limit=10)["rows"], 10)

    async def test_fresh_snapshot_does_not_replay_from_unrestored_old_checkpoint(self):
        store = MemoryCursorStore()
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        query = FakeQueryTransport("fresh-token", watermark=5)
        stream = ScriptedStreamTransport((
            (StreamEvent(6, "token-6", envelope(self.record, 6)),),
        ))
        telemetry = UsageTelemetry()
        client = AsyncDataLayerClient(
            query_transport=query, stream_transport=stream,
            consumer_id="alpha-shadow", cursor_store=store, telemetry=telemetry,
        )
        key = client._cursor_key(requirement)
        from qdl_sdk.cursor import CursorCheckpoint
        store.save(key, CursorCheckpoint("old-generation-token", 200))

        async with client.warmup_then_stream(requirement) as session:
            event = await session.__anext__()
            session.acknowledge(event)
        self.assertEqual(stream.tokens, ["fresh-token"])
        self.assertEqual(store.load(key), CursorCheckpoint("token-6", 6))
        with self.assertRaisesRegex(ValueError, "backwards"):
            store.save(key, CursorCheckpoint("regression", 5))
        contracts = {item["contract"] for item in telemetry.snapshot()}
        self.assertEqual(
            contracts, {"/v2/market-data/warmup", "grpc:Subscribe"}
        )

    async def test_filtered_durable_offsets_are_strictly_monotonic_not_contiguous(self):
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        stream = ScriptedStreamTransport((
            (
                StreamEvent(7, "token-7", envelope(self.record, 7)),
                StreamEvent(12, "token-12", envelope(self.record, 12)),
            ),
        ))
        store = MemoryCursorStore()
        client = AsyncDataLayerClient(
            query_transport=FakeQueryTransport("filtered-token", watermark=5),
            stream_transport=stream,
            consumer_id="alpha-shadow",
            cursor_store=store,
        )

        async with client.warmup_then_stream(requirement) as session:
            first = await session.__anext__()
            second = await session.__anext__()
            self.assertEqual((first.logical_offset, second.logical_offset), (7, 12))
            session.acknowledge(first)
            session.acknowledge(second)

        self.assertEqual(
            store.load(client._cursor_key(requirement)),
            CursorCheckpoint("token-12", 12),
        )

    async def test_filtered_durable_offsets_still_reject_duplicates_or_rewinds(self):
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        stream = ScriptedStreamTransport((
            (
                StreamEvent(7, "token-7", envelope(self.record, 7)),
                StreamEvent(7, "token-7-repeat", envelope(self.record, 7)),
            ),
        ))
        client = AsyncDataLayerClient(
            query_transport=FakeQueryTransport("filtered-token", watermark=5),
            stream_transport=stream,
            consumer_id="alpha-shadow",
        )

        async with client.warmup_then_stream(requirement) as session:
            self.assertEqual((await session.__anext__()).logical_offset, 7)
            with self.assertRaisesRegex(ContinuityError, "non-monotonic"):
                await session.__anext__()

    async def test_fresh_snapshot_reconnect_before_first_ack_keeps_new_generation(self):
        store = MemoryCursorStore()
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        query = FakeQueryTransport("fresh-token", watermark=5)
        stream = ScriptedStreamTransport((
            (DataLayerError("DEPENDENCY_UNAVAILABLE", "retry", retryable=True),),
            (StreamEvent(6, "token-6", envelope(self.record, 6)),),
        ))
        client = AsyncDataLayerClient(
            query_transport=query, stream_transport=stream,
            consumer_id="alpha-shadow", cursor_store=store,
            max_reconnect_attempts=1,
        )
        key = client._cursor_key(requirement)
        from qdl_sdk.cursor import CursorCheckpoint
        store.save(key, CursorCheckpoint("old-generation-token", 200))

        async with client.warmup_then_stream(requirement) as session:
            reconnected = await session.__anext__()
            self.assertEqual(reconnected.code, "RECONNECTED")
            event = await session.__anext__()
            session.acknowledge(event)

        self.assertEqual(stream.tokens, ["fresh-token", "fresh-token"])
        self.assertEqual(store.load(key), CursorCheckpoint("token-6", 6))

    async def test_cursor_expiration_rebuilds_snapshot_and_transient_error_reconnects(self):
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA, "alpha_binance_v1",
            interval="1m", warmup_limit=1,
        )
        query = FakeQueryTransport("snapshot-token", watermark=0)
        stream = ScriptedStreamTransport((
            (CursorExpiredError("CURSOR_EXPIRED", "expired"),),
            (
                StreamEvent(1, "token-1", envelope(self.record, 1)),
                DataLayerError("DEPENDENCY_UNAVAILABLE", "reset", retryable=True),
            ),
            (StreamEvent(2, "token-2", envelope(self.record, 2)),),
        ))
        store = MemoryCursorStore()
        client = AsyncDataLayerClient(
            query_transport=query, stream_transport=stream,
            consumer_id="alpha-shadow", cursor_store=store,
            max_reconnect_attempts=2,
        )
        key = client._cursor_key(requirement)
        from qdl_sdk.cursor import CursorCheckpoint
        store.save(key, CursorCheckpoint("old-generation-token", 200))
        async with client.warmup_then_stream(requirement) as session:
            replaced = await session.__anext__()
            self.assertEqual(replaced.code, "SNAPSHOT_REPLACED")
            first = await session.__anext__()
            session.acknowledge(first)
            reconnected = await session.__anext__()
            self.assertEqual(reconnected.code, "RECONNECTED")
            second = await session.__anext__()
            self.assertEqual(second.logical_offset, 2)
        self.assertEqual(query.calls, 2)
        self.assertEqual(
            stream.tokens,
            ["snapshot-token", "snapshot-token", "token-1"],
        )
        self.assertEqual(
            [iterator.close_calls for iterator in stream.iterators],
            [1, 1, 1],
        )
        self.assertEqual(store.load(key), CursorCheckpoint("token-1", 1))
        await session.aclose()
        self.assertEqual(
            [iterator.close_calls for iterator in stream.iterators],
            [1, 1, 1],
        )

    async def test_warmup_applies_realtime_quality_only_to_tail_watermark(self):
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.EXECUTION,
            "execution_binance_v1", interval="1m", warmup_limit=2,
            max_freshness_ms=500,
        )
        query = FakeQueryTransport("snapshot-token")
        original = query.warmup

        async def historical_context(*args, **kwargs):
            payload = await original(*args, **kwargs)
            old = {**payload["data"][0]}
            old["quality"] = {
                **payload["data"][0]["quality"],
                "state": "STALE",
                "freshness_ms": 86_400_000,
                "execution_eligible": False,
            }
            latest = json.loads(json.dumps(payload["data"][0]))
            latest["observed_at_ns"] += 60_000_000_000
            latest["payload"]["open_time_ns"] += 60_000_000_000
            latest["payload"]["close_time_ns"] += 60_000_000_000
            payload["count"] = 2
            payload["data_as_of_ns"] = latest["payload"]["close_time_ns"]
            payload["data"] = [old, latest]
            return payload

        query.warmup = historical_context
        client = AsyncDataLayerClient(
            query_transport=query,
            stream_transport=ScriptedStreamTransport(()),
            consumer_id="trading-system-shadow",
        )
        response = await client.warmup(requirement)
        self.assertEqual(response.count, 2)
        self.assertEqual(response.data[0].quality.state, "STALE")
        self.assertTrue(response.data[-1].quality.execution_eligible)

    async def test_sdk_rejects_semantically_invalid_success_response(self):
        requirement = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.EXECUTION,
            "execution_binance_v1",
            interval="1m", warmup_limit=1,
        )
        query = FakeQueryTransport("snapshot-token")
        original = query.warmup

        async def stale(*args, **kwargs):
            payload = await original(*args, **kwargs)
            payload["data"][0]["quality"]["state"] = "STALE"
            payload["data"][0]["quality"]["execution_eligible"] = False
            return payload

        query.warmup = stale
        client = AsyncDataLayerClient(
            query_transport=query,
            stream_transport=ScriptedStreamTransport(()),
            consumer_id="trading-system-shadow",
        )
        with self.assertRaisesRegex(DataLayerError, "STALE"):
            async with client.warmup_then_stream(
                requirement
            ):
                pass

    async def test_market_closed_is_readable_for_alpha_but_execution_fails_closed(self):
        alpha = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.ALPHA,
            "alpha_binance_v1", interval="1m", warmup_limit=1,
            max_freshness_ms=500,
        )
        query = FakeQueryTransport(self.token)
        original = query.warmup

        async def market_closed(*args, **kwargs):
            payload = await original(*args, **kwargs)
            payload["data"][0]["quality"].update({
                "state": "MARKET_CLOSED",
                "freshness_ms": 86_400_000,
                "execution_eligible": False,
                "flags": ["MARKET_CLOSED"],
            })
            return payload

        query.warmup = market_closed
        alpha_client = AsyncDataLayerClient(
            query_transport=query,
            stream_transport=ScriptedStreamTransport(()),
            consumer_id="alpha-shadow",
        )
        response = await alpha_client.warmup(alpha)
        self.assertEqual(response.data[0].quality.state, "MARKET_CLOSED")
        self.assertFalse(response.data[0].quality.execution_eligible)

        execution = DataRequirement(
            self.record.instrument_uid, Feed.BAR, Grade.EXECUTION,
            "alpha_binance_v1", interval="1m", warmup_limit=1,
            max_freshness_ms=500,
        )
        with self.assertRaises(DataLayerError) as raised:
            await alpha_client.warmup(execution)
        self.assertEqual(raised.exception.code, "SOURCE_NON_AUTHORITATIVE")

    async def test_public_query_wrappers_preserve_all_requirement_policies(self):
        requirement = DataRequirement(
            self.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            warmup_limit=1,
            max_freshness_ms=500,
            require_full_coverage=False,
            require_final_bars=False,
            stale_policy=SdkStalePolicy.OBSERVE,
            gap_policy=SdkGapPolicy.OBSERVE,
            recovery=SdkRecoveryPolicy.FRESH_SNAPSHOT,
            bar_revision_policy=SdkBarRevisionPolicy.EMIT_REVISIONS,
        )
        self.assertEqual(requirement.stale_policy, SdkStalePolicy.OBSERVE)
        self.assertEqual(requirement.query_params()["recovery"], "FRESH_SNAPSHOT")
        query = FakeQueryTransport(self.token)
        client = AsyncDataLayerClient(
            query_transport=query,
            stream_transport=ScriptedStreamTransport(()),
            consumer_id="alpha-shadow",
        )
        self.assertEqual((await client.warmup(requirement)).count, 1)
        self.assertEqual((await client.snapshot(requirement)).data.feed.value, "BAR")
        facade = DataLayerClientV2(client)
        sync_snapshot = await asyncio.to_thread(facade.snapshot, requirement)
        self.assertEqual(sync_snapshot.data.instrument_uid, self.record.instrument_uid)

        with self.assertRaisesRegex(TypeError, "stale_policy"):
            DataRequirement(
                self.record.instrument_uid, Feed.TRADE, Grade.ALPHA,
                "alpha_binance_v1",
                stale_policy="UNKNOWN",
            )

    async def test_signed_legacy_cursor_maps_to_cursor_expired_after_generation_change(self):
        generation_handoff = GapFreeHandoff(
            self.spool,
            SignedHandoffCursorCodec(
                {"phase5": b"s" * 32},
                active_key_id="phase5",
                generation_id="cache-generation-new",
            ),
        )
        generation_gateway = DurableStreamGateway(
            handoff=generation_handoff,
            sink=self.spool,
            max_buffer_events=2,
        )
        service = GrpcMarketDataService(
            gateway=generation_gateway,
            query_service=None,
            snapshot_loader=SnapshotLoader(self.record, self.token),
        )
        server = create_grpc_server(service, identity_service=self.identity)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}",
            allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        requirement = DataRequirement(
            self.record.instrument_uid,
            Feed.BAR,
            Grade.ALPHA,
            "alpha_binance_v1",
            interval="1m",
            warmup_limit=1,
        )
        events = transport.subscribe(
            requirement,
            consumer_id="alpha-shadow",
            cursor_token=self.token,
            max_buffer_events=1,
        ).__aiter__()
        try:
            with self.assertRaises(CursorExpiredError) as raised:
                await events.__anext__()
            self.assertEqual(raised.exception.code, "CURSOR_EXPIRED")
            self.assertFalse(raised.exception.retryable)
        finally:
            await transport.close()
            await server.stop(grace=0)

    async def test_signed_cursor_scope_mismatch_fails_closed_without_retry(self):
        service = GrpcMarketDataService(
            gateway=self.gateway,
            query_service=None,
            snapshot_loader=SnapshotLoader(self.record, self.token),
        )
        server = create_grpc_server(service, identity_service=self.identity)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        transport = GrpcStreamTransport(
            f"127.0.0.1:{port}", allow_insecure_loopback=True,
            credential_provider=self.credential,
        )
        wrong_requirement = DataRequirement(
            self.record.instrument_uid, Feed.TRADE, Grade.ALPHA,
            "alpha_binance_v1"
        )
        events = transport.subscribe(
            wrong_requirement,
            consumer_id="alpha-shadow",
            cursor_token=self.token,
            max_buffer_events=1,
        ).__aiter__()
        try:
            with self.assertRaises(DataLayerError) as raised:
                await events.__anext__()
            self.assertEqual(raised.exception.code, "CURSOR_INVALID")
            self.assertFalse(raised.exception.retryable)
        finally:
            await transport.close()
            await server.stop(grace=0)


if __name__ == "__main__":
    unittest.main()

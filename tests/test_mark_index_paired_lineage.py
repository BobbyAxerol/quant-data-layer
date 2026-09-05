from __future__ import annotations

import asyncio
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx
from fastapi import FastAPI

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.stable import (
    InMemoryStableProjectionTarget,
    StableCompatibilityProjector,
)
from qdl.query import FeedType
from qdl.raw.capture import capture_exact_frame
from qdl.runtime.mark_index_lineage import (
    DERIVED_MARK_INDEX_COMPONENT_V1,
    validate_derived_mark_index_component,
    validate_single_raw_lineage,
)
from qdl.runtime.stable_ingest import (
    StableHttpCanonicalSink,
    install_stable_canonical_ingest,
)
from qdl.runtime.stable_projector import (
    LocalStableCanonicalSink,
    StableProjectorEngine,
)
from qdl.replay import GapFreeHandoff, SignedHandoffCursorCodec
from qdl.stream import DurableStreamGateway
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig
from qdl.transport.kafka_projector import KafkaProjectorRecord


_STREAM = "md.canonical.test.v2"
_SECRET = b"paired-mark-index-test-secret-32b"


def _pair_identity(mark, index) -> tuple[bytes, bytes]:
    capture = hashlib.sha256()
    capture.update(b"qdl-mark-index-capture-v1")
    capture.update(bytes(mark.capture_id))
    capture.update(bytes(index.capture_id))
    payload = hashlib.sha256()
    payload.update(b"qdl-mark-index-payload-v1")
    payload.update(bytes(mark.raw_frame_sha256))
    payload.update(bytes(index.raw_frame_sha256))
    return capture.digest()[:16], payload.digest()


def _raw(
    *,
    provider: str,
    venue: str,
    native_symbol: str,
    native_channel: str,
    received_at_ns: int,
) -> object:
    return capture_exact_frame(
        provider=provider,
        venue=venue,
        market="SWAP" if venue == "OKX" else "USDM",
        product_type="PERPETUAL",
        native_symbol=native_symbol,
        native_channel=native_channel,
        subscription_id=f"paired-{native_channel}",
        source_session_id="paired-mark-index-session",
        connection_generation=7,
        lease_epoch=3,
        authority_revision=11,
        partition_plan_epoch=5,
        received_at_ns=received_at_ns,
        raw_frame_bytes=(
            b'{"channel":"' + native_channel.encode() + b'","symbol":"'
            + native_symbol.encode() + b'"}'
        ),
        adapter_version="paired-test/1",
        config_revision=9,
        instrument_catalog_revision=4,
        correlation_id=f"paired-{native_channel}-{received_at_ns}",
        test_provenance=True,
    )


def _pair(*, venue: str = "OKX"):
    if venue == "OKX":
        provider = "OKX_DIRECT"
        native_symbol = "SOL-USDT-SWAP"
        mark = _raw(
            provider=provider,
            venue=venue,
            native_symbol=native_symbol,
            native_channel="mark-price",
            received_at_ns=1_700_000_000_000_000_000,
        )
        index = _raw(
            provider=provider,
            venue=venue,
            native_symbol="SOL-USDT",
            native_channel="index-tickers",
            received_at_ns=1_700_000_000_100_000_000,
        )
    else:
        provider = "BINANCE_DIRECT"
        native_symbol = "SOLUSDT"
        mark = index = _raw(
            provider=provider,
            venue=venue,
            native_symbol=native_symbol,
            native_channel="markPrice@1s",
            received_at_ns=1_700_000_000_000_000_000,
        )
    raw_capture_id, raw_payload_hash = _pair_identity(mark, index)
    envelope = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.v2",
        schema_major=2,
        schema_minor=0,
        event_id=hashlib.sha256(raw_capture_id + b"event").digest()[:16],
        instrument_uid=f"{venue}/PERPETUAL/SOL-USDT",
        instrument_id=f"{venue}.PERPETUAL.SOL-USDT",
        instrument_revision=4,
        venue=venue,
        market="SWAP" if venue == "OKX" else "USDM",
        product_type="PERPETUAL",
        native_symbol=native_symbol,
        provider=provider,
        source_id=f"{venue.lower()}-sol-mark-index",
        source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        lease_epoch=3,
        source_event_time_ns=1_700_000_000_000_000_000,
        received_at_ns=min(mark.received_at_ns, index.received_at_ns),
        normalized_at_ns=max(mark.received_at_ns, index.received_at_ns),
        published_at_ns=max(mark.received_at_ns, index.received_at_ns),
        source_sequence=(
            f"1700000000000:1700000000000:{mark.received_at_ns}:"
            f"{index.received_at_ns}:{bytes(mark.capture_id).hex()}:"
            f"{bytes(index.capture_id).hex()}"
        ),
        partition_sequence=1,
        normalizer_version="paired-test-core/1",
        adapter_version="paired-test/1",
        raw_capture_id=raw_capture_id,
        raw_payload_hash=raw_payload_hash,
        correlation_id=raw_capture_id.hex(),
        config_revision=9,
        source_session_id="paired-mark-index-session",
        connection_generation=7,
        authority_revision=11,
        partition_plan_epoch=5,
    )
    envelope.mark_index_price.mark_price.mantissa = 101
    envelope.mark_index_price.mark_price.source_text = "101"
    envelope.mark_index_price.index_price.mantissa = 100
    envelope.mark_index_price.index_price.source_text = "100"
    envelope.canonical_payload_hash = hashlib.sha256(
        envelope.mark_index_price.SerializeToString(deterministic=True)
    ).digest()
    binding = SimpleNamespace(
        feed=FeedType.MARK_INDEX_PRICE,
        v1_compatibility="NONE",
        partition_key="paired/mark-index",
    )
    return binding, mark, index, envelope


class _Catalog:
    canonical_stream = _STREAM

    def __init__(self, binding):
        self.binding = binding

    def binding_for_envelope(self, _envelope):
        return self.binding


class PairedMarkIndexLineageTests(unittest.IsolatedAsyncioTestCase):
    def _spool_gateway(self):
        temp = tempfile.TemporaryDirectory()
        spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(temp.name) / "paired.sqlite3",
            max_records=100,
            max_payload_bytes=2 * 1024 * 1024,
            max_storage_bytes=4 * 1024 * 1024,
            min_free_disk_bytes=0,
        ))
        gateway = DurableStreamGateway(
            handoff=GapFreeHandoff(
                spool,
                SignedHandoffCursorCodec({"k": b"k" * 32}, active_key_id="k"),
                checkpoint_ttl_seconds=3600,
            ),
            sink=spool,
        )
        return temp, spool, gateway

    def test_valid_okx_and_binance_components_validate(self):
        for venue in ("OKX", "BINANCE"):
            with self.subTest(venue=venue):
                binding, mark, index, envelope = _pair(venue=venue)
                self.assertEqual(
                    validate_derived_mark_index_component(envelope, mark, binding)
                    .mark_capture_id,
                    bytes(mark.capture_id),
                )
                self.assertEqual(
                    validate_derived_mark_index_component(envelope, index, binding)
                    .index_capture_id,
                    bytes(index.capture_id),
                )
                with self.assertRaisesRegex(ValueError, "stable lineage mismatch"):
                    validate_single_raw_lineage(envelope, mark)

    def test_wrong_component_or_shared_lineage_fails_closed(self):
        binding, _mark, index, envelope = _pair()
        wrong = _raw(
            provider="OKX_DIRECT",
            venue="OKX",
            native_symbol="DOGE-USDT",
            native_channel="index-tickers",
            received_at_ns=1_700_000_000_200_000_000,
        )
        with self.assertRaisesRegex(ValueError, "component lineage mismatch"):
            validate_derived_mark_index_component(envelope, wrong, binding)
        for field, value in (
            ("source_session_id", "wrong-session"),
            ("connection_generation", 8),
            ("authority_revision", 12),
        ):
            with self.subTest(field=field):
                changed = type(index)()
                changed.CopyFrom(index)
                setattr(changed, field, value)
                with self.assertRaisesRegex(ValueError, "component lineage mismatch"):
                    validate_derived_mark_index_component(envelope, changed, binding)
        malformed = market_data_pb2.EventEnvelope()
        malformed.CopyFrom(envelope)
        malformed.source_sequence = "not:a:paired:source"
        with self.assertRaisesRegex(ValueError, "source sequence"):
            validate_derived_mark_index_component(malformed, index, binding)
        with self.assertRaisesRegex(ValueError, "not permitted"):
            validate_derived_mark_index_component(
                envelope,
                index,
                SimpleNamespace(feed=FeedType.TRADE, v1_compatibility="NONE"),
            )

    async def test_projector_and_signed_ingest_preserve_valid_component_marker(self):
        binding, _mark, index, envelope = _pair()
        catalog = _Catalog(binding)
        temp, spool, gateway = self._spool_gateway()
        try:
            record = KafkaProjectorRecord(
                topic="md.canonical.test.v2",
                partition=0,
                offset=0,
                key=binding.partition_key,
                event_id=bytes(envelope.event_id),
                payload=envelope.SerializeToString(deterministic=True),
                accepted_at_ns=envelope.received_at_ns,
                raw_provider_envelope=index.SerializeToString(deterministic=True),
            )
            broker = SimpleNamespace(
                checkpoint_many=lambda records: None,
                checkpoint=lambda record: None,
                pause_canonical=lambda: None,
                resume_canonical=lambda: None,
            )
            target = InMemoryStableProjectionTarget()
            engine = StableProjectorEngine(
                broker=broker,
                spool=spool,
                catalog=catalog,
                canonical_topic=record.topic,
                raw_topics=(),
                sink=LocalStableCanonicalSink(gateway, spool),
                projector=StableCompatibilityProjector(catalog),
                target=target,
            )
            await engine.accept(record)
            self.assertEqual(len(target.latest), 1)
            stored = spool.read_tail(
                stream=_STREAM, partition_key=binding.partition_key, limit=1
            )[0]
            self.assertEqual(
                stored.event.headers["raw_lineage_kind"],
                DERIVED_MARK_INDEX_COMPONENT_V1,
            )

            app = FastAPI()
            install_stable_canonical_ingest(
                app, gateway=gateway, catalog=catalog, spool=spool, secret=_SECRET
            )
            client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://localhost"
            )
            sink = StableHttpCanonicalSink(
                ("http://localhost",), _SECRET, spool, client=client
            )
            replay = DurableEvent(
                stream=_STREAM,
                partition_key=binding.partition_key,
                event_id=bytes(envelope.event_id),
                payload=envelope.SerializeToString(deterministic=True),
                accepted_at_ns=envelope.received_at_ns,
                headers={
                    "raw_stream": "kafka-header:qdl-raw-provider-envelope",
                    "raw_event_id": bytes(envelope.raw_capture_id).hex(),
                    "raw_provider_envelope": base64.b64encode(
                        index.SerializeToString(deterministic=True)
                    ).decode("ascii"),
                    "raw_lineage_kind": DERIVED_MARK_INDEX_COMPONENT_V1,
                },
            )
            accepted = await sink.publish(replay)
            self.assertEqual(accepted.event.event_id, bytes(envelope.event_id))
            await sink.close()
            await client.aclose()
        finally:
            spool.close()
            temp.cleanup()

    async def test_ingest_rejects_marker_without_inline_component(self):
        binding, _mark, _index, envelope = _pair()
        catalog = _Catalog(binding)
        temp, spool, gateway = self._spool_gateway()
        try:
            app = FastAPI()
            install_stable_canonical_ingest(
                app, gateway=gateway, catalog=catalog, spool=spool, secret=_SECRET
            )
            client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://localhost"
            )
            body = (
                b'{"batch_id":"00000000-0000-4000-8000-000000000001",'
                b'"events":[{"canonical":"'
                + base64.b64encode(envelope.SerializeToString(deterministic=True))
                + b'","raw_event_id":"'
                + bytes(envelope.raw_capture_id).hex().encode()
                + b'","raw_lineage_kind":"'
                + DERIVED_MARK_INDEX_COMPONENT_V1.encode()
                + b'","raw_stream":"raw"}],'
                b'"schema":"qdl.v2.stable-canonical-ingest.v1"}'
            )
            import hmac

            signature = "sha256=" + hmac.new(
                _SECRET, body, hashlib.sha256
            ).hexdigest()
            response = await client.post(
                "/internal/v2/canonical/events",
                content=body,
                headers={"X-QDL-Stable-Signature": signature},
            )
            self.assertEqual(response.status_code, 422)
            self.assertIn("requires an inline raw component", response.json()["detail"])
            await client.aclose()
        finally:
            spool.close()
            temp.cleanup()


if __name__ == "__main__":
    unittest.main()

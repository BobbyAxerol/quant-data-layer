from __future__ import annotations

import hashlib
from dataclasses import replace

from qdl.canonical.trade import TradeContext
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import build_raw_envelope


def derive_capture_id(
    *,
    source_session_id: str,
    connection_generation: int,
    received_at_ns: int,
    raw_frame_bytes: bytes,
) -> bytes:
    if not source_session_id.strip() or connection_generation <= 0 or received_at_ns <= 0:
        raise ValueError("capture identity requires session, generation and receive time")
    digest = hashlib.blake2b(digest_size=16, person=b"qdl-raw-cap-v1")
    for value in (
        source_session_id.encode("utf-8"),
        str(connection_generation).encode("ascii"),
        str(received_at_ns).encode("ascii"),
        hashlib.sha256(raw_frame_bytes).digest(),
    ):
        digest.update(len(value).to_bytes(4, "big"))
        digest.update(value)
    return digest.digest()


def capture_exact_frame(
    *,
    provider: str,
    venue: str,
    market: str,
    product_type: str,
    native_symbol: str,
    native_channel: str,
    subscription_id: str,
    source_session_id: str,
    connection_generation: int,
    lease_epoch: int,
    authority_revision: int,
    partition_plan_epoch: int,
    received_at_ns: int,
    raw_frame_bytes: bytes,
    adapter_version: str,
    config_revision: int,
    instrument_catalog_revision: int,
    correlation_id: str,
    transport_protocol: int = raw_provider_pb2.TRANSPORT_PROTOCOL_WEBSOCKET,
    transport_compression: int = raw_provider_pb2.TRANSPORT_COMPRESSION_NONE,
    capture_boundary: int = raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    capture_id = derive_capture_id(
        source_session_id=source_session_id,
        connection_generation=connection_generation,
        received_at_ns=received_at_ns,
        raw_frame_bytes=raw_frame_bytes,
    )
    return build_raw_envelope(
        capture_id=capture_id,
        provider=provider,
        venue=venue,
        market=market,
        product_type=product_type,
        native_symbol=native_symbol,
        native_channel=native_channel,
        subscription_id=subscription_id,
        source_session_id=source_session_id,
        connection_generation=connection_generation,
        lease_epoch=lease_epoch,
        authority_revision=authority_revision,
        partition_plan_epoch=partition_plan_epoch,
        received_at_ns=received_at_ns,
        transport_protocol=transport_protocol,
        transport_compression=transport_compression,
        capture_boundary=capture_boundary,
        raw_frame_bytes=raw_frame_bytes,
        adapter_version=adapter_version,
        config_revision=config_revision,
        instrument_catalog_revision=instrument_catalog_revision,
        correlation_id=correlation_id,
        test_provenance=test_provenance,
    )


def bind_capture_context(
    context: TradeContext,
    raw_envelope: raw_provider_pb2.RawProviderEnvelope,
) -> TradeContext:
    return replace(
        context,
        received_at_ns=raw_envelope.received_at_ns,
        source_session_id=raw_envelope.source_session_id,
        connection_generation=raw_envelope.connection_generation,
        authority_revision=raw_envelope.authority_revision,
        partition_plan_epoch=raw_envelope.partition_plan_epoch,
        raw_capture_id=bytes(raw_envelope.capture_id),
        raw_frame_sha256=bytes(raw_envelope.raw_frame_sha256),
    )

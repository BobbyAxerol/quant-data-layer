from __future__ import annotations

import hashlib
import hmac
from typing import Final

from qdl.provider.v1 import raw_provider_pb2


RAW_SCHEMA_NAME: Final = "qdl.provider.raw"
RAW_SCHEMA_MAJOR: Final = 1
MAX_RAW_FRAME_BYTES: Final = 1_048_576


def validate_raw_envelope(value: raw_provider_pb2.RawProviderEnvelope) -> None:
    required = {
        "raw_schema_name": value.raw_schema_name,
        "provider": value.provider,
        "venue": value.venue,
        "market": value.market,
        "product_type": value.product_type,
        "native_symbol": value.native_symbol,
        "native_channel": value.native_channel,
        "subscription_id": value.subscription_id,
        "source_session_id": value.source_session_id,
        "adapter_version": value.adapter_version,
        "correlation_id": value.correlation_id,
    }
    missing = [name for name, item in required.items() if not item.strip()]
    if missing:
        raise ValueError(f"raw envelope required fields missing: {','.join(missing)}")
    if value.raw_schema_name != RAW_SCHEMA_NAME or value.raw_schema_major != 1:
        raise ValueError("unsupported raw envelope schema")
    if len(value.capture_id) != 16:
        raise ValueError("capture_id must be exactly 16 bytes")
    if not value.raw_frame_bytes:
        raise ValueError("raw_frame_bytes must not be empty")
    if len(value.raw_frame_bytes) > MAX_RAW_FRAME_BYTES:
        raise ValueError("raw_frame_bytes exceeds maximum")
    if value.received_at_ns <= 0:
        raise ValueError("received_at_ns must be positive")
    if min(
        value.connection_generation,
        value.lease_epoch,
        value.authority_revision,
        value.partition_plan_epoch,
        value.config_revision,
        value.instrument_catalog_revision,
    ) <= 0:
        raise ValueError("all epochs/revisions must be positive")
    if (
        value.transport_protocol == raw_provider_pb2.TRANSPORT_PROTOCOL_UNSPECIFIED
        or value.transport_compression
        == raw_provider_pb2.TRANSPORT_COMPRESSION_UNSPECIFIED
        or value.capture_boundary == raw_provider_pb2.CAPTURE_BOUNDARY_UNSPECIFIED
    ):
        raise ValueError("transport and capture semantics must be explicit")
    if not hmac.compare_digest(
        value.raw_frame_sha256, hashlib.sha256(value.raw_frame_bytes).digest()
    ):
        raise ValueError("raw frame SHA-256 mismatch")


def build_raw_envelope(
    *,
    capture_id: bytes,
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
    transport_protocol: int,
    transport_compression: int,
    capture_boundary: int,
    raw_frame_bytes: bytes,
    adapter_version: str,
    config_revision: int,
    instrument_catalog_revision: int,
    correlation_id: str,
    test_provenance: bool,
) -> raw_provider_pb2.RawProviderEnvelope:
    value = raw_provider_pb2.RawProviderEnvelope(
        raw_schema_name=RAW_SCHEMA_NAME,
        raw_schema_major=RAW_SCHEMA_MAJOR,
        raw_schema_minor=0,
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
        raw_frame_sha256=hashlib.sha256(raw_frame_bytes).digest(),
        adapter_version=adapter_version,
        config_revision=config_revision,
        instrument_catalog_revision=instrument_catalog_revision,
        correlation_id=correlation_id,
        test_provenance=test_provenance,
    )
    validate_raw_envelope(value)
    return value

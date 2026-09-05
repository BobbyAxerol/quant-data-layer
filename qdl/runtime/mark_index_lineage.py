from __future__ import annotations

import hashlib
from dataclasses import dataclass

from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.query import FeedType
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceBinding


DERIVED_MARK_INDEX_COMPONENT_V1 = "DERIVED_MARK_INDEX_COMPONENT_V1"
_CAPTURE_DOMAIN = b"qdl-mark-index-capture-v1"


@dataclass(frozen=True, slots=True)
class DerivedMarkIndexLineage:
    """Validated Rust-owned pairing identity for a logical mark/index event."""

    mark_capture_id: bytes
    index_capture_id: bytes


def paired_mark_index_lineage(
    envelope: market_data_pb2.EventEnvelope,
) -> DerivedMarkIndexLineage:
    """Parse and recompute the pair identity emitted by ``qdl-realtime-core``.

    This is deliberately narrow. A component raw frame is not a replacement for
    the pair: Rust remains the only layer which assembles mark and index values.
    The reader merely verifies that an inline component belongs to that exact
    Rust-derived pair before retaining the canonical event.
    """

    if envelope.WhichOneof("payload") != "mark_index_price":
        raise ValueError("derived MARK_INDEX lineage requires MARK_INDEX_PRICE")
    fields = envelope.source_sequence.split(":")
    if len(fields) != 6:
        raise ValueError("derived MARK_INDEX source sequence is invalid")
    try:
        source_times = tuple(int(value) for value in fields[:4])
        mark_capture_id = bytes.fromhex(fields[4])
        index_capture_id = bytes.fromhex(fields[5])
    except ValueError as error:
        raise ValueError("derived MARK_INDEX source sequence is invalid") from error
    if (
        any(value < 0 for value in source_times)
        or source_times[2] <= 0
        or source_times[3] <= 0
        or len(mark_capture_id) != 16
        or len(index_capture_id) != 16
    ):
        raise ValueError("derived MARK_INDEX source sequence is invalid")
    digest = hashlib.sha256()
    digest.update(_CAPTURE_DOMAIN)
    digest.update(mark_capture_id)
    digest.update(index_capture_id)
    if (
        len(envelope.raw_capture_id) != 16
        or digest.digest()[:16] != bytes(envelope.raw_capture_id)
        or len(envelope.raw_payload_hash) != 32
    ):
        raise ValueError("derived MARK_INDEX capture lineage is invalid")
    return DerivedMarkIndexLineage(
        mark_capture_id=mark_capture_id,
        index_capture_id=index_capture_id,
    )


def is_derived_mark_index_lineage(
    envelope: market_data_pb2.EventEnvelope,
) -> bool:
    try:
        paired_mark_index_lineage(envelope)
    except ValueError:
        return False
    return True


def validate_single_raw_lineage(
    envelope: market_data_pb2.EventEnvelope,
    raw: raw_provider_pb2.RawProviderEnvelope,
) -> None:
    """Validate the normal one-canonical-event-to-one-raw-frame contract."""

    validate_raw_envelope(raw)
    if (
        bytes(raw.capture_id) != bytes(envelope.raw_capture_id)
        or bytes(raw.raw_frame_sha256) != bytes(envelope.raw_payload_hash)
        or raw.provider != envelope.provider
        or raw.venue != envelope.venue
        or raw.market != envelope.market
        or raw.native_symbol != envelope.native_symbol
        or raw.source_session_id != envelope.source_session_id
        or raw.connection_generation != envelope.connection_generation
        or raw.authority_revision != envelope.authority_revision
    ):
        raise ValueError("canonical/raw stable lineage mismatch")


def validate_derived_mark_index_component(
    envelope: market_data_pb2.EventEnvelope,
    raw: raw_provider_pb2.RawProviderEnvelope,
    binding: StableSourceBinding,
) -> DerivedMarkIndexLineage:
    """Validate one physical raw component of a Rust-derived mark/index pair."""

    if (
        binding.feed is not FeedType.MARK_INDEX_PRICE
        or binding.v1_compatibility != "NONE"
    ):
        raise ValueError("derived MARK_INDEX lineage is not permitted for binding")
    lineage = paired_mark_index_lineage(envelope)
    validate_raw_envelope(raw)
    if (
        bytes(raw.capture_id)
        not in {lineage.mark_capture_id, lineage.index_capture_id}
        or raw.provider != envelope.provider
        or raw.venue != envelope.venue
        or raw.market != envelope.market
        or raw.source_session_id != envelope.source_session_id
        or raw.connection_generation != envelope.connection_generation
        or raw.authority_revision != envelope.authority_revision
    ):
        raise ValueError("derived MARK_INDEX component lineage mismatch")
    return lineage

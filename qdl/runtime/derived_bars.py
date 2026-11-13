"""DERIVED_FINAL bars (DL v2.3.0): what the bar edge derives, and the raw record.

Purpose: a ``DERIVED_FINAL`` binding (``stable_deployment.StableDerivedBarAcquisition``)
is a final BAR built from the final bars of the next finer interval of the
same instrument (3m<-1m ... 1h<-30m ... 1w<-1d). This module decides, for one
target open, whether the edge publishes, waits, skips or records a gap, and
builds the raw ``qdl_derived_bar`` record. It reads nothing and writes nothing
itself: the edge passes it durable rows read from the market cache
(``KnBarReadback.final_bar_rows``) and publishes what it returns.

Boundary and invariants:

* the record carries constituent identities and values only. The Rust core
  (``rust/qdl-core/src/derived_bar.rs``) recomputes the aggregate and is the
  only canonical writer (``BAR_ORIGIN_AGGREGATED``), so a deriver bug cannot
  assert a value the core did not compute;
* final constituents only, a complete contiguous bucket only. A missing
  constituent is never synthesized and never forward/back-filled: the target
  waits for the bounded grace, then stays a visible gap (no bar);
* a row already in the durable cache is never replaced except by a higher
  revision of an AGGREGATED row (``supersedes_event_id`` names it). The derived
  revision is the sum of constituent revisions, so any constituent correction
  raises it strictly (the projector refuses equal-revision different content);
* calendar buckets come from ``qdl.adapters.intervals.bar_bucket_open_ms``
  (Monday-anchored weeks), the one interval owner.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Mapping, Sequence

from qdl.adapters.intervals import canonical_interval_ms, is_valid_bar_open_ms
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import capture_exact_frame
from qdl.runtime.kn_bar_readback import DurableBarRow
from qdl.runtime.stable_source import bar_item_fields

DERIVED_BAR_SCHEMA = "qdl.derived-bar.v1"
DERIVED_BAR_KIND = "qdl_derived_bar"

PUBLISH = "PUBLISH"
SKIP = "SKIP"
WAIT = "WAIT"
GAP = "GAP"


def constituent_sha256(rows: Sequence[DurableBarRow]) -> str:
    """``qdl_core::derived_bar::constituent_sha256`` over the same lines."""

    digest = hashlib.sha256()
    for row in rows:
        envelope = row.envelope
        digest.update(
            f"{bytes(envelope.event_id).hex()}:{int(envelope.bar.open_time_ns)}:"
            f"{int(envelope.bar.revision)}\n".encode("ascii")
        )
    return digest.hexdigest()


def _constituent(row: DurableBarRow) -> dict:
    envelope = row.envelope
    payload = bar_item_fields(envelope)["payload"]
    return {
        "event_id": bytes(envelope.event_id).hex(),
        "open_time_ns": int(envelope.bar.open_time_ns),
        "close_time_ns": int(envelope.bar.close_time_ns),
        "open": payload["open"],
        "high": payload["high"],
        "low": payload["low"],
        "close": payload["close"],
        "volume": payload["volume"],
        "base_volume": payload["base_volume"],
        "quote_volume": payload["quote_volume"],
        "contract_volume": payload["contract_volume"],
        "trade_count": int(envelope.bar.trade_count),
        "revision": int(envelope.bar.revision),
        "lifecycle": int(envelope.bar.lifecycle),
        "is_final": bool(envelope.bar.is_final),
        "volume_unit": int(envelope.bar.volume_unit),
        "quality_flags": [int(value) for value in envelope.quality_flags],
        "source_offset": int(row.source_offset),
    }


def derived_record_bytes(
    *,
    interval: str,
    base_interval: str,
    base_source_id: str,
    native_symbol: str,
    open_ms: int,
    constituents: Sequence[DurableBarRow],
    supersedes_event_id: bytes | None = None,
) -> bytes:
    """The exact ``raw_frame_bytes`` of one derived-bar record."""

    record = {
        "schema": DERIVED_BAR_SCHEMA,
        "interval": interval,
        "base_interval": base_interval,
        "base_source_id": base_source_id,
        "native_symbol": native_symbol,
        "open_time_ms": int(open_ms),
        "constituent_sha256": constituent_sha256(constituents),
        "constituents": [_constituent(row) for row in constituents],
    }
    if supersedes_event_id is not None:
        record["supersedes_event_id"] = bytes(supersedes_event_id).hex()
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


@dataclass(frozen=True, slots=True)
class DerivedDecision:
    """What the edge does for one target open."""

    action: str
    open_ms: int
    constituents: tuple[DurableBarRow, ...] = ()
    supersedes_event_id: bytes | None = None
    reason: str = ""


def constituent_opens(open_ms: int, *, interval: str, base_interval: str) -> tuple[int, ...]:
    target_ms = canonical_interval_ms(interval)
    base_ms = canonical_interval_ms(base_interval)
    return tuple(open_ms + index * base_ms for index in range(target_ms // base_ms))


def decide(
    *,
    open_ms: int,
    interval: str,
    base_interval: str,
    parent_rows: Mapping[int, DurableBarRow],
    existing: DurableBarRow | None,
    now_ms: int,
    gap_grace_ms: int,
) -> DerivedDecision:
    """Decide one target open from durable rows only (no I/O)."""

    opens = constituent_opens(open_ms, interval=interval, base_interval=base_interval)
    rows = tuple(parent_rows.get(item) for item in opens)
    if all(row is not None for row in rows):
        revision = sum(int(row.envelope.bar.revision) for row in rows)
        if existing is None:
            return DerivedDecision(PUBLISH, open_ms, rows)
        if existing.envelope.bar.origin != common_pb2.BAR_ORIGIN_AGGREGATED:
            # Venue history (the binding's bootstrap) is kept, never replaced
            # by a derivation of the same open.
            return DerivedDecision(SKIP, open_ms, reason="NATIVE_ROW_KEPT")
        if int(existing.envelope.bar.revision) >= revision:
            return DerivedDecision(SKIP, open_ms, reason="ALREADY_DERIVED")
        return DerivedDecision(
            PUBLISH, open_ms, rows, supersedes_event_id=bytes(existing.envelope.event_id),
            reason="REVISION",
        )
    if existing is not None:
        return DerivedDecision(SKIP, open_ms, reason="DURABLE_ROW_PRESENT")
    close_ms = open_ms + canonical_interval_ms(interval)
    missing = sum(1 for row in rows if row is None)
    if now_ms >= close_ms + gap_grace_ms:
        return DerivedDecision(GAP, open_ms, reason=f"MISSING_CONSTITUENTS:{missing}")
    return DerivedDecision(WAIT, open_ms, reason=f"MISSING_CONSTITUENTS:{missing}")


def derived_raw_envelope(
    *,
    source,
    acquisition,
    base_source,
    decision: DerivedDecision,
    source_session_id: str,
    connection_generation: int,
    authority_revision: int,
    config_revision: int,
    instrument_catalog_revision: int,
    received_at_ns: int,
) -> raw_provider_pb2.RawProviderEnvelope:
    """The raw record for one PUBLISH decision, on the binding's core key."""

    if decision.action != PUBLISH or not decision.constituents:
        raise ValueError("only a PUBLISH decision has a derived record")
    identity = source.instrument.identity
    if not is_valid_bar_open_ms(source.interval, decision.open_ms, provider=identity.venue):
        raise ValueError("derived open is not on the provider calendar")
    frame = derived_record_bytes(
        interval=source.interval,
        base_interval=acquisition.derived_from.interval,
        base_source_id=base_source.source_id,
        native_symbol=source.instrument.native_symbol,
        open_ms=decision.open_ms,
        constituents=decision.constituents,
        supersedes_event_id=decision.supersedes_event_id,
    )
    revision = sum(int(row.envelope.bar.revision) for row in decision.constituents)
    return capture_exact_frame(
        provider=source.provider,
        venue=identity.venue,
        market=identity.market,
        product_type=identity.product_type.value,
        native_symbol=source.instrument.native_symbol,
        # The binding's core key: the venue history channel it was registered
        # under (`core_config`), so the core resolves the DERIVED_FINAL binding.
        native_channel=acquisition.native_channel,
        subscription_id=source.source_id,
        source_session_id=source_session_id,
        connection_generation=connection_generation,
        lease_epoch=1,
        authority_revision=authority_revision,
        partition_plan_epoch=1,
        received_at_ns=received_at_ns,
        raw_frame_bytes=frame,
        adapter_version=source.adapter_version,
        config_revision=config_revision,
        instrument_catalog_revision=instrument_catalog_revision,
        correlation_id=f"derived:{source.binding_id}:{decision.open_ms}:r{revision}",
        # Read back from the durable cache, never off a venue transport; the
        # core refuses a derived record on any other transport.
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_FILE_REPLAY,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_REPLAY_BYTES,
        test_provenance=False,
    )


def final_lifecycles() -> frozenset[int]:
    return frozenset({market_data_pb2.BAR_LIFECYCLE_FINAL, market_data_pb2.BAR_LIFECYCLE_REVISED})

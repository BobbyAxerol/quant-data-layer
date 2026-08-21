from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from qdl.common.v1 import common_pb2
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.event_id import deterministic_event_id
from qdl.domain.quantity import quantity_unit_proto
from qdl.marketdata.v2 import market_data_pb2
from qdl.transport.contracts import DurableEvent, partition_key


@dataclass(frozen=True)
class TradeContext:
    instrument_uid: str
    instrument_id: str
    instrument_revision: int
    venue: str
    market: str
    product_type: str
    native_symbol: str
    provider: str
    source_id: str
    lease_epoch: int
    received_at_ns: int
    normalized_at_ns: int
    published_at_ns: int
    partition_sequence: int
    normalizer_version: str
    adapter_version: str
    config_revision: int
    correlation_id: str = ""
    source_session_id: str = ""
    connection_generation: int = 0
    authority_revision: int = 0
    partition_plan_epoch: int = 0
    raw_capture_id: bytes = b""
    raw_frame_sha256: bytes = b""
    source_role: str = "PRIMARY"

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_capture_id", bytes(self.raw_capture_id))
        object.__setattr__(self, "raw_frame_sha256", bytes(self.raw_frame_sha256))


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _decimal(value: Any) -> common_pb2.DecimalValue:
    parsed = CanonicalDecimal.from_text(str(value))
    message = common_pb2.DecimalValue(scale=parsed.scale, source_text=parsed.source_text)
    if parsed.uses_text_coefficient:
        message.mantissa_text = str(parsed.coefficient)
    else:
        message.mantissa = int(parsed.coefficient)
    return message


def _positive_trade_decimal(value: Any, *, field: str) -> common_pb2.DecimalValue:
    parsed = CanonicalDecimal.from_text(str(value))
    if parsed.as_decimal() <= 0:
        raise ValueError(f"{field} must be positive")
    return _decimal(value)


def _required(raw: Mapping[str, Any], field: str) -> Any:
    value = raw.get(field)
    if value is None or value == "":
        raise ValueError(f"required provider field is missing: {field}")
    return value


def _required_bool(raw: Mapping[str, Any], field: str) -> bool:
    value = raw.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"required provider boolean is missing or invalid: {field}")
    return value


def source_role_proto(value: str) -> int:
    normalized = value.strip().upper()
    if normalized not in {"PRIMARY", "SECONDARY", "REFERENCE", "BACKFILL"}:
        raise ValueError("canonical source role is invalid")
    return getattr(common_pb2, f"SOURCE_ROLE_{normalized}")


def _set_canonical_payload_hash(
    envelope: market_data_pb2.EventEnvelope, *, enabled: bool
) -> None:
    if not enabled:
        return
    payload_name = envelope.WhichOneof("payload")
    if not payload_name:
        raise ValueError("canonical payload is required before hashing")
    payload = getattr(envelope, payload_name).SerializeToString(deterministic=True)
    envelope.canonical_payload_hash = hashlib.sha256(payload).digest()


def _validate_shadow_context(context: TradeContext) -> None:
    if context.source_session_id and len(context.raw_capture_id) != 16:
        raise ValueError("exact-frame shadow context requires a 16-byte raw_capture_id")
    if context.source_session_id and len(context.raw_frame_sha256) != 32:
        raise ValueError("exact-frame shadow context requires a 32-byte raw_frame_sha256")


def _trade_envelope(
    *,
    raw: Mapping[str, Any],
    context: TradeContext,
    native_trade_id: str,
    price: Any,
    quantity: Any,
    side: int,
    source_event_time_ms: int,
    is_buyer_maker: bool,
    identity_kind: int = market_data_pb2.TRADE_IDENTITY_KIND_NATIVE,
) -> market_data_pb2.EventEnvelope:
    _validate_shadow_context(context)
    raw_bytes = canonical_json_bytes(raw)
    event_id = deterministic_event_id(
        [
            2,
            context.venue,
            context.market,
            context.instrument_uid,
            "trade",
            context.source_id,
            native_trade_id,
        ]
    )
    envelope = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.trade",
        schema_major=2,
        schema_minor=0,
        event_id=event_id,
        instrument_uid=context.instrument_uid,
        instrument_id=context.instrument_id,
        instrument_revision=context.instrument_revision,
        venue=context.venue,
        market=context.market,
        product_type=context.product_type,
        native_symbol=context.native_symbol,
        provider=context.provider,
        source_id=context.source_id,
        source_role=source_role_proto(context.source_role),
        lease_epoch=context.lease_epoch,
        source_event_time_ns=source_event_time_ms * 1_000_000,
        received_at_ns=context.received_at_ns,
        normalized_at_ns=context.normalized_at_ns,
        published_at_ns=context.published_at_ns,
        source_sequence=native_trade_id,
        partition_sequence=context.partition_sequence,
        normalizer_version=context.normalizer_version,
        adapter_version=context.adapter_version,
        raw_payload_hash=context.raw_frame_sha256 or hashlib.sha256(raw_bytes).digest(),
        correlation_id=context.correlation_id,
        config_revision=context.config_revision,
        source_session_id=context.source_session_id,
        connection_generation=context.connection_generation,
        authority_revision=context.authority_revision,
        partition_plan_epoch=context.partition_plan_epoch,
        raw_capture_id=context.raw_capture_id,
        trade=market_data_pb2.Trade(
            native_trade_id=native_trade_id,
            price=_positive_trade_decimal(price, field="trade price"),
            quantity=_positive_trade_decimal(quantity, field="trade quantity"),
            aggressor_side=side,
            is_block_trade=False,
            is_buyer_maker=is_buyer_maker,
            quantity_unit=quantity_unit_proto(
                venue=context.venue,
                market=context.market,
                product_type=context.product_type,
            ),
            identity_kind=identity_kind,
        ),
    )
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def canonicalize_binance_usdm_trade(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    symbol = str(_required(raw, "s")).upper()
    if symbol != context.native_symbol.upper():
        raise ValueError("provider symbol does not match resolved instrument")
    native_trade_id = str(raw.get("a") or _required(raw, "t"))
    buyer_maker = _required_bool(raw, "m")
    return _trade_envelope(
        raw=raw,
        context=context,
        native_trade_id=native_trade_id,
        price=_required(raw, "p"),
        quantity=_required(raw, "q"),
        side=(
            common_pb2.AGGRESSOR_SIDE_SELL
            if buyer_maker
            else common_pb2.AGGRESSOR_SIDE_BUY
        ),
        source_event_time_ms=int(raw.get("T") or _required(raw, "E")),
        is_buyer_maker=buyer_maker,
    )


def canonicalize_okx_trade(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    symbol = str(_required(raw, "instId"))
    if symbol != context.native_symbol:
        raise ValueError("provider symbol does not match resolved instrument")
    side_value = str(_required(raw, "side")).lower()
    if side_value not in {"buy", "sell"}:
        raise ValueError("unsupported OKX aggressor side")
    return _trade_envelope(
        raw=raw,
        context=context,
        native_trade_id=str(_required(raw, "tradeId")),
        price=_required(raw, "px"),
        quantity=_required(raw, "sz"),
        side=(
            common_pb2.AGGRESSOR_SIDE_BUY
            if side_value == "buy"
            else common_pb2.AGGRESSOR_SIDE_SELL
        ),
        source_event_time_ms=int(_required(raw, "ts")),
        is_buyer_maker=False,
    )


def canonicalize_dnse_trade(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    symbol = str(_required(raw, "symbol")).upper()
    if symbol != context.native_symbol.upper():
        raise ValueError("provider symbol does not match resolved instrument")
    if len(context.raw_capture_id) != 16:
        raise ValueError("DNSE trade identity requires an exact raw capture id")
    source_time_ms = context.received_at_ns // 1_000_000
    envelope = _trade_envelope(
        raw=raw,
        context=context,
        native_trade_id=f"derived:{context.raw_capture_id.hex()}",
        price=_required(raw, "price"),
        quantity=_required(raw, "quantity"),
        side=common_pb2.AGGRESSOR_SIDE_UNSPECIFIED,
        source_event_time_ms=source_time_ms,
        is_buyer_maker=False,
        identity_kind=market_data_pb2.TRADE_IDENTITY_KIND_DERIVED_RAW_CAPTURE,
    )
    envelope.quality_flags.extend(
        (
            common_pb2.QUALITY_FLAG_SOURCE_TIME_MISSING,
            common_pb2.QUALITY_FLAG_FIELD_MISSING,
        )
    )
    return envelope


def canonical_event(
    envelope: market_data_pb2.EventEnvelope,
    *,
    accepted_at_ns: int,
    raw_event: DurableEvent | None = None,
) -> DurableEvent:
    feed_type = envelope.WhichOneof("payload") or "unknown"
    headers = {
        "adapter_version": envelope.adapter_version,
        "normalizer_version": envelope.normalizer_version,
        "schema": f"{envelope.schema_name}/{envelope.schema_major}",
    }
    if raw_event is not None:
        headers["raw_event_id"] = raw_event.event_id.hex()
        headers["raw_stream"] = raw_event.stream
    return DurableEvent(
        stream=f"md.canonical.v2.{feed_type}",
        partition_key=partition_key(
            instrument_uid=envelope.instrument_uid,
            feed_type=feed_type,
            source_id=envelope.source_id,
        ),
        event_id=bytes(envelope.event_id),
        payload=envelope.SerializeToString(deterministic=True),
        accepted_at_ns=accepted_at_ns,
        headers=headers,
    )


def raw_market_event(
    raw: Mapping[str, Any], *, context: TradeContext, feed_type: str, accepted_at_ns: int
) -> DurableEvent:
    feed = feed_type.strip().lower()
    if not feed:
        raise ValueError("feed_type is required")
    raw_bytes = canonical_json_bytes(raw)
    raw_id = deterministic_event_id(
        [
            "raw-v1",
            context.venue,
            context.market,
            context.source_id,
            feed,
            hashlib.sha256(raw_bytes).digest(),
        ]
    )
    return DurableEvent(
        stream=f"md.raw.v1.{context.venue.lower()}.{context.market.lower()}.{feed}",
        partition_key=partition_key(
            instrument_uid=context.instrument_uid,
            feed_type=feed,
            source_id=context.source_id,
        ),
        event_id=raw_id,
        payload=raw_bytes,
        accepted_at_ns=accepted_at_ns,
        content_type="application/json",
        headers={"adapter_version": context.adapter_version},
    )


def raw_trade_event(
    raw: Mapping[str, Any], *, context: TradeContext, accepted_at_ns: int
) -> DurableEvent:
    return raw_market_event(
        raw, context=context, feed_type="trade", accepted_at_ns=accepted_at_ns
    )

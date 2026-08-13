from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from qdl.common.v1 import common_pb2
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.event_id import deterministic_event_id
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


def _required(raw: Mapping[str, Any], field: str) -> Any:
    value = raw.get(field)
    if value is None or value == "":
        raise ValueError(f"required provider field is missing: {field}")
    return value


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
) -> market_data_pb2.EventEnvelope:
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
    return market_data_pb2.EventEnvelope(
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
        source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        lease_epoch=context.lease_epoch,
        source_event_time_ns=source_event_time_ms * 1_000_000,
        received_at_ns=context.received_at_ns,
        normalized_at_ns=context.normalized_at_ns,
        published_at_ns=context.published_at_ns,
        source_sequence=native_trade_id,
        partition_sequence=context.partition_sequence,
        normalizer_version=context.normalizer_version,
        adapter_version=context.adapter_version,
        raw_payload_hash=hashlib.sha256(raw_bytes).digest(),
        correlation_id=context.correlation_id,
        config_revision=context.config_revision,
        trade=market_data_pb2.Trade(
            native_trade_id=native_trade_id,
            price=_decimal(price),
            quantity=_decimal(quantity),
            aggressor_side=side,
            is_block_trade=False,
            is_buyer_maker=is_buyer_maker,
        ),
    )


def canonicalize_binance_usdm_trade(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    symbol = str(_required(raw, "s")).upper()
    if symbol != context.native_symbol.upper():
        raise ValueError("provider symbol does not match resolved instrument")
    native_trade_id = str(raw.get("a") or _required(raw, "t"))
    buyer_maker = bool(raw.get("m", False))
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


def canonical_event(
    envelope: market_data_pb2.EventEnvelope, *, accepted_at_ns: int
) -> DurableEvent:
    feed_type = envelope.WhichOneof("payload") or "unknown"
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
        headers={
            "adapter_version": envelope.adapter_version,
            "normalizer_version": envelope.normalizer_version,
            "schema": f"{envelope.schema_name}/{envelope.schema_major}",
        },
    )


def raw_trade_event(
    raw: Mapping[str, Any], *, context: TradeContext, accepted_at_ns: int
) -> DurableEvent:
    raw_bytes = canonical_json_bytes(raw)
    raw_id = deterministic_event_id(
        [
            "raw-v1",
            context.venue,
            context.market,
            context.source_id,
            hashlib.sha256(raw_bytes).digest(),
        ]
    )
    return DurableEvent(
        stream=f"md.raw.v1.{context.venue.lower()}.{context.market.lower()}.trade",
        partition_key=partition_key(
            instrument_uid=context.instrument_uid,
            feed_type="trade",
            source_id=context.source_id,
        ),
        event_id=raw_id,
        payload=raw_bytes,
        accepted_at_ns=accepted_at_ns,
        content_type="application/json",
        headers={"adapter_version": context.adapter_version},
    )

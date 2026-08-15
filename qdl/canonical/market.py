from __future__ import annotations

import hashlib
from typing import Any, Mapping

from qdl.common.v1 import common_pb2
from qdl.domain.event_id import deterministic_event_id
from qdl.marketdata.v2 import market_data_pb2

from qdl.canonical.trade import (
    TradeContext,
    _decimal,
    _required,
    _required_bool,
    _set_canonical_payload_hash,
    _validate_shadow_context,
    canonical_json_bytes,
)


def _envelope(
    *, raw: Mapping[str, Any], context: TradeContext, feed: str,
    source_sequence: str, source_event_time_ms: int,
) -> market_data_pb2.EventEnvelope:
    _validate_shadow_context(context)
    raw_bytes = canonical_json_bytes(raw)
    return market_data_pb2.EventEnvelope(
        schema_name=f"qdl.marketdata.{feed}", schema_major=2, schema_minor=0,
        event_id=deterministic_event_id([
            2, context.venue, context.market, context.instrument_uid,
            feed, context.source_id, source_sequence,
        ]),
        instrument_uid=context.instrument_uid, instrument_id=context.instrument_id,
        instrument_revision=context.instrument_revision, venue=context.venue,
        market=context.market, product_type=context.product_type,
        native_symbol=context.native_symbol, provider=context.provider,
        source_id=context.source_id, source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        lease_epoch=context.lease_epoch,
        source_event_time_ns=source_event_time_ms * 1_000_000,
        received_at_ns=context.received_at_ns,
        normalized_at_ns=context.normalized_at_ns,
        published_at_ns=context.published_at_ns,
        source_sequence=source_sequence,
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
    )


def _verify_symbol(raw: Mapping[str, Any], context: TradeContext) -> None:
    if str(_required(raw, "s")).upper() != context.native_symbol.upper():
        raise ValueError("provider symbol does not match resolved instrument")


def canonicalize_binance_usdm_bbo(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    _verify_symbol(raw, context)
    sequence = str(_required(raw, "u"))
    envelope = _envelope(
        raw=raw, context=context, feed="quote", source_sequence=sequence,
        source_event_time_ms=int(raw.get("T") or _required(raw, "E")),
    )
    envelope.quote.CopyFrom(market_data_pb2.Quote(
        bid_price=_decimal(_required(raw, "b")),
        bid_quantity=_decimal(_required(raw, "B")),
        ask_price=_decimal(_required(raw, "a")),
        ask_quantity=_decimal(_required(raw, "A")), level=1,
    ))
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def canonicalize_binance_usdm_bar(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    _verify_symbol(raw, context)
    kline = raw.get("k")
    if not isinstance(kline, Mapping):
        raise ValueError("Binance kline frame requires k object")
    if str(_required(kline, "s")).upper() != context.native_symbol.upper():
        raise ValueError("provider kline symbol does not match resolved instrument")
    source_time = int(_required(raw, "E"))
    sequence = f"{_required(kline, 't')}:{_required(kline, 'L')}:{source_time}"
    envelope = _envelope(
        raw=raw, context=context, feed="bar", source_sequence=sequence,
        source_event_time_ms=source_time,
    )
    is_final = _required_bool(kline, "x")
    envelope.bar.CopyFrom(market_data_pb2.Bar(
        interval=str(_required(kline, "i")),
        open_time_ns=int(_required(kline, "t")) * 1_000_000,
        close_time_ns=int(_required(kline, "T")) * 1_000_000,
        open=_decimal(_required(kline, "o")), high=_decimal(_required(kline, "h")),
        low=_decimal(_required(kline, "l")), close=_decimal(_required(kline, "c")),
        volume=_decimal(_required(kline, "v")), trade_count=int(_required(kline, "n")),
        is_final=is_final, revision=0,
        origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE,
        lifecycle=(
            market_data_pb2.BAR_LIFECYCLE_FINAL
            if is_final
            else market_data_pb2.BAR_LIFECYCLE_IN_PROGRESS
        ),
    ))
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def canonicalize_binance_usdm_rest_bar(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    if str(_required(raw, "symbol")).upper() != context.native_symbol.upper():
        raise ValueError("provider symbol does not match resolved instrument")
    row = raw.get("row")
    if not isinstance(row, list) or len(row) < 11:
        raise ValueError("Binance REST kline requires the unmodified native row")
    sequence = f"{row[0]}:{row[6]}"
    envelope = _envelope(
        raw=raw, context=context, feed="bar", source_sequence=sequence,
        source_event_time_ms=int(row[6]),
    )
    envelope.quality_flags.append(common_pb2.QUALITY_FLAG_BACKFILLED)
    envelope.bar.CopyFrom(market_data_pb2.Bar(
        interval=str(_required(raw, "interval")),
        open_time_ns=int(row[0]) * 1_000_000,
        close_time_ns=int(row[6]) * 1_000_000,
        open=_decimal(row[1]), high=_decimal(row[2]), low=_decimal(row[3]),
        close=_decimal(row[4]), volume=_decimal(row[5]), trade_count=int(row[8]),
        is_final=True, revision=0, origin=common_pb2.BAR_ORIGIN_BACKFILLED,
        lifecycle=market_data_pb2.BAR_LIFECYCLE_FINAL,
    ))
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def canonicalize_dnse_bar(
    raw: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    if str(_required(raw, "symbol")).upper() != context.native_symbol.upper():
        raise ValueError("DNSE bar symbol does not match resolved instrument")
    open_time_ms = int(_required(raw, "open_time_ms"))
    close_time_ms = int(_required(raw, "close_time_ms"))
    is_final = _required_bool(raw, "is_final")
    trade_count_available = _required_bool(raw, "trade_count_available")
    trade_count = int(_required(raw, "trade_count")) if trade_count_available else 0
    sequence = f"{open_time_ms}:{close_time_ms}"
    envelope = _envelope(
        raw=raw,
        context=context,
        feed="bar",
        source_sequence=sequence,
        source_event_time_ms=close_time_ms,
    )
    if not trade_count_available:
        envelope.quality_flags.append(common_pb2.QUALITY_FLAG_FIELD_MISSING)
    envelope.bar.CopyFrom(
        market_data_pb2.Bar(
            interval=str(_required(raw, "interval")),
            open_time_ns=open_time_ms * 1_000_000,
            close_time_ns=close_time_ms * 1_000_000,
            open=_decimal(_required(raw, "o")),
            high=_decimal(_required(raw, "h")),
            low=_decimal(_required(raw, "l")),
            close=_decimal(_required(raw, "c")),
            volume=_decimal(_required(raw, "v")),
            trade_count=trade_count,
            is_final=is_final,
            revision=int(_required(raw, "revision")),
            origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE,
            lifecycle=(
                market_data_pb2.BAR_LIFECYCLE_FINAL
                if is_final
                else market_data_pb2.BAR_LIFECYCLE_IN_PROGRESS
            ),
        )
    )
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope

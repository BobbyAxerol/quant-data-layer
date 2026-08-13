from __future__ import annotations

import hashlib
from typing import Any, Mapping

from qdl.common.v1 import common_pb2
from qdl.domain.event_id import deterministic_event_id
from qdl.marketdata.v2 import market_data_pb2

from qdl.canonical.trade import TradeContext, _decimal, _required, canonical_json_bytes


def _envelope(
    *, raw: Mapping[str, Any], context: TradeContext, feed: str,
    source_sequence: str, source_event_time_ms: int,
) -> market_data_pb2.EventEnvelope:
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
        raw_payload_hash=hashlib.sha256(raw_bytes).digest(),
        correlation_id=context.correlation_id,
        config_revision=context.config_revision,
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
    sequence = f"{_required(kline, 't')}:{kline.get('L', 0)}:{source_time}"
    envelope = _envelope(
        raw=raw, context=context, feed="bar", source_sequence=sequence,
        source_event_time_ms=source_time,
    )
    envelope.bar.CopyFrom(market_data_pb2.Bar(
        interval=str(_required(kline, "i")),
        open_time_ns=int(_required(kline, "t")) * 1_000_000,
        close_time_ns=int(_required(kline, "T")) * 1_000_000,
        open=_decimal(_required(kline, "o")), high=_decimal(_required(kline, "h")),
        low=_decimal(_required(kline, "l")), close=_decimal(_required(kline, "c")),
        volume=_decimal(_required(kline, "v")), trade_count=int(kline.get("n") or 0),
        is_final=bool(kline.get("x", False)), revision=0,
        origin=common_pb2.BAR_ORIGIN_VENUE_NATIVE,
    ))
    return envelope

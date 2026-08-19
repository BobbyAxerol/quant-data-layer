from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.domain.quantity import quantity_unit_proto

from qdl.canonical.market import _envelope
from qdl.canonical.trade import (
    TradeContext,
    _decimal,
    _required,
    _set_canonical_payload_hash,
)


def _levels(
    rows: list[list[str]], side: int, quantity_unit: int
) -> list[market_data_pb2.BookLevel]:
    output = []
    for row in rows:
        if len(row) < 4:
            raise ValueError("OKX book level requires price, size and order count")
        output.append(
            market_data_pb2.BookLevel(
                side=side,
                price=_decimal(row[0]),
                quantity=_decimal(row[1]),
                order_count=int(_required({"order_count": row[3]}, "order_count")),
                quantity_unit=quantity_unit,
            )
        )
    return output


def canonicalize_okx_book(
    frame: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    argument = frame.get("arg")
    rows = frame.get("data")
    if not isinstance(argument, Mapping) or argument.get("instId") != context.native_symbol:
        raise ValueError("OKX book frame instrument mismatch")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise ValueError("OKX book frame requires one data row")
    row = rows[0]
    action = str(frame.get("action") or "")
    if action not in {"snapshot", "update"}:
        raise ValueError("OKX book action must be snapshot or update")
    sequence = str(_required(row, "seqId"))
    envelope = _envelope(
        raw=frame,
        context=context,
        feed="book_snapshot" if action == "snapshot" else "book_delta",
        source_sequence=sequence,
        source_event_time_ms=int(_required(row, "ts")),
    )
    unit = quantity_unit_proto(
        venue=context.venue,
        market=context.market,
        product_type=context.product_type,
    )
    levels = [
        *_levels(row.get("bids", []), common_pb2.BOOK_SIDE_BID, unit),
        *_levels(row.get("asks", []), common_pb2.BOOK_SIDE_ASK, unit),
    ]
    checksum = str(row.get("checksum") or "")
    if action == "snapshot":
        envelope.book_snapshot.CopyFrom(
            market_data_pb2.OrderBookSnapshot(
                native_sequence=sequence,
                checksum=checksum,
                levels=levels,
                depth=max(len(row.get("bids", [])), len(row.get("asks", []))),
            )
        )
    else:
        envelope.book_delta.CopyFrom(
            market_data_pb2.OrderBookDelta(
                native_sequence_start=str(_required(row, "prevSeqId")),
                native_sequence_end=sequence,
                checksum=checksum,
                updates=levels,
                reset=False,
            )
        )
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope


def canonicalize_deribit_option_book_fixture(
    frame: Mapping[str, Any], context: TradeContext
) -> market_data_pb2.EventEnvelope:
    if frame.get("provenance") != "TEST_SYNTHETIC_EXTENSION_FIXTURE":
        raise ValueError("Deribit fixture parser cannot accept live provenance")
    if str(_required(frame, "native_symbol")) != context.native_symbol:
        raise ValueError("Deribit fixture instrument mismatch")
    sequence = str(_required(frame, "change_id"))
    envelope = _envelope(
        raw=frame,
        context=context,
        feed="book_snapshot",
        source_sequence=sequence,
        source_event_time_ms=int(_required(frame, "timestamp")),
    )
    unit = quantity_unit_proto(
        venue=context.venue,
        market=context.market,
        product_type=context.product_type,
    )
    levels = []
    for side, key in (
        (common_pb2.BOOK_SIDE_BID, "bids"),
        (common_pb2.BOOK_SIDE_ASK, "asks"),
    ):
        rows = frame.get(key)
        if not isinstance(rows, list):
            raise ValueError(f"Deribit fixture {key} must be a list")
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                raise ValueError("Deribit fixture level requires price and amount")
            levels.append(
                market_data_pb2.BookLevel(
                    side=side,
                    price=_decimal(row[0]),
                    quantity=_decimal(row[1]),
                    order_count=0,
                )
            )
    envelope.quality_flags.append(common_pb2.QUALITY_FLAG_FIELD_MISSING)
    envelope.book_snapshot.CopyFrom(
        market_data_pb2.OrderBookSnapshot(
            native_sequence=sequence,
            levels=levels,
            depth=max(len(frame.get("bids", [])), len(frame.get("asks", []))),
        )
    )
    _set_canonical_payload_hash(envelope, enabled=bool(context.source_session_id))
    return envelope

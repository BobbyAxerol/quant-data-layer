from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2

from qdl.canonical.market import _envelope
from qdl.canonical.trade import TradeContext, _decimal, _required


def _levels(rows: list[list[str]], side: int) -> list[market_data_pb2.BookLevel]:
    output = []
    for row in rows:
        if len(row) < 2:
            raise ValueError("OKX book level requires price and quantity")
        output.append(
            market_data_pb2.BookLevel(
                side=side,
                price=_decimal(row[0]),
                quantity=_decimal(row[1]),
                order_count=int(row[3]) if len(row) > 3 and row[3] else 0,
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
    levels = [
        *_levels(row.get("bids", []), common_pb2.BOOK_SIDE_BID),
        *_levels(row.get("asks", []), common_pb2.BOOK_SIDE_ASK),
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
    return envelope

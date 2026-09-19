from __future__ import annotations

from qdl.marketdata.v2 import market_data_pb2
from qdl.transport import FINAL_BAR_CLOSE_TIME_NS_HEADER


def final_bar_close_time_ns(
    envelope: market_data_pb2.EventEnvelope,
) -> int | None:
    """Return the durable latest-state watermark for an admitted final BAR."""

    if envelope.WhichOneof("payload") != "bar":
        return None
    bar = envelope.bar
    if (
        not bar.is_final
        or bar.lifecycle
        not in {
            market_data_pb2.BAR_LIFECYCLE_FINAL,
            market_data_pb2.BAR_LIFECYCLE_REVISED,
        }
    ):
        return None
    close_time_ns = int(bar.close_time_ns)
    if close_time_ns <= 0:
        raise ValueError("final BAR close time is invalid")
    return close_time_ns


def final_bar_watermark_headers(
    envelope: market_data_pb2.EventEnvelope,
) -> dict[str, str]:
    """Return private durable metadata; the protobuf payload remains unchanged."""

    close_time_ns = final_bar_close_time_ns(envelope)
    return (
        {FINAL_BAR_CLOSE_TIME_NS_HEADER: str(close_time_ns)}
        if close_time_ns is not None
        else {}
    )

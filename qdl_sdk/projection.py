"""SDK-owned canonical protobuf to typed V2 view projection."""

from __future__ import annotations

import time
from typing import Any

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl_sdk.errors import ContinuityError
from qdl_sdk.models import DataRequirement, Feed, MarketDataView, StreamEvent

_GAP_FLAGS = {"SEQUENCE_GAP_BEFORE", "OUT_OF_ORDER", "RESYNC_REQUIRED"}


def _enum_name(enum_wrapper: Any, value: int, prefix: str) -> str:
    try:
        name = enum_wrapper.Name(value)
    except ValueError as error:
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", f"unknown canonical enum value {value}"
        ) from error
    result = name.removeprefix(prefix)
    if result == "UNSPECIFIED":
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", f"canonical enum {prefix} is unspecified"
        )
    return result


def _decimal(value) -> dict[str, str | int]:
    selected = value.WhichOneof("coefficient")
    if selected == "mantissa":
        coefficient = str(value.mantissa)
    elif selected == "mantissa_text":
        coefficient = value.mantissa_text
    else:
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", "canonical decimal coefficient is missing"
        )
    return {
        "coefficient": coefficient,
        "scale": int(value.scale),
        "source_text": value.source_text,
    }


def _quantity_unit(value: int) -> str:
    return _enum_name(common_pb2.QuantityUnit, value, "QUANTITY_UNIT_")


def _book_level(value) -> dict[str, Any]:
    return {
        "side": _enum_name(common_pb2.BookSide, value.side, "BOOK_SIDE_"),
        "price": _decimal(value.price),
        "quantity": _decimal(value.quantity),
        "quantity_unit": _quantity_unit(value.quantity_unit),
        "order_count": int(value.order_count),
    }


def _payload(
    envelope: market_data_pb2.EventEnvelope,
) -> tuple[Feed, str | None, int, dict[str, Any]]:
    name = envelope.WhichOneof("payload")
    if name == "trade":
        side = common_pb2.AggressorSide.Name(
            envelope.trade.aggressor_side
        ).removeprefix("AGGRESSOR_SIDE_")
        if side == "UNSPECIFIED":
            side = "UNKNOWN"
        identity = _enum_name(
            market_data_pb2.TradeIdentityKind,
            envelope.trade.identity_kind,
            "TRADE_IDENTITY_KIND_",
        )
        return (
            Feed.TRADE,
            None,
            0,
            {
                "feed": "TRADE",
                "native_trade_id": envelope.trade.native_trade_id,
                "price": _decimal(envelope.trade.price),
                "quantity": _decimal(envelope.trade.quantity),
                "quantity_unit": _quantity_unit(envelope.trade.quantity_unit),
                "aggressor_side": side,
                "identity_kind": identity,
                "is_block_trade": bool(envelope.trade.is_block_trade),
                "is_buyer_maker": bool(envelope.trade.is_buyer_maker),
            },
        )
    if name == "quote":
        return (
            Feed.QUOTE,
            None,
            0,
            {
                "feed": "QUOTE",
                "bid_price": _decimal(envelope.quote.bid_price),
                "bid_quantity": _decimal(envelope.quote.bid_quantity),
                "ask_price": _decimal(envelope.quote.ask_price),
                "ask_quantity": _decimal(envelope.quote.ask_quantity),
                "quantity_unit": _quantity_unit(envelope.quote.quantity_unit),
                "level": int(envelope.quote.level),
            },
        )
    if name == "bar":
        lifecycle = _enum_name(
            market_data_pb2.BarLifecycle,
            envelope.bar.lifecycle,
            "BAR_LIFECYCLE_",
        )
        payload: dict[str, Any] = {
            "feed": "BAR",
            "interval": envelope.bar.interval,
            "open_time_ns": int(envelope.bar.open_time_ns),
            "close_time_ns": int(envelope.bar.close_time_ns),
            "open": _decimal(envelope.bar.open),
            "high": _decimal(envelope.bar.high),
            "low": _decimal(envelope.bar.low),
            "close": _decimal(envelope.bar.close),
            "volume": _decimal(envelope.bar.volume),
            "volume_unit": _quantity_unit(envelope.bar.volume_unit),
            "trade_count": int(envelope.bar.trade_count),
            "lifecycle": lifecycle,
            "revision": int(envelope.bar.revision),
            "origin": _enum_name(
                common_pb2.BarOrigin, envelope.bar.origin, "BAR_ORIGIN_"
            ),
            "supersedes_event_id": (
                bytes(envelope.bar.supersedes_event_id).hex()
                if envelope.bar.HasField("supersedes_event_id")
                else None
            ),
        }
        for field in ("base_volume", "quote_volume", "contract_volume"):
            payload[field] = (
                _decimal(getattr(envelope.bar, field))
                if envelope.bar.HasField(field)
                else None
            )
        return Feed.BAR, envelope.bar.interval, int(envelope.bar.revision), payload
    if name == "book_snapshot":
        return (
            Feed.BOOK_SNAPSHOT,
            None,
            0,
            {
                "feed": "BOOK_SNAPSHOT",
                "native_sequence": envelope.book_snapshot.native_sequence,
                "checksum": envelope.book_snapshot.checksum or None,
                "levels": [_book_level(item) for item in envelope.book_snapshot.levels],
                "depth": int(envelope.book_snapshot.depth),
            },
        )
    if name == "book_delta":
        return (
            Feed.BOOK_DELTA,
            None,
            0,
            {
                "feed": "BOOK_DELTA",
                "native_sequence_start": envelope.book_delta.native_sequence_start,
                "native_sequence_end": envelope.book_delta.native_sequence_end,
                "snapshot_sequence": envelope.book_delta.snapshot_sequence,
                "checksum": envelope.book_delta.checksum or None,
                "updates": [_book_level(item) for item in envelope.book_delta.updates],
                "reset": bool(envelope.book_delta.reset),
            },
        )
    if name == "funding_rate":
        return (
            Feed.FUNDING_RATE,
            None,
            0,
            {
                "feed": "FUNDING_RATE",
                "rate": _decimal(envelope.funding_rate.rate),
                "funding_time_ns": int(envelope.funding_rate.funding_time_ns),
                "next_funding_time_ns": (
                    int(envelope.funding_rate.next_funding_time_ns)
                    if envelope.funding_rate.HasField("next_funding_time_ns")
                    else None
                ),
            },
        )
    if name == "open_interest":
        return (
            Feed.OPEN_INTEREST,
            None,
            0,
            {
                "feed": "OPEN_INTEREST",
                "quantity": _decimal(envelope.open_interest.quantity),
                "notional": (
                    _decimal(envelope.open_interest.notional)
                    if envelope.open_interest.HasField("notional")
                    else None
                ),
                "quantity_unit": _quantity_unit(envelope.open_interest.quantity_unit),
            },
        )
    if name == "mark_index_price":
        return (
            Feed.MARK_INDEX_PRICE,
            None,
            0,
            {
                "feed": "MARK_INDEX_PRICE",
                "mark_price": _decimal(envelope.mark_index_price.mark_price),
                "index_price": _decimal(envelope.mark_index_price.index_price),
            },
        )
    if name == "ticker":
        payload = {"feed": "TICKER", "last_price": _decimal(envelope.ticker.last_price)}
        for field in ("last_quantity", "open_24h", "high_24h", "low_24h", "volume_24h"):
            payload[field] = (
                _decimal(getattr(envelope.ticker, field))
                if envelope.ticker.HasField(field)
                else None
            )
        payload["last_quantity_unit"] = (
            _quantity_unit(envelope.ticker.last_quantity_unit)
            if envelope.ticker.HasField("last_quantity")
            else None
        )
        payload["volume_24h_unit"] = (
            _quantity_unit(envelope.ticker.volume_24h_unit)
            if envelope.ticker.HasField("volume_24h")
            else None
        )
        return Feed.TICKER, None, 0, payload
    raise ContinuityError(
        "SCHEMA_NOT_SUPPORTED",
        f"unsupported canonical stream payload: {name or 'none'}",
    )


def market_data_view_from_stream(
    event: StreamEvent,
    *,
    template: MarketDataView,
    requirement: DataRequirement,
    now_ns: int | None = None,
) -> MarketDataView:
    """Project one SDK stream event through its authoritative query handoff.

    The query template owns source policy/catalog metadata. Any stream identity
    or source transition requires a new snapshot and fails closed here.
    """

    envelope = event.event
    if not isinstance(envelope, market_data_pb2.EventEnvelope):
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", "stream event is not a canonical V2 envelope"
        )
    if envelope.schema_major != 2 or not envelope.schema_name:
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", "stream envelope schema is not V2"
        )
    if (
        envelope.instrument_uid != requirement.instrument_uid
        or envelope.instrument_uid != template.instrument_uid
    ):
        raise ContinuityError("CONFLICT", "stream instrument UID differs from handoff")
    if int(envelope.instrument_revision) != template.instrument_revision:
        raise ContinuityError(
            "CONFLICT", "stream instrument revision differs from handoff"
        )
    if (
        envelope.venue.upper() != template.source.venue.upper()
        or envelope.provider != template.source.provider
        or envelope.source_id != template.source.source_id
    ):
        raise ContinuityError(
            "SOURCE_NON_AUTHORITATIVE",
            "stream source transition requires a new snapshot",
        )
    feed, interval, revision, payload = _payload(envelope)
    if feed is not requirement.feed or interval != requirement.interval:
        raise ContinuityError(
            "CONFLICT", "stream feed/interval differs from requirement"
        )
    if requirement.source_policy_id != template.quality.policy_id:
        raise ContinuityError(
            "CONFLICT", "stream handoff source policy differs from requirement"
        )
    if int(envelope.authority_revision) < template.contract.authority_revision:
        raise ContinuityError(
            "SOURCE_NON_AUTHORITATIVE", "stream authority revision moved backwards"
        )
    if (
        not envelope.normalizer_version
        or not envelope.adapter_version
        or envelope.config_revision < 1
    ):
        raise ContinuityError(
            "SCHEMA_NOT_SUPPORTED", "stream contract metadata is incomplete"
        )

    flags = [
        common_pb2.QualityFlag.Name(value).removeprefix("QUALITY_FLAG_")
        for value in envelope.quality_flags
    ]
    gap_open = any(value in _GAP_FLAGS for value in flags)
    observed_at_ns = int(envelope.source_event_time_ns)
    if feed is Feed.BAR:
        observed_for_freshness = int(envelope.bar.close_time_ns)
    else:
        observed_for_freshness = observed_at_ns
    freshness_ms = max(
        0, ((now_ns or time.time_ns()) - observed_for_freshness) // 1_000_000
    )
    stale = (
        requirement.max_freshness_ms is not None
        and freshness_ms > requirement.max_freshness_ms
    )
    state = "GAPPED" if gap_open else "STALE" if stale else "LIVE"
    if gap_open and requirement.gap_policy.value in {"BLOCK", "PAUSE"}:
        raise ContinuityError(
            "OPEN_SEQUENCE_GAP", "stream event violates the requested gap policy"
        )
    if stale and requirement.stale_policy.value in {"BLOCK", "PAUSE"}:
        raise ContinuityError(
            "DATA_STALE", "stream event violates the requested freshness policy"
        )

    authoritative = (
        template.source.authoritative
        and template.source.source_role == "PRIMARY"
        and state == "LIVE"
    )
    if requirement.consumer_grade.value == "EXECUTION" and not authoritative:
        code = (
            "OPEN_SEQUENCE_GAP"
            if gap_open
            else "DATA_STALE"
            if stale
            else "SOURCE_NON_AUTHORITATIVE"
        )
        raise ContinuityError(code, "stream event is not execution eligible")
    if (
        feed is Feed.BAR
        and requirement.require_final_bars
        and payload["lifecycle"] not in {"FINAL", "REVISED"}
    ):
        raise ContinuityError("DATA_NOT_READY", "stream bar is not final")

    return MarketDataView.model_validate(
        {
            "instrument_uid": envelope.instrument_uid,
            "instrument_id": envelope.instrument_id,
            "instrument_revision": int(envelope.instrument_revision),
            "feed": feed.value,
            "interval": interval,
            "observed_at_ns": observed_at_ns,
            "received_at_ns": max(1, int(envelope.received_at_ns)),
            "revision": revision,
            "payload": payload,
            "source": {
                "venue": envelope.venue,
                "provider": envelope.provider,
                "source_id": envelope.source_id,
                "source_role": _enum_name(
                    common_pb2.SourceRole, envelope.source_role, "SOURCE_ROLE_"
                ),
                "authoritative": template.source.authoritative,
            },
            "quality": {
                "state": state,
                "freshness_ms": int(freshness_ms),
                "gap_open": gap_open,
                "complete": not gap_open,
                "execution_eligible": authoritative,
                "policy_id": requirement.source_policy_id,
                "flags": flags,
            },
            "contract": {
                **template.contract.model_dump(mode="json"),
                "normalizer_version": envelope.normalizer_version,
                "adapter_version": envelope.adapter_version,
                "authority_revision": int(envelope.authority_revision),
                "config_revision": int(envelope.config_revision),
                "correlation_id": envelope.correlation_id
                or bytes(envelope.event_id).hex(),
            },
            "cursor": event.resume_token,
            "watermark_offset": event.logical_offset,
        }
    )

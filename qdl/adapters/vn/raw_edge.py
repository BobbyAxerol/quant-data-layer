from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import capture_exact_frame


@dataclass(frozen=True)
class VnRawBinding:
    venue: str
    market: str
    product_type: str
    native_symbol: str
    subscription_id: str
    source_session_id: str
    connection_generation: int
    lease_epoch: int
    authority_revision: int
    partition_plan_epoch: int
    adapter_version: str
    config_revision: int
    instrument_catalog_revision: int

    def __post_init__(self) -> None:
        strings = (
            self.venue, self.market, self.product_type, self.native_symbol,
            self.subscription_id, self.source_session_id, self.adapter_version,
        )
        if any(not value.strip() for value in strings):
            raise ValueError("VN raw binding identity is incomplete")
        revisions = (
            self.connection_generation, self.lease_epoch, self.authority_revision,
            self.partition_plan_epoch, self.config_revision,
            self.instrument_catalog_revision,
        )
        if min(revisions) <= 0:
            raise ValueError("VN raw binding revisions/epochs must be positive")


def _decimal_text(value: Any, field: str) -> str:
    if value is None or isinstance(value, bool):
        raise ValueError(f"DNSE {field} is missing or invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"DNSE {field} is invalid") from error
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"DNSE {field} must be positive")
    return format(parsed, "f")


def build_dnse_trade_raw_envelope(
    delivery: Mapping[str, Any],
    binding: VnRawBinding,
    *,
    received_at_ns: int,
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    symbol = str(delivery.get("symbol") or "").upper()
    if symbol != binding.native_symbol.upper():
        raise ValueError("DNSE delivery symbol does not match approved binding")
    raw = {
        "symbol": symbol,
        "price": _decimal_text(delivery.get("price"), "price"),
        "quantity": _decimal_text(delivery.get("quantity"), "quantity"),
        "market_id": str(delivery.get("market_id") or delivery.get("marketId") or ""),
        "board_id": str(delivery.get("board_id") or delivery.get("boardId") or ""),
        "trading_session_id": str(
            delivery.get("trading_session_id")
            or delivery.get("tradingSessionId")
            or ""
        ),
        "total_volume_traded": _decimal_text(
            delivery.get("total_volume_traded")
            or delivery.get("totalVolumeTraded")
            or delivery.get("total_volume"),
            "total_volume_traded",
        ),
    }
    raw_bytes = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return capture_exact_frame(
        provider="DNSE_DIRECT",
        venue=binding.venue,
        market=binding.market,
        product_type=binding.product_type,
        native_symbol=binding.native_symbol,
        native_channel="trades",
        subscription_id=binding.subscription_id,
        source_session_id=binding.source_session_id,
        connection_generation=binding.connection_generation,
        lease_epoch=binding.lease_epoch,
        authority_revision=binding.authority_revision,
        partition_plan_epoch=binding.partition_plan_epoch,
        received_at_ns=received_at_ns,
        raw_frame_bytes=raw_bytes,
        adapter_version=binding.adapter_version,
        config_revision=binding.config_revision,
        instrument_catalog_revision=binding.instrument_catalog_revision,
        correlation_id=f"dnse:{binding.native_symbol}:{received_at_ns}",
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_SDK_CALLBACK,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_SDK_DELIVERY,
        test_provenance=test_provenance,
    )


def build_dnse_bar_raw_envelope(
    row: Mapping[str, Any],
    binding: VnRawBinding,
    *,
    received_at_ns: int,
    interval: str = "1m",
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    if interval != "1m":
        raise ValueError("stable DNSE BAR edge currently supports native 1m only")
    try:
        open_time_ms = int(row["t"]) * 1000
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("DNSE BAR timestamp is invalid") from error
    if open_time_ms <= 0:
        raise ValueError("DNSE BAR timestamp must be positive")
    values = {}
    for field in ("o", "h", "l", "c"):
        values[field] = _decimal_text(row.get(field), field)
    try:
        volume = Decimal(str(row.get("v")))
    except (InvalidOperation, ValueError, TypeError) as error:
        raise ValueError("DNSE BAR volume is invalid") from error
    if not volume.is_finite() or volume < 0:
        raise ValueError("DNSE BAR volume must be non-negative")
    raw = {
        "symbol": binding.native_symbol.upper(),
        "interval": interval,
        "open_time_ms": open_time_ms,
        "close_time_ms": open_time_ms + 59_999,
        **values,
        "v": format(volume, "f"),
        "is_final": True,
        "revision": 0,
        "trade_count_available": False,
    }
    raw_bytes = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return capture_exact_frame(
        provider="DNSE_DIRECT",
        venue=binding.venue,
        market=binding.market,
        product_type=binding.product_type,
        native_symbol=binding.native_symbol,
        native_channel="ohlcv/1m",
        subscription_id=binding.subscription_id,
        source_session_id=binding.source_session_id,
        connection_generation=binding.connection_generation,
        lease_epoch=binding.lease_epoch,
        authority_revision=binding.authority_revision,
        partition_plan_epoch=binding.partition_plan_epoch,
        received_at_ns=received_at_ns,
        raw_frame_bytes=raw_bytes,
        adapter_version=binding.adapter_version,
        config_revision=binding.config_revision,
        instrument_catalog_revision=binding.instrument_catalog_revision,
        correlation_id=f"dnse:{binding.native_symbol}:1m:{open_time_ms}",
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_HTTP,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
        test_provenance=test_provenance,
    )

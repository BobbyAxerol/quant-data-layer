from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Callable

from app.providers.binance.rest import BinanceProviderError, fetch_klines
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import capture_exact_frame


@dataclass(frozen=True)
class BinanceBarRawBinding:
    market: str
    product_type: str
    native_symbol: str
    interval: str
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
        if self.market not in {"USDM", "SPOT"}:
            raise ValueError("Binance bar market must be USDM or SPOT")
        if self.product_type not in {"PERPETUAL", "SPOT"}:
            raise ValueError("Binance bar product type is invalid")
        if self.market == "USDM" and self.product_type != "PERPETUAL":
            raise ValueError("Binance USDM bar requires PERPETUAL product")
        if self.market == "SPOT" and self.product_type != "SPOT":
            raise ValueError("Binance Spot bar requires SPOT product")
        strings = (
            self.native_symbol, self.interval, self.subscription_id,
            self.source_session_id, self.adapter_version,
        )
        if any(not value.strip() for value in strings):
            raise ValueError("Binance bar binding identity is incomplete")
        if min(
            self.connection_generation,
            self.lease_epoch,
            self.authority_revision,
            self.partition_plan_epoch,
            self.config_revision,
            self.instrument_catalog_revision,
        ) <= 0:
            raise ValueError("Binance bar binding revisions/epochs must be positive")


def fetch_latest_closed_bar_raw_envelope(
    binding: BinanceBarRawBinding,
    *,
    now_ms: int | None = None,
    attempts: int = 4,
    fetcher: Callable = fetch_klines,
    sleep: Callable[[float], None] = time.sleep,
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    if attempts < 1 or attempts > 10:
        raise ValueError("Binance bar attempts must be between 1 and 10")
    observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    last_error: BaseException | None = None
    response = None
    for attempt in range(attempts):
        try:
            response = fetcher(
                binding.native_symbol,
                interval=binding.interval,
                limit=3,
                end_time=observed_ms,
                market=binding.market.lower(),
            )
            break
        except (BinanceProviderError, OSError, ValueError) as error:
            last_error = error
            if attempt + 1 < attempts:
                sleep(min(2**attempt, 4) + random.random() * 0.25)
    if response is None:
        raise RuntimeError("Binance latest-closed bar fetch exhausted retries") from last_error
    rows = response.get("data")
    if not isinstance(rows, list):
        raise ValueError("Binance kline response data must be a list")
    closed = [
        row for row in rows
        if isinstance(row, list) and len(row) >= 11 and int(row[6]) < observed_ms
    ]
    if not closed:
        raise RuntimeError("Binance returned no closed bar before the observation time")
    row = max(closed, key=lambda value: int(value[0]))
    raw = {
        "symbol": binding.native_symbol.upper(),
        "interval": binding.interval,
        "bar_origin": "VENUE_NATIVE",
        "row": row,
    }
    raw_bytes = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    received_at_ns = time.time_ns()
    return capture_exact_frame(
        provider="BINANCE_DIRECT",
        venue="BINANCE",
        market=binding.market,
        product_type=binding.product_type,
        native_symbol=binding.native_symbol.upper(),
        native_channel=f"rest-klines/{binding.interval}",
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
        correlation_id=(
            f"binance:{binding.market}:{binding.native_symbol}:"
            f"{binding.interval}:{row[0]}"
        ),
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_HTTP,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
        test_provenance=test_provenance,
    )

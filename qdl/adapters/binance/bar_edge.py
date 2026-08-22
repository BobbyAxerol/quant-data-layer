from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from typing import Callable

from app.providers.binance.rest import BinanceProviderError, fetch_klines
from qdl.adapters.intervals import canonical_interval_ms
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


def _interval_ms(interval: str) -> int:
    # Duration arithmetic is shared; the venue guard stays local because
    # Binance REST klines expose no sub-minute bar. Weekly is a real Binance
    # interval and was rejected here until a live certification run hit it.
    if not interval or interval[-1] not in {"m", "h", "d", "w"}:
        raise ValueError("Binance history interval must have a fixed m/h/d/w duration")
    return canonical_interval_ms(interval)


def _fetch_rows(
    binding: BinanceBarRawBinding,
    *,
    observed_ms: int,
    limit: int,
    attempts: int,
    fetcher: Callable,
    sleep: Callable[[float], None],
) -> list[list]:
    if attempts < 1 or attempts > 10:
        raise ValueError("Binance bar attempts must be between 1 and 10")
    last_error: BaseException | None = None
    response = None
    for attempt in range(attempts):
        try:
            response = fetcher(
                binding.native_symbol,
                interval=binding.interval,
                limit=limit,
                end_time=observed_ms,
                market=binding.market.lower(),
            )
            break
        except (BinanceProviderError, OSError, ValueError) as error:
            last_error = error
            if attempt + 1 < attempts:
                sleep(min(2**attempt, 4) + random.random() * 0.25)
    if response is None:
        raise RuntimeError("Binance closed-bar fetch exhausted retries") from last_error
    rows = response.get("data")
    if not isinstance(rows, list):
        raise ValueError("Binance kline response data must be a list")
    return rows


def _capture_row(
    binding: BinanceBarRawBinding,
    row: list,
    *,
    origin: str,
    received_at_ns: int,
    test_provenance: bool,
) -> raw_provider_pb2.RawProviderEnvelope:
    raw = {
        "symbol": binding.native_symbol.upper(),
        "interval": binding.interval,
        "bar_origin": origin,
        "row": row,
    }
    raw_bytes = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
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


def _closed_rows(
    rows: list[list],
    *,
    observed_ms: int,
    interval_ms: int,
) -> tuple[list, ...]:
    by_open: dict[int, list] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < 11:
            raise ValueError("Binance kline row has invalid native shape")
        open_time = int(row[0])
        close_time = int(row[6])
        if close_time >= observed_ms:
            continue
        if close_time != open_time + interval_ms - 1:
            raise ValueError("Binance kline boundary differs from requested interval")
        previous = by_open.get(open_time)
        if previous is not None and previous != row:
            raise ValueError("Binance returned conflicting rows for one open time")
        by_open[open_time] = row
    return tuple(by_open[key] for key in sorted(by_open))


def fetch_closed_bar_history_raw_envelopes(
    binding: BinanceBarRawBinding,
    *,
    limit: int,
    now_ms: int | None = None,
    attempts: int = 4,
    fetcher: Callable = fetch_klines,
    sleep: Callable[[float], None] = time.sleep,
    test_provenance: bool = False,
) -> tuple[raw_provider_pb2.RawProviderEnvelope, ...]:
    if limit < 1 or limit > 1000:
        raise ValueError("Binance history limit must be between 1 and 1000")
    observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    interval_ms = _interval_ms(binding.interval)
    rows = _fetch_rows(
        binding,
        observed_ms=observed_ms,
        limit=min(1000, limit + 2),
        attempts=attempts,
        fetcher=fetcher,
        sleep=sleep,
    )
    closed = _closed_rows(rows, observed_ms=observed_ms, interval_ms=interval_ms)
    selected = closed[-limit:]
    if len(selected) != limit:
        raise RuntimeError(
            f"Binance closed-bar history is incomplete requested={limit} observed={len(selected)}"
        )
    opens = [int(row[0]) for row in selected]
    if any(current - previous != interval_ms for previous, current in zip(opens, opens[1:])):
        raise RuntimeError("Binance closed-bar history contains a time gap")
    received_at_ns = time.time_ns()
    return tuple(
        _capture_row(
            binding,
            row,
            origin="BACKFILLED",
            received_at_ns=received_at_ns + index,
            test_provenance=test_provenance,
        )
        for index, row in enumerate(selected)
    )


def fetch_latest_closed_bar_raw_envelope(
    binding: BinanceBarRawBinding,
    *,
    now_ms: int | None = None,
    attempts: int = 4,
    fetcher: Callable = fetch_klines,
    sleep: Callable[[float], None] = time.sleep,
    test_provenance: bool = False,
) -> raw_provider_pb2.RawProviderEnvelope:
    observed_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    interval_ms = _interval_ms(binding.interval)
    rows = _fetch_rows(
        binding,
        observed_ms=observed_ms,
        limit=3,
        attempts=attempts,
        fetcher=fetcher,
        sleep=sleep,
    )
    closed = _closed_rows(rows, observed_ms=observed_ms, interval_ms=interval_ms)
    if not closed:
        raise RuntimeError("Binance returned no closed bar before the observation time")
    return _capture_row(
        binding,
        closed[-1],
        origin="VENUE_NATIVE",
        received_at_ns=time.time_ns(),
        test_provenance=test_provenance,
    )

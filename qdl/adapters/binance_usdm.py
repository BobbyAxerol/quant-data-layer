from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import requests

from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentStatus,
    ProductType,
)
from qdl.ingestion.contracts import ConnectionShard, FeedType


BINANCE_USDM_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_USDM_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_USDM_WS_BASE = "wss://fstream.binance.com/stream?streams="


@dataclass(frozen=True)
class BinanceDiscovery:
    records: tuple[InstrumentRecord, ...]
    aliases: tuple[InstrumentAlias, ...]
    server_time_ms: int


def _filter_value(payload: Mapping[str, Any], filter_type: str, field: str) -> str:
    for item in payload.get("filters", []):
        if item.get("filterType") == filter_type and item.get(field) not in (None, ""):
            return str(item[field])
    raise ValueError(f"Binance exchangeInfo missing {filter_type}.{field}")


def parse_exchange_info(payload: Mapping[str, Any], *, valid_from_ns: int) -> BinanceDiscovery:
    records: list[InstrumentRecord] = []
    aliases: list[InstrumentAlias] = []
    for item in payload.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        contract_type = str(item.get("contractType") or "")
        if contract_type not in {"PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"}:
            continue
        native_symbol = str(item.get("symbol") or "").upper()
        if not native_symbol:
            raise ValueError("Binance exchangeInfo contains an empty symbol")
        product_type = ProductType.PERPETUAL if contract_type == "PERPETUAL" else ProductType.FUTURE
        identity = InstrumentIdentity.create(
            venue="BINANCE",
            market="USDM",
            product_type=product_type,
            canonical_symbol=native_symbol,
        )
        expiry_ms = int(item.get("deliveryDate") or 0)
        record = InstrumentRecord(
            identity=identity,
            metadata_revision=1,
            asset_class=AssetClass.DERIVATIVE,
            native_symbol=native_symbol,
            base_asset=str(item.get("baseAsset") or "").upper(),
            quote_asset=str(item.get("quoteAsset") or "").upper(),
            settlement_asset=str(item.get("marginAsset") or "").upper(),
            price_tick=CanonicalDecimal.from_text(_filter_value(item, "PRICE_FILTER", "tickSize")),
            quantity_step=CanonicalDecimal.from_text(_filter_value(item, "LOT_SIZE", "stepSize")),
            contract_multiplier=CanonicalDecimal.from_text("1"),
            session_calendar_id="CRYPTO_24X7",
            status=InstrumentStatus.ACTIVE,
            expiry_time_ns=expiry_ms * 1_000_000 if product_type is ProductType.FUTURE else None,
            valid_from_ns=valid_from_ns,
            attributes={"contractType": contract_type},
        )
        records.append(record)
        aliases.append(
            InstrumentAlias(
                provider="BINANCE_DIRECT",
                market="USDM",
                native_symbol=native_symbol,
                instrument_uid=record.instrument_uid,
                instrument_revision=record.metadata_revision,
                valid_from_ns=valid_from_ns,
            )
        )
    if not records:
        raise ValueError("Binance exchangeInfo returned no active USD-M instruments")
    return BinanceDiscovery(tuple(records), tuple(aliases), int(payload.get("serverTime") or 0))


async def discover_instruments(
    *, timeout_seconds: float = 10.0, attempts: int = 3
) -> BinanceDiscovery:
    def request() -> Mapping[str, Any]:
        response = requests.get(BINANCE_USDM_EXCHANGE_INFO, timeout=timeout_seconds)
        response.raise_for_status()
        decoded = response.json()
        if not isinstance(decoded, dict):
            raise ValueError("Binance exchangeInfo response is not an object")
        return decoded

    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            received_at_ns = time.time_ns()
            return parse_exchange_info(await asyncio.to_thread(request), valid_from_ns=received_at_ns)
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(min(2**attempt, 4) + random.random() * 0.25)
    raise RuntimeError("Binance USD-M instrument discovery exhausted retries") from last_error


async def fetch_klines(
    symbol: str, interval: str, *, limit: int = 3, timeout_seconds: float = 10.0
) -> list[list[Any]]:
    if limit < 1 or limit > 1500:
        raise ValueError("Binance kline limit must be between 1 and 1500")
    response = await asyncio.to_thread(
        requests.get,
        BINANCE_USDM_KLINES,
        params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list) or any(not isinstance(row, list) or len(row) < 11 for row in rows):
        raise ValueError("Binance kline response has invalid native row shape")
    return rows


def stream_name(feed: FeedType, symbol: str, interval: str | None = None) -> str:
    native = symbol.lower()
    if feed is FeedType.TRADE:
        return f"{native}@trade"
    if feed is FeedType.BBO:
        return f"{native}@bookTicker"
    if feed is FeedType.BAR and interval:
        return f"{native}@kline_{interval}"
    raise ValueError(f"unsupported Binance USD-M stream: {feed.value}")


def websocket_url(shard: ConnectionShard, active_symbols: set[str]) -> str:
    streams: list[str] = []
    for item in shard.subscriptions:
        if item.native_symbol.upper() not in active_symbols:
            raise ValueError(f"demanded Binance instrument is not active: {item.native_symbol}")
        streams.append(stream_name(item.feed, item.native_symbol, item.interval))
    if not streams:
        raise ValueError("a Binance shard cannot be empty")
    return BINANCE_USDM_WS_BASE + "/".join(streams)


def decode_combined_frame(message: str | bytes) -> tuple[str, dict[str, Any]]:
    payload = json.loads(message)
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise ValueError("Binance combined frame requires data object")
    stream = str(payload.get("stream") or "")
    data = payload["data"]
    if not stream or not data.get("s"):
        raise ValueError("Binance combined frame is missing stream/symbol")
    return stream, data


class BinanceUsdmSupervisor:
    """Bounded, demand-scoped real WebSocket reader; publication is callback-owned."""

    def __init__(
        self,
        *,
        on_frame: Callable[[str, dict[str, Any], int], Awaitable[None]],
        connect_timeout_seconds: float = 10.0,
        heartbeat_seconds: float = 20.0,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self._on_frame = on_frame
        self._connect_timeout = connect_timeout_seconds
        self._heartbeat = heartbeat_seconds
        self._max_backoff = max_backoff_seconds

    async def run(
        self,
        shard: ConnectionShard,
        *,
        active_symbols: set[str],
        stop: asyncio.Event,
        max_events: int | None = None,
    ) -> int:
        from websockets.asyncio.client import connect

        url = websocket_url(shard, active_symbols)
        received = 0
        failures = 0
        while not stop.is_set() and (max_events is None or received < max_events):
            try:
                async with connect(
                    url,
                    open_timeout=self._connect_timeout,
                    ping_interval=self._heartbeat,
                    ping_timeout=self._heartbeat,
                    max_queue=16,
                ) as socket:
                    failures = 0
                    while not stop.is_set() and (max_events is None or received < max_events):
                        message = await asyncio.wait_for(socket.recv(), timeout=self._heartbeat * 2)
                        stream, frame = decode_combined_frame(message)
                        await self._on_frame(stream, frame, time.time_ns())
                        received += 1
            except asyncio.CancelledError:
                raise
            except (TimeoutError, OSError):
                failures += 1
                if stop.is_set():
                    break
                delay = min(self._max_backoff, 0.5 * 2 ** min(failures, 8))
                await asyncio.sleep(delay + random.random() * delay * 0.2)
        return received

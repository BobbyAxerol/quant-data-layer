import asyncio
import json
import logging
import traceback
import time
import random
import os

import requests
from websockets.asyncio.client import connect as websocket_connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK, InvalidStatus

from app.stream.binance_ws import get_usdm_symbols
from app.stream.feed_builder import build_urls, validate_symbols
from app.stream.feed_parsers import PARSERS
from app.stream.supervisor import StreamSupervisor
from app.config import (
    BINANCE_SPOT_SYMBOLS_FILE,
    BINANCE_WS_BATCH_SIZE,
    BINANCE_WS_MAX_CONNS_PER_SOURCE,
    BINANCE_WS_QUEUE_MAXSIZE,
    BINANCE_WS_FIRST_FRAME_TIMEOUT_SECONDS,
    BINANCE_WS_IDLE_TIMEOUT_SECONDS,
    BINANCE_WS_QUEUE_PUT_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)

def get_spot_symbols(
    file_path: str = BINANCE_SPOT_SYMBOLS_FILE,
    *,
    refresh: bool = False,
) -> list[str]:
    cached_symbols = None
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                cached_symbols = json.load(f)
            if not refresh:
                logger.info(f"Loaded {len(cached_symbols)} spot symbols from {file_path}")
                return cached_symbols
        except Exception as exc:
            logger.warning(f"Failed to load spot symbols from {file_path}: {exc}")

    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
        symbols = [
            item["symbol"]
            for item in payload.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("isSpotTradingAllowed", True)
        ]
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(symbols, f)
            logger.info(f"Fetched and cached {len(symbols)} spot symbols from Binance API")
        except Exception as exc:
            logger.warning(f"Failed to cache spot symbols to {file_path}: {exc}")
        return symbols
    except Exception as exc:
        if cached_symbols:
            logger.warning(f"Failed to refresh Binance spot exchangeInfo, using last good cache: {exc}")
            return cached_symbols
        logger.warning(f"Failed to fetch Binance spot exchangeInfo, using static fallback symbols: {exc}")
        return ["BTCUSDT", "ETHUSDT"]

# Parser routing helper
def get_parser_key(source: str):
    s = (source or "").lower()
    if s.startswith("binance"):
        return "binance"
    if s.startswith("dnse"):
        return "dnse"
    return None


def coalesce_redis_items(items: list[dict]) -> list[dict]:
    """Keep only the latest event per Redis key/channel within a publisher batch."""
    coalesced: dict[tuple[str, str, str | None], dict] = {}
    order: list[tuple[str, str, str | None]] = []
    for item in items:
        dedupe_key = (item.get("key"), item.get("channel"), item.get("coalesce_id"))
        if dedupe_key not in coalesced:
            order.append(dedupe_key)
        coalesced[dedupe_key] = item
    return [coalesced[key] for key in order if key in coalesced]


def _source_market_namespace(source: str) -> str | None:
    value = str(source or "").lower()
    if value.startswith("binance_futures"):
        return "binance_usdm"
    if value.startswith("binance_spot"):
        return "binance_spot"
    return None


class StreamDataTimeout(RuntimeError):
    """A connected websocket failed to produce valid provider data in time."""


class StreamBackpressureTimeout(RuntimeError):
    """The bounded publisher queue could not accept a provider event in time."""


def valid_provider_frame(source: str, item: object, interval: str = "1m") -> bool:
    """Return true only for a complete frame belonging to the configured feed."""
    if not isinstance(item, dict):
        return False
    if source.endswith("_trade"):
        required = ("s", "p", "q", "t", "T")
        return item.get("e") == "trade" and all(item.get(field) is not None for field in required)
    if source.endswith("_kline"):
        kline = item.get("k")
        required = ("s", "i", "t", "T", "o", "h", "l", "c", "v", "x")
        return (
            item.get("e") == "kline"
            and isinstance(kline, dict)
            and str(kline.get("i")) == interval
            and all(kline.get(field) is not None for field in required)
        )
    return False


def provider_items(source: str, payload: object, interval: str = "1m") -> list[dict]:
    """Extract validated data frames while ignoring subscription ACKs."""
    if not isinstance(payload, dict):
        raise ValueError("websocket payload must be an object")
    if "data" not in payload and "id" in payload and "result" in payload:
        return []
    data = payload.get("data", payload)
    items = data if isinstance(data, list) else [data]
    if not items or not all(valid_provider_frame(source, item, interval) for item in items):
        raise ValueError(f"invalid or wrong-feed provider frame for {source}")
    return items


async def _put_provider_item(
    queue: asyncio.Queue,
    item: tuple[str, dict],
    *,
    supervisor: StreamSupervisor,
    timeout_seconds: float,
) -> None:
    try:
        queue.put_nowait(item)
    except asyncio.QueueFull:
        supervisor.record_queue_pressure()
        try:
            await asyncio.wait_for(queue.put(item), timeout=max(0.05, timeout_seconds))
        except asyncio.TimeoutError as exc:
            raise StreamBackpressureTimeout(
                f"publisher queue remained full for {timeout_seconds:.3f}s"
            ) from exc


async def handle_ws(
    url: str,
    queue: asyncio.Queue,
    source: str,
    shard_id: str,
    supervisor: StreamSupervisor,
    reconnect_delay: int = 5,
    parser_mode: str = "raw",
    max_backoff: int = 300,
    demand_registry=None,
    interval: str = "1m",
    first_frame_timeout_seconds: float = BINANCE_WS_FIRST_FRAME_TIMEOUT_SECONDS,
    idle_timeout_seconds: float = BINANCE_WS_IDLE_TIMEOUT_SECONDS,
    queue_put_timeout_seconds: float = BINANCE_WS_QUEUE_PUT_TIMEOUT_SECONDS,
):
    """Receive valid provider frames with bounded readiness and backpressure."""
    del demand_registry  # Kept in the public signature for V1 caller compatibility.
    backoff = reconnect_delay
    last_connected_at = None
    while True:
        try:
            supervisor.mark_connecting(shard_id)
            logger.info("[WS] Connecting %s -> %s...", source, url[:60])
            async with websocket_connect(url, ping_interval=30, max_size=None) as ws:
                logger.info("[WS] Connected %s", source)
                supervisor.mark_connected(shard_id)
                last_connected_at = time.monotonic()
                session_has_valid_frame = False

                while True:
                    timeout = (
                        idle_timeout_seconds
                        if session_has_valid_frame
                        else first_frame_timeout_seconds
                    )
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=max(0.05, timeout))
                    except asyncio.TimeoutError as exc:
                        reason = "idle" if session_has_valid_frame else "first_frame"
                        supervisor.mark_data_timeout(shard_id, reason)
                        raise StreamDataTimeout(
                            f"{source} {reason} timeout after {timeout:.3f}s"
                        ) from exc

                    try:
                        raw_items = provider_items(source, json.loads(msg), interval)
                        if not raw_items:
                            continue
                        supervisor.mark_message(shard_id)
                        session_has_valid_frame = True
                        backoff = reconnect_delay

                        for data in raw_items:
                            output = data
                            if parser_mode == "unified":
                                parser = PARSERS.get(get_parser_key(source) or "")
                                if parser:
                                    parsed = parser(data)
                                    if parsed is None:
                                        continue
                                    output = parsed
                            items = output if isinstance(output, list) else [output]
                            for item in items:
                                await _put_provider_item(
                                    queue,
                                    (source, item),
                                    supervisor=supervisor,
                                    timeout_seconds=queue_put_timeout_seconds,
                                )
                    except StreamBackpressureTimeout:
                        supervisor.mark_data_timeout(shard_id, "publisher_backpressure")
                        raise
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        supervisor.mark_parse_error(shard_id, exc)
                        logger.warning("[WS] rejected provider frame source=%s error=%s", source, exc)
        except InvalidStatus as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            if status == 429:
                backoff = min(max(backoff * 2, reconnect_delay * 2), max_backoff)
            else:
                backoff = min(backoff + 5, max_backoff)
            if elapsed >= 60:
                backoff = reconnect_delay
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, exc)
            logger.error(
                "[WS] connection rejected (%s): HTTP %s. Reconnecting in %ss...",
                source,
                status,
                int(sleep_for),
            )
            await asyncio.sleep(sleep_for)
        except (ConnectionClosedError, ConnectionClosedOK) as exc:
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            backoff = reconnect_delay if elapsed >= 60 else min(backoff + 5, max_backoff)
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, exc)
            logger.error(
                "[WS] connection closed (%s): %s. Reconnecting in %ss...",
                source,
                exc,
                int(sleep_for),
            )
            await asyncio.sleep(sleep_for)
        except Exception as exc:
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            backoff = reconnect_delay if elapsed >= 60 else min(backoff + 5, max_backoff)
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, exc)
            logger.error(
                "[WS] connection error (%s): %s. Reconnecting in %ss...",
                source,
                exc,
                int(sleep_for),
            )
            await asyncio.sleep(sleep_for)


async def redis_publisher_task(
    queue: asyncio.Queue,
    redis_cache,
    interval: str = "1m",
    supervisor: StreamSupervisor | None = None,
):
    """
    Consumes the asyncio queue and pushes batches to Redis.
    Uses async Redis pipeline for high throughput.
    """
    logger.info("[Publisher] Started Redis batch publisher task")
    batch_size_limit = 50
    batch_timeout = 0.05
    
    while True:
        batch = []
        start_time = time.monotonic()
        if supervisor:
            supervisor.record_queue_size(queue.qsize(), queue.maxsize)
        
        try:
            item = await asyncio.wait_for(queue.get(), timeout=batch_timeout)
            batch.append(item)
            queue.task_done()
            
            # Try to grab more up to batch_size_limit
            while len(batch) < batch_size_limit and (time.monotonic() - start_time) < batch_timeout:
                try:
                    item = queue.get_nowait()
                    batch.append(item)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
        except asyncio.TimeoutError:
            pass # Just timeout, process what we have if any
        except asyncio.CancelledError:
            break
            
        if batch:
            redis_items = []
            for source, data in batch:
                if source.endswith("_trade"):
                    sym = str(data.get("s", "")).upper() if isinstance(data, dict) else ""
                    if not sym:
                        continue

                    event_ts = data.get("T") or data.get("E") or 0
                    trade_price = data.get("p")
                    trade_qty = data.get("q")
                    side = "buy" if not data.get("m", False) else "sell"
                    payload = {
                        "symbol": sym,
                        "price": float(trade_price) if trade_price is not None else None,
                        "quantity": float(trade_qty) if trade_qty is not None else None,
                        "trade_id": data.get("t"),
                        "event_time": event_ts,
                        "trade_time": data.get("T"),
                        "side": side,
                        "source": source,
                        "provider": "binance",
                        "market": _source_market_namespace(source),
                        "authoritative": True,
                        "is_live": True,
                        "raw": data,
                    }
                    market = _source_market_namespace(source)
                    if market:
                        redis_items.append(
                            {
                                "key": f"trade:price:{market}:{sym}",
                                "channel": f"stream:trade:{market}:{sym}",
                                "data": payload,
                            }
                        )
                    key = f"trade:price:{sym}"
                    channel = f"stream:trade:{sym}"
                    redis_items.append({"key": key, "channel": channel, "data": payload})

                elif source.startswith("binance"):
                    # Extract raw data to maintain legacy execution service compatibility
                    # Data is either raw 'k' or unified containing 'raw'
                    raw_data = data.get("raw") if isinstance(data, dict) and "raw" in data else data
                    # Determine symbol
                    sym = ""
                    if isinstance(raw_data, dict):
                        sym = raw_data.get("s", "")
                    elif isinstance(data, dict):
                        sym = data.get("symbol", "")
                        
                    if not sym:
                        continue
                        
                    sym = sym.upper()
                    kline = raw_data.get("k") if isinstance(raw_data, dict) else None
                    event_interval = str((kline or {}).get("i") or interval)
                    key = f"kline:{event_interval}:{sym}"
                    channel = f"stream:kline:{event_interval}:{sym}"
                    redis_item = {
                        "key": key,
                        "channel": channel,
                        "data": raw_data,
                        "source": source,
                    }
                    if isinstance(raw_data, dict) and raw_data.get("recovery_source"):
                        kline = raw_data.get("k") or {}
                        redis_item["coalesce_id"] = f"recovery:{kline.get('t')}"
                    redis_items.append(redis_item)
                    
                elif source == "dnse":
                    sym = data.get("symbol", "")
                    if sym:
                        sym = sym.upper()
                        key = f"vn:quote:{sym}"
                        channel = f"stream:vn:{sym}"
                        redis_items.append({"key": key, "channel": channel, "data": data})

            if redis_items:
                redis_items = coalesce_redis_items(redis_items)
                try:
                    await redis_cache.push_batch(redis_items)
                    if supervisor:
                        for item in redis_items:
                            supervisor.record_publish(item)
                        supervisor.record_batch_published(len(redis_items))
                except Exception as e:
                    if supervisor:
                        supervisor.record_redis_error(e)
                    logger.error(f"[Publisher] Redis push error: {e}")

async def start_stream(
    redis_cache,
    interval: str = "1m",
    parser_mode: str = "raw",
    enabled_sources: list = ["binance_spot"],
    reconnect_delay: int = 15,
    max_backoff: int = 300,
    max_conns_per_source: int | None = None,
    supervisor: StreamSupervisor | None = None,
    demand_registry=None,
    queue: asyncio.Queue | None = None,
):
    """
    Main entrypoint used by services.
    """
    logger.info(f"Starting unified WS streams for: {enabled_sources}")
    supervisor = supervisor or StreamSupervisor()
    max_conns_per_source = (
        BINANCE_WS_MAX_CONNS_PER_SOURCE
        if max_conns_per_source is None
        else max_conns_per_source
    )
    
    # 1. Setup Queue and Publisher
    queue = queue if queue is not None else asyncio.Queue(maxsize=BINANCE_WS_QUEUE_MAXSIZE)
    supervisor.record_queue_size(queue.qsize(), queue.maxsize)
    publisher_task = asyncio.create_task(redis_publisher_task(queue, redis_cache, interval, supervisor))
    
    # 2. Get Symbols
    symbols_by_source = {}
    needs_spot = any(source.startswith("binance_spot") for source in enabled_sources)
    needs_usdm = any(source.startswith("binance_futures") for source in enabled_sources)
    spot_symbols, usdm_symbols = await asyncio.gather(
        asyncio.to_thread(get_spot_symbols, refresh=True)
        if needs_spot else asyncio.sleep(0, result=[]),
        # Execution demand may include active USD-M delivery contracts.  The
        # stream universe must therefore include every currently tradable USD-M
        # contract, not only PERPETUAL symbols.
        asyncio.to_thread(get_usdm_symbols, contract_type=None, refresh=True)
        if needs_usdm else asyncio.sleep(0, result=[]),
    )
    
    for source in enabled_sources:
        if source.startswith("binance"):
            # Use source-specific universes. Futures and spot do not have the
            # same listed symbols, and expecting futures-only symbols on spot
            # makes health degrade even when the stream is healthy.
            try:
                raw_symbols = spot_symbols if source.startswith("binance_spot") else usdm_symbols
                syms = validate_symbols(raw_symbols)
                symbols_by_source[source] = syms
                feed = "trade" if source.endswith("_trade") else "kline"
                for sym in syms:
                    supervisor.expect_feed(source, feed, sym, None if feed == "trade" else interval)
            except Exception as e:
                logger.error(f"Failed to load Binance symbols: {e}")
                symbols_by_source[source] = ["BTCUSDT", "ETHUSDT"]
                feed = "trade" if source.endswith("_trade") else "kline"
                for sym in symbols_by_source[source]:
                    supervisor.expect_feed(source, feed, sym, None if feed == "trade" else interval)
        
        # DNSE could be added here if we migrate it from its SDK. 
        # For now, DNSE still runs via its own SDK loop but can push to this same `redis_cache`.

    # 3. Build URLs
    urls_by_source = build_urls(symbols_by_source, interval=interval, batch_size=BINANCE_WS_BATCH_SIZE)

    # 4. Cap connections
    for source, urls in urls_by_source.items():
        if max_conns_per_source and len(urls) > max_conns_per_source:
            logger.warning(f"[start_stream] Capping {source} connections from {len(urls)} to {max_conns_per_source}")
            urls_by_source[source] = urls[:max_conns_per_source]

    # 5. Create WS tasks
    tasks = [publisher_task]
    for source, urls in urls_by_source.items():
        for idx, u in enumerate(urls):
            if not u:
                continue
            shard_id = supervisor.register_shard(source, u, shard_id=f"{source}:{idx}")
            tasks.append(asyncio.create_task(
                handle_ws(
                    u, queue, source, shard_id, supervisor,
                    parser_mode=parser_mode,
                    reconnect_delay=reconnect_delay,
                    max_backoff=max_backoff,
                    demand_registry=demand_registry,
                    interval=interval,
                )
            ))

    if len(tasks) <= 1: # Only publisher_task exists
        logger.warning("[start_stream] No websocket URLs to connect. Publisher will still run.")
    else:
        logger.info(f"[start_stream] Starting {len(tasks)-1} websocket tasks.")
        
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("[start_stream] Cancelled, exiting streams.")
    except Exception as e:
        logger.error(f"[start_stream] Unexpected error: {e}\n{traceback.format_exc()}")

import asyncio
import json
import logging
import traceback
import time
import random
import os
import math
import re

import requests
import websockets
from websockets.exceptions import InvalidStatusCode, ConnectionClosedError, ConnectionClosedOK

from app.stream.binance_ws import get_usdm_symbols
from app.stream.feed_builder import build_urls, validate_symbols
from app.stream.feed_parsers import PARSERS
from app.stream.supervisor import StreamSupervisor
from app.stream.demand_registry import feed_key_for
from app.providers.binance import rest as binance_rest
from app.config import (
    BINANCE_SPOT_SYMBOLS_FILE,
    BINANCE_WS_BATCH_SIZE,
    BINANCE_WS_MAX_CONNS_PER_SOURCE,
    BINANCE_WS_QUEUE_MAXSIZE,
)

logger = logging.getLogger(__name__)

_STREAM_SYMBOL_RE = re.compile(r"[=/]([a-z0-9_]+)@(kline_[^/]+|trade)")


def symbols_from_stream_url(url: str) -> list[str]:
    return [match.group(1).upper() for match in _STREAM_SYMBOL_RE.finditer(url)]


def _interval_seconds(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    return value * {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 60)


def _rest_kline_event(symbol: str, interval: str, row: list) -> dict:
    return {
        "e": "kline_recovery",
        "E": int(time.time() * 1000),
        "s": symbol,
        "k": {
            "t": row[0], "T": row[6], "s": symbol, "i": interval,
            "o": row[1], "c": row[4], "h": row[2], "l": row[3], "v": row[5], "x": True,
        },
        "recovery_source": "BINANCE_REST_GAP_FILL",
    }


async def recover_demanded_kline_gap(
    *,
    source: str,
    url: str,
    interval: str,
    queue: asyncio.Queue,
    supervisor: StreamSupervisor,
    shard_id: str,
    demand_registry,
) -> int:
    if source.endswith("_trade") or "kline" not in url:
        return 0
    active = await demand_registry.snapshot()
    demanded = set(active["feed_keys"])
    symbols = [
        symbol for symbol in symbols_from_stream_url(url)
        if feed_key_for(source, "kline", symbol, interval) in demanded
    ]
    if not symbols:
        return 0
    supervisor.record_gap_detected(shard_id)
    shard = supervisor.shards.get(shard_id)
    outage_seconds = float(shard.last_outage_seconds or 0) if shard else 0.0
    limit = min(1000, max(3, math.ceil(outage_seconds / max(1, _interval_seconds(interval))) + 2))
    market = "usdm" if source.startswith("binance_futures") else "spot"
    recovered = 0
    now_ms = int(time.time() * 1000)
    try:
        for symbol in symbols:
            payload = await asyncio.to_thread(
                binance_rest.fetch_klines,
                symbol,
                interval,
                limit,
                None,
                None,
                market,
            )
            for row in payload.get("data") or []:
                if len(row) <= 6 or int(row[6]) > now_ms:
                    continue
                await queue.put((source, _rest_kline_event(symbol, interval, row)))
                recovered += 1
        supervisor.record_gap_fill(shard_id, success=True)
        return recovered
    except Exception:
        supervisor.record_gap_fill(shard_id, success=False)
        logger.exception("[WS] demanded kline gap-fill failed source=%s shard=%s", source, shard_id)
        return 0


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
    coalesced: dict[tuple[str, str], dict] = {}
    order: list[tuple[str, str]] = []
    for item in items:
        dedupe_key = (item.get("key"), item.get("channel"))
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
):
    """
    Connect and stream messages from websocket. Reconnects on error.
    parser_mode: "raw" (default) forwards original payload; "unified" forwards parsed data.
    """
    backoff = reconnect_delay
    last_connected_at = None
    while True:
        try:
            supervisor.mark_connecting(shard_id)
            logger.info(f"[WS] Connecting {source} -> {url[:60]}...")
            async with websockets.connect(url, ping_interval=30, max_size=None) as ws:
                logger.info(f"[WS] Connected {source}")
                recovered = supervisor.mark_connected(shard_id)
                last_connected_at = time.monotonic()
                if recovered and demand_registry is not None:
                    filled = await recover_demanded_kline_gap(
                        source=source,
                        url=url,
                        interval=interval,
                        queue=queue,
                        supervisor=supervisor,
                        shard_id=shard_id,
                        demand_registry=demand_registry,
                    )
                    if filled:
                        logger.info(
                            "[WS] demanded kline gap-fill complete source=%s shard=%s rows=%s",
                            source,
                            shard_id,
                            filled,
                        )

                async for msg in ws:
                    try:
                        supervisor.mark_message(shard_id)
                        payload = json.loads(msg)
                        data = payload.get("data") or payload.get("result") or payload
                        if not data:
                            continue

                        output = data
                        if parser_mode == "unified":
                            parser_key = get_parser_key(source)
                            if parser_key:
                                parser = PARSERS.get(parser_key)
                                if parser:
                                    parsed = parser(data)
                                    if parsed is None:
                                        continue
                                    output = parsed

                        items = output if isinstance(output, list) else [output]
                        for item in items:
                            # We put to the queue for the Redis publisher
                            # Structure: (source, item)
                            try:
                                queue.put_nowait((source, item))
                            except asyncio.QueueFull:
                                # Pop one to make room
                                try:
                                    queue.get_nowait()
                                    queue.task_done()
                                except asyncio.QueueEmpty:
                                    pass
                                supervisor.record_queue_drop(source, shard_id)
                                queue.put_nowait((source, item))
                                
                    except Exception as e:
                        supervisor.mark_parse_error(shard_id, e)
                        logger.error(f"[WS] parse error: {e}")
                        
        except InvalidStatusCode as e:
            status = getattr(e, "status_code", None)
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            if status == 429:
                backoff = min(max(backoff * 2, reconnect_delay * 2), max_backoff)
            else:
                backoff = min(backoff + 5, max_backoff)
            if elapsed >= 60:
                backoff = reconnect_delay
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, e)
            logger.error(f"[WS] connection rejected ({source}): HTTP {status}. Reconnecting in {int(sleep_for)}s...")
            await asyncio.sleep(sleep_for)
        except (ConnectionClosedError, ConnectionClosedOK) as e:
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            if elapsed >= 60:
                backoff = reconnect_delay
            else:
                backoff = min(backoff + 5, max_backoff)
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, e)
            logger.error(f"[WS] connection closed ({source}): {e}. Reconnecting in {int(sleep_for)}s...")
            await asyncio.sleep(sleep_for)
        except Exception as e:
            elapsed = (time.monotonic() - last_connected_at) if last_connected_at else 0
            if elapsed >= 60:
                backoff = reconnect_delay
            else:
                backoff = min(backoff + 5, max_backoff)
            sleep_for = backoff + random.uniform(0, min(5, backoff * 0.2))
            supervisor.mark_reconnect(shard_id, e)
            logger.error(f"[WS] connection error ({source}): {e}. Reconnecting in {int(sleep_for)}s...")
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
                    key = f"kline:{interval}:{sym}"
                    channel = f"stream:kline:{interval}:{sym}"
                    redis_items.append(
                        {"key": key, "channel": channel, "data": raw_data, "source": source}
                    )
                    
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
    queue = asyncio.Queue(maxsize=BINANCE_WS_QUEUE_MAXSIZE)
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

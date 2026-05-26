import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone
import time
import random
import os

import requests
import websockets
from websockets.exceptions import InvalidStatusCode, ConnectionClosedError, ConnectionClosedOK

from app.stream.binance_ws import get_usdm_symbols
from app.stream.feed_builder import build_urls, validate_symbols
from app.stream.feed_parsers import PARSERS
from app.stream.supervisor import StreamSupervisor
from app.config import (
    BINANCE_SPOT_SYMBOLS_FILE,
    BINANCE_WS_BATCH_SIZE,
    BINANCE_WS_MAX_CONNS_PER_SOURCE,
    BINANCE_WS_QUEUE_MAXSIZE,
)

logger = logging.getLogger(__name__)


def get_spot_symbols(file_path: str = BINANCE_SPOT_SYMBOLS_FILE) -> list[str]:
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                symbols = json.load(f)
            logger.info(f"Loaded {len(symbols)} spot symbols from {file_path}")
            return symbols
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

async def handle_ws(
    url: str,
    queue: asyncio.Queue,
    source: str,
    shard_id: str,
    supervisor: StreamSupervisor,
    reconnect_delay: int = 5,
    parser_mode: str = "raw",
    max_backoff: int = 300,
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
                supervisor.mark_connected(shard_id)
                last_connected_at = time.monotonic()

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
                        "raw": data,
                    }
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
                    redis_items.append({"key": key, "channel": channel, "data": raw_data})
                    
                elif source == "dnse":
                    sym = data.get("symbol", "")
                    if sym:
                        sym = sym.upper()
                        key = f"vn:quote:{sym}"
                        channel = f"stream:vn:{sym}"
                        redis_items.append({"key": key, "channel": channel, "data": data})

            if redis_items:
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
    
    for source in enabled_sources:
        if source.startswith("binance"):
            # Use source-specific universes. Futures and spot do not have the
            # same listed symbols, and expecting futures-only symbols on spot
            # makes health degrade even when the stream is healthy.
            try:
                raw_symbols = get_spot_symbols() if source.startswith("binance_spot") else get_usdm_symbols()
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

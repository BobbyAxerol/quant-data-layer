"""
data_layer FastAPI application.

This is the main entrypoint. It:
1. Boots the ultra-lean Redis cache
2. Starts the Binance WebSocket multiplexer (background daemon)
3. Starts the DNSE WebSocket (PRIMARY for VN stock - background daemon)
4. Starts the vnstock REST poller (FALLBACK for VN stock - background daemon)
5. Exposes async REST endpoints for other services to call:
   - GET /v1/health
   - GET /v1/binance/price/{symbol}
   - GET /v1/binance/kline/{symbol}
   - GET /v1/vn/quote/{symbol}
   - GET /v1/vn/board
   - GET /v1/preload/{symbol}    (read latest warmup candles from preloaded parquet)
   - POST /v1/preload/run         (trigger preload)
   - POST /v1/preload/append/{symbol} (append delta for 1 symbol)
"""
import os
import logging
import threading
import asyncio
from datetime import datetime, time, timedelta, timezone
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.config import PRELOAD_DIR
from app.cache.redis_cache import RedisCache
from app.stream.async_live_feed import start_stream
from app.stream.vnstock_poller import VnstockPoller
from app.stream.dnse_ws import DnseStreamManager
from app.database.preload import run_preload, update_symbol, load_last_preload_snapshot, load_vn_symbols
from app.logging_config import setup_logging

# Configure logging to write to /app/logs/app.log
setup_logging()
logger = logging.getLogger(__name__)

# ── Shared Instances ────────────────────────────────────────────
redis_cache = RedisCache()
dnse_stream_manager = None  # Will be initialized in lifespan
preload_thread = None
preload_stop_event = threading.Event()
unified_stream_task = None

# Vietnam market schedule (local time UTC+7)
VN_TZ = timezone(timedelta(hours=7))
VN_MARKET_MORNING_START = time(9, 0)
VN_MARKET_MORNING_END = time(11, 30)
VN_MARKET_AFTERNOON_START = time(13, 0)
VN_MARKET_AFTERNOON_END = time(14, 30)


def _vn_now() -> datetime:
    return datetime.now(tz=VN_TZ)


def _is_vn_market_open(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    local_time = now.time()
    return (
        VN_MARKET_MORNING_START <= local_time < VN_MARKET_MORNING_END
        or VN_MARKET_AFTERNOON_START <= local_time < VN_MARKET_AFTERNOON_END
    )


def _next_vn_market_open(now: datetime) -> datetime:
    current_time = now.time()
    next_open = now.replace(hour=9, minute=0, second=0, microsecond=0)

    if current_time < VN_MARKET_MORNING_START:
        return next_open
    if current_time < VN_MARKET_AFTERNOON_START:
        return now.replace(hour=13, minute=0, second=0, microsecond=0)

    next_day = now + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day.replace(hour=9, minute=0, second=0, microsecond=0)


def _preload_watchdog(symbols: list):
    logger.info("Preload watchdog starting")

    # Run an immediate boot preload before entering the loop.
    try:
        logger.info("Preload watchdog: initial run")
        run_preload(symbols)
    except Exception as e:
        logger.error(f"Preload watchdog initial run failed: {e}", exc_info=True)

    while not preload_stop_event.is_set():
        now = _vn_now()

        if _is_vn_market_open(now):
            try:
                logger.info("Preload watchdog: VN market open, updating preload data")
                run_preload(symbols)
            except Exception as e:
                logger.error(f"Preload watchdog update failed: {e}", exc_info=True)

            # During trading hours, refresh every 60 seconds to stay close to latest candle.
            if preload_stop_event.wait(60):
                break
            continue

        next_open = _next_vn_market_open(now)
        sleep_seconds = max(60, (next_open - now).total_seconds())
        logger.info(
            f"Preload watchdog: market closed, sleeping until {next_open.isoformat()} "
            f"({sleep_seconds / 60:.1f} minutes)"
        )
        if preload_stop_event.wait(sleep_seconds):
            break

    logger.info("Preload watchdog stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    On startup:
    - Init async Redis
    - Start the Unified WS multiplexer (Binance, IBIT, etc.)
    - Start the DNSE WebSocket (PRIMARY for VN stock)
    - Start the vnstock REST poller (FALLBACK only if DNSE stale)
    """
    global dnse_stream_manager, unified_stream_task
    logger.info("=== data_layer service starting ===")

    await redis_cache.init_ping()

    # 1. Unified stream (Binance)
    unified_stream_task = asyncio.create_task(
        start_stream(
            redis_cache, 
            interval="1m", 
            enabled_sources=[
                "binance_spot_trade",
                "binance_futures_trade",
                "binance_spot_kline",
                "binance_futures_kline",
            ]
        )
    )
    logger.info("Unified async Binance trade + kline streams started")

    # Load VN symbols early (needed for DNSE and vnstock)
    vn_symbols = load_vn_symbols()

    # Seed last-known VN snapshots from preload so quote-last is useful even
    # if the service boots after market hours before any live quote arrives.
    for symbol in vn_symbols:
        try:
            snapshot = load_last_preload_snapshot(symbol)
            if snapshot:
                redis_cache.set_vn_quote_last_sync(symbol, snapshot)
        except Exception as e:
            logger.warning(f"Failed to seed preload snapshot for {symbol}: {e}")

    # 2. DNSE WebSocket (PRIMARY for VN stock)
    if vn_symbols:
        try:
            dnse_stream_manager = DnseStreamManager(redis_cache, vn_symbols)
            dnse_stream_manager.start()
            logger.info(f"✓ DNSE stream started for {len(vn_symbols)} symbols (PRIMARY source)")
        except Exception as e:
            logger.error(f"✗ DNSE stream initialization failed: {e}", exc_info=True)
            logger.warning("Will use vnstock as primary fallback source")
            dnse_stream_manager = None
    else:
        logger.info("No VN symbols configured, skipping DNSE stream")

    # 3. vnstock poller (FALLBACK only if DNSE stale or unavailable)
    vn_poller = None
    if vn_symbols:
        vn_poller = VnstockPoller(redis_cache, vn_symbols)
        vn_poller.start()
        logger.info(f"vnstock fallback poller started for {len(vn_symbols)} symbols (SECONDARY source)")
    else:
        logger.info("No VN symbols configured, skipping vnstock poller")

    # 4. Preload watchdog thread
    global preload_thread
    preload_stop_event.clear()
    if vn_symbols:
        preload_thread = threading.Thread(target=_preload_watchdog, args=(vn_symbols,), daemon=True)
        preload_thread.start()
        logger.info("Preload watchdog started")
    else:
        logger.info("No VN symbols configured, skipping preload watchdog")

    yield

    # Shutdown
    logger.info("Shutting down services...")
    
    if unified_stream_task:
        unified_stream_task.cancel()
        
    if vn_poller:
        vn_poller.stop()
        logger.info("vnstock poller stopped")

    preload_stop_event.set()
    if preload_thread:
        preload_thread.join(timeout=10)
        logger.info("Preload watchdog joined")

    if dnse_stream_manager:
        try:
            await dnse_stream_manager.stop()
            logger.info("DNSE stream stopped")
        except Exception as e:
            logger.error(f"Error stopping DNSE stream: {e}")
    
    logger.info("=== data_layer service stopped ===")


app = FastAPI(
    title="data_layer",
    description="Centralized market data gateway for Binance + VN stock",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Health ──────────────────────────────────────────────────────

@app.get("/v1/health")
async def health():
    redis_ok = await redis_cache.health_check()
    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "binance_trade_stream": True,
        "binance_kline_stream": True,
    }


# ── Binance Endpoints (Tier A: Redis Cache) ────────────────────

@app.get("/v1/binance/price/{symbol}")
async def get_binance_price(symbol: str):
    """
    Get the latest cached Binance trade price for execution/papertrade use.
    This is the live price stream and is separate from kline/candle data.
    """
    data = await redis_cache.get_binance_price(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached trade price for {symbol}")
    return data

@app.get("/v1/binance/kline/{symbol}")
async def get_binance_kline(symbol: str, interval: str = "1m"):
    """
    Get the latest cached kline for a Binance symbol.
    Returns the single most recent event (1-event cache).
    """
    data = await redis_cache.get_binance_kline(symbol.upper(), interval)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached kline for {symbol} @ {interval}")
    return data


@app.get("/v1/binance/klines/{symbol}")
async def get_binance_klines(symbol: str, interval: str = "1m", limit: int = 500):
    """
    Proxy to fetch historical klines from Binance API.
    Used for warmup by other services to avoid direct external calls.
    """
    # Use fapi for futures, fallback to spot
    urls = [
        f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}",
        f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
    ]
    
    import requests
    for url in urls:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch klines from {url}: {e}")
            
    raise HTTPException(status_code=502, detail=f"Failed to fetch historical klines for {symbol} from Binance")


# ── VN Stock Endpoints (Tier A: Redis Cache) ───────────────────

@app.get("/v1/vn/quote/{symbol}")
async def get_vn_quote(symbol: str):
    """
    Get the latest live cached quote for a VN stock symbol.
    Returns only the short-TTL live event.
    """
    data = await redis_cache.get_vn_quote(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached quote for {symbol}")
    return data


@app.get("/v1/vn/quote-last/{symbol}")
async def get_vn_quote_last(symbol: str):
    """
    Get the latest known VN quote snapshot for a symbol.
    Unlike /v1/vn/quote/{symbol}, this may still exist after market close.
    """
    data = await redis_cache.get_vn_quote_last(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No last quote snapshot for {symbol}")
    live = await redis_cache.get_vn_quote(symbol.upper())
    return {
        "symbol": symbol.upper(),
        "is_live": live is not None,
        "snapshot": data,
    }


@app.get("/v1/vn/board")
async def get_vn_board():
    """Get the latest VN price board snapshot."""
    data = await redis_cache.get_vn_board()
    if data is None:
        raise HTTPException(status_code=404, detail="No cached VN board data")
    return data


# ── Preload Endpoints (Tier B: Parquet Disk) ───────────────────

@app.get("/v1/preload/status")
async def preload_status():
    """
    Check which symbols have preloaded data and their row counts.
    """
    preload_dir = PRELOAD_DIR
    if not os.path.exists(preload_dir):
        return {"symbols": []}

    result = []
    for f in os.listdir(preload_dir):
        if f.endswith(".parquet"):
            symbol = f.replace(".parquet", "")
            file_path = os.path.join(preload_dir, f)
            try:
                df = pd.read_parquet(file_path)
                df["time"] = pd.to_datetime(df["time"])
                first_local = df["time"].min() if not df.empty else None
                last_local = df["time"].max() if not df.empty else None
                first_utc = None
                last_utc = None
                if first_local is not None and last_local is not None:
                    try:
                        first_utc = pd.Timestamp(first_local).tz_localize("Asia/Ho_Chi_Minh").tz_convert("UTC")
                        last_utc = pd.Timestamp(last_local).tz_localize("Asia/Ho_Chi_Minh").tz_convert("UTC")
                    except Exception:
                        first_utc = None
                        last_utc = None
                result.append({
                    "symbol": symbol,
                    "rows": len(df),
                    "timezone_local": "Asia/Ho_Chi_Minh",
                    "first_local": str(first_local) if first_local is not None else None,
                    "last_local": str(last_local) if last_local is not None else None,
                    "first_utc": str(first_utc) if first_utc is not None else None,
                    "last_utc": str(last_utc) if last_utc is not None else None,
                })
            except Exception:
                result.append({"symbol": symbol, "rows": 0, "error": "corrupt"})

    return {"symbols": result}

@app.get("/v1/preload/{symbol}")
async def get_preload_data(
    symbol: str,
    limit: int = Query(1000, ge=1, le=20000, description="Latest N candles for warm-up lookback"),
):
    """
    Read the preloaded 1m parquet data for a VN stock symbol as a warm-up lookback.
    This endpoint is intentionally business-oriented:
    it returns the latest N candles backward from the newest available candle.
    It does not support arbitrary start/end date slicing.
    """
    file_path = os.path.join(PRELOAD_DIR, f"{symbol.upper()}.parquet")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"No preload data for {symbol}")

    df = pd.read_parquet(file_path)
    # Ensure time is datetime
    df["time"] = pd.to_datetime(df["time"])
    
    # VN market data from vnstock is in Asia/Ho_Chi_Minh.
    # Convert to UTC for consistency with Binance and internal stream.
    try:
        df["time"] = df["time"].dt.tz_localize("Asia/Ho_Chi_Minh").dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception as e:
        # Fallback if already localized or other issues
        logger.warning(f"Timezone conversion failed for {symbol}: {e}")

    df = df.sort_values("time").tail(limit)

    # Return as list of dicts for JSON serialization
    records = df.to_dict(orient="records")
    # Convert timestamps to ISO strings
    for r in records:
        if "time" in r:
            r["time"] = str(r["time"])
    return {"symbol": symbol.upper(), "count": len(records), "data": records}


@app.post("/v1/preload/run")
async def trigger_preload():
    """
    Trigger the preload process for all symbols in symbols_vn.yaml.
    Runs in a background thread so the API stays responsive.
    """
    def _bg():
        try:
            run_preload()
        except Exception as e:
            logger.error(f"Background preload failed: {e}")

    threading.Thread(target=_bg, daemon=True).start()
    return {"status": "preload_started", "message": "Preload running in background"}


@app.post("/v1/preload/append/{symbol}")
async def trigger_append(symbol: str):
    """
    Trigger an append-only delta update for a single symbol.
    Detects last_index_time from existing parquet and fetches only missing candles.
    """
    def _bg():
        try:
            update_symbol(symbol.upper())
        except Exception as e:
            logger.error(f"Append for {symbol} failed: {e}")

    threading.Thread(target=_bg, daemon=True).start()
    return {"status": "append_started", "symbol": symbol.upper()}

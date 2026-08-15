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

from fastapi import FastAPI, HTTPException

from app.config import (
    PRELOAD_DAILY_RUN_TIME,
    PRELOAD_DIR,
    STREAM_STALE_SECONDS,
    STREAM_STRICT_FEED_HEALTH,
)
from app.api.context import DataLayerContext
from app.api import (
    routes_binance_derivatives,
    routes_control_plane,
    routes_fallback,
    routes_health,
    routes_history,
    routes_latest,
    routes_preload,
)
from app.cache.redis_cache import RedisCache
from app.stream.async_live_feed import start_stream
from app.ingestion.supervisor import StreamSupervisor
from app.stream.vnstock_poller import VnstockPoller
from app.stream.dnse_ws import DnseStreamManager
from app.stream.demand_registry import FeedDemandRegistry
from app.history.topup_coordinator import PreloadTopupCoordinator
from app.history.preload_vn import (
    run_preload,
    load_last_preload_snapshot,
    load_vn_symbols,
    topup_existing_symbol_if_needed,
)
from app.providers.binance import rest as binance_rest
from app.providers.okx import rest as okx_rest
from app.logging_config import setup_logging
from app.runtime_source_config import RuntimeSourceConfig

# Configure logging to write to /app/logs/app.log
setup_logging()
logger = logging.getLogger(__name__)

# ── Shared Instances ────────────────────────────────────────────
redis_cache = RedisCache()
demand_registry = FeedDemandRegistry(redis_cache.r)
preload_topup_coordinator = PreloadTopupCoordinator(
    redis_cache.r,
    topup_existing_symbol_if_needed,
)
dnse_stream_manager = None  # Will be initialized in lifespan
preload_thread = None
preload_stop_event = threading.Event()
unified_stream_task = None
binance_stream_supervisor = StreamSupervisor(
    stale_after_seconds=STREAM_STALE_SECONDS,
    strict_feed_health=STREAM_STRICT_FEED_HEALTH,
)
preload_daily_state_dir = os.path.join(os.path.dirname(PRELOAD_DIR), "_state")

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


def _parse_daily_preload_time(value: str) -> time:
    try:
        hour, minute = str(value).split(":", 1)
        return time(int(hour), int(minute))
    except Exception:
        logger.warning("Invalid PRELOAD_DAILY_RUN_TIME=%s, falling back to 16:00", value)
        return time(16, 0)


def _daily_preload_marker(day) -> str:
    return os.path.join(preload_daily_state_dir, f"daily_preload_{day.isoformat()}.ok")


def _mark_daily_preload_done(day):
    os.makedirs(preload_daily_state_dir, exist_ok=True)
    with open(_daily_preload_marker(day), "w", encoding="utf-8") as f:
        f.write(datetime.now(tz=VN_TZ).isoformat())


def _daily_preload_done(day) -> bool:
    return os.path.exists(_daily_preload_marker(day))


def _next_daily_preload_run(now: datetime) -> datetime:
    run_time = _parse_daily_preload_time(PRELOAD_DAILY_RUN_TIME)
    candidate = now.replace(
        hour=run_time.hour,
        minute=run_time.minute,
        second=0,
        microsecond=0,
    )
    if now <= candidate:
        return candidate
    next_day = now + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    return next_day.replace(
        hour=run_time.hour,
        minute=run_time.minute,
        second=0,
        microsecond=0,
    )


def _normalize_binance_interval(interval: str) -> str:
    try:
        return binance_rest.normalize_interval(interval)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_binance_interval",
                "interval": interval,
                "supported": sorted(binance_rest.BINANCE_KLINE_INTERVALS),
            },
        )


def _normalize_okx_interval(interval: str) -> str:
    try:
        return okx_rest.normalize_interval(interval)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_okx_interval",
                "interval": interval,
                "supported": sorted(okx_rest.OKX_INTERVAL_ALIASES),
            },
        )


def _okx_symbol(symbol: str) -> str:
    return okx_rest.normalize_symbol(symbol)


def _binance_kline_urls(market: str) -> list[tuple[str, str]]:
    try:
        return binance_rest.kline_urls(market)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_binance_market",
                "market": str(market or "").lower().strip(),
                "supported": sorted(binance_rest.BINANCE_KLINE_URLS),
            },
        )


def _preload_watchdog(symbols: list):
    logger.info(
        "Preload watchdog starting: daily_run_time=%s, symbols=%s",
        PRELOAD_DAILY_RUN_TIME,
        len(symbols),
    )

    while not preload_stop_event.is_set():
        now = _vn_now()
        run_time = _parse_daily_preload_time(PRELOAD_DAILY_RUN_TIME)
        should_run_now = (
            now.weekday() < 5
            and now.time() >= run_time
            and not _daily_preload_done(now.date())
        )

        if should_run_now:
            try:
                logger.info(
                    "Preload watchdog: daily %s VN update starting for %s symbols",
                    PRELOAD_DAILY_RUN_TIME,
                    len(symbols),
                )
                run_preload(symbols)
                _mark_daily_preload_done(now.date())
                logger.info("Preload watchdog: daily update complete")
            except Exception as e:
                logger.error(f"Preload watchdog daily update failed: {e}", exc_info=True)
                if preload_stop_event.wait(300):
                    break
            continue

        next_run = _next_daily_preload_run(now)
        sleep_seconds = max(60, min(3600, (next_run - now).total_seconds()))
        logger.info(
            f"Preload watchdog: sleeping toward daily run at {next_run.isoformat()} "
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
    global dnse_stream_manager, unified_stream_task, binance_stream_supervisor
    logger.info("=== data_layer service starting ===")

    await redis_cache.init_ping()
    runtime_sources = RuntimeSourceConfig.from_env()
    logger.info("Runtime source configuration: %s", runtime_sources.public_summary())

    # 1. Unified stream (Binance)
    if runtime_sources.binance_sources:
        unified_stream_task = asyncio.create_task(
            start_stream(
                redis_cache,
                interval="1m",
                supervisor=binance_stream_supervisor,
                demand_registry=demand_registry,
                enabled_sources=list(runtime_sources.binance_sources),
            )
        )
        logger.info("Unified async Binance streams started: %s", runtime_sources.binance_sources)
    else:
        unified_stream_task = None
        logger.info("Binance live streams disabled by DATA_LAYER_BINANCE_SOURCES")

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
    if vn_symbols and runtime_sources.dnse_stream_enabled:
        try:
            dnse_stream_manager = DnseStreamManager(redis_cache, vn_symbols)
            dnse_stream_manager.start()
            logger.info(f"✓ DNSE stream started for {len(vn_symbols)} symbols (PRIMARY source)")
        except Exception as e:
            logger.error(f"✗ DNSE stream initialization failed: {e}", exc_info=True)
            logger.warning("Will use vnstock as primary fallback source")
            dnse_stream_manager = None
    else:
        logger.info("DNSE stream disabled or no VN symbols configured")

    # 3. vnstock poller (FALLBACK only if DNSE stale or unavailable)
    vn_poller = None
    if vn_symbols and runtime_sources.vnstock_poller_enabled:
        vn_poller = VnstockPoller(redis_cache, vn_symbols)
        vn_poller.start()
        logger.info(f"vnstock fallback poller started for {len(vn_symbols)} symbols (SECONDARY source)")
    else:
        logger.info("vnstock poller disabled or no VN symbols configured")

    # 4. Preload watchdog thread
    global preload_thread
    preload_stop_event.clear()
    if vn_symbols and runtime_sources.preload_watchdog_enabled:
        preload_thread = threading.Thread(target=_preload_watchdog, args=(vn_symbols,), daemon=True)
        preload_thread.start()
        logger.info("Preload watchdog started")
    else:
        logger.info("Preload watchdog disabled or no VN symbols configured")

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

app.state.context = DataLayerContext(
    redis_cache=redis_cache,
    binance_stream_supervisor=binance_stream_supervisor,
    get_dnse_stream_manager=lambda: dnse_stream_manager,
    demand_registry=demand_registry,
    preload_topup_coordinator=preload_topup_coordinator,
)

app.include_router(routes_health.router)
app.include_router(routes_latest.router)
app.include_router(routes_history.router)
app.include_router(routes_binance_derivatives.router)
app.include_router(routes_preload.router)
app.include_router(routes_control_plane.router)
app.include_router(routes_fallback.router)

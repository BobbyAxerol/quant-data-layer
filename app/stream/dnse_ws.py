"""
DNSE WebSocket streamer — PRIMARY real-time source for VN market trades & ticks.

Architecture (queue-decoupled, matching upgrade-redis-ws-streamming.md):
    DNSE WS → _on_trade() → queue.put_nowait() → Redis Worker → batch pipeline

Upgrades applied:
- Bounded queue between WS callback and Redis IO (no blocking WS thread)
- Redis batch pipeline (1 roundtrip per batch)
- orjson serialization
- Lightweight metrics (msg/sec, queue, latency, drops)
"""
import sys
import os
import asyncio
import time
import logging
from queue import Queue, Full, Empty
from time import perf_counter
import threading
from typing import Optional

# Add openapi_sdk to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "openapi_sdk", "python"))

from websocket_marketdata.trading_websocket import TradingClient
from websocket_marketdata.trading_websocket.models import Trade, Quote

from app.cache.redis_cache import RedisCache
from app.config import DNSE_API_KEY, DNSE_API_SECRET_KEY, DNSE_WS_BASE

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────
_QUEUE_MAXSIZE = 5_000
_BATCH_SIZE = 30
_BATCH_TIMEOUT = 0.05
_METRICS_INTERVAL = 60.0


class DnseStreamManager:
    """
    Primary WebSocket streamer for VN market data via DNSE OpenAPI.

    WS callback only does: extract fields → queue.put_nowait()
    Redis IO is fully decoupled in a separate worker thread.
    """

    def __init__(self, redis_cache: RedisCache, symbols: list):
        self.cache = redis_cache
        self.symbols = [s.upper() for s in symbols]
        self._client: Optional[TradingClient] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._queue: Queue = Queue(maxsize=_QUEUE_MAXSIZE)

        # Track last trade price per symbol for spread checking
        self._last_prices: dict = {}

        # Metrics
        self._ws_msg_count = 0
        self._redis_write_count = 0
        self._drop_count = 0
        self._last_redis_latency_ms = 0.0
        self._last_msg_ts = time.time()

    async def _connect_and_subscribe(self):
        """Connect, authenticate, subscribe to trades for all symbols."""
        if not DNSE_API_KEY or not DNSE_API_SECRET_KEY:
            logger.error("DNSE_API_KEY or DNSE_API_SECRET_KEY not set.")
            return False

        try:
            self._client = TradingClient(
                api_key=DNSE_API_KEY,
                api_secret=DNSE_API_SECRET_KEY,
                base_url=DNSE_WS_BASE,
                encoding="msgpack",
                auto_reconnect=True,
                max_retries=10,
                heartbeat_interval=25.0,
            )
            await self._client.connect()
            logger.info("DNSE WS connected and authenticated")

            # Subscribe to trades on HOSE (G1) and HNX (G3)
            await self._client.subscribe_trades(
                symbols=self.symbols,
                on_trade=self._on_trade,
                encoding="msgpack",
                board_id="G1",
            )
            await self._client.subscribe_trades(
                symbols=self.symbols,
                on_trade=self._on_trade,
                encoding="msgpack",
                board_id="G3",
            )
            logger.info(f"Subscribed to trades for {len(self.symbols)} symbols on G1+G3")
            return True
        except Exception as e:
            logger.error(f"DNSE WS connect failed: {e}")
            return False

    def _on_trade(self, trade: Trade):
        """
        WS callback — lightweight, non-blocking.
        Extracts fields and pushes to queue. Never touches Redis.
        """
        try:
            symbol = trade.symbol
            if not symbol:
                return

            self._ws_msg_count += 1
            self._last_msg_ts = time.time()

            price = float(trade.price) if trade.price else 0.0
            self._last_prices[symbol] = price

            tick_data = {
                "symbol": symbol,
                "price": price,
                "quantity": int(trade.quantity) if trade.quantity else 0,
                "total_volume": trade.totalVolumeTraded,
                "high": float(trade.highestPrice) if trade.highestPrice else 0.0,
                "low": float(trade.lowestPrice) if trade.lowestPrice else 0.0,
                "open": float(trade.openPrice) if trade.openPrice else 0.0,
                "source": "dnse",
                "timestamp": self._last_msg_ts,
            }

            try:
                self._queue.put_nowait(("vn", symbol, tick_data))
            except Full:
                try:
                    self._queue.get_nowait()
                except Empty:
                    pass
                self._queue.put_nowait(("vn", symbol, tick_data))
                self._drop_count += 1

        except Exception as e:
            logger.warning(f"DNSE trade callback error: {e}")

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._last_prices.get(symbol.upper())

    # ── Redis Worker (decoupled from WS) ────────────────────────

    def _redis_worker(self):
        """Drains queue in batches and writes to Redis via pipeline."""
        logger.info("DNSE Redis batch worker started")
        while self._running:
            batch = []
            try:
                first = self._queue.get(timeout=1.0)
                batch.append(first)
            except Empty:
                continue

            deadline = time.time() + _BATCH_TIMEOUT
            while len(batch) < _BATCH_SIZE and time.time() < deadline:
                try:
                    batch.append(self._queue.get_nowait())
                except Empty:
                    break

            # Deduplicate: keep latest per symbol
            latest = {}
            for source, symbol, item in batch:
                latest[(source, symbol)] = item

            t0 = perf_counter()
            try:
                pipeline_items = []
                for (source, symbol), item in latest.items():
                    key = f"vn:quote:{symbol}"
                    channel = f"stream:vn:{symbol}"
                    pipeline_items.append({"key": key, "channel": channel, "data": item})
                self.cache.push_batch_sync(pipeline_items)
                self._redis_write_count += len(latest)
            except Exception as e:
                logger.error(f"DNSE Redis batch write error: {e}")

            self._last_redis_latency_ms = (perf_counter() - t0) * 1000

    # ── Metrics Logger ──────────────────────────────────────────

    def _metrics_loop(self):
        prev_ws = 0
        prev_redis = 0
        while self._running:
            time.sleep(_METRICS_INTERVAL)
            ws_delta = self._ws_msg_count - prev_ws
            redis_delta = self._redis_write_count - prev_redis
            prev_ws = self._ws_msg_count
            prev_redis = self._redis_write_count

            ws_per_sec = ws_delta / _METRICS_INTERVAL
            redis_per_sec = redis_delta / _METRICS_INTERVAL
            stale_age = time.time() - self._last_msg_ts

            logger.info(
                f"[DNSE METRICS] "
                f"ws_msg/s={ws_per_sec:.1f} "
                f"redis_write/s={redis_per_sec:.1f} "
                f"queue={self._queue.qsize()} "
                f"redis_latency={self._last_redis_latency_ms:.1f}ms "
                f"stale_age={stale_age:.1f}s "
                f"dropped={self._drop_count}"
            )

    # ── Lifecycle ───────────────────────────────────────────────

    async def _run_loop(self):
        """Main async loop: connect, subscribe, monitor health."""
        retry_count = 0
        while self._running:
            connected = await self._connect_and_subscribe()
            if not connected:
                retry_count += 1
                logger.warning(f"DNSE WS connect failed (attempt {retry_count}). Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            retry_count = 0
            try:
                while self._running:
                    if self._client and self._client.is_healthy:
                        await asyncio.sleep(30)
                    else:
                        logger.warning("DNSE WS unhealthy, reconnecting...")
                        break
            except Exception as e:
                logger.error(f"DNSE stream loop error: {e}")

            try:
                if self._client:
                    await self._client.disconnect()
            except Exception:
                pass
            self._client = None

            if self._running:
                logger.info("DNSE WS restarting in 5s...")
                await asyncio.sleep(5)

    def start(self, loop: asyncio.AbstractEventLoop = None):
        """Start DNSE stream + Redis worker + metrics logger."""
        self._running = True

        # Redis worker thread (decoupled from async WS)
        threading.Thread(target=self._redis_worker, daemon=True, name="dnse-redis-worker").start()

        # Metrics thread
        threading.Thread(target=self._metrics_loop, daemon=True, name="dnse-metrics").start()

        # Async WS task
        if loop:
            self._task = loop.create_task(self._run_loop())
        else:
            self._task = asyncio.ensure_future(self._run_loop())
        logger.info(f"DNSE stream manager started for {len(self.symbols)} symbols")

    async def stop(self):
        self._running = False
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
        logger.info("DNSE stream manager stopped")

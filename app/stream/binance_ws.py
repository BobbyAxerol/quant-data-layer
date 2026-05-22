"""
Binance WebSocket multiplexer — upgraded with queue-decoupled Redis IO.

Architecture (from upgrade-redis-ws-streamming.md):
    Binance WS → _on_message() → queue.put_nowait() → Redis Worker → batch pipeline

Upgrades applied:
- Priority 1: Bounded queue between WS callback and Redis IO
- Priority 2: Redis pipeline batch (multiple events per execute)
- Priority 3: orjson for fast JSON parsing
- Priority 4: Lightweight metrics logging (msg/sec, queue, latency, drops)
"""
import orjson
import threading
import time
import os
import logging
import requests
import websocket
from queue import Queue, Full, Empty
from time import perf_counter

from app.cache.redis_cache import RedisCache
from app.config import BINANCE_WS_BATCH_SIZE, BINANCE_SYMBOLS_FILE

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────
_QUEUE_MAXSIZE = 10_000
_BATCH_SIZE = 50          # Drain up to N items per pipeline execute
_BATCH_TIMEOUT = 0.05     # Max seconds to wait for a batch to fill
_METRICS_INTERVAL = 60.0  # Log metrics every N seconds


# ── Symbol Discovery ────────────────────────────────────────────

def get_usdm_symbols(file_path: str = None, contract_type: str = "PERPETUAL") -> list:
    """Load symbols from local JSON cache, fallback to Binance exchangeInfo API."""
    file_path = file_path or BINANCE_SYMBOLS_FILE
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            import json
            symbols = json.load(f)
        logger.info(f"Loaded {len(symbols)} symbols from {file_path}")
        return symbols

    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10).json()
            symbols = [
                s["symbol"] for s in response["symbols"]
                if s["contractType"] == contract_type
                and s["status"] == "TRADING"
            ]
            with open(file_path, "w") as f:
                import json
                json.dump(symbols, f)
            logger.info(f"Fetched and cached {len(symbols)} symbols from Binance API")
            return symbols
        except requests.exceptions.RequestException as e:
            logger.warning(f"[get_usdm_symbols] Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    logger.warning("All retries failed, using static fallback symbols")
    return ["BTCUSDT", "ETHUSDT"]


def _build_urls(symbols: list, interval: str, batch_size: int) -> list:
    """
    Builds Binance multiplex stream URLs.
    Limit batch_size to avoid hitting URL length limits (approx 2048-4096 chars).
    """
    # Use Spot stream as fallback for VN region ISP blocking of fstream
    base = 'wss://stream.binance.com:9443/stream?streams='
    urls = []
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        stream_path = "/".join([f"{s.lower()}@kline_{interval}" for s in batch])
        urls.append(base + stream_path)
    return urls


def _run_ws(url: str, on_message, on_open=None, on_close=None, on_error=None):
    """Persistent WebSocket runner with auto-reconnect."""
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
            )
            ws.run_forever()
        except Exception as e:
            logger.warning(f"[run_ws] Reconnecting due to error: {e}")
            time.sleep(5)


# ── Stream Manager ──────────────────────────────────────────────

class BinanceStreamManager:
    """
    Multiplexes Binance WebSocket connections across all symbols.

    WS callback only does: orjson.loads → dict → queue.put_nowait()
    Redis IO is fully decoupled in a separate worker thread.
    """

    def __init__(self, redis_cache: RedisCache, interval: str = "1m"):
        self.cache = redis_cache
        self.interval = interval
        self.last_msg_ts = time.time()
        self._queue: Queue = Queue(maxsize=_QUEUE_MAXSIZE)

        # Metrics counters
        self._ws_msg_count = 0
        self._redis_write_count = 0
        self._drop_count = 0
        self._reconnect_count = 0
        self._last_redis_latency_ms = 0.0

    def start(self, symbols_filter: list = None, contract_type: str = "PERPETUAL"):
        """Start WS threads + Redis worker + metrics logger."""
        symbols = symbols_filter or get_usdm_symbols(contract_type=contract_type)
        urls = _build_urls(symbols, self.interval, BINANCE_WS_BATCH_SIZE)
        logger.info(
            f"Starting Binance WS multiplexer: {len(symbols)} symbols, "
            f"{len(urls)} connections, interval={self.interval}"
        )

        # 1. Start the Redis batch worker
        threading.Thread(target=self._redis_worker, daemon=True, name="redis-worker").start()

        # 2. Start the metrics logger
        threading.Thread(target=self._metrics_loop, daemon=True, name="metrics").start()

        # 3. Start WS connections
        def _on_message(ws, message):
            try:
                msg = orjson.loads(message)
                data = msg.get("data")
                if not data:
                    return
                self.last_msg_ts = time.time()
                self._ws_msg_count += 1

                kline = data.get("k")
                symbol = data.get("s", "")
                if symbol and kline:
                    item = {
                        "t": kline.get("t"),
                        "o": kline.get("o"),
                        "h": kline.get("h"),
                        "l": kline.get("l"),
                        "c": kline.get("c"),
                        "v": kline.get("v"),
                        "x": kline.get("x"),
                        "s": symbol,
                        "i": kline.get("i"),
                    }
                    try:
                        self._queue.put_nowait(("binance", symbol, item))
                    except Full:
                        # Drop oldest, keep newest — newest data > old data
                        try:
                            self._queue.get_nowait()
                        except Empty:
                            pass
                        self._queue.put_nowait(("binance", symbol, item))
                        self._drop_count += 1
            except Exception as e:
                logger.error(f"[on_message] Parse error: {e}")

        def _on_open(ws):
            logger.info(f"Binance WS connected [{self.interval}]")

        def _on_close(ws, code, msg):
            self._reconnect_count += 1
            logger.error(f"Binance WS closed [{self.interval}] code={code} msg={msg}")

        def _on_error(ws, error):
            logger.error(f"Binance WS error [{self.interval}] {error}")

        for url in urls:
            t = threading.Thread(
                target=_run_ws,
                args=(url, _on_message, _on_open, _on_close, _on_error),
                daemon=True,
            )
            t.start()
            time.sleep(0.2)

        logger.info(f"Binance stream multiplexer started for interval={self.interval}")

    # ── Redis Worker (decoupled from WS thread) ────────────────

    def _redis_worker(self):
        """
        Drains the queue in batches and writes to Redis via pipeline.
        Runs in its own thread — never blocks the WS callback.
        """
        logger.info("Redis batch worker started")
        while True:
            batch = []
            try:
                # Block on first item
                first = self._queue.get(timeout=1.0)
                batch.append(first)
            except Empty:
                continue

            # Drain up to _BATCH_SIZE more items without blocking
            deadline = time.time() + _BATCH_TIMEOUT
            while len(batch) < _BATCH_SIZE and time.time() < deadline:
                try:
                    batch.append(self._queue.get_nowait())
                except Empty:
                    break

            # Deduplicate: keep only the latest event per symbol
            latest = {}
            for source, symbol, item in batch:
                latest[(source, symbol)] = item

            # Execute pipeline
            t0 = perf_counter()
            try:
                pipeline_items = []
                for (source, symbol), item in latest.items():
                    key = f"kline:{self.interval}:{symbol}"
                    channel = f"stream:kline:{self.interval}:{symbol}"
                    pipeline_items.append({"key": key, "channel": channel, "data": item})
                self.cache.push_batch_sync(pipeline_items)
                self._redis_write_count += len(latest)
            except Exception as e:
                logger.error(f"Redis batch write error: {e}")

            self._last_redis_latency_ms = (perf_counter() - t0) * 1000

    # ── Metrics Logger ──────────────────────────────────────────

    def _metrics_loop(self):
        """Log lightweight metrics every _METRICS_INTERVAL seconds."""
        prev_ws = 0
        prev_redis = 0
        while True:
            time.sleep(_METRICS_INTERVAL)
            ws_delta = self._ws_msg_count - prev_ws
            redis_delta = self._redis_write_count - prev_redis
            prev_ws = self._ws_msg_count
            prev_redis = self._redis_write_count

            ws_per_sec = ws_delta / _METRICS_INTERVAL
            redis_per_sec = redis_delta / _METRICS_INTERVAL
            stale_age = time.time() - self.last_msg_ts

            logger.info(
                f"[BINANCE METRICS] "
                f"ws_msg/s={ws_per_sec:.1f} "
                f"redis_write/s={redis_per_sec:.1f} "
                f"queue={self._queue.qsize()} "
                f"redis_latency={self._last_redis_latency_ms:.1f}ms "
                f"reconnects={self._reconnect_count} "
                f"stale_age={stale_age:.1f}s "
                f"dropped={self._drop_count}"
            )

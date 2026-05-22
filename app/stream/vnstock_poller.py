"""
vnstock polling loop — FALLBACK price source for VN market.

Primary source is DNSE WebSocket. This poller:
1. Polls Trading.price_board every N seconds
2. Pushes to Redis ONLY if DNSE has not already pushed a fresher tick
3. Runs spread checks between DNSE and vnstock prices (minimal CPU)
"""
import time
import threading
import logging
import pandas as pd
import redis
from typing import Optional

from app.cache.redis_cache import RedisCache
from app.config import VNSTOCK_SOURCE, VNSTOCK_POLL_INTERVAL, VNSTOCK_API_KEY

logger = logging.getLogger(__name__)


class VnstockPoller:
    """
    REST-based polling loop for VN stock quotes.
    Acts as FALLBACK: only overwrites Redis if DNSE data is stale.
    """

    def __init__(
        self,
        redis_cache: RedisCache,
        symbols: list,
        source: str = None,
        price_manager=None,
    ):
        self.cache = redis_cache
        self.symbols = [s.upper() for s in symbols]
        self.source = source or VNSTOCK_SOURCE
        self.price_manager = price_manager
        self._running = False
        self._thread = None

    def start(self, poll_interval: float = None):
        """Start the background polling loop as a daemon thread."""
        interval = poll_interval or VNSTOCK_POLL_INTERVAL
        self._running = True

        def _poll():
            # Lazy import to avoid import errors if vnstock not in container
            from vnstock import Trading
            if VNSTOCK_API_KEY:
                try:
                    from vnstock import register_user
                    register_user(api_key=VNSTOCK_API_KEY)
                    logger.info("vnstock registered with API key (higher rate limits)")
                except Exception as e:
                    logger.warning(f"vnstock register_user failed: {e}")

            trading = Trading(source=self.source)
            logger.info(
                f"vnstock fallback poller started: {len(self.symbols)} symbols, "
                f"interval={interval}s, source={self.source}"
            )

            while self._running:
                try:
                    df = trading.price_board(self.symbols)
                    ts = time.time()

                    symbols_processed = 0
                    symbols_updated = 0
                    symbols_skipped = 0

                    for _, row in df.iterrows():
                        symbol = row.get("symbol", "")
                        if not symbol:
                            continue

                        symbols_processed += 1
                        close_price = _safe_float(row.get("close_price"))

                        # Check if DNSE already has a fresh tick (< 60s)
                        existing = self.cache.get_vn_quote_sync(symbol)
                        if existing and existing.get("source") == "dnse":
                            existing_ts = existing.get("timestamp", 0)
                            if ts - existing_ts < 60:
                                # DNSE is fresh, skip overwrite but still check spread
                                symbols_skipped += 1
                                if self.price_manager and close_price > 0:
                                    self.price_manager.check_spread(symbol, close_price)
                                continue

                        # DNSE stale or missing — push vnstock as fallback
                        symbols_updated += 1
                        quote = {
                            "symbol": symbol,
                            "price": close_price,
                            "bid1": _safe_float(row.get("bid_price_1")),
                            "ask1": _safe_float(row.get("ask_price_1")),
                            "volume": _safe_float(row.get("volume_accumulated")),
                            "source": "vnstock",
                            "timestamp": ts,
                        }
                        self.cache.push_vn_quote_sync(symbol, quote)

                        # Spread check
                        if self.price_manager and close_price > 0:
                            self.price_manager.check_spread(symbol, close_price)

                    # Log poll metrics
                    logger.debug(
                        f"vnstock poll: {symbols_processed} symbols, "
                        f"{symbols_updated} updated, {symbols_skipped} skipped (DNSE fresh)"
                    )

                except redis.ConnectionError as e:
                    logger.error(f"vnstock poll ERROR: Redis connection failed: {e}")
                    logger.error(f"  This indicates network/DNS issue with redis_service")
                except Exception as e:
                    logger.error(f"vnstock poll ERROR: {type(e).__name__}: {e}", exc_info=True)

                time.sleep(interval)

        self._thread = threading.Thread(target=_poll, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False


def _safe_float(val) -> float:
    """Safely convert a value to float, returning 0.0 on failure."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0

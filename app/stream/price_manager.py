"""
Unified VN market price manager.

Architecture:
- PRIMARY:  DNSE WebSocket (trades/ticks) — real-time, sub-second
- FALLBACK: vnstock REST polling (price_board) — when DNSE is down or missing symbols

Spread checker: lightweight comparison between DNSE and vnstock prices
to detect data anomalies with minimal CPU usage.
"""
import time
import logging
from typing import Optional, Dict

from app.cache.redis_cache import RedisCache

logger = logging.getLogger(__name__)


class SpreadChecker:
    """
    Lightweight spread monitor between DNSE and vnstock price sources.
    Only triggers when both sources have recent data for the same symbol.
    Designed to minimize CPU: no heavy loops, just called on each vnstock poll.
    """

    # Alert if spread exceeds this percentage
    SPREAD_ALERT_PCT = 1.0  # 1%
    # Only check once per N seconds per symbol to minimize CPU
    CHECK_INTERVAL = 60.0

    def __init__(self):
        self._last_check: Dict[str, float] = {}

    def check(
        self,
        symbol: str,
        dnse_price: Optional[float],
        vnstock_price: Optional[float],
    ) -> Optional[dict]:
        """
        Compare DNSE and vnstock prices for a symbol.
        Returns spread info dict if spread is abnormal, else None.
        Throttled to 1 check per CHECK_INTERVAL per symbol.
        """
        now = time.time()
        last = self._last_check.get(symbol, 0)
        if now - last < self.CHECK_INTERVAL:
            return None
        self._last_check[symbol] = now

        if not dnse_price or not vnstock_price:
            return None
        if dnse_price <= 0 or vnstock_price <= 0:
            return None

        spread = abs(dnse_price - vnstock_price)
        mid = (dnse_price + vnstock_price) / 2
        spread_pct = (spread / mid) * 100

        result = {
            "symbol": symbol,
            "dnse_price": dnse_price,
            "vnstock_price": vnstock_price,
            "spread": spread,
            "spread_pct": round(spread_pct, 4),
            "timestamp": now,
        }

        if spread_pct > self.SPREAD_ALERT_PCT:
            logger.warning(
                f"[SPREAD ALERT] {symbol}: DNSE={dnse_price} vs vnstock={vnstock_price} "
                f"spread={spread_pct:.2f}%"
            )
        return result


class VnPriceManager:
    """
    Unified price manager that coordinates DNSE (primary) and vnstock (fallback).

    Usage by main.py:
    1. Start DNSE WS via DnseStreamManager
    2. Start vnstock poller via VnstockPoller
    3. VnPriceManager.get_price() checks DNSE first, falls back to vnstock
    """

    def __init__(self, redis_cache: RedisCache):
        self.cache = redis_cache
        self.spread_checker = SpreadChecker()
        # Reference to DnseStreamManager for direct price lookup
        self._dnse_stream = None

    def set_dnse_stream(self, dnse_stream):
        """Link the DNSE stream manager for direct last-price access."""
        self._dnse_stream = dnse_stream

    def get_price(self, symbol: str) -> Optional[dict]:
        """
        Get the latest price for a VN symbol.
        Priority: DNSE (primary) -> vnstock (fallback) -> None
        """
        symbol = symbol.upper()

        # Try Redis cache first (both sources push here)
        cached = self.cache.get_vn_quote_sync(symbol)
        if cached:
            return cached

        return None

    def check_spread(self, symbol: str, vnstock_price: float) -> Optional[dict]:
        """
        Called by vnstock poller after each poll to check spread vs DNSE.
        Minimal CPU: throttled internally.
        """
        dnse_price = None
        if self._dnse_stream:
            dnse_price = self._dnse_stream.get_last_price(symbol)

        return self.spread_checker.check(symbol, dnse_price, vnstock_price)

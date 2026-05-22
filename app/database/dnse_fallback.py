"""
DNSE OHLC fallback fetcher for preloading.

Used ONLY when vnstock preload/storage fails with unknown issues.
This fetches data DIRECTLY from DNSE REST API — no disk storage.
Returns a DataFrame in memory for immediate consumption.

Limitations: DNSE only offers ~87 days lookback for 1m data.
Rate limits: 1,000 req/hour, 10,000 req/day.
"""
import os
import sys
import json
import time
import uuid
import random
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import deque

# Add openapi_sdk to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "openapi_sdk", "python"))

from dnse import DNSEClient
from dnse.common import build_signature

from app.config import DNSE_API_KEY, DNSE_API_SECRET_KEY, DNSE_REST_BASE

logger = logging.getLogger(__name__)


# ── Rate Limiter (sliding window) ───────────────────────────────

class OHLCRateLimiter:
    """
    Token bucket rate limiter for DNSE /price/ohlc endpoint.
    1,000 req/hour, 10,000 req/day.
    Uses 90% safety margin.
    """
    SAFE_HOURLY = 900
    SAFE_DAILY = 9000
    MIN_DELAY = 3600 / 900  # ~4.0s between requests

    def __init__(self):
        self._hourly_window: deque = deque()
        self._daily_window: deque = deque()
        self._last_request_time: float = 0.0

    def _cleanup(self):
        now = time.time()
        while self._hourly_window and self._hourly_window[0] < now - 3600:
            self._hourly_window.popleft()
        while self._daily_window and self._daily_window[0] < now - 86400:
            self._daily_window.popleft()

    def wait_if_needed(self):
        self._cleanup()
        now = time.time()

        # Minimum delay
        elapsed = now - self._last_request_time
        if elapsed < self.MIN_DELAY:
            time.sleep(self.MIN_DELAY - elapsed)

        # Daily quota
        if len(self._daily_window) >= self.SAFE_DAILY:
            oldest = self._daily_window[0]
            wait = max(0, oldest + 86400 - time.time())
            logger.warning(f"DNSE daily quota reached. Waiting {wait/3600:.1f}h...")
            time.sleep(wait + 1)
            self._cleanup()

        # Hourly limit
        if len(self._hourly_window) >= self.SAFE_HOURLY:
            oldest = self._hourly_window[0]
            wait = max(0, oldest + 3600 - time.time())
            logger.warning(f"DNSE hourly limit reached. Waiting {wait:.1f}s...")
            time.sleep(wait + 1)
            self._cleanup()

    def record(self):
        now = time.time()
        self._hourly_window.append(now)
        self._daily_window.append(now)
        self._last_request_time = now


# ── Singleton rate limiter ──────────────────────────────────────
_rate_limiter = OHLCRateLimiter()

# Symbols that require bar_type='DERIVATIVE'
DERIVATIVE_SYMBOLS = {"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"}


def _get_bar_type(symbol: str) -> str:
    return "DERIVATIVE" if symbol.upper() in DERIVATIVE_SYMBOLS else "STOCK"


def _to_unix(date_str: str) -> int:
    """Convert 'YYYY-MM-DD' to Unix timestamp (seconds, UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _fetch_ohlc_raw(symbol: str, resolution: str, from_ts: int, to_ts: int) -> list:
    """
    Fetch OHLC from DNSE REST using X-Aux-Date header + pagination.
    Returns list of dicts [{t, o, h, l, c, v}, ...].
    Uses raw requests because DNSEClient.get_ohlc() uses Date header
    but /price/ohlc requires X-Aux-Date.
    """
    import requests as req

    bar_type = _get_bar_type(symbol)
    path = "/price/ohlc"
    rows = []
    current_from = from_ts

    while current_from < to_ts:
        _rate_limiter.wait_if_needed()

        date_value = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
        nonce = uuid.uuid4().hex
        headers_list, sig = build_signature(
            DNSE_API_SECRET_KEY, "GET", path, date_value,
            algorithm="hmac-sha256", nonce=nonce, header_name="X-Aux-Date",
        )
        r = req.get(
            f"{DNSE_REST_BASE}{path}",
            params={
                "symbol": symbol, "type": bar_type,
                "resolution": resolution,
                "from": str(current_from), "to": str(to_ts),
            },
            headers={
                "X-API-Key": DNSE_API_KEY,
                "X-Aux-Date": date_value,
                "X-Signature": (
                    f'Signature keyId="{DNSE_API_KEY}",algorithm="hmac-sha256",'
                    f'headers="{headers_list}",signature="{sig}",nonce="{nonce}"'
                ),
                "Accept": "application/json",
            },
            timeout=30,
        )
        _rate_limiter.record()

        if r.status_code == 429:
            wait = 60 + random.uniform(0, 30)
            logger.warning(f"DNSE 429 for {symbol}. Backoff {wait:.0f}s...")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            logger.warning(f"DNSE HTTP {r.status_code} for {symbol}: {r.text[:200]}")
            break

        data = r.json()
        t_arr = data.get("t") or []
        for i, t in enumerate(t_arr):
            rows.append({
                "t": t,
                "o": data["o"][i],
                "h": data["h"][i],
                "l": data["l"][i],
                "c": data["c"][i],
                "v": data["v"][i],
            })

        next_time = data.get("nextTime", 0)
        if next_time == 0 or next_time >= to_ts:
            break
        current_from = next_time

    return rows


def fetch_dnse_ohlcv_direct(
    symbol: str,
    start: str,
    end: str,
    resolution: str = "1",
    chunk_days: int = 7,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch OHLCV directly from DNSE REST — NO disk storage.
    Used as fallback when vnstock preload fails.

    Args:
        symbol: Stock symbol (e.g., 'VCB') or derivative ('VN30F1M')
        start: Start date 'YYYY-MM-DD'
        end: End date 'YYYY-MM-DD'
        resolution: '1' for 1m, '5' for 5m, etc. (1 3 5 15 30 1H 1D 1W) 
        chunk_days: Days per chunk to avoid timeout
        max_retries: Retry count per chunk

    Returns:
        DataFrame with columns [time, open, high, low, close, volume, symbol]
        This is returned in-memory, NOT written to disk.
    """
    if not DNSE_API_KEY or not DNSE_API_SECRET_KEY:
        logger.error("DNSE credentials not set. Cannot use DNSE fallback.")
        return pd.DataFrame()

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    all_rows = []
    current_start = start_dt

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)
        from_ts = _to_unix(current_start.strftime("%Y-%m-%d"))
        to_ts = _to_unix(current_end.strftime("%Y-%m-%d"))

        for attempt in range(max_retries):
            try:
                rows = _fetch_ohlc_raw(symbol, resolution, from_ts, to_ts)
                if rows:
                    all_rows.extend(rows)
                    logger.info(
                        f"[DNSE-fallback] {symbol} {current_start.date()}->{current_end.date()}: "
                        f"{len(rows)} bars"
                    )
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.warning(f"[DNSE-fallback] {symbol} chunk failed: {e}")
                else:
                    time.sleep(2 ** attempt + random.uniform(0, 1))

        current_start = current_end + timedelta(days=1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # Rename columns to standard format
    df = df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    # Convert Unix timestamp to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="s")
    df["symbol"] = symbol
    df = df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    logger.info(f"[DNSE-fallback] {symbol}: {len(df)} total bars fetched (in-memory, no storage)")
    return df

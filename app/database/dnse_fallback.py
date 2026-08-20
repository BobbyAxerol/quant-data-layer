"""Bounded DNSE OHLC read-through fallback for legacy preload consumers."""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.config import DNSE_API_KEY, DNSE_API_SECRET_KEY
from app.providers.dnse import fetch_dnse_ohlc_raw


logger = logging.getLogger(__name__)

# Compatibility export used by the V1 preload/materialization policy.
DERIVATIVE_SYMBOLS = {"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"}


def _to_unix(date_str: str) -> int:
    value = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _fetch_ohlc_raw(
    symbol: str, resolution: str, from_ts: int, to_ts: int
) -> list[dict]:
    """Compatibility signature backed by the strict versioned DNSE provider."""
    return fetch_dnse_ohlc_raw(symbol, resolution, from_ts, to_ts)


def fetch_dnse_ohlcv_direct(
    symbol: str,
    start: str,
    end: str,
    resolution: str = "1",
    chunk_days: int = 7,
    max_retries: int = 3,
) -> pd.DataFrame:
    """Fetch bounded DNSE OHLCV in memory without writing provider data to disk."""
    if not DNSE_API_KEY or not DNSE_API_SECRET_KEY:
        logger.error("DNSE credentials not set; direct OHLC fallback unavailable")
        return pd.DataFrame()
    if chunk_days < 1 or not 1 <= max_retries <= 8:
        raise ValueError("DNSE fallback chunk/retry bounds are invalid")

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    # End is an inclusive trading date; provider epochs use an exclusive upper day.
    end_dt = datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)
    all_rows: list[dict] = []
    current_start = start_dt

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)
        from_ts = _to_unix(current_start.strftime("%Y-%m-%d"))
        to_ts = _to_unix(current_end.strftime("%Y-%m-%d"))
        for attempt in range(max_retries):
            try:
                rows = _fetch_ohlc_raw(symbol, resolution, from_ts, to_ts)
                all_rows.extend(rows)
                logger.info(
                    "DNSE fallback chunk complete symbol=%s resolution=%s start=%s end=%s rows=%s",
                    symbol,
                    resolution,
                    current_start.date(),
                    current_end.date(),
                    len(rows),
                )
                break
            except Exception:
                if attempt + 1 == max_retries:
                    logger.exception(
                        "DNSE fallback chunk exhausted symbol=%s resolution=%s start=%s end=%s",
                        symbol,
                        resolution,
                        current_start.date(),
                        current_end.date(),
                    )
                    raise
                time.sleep(2 ** attempt + random.uniform(0, 0.25))
        # current_end is already the next exclusive boundary. Do not skip a day.
        current_start = current_end

    if not all_rows:
        return pd.DataFrame()

    frame = pd.DataFrame(all_rows).rename(columns={
        "t": "time",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    })
    frame["time"] = (
        pd.to_datetime(frame["time"], unit="s", utc=True)
        .dt.tz_convert("Asia/Ho_Chi_Minh")
        .dt.tz_localize(None)
    )
    frame["symbol"] = symbol.strip().upper()
    frame = frame.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    logger.info(
        "DNSE fallback complete symbol=%s resolution=%s rows=%s storage=memory_only",
        symbol,
        resolution,
        len(frame),
    )
    return frame

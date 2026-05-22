"""
Intelligent Parquet preloader for VN stock 1m OHLCV data.

Features:
- Initialization: fetches exactly 6 months in 7-day chunks with rate-limit bypass
- Append-only delta: detects last_index_time, fetches only missing gap
- Exponential backoff retry on failures
- Configurable via symbols_vn.yaml
- DNSE fallback: if vnstock fails, fetches directly from DNSE REST (in-memory, not stored separately)
"""
import os
import time
import yaml
import pandas as pd
import logging
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path

from app.config import (
    PRELOAD_DIR,
    PRELOAD_MONTHS,
    PRELOAD_CHUNK_DAYS,
    PRELOAD_DELAY,
    PRELOAD_MAX_RETRIES,
    VNSTOCK_SOURCE,
)
from app.database.dnse_fallback import DERIVATIVE_SYMBOLS, fetch_dnse_ohlcv_direct

logger = logging.getLogger(__name__)
VN_TZ = timezone(timedelta(hours=7))


def _vn_now() -> datetime:
    return datetime.now(VN_TZ)


def _vn_market_start(symbol: str) -> dt_time:
    return dt_time(9, 0) if symbol.upper() in DERIVATIVE_SYMBOLS else dt_time(9, 15)


def _is_vn_trading_day(day) -> bool:
    return day.weekday() < 5


def _prev_vn_trading_day(day):
    prev = day - timedelta(days=1)
    while not _is_vn_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def _expected_latest_vn_bar(symbol: str, now_local: datetime) -> datetime:
    day = now_local.date()
    local_time = now_local.timetz().replace(tzinfo=None)
    market_start = _vn_market_start(symbol)

    if not _is_vn_trading_day(day):
        day = _prev_vn_trading_day(day)
        return datetime.combine(day, dt_time(14, 45), tzinfo=VN_TZ)
    if local_time < market_start:
        prev_day = _prev_vn_trading_day(day)
        return datetime.combine(prev_day, dt_time(14, 45), tzinfo=VN_TZ)
    if market_start <= local_time <= dt_time(11, 29):
        return now_local.replace(second=0, microsecond=0)
    if dt_time(11, 30) <= local_time < dt_time(13, 0):
        return datetime.combine(day, dt_time(11, 29), tzinfo=VN_TZ)
    if dt_time(13, 0) <= local_time <= dt_time(14, 29):
        return now_local.replace(second=0, microsecond=0)
    if dt_time(14, 30) <= local_time < dt_time(14, 45):
        return datetime.combine(day, dt_time(14, 29), tzinfo=VN_TZ)
    return datetime.combine(day, dt_time(14, 45), tzinfo=VN_TZ)


def load_vn_symbols(yaml_path: str = None) -> list:
    """Load VN symbols from YAML config."""
    if yaml_path is None:
        # Default: look next to the app directory
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "symbols_vn.yaml")
    if not os.path.exists(yaml_path):
        logger.error(f"symbols_vn.yaml not found at {yaml_path}")
        return []
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("symbols", [])


def fetch_ohlcv_chunked(
    symbol: str,
    start: str,
    end: str,
    interval: str = "1m",
    chunk_days: int = None,
    delay: float = None,
    source: str = None,
    max_retries: int = None,
) -> pd.DataFrame:
    """
    Fetch OHLCV klines with intelligent chunking to bypass vnstock/KBS rate limits.

    - Splits the date range into N-day chunks (default 7 days for 1m)
    - Retries each chunk with exponential backoff
    - Mandatory delay between chunks to respect rate limits
    """
    from vnstock import Quote

    chunk_days = chunk_days or PRELOAD_CHUNK_DAYS
    delay = delay or PRELOAD_DELAY
    source = source or VNSTOCK_SOURCE
    max_retries = max_retries or PRELOAD_MAX_RETRIES

    quote = Quote(symbol=symbol, source=source)

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")

    all_chunks = []
    current_start = start_dt

    while current_start <= end_dt:
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)

        for attempt in range(max_retries):
            try:
                df_chunk = quote.history(
                    start=current_start.strftime("%Y-%m-%d"),
                    end=current_end.strftime("%Y-%m-%d"),
                    interval=interval,
                    show_log=False,
                )
                if not df_chunk.empty:
                    all_chunks.append(df_chunk)
                logger.info(f"[{symbol}] chunk {current_start.date()} -> {current_end.date()} OK ({len(df_chunk) if not df_chunk.empty else 0} bars)")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.warning(f"[{symbol}] chunk {current_start.date()} FAILED after {max_retries} attempts: {e}")
                else:
                    sleep_time = 2 ** attempt
                    logger.debug(f"[{symbol}] retry in {sleep_time}s...")
                    time.sleep(sleep_time)

        current_start = current_end + timedelta(days=1)
        time.sleep(delay)

    if not all_chunks:
        return pd.DataFrame()

    df = pd.concat(all_chunks, ignore_index=True)
    df = df.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df


def update_symbol(symbol: str, months: int = None, preload_dir: str = None):
    """
    Append-only Parquet preloader for a single symbol.

    1. If parquet does NOT exist -> initialize full 6-month fetch via vnstock
    2. If parquet EXISTS -> read last_index_time, fetch only the missing delta
    3. If vnstock returns empty (unknown issue) -> fallback to DNSE direct fetch (in-memory)
    4. Concat, deduplicate, clip to exactly 6 months window, save
    """
    months = months or PRELOAD_MONTHS
    preload_dir = preload_dir or PRELOAD_DIR
    os.makedirs(preload_dir, exist_ok=True)

    file_path = os.path.join(preload_dir, f"{symbol}.parquet")
    now_local = _vn_now()
    expected_latest = _expected_latest_vn_bar(symbol, now_local).replace(tzinfo=None)
    end_str = now_local.strftime("%Y-%m-%d")

    existing_df = pd.DataFrame()
    start_str = (now_local - timedelta(days=30 * months)).strftime("%Y-%m-%d")

    if os.path.exists(file_path):
        logger.info(f"[{symbol}] Found existing parquet, checking last_index_time...")
        existing_df = pd.read_parquet(file_path)
        if not existing_df.empty:
            existing_df["time"] = pd.to_datetime(existing_df["time"])
            max_time = existing_df["time"].max()
            if max_time >= expected_latest:
                logger.info(
                    f"[{symbol}] Already up to date for expected VN checkpoint "
                    f"{expected_latest} (last_index_time={max_time})."
                )
                return
            # Start from the day of max_time to catch any intraday gap
            start_str = max_time.strftime("%Y-%m-%d")
            logger.info(f"[{symbol}] last_index_time={max_time}, appending from {start_str} to {end_str}")
    else:
        logger.info(f"[{symbol}] No parquet found, initializing {months}-month fetch from {start_str}")

    # ── PRIMARY: Use DNSE for derivatives like VN30F1M ─────────────
    if symbol.upper() in DERIVATIVE_SYMBOLS:
        logger.info(f"[{symbol}] derivative symbol detected, using DNSE direct source for preload")
        try:
            new_df = fetch_dnse_ohlcv_direct(
                symbol=symbol,
                start=start_str,
                end=end_str,
                resolution="1",
            )
            if new_df.empty:
                logger.warning(f"[{symbol}] DNSE direct fetch returned empty.")
        except Exception as e:
            logger.error(f"[{symbol}] DNSE direct fetch failed: {e}", exc_info=True)
            new_df = pd.DataFrame()
    else:
        # ── PRIMARY: Fetch via vnstock ──────────────────────────────
        new_df = fetch_ohlcv_chunked(symbol, start=start_str, end=end_str)

        # ── FALLBACK: If vnstock returned empty, try DNSE direct fetch ──
        if new_df.empty:
            logger.warning(f"[{symbol}] vnstock returned empty data. Trying DNSE fallback (direct, no storage)...")
            try:
                new_df = fetch_dnse_ohlcv_direct(
                    symbol=symbol,
                    start=start_str,
                    end=end_str,
                    resolution="1",
                )
                if not new_df.empty:
                    logger.info(f"[{symbol}] DNSE fallback returned {len(new_df)} bars")
                else:
                    logger.warning(f"[{symbol}] DNSE fallback also returned empty.")
            except Exception as e:
                logger.error(f"[{symbol}] DNSE fallback failed: {e}")

    if new_df.empty:
        logger.info(f"[{symbol}] No new data from any source.")
        return

    new_df["time"] = pd.to_datetime(new_df["time"])

    # Combine with existing
    if not existing_df.empty:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df

    # Deduplicate and sort
    combined = combined.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)

    # Clip to exactly 6-month window (rolling window from now)
    cutoff = now_local.replace(tzinfo=None) - timedelta(days=30 * months)
    combined = combined[combined["time"] >= cutoff].reset_index(drop=True)

    # Save
    combined.to_parquet(file_path, index=False, compression="snappy")
    logger.info(f"[{symbol}] Saved parquet: {len(combined)} rows, range {combined['time'].min()} -> {combined['time'].max()}")


def load_last_preload_snapshot(symbol: str, preload_dir: str = None) -> dict | None:
    preload_dir = preload_dir or PRELOAD_DIR
    file_path = os.path.join(preload_dir, f"{symbol}.parquet")
    if not os.path.exists(file_path):
        return None

    df = pd.read_parquet(file_path)
    if df.empty:
        return None

    df["time"] = pd.to_datetime(df["time"])
    last_row = df.sort_values("time").iloc[-1]
    ts = last_row["time"].to_pydatetime().replace(tzinfo=VN_TZ)
    return {
        "symbol": symbol.upper(),
        "price": float(last_row.get("close", 0.0)),
        "open": float(last_row.get("open", 0.0)),
        "high": float(last_row.get("high", 0.0)),
        "low": float(last_row.get("low", 0.0)),
        "volume": float(last_row.get("volume", 0.0)),
        "source": "preload_snapshot",
        "timestamp": ts.timestamp(),
    }


def run_preload(symbols: list = None):
    """
    Run the preload process for all configured VN symbols.
    Safe to call repeatedly — each run only appends missing data.
    """
    if symbols is None:
        symbols = load_vn_symbols()
    if not symbols:
        logger.warning("No symbols to preload.")
        return

    logger.info(f"Preload starting for {len(symbols)} symbols ===")
    for i, symbol in enumerate(symbols):
        logger.info(f"[{i + 1}/{len(symbols)}] Processing {symbol}...")
        try:
            update_symbol(symbol)
        except Exception as e:
            logger.error(f"[{symbol}] Critical error: {e}")
    logger.info("=== Preload complete ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_preload()

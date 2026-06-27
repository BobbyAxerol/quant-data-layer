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
VN_MATERIALIZED_INTERVALS = ("5m", "10m", "15m", "30m", "1h", "4h")
_PANDAS_INTERVALS = {
    "1m": "1min",
    "5m": "5min",
    "10m": "10min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
}


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


def preload_root(preload_dir: str = None) -> Path:
    base = Path(preload_dir or PRELOAD_DIR)
    return base.parent if base.name == "1m" else base


def preload_interval_dir(interval: str = "1m", preload_dir: str = None) -> Path:
    interval = normalize_preload_interval(interval)
    root = preload_root(preload_dir)
    return root / interval


def preload_file_path(symbol: str, interval: str = "1m", preload_dir: str = None) -> Path:
    return preload_interval_dir(interval, preload_dir) / f"{symbol.upper()}.parquet"


def normalize_preload_interval(interval: str) -> str:
    normalized = str(interval or "1m").lower().strip()
    aliases = {
        "1": "1m",
        "1min": "1m",
        "1t": "1m",
        "5": "5m",
        "5min": "5m",
        "5t": "5m",
        "10": "10m",
        "10min": "10m",
        "10t": "10m",
        "15": "15m",
        "15min": "15m",
        "15t": "15m",
        "30": "30m",
        "30min": "30m",
        "30t": "30m",
        "60": "1h",
        "60m": "1h",
        "60min": "1h",
        "1hr": "1h",
        "1hour": "1h",
        "240": "4h",
        "240m": "4h",
        "240min": "4h",
        "4hr": "4h",
        "4hour": "4h",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in _PANDAS_INTERVALS:
        raise ValueError(f"Unsupported VN preload interval: {interval}")
    return normalized


def _resample_offset(symbol: str) -> str:
    return "0min" if symbol.upper() in DERIVATIVE_SYMBOLS else "15min"


def resample_vn_ohlcv(df: pd.DataFrame, interval: str, symbol: str) -> pd.DataFrame:
    """
    Aggregate canonical VN 1m OHLCV into a materialized interval.

    The canonical parquet is local VN time, usually timezone-naive. We keep that
    convention in materialized parquet and convert to UTC only at API boundary.
    """
    interval = normalize_preload_interval(interval)
    if interval == "1m":
        return df.copy()
    if df.empty:
        return df.copy()
    required = {"time", "open", "high", "low", "close", "volume"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Cannot resample {symbol}: missing columns {sorted(missing)}")

    work = df.copy()
    work["time"] = pd.to_datetime(work["time"], errors="coerce")
    work = work.dropna(subset=["time"])
    if work.empty:
        return work
    for column in ["open", "high", "low", "close", "volume"]:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.dropna(subset=["open", "high", "low", "close"])
    if work.empty:
        return work

    work = work.sort_values("time").set_index("time")
    rule = _PANDAS_INTERVALS[interval]
    result = work.resample(
        rule,
        origin="start_day",
        offset=_resample_offset(symbol),
        label="left",
        closed="left",
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    result = result.dropna(subset=["open", "high", "low", "close"]).reset_index()
    if "symbol" in df.columns:
        result["symbol"] = symbol.upper()
    return result


def materialize_symbol_intervals(
    symbol: str,
    intervals: tuple[str, ...] = VN_MATERIALIZED_INTERVALS,
    preload_dir: str = None,
) -> dict:
    """
    Rebuild materialized VN interval parquet files from canonical 1m parquet.
    """
    symbol = symbol.upper()
    canonical_path = preload_file_path(symbol, "1m", preload_dir)
    report = {
        "symbol": symbol,
        "canonical_path": str(canonical_path),
        "canonical_exists": canonical_path.exists(),
        "intervals": [],
    }
    if not canonical_path.exists():
        return report

    df = pd.read_parquet(canonical_path)
    for interval in intervals:
        interval = normalize_preload_interval(interval)
        target_dir = preload_interval_dir(interval, preload_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{symbol}.parquet"
        resampled = resample_vn_ohlcv(df, interval, symbol)
        resampled.to_parquet(target_path, index=False, compression="snappy")
        report["intervals"].append(
            {
                "interval": interval,
                "path": str(target_path),
                "rows": int(len(resampled)),
                "status": "ok" if not resampled.empty else "empty",
            }
        )
        logger.info(f"[{symbol}] materialized {interval}: {len(resampled)} rows -> {target_path}")
    return report


def materialize_all_intervals(symbols: list = None, intervals: tuple[str, ...] = VN_MATERIALIZED_INTERVALS) -> list:
    symbols = symbols or load_vn_symbols()
    reports = []
    for symbol in symbols:
        try:
            reports.append(materialize_symbol_intervals(symbol, intervals=intervals))
        except Exception as exc:
            logger.error(f"[{symbol}] materialization failed: {exc}", exc_info=True)
            reports.append({"symbol": symbol.upper(), "status": "error", "error": str(exc)})
    return reports


def read_preload_data(symbol: str, interval: str = "1m", limit: int = 1000) -> pd.DataFrame:
    interval = normalize_preload_interval(interval)
    path = preload_file_path(symbol, interval)
    if not path.exists() and interval != "1m":
        materialize_symbol_intervals(symbol, intervals=(interval,))
    if not path.exists():
        raise FileNotFoundError(str(path))
    df = pd.read_parquet(path)
    if df.empty:
        return df
    df["time"] = pd.to_datetime(df["time"])
    return df.sort_values("time").tail(limit)


def get_preload_last_time(symbol: str, preload_dir: str = None) -> pd.Timestamp | None:
    path = preload_file_path(symbol, "1m", preload_dir)
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["time"])
    if df.empty:
        return None
    times = pd.to_datetime(df["time"], errors="coerce").dropna()
    if times.empty:
        return None
    return times.max()


def preload_needs_topup(
    symbol: str,
    now_local: datetime = None,
    preload_dir: str = None,
    max_lag_minutes: int = 1,
) -> dict:
    """
    Decide whether an existing canonical 1m parquet should be topped up.

    This never asks for a full historical bootstrap. It is intended for API
    read-through freshness when a symbol already has canonical parquet and only
    needs today's recent delta.
    """
    symbol = symbol.upper()
    now_local = now_local or _vn_now()
    last_time = get_preload_last_time(symbol, preload_dir)
    expected_latest = _expected_latest_vn_bar(symbol, now_local).replace(tzinfo=None)
    if last_time is None:
        return {
            "symbol": symbol,
            "needs_topup": False,
            "reason": "canonical_missing",
            "last_time": None,
            "expected_latest": expected_latest,
        }
    lag_minutes = max(0.0, (expected_latest - last_time.to_pydatetime()).total_seconds() / 60.0)
    return {
        "symbol": symbol,
        "needs_topup": lag_minutes > max_lag_minutes,
        "reason": "stale" if lag_minutes > max_lag_minutes else "fresh",
        "last_time": last_time,
        "expected_latest": expected_latest,
        "lag_minutes": lag_minutes,
    }


def topup_existing_symbol_if_needed(
    symbol: str,
    interval: str = "1m",
    max_lag_minutes: int = 1,
) -> dict:
    """
    Sync top-up an existing canonical 1m parquet before serving preload API.

    If canonical does not exist, this returns immediately. Full six-month
    bootstrap belongs to the daily preload job or explicit append/run commands,
    not to a read endpoint.
    """
    symbol = symbol.upper()
    decision = preload_needs_topup(symbol, max_lag_minutes=max_lag_minutes)
    if decision["needs_topup"]:
        logger.info(
            f"[{symbol}] API read-through top-up: lag={decision.get('lag_minutes'):.1f}m, "
            f"last={decision.get('last_time')}, expected={decision.get('expected_latest')}"
        )
        update_symbol(symbol)
        materialize_symbol_intervals(symbol, intervals=(normalize_preload_interval(interval),))
        decision = preload_needs_topup(symbol, max_lag_minutes=max_lag_minutes)
        decision["topup_attempted"] = True
        decision["fresh_after_topup"] = not decision.get("needs_topup", False)
        if decision.get("needs_topup"):
            logger.warning(
                f"[{symbol}] API read-through top-up did not advance enough: "
                f"lag={decision.get('lag_minutes'):.1f}m, last={decision.get('last_time')}, "
                f"expected={decision.get('expected_latest')}"
            )
    else:
        decision["topup_attempted"] = False
        decision["fresh_after_topup"] = not decision.get("needs_topup", False)
    return decision


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
                materialize_symbol_intervals(symbol, preload_dir=preload_dir)
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
        if new_df.empty:
            logger.warning(f"[{symbol}] DNSE derivative preload returned empty. Trying vnstock fallback.")
            try:
                new_df = fetch_ohlcv_chunked(symbol, start=start_str, end=end_str)
                if not new_df.empty:
                    logger.info(f"[{symbol}] vnstock derivative fallback returned {len(new_df)} bars")
                else:
                    logger.warning(f"[{symbol}] vnstock derivative fallback also returned empty.")
            except Exception as e:
                logger.error(f"[{symbol}] vnstock derivative fallback failed: {e}", exc_info=True)
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
    materialize_symbol_intervals(symbol, preload_dir=preload_dir)


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

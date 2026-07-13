from __future__ import annotations

import io
import json
import math
import os
import random
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from app.providers.binance.derivatives import fetch_klines, fetch_exchange_info
from app.providers.binance.rest import BinanceProviderError, normalize_interval


BINANCE_VISION_BASE_URL = "https://data.binance.vision/data/futures/um"
DEFAULT_CACHE_DIR = os.getenv("BINANCE_VISION_CACHE_DIR", "/app/data/binance_vision_cache")


@dataclass(frozen=True)
class ContinuousBasisRequest:
    pair: str
    interval: str = "1d"
    lookback_days: int = 365
    buffer_days: int = 14
    roll_policy: str = "research_volume_crossover"
    current_delivery_symbol: str | None = None
    end_time: datetime | None = None


def fetch_continuous_basis_bundle(
    pair: str,
    interval: str = "1d",
    lookback_days: int = 365,
    buffer_days: int = 14,
    roll_policy: str = "research_volume_crossover",
    current_delivery_symbol: str | None = None,
    include_components: bool = False,
    fallback_url: str | None = None,
    http_get: Callable[..., Any] = requests.get,
    **kwargs,
) -> dict[str, Any]:
    request = ContinuousBasisRequest(
        pair=pair.upper().strip(),
        interval=normalize_interval(interval),
        lookback_days=int(lookback_days),
        buffer_days=int(buffer_days),
        roll_policy=roll_policy,
        current_delivery_symbol=current_delivery_symbol.upper().strip() if current_delivery_symbol else None,
    )
    try:
        builder = ContinuousBasisBuilder(http_get=http_get)
        df, meta = builder.build(request, **kwargs)
        source = "binance_vision"
    except Exception as exc:
        if not fallback_url:
            raise
        df, meta = _fetch_fallback(fallback_url, request, http_get=http_get, original_error=exc)
        source = "research_fallback"

    records = _frame_to_records(df.tail(request.lookback_days))
    payload: dict[str, Any] = {
        "provider": "binance",
        "market": "usdm_futures",
        "kind": "continuous_basis_bundle",
        "pair": request.pair,
        "perp_symbol": request.pair,
        "interval": request.interval,
        "lookback_days": request.lookback_days,
        "roll_policy": request.roll_policy,
        "source": source,
        "cached": True,
        "stored": False,
        "rows": len(records),
        "data": records,
        "meta": meta,
    }
    if include_components:
        payload["components"] = meta.get("components", {})
    return payload


class ContinuousBasisBuilder:
    def __init__(self, *, cache_dir: str | None = None, http_get: Callable[..., Any] = requests.get) -> None:
        self.cache_dir = Path(cache_dir or DEFAULT_CACHE_DIR)
        self.http_get = http_get

    def build(self, request: ContinuousBasisRequest, **kwargs) -> tuple[pd.DataFrame, dict[str, Any]]:
        if request.interval != "1d":
            raise ValueError("continuous basis currently supports interval=1d only")
        if request.lookback_days < 30 or request.lookback_days > 1500:
            raise ValueError("lookback_days must be between 30 and 1500")
        if request.roll_policy != "research_volume_crossover":
            raise ValueError(f"unsupported roll_policy={request.roll_policy}")

        end_dt = request.end_time or datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=request.lookback_days + request.buffer_days)
        end_date = end_dt.date()
        start_date = start_dt.date()

        contracts = self._candidate_quarterlies(request.pair, start_date, end_date)
        if request.current_delivery_symbol and request.current_delivery_symbol not in contracts:
            contracts.append(request.current_delivery_symbol)
            contracts = sorted(set(contracts), key=_expiry_from_symbol)

        perp = self._perp_frame(request.pair, request.interval, start_dt, end_dt, **kwargs)
        quarterly = self._stitch_quarterly_contracts(
            request.pair,
            contracts,
            request.interval,
            start_dt,
            end_dt,
            **kwargs,
        )
        common_idx = quarterly.index.intersection(perp.index)
        if len(common_idx) == 0:
            raise RuntimeError("No overlapping timestamp index between perpetual and stitched quarterly data")

        aligned = pd.DataFrame(index=common_idx)
        aligned["quarterly_close"] = quarterly.loc[common_idx, "close"].astype(float)
        aligned["quarterly_volume"] = quarterly.loc[common_idx, "volume"].astype(float)
        aligned["active_contract"] = quarterly.loc[common_idx, "active_contract"].astype(str)
        aligned["days_to_expiry"] = quarterly.loc[common_idx, "days_to_expiry"].astype(float)
        aligned["is_after_crossover"] = quarterly.loc[common_idx, "is_after_crossover"].astype(bool)
        aligned["perpetual_close"] = perp.loc[common_idx, "close"].astype(float)
        aligned["perpetual_volume"] = perp.loc[common_idx, "volume"].astype(float)
        aligned["basis"] = aligned["quarterly_close"] - aligned["perpetual_close"]
        aligned = aligned[(aligned.index >= pd.Timestamp(start_dt)) & (aligned.index <= pd.Timestamp(end_dt))]
        aligned = aligned.tail(request.lookback_days)
        if len(aligned) < request.lookback_days:
            raise RuntimeError(
                f"Continuous basis rows below requested lookback: rows={len(aligned)} "
                f"lookback_days={request.lookback_days}"
            )
        meta = {
            "start_time": aligned.index.min().isoformat(),
            "end_time": aligned.index.max().isoformat(),
            "candidate_contracts": contracts,
            "active_contracts": sorted(aligned["active_contract"].dropna().unique().tolist()),
            "components": {
                "perp_rows": len(perp),
                "quarterly_rows": len(quarterly),
            },
        }
        return aligned, meta

    def _perp_frame(
        self,
        symbol: str,
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        payload = fetch_klines(symbol, interval=interval, limit=1500, start_time=start_ms, end_time=end_ms, **kwargs)
        df = _klines_to_frame(payload.get("data", []), symbol=symbol)
        if df.empty:
            raise RuntimeError(f"No perpetual klines returned for {symbol}")
        return df[(df.index >= pd.Timestamp(start_dt)) & (df.index <= pd.Timestamp(end_dt))]

    def _stitch_quarterly_contracts(
        self,
        base_symbol: str,
        symbols: list[str],
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        contract_dfs: dict[str, pd.DataFrame] = {}
        contracts = sorted(
            [{"symbol": symbol, "expiry": _expiry_from_symbol(symbol)} for symbol in symbols],
            key=lambda row: row["expiry"],
        )
        for i, contract in enumerate(contracts):
            symbol = contract["symbol"]
            previous_expiry = contracts[i - 1]["expiry"] if i > 0 else pd.Timestamp(start_dt)
            window_start = max(pd.Timestamp(start_dt), previous_expiry - pd.Timedelta(days=30)).to_pydatetime()
            window_end = min(pd.Timestamp(end_dt), contract["expiry"] + pd.Timedelta(days=30)).to_pydatetime()
            if window_start > pd.Timestamp(end_dt).to_pydatetime() or window_end < pd.Timestamp(start_dt).to_pydatetime():
                continue
            df = self._vision_kline_frame(symbol, interval, window_start, window_end, **kwargs)
            if not df.empty:
                contract_dfs[symbol] = df
        if not contract_dfs:
            raise RuntimeError(f"No quarterly Vision klines loaded for {base_symbol}")

        contracts = [contract for contract in contracts if contract["symbol"] in contract_dfs]
        crossover_times: dict[str, pd.Timestamp] = {}
        for i in range(len(contracts) - 1):
            current = contracts[i]
            next_contract = contracts[i + 1]
            df_current = contract_dfs[current["symbol"]]
            df_next = contract_dfs[next_contract["symbol"]]
            overlap_start = max(df_current.index.min(), df_next.index.min())
            overlap_end = min(df_current.index.max(), df_next.index.max(), current["expiry"])
            if overlap_start < overlap_end:
                vol_current = df_current.loc[overlap_start:overlap_end, "volume"].rolling("3D").mean()
                vol_next = df_next.loc[overlap_start:overlap_end, "volume"].rolling("3D").mean()
                crossover_mask = vol_next > vol_current
                crossover_times[current["symbol"]] = crossover_mask.idxmax() if crossover_mask.any() else current["expiry"] - pd.Timedelta(days=4)
            else:
                crossover_times[current["symbol"]] = current["expiry"] - pd.Timedelta(days=4)

        stitched_rows = []
        for i, contract in enumerate(contracts):
            symbol = contract["symbol"]
            df = contract_dfs[symbol].copy()
            start_time = df.index.min() if i == 0 else contracts[i - 1]["expiry"]
            end_time = contract["expiry"]
            if i == len(contracts) - 1:
                end_time = df.index.max()
            active_df = df.loc[start_time:end_time].copy()
            if active_df.empty:
                continue
            active_df["active_contract"] = symbol
            active_df["days_to_expiry"] = (contract["expiry"] - active_df.index).total_seconds() / 86400.0
            cross_time = crossover_times.get(symbol)
            active_df["is_after_crossover"] = active_df.index >= cross_time if cross_time is not None else False
            stitched_rows.append(active_df)
        if not stitched_rows:
            raise RuntimeError("No stitched quarterly rows generated")
        stitched = pd.concat(stitched_rows)
        stitched = stitched[~stitched.index.duplicated(keep="last")].sort_index()
        return stitched[(stitched.index >= pd.Timestamp(start_dt)) & (stitched.index <= pd.Timestamp(end_dt))]

    def _vision_kline_frame(
        self,
        symbol: str,
        interval: str,
        start_dt: datetime,
        end_dt: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for year, month in _month_range(start_dt.date(), end_dt.date()):
            rows = self._load_monthly_vision(symbol, interval, year, month, **kwargs)
            if rows is None and _allow_daily_fallback(year, month):
                rows = self._load_daily_vision_month(symbol, interval, year, month, start_dt.date(), end_dt.date(), **kwargs)
            if rows:
                frames.append(_klines_to_frame(rows, symbol=symbol))
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames)
        df = df[~df.index.duplicated(keep="last")].sort_index()
        return df[(df.index >= pd.Timestamp(start_dt)) & (df.index <= pd.Timestamp(end_dt))]

    def _load_monthly_vision(self, symbol: str, interval: str, year: int, month: int, **kwargs) -> list[list[Any]] | None:
        cache_path = self._cache_path("monthly", symbol, interval, f"{year}-{month:02d}")
        cached = self._read_cache(cache_path)
        if cached is not None:
            return cached
        url = f"{BINANCE_VISION_BASE_URL}/monthly/klines/{symbol}/{interval}/{symbol}-{interval}-{year}-{month:02d}.zip"
        rows = self._fetch_zip_rows(url, **kwargs)
        if rows is not None:
            self._write_cache(cache_path, rows)
        return rows

    def _load_daily_vision_month(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int,
        start_date: date,
        end_date: date,
        **kwargs,
    ) -> list[list[Any]]:
        rows: list[list[Any]] = []
        day = date(year, month, 1)
        while day.month == month:
            if start_date <= day <= end_date:
                cache_path = self._cache_path("daily", symbol, interval, day.isoformat())
                cached = self._read_cache(cache_path)
                if cached is None:
                    url = f"{BINANCE_VISION_BASE_URL}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{day.isoformat()}.zip"
                    cached = self._fetch_zip_rows(url, **kwargs)
                    if cached is not None:
                        self._write_cache(cache_path, cached)
                if cached:
                    rows.extend(cached)
            day += timedelta(days=1)
        return rows

    def _fetch_zip_rows(
        self,
        url: str,
        *,
        timeout: float = 20.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        **_kwargs,
    ) -> list[list[Any]] | None:
        attempts: list[dict[str, Any]] = []
        for attempt in range(1, int(max_attempts) + 1):
            try:
                response = self.http_get(url, timeout=timeout)
                status_code = int(response.status_code)
                if status_code == 404:
                    return None
                if status_code == 200:
                    return _parse_vision_zip(response.content)
                attempts.append({"attempt": attempt, "status_code": status_code, "body": getattr(response, "text", "")[:200]})
                if status_code not in {408, 418, 429, 500, 502, 503, 504}:
                    break
            except Exception as exc:
                attempts.append({"attempt": attempt, "error": str(exc)})
            if attempt < int(max_attempts):
                time.sleep(backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0, backoff_seconds))
        raise BinanceProviderError(f"Failed to fetch Binance Vision zip {url}", attempts=attempts)

    def _cache_path(self, scope: str, symbol: str, interval: str, key: str) -> Path:
        return self.cache_dir / "futures_um" / scope / "klines" / interval / f"symbol={symbol}" / f"{key}.json"

    @staticmethod
    def _read_cache(path: Path) -> list[list[Any]] | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    @staticmethod
    def _write_cache(path: Path, rows: list[list[Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows), encoding="utf-8")

    @staticmethod
    def _candidate_quarterlies(pair: str, start_date: date, end_date: date) -> list[str]:
        begin = start_date - timedelta(days=120)
        finish = end_date + timedelta(days=190)
        symbols = []
        year = begin.year
        while year <= finish.year:
            for month in (3, 6, 9, 12):
                expiry = _last_friday(year, month)
                if begin <= expiry <= finish:
                    symbols.append(f"{pair}_{expiry.strftime('%y%m%d')}")
            year += 1
        return symbols


def _fetch_fallback(
    fallback_url: str,
    request: ContinuousBasisRequest,
    *,
    http_get: Callable[..., Any],
    original_error: Exception,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    response = http_get(
        fallback_url,
        params={
            "pair": request.pair,
            "symbol": request.pair,
            "quarterly_base_symbol": request.pair,
            "interval": request.interval,
            "resolution": request.interval,
            "lookback_days": request.lookback_days,
            "limit_days": request.lookback_days,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") or payload.get("records") or payload
    if not isinstance(rows, list):
        raise RuntimeError(f"Research fallback returned unsupported payload after Vision error: {original_error}")
    df = pd.DataFrame(rows)
    if "timestamp" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "timestamp"})
    if "timestamp" not in df.columns:
        raise RuntimeError("Research fallback payload missing timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp").sort_index()
    meta = {"fallback_url": fallback_url, "original_error": str(original_error)}
    return df.tail(request.lookback_days), meta


def _parse_vision_zip(content: bytes) -> list[list[Any]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not names:
            return []
        with archive.open(names[0]) as fh:
            raw = fh.read()
    df = pd.read_csv(io.BytesIO(raw), header=None)
    if not df.empty and str(df.iloc[0, 0]).lower() in {"open_time", "open time"}:
        df = df.iloc[1:].reset_index(drop=True)
    return df.values.tolist()


def _klines_to_frame(rows: list[Any], *, symbol: str) -> pd.DataFrame:
    parsed = []
    for row in rows:
        if isinstance(row, dict):
            open_time = row.get("open_time") or row.get("openTime") or row.get(0)
            open_ = row.get("open")
            high = row.get("high")
            low = row.get("low")
            close = row.get("close")
            volume = row.get("volume")
        else:
            if len(row) < 6:
                continue
            open_time, open_, high, low, close, volume = row[:6]
        parsed.append(
            {
                "time": pd.to_datetime(int(float(open_time)), unit="ms", utc=True),
                "symbol": symbol,
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume),
            }
        )
    if not parsed:
        return pd.DataFrame()
    df = pd.DataFrame(parsed).drop_duplicates("time").set_index("time").sort_index()
    return df


def _frame_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    out = df.reset_index().rename(columns={"index": "timestamp", "time": "timestamp"})
    if "timestamp" not in out.columns:
        first = out.columns[0]
        out = out.rename(columns={first: "timestamp"})
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return json.loads(out.to_json(orient="records"))


def _month_range(start_date: date, end_date: date):
    current = date(start_date.year, start_date.month, 1)
    final = date(end_date.year, end_date.month, 1)
    while current <= final:
        yield current.year, current.month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def _allow_daily_fallback(year: int, month: int) -> bool:
    current = datetime.now(timezone.utc).date().replace(day=1)
    if current.month == 1:
        previous = date(current.year - 1, 12, 1)
    else:
        previous = date(current.year, current.month - 1, 1)
    target = date(year, month, 1)
    return target >= previous


def _last_friday(year: int, month: int) -> date:
    if month == 12:
        day = date(year, 12, 31)
    else:
        day = date(year, month + 1, 1) - timedelta(days=1)
    while day.weekday() != 4:
        day -= timedelta(days=1)
    return day


def _expiry_from_symbol(symbol: str) -> pd.Timestamp:
    suffix = symbol.split("_")[-1]
    expiry = pd.to_datetime(suffix, format="%y%m%d", utc=True)
    return expiry.replace(hour=8, minute=0, second=0, microsecond=0)

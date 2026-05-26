from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.config import (
    PRELOAD_API_FRESH_TOPUP,
    PRELOAD_API_TOPUP_MAX_LAG_MINUTES,
)
from app.history.preload_vn import (
    VN_MATERIALIZED_INTERVALS,
    materialize_all_intervals,
    materialize_symbol_intervals,
    normalize_preload_interval,
    preload_interval_dir,
    read_preload_data,
    run_preload,
    topup_existing_symbol_if_needed,
    update_symbol,
)


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/preload", tags=["preload"])


@router.get("/status")
async def preload_status():
    preload_dir = str(preload_interval_dir("1m"))
    if not os.path.exists(preload_dir):
        return {"intervals": {}, "symbols": []}

    result = []
    for f in os.listdir(preload_dir):
        if f.endswith(".parquet"):
            symbol = f.replace(".parquet", "")
            file_path = os.path.join(preload_dir, f)
            try:
                df = pd.read_parquet(file_path)
                df["time"] = pd.to_datetime(df["time"])
                first_local = df["time"].min() if not df.empty else None
                last_local = df["time"].max() if not df.empty else None
                first_utc = None
                last_utc = None
                if first_local is not None and last_local is not None:
                    try:
                        first_utc = pd.Timestamp(first_local).tz_localize("Asia/Ho_Chi_Minh").tz_convert("UTC")
                        last_utc = pd.Timestamp(last_local).tz_localize("Asia/Ho_Chi_Minh").tz_convert("UTC")
                    except Exception:
                        first_utc = None
                        last_utc = None
                result.append({
                    "symbol": symbol,
                    "rows": len(df),
                    "timezone_local": "Asia/Ho_Chi_Minh",
                    "first_local": str(first_local) if first_local is not None else None,
                    "last_local": str(last_local) if last_local is not None else None,
                    "first_utc": str(first_utc) if first_utc is not None else None,
                    "last_utc": str(last_utc) if last_utc is not None else None,
                })
            except Exception:
                result.append({"symbol": symbol, "rows": 0, "error": "corrupt"})

    interval_dirs = {}
    for interval in ("1m",) + VN_MATERIALIZED_INTERVALS:
        interval_dir = preload_interval_dir(interval)
        interval_dirs[interval] = {
            "dir": str(interval_dir),
            "exists": interval_dir.exists(),
            "files": len(list(interval_dir.glob("*.parquet"))) if interval_dir.exists() else 0,
        }

    return {"intervals": interval_dirs, "symbols": result}


@router.get("/{symbol}")
async def get_preload_data(
    symbol: str,
    interval: str = Query("1m", description="VN warmup interval: 1m, 5m, 10m, 15m, 30m, 1h, 4h"),
    limit: int = Query(1000, ge=1, le=20000, description="Latest N candles for warm-up lookback"),
    fresh: bool = Query(True, description="Top up existing canonical 1m parquet if it is stale before reading"),
):
    try:
        interval = normalize_preload_interval(interval)
        freshness = None
        if fresh and PRELOAD_API_FRESH_TOPUP:
            freshness = topup_existing_symbol_if_needed(
                symbol.upper(),
                interval=interval,
                max_lag_minutes=PRELOAD_API_TOPUP_MAX_LAG_MINUTES,
            )
        df = read_preload_data(symbol.upper(), interval=interval, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No preload data for {symbol} @ {interval}")

    try:
        df["time"] = df["time"].dt.tz_localize("Asia/Ho_Chi_Minh").dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception as exc:
        logger.warning(f"Timezone conversion failed for {symbol}: {exc}")

    records = df.to_dict(orient="records")
    for row in records:
        if "time" in row:
            row["time"] = str(row["time"])
    payload = {"symbol": symbol.upper(), "interval": interval, "count": len(records), "data": records}
    if freshness:
        payload["freshness"] = {
            key: (str(value) if isinstance(value, (pd.Timestamp, datetime)) else value)
            for key, value in freshness.items()
        }
    return payload


@router.post("/run")
async def trigger_preload():
    def _bg():
        try:
            run_preload()
        except Exception as exc:
            logger.error(f"Background preload failed: {exc}")

    threading.Thread(target=_bg, daemon=True).start()
    return {"status": "preload_started", "message": "Preload running in background"}


@router.post("/append/{symbol}")
async def trigger_append(symbol: str):
    def _bg():
        try:
            update_symbol(symbol.upper())
        except Exception as exc:
            logger.error(f"Append for {symbol} failed: {exc}")

    threading.Thread(target=_bg, daemon=True).start()
    return {"status": "append_started", "symbol": symbol.upper()}


@router.post("/materialize")
async def trigger_materialize_all():
    def _bg():
        try:
            materialize_all_intervals()
        except Exception as exc:
            logger.error(f"Materialize all failed: {exc}")

    threading.Thread(target=_bg, daemon=True).start()
    return {"status": "materialize_started", "intervals": list(VN_MATERIALIZED_INTERVALS)}


@router.post("/materialize/{symbol}")
async def trigger_materialize_symbol(symbol: str):
    try:
        report = materialize_symbol_intervals(symbol.upper())
        return {"status": "ok", "report": report}
    except Exception as exc:
        logger.error(f"Materialize for {symbol} failed: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


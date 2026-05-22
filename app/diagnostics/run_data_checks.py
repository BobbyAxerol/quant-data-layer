import os
import sys
import time
from datetime import UTC, date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import orjson
import pandas as pd
import redis
import requests
import yaml

from app.config import BINANCE_SYMBOLS_FILE, PRELOAD_DIR, REDIS_DB, REDIS_HOST, REDIS_PORT


DATA_LAYER_URL = os.getenv("DATA_LAYER_URL", "http://data_layer:8100").rstrip("/")
STREAM_STALE_SECONDS = float(os.getenv("STREAM_STALE_SECONDS", "180"))
BINANCE_SAMPLE_SIZE = int(os.getenv("BINANCE_SAMPLE_SIZE", "5"))
VN_SAMPLE_SIZE = int(os.getenv("VN_SAMPLE_SIZE", "5"))
PRELOAD_TAIL_ROWS = int(os.getenv("PRELOAD_TAIL_ROWS", "5"))
PRELOAD_LAG_TOLERANCE_MINUTES = float(os.getenv("PRELOAD_LAG_TOLERANCE_MINUTES", "20"))
REPORT_DIR = Path(os.getenv("DIAG_REPORT_DIR", "/app/logs/diagnostics"))
REPORT_BASENAME = os.getenv("DIAG_REPORT_BASENAME", "data_source_check")
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DERIVATIVE_SYMBOLS = {"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_yaml_symbols() -> List[str]:
    path = _project_root() / "symbols_vn.yaml"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return [str(s).upper() for s in payload.get("symbols", [])]


def _load_binance_symbols() -> List[str]:
    path = Path(BINANCE_SYMBOLS_FILE)
    if not path.exists():
        return []
    return [str(s).upper() for s in orjson.loads(path.read_bytes())]


def _is_derivative_symbol(symbol: str) -> bool:
    return symbol.upper() in DERIVATIVE_SYMBOLS


def _vn_now() -> datetime:
    return datetime.now(VN_TZ)


def _vn_market_start(symbol: str) -> dt_time:
    return dt_time(9, 0) if _is_derivative_symbol(symbol) else dt_time(9, 15)


def _vn_session_ranges(day: date, symbol: str) -> List[tuple[datetime, datetime]]:
    start_morning = _vn_market_start(symbol)
    ranges = [
        (
            datetime.combine(day, start_morning, tzinfo=VN_TZ),
            datetime.combine(day, dt_time(11, 29), tzinfo=VN_TZ),
        ),
        (
            datetime.combine(day, dt_time(13, 0), tzinfo=VN_TZ),
            datetime.combine(day, dt_time(14, 29), tzinfo=VN_TZ),
        ),
    ]
    return ranges


def _vn_auction_time(day: date) -> datetime:
    return datetime.combine(day, dt_time(14, 45), tzinfo=VN_TZ)


def _is_vn_trading_day(day: date) -> bool:
    return day.weekday() < 5


def _prev_vn_trading_day(day: date) -> date:
    prev = day - timedelta(days=1)
    while not _is_vn_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def _vn_market_open(now_local: datetime, symbol: str) -> bool:
    if not _is_vn_trading_day(now_local.date()):
        return False
    local_time = now_local.timetz().replace(tzinfo=None)
    morning_start = _vn_market_start(symbol)
    return (
        morning_start <= local_time <= dt_time(11, 29)
        or dt_time(13, 0) <= local_time <= dt_time(14, 29)
        or local_time == dt_time(14, 45)
    )


def _expected_latest_vn_bar(now_local: datetime, symbol: str) -> datetime:
    day = now_local.date()
    if not _is_vn_trading_day(day):
        day = _prev_vn_trading_day(day)
        return _vn_auction_time(day)

    local_time = now_local.timetz().replace(tzinfo=None)
    morning_start = _vn_market_start(symbol)

    if local_time < morning_start:
        prev_day = _prev_vn_trading_day(day)
        return _vn_auction_time(prev_day)
    if morning_start <= local_time <= dt_time(11, 29):
        return now_local.replace(second=0, microsecond=0)
    if dt_time(11, 30) <= local_time < dt_time(13, 0):
        return datetime.combine(day, dt_time(11, 29), tzinfo=VN_TZ)
    if dt_time(13, 0) <= local_time <= dt_time(14, 29):
        return now_local.replace(second=0, microsecond=0)
    if dt_time(14, 30) <= local_time < dt_time(14, 45):
        return datetime.combine(day, dt_time(14, 29), tzinfo=VN_TZ)
    return _vn_auction_time(day)


def _vn_expected_minutes_for_day(day: date, symbol: str) -> pd.DatetimeIndex:
    if not _is_vn_trading_day(day):
        return pd.DatetimeIndex([])
    parts = []
    for start_dt, end_dt in _vn_session_ranges(day, symbol):
        parts.append(
            pd.date_range(
                start=start_dt.replace(tzinfo=None),
                end=end_dt.replace(tzinfo=None),
                freq="1min",
            )
        )
    parts.append(pd.DatetimeIndex([_vn_auction_time(day).replace(tzinfo=None)]))
    if not parts:
        return pd.DatetimeIndex([])
    result = parts[0]
    for part in parts[1:]:
        result = result.append(part)
    return result


def _ms_to_iso(ms_value: Any) -> Optional[str]:
    if not isinstance(ms_value, (int, float)) or not ms_value:
        return None
    return datetime.fromtimestamp(float(ms_value) / 1000.0, tz=UTC).isoformat()


def _write_report_files(summary: Dict[str, Any], started_at: str) -> Dict[str, str]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    safe_summary = _json_safe(summary)
    payload = orjson.dumps(safe_summary, option=orjson.OPT_INDENT_2)

    timestamp_slug = (
        started_at.replace(":", "")
        .replace("-", "")
        .replace("+00:00", "Z")
    )
    latest_path = REPORT_DIR / f"{REPORT_BASENAME}_latest.json"
    archive_path = REPORT_DIR / f"{REPORT_BASENAME}_{timestamp_slug}.json"

    latest_path.write_bytes(payload)
    archive_path.write_bytes(payload)

    return {
        "latest": str(latest_path),
        "archive": str(archive_path),
    }


def _to_dt_series(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df["time"], errors="coerce")


def _format_dt(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def _check_preload_symbol(symbol: str, file_path: Path) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "symbol": symbol,
        "exists": file_path.exists(),
    }
    if not file_path.exists():
        report["status"] = "missing_file"
        return report

    try:
        df = pd.read_parquet(file_path)
    except Exception as exc:
        report["status"] = "read_error"
        report["error"] = str(exc)
        return report

    report["rows"] = int(len(df))
    if df.empty:
        report["status"] = "empty"
        return report

    if "time" not in df.columns:
        report["status"] = "missing_time_column"
        report["columns"] = list(df.columns)
        return report

    times = _to_dt_series(df)
    sorted_times = times.sort_values()
    diffs = sorted_times.diff().dropna()
    non_positive_steps = int((diffs <= pd.Timedelta(0)).sum())
    duplicate_times = int(times.duplicated().sum())
    na_counts = {k: int(v) for k, v in df.isna().sum().to_dict().items() if int(v) > 0}
    one_minute_gaps = diffs[diffs > pd.Timedelta(minutes=1)]
    actual_index = pd.DatetimeIndex(sorted_times.dropna().drop_duplicates())

    unexpected_missing: List[pd.Timestamp] = []
    for trading_day in sorted({ts.date() for ts in actual_index}):
        expected_index = _vn_expected_minutes_for_day(trading_day, symbol)
        if expected_index.empty:
            continue
        actual_day = actual_index[actual_index.date == trading_day]
        unexpected_missing.extend(expected_index.difference(actual_day).tolist())

    now_local = _vn_now()
    latest_local = actual_index.max().to_pydatetime().replace(tzinfo=VN_TZ) if len(actual_index) else None
    expected_latest_local = _expected_latest_vn_bar(now_local, symbol)
    latest_lag_minutes = None
    if latest_local is not None:
        latest_lag_minutes = round(
            max(0.0, (expected_latest_local - latest_local).total_seconds()) / 60.0,
            3,
        )
    preload_fresh_enough = latest_lag_minutes is not None and latest_lag_minutes <= PRELOAD_LAG_TOLERANCE_MINUTES

    report.update(
        {
            "status": "ok",
            "columns": list(df.columns),
            "start_time": _format_dt(times.min()),
            "end_time": _format_dt(times.max()),
            "duplicate_time_rows": duplicate_times,
            "non_positive_time_steps": non_positive_steps,
            "na_counts": na_counts,
            "gap_count_gt_1m": int(len(one_minute_gaps)),
            "largest_gap": str(one_minute_gaps.max()) if not one_minute_gaps.empty else None,
            "unexpected_missing_intraday_count": len(unexpected_missing),
            "unexpected_missing_intraday_samples": [
                _format_dt(ts) for ts in unexpected_missing[:10]
            ],
            "expected_latest_time_local": expected_latest_local.isoformat(),
            "latest_time_lag_minutes": latest_lag_minutes,
            "preload_fresh_enough": preload_fresh_enough,
            "tail": _json_safe(df.tail(PRELOAD_TAIL_ROWS).to_dict(orient="records")),
        }
    )
    return report


def run_preload_checks() -> Dict[str, Any]:
    preload_dir = Path(PRELOAD_DIR)
    symbols = _load_yaml_symbols()
    reports = []

    for symbol in symbols:
        reports.append(_check_preload_symbol(symbol, preload_dir / f"{symbol}.parquet"))

    failures = [
        r for r in reports
        if r["status"] not in {"ok"}
        or r.get("duplicate_time_rows", 0) > 0
        or r.get("non_positive_time_steps", 0) > 0
        or r.get("unexpected_missing_intraday_count", 0) > 0
        or not r.get("preload_fresh_enough", False)
    ]

    return {
        "preload_dir": str(preload_dir),
        "symbols_checked": len(symbols),
        "failed_symbols": [r["symbol"] for r in failures],
        "reports": reports,
    }


def _redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=False,
    )


def _decode_redis_json(raw: Any) -> Optional[dict]:
    if not raw:
        return None
    if isinstance(raw, str):
        raw = raw.encode()
    return orjson.loads(raw)


def _check_binance_stream(client: redis.Redis, symbol: str) -> Dict[str, Any]:
    key = f"kline:1m:{symbol}"
    payload = _decode_redis_json(client.get(key))
    report: Dict[str, Any] = {"symbol": symbol, "key": key}
    if not payload:
        report["status"] = "missing"
        return report

    kline = payload.get("k", payload)
    bar_ts = kline.get("t") if isinstance(kline, dict) else None
    close_price = kline.get("c") if isinstance(kline, dict) else None
    is_closed = kline.get("x") if isinstance(kline, dict) else None
    interval = kline.get("i") if isinstance(kline, dict) else None
    event_ts = payload.get("E") if isinstance(payload, dict) else None
    latency_seconds = None
    if isinstance(bar_ts, (int, float)):
        latency_seconds = round(time.time() - (bar_ts / 1000.0), 3)

    report.update(
        {
            "status": "ok",
            "close": close_price,
            "interval": interval,
            "is_closed": is_closed,
            "bar_time_ms": bar_ts,
            "bar_time_iso": _ms_to_iso(bar_ts),
            "bar_close_time_ms": kline.get("T") if isinstance(kline, dict) else None,
            "bar_close_time_iso": _ms_to_iso(kline.get("T") if isinstance(kline, dict) else None),
            "event_time_ms": event_ts,
            "event_time_iso": _ms_to_iso(event_ts),
            "latency_seconds_from_bar_open": latency_seconds,
            "stale": bool(latency_seconds is not None and latency_seconds > STREAM_STALE_SECONDS),
            "payload": payload,
        }
    )
    return report


def _check_binance_trade_stream(client: redis.Redis, symbol: str) -> Dict[str, Any]:
    key = f"trade:price:{symbol}"
    payload = _decode_redis_json(client.get(key))
    report: Dict[str, Any] = {"symbol": symbol, "key": key}
    if not payload:
        report["status"] = "missing"
        return report

    event_ts = payload.get("event_time") or payload.get("trade_time")
    latency_seconds = None
    if isinstance(event_ts, (int, float)) and event_ts:
        latency_seconds = round(time.time() - (float(event_ts) / 1000.0), 3)

    report.update(
        {
            "status": "ok",
            "price": payload.get("price"),
            "quantity": payload.get("quantity"),
            "side": payload.get("side"),
            "event_time_iso": _ms_to_iso(payload.get("event_time")),
            "trade_time_iso": _ms_to_iso(payload.get("trade_time")),
            "latency_seconds": latency_seconds,
            "stale": bool(latency_seconds is not None and latency_seconds > STREAM_STALE_SECONDS),
            "payload": payload,
        }
    )
    return report


def _check_vn_stream(client: redis.Redis, symbol: str) -> Dict[str, Any]:
    key = f"vn:quote:{symbol}"
    last_key = f"vn:quote:last:{symbol}"
    payload = _decode_redis_json(client.get(key))
    last_payload = _decode_redis_json(client.get(last_key))
    report: Dict[str, Any] = {"symbol": symbol, "key": key}
    now_local = _vn_now()
    market_open = _vn_market_open(now_local, symbol)

    if not payload:
        if last_payload:
            report["status"] = "missing_live_during_market" if market_open else "market_closed_snapshot_only"
            report["last_snapshot_key"] = last_key
            report["last_snapshot"] = last_payload
            last_ts = last_payload.get("timestamp")
            report["last_snapshot_timestamp"] = _format_dt(
                datetime.fromtimestamp(float(last_ts), tz=UTC)
            ) if isinstance(last_ts, (int, float)) else _format_dt(last_ts)
        else:
            report["status"] = "missing"
        report["market_open"] = market_open
        return report

    src_ts = payload.get("timestamp")
    latency_seconds = None
    if isinstance(src_ts, (int, float)):
        latency_seconds = round(time.time() - float(src_ts), 3)

    report.update(
        {
            "status": "ok",
            "source": payload.get("source"),
            "price": payload.get("price"),
            "timestamp_iso": _format_dt(datetime.fromtimestamp(float(src_ts), tz=UTC)) if isinstance(src_ts, (int, float)) else None,
            "latency_seconds": latency_seconds,
            "stale": bool(latency_seconds is not None and latency_seconds > STREAM_STALE_SECONDS),
            "market_open": market_open,
            "has_last_snapshot": last_payload is not None,
            "payload": payload,
        }
    )
    return report


def run_stream_checks() -> Dict[str, Any]:
    client = _redis_client()
    binance_symbols = _load_binance_symbols()[:BINANCE_SAMPLE_SIZE]
    vn_symbols = _load_yaml_symbols()[:VN_SAMPLE_SIZE]

    binance_trade_reports = [_check_binance_trade_stream(client, symbol) for symbol in binance_symbols]
    binance_kline_reports = [_check_binance_stream(client, symbol) for symbol in binance_symbols]
    vn_reports = [_check_vn_stream(client, symbol) for symbol in vn_symbols]

    stale_binance_trade = [r["symbol"] for r in binance_trade_reports if r.get("stale")]
    stale_binance_kline = [r["symbol"] for r in binance_kline_reports if r.get("stale")]
    stale_vn = [r["symbol"] for r in vn_reports if r.get("stale")]
    missing = [
        r["symbol"]
        for r in binance_trade_reports + binance_kline_reports + vn_reports
        if r["status"] != "ok"
    ]
    critical_missing = [
        r["symbol"]
        for r in binance_trade_reports + binance_kline_reports + vn_reports
        if r["status"] in {"missing", "missing_live_during_market"}
    ]

    return {
        "redis": {"host": REDIS_HOST, "port": REDIS_PORT, "db": REDIS_DB},
        "checked_at_vn_local": _vn_now().isoformat(),
        "binance_sampled": binance_symbols,
        "vn_sampled": vn_symbols,
        "missing_streams": missing,
        "critical_missing_streams": critical_missing,
        "stale_binance_trade": stale_binance_trade,
        "stale_binance_kline": stale_binance_kline,
        "stale_vn": stale_vn,
        "binance_trade_reports": binance_trade_reports,
        "binance_kline_reports": binance_kline_reports,
        "vn_reports": vn_reports,
    }


def run_api_checks() -> Dict[str, Any]:
    endpoints = [
        "/v1/health",
        "/v1/binance/price/BTCUSDT",
        "/v1/binance/kline/BTCUSDT?interval=1m",
        "/v1/vn/quote-last/VCB",
        "/v1/preload/status",
    ]
    results = []
    for endpoint in endpoints:
        url = f"{DATA_LAYER_URL}{endpoint}"
        try:
            response = requests.get(url, timeout=10)
            body: Any
            try:
                body = response.json()
            except Exception:
                body = response.text[:500]
            results.append(
                {
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "ok": response.ok,
                    "body": body,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "endpoint": endpoint,
                    "status_code": None,
                    "ok": False,
                    "error": str(exc),
                }
            )
    return {"base_url": DATA_LAYER_URL, "results": results}


def main() -> int:
    started_at = datetime.now(UTC).isoformat()

    api_report = run_api_checks()
    preload_report = run_preload_checks()
    stream_report = run_stream_checks()

    summary = {
        "started_at": started_at,
        "api": api_report,
        "preload": preload_report,
        "stream": stream_report,
    }
    summary["report_files"] = _write_report_files(summary, started_at)

    print(orjson.dumps(_json_safe(summary), option=orjson.OPT_INDENT_2).decode())

    has_api_failure = any(not item.get("ok") for item in api_report["results"])
    has_preload_failure = len(preload_report["failed_symbols"]) > 0
    has_stream_failure = (
        len(stream_report["critical_missing_streams"]) > 0
        or len(stream_report["stale_binance_trade"]) > 0
        or len(stream_report["stale_binance_kline"]) > 0
        or len(stream_report["stale_vn"]) > 0
    )

    return 1 if (has_api_failure or has_preload_failure or has_stream_failure) else 0


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import requests


BINANCE_KLINE_INTERVALS = {
    "1s", "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}

BINANCE_KLINE_URLS = {
    "spot": [("spot", "https://api.binance.com/api/v3/klines")],
    "usdm": [("usdm", "https://fapi.binance.com/fapi/v1/klines")],
    "futures": [("usdm", "https://fapi.binance.com/fapi/v1/klines")],
    "auto": [
        ("usdm", "https://fapi.binance.com/fapi/v1/klines"),
        ("spot", "https://api.binance.com/api/v3/klines"),
    ],
}


class BinanceProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        attempts: list[dict] | None = None,
        *,
        retry_after_ms: int | None = None,
    ):
        super().__init__(message)
        self.attempts = attempts or []
        if retry_after_ms is not None and retry_after_ms < 0:
            raise ValueError("retry_after_ms must be non-negative")
        self.retry_after_ms = retry_after_ms


def normalize_interval(interval: str) -> str:
    value = str(interval or "").strip()
    if value not in BINANCE_KLINE_INTERVALS:
        raise ValueError(f"Unsupported Binance interval: {interval}")
    return value


def kline_urls(market: str) -> list[tuple[str, str]]:
    value = str(market or "").lower().strip()
    if value not in BINANCE_KLINE_URLS:
        raise ValueError(f"Unsupported Binance market: {market}")
    return BINANCE_KLINE_URLS[value]


def fetch_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = 500,
    start_time: int | None = None,
    end_time: int | None = None,
    market: str = "auto",
    http_get=requests.get,
) -> dict:
    provider_interval = normalize_interval(interval)
    params = {"symbol": symbol.upper().strip(), "interval": provider_interval, "limit": limit}
    if start_time is not None:
        params["startTime"] = start_time
    if end_time is not None:
        params["endTime"] = end_time

    errors = []
    for resolved_market, url in kline_urls(market):
        try:
            resp = http_get(url, params=params, timeout=10)
            if resp.status_code == 200:
                return {
                    "provider": "binance",
                    "market": resolved_market,
                    "symbol": symbol.upper().strip(),
                    "requested_interval": provider_interval,
                    "provider_interval": provider_interval,
                    "params": params,
                    "data": resp.json(),
                    "cached": False,
                    "stored": False,
                }
            errors.append({"market": resolved_market, "status_code": resp.status_code, "body": resp.text[:300]})
        except Exception as exc:
            errors.append({"market": resolved_market, "error": str(exc)})

    raise BinanceProviderError(
        f"Failed to fetch historical klines for {symbol} from Binance",
        attempts=errors,
    )

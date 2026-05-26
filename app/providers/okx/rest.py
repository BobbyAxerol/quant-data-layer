from __future__ import annotations

import requests


OKX_INTERVAL_ALIASES = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "2d": "2D",
    "3d": "3D",
    "1w": "1W",
    "1M": "1M",
    "6Hutc": "6Hutc",
    "12Hutc": "12Hutc",
    "1Dutc": "1Dutc",
    "2Dutc": "2Dutc",
    "3Dutc": "3Dutc",
    "1Wutc": "1Wutc",
    "1Mutc": "1Mutc",
    "3Mutc": "3Mutc",
}


class OkxProviderError(RuntimeError):
    def __init__(self, message: str, detail: dict | str | None = None):
        super().__init__(message)
        self.detail = detail


def normalize_interval(interval: str) -> str:
    value = str(interval or "").strip()
    if value in OKX_INTERVAL_ALIASES:
        return OKX_INTERVAL_ALIASES[value]
    lowered = value.lower()
    if lowered in OKX_INTERVAL_ALIASES:
        return OKX_INTERVAL_ALIASES[lowered]
    raise ValueError(f"Unsupported OKX interval: {interval}")


def normalize_symbol(symbol: str) -> str:
    value = symbol.upper().strip()
    if "-" in value:
        return value
    for quote in ("USDT", "USDC", "BTC", "ETH"):
        if value.endswith(quote) and len(value) > len(quote):
            return f"{value[:-len(quote)]}-{quote}"
    return value


def fetch_candles(
    symbol: str,
    interval: str = "1m",
    limit: int = 500,
    start_time: int | None = None,
    end_time: int | None = None,
    http_get=requests.get,
) -> dict:
    provider_interval = normalize_interval(interval)
    normalized_symbol = symbol.upper().strip()
    okx_symbol = normalize_symbol(normalized_symbol)
    okx_limit = min(limit, 300)
    params = {"instId": okx_symbol, "bar": provider_interval, "limit": okx_limit}
    if end_time is not None:
        params["before"] = end_time
    if start_time is not None:
        params["after"] = start_time

    try:
        resp = http_get("https://www.okx.com/api/v5/market/candles", params=params, timeout=10)
    except Exception as exc:
        raise OkxProviderError(f"Failed to fetch OKX candles for {symbol}", detail=str(exc)) from exc

    if resp.status_code != 200:
        raise OkxProviderError(
            f"Failed to fetch OKX candles for {symbol}",
            detail={"status_code": resp.status_code, "body": resp.text[:300]},
        )

    payload = resp.json()
    if payload.get("code") != "0":
        raise OkxProviderError(f"Failed to fetch OKX candles for {symbol}", detail=payload)

    return {
        "provider": "okx",
        "symbol": normalized_symbol,
        "instId": okx_symbol,
        "requested_interval": interval,
        "provider_interval": provider_interval,
        "params": params,
        "data": payload.get("data", []),
        "cached": False,
        "stored": False,
    }


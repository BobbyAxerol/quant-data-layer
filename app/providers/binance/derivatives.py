from __future__ import annotations

import os
import random
import time
from typing import Any

import requests

from app.providers.binance.rest import BINANCE_KLINE_INTERVALS, BinanceProviderError, normalize_interval


BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"

BINANCE_DERIVATIVE_PERIODS = {
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
}

BINANCE_DERIVATIVE_ENDPOINTS = {
    "exchange_info": "/fapi/v1/exchangeInfo",
    "klines": "/fapi/v1/klines",
    "depth": "/fapi/v1/depth",
    "funding_rate": "/fapi/v1/fundingRate",
    "open_interest": "/fapi/v1/openInterest",
    "mark_index_price": "/fapi/v1/premiumIndex",
    "open_interest_hist": "/futures/data/openInterestHist",
    "global_long_short_account_ratio": "/futures/data/globalLongShortAccountRatio",
    "top_long_short_account_ratio": "/futures/data/topLongShortAccountRatio",
    "top_long_short_position_ratio": "/futures/data/topLongShortPositionRatio",
    "taker_long_short_ratio": "/futures/data/takerlongshortRatio",
    "basis": "/futures/data/basis",
}

LONG_SHORT_KIND_TO_ENDPOINT = {
    "global_account": "global_long_short_account_ratio",
    "top_account": "top_long_short_account_ratio",
    "top_position": "top_long_short_position_ratio",
}


def normalize_period(period: str) -> str:
    value = str(period or "").strip()
    if value not in BINANCE_DERIVATIVE_PERIODS:
        raise ValueError(f"Unsupported Binance derivatives period: {period}")
    return value


def normalize_contract_type(contract_type: str) -> str:
    value = str(contract_type or "").upper().strip()
    supported = {"PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"}
    if value not in supported:
        raise ValueError(f"Unsupported Binance futures contract_type: {contract_type}")
    return value


def normalize_depth_limit(limit: int) -> int:
    value = int(limit)
    supported = {5, 10, 20, 50, 100, 500, 1000}
    if value not in supported:
        raise ValueError(f"Unsupported Binance depth limit: {limit}; supported={sorted(supported)}")
    return value


def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value is not None}


def _headers(endpoint_key: str) -> dict[str, str]:
    api_key = os.getenv("BINANCE_MARKET_DATA_API_KEY", "").strip()
    if not api_key:
        return {}
    return {"X-MBX-APIKEY": api_key}


def _public_get(
    endpoint_key: str,
    params: dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
    max_attempts: int = 4,
    backoff_seconds: float = 0.25,
    http_get=requests.get,
) -> dict[str, Any]:
    if endpoint_key not in BINANCE_DERIVATIVE_ENDPOINTS:
        raise ValueError(f"Unsupported Binance derivatives endpoint: {endpoint_key}")
    url = f"{BINANCE_FUTURES_BASE_URL}{BINANCE_DERIVATIVE_ENDPOINTS[endpoint_key]}"
    request_params = _clean_params(params or {})
    attempts: list[dict[str, Any]] = []

    for attempt in range(1, max_attempts + 1):
        try:
            resp = http_get(url, params=request_params, headers=_headers(endpoint_key), timeout=timeout)
            status_code = int(resp.status_code)
            if status_code == 200:
                return {
                    "endpoint": endpoint_key,
                    "url_path": BINANCE_DERIVATIVE_ENDPOINTS[endpoint_key],
                    "params": request_params,
                    "data": resp.json(),
                    "attempts": attempts + [{"attempt": attempt, "status_code": status_code}],
                }
            body = getattr(resp, "text", "")[:300]
            attempts.append({"attempt": attempt, "status_code": status_code, "body": body})
            if status_code not in {408, 418, 429, 500, 502, 503, 504}:
                break
        except Exception as exc:
            attempts.append({"attempt": attempt, "error": str(exc)})

        if attempt < max_attempts:
            jitter = random.uniform(0, backoff_seconds)
            time.sleep(backoff_seconds * (2 ** (attempt - 1)) + jitter)

    raise BinanceProviderError(
        f"Failed to fetch Binance derivatives endpoint {endpoint_key}",
        attempts=attempts,
    )


def _wrap(endpoint_key: str, payload: dict[str, Any], *, symbol: str | None = None, pair: str | None = None) -> dict[str, Any]:
    return {
        "provider": "binance",
        "market": "usdm_futures",
        "endpoint": endpoint_key,
        "symbol": symbol.upper().strip() if symbol else None,
        "pair": pair.upper().strip() if pair else None,
        "params": payload["params"],
        "data": payload["data"],
        "cached": False,
        "stored": False,
        "attempts": payload.get("attempts", []),
    }


def fetch_exchange_info(symbol: str | None = None, **kwargs) -> dict[str, Any]:
    params = {"symbol": symbol.upper().strip() if symbol else None}
    return _wrap("exchange_info", _public_get("exchange_info", params, **kwargs), symbol=symbol)


def fetch_klines(
    symbol: str,
    interval: str = "1d",
    limit: int = 30,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    provider_interval = normalize_interval(interval)
    if int(limit) < 1 or int(limit) > 1500:
        raise ValueError("Binance derivatives kline limit must be between 1 and 1500")
    params = {
        "symbol": symbol.upper().strip(),
        "interval": provider_interval,
        "limit": int(limit),
        "startTime": start_time,
        "endTime": end_time,
    }
    payload = _public_get("klines", params, **kwargs)
    wrapped = _wrap("klines", payload, symbol=symbol)
    wrapped["requested_interval"] = provider_interval
    wrapped["provider_interval"] = provider_interval
    return wrapped


def fetch_depth(symbol: str, limit: int = 5, **kwargs) -> dict[str, Any]:
    params = {"symbol": symbol.upper().strip(), "limit": normalize_depth_limit(limit)}
    return _wrap("depth", _public_get("depth", params, **kwargs), symbol=symbol)


def fetch_open_interest(symbol: str, **kwargs) -> dict[str, Any]:
    params = {"symbol": symbol.upper().strip()}
    return _wrap("open_interest", _public_get("open_interest", params, **kwargs), symbol=symbol)


def fetch_mark_index_price(symbol: str, **kwargs) -> dict[str, Any]:
    """Fetch the exact USD-M mark/index reference snapshot for one symbol."""

    params = {"symbol": symbol.upper().strip()}
    return _wrap(
        "mark_index_price",
        _public_get("mark_index_price", params, **kwargs),
        symbol=symbol,
    )


def fetch_metric_history(
    endpoint_key: str,
    symbol: str,
    period: str = "1d",
    limit: int = 30,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    if endpoint_key not in {
        "open_interest_hist",
        "global_long_short_account_ratio",
        "top_long_short_account_ratio",
        "top_long_short_position_ratio",
        "taker_long_short_ratio",
    }:
        raise ValueError(f"Unsupported Binance metric history endpoint: {endpoint_key}")
    provider_period = normalize_period(period)
    if int(limit) < 1 or int(limit) > 500:
        raise ValueError("Binance derivatives metric limit must be between 1 and 500")
    params = {
        "symbol": symbol.upper().strip(),
        "period": provider_period,
        "limit": int(limit),
        "startTime": start_time,
        "endTime": end_time,
    }
    payload = _public_get(endpoint_key, params, **kwargs)
    wrapped = _wrap(endpoint_key, payload, symbol=symbol)
    wrapped["requested_period"] = provider_period
    wrapped["latest_30_days_only"] = True
    return wrapped


def fetch_long_short_ratio(
    kind: str,
    symbol: str,
    period: str = "1d",
    limit: int = 30,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    normalized_kind = str(kind or "").lower().strip()
    if normalized_kind not in LONG_SHORT_KIND_TO_ENDPOINT:
        raise ValueError(f"Unsupported long/short ratio kind: {kind}")
    return fetch_metric_history(
        LONG_SHORT_KIND_TO_ENDPOINT[normalized_kind],
        symbol,
        period,
        limit,
        start_time,
        end_time,
        **kwargs,
    )


def fetch_taker_long_short_ratio(
    symbol: str,
    period: str = "1d",
    limit: int = 30,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    return fetch_metric_history("taker_long_short_ratio", symbol, period, limit, start_time, end_time, **kwargs)


def fetch_funding_rate(
    symbol: str,
    limit: int = 100,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    if int(limit) < 1 or int(limit) > 1000:
        raise ValueError("Binance funding rate limit must be between 1 and 1000")
    params = {
        "symbol": symbol.upper().strip(),
        "limit": int(limit),
        "startTime": start_time,
        "endTime": end_time,
    }
    return _wrap("funding_rate", _public_get("funding_rate", params, **kwargs), symbol=symbol)


def fetch_basis(
    pair: str,
    contract_type: str = "CURRENT_QUARTER",
    period: str = "1d",
    limit: int = 30,
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    provider_period = normalize_period(period)
    normalized_contract_type = normalize_contract_type(contract_type)
    if int(limit) < 1 or int(limit) > 500:
        raise ValueError("Binance basis limit must be between 1 and 500")
    params = {
        "pair": pair.upper().strip(),
        "contractType": normalized_contract_type,
        "period": provider_period,
        "limit": int(limit),
        "startTime": start_time,
        "endTime": end_time,
    }
    payload = _public_get("basis", params, **kwargs)
    wrapped = _wrap("basis", payload, pair=pair)
    wrapped["requested_period"] = provider_period
    wrapped["contract_type"] = normalized_contract_type
    wrapped["latest_30_days_only"] = True
    return wrapped


def fetch_basis_bundle(
    perp_symbol: str,
    delivery_symbol: str,
    pair: str | None = None,
    interval: str = "1d",
    period: str = "1d",
    limit: int = 30,
    include_depth: bool = True,
    depth_limit: int = 5,
    contract_type: str = "CURRENT_QUARTER",
    start_time: int | None = None,
    end_time: int | None = None,
    **kwargs,
) -> dict[str, Any]:
    normalized_perp = perp_symbol.upper().strip()
    normalized_delivery = delivery_symbol.upper().strip()
    normalized_pair = (pair or normalized_perp).upper().strip()

    bundle: dict[str, Any] = {
        "provider": "binance",
        "market": "usdm_futures",
        "kind": "basis_bundle",
        "perp_symbol": normalized_perp,
        "delivery_symbol": normalized_delivery,
        "pair": normalized_pair,
        "requested_interval": normalize_interval(interval),
        "requested_period": normalize_period(period),
        "limit": int(limit),
        "cached": False,
        "stored": False,
        "components": {},
        "errors": {},
    }
    fetches = {
        "perp_klines": lambda: fetch_klines(normalized_perp, interval, limit, start_time, end_time, **kwargs),
        "delivery_klines": lambda: fetch_klines(normalized_delivery, interval, limit, start_time, end_time, **kwargs),
        "funding_rate": lambda: fetch_funding_rate(normalized_perp, min(int(limit), 1000), start_time, end_time, **kwargs),
        "open_interest_hist": lambda: fetch_metric_history("open_interest_hist", normalized_perp, period, limit, start_time, end_time, **kwargs),
        "global_long_short": lambda: fetch_long_short_ratio("global_account", normalized_perp, period, limit, start_time, end_time, **kwargs),
        "top_account_long_short": lambda: fetch_long_short_ratio("top_account", normalized_perp, period, limit, start_time, end_time, **kwargs),
        "top_position_long_short": lambda: fetch_long_short_ratio("top_position", normalized_perp, period, limit, start_time, end_time, **kwargs),
        "taker_long_short": lambda: fetch_taker_long_short_ratio(normalized_perp, period, limit, start_time, end_time, **kwargs),
        "basis": lambda: fetch_basis(normalized_pair, contract_type, period, limit, start_time, end_time, **kwargs),
    }
    if include_depth:
        fetches["perp_depth"] = lambda: fetch_depth(normalized_perp, depth_limit, **kwargs)
        fetches["delivery_depth"] = lambda: fetch_depth(normalized_delivery, depth_limit, **kwargs)

    for name, fetcher in fetches.items():
        try:
            bundle["components"][name] = fetcher()
        except Exception as exc:
            bundle["errors"][name] = {"error": str(exc), "attempts": getattr(exc, "attempts", [])}

    bundle["success_count"] = len(bundle["components"])
    bundle["error_count"] = len(bundle["errors"])
    bundle["partial"] = bool(bundle["errors"])
    return bundle

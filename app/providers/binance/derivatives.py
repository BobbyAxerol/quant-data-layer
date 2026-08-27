from __future__ import annotations

import os
import random
import time
from collections.abc import Mapping
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

# Binance sometimes returns its documented `{code, msg}` error body with HTTP
# 200. Treat it at the common provider edge so no downstream adapter can
# mistake an error envelope for a product-specific payload.
_TRANSIENT_PROVIDER_ERROR_CODES = frozenset({
    -1000,  # UNKNOWN
    -1001,  # DISCONNECTED
    -1003,  # TOO_MANY_REQUESTS
    -1004,  # SERVER_BUSY
    -1006,  # UNEXPECTED_RESP
    -1007,  # TIMEOUT
    -1008,  # SERVER_BUSY
})


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


def _provider_error_code(payload: Any) -> int | None:
    """Return a nonzero documented Binance error code, if this is one."""

    if not isinstance(payload, Mapping) or "code" not in payload:
        return None
    try:
        code = int(payload["code"])
    except (TypeError, ValueError):
        return None
    return code if code else None


def _retry_after_ms(response: Any) -> int | None:
    """Read Binance's official rate-limit delay without exposing response text."""

    headers = getattr(response, "headers", {}) or {}
    if not isinstance(headers, Mapping):
        return None
    value = next(
        (item for key, item in headers.items() if str(key).lower() == "retry-after"),
        None,
    )
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    # Binance documents Retry-After in seconds for 418/429. Keep malformed or
    # unbounded provider values out of the caller's scheduling decision.
    if not 0 <= seconds <= 3 * 24 * 60 * 60:
        return None
    return int(seconds * 1_000)


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
    retry_after_ms: int | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            resp = http_get(url, params=request_params, headers=_headers(endpoint_key), timeout=timeout)
            status_code = int(resp.status_code)
            if status_code == 200:
                payload = resp.json()
                provider_code = _provider_error_code(payload)
                if provider_code is not None:
                    attempt_record = {
                        "attempt": attempt,
                        "status_code": status_code,
                        "provider_code": provider_code,
                    }
                    hint = _retry_after_ms(resp)
                    if hint is not None:
                        attempt_record["retry_after_ms"] = hint
                        retry_after_ms = hint
                    attempts.append(attempt_record)
                    # -1003 is Binance's rate-limit control response. Do not
                    # turn one rejected request into a burst of retries.
                    if provider_code == -1003:
                        break
                    if provider_code not in _TRANSIENT_PROVIDER_ERROR_CODES:
                        break
                else:
                    return {
                        "endpoint": endpoint_key,
                        "url_path": BINANCE_DERIVATIVE_ENDPOINTS[endpoint_key],
                        "params": request_params,
                        "data": payload,
                        "attempts": attempts + [{"attempt": attempt, "status_code": status_code}],
                    }
            else:
                body = getattr(resp, "text", "")[:300]
                attempt_record = {"attempt": attempt, "status_code": status_code, "body": body}
                hint = _retry_after_ms(resp)
                if hint is not None:
                    attempt_record["retry_after_ms"] = hint
                    retry_after_ms = hint
                attempts.append(attempt_record)
                # 418 is an IP ban and 429 is an explicit rate limit. Both
                # must be rescheduled by the caller using Retry-After rather
                # than retried inside this request worker.
                if status_code in {418, 429}:
                    break
                if status_code not in {408, 500, 502, 503, 504}:
                    break
        except Exception as exc:
            attempts.append({"attempt": attempt, "error": str(exc)})

        if attempt < max_attempts:
            jitter = random.uniform(0, backoff_seconds)
            time.sleep(backoff_seconds * (2 ** (attempt - 1)) + jitter)

    raise BinanceProviderError(
        f"Failed to fetch Binance derivatives endpoint {endpoint_key}",
        attempts=attempts,
        retry_after_ms=retry_after_ms,
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

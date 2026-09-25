"""Binance kline REST behind the Rust provider admission (KN-4 D47-2).

Purpose: the BAR edge's venue history and final-bar reads must not spend the
host IP's shared Binance budget blindly. A drop-in ``fetcher`` for
``qdl.adapters.binance.bar_edge`` (same call shape as
``app.providers.binance.rest.fetch_klines``) that:

* holds a Rust grant per page, costing the documented request weight of the
  page size (USD-M ``/fapi/v1/klines``: limit <100 -> 1, <500 -> 2, <=1000 -> 5,
  >1000 -> 10; Spot ``/api/v3/klines``: 2);
* uses one pooled HTTP session;
* turns HTTP 418/429 or code -1003 into ``ProviderRateLimited`` after relaying
  it (with ``Retry-After``) to Rust - never retried here;
* reads ``X-MBX-USED-WEIGHT-1M`` (the whole IP's usage, production included):
  BATCH work (history) above ``ip_share`` of the IP limit waits for the next
  minute window, so realtime reads keep the rest.

Boundary: public market data only; no key; no budget of its own (Rust owns it).
"""
from __future__ import annotations

import time
from typing import Any, Callable

import requests

from app.providers.binance.rest import BinanceProviderError, normalize_interval
from qdl.admission.contracts import AdmissionPriority, ProviderLane
from qdl.admission.edge import BlockingProviderAdmission

_URLS = {"usdm": "https://fapi.binance.com/fapi/v1/klines", "spot": "https://api.binance.com/api/v3/klines"}
# Documented request-weight limits per IP per minute.
IP_WEIGHT_LIMIT_1M = {"usdm": 2400, "spot": 6000}


def binance_klines_weight(market: str, limit: int) -> int:
    if market == "spot":
        return 2
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def _market(value: str) -> str:
    market = str(value or "").lower().strip()
    market = "usdm" if market in {"usdm", "futures"} else market
    if market not in _URLS:
        raise ValueError(f"admitted Binance klines need an explicit market, not {value!r}")
    return market


class AdmittedBinanceKlines:
    def __init__(self, admission: BlockingProviderAdmission, *, priority: AdmissionPriority,
                 session: Any | None = None, ip_share: float = 0.5, timeout_s: float = 10.0,
                 clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep) -> None:
        if not 0 < ip_share <= 1:
            raise ValueError("ip_share must be within (0, 1]")
        self._admission = admission
        self._priority = priority
        self._session = session or requests.Session()
        self._ip_share = ip_share
        self._timeout_s = timeout_s
        self._clock = clock
        self._sleep = sleep

    def __call__(self, symbol: str, *, interval: str, limit: int, end_time: int | None = None,
                 start_time: int | None = None, market: str = "usdm") -> dict:
        market = _market(market)
        lane = ProviderLane("BINANCE", "USDM" if market == "usdm" else "SPOT", "KLINES")
        params: dict[str, Any] = {"symbol": symbol.upper().strip(), "interval": normalize_interval(interval),
                                  "limit": int(limit)}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        with self._admission.lease(lane, f"klines:{params['symbol']}:{params['interval']}",
                                   priority=self._priority, token_cost=binance_klines_weight(market, limit)):
            response = self._session.get(_URLS[market], params=params, timeout=self._timeout_s)
        used = _int_header(response, "X-MBX-USED-WEIGHT-1M")
        if response.status_code == 200:
            data = response.json()
            if (self._priority is AdmissionPriority.BATCH and used is not None
                    and used >= self._ip_share * IP_WEIGHT_LIMIT_1M[market]):
                # The whole IP (production included) is past its share: history waits.
                now = self._clock()
                self._sleep(max(0.0, 60.0 - (now % 60.0)) + 0.5)
            return {"provider": "binance", "market": market, "symbol": params["symbol"],
                    "requested_interval": params["interval"], "provider_interval": params["interval"],
                    "params": params, "data": data, "used_weight_1m": used}
        code = _provider_code(response)
        if response.status_code in {418, 429} or code == -1003:
            retry_after_s = _int_header(response, "Retry-After")
            raise self._admission.rate_limited(
                lane, http_status=response.status_code if response.status_code in {418, 429} else None,
                provider_code=-1003 if code == -1003 else None,
                retry_after_ms=retry_after_s * 1000 if retry_after_s else None)
        raise BinanceProviderError(
            f"Binance klines failed for {params['symbol']}",
            attempts=[{"market": market, "status_code": response.status_code, "provider_code": code}])


def _int_header(response: Any, name: str) -> int | None:
    value = (getattr(response, "headers", None) or {}).get(name)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _provider_code(response: Any) -> int | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    code = payload.get("code") if isinstance(payload, dict) else None
    return code if isinstance(code, int) else None

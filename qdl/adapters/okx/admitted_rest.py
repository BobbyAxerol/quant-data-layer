"""OKX public REST behind the Rust provider admission (KN-4 D47-2).

Purpose: ``OkxRestClient`` keeps a process-local token bucket per client
instance, and the BAR edge built a new client for every history call, so no
budget crossed calls; it also retried a rate-limit reply like any error. This
subclass is the drop-in client for ``OkxHistoricalClient`` in the edge: one
shared instance, one pooled session, one Rust grant per request (cost 1; OKX
market endpoints are limited per IP per endpoint), and HTTP 429 or code
``50011`` relayed to Rust and raised as ``ProviderRateLimited`` - never
retried here. Other transient errors keep the parent's bounded retries.

Boundary: public market data only; no budget of its own (Rust owns it).
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping
from typing import Any

import requests

from qdl.admission.contracts import AdmissionPriority, ProviderLane
from qdl.admission.edge import BlockingProviderAdmission, ProviderRateLimited
from qdl.adapters.okx.client import OKX_REST_BASE, OkxRestClient

_FAMILIES = {
    "/api/v5/market/history-candles": "HISTORY_CANDLES",
    "/api/v5/market/candles": "CANDLES",
}


class AdmittedOkxRestClient(OkxRestClient):
    def __init__(self, admission: BlockingProviderAdmission, *, priority: AdmissionPriority,
                 session: Any | None = None, base_url: str = OKX_REST_BASE, timeout_seconds: float = 10.0) -> None:
        super().__init__(base_url=base_url, timeout_seconds=timeout_seconds)
        self._admission = admission
        self._priority = priority
        self._session = session or requests.Session()

    @staticmethod
    def lane_for(path: str, params: Mapping[str, str]) -> ProviderLane:
        family = _FAMILIES.get(path)
        if family is None:
            raise ValueError(f"admitted OKX client serves market candle endpoints only, not {path}")
        inst = str(params.get("instId") or "").upper()
        return ProviderLane("OKX", "SWAP" if inst.endswith("-SWAP") else "SPOT", family)

    def _leased_get(self, path: str, params: Mapping[str, str]) -> list[dict[str, Any]]:
        lane = self.lane_for(path, params)
        with self._admission.lease(lane, f"{lane.endpoint_family.lower()}:{params.get('instId')}",
                                   priority=self._priority, token_cost=1):
            response = self._session.get(f"{self._base_url}{path}", params=dict(params), timeout=self._timeout)
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        code = str(payload.get("code")) if isinstance(payload, dict) else None
        if response.status_code == 429 or code == "50011":
            raise self._admission.rate_limited(
                lane, http_status=429 if response.status_code == 429 else None,
                provider_code=50011 if code == "50011" else None, retry_after_ms=None)
        if response.status_code != 200:
            raise requests.HTTPError(f"OKX HTTP {response.status_code} {path}")
        if code != "0" or not isinstance(payload.get("data"), list):
            raise ValueError(f"OKX V5 error code={code} msg={payload.get('msg')}")
        return payload["data"]

    async def get(self, path: str, *, params: Mapping[str, str], bucket: str,
                  attempts: int = 3) -> list[dict[str, Any]]:
        last_error: BaseException | None = None
        for attempt in range(attempts):
            try:
                return await asyncio.to_thread(self._leased_get, path, params)
            except ProviderRateLimited:
                raise
            except (requests.RequestException, ValueError) as error:
                last_error = error
                if attempt + 1 < attempts:
                    delay = min(4.0, 0.5 * 2**attempt)
                    await asyncio.sleep(delay + random.random() * delay * 0.2)
        raise RuntimeError(f"OKX V5 request exhausted retries: {path}") from last_error

"""OKX public REST behind the Rust provider admission (KN-4 D47-2).

Purpose: ``OkxRestClient`` keeps a process-local token bucket per client
instance, and the BAR edge built a new client for every history call, so no
budget crossed calls; it also retried a rate-limit reply like any error. This
subclass is the drop-in client for ``OkxHistoricalClient`` in the edge: one
shared instance, one pooled session, one Rust grant per request (cost 1; OKX
market endpoints are limited per IP per endpoint), and HTTP 429 or code
``50011`` relayed to Rust and raised as ``ProviderRateLimited`` - never
retried here. Other transient errors keep the parent's bounded retries.

Contract statistics can use the same lease path via REFERENCE_CONTRACT_STATISTICS
lanes. They also keep the shared client's conservative endpoint/instrument pacing.
Wiring those lanes into a deployed reference runtime/policy is separately required.
Boundary: public market data only; Rust owns shared admission.
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping
from typing import Any

import requests

from qdl.admission import AdmissionContractError, AdmissionTransportError
from qdl.admission.contracts import (
    AdmissionDisposition, AdmissionPriority, AdmissionRequest, ProviderAdmissionRuntime, ProviderLane,
)
from qdl.admission.edge import BlockingProviderAdmission, ProviderRateLimited, request_id
from qdl.adapters.okx.client import (
    OKX_CONTRACT_STATISTICS_LIMITS, OKX_REST_BASE, OkxRestClient,
    OkxStatisticsRateLimited, statistics_retry_after_ms,
)

from qdl.reference.contracts import ReferenceProviderExhausted

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
        if path in OKX_CONTRACT_STATISTICS_LIMITS:
            inst = str(params.get("instId") or "").upper()
            parts = inst.split("-")
            if len(parts) != 3 or not all(parts):
                raise ValueError("admitted OKX statistics requires one exact SWAP/FUTURES instId")
            if parts[-1] == "SWAP":
                market = "SWAP"
            elif len(parts[-1]) == 6 and parts[-1].isascii() and parts[-1].isdigit():
                market = "FUTURES"
            else:
                raise ValueError("admitted OKX statistics requires a SWAP or dated FUTURES instId")
            return ProviderLane("OKX", market, "REFERENCE_CONTRACT_STATISTICS")
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
                provider_code=50011 if code == "50011" else None,
                retry_after_ms=statistics_retry_after_ms(response) if path in OKX_CONTRACT_STATISTICS_LIMITS else None)
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
                if path in OKX_CONTRACT_STATISTICS_LIMITS:
                    await (await self._bucket_for(path, params, bucket)).acquire()
                return await asyncio.to_thread(self._leased_get, path, params)
            except ProviderRateLimited:
                raise
            except (requests.RequestException, ValueError) as error:
                last_error = error
                if attempt + 1 < attempts:
                    delay = min(4.0, 0.5 * 2**attempt)
                    await asyncio.sleep(delay + random.random() * delay * 0.2)
        raise RuntimeError(f"OKX V5 request exhausted retries: {path}") from last_error


class AsyncAdmittedOkxStatisticsClient:
    """New reference pages share Query's existing async Rust admission runtime."""

    def __init__(self, client: OkxRestClient, admission: ProviderAdmissionRuntime) -> None:
        self._client = client
        self._admission = admission

    async def _page(self, path: str, params: Mapping[str, str], bucket: str, lane: ProviderLane):
        try:
            return await self._client.get(path, params=params, bucket=bucket, attempts=1)
        except OkxStatisticsRateLimited as error:
            try:
                await self._admission.record_rate_limit(
                    lane, None, http_status=error.http_status,
                    provider_code=error.provider_code, retry_after_ms=error.retry_after_ms,
                )
            except (AdmissionContractError, AdmissionTransportError):
                pass  # The typed provider failure still reaches the caller.
            raise

    async def get(self, path: str, *, params: Mapping[str, str], bucket: str,
                  attempts: int = 3) -> list[dict[str, Any]]:
        if path not in OKX_CONTRACT_STATISTICS_LIMITS:
            raise ValueError("async OKX admission serves contract statistics only")
        if not 1 <= attempts <= 3:
            raise ValueError("OKX statistics attempts must be between 1 and 3")
        lane = AdmittedOkxRestClient.lane_for(path, params)
        for attempt in range(attempts):
            request = AdmissionRequest(
                lane=lane, request_id=request_id("okx-stats"),
                priority=AdmissionPriority.BATCH, token_cost=1,
            )
            try:
                decision = await self._admission.admit(request)
                decision.validate_for(request)
            except (AdmissionContractError, AdmissionTransportError) as error:
                raise ReferenceProviderExhausted("OKX statistics shared admission failed") from error
            if decision.disposition is not AdmissionDisposition.GRANTED:
                raise ReferenceProviderExhausted(
                    "OKX statistics deferred by shared admission", retry_after_ms=decision.retry_after_ms,
                )
            try:
                try:
                    # requests runs in a worker thread: cancellation cannot end
                    # the HTTP operation. Retain its lease until the page exits.
                    page = asyncio.create_task(self._page(path, params, bucket, lane))
                    try:
                        return await asyncio.shield(page)
                    except asyncio.CancelledError:
                        while not page.done():
                            try:
                                await asyncio.shield(page)
                            except asyncio.CancelledError:
                                continue
                            except Exception:
                                break
                        if not page.cancelled():
                            try:
                                page.result()
                            except Exception:
                                pass
                        raise
                except RuntimeError as error:
                    if not isinstance(error.__cause__, (requests.RequestException, ValueError)):
                        raise
                    if attempt + 1 == attempts:
                        raise ReferenceProviderExhausted("OKX statistics exhausted admitted retries") from error
            finally:
                try:
                    completed = await asyncio.shield(self._admission.complete(lane, request.request_id))
                except (AdmissionContractError, AdmissionTransportError) as error:
                    raise ReferenceProviderExhausted("OKX statistics admission completion failed") from error
                if not completed:
                    raise ReferenceProviderExhausted("OKX statistics admission lease was not active at completion")
            delay = min(4.0, 0.5 * 2**attempt)
            await asyncio.sleep(delay + random.random() * delay * 0.2)
        raise AssertionError("bounded OKX statistics retry loop did not terminate")

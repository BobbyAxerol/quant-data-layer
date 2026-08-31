"""Private authenticated relay to the existing Rust-core admission endpoint.

The client has no policy or retry authority.  It signs a canonical operation,
checks the exact Rust response, and turns a transport/shape fault into a
fail-closed adapter error.  The endpoint is intentionally restricted to the
stable-internal Docker name; it is not a public V2 API surface.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from urllib.parse import urlparse

import httpx

from qdl.admission.contracts import (
    ADMISSION_AUTHORITY,
    ADMISSION_SCHEMA,
    AdmissionContractError,
    AdmissionDecision,
    AdmissionRequest,
    ProviderLane,
    RustAdmissionProjection,
)


_PATH = "/internal/provider-admission/v1"
_ALLOWED_URL = "http://rust_core:8300"


class AdmissionTransportError(RuntimeError):
    """The configured Rust authority cannot be reached or verified."""


class RustHttpProviderAdmission:
    """Persistent, bounded private client for the Rust coordinator."""

    def __init__(
        self,
        *,
        base_url: str,
        secret: bytes,
        timeout_seconds: float = 2.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if base_url.rstrip("/") != _ALLOWED_URL:
            parsed = urlparse(base_url)
            raise ValueError(
                "provider admission URL must be the fixed stable-internal rust_core endpoint"
                if parsed.scheme or parsed.netloc
                else "provider admission URL is invalid"
            )
        if len(secret) < 32:
            raise ValueError("provider admission secret must contain at least 256 bits")
        if not 0 < timeout_seconds <= 5:
            raise ValueError("provider admission timeout must be within 0..5 seconds")
        self._secret = bytes(secret)
        self._client = client or httpx.AsyncClient(
            base_url=_ALLOWED_URL,
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        )
        self._owns_client = client is None

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        response = await RustAdmissionProjection(self).admit(request)
        return response

    async def decide(self, request: Mapping[str, object]) -> Mapping[str, object]:
        response = await self._post({"operation": "ADMIT", **dict(request)})
        return _mapping(response, "Rust admission decision")

    async def complete(self, lane: ProviderLane, request_id: str) -> bool:
        response = _mapping(
            await self._post({
                "schema": ADMISSION_SCHEMA,
                "operation": "COMPLETE",
                "lane": lane.as_mapping(),
                "request_id": request_id,
            }),
            "Rust admission completion",
        )
        _exact_keys(
            response,
            {"schema", "authority", "operation", "lane", "request_id", "completed"},
            "Rust admission completion",
        )
        if (
            response["schema"] != ADMISSION_SCHEMA
            or response["authority"] != ADMISSION_AUTHORITY
            or response["operation"] != "COMPLETE"
            or ProviderLane.from_mapping(_mapping(response["lane"], "completion lane")) != lane
            or response["request_id"] != request_id
            or not isinstance(response["completed"], bool)
        ):
            raise AdmissionContractError("Rust admission completion is invalid")
        return bool(response["completed"])

    async def record_rate_limit(
        self,
        lane: ProviderLane,
        request_id: str | None,
        *,
        http_status: int | None,
        provider_code: int | None,
        retry_after_ms: int | None,
    ) -> AdmissionDecision:
        if http_status not in {None, 418, 429} or provider_code not in {None, -1003}:
            raise AdmissionContractError("provider rate-limit signal is not recognized")
        if http_status is None and provider_code is None:
            raise AdmissionContractError("provider rate-limit signal is empty")
        if retry_after_ms is not None and retry_after_ms <= 0:
            raise AdmissionContractError("provider retry_after_ms must be positive")
        response = await self._post({
            "schema": ADMISSION_SCHEMA,
            "operation": "RATE_LIMIT",
            "lane": lane.as_mapping(),
            "request_id": request_id,
            "signal": {
                "http_status": http_status,
                "provider_code": provider_code,
                "retry_after_ns": retry_after_ms * 1_000_000 if retry_after_ms else None,
            },
        })
        decision = AdmissionDecision.from_rust_mapping(
            _mapping(response, "Rust rate-limit decision")
        )
        if decision.lane != lane:
            raise AdmissionContractError("Rust rate-limit decision lane does not match")
        return decision

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _post(self, payload: Mapping[str, object]) -> object:
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        signature = "sha256=" + hmac.new(self._secret, body, hashlib.sha256).hexdigest()
        try:
            response = await self._client.post(
                _PATH,
                content=body,
                headers={
                    "content-type": "application/json",
                    "x-qdl-stable-signature": signature,
                },
            )
            if response.status_code != 200:
                raise AdmissionTransportError(
                    f"Rust provider admission returned HTTP {response.status_code}"
                )
            return response.json()
        except (httpx.HTTPError, json.JSONDecodeError, ValueError) as error:
            if isinstance(error, AdmissionTransportError):
                raise
            raise AdmissionTransportError("Rust provider admission transport failed") from error


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AdmissionContractError(f"{label} must be an object")
    return value


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown or missing:
        raise AdmissionContractError(
            f"{label} fields mismatch missing={sorted(missing)} unknown={sorted(unknown)}"
        )

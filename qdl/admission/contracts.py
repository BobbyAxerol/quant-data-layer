"""Strict Python projection of the Rust provider-admission contract.

This module intentionally has no local token bucket, cooldown, retry or
provider policy. It validates and relays decisions created by Rust so a future
provider adapter cannot widen a grant or reinterpret a deferred response.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol


ADMISSION_SCHEMA = "qdl.provider_admission.v1"
ADMISSION_AUTHORITY = "RUST_QDL_CORE_V1"


class AdmissionContractError(ValueError):
    """A Rust admission payload is malformed or does not match its request."""


class AdmissionPriority(str, Enum):
    REALTIME = "REALTIME"
    BATCH = "BATCH"


class AdmissionDisposition(str, Enum):
    GRANTED = "GRANTED"
    DEFERRED = "DEFERRED"


class AdmissionDeferReason(str, Enum):
    COOLDOWN = "COOLDOWN"
    TOKEN_BUDGET = "TOKEN_BUDGET"
    INFLIGHT_CAPACITY = "INFLIGHT_CAPACITY"


@dataclass(frozen=True, slots=True)
class ProviderLane:
    provider: str
    market: str
    endpoint_family: str

    def __post_init__(self) -> None:
        for name, value in (
            ("provider", self.provider),
            ("market", self.market),
            ("endpoint_family", self.endpoint_family),
        ):
            if not _is_lane_segment(value):
                raise AdmissionContractError(
                    f"{name} must be 1..64 ASCII uppercase/digit/_/- characters"
                )

    def as_mapping(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "market": self.market,
            "endpoint_family": self.endpoint_family,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ProviderLane":
        _exact_keys(value, {"provider", "market", "endpoint_family"}, "lane")
        return cls(
            provider=_string(value["provider"], "lane.provider"),
            market=_string(value["market"], "lane.market"),
            endpoint_family=_string(value["endpoint_family"], "lane.endpoint_family"),
        )


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    lane: ProviderLane
    request_id: str
    priority: AdmissionPriority
    token_cost: int

    def __post_init__(self) -> None:
        if not _is_request_id(self.request_id):
            raise AdmissionContractError(
                "request_id must be 1..128 ASCII alphanumeric/._:- characters"
            )
        if self.token_cost < 1:
            raise AdmissionContractError("token_cost must be positive")

    def as_mapping(self) -> dict[str, object]:
        return {
            "schema": ADMISSION_SCHEMA,
            "lane": self.lane.as_mapping(),
            "request": {
                "request_id": self.request_id,
                "priority": self.priority.value,
                "token_cost": self.token_cost,
            },
        }


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    lane: ProviderLane
    request_id: str
    disposition: AdmissionDisposition
    defer_reason: AdmissionDeferReason | None
    retry_after_ms: int | None
    lease_expires_at_ns: int | None
    coalesced: bool

    @classmethod
    def from_rust_mapping(cls, value: Mapping[str, object]) -> "AdmissionDecision":
        _exact_keys(
            value,
            {
                "schema",
                "authority",
                "lane",
                "request_id",
                "disposition",
                "defer_reason",
                "retry_after_ms",
                "lease_expires_at_ns",
                "coalesced",
            },
            "Rust admission decision",
        )
        if value["schema"] != ADMISSION_SCHEMA or value["authority"] != ADMISSION_AUTHORITY:
            raise AdmissionContractError("admission decision is not Rust-authoritative v1")
        lane_value = value["lane"]
        if not isinstance(lane_value, Mapping):
            raise AdmissionContractError("admission decision lane must be an object")
        try:
            disposition = AdmissionDisposition(_string(value["disposition"], "disposition"))
        except ValueError as error:
            raise AdmissionContractError("admission disposition is invalid") from error
        defer_reason = _optional_enum(value["defer_reason"], AdmissionDeferReason, "defer_reason")
        retry_after_ms = _optional_positive_int(value["retry_after_ms"], "retry_after_ms")
        lease_expires_at_ns = _optional_positive_int(
            value["lease_expires_at_ns"], "lease_expires_at_ns"
        )
        coalesced = value["coalesced"]
        if not isinstance(coalesced, bool):
            raise AdmissionContractError("coalesced must be boolean")
        decision = cls(
            lane=ProviderLane.from_mapping(lane_value),
            request_id=_string(value["request_id"], "request_id"),
            disposition=disposition,
            defer_reason=defer_reason,
            retry_after_ms=retry_after_ms,
            lease_expires_at_ns=lease_expires_at_ns,
            coalesced=coalesced,
        )
        decision._validate_shape()
        return decision

    def validate_for(self, request: AdmissionRequest) -> None:
        if self.lane != request.lane or self.request_id != request.request_id:
            raise AdmissionContractError("Rust admission decision identity does not match request")
        self._validate_shape()

    def _validate_shape(self) -> None:
        if self.disposition is AdmissionDisposition.GRANTED:
            if (
                self.defer_reason is not None
                or self.retry_after_ms is not None
                or self.lease_expires_at_ns is None
            ):
                raise AdmissionContractError("GRANTED admission decision shape is invalid")
            return
        if (
            self.defer_reason is None
            or self.retry_after_ms is None
            or self.lease_expires_at_ns is not None
            or self.coalesced
        ):
            raise AdmissionContractError("DEFERRED admission decision shape is invalid")


class ProviderAdmissionClient(Protocol):
    """Transport owned by C3.6-C.2's Rust-core runtime binding."""

    async def decide(self, request: Mapping[str, object]) -> Mapping[str, object]: ...


class ProviderAdmissionRuntime(Protocol):
    """Private Rust admission operations used by a provider edge.

    This protocol intentionally exposes only exact Rust operations.  It has no
    local policy, queue, retry, cooldown, or provider transport capability.
    """

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision: ...

    async def complete(self, lane: ProviderLane, request_id: str) -> bool: ...

    async def record_rate_limit(
        self,
        lane: ProviderLane,
        request_id: str | None,
        *,
        http_status: int | None,
        provider_code: int | None,
        retry_after_ms: int | None,
    ) -> AdmissionDecision: ...


@dataclass(frozen=True, slots=True)
class RustAdmissionProjection:
    """Thin adapter gate: submit exact request, validate exact Rust response."""

    client: ProviderAdmissionClient

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        response = await self.client.decide(request.as_mapping())
        decision = AdmissionDecision.from_rust_mapping(response)
        decision.validate_for(request)
        return decision


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown or missing:
        raise AdmissionContractError(
            f"{label} fields mismatch missing={sorted(missing)} unknown={sorted(unknown)}"
        )


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise AdmissionContractError(f"{label} must be a non-empty string")
    return value


def _optional_positive_int(value: object, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AdmissionContractError(f"{label} must be a positive integer or null")
    return value


def _optional_enum(
    value: object,
    enum: type[AdmissionDeferReason],
    label: str,
) -> AdmissionDeferReason | None:
    if value is None:
        return None
    try:
        return enum(_string(value, label))
    except ValueError as error:
        raise AdmissionContractError(f"{label} is invalid") from error


def _is_lane_segment(value: str) -> bool:
    return bool(value) and len(value) <= 64 and all(
        character.isascii() and (character.isupper() or character.isdigit() or character in "_-")
        for character in value
    )


def _is_request_id(value: str) -> bool:
    return bool(value) and len(value) <= 128 and all(
        character.isascii() and (character.isalnum() or character in "._:-")
        for character in value
    )

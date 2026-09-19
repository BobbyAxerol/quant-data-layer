"""Private stable-stream reader for execution-grade MARK/INDEX snapshots.

The public V2 reference contract remains unchanged.  This client replaces only
the provider REST call for an execution request after the stable stream gateway
has already admitted the canonical paired event under its writer lease.
"""

from __future__ import annotations

import asyncio
import base64
import json
import ssl
import time
from dataclasses import dataclass, field
from typing import Protocol

import httpx
from google.protobuf.message import DecodeError

from qdl.domain.capabilities import FeedCapability
from qdl.reference.batch import CapabilityResolver, default_capability_resolver
from qdl.reference.contracts import (
    MarkIndexKind,
    ReferenceBatchResult,
    ReferenceCoverage,
    ReferenceField,
    ReferenceLineage,
    ReferenceObservation,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
    decimal_field,
    product_feed_name,
)
from qdl.runtime.internal_auth import is_stable_internal_url, stable_hmac_signature
from qdl.runtime.mark_index_lineage import paired_mark_index_lineage
from qdl.marketdata.v2 import market_data_pb2
from qdl.query.contracts import StalePolicy


_LEGACY_REQUEST_SCHEMA = "qdl.v2.execution-mark-index-read.v1"
_REQUEST_SCHEMA = "qdl.v2.execution-mark-index-read.v2"
_RESPONSE_SCHEMA = "qdl.v2.execution-mark-index-view.v2"
_ENDPOINT = "/internal/v2/execution/mark-index/latest"
_FRESHNESS_BASIS_HEADER = "X-QDL-Execution-Freshness-Basis"
_RECENCY_MODE_HEADER = "X-QDL-Execution-Recency-Mode"
_SESSION_STATE_HEADER = "X-QDL-Execution-Session-State"
_SESSION_LIVENESS_HEADER = "X-QDL-Execution-Session-Liveness-Ms"
_SESSION_CHECKED_AT_HEADER = "X-QDL-Execution-Session-Checked-At-Ns"
_COMPONENT_RECEIPTS_HEADER = "X-QDL-Execution-Component-Receipts-Ns"
_COMPONENT_CADENCE_HEADER = "X-QDL-Execution-Component-Quiet-After-Ms"


class ExecutionMarkIndexReader(Protocol):
    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        max_freshness_ms: int,
        source_policy_id: str,
        event_recency_policy: StalePolicy = StalePolicy.BLOCK,
        max_session_liveness_ms: int | None = None,
        deadline_ms: int | None = None,
    ) -> ReferenceBatchResult: ...

    def stats(self) -> dict[str, int]: ...


@dataclass(slots=True)
class HttpExecutionMarkIndexReader:
    """Read the current active gateway view, never venue REST, for execution."""

    urls: tuple[str, ...]
    secret: bytes
    capability_resolver: CapabilityResolver = default_capability_resolver
    timeout_seconds: float = 2.0
    client: httpx.AsyncClient | None = None
    ssl_context: ssl.SSLContext | None = None
    _owns_client: bool = field(init=False)
    _calls: int = field(init=False, default=0)
    _successes: int = field(init=False, default=0)
    _failures: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        if (
            not self.urls
            or any(not is_stable_internal_url(value) for value in self.urls)
            or len(self.secret) < 32
            or not 0.1 <= self.timeout_seconds <= 10.0
        ):
            raise ValueError("execution MARK/INDEX live reader configuration is invalid")
        if self.client is not None and self.ssl_context is not None:
            raise ValueError("execution MARK/INDEX client and TLS context are mutually exclusive")
        self._owns_client = self.client is None
        if self.client is None:
            self.client = httpx.AsyncClient(
                follow_redirects=False,
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
                timeout=self.timeout_seconds,
                verify=self.ssl_context or True,
            )

    async def fetch(
        self,
        request: ReferenceRequest,
        *,
        max_freshness_ms: int,
        source_policy_id: str,
        event_recency_policy: StalePolicy = StalePolicy.BLOCK,
        max_session_liveness_ms: int | None = None,
        deadline_ms: int | None = None,
    ) -> ReferenceBatchResult:
        """Fetch one exact current snapshot without an external-provider fallback."""

        self._calls += 1
        if deadline_ms is not None and deadline_ms < 1:
            raise ValueError("execution MARK/INDEX read deadline must be positive")
        capability = self._capability(request)
        if (
            request.product is not ReferenceProduct.MARK_INDEX_PRICE
            or request.is_history
            or max_freshness_ms <= 0
            or not source_policy_id.strip()
            or not isinstance(event_recency_policy, StalePolicy)
            or (
                event_recency_policy is StalePolicy.OBSERVE
                and (max_session_liveness_ms is None or max_session_liveness_ms <= 0)
            )
        ):
            self._failures += 1
            return self._failure(
                request, capability, "LIVE_VIEW_INVALID_REQUEST",
                "execution live view accepts only a current MARK_INDEX_PRICE request",
            )
        quiet_request = event_recency_policy is StalePolicy.OBSERVE
        payload = {
            "schema": _REQUEST_SCHEMA if quiet_request else _LEGACY_REQUEST_SCHEMA,
            "instrument_uid": request.instrument.instrument_uid,
            "instrument_revision": request.instrument.metadata_revision,
            "source_policy_id": source_policy_id,
            "max_freshness_ms": max_freshness_ms,
        }
        if quiet_request:
            assert max_session_liveness_ms is not None
            payload.update({
                "event_recency_policy": event_recency_policy.value,
                "max_session_liveness_ms": max_session_liveness_ms,
            })
        body = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        errors: list[str] = []
        deadline_at = (
            time.monotonic() + deadline_ms / 1_000 if deadline_ms is not None else None
        )
        assert self.client is not None
        for url in self.urls:
            timeout_seconds = self.timeout_seconds
            if deadline_at is not None:
                timeout_seconds = min(timeout_seconds, deadline_at - time.monotonic())
                if timeout_seconds <= 0:
                    errors.append("DEADLINE")
                    break
            try:
                response = await asyncio.wait_for(
                    self.client.post(
                        f"{url.rstrip('/')}{_ENDPOINT}",
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "X-QDL-Stable-Signature": stable_hmac_signature(self.secret, body),
                        },
                        timeout=timeout_seconds,
                    ),
                    timeout=timeout_seconds,
                )
            except (asyncio.TimeoutError, httpx.TransportError):
                errors.append("TIMEOUT" if deadline_at is not None else "TRANSPORT")
                continue
            if response.status_code == 409:
                errors.append(self._bounded_reason(response))
                continue
            if response.status_code != 200:
                errors.append(f"HTTP_{response.status_code}")
                continue
            try:
                result = self._result_from_response(
                    request,
                    capability,
                    response,
                    freshness_basis=response.headers.get(
                        _FRESHNESS_BASIS_HEADER, "SOURCE_EVENT"
                    ).strip().upper(),
                    event_recency_policy=event_recency_policy,
                    max_session_liveness_ms=max_session_liveness_ms,
                )
            except (DecodeError, ValueError, TypeError, KeyError):
                errors.append("PROTOCOL")
                continue
            self._successes += 1
            return result
        self._failures += 1
        code = self._failure_code(errors)
        return self._failure(
            request,
            capability,
            code,
            "execution MARK/INDEX live view did not return a current active record",
        )

    def stats(self) -> dict[str, int]:
        return {
            "calls": self._calls,
            "successes": self._successes,
            "failures": self._failures,
        }

    async def close(self) -> None:
        if self._owns_client and self.client is not None:
            await self.client.aclose()

    def _capability(self, request: ReferenceRequest) -> FeedCapability:
        profile = self.capability_resolver(request.instrument)
        capability = profile.capability(product_feed_name(request.product))
        if not capability.enabled:
            raise ValueError("execution MARK/INDEX live capability is unavailable")
        return capability

    @staticmethod
    def _bounded_reason(response: httpx.Response) -> str:
        try:
            detail = response.json().get("detail", "")
        except (ValueError, AttributeError):
            return "UNAVAILABLE"
        normalized = str(detail).upper()
        if normalized.endswith(":STALE"):
            return "STALE"
        if normalized.endswith(":GAP_OR_RESYNC"):
            return "GAPPED"
        if normalized.endswith(":IDENTITY_MISMATCH"):
            return "IDENTITY_MISMATCH"
        if normalized.endswith(":SOURCE_POLICY_MISMATCH"):
            return "SOURCE_POLICY_MISMATCH"
        if "FENCED" in normalized:
            return "FENCED"
        return "NOT_READY"

    @staticmethod
    def _failure_code(errors: list[str]) -> str:
        if "STALE" in errors:
            return "LIVE_VIEW_STALE"
        if "GAPPED" in errors:
            return "LIVE_VIEW_GAPPED"
        if "IDENTITY_MISMATCH" in errors or "SOURCE_POLICY_MISMATCH" in errors:
            return "LIVE_VIEW_IDENTITY"
        if "PROTOCOL" in errors:
            return "LIVE_VIEW_PROTOCOL"
        return "LIVE_VIEW_UNAVAILABLE"

    def _result_from_response(
        self,
        request: ReferenceRequest,
        capability: FeedCapability,
        response: httpx.Response,
        *,
        freshness_basis: str,
        event_recency_policy: StalePolicy,
        max_session_liveness_ms: int | None,
    ) -> ReferenceBatchResult:
        payload = response.json()
        if (
            set(payload) != {
                "schema", "lease_epoch", "spool_watermark_offset",
                "delivery_stage", "canonical",
            }
            or payload["schema"] != _RESPONSE_SCHEMA
            or int(payload["lease_epoch"]) < 1
            or payload["delivery_stage"] not in {
                "CANONICAL_READ_COMMITTED", "SPOOL_CONFIRMED",
            }
            or freshness_basis not in {"SOURCE_EVENT", "PROVIDER_CONFIRMATION"}
        ):
            raise ValueError("execution MARK/INDEX live view response is invalid")
        spool_watermark_offset = payload["spool_watermark_offset"]
        if spool_watermark_offset is not None and int(spool_watermark_offset) < 0:
            raise ValueError("execution MARK/INDEX spool watermark is invalid")
        canonical = base64.b64decode(str(payload["canonical"]), validate=True)
        envelope = market_data_pb2.EventEnvelope.FromString(canonical)
        if (
            envelope.WhichOneof("payload") != "mark_index_price"
            or envelope.instrument_uid != request.instrument.instrument_uid
            or int(envelope.instrument_revision) != request.instrument.metadata_revision
            or envelope.instrument_id != request.instrument.instrument_id
            or envelope.venue != request.instrument.identity.venue
            or envelope.market != request.instrument.identity.market
            or envelope.native_symbol != request.instrument.native_symbol
            or envelope.source_event_time_ns <= 0
            or envelope.received_at_ns < envelope.source_event_time_ns
            or not envelope.source_session_id
            or envelope.connection_generation < 1
            or envelope.config_revision < 1
        ):
            raise ValueError("execution MARK/INDEX live view identity/provenance mismatch")
        fields = self._fields(request, envelope)
        observed_at_ns = int(envelope.source_event_time_ns)
        labels = [
            ("native_symbol", request.instrument.native_symbol),
            ("execution_view", "STABLE_STREAM_GATEWAY"),
            ("freshness_basis", freshness_basis),
            ("source_event_time_ns", str(observed_at_ns)),
            ("provider_confirmation_ns", str(int(envelope.received_at_ns))),
            ("connection_generation", str(int(envelope.connection_generation))),
            ("gateway_lease_epoch", str(int(payload["lease_epoch"]))),
            ("delivery_stage", str(payload["delivery_stage"])),
            (
                "spool_watermark_offset",
                "PENDING" if spool_watermark_offset is None
                else str(int(spool_watermark_offset)),
            ),
        ]
        if event_recency_policy is StalePolicy.OBSERVE:
            labels.extend(self._quiet_labels(
                envelope,
                response,
                max_session_liveness_ms=max_session_liveness_ms,
            ))
        observation = ReferenceObservation(
            instrument_uid=request.instrument.instrument_uid,
            instrument_revision=request.instrument.metadata_revision,
            product=ReferenceProduct.MARK_INDEX_PRICE,
            observed_at_ns=observed_at_ns,
            fields=fields,
            labels=tuple(labels),
        )
        lineage = ReferenceLineage(
            provider=self.capability_resolver(request.instrument).provider,
            provider_endpoint="qdl://stable-stream/internal/v2/execution/mark-index/latest",
            source_role="REFERENCE",
            adapter_version=(
                "qdl-execution-mark-index-live/1+" + envelope.adapter_version
            ),
            capability_name="mark_index_price",
            capability_constraint=capability.constraint,
        )
        observed_ms = observed_at_ns // 1_000_000
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.OK,
            capability=capability,
            lineage=(lineage,),
            coverage=ReferenceCoverage(
                requested_start_ms=None,
                requested_end_ms=None,
                observed_min_ms=observed_ms,
                observed_max_ms=observed_ms,
                complete_left=True,
                complete_right=True,
                truncated=False,
                terminal_reason="LIVE_EXECUTION_VIEW",
            ),
            received_at_ns=int(envelope.received_at_ns),
            observations=(observation,),
        )

    @staticmethod
    def _quiet_labels(
        envelope: market_data_pb2.EventEnvelope,
        response: httpx.Response,
        *,
        max_session_liveness_ms: int | None,
    ) -> tuple[tuple[str, str], ...]:
        if max_session_liveness_ms is None:
            raise ValueError("quiet execution MARK/INDEX request has no session SLA")
        recency_mode = response.headers.get(_RECENCY_MODE_HEADER, "").strip()
        if recency_mode not in {
            "STRICT_EVENT_SESSION_LIVE",
            "COMPONENT_SESSION_LIVE",
        }:
            raise ValueError("quiet execution MARK/INDEX response mode is invalid")
        session_state = response.headers.get(_SESSION_STATE_HEADER, "").strip()
        if session_state != "LIVE":
            raise ValueError("quiet execution MARK/INDEX session state is invalid")
        try:
            session_liveness_ms = int(response.headers[_SESSION_LIVENESS_HEADER])
            session_checked_at_ns = int(response.headers[_SESSION_CHECKED_AT_HEADER])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("quiet execution MARK/INDEX session evidence is invalid") from error
        if (
            not 0 <= session_liveness_ms <= max_session_liveness_ms
            or session_checked_at_ns <= 0
        ):
            raise ValueError("quiet execution MARK/INDEX session evidence exceeds policy")
        receipts = _component_header_values(
            response.headers.get(_COMPONENT_RECEIPTS_HEADER, "")
        )
        cadence = _component_header_values(
            response.headers.get(_COMPONENT_CADENCE_HEADER, "")
        )
        lineage = paired_mark_index_lineage(envelope)
        if receipts != {
            "MARK": lineage.mark_received_at_ns,
            "INDEX": lineage.index_received_at_ns,
        }:
            raise ValueError("quiet execution MARK/INDEX receipt lineage differs")
        if any(not 250 <= value <= 120_000 for value in cadence.values()):
            raise ValueError("quiet execution MARK/INDEX cadence is invalid")
        return (
            ("event_recency_policy", StalePolicy.OBSERVE.value),
            ("recency_mode", recency_mode),
            ("provider_session_state", session_state),
            ("provider_session_liveness_ms", str(session_liveness_ms)),
            ("provider_session_checked_at_ns", str(session_checked_at_ns)),
            ("component_mark_received_at_ns", str(receipts["MARK"])),
            ("component_index_received_at_ns", str(receipts["INDEX"])),
            ("component_mark_quiet_after_ms", str(cadence["MARK"])),
            ("component_index_quiet_after_ms", str(cadence["INDEX"])),
        )

    @staticmethod
    def _fields(
        request: ReferenceRequest,
        envelope: market_data_pb2.EventEnvelope,
    ) -> tuple[ReferenceField, ...]:
        values: list[ReferenceField] = []
        pairs = (
            ("mark_price", envelope.mark_index_price.mark_price),
            ("index_price", envelope.mark_index_price.index_price),
        )
        required = {
            MarkIndexKind.MARK: {"mark_price"},
            MarkIndexKind.INDEX: {"index_price"},
            MarkIndexKind.BOTH: {"mark_price", "index_price"},
        }[request.mark_index_kind]
        for name, value in pairs:
            if name not in required:
                continue
            field = decimal_field(name, value.source_text, "QUOTE_PRICE")
            if field is None or field.value.as_decimal() <= 0:
                raise ValueError("execution MARK/INDEX live view price is invalid")
            values.append(field)
        if {item.name for item in values} != required:
            raise ValueError("execution MARK/INDEX live view fields are incomplete")
        return tuple(values)

    @staticmethod
    def _failure(
        request: ReferenceRequest,
        capability: FeedCapability,
        code: str,
        detail: str,
    ) -> ReferenceBatchResult:
        return ReferenceBatchResult(
            request=request,
            status=ReferenceStatus.ERROR,
            capability=capability,
            lineage=(ReferenceLineage(
                provider=request.instrument.identity.venue.upper() + "_DIRECT",
                provider_endpoint="qdl://stable-stream/internal/v2/execution/mark-index/latest",
                source_role="REFERENCE",
                adapter_version="qdl-execution-mark-index-live/1",
                capability_name="mark_index_price",
                capability_constraint=capability.constraint,
            ),),
            coverage=ReferenceCoverage(
                requested_start_ms=None,
                requested_end_ms=None,
                observed_min_ms=None,
                observed_max_ms=None,
                complete_left=False,
                complete_right=False,
                truncated=False,
                terminal_reason=code,
            ),
            received_at_ns=time.time_ns(),
            error_code=code,
            error_detail=detail,
        )


def _component_header_values(value: str) -> dict[str, int]:
    """Parse one bounded private header without accepting partial components."""

    result: dict[str, int] = {}
    for item in value.split(","):
        name, separator, raw = item.partition("=")
        if not separator or name in result:
            raise ValueError("quiet execution MARK/INDEX component header is invalid")
        try:
            parsed = int(raw)
        except ValueError as error:
            raise ValueError(
                "quiet execution MARK/INDEX component header is invalid"
            ) from error
        if name not in {"MARK", "INDEX"} or parsed <= 0:
            raise ValueError("quiet execution MARK/INDEX component header is invalid")
        result[name] = parsed
    if set(result) != {"MARK", "INDEX"}:
        raise ValueError("quiet execution MARK/INDEX component header is incomplete")
    return result

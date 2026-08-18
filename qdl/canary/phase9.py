from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from qdl.certification.prerequisites import CandidateSlice


_REQUIRED_PRODUCTION_GATES = frozenset({
    "replicated_durable_transport",
    "production_observability",
    "workload_identity_rbac_network",
    "external_secret_rotation",
    "signed_artifact_admission",
    "postgres_pitr",
    "object_store_restore",
    "independent_failure_domain_dr",
    "redis_projector_rebuild",
    "consumer_registration_rollback",
    "persistent_authority_sink_fencing",
    "exact_slice_approval",
})


class CanaryAuthorizationMode(StrEnum):
    PRODUCTION = "PRODUCTION"
    ISOLATED_REHEARSAL = "ISOLATED_REHEARSAL"


@dataclass(frozen=True, slots=True)
class CanaryAuthorization:
    allowed: bool
    production_authorized: bool
    mode: CanaryAuthorizationMode
    reason: str
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str | None


class ProductionCanaryAuthorizer:
    """Fail-closed Phase 9.1 gate; it never mutates authority state."""

    def __init__(self, *, max_decision_age_seconds: int = 900) -> None:
        if max_decision_age_seconds <= 0:
            raise ValueError("canary decision age must be positive")
        self.max_decision_age_ns = max_decision_age_seconds * 1_000_000_000

    def authorize(
        self,
        *,
        candidate: CandidateSlice,
        decision: Mapping[str, Any],
        expected_bundle_id: str,
        now_ns: int,
    ) -> CanaryAuthorization:
        reason = self._validate(
            candidate=candidate,
            decision=decision,
            expected_bundle_id=expected_bundle_id,
            now_ns=now_ns,
        )
        return CanaryAuthorization(
            allowed=reason == "AUTHORIZED",
            production_authorized=reason == "AUTHORIZED",
            mode=CanaryAuthorizationMode.PRODUCTION,
            reason=reason,
            slice_id=str(candidate.payload["slice_id"]),
            candidate_digest=candidate.digest,
            prerequisite_bundle_id=str(decision.get("bundle_id") or "") or None,
        )

    @staticmethod
    def authorize_isolated_rehearsal(*, candidate: CandidateSlice) -> CanaryAuthorization:
        return CanaryAuthorization(
            allowed=True,
            production_authorized=False,
            mode=CanaryAuthorizationMode.ISOLATED_REHEARSAL,
            reason="ISOLATED_REHEARSAL_ONLY",
            slice_id=str(candidate.payload["slice_id"]),
            candidate_digest=candidate.digest,
            prerequisite_bundle_id=None,
        )

    def _validate(
        self,
        *,
        candidate: CandidateSlice,
        decision: Mapping[str, Any],
        expected_bundle_id: str,
        now_ns: int,
    ) -> str:
        if now_ns <= 0:
            return "INVALID_CLOCK"
        if candidate.payload["authority_state"] != "RUST_SHADOW":
            return "CANDIDATE_NOT_SHADOW"
        if candidate.payload["public_write_allowed"] or candidate.payload["legacy_write_allowed"]:
            return "CANDIDATE_WRITE_AUTHORITY_INVALID"
        if decision.get("schema") != "qdl.production-prerequisite-decision.v1":
            return "DECISION_SCHEMA_INVALID"
        if decision.get("decision") != "GO":
            return "PREREQUISITE_DECISION_NOT_GO"
        if decision.get("candidate_digest") != candidate.digest:
            return "CANDIDATE_DIGEST_MISMATCH"
        if decision.get("slice_id") != candidate.payload["slice_id"]:
            return "SLICE_MISMATCH"
        bundle_id = str(decision.get("bundle_id") or "")
        try:
            uuid.UUID(bundle_id)
            uuid.UUID(expected_bundle_id)
        except ValueError:
            return "PREREQUISITE_BUNDLE_INVALID"
        if bundle_id != expected_bundle_id:
            return "PREREQUISITE_BUNDLE_MISMATCH"
        issued_at_ns = decision.get("issued_at_ns")
        if not isinstance(issued_at_ns, int) or isinstance(issued_at_ns, bool) or issued_at_ns <= 0:
            return "DECISION_TIME_INVALID"
        if issued_at_ns > now_ns + 60_000_000_000:
            return "DECISION_FROM_FUTURE"
        if now_ns - issued_at_ns > self.max_decision_age_ns:
            return "DECISION_EXPIRED"
        gates = decision.get("gates")
        if not isinstance(gates, list) or not gates:
            return "GATE_RESULTS_MISSING"
        gate_ids = [item.get("gate_id") for item in gates if isinstance(item, Mapping)]
        if (
            len(gate_ids) != len(gates)
            or len(gate_ids) != len(set(gate_ids))
            or set(gate_ids) != _REQUIRED_PRODUCTION_GATES
        ):
            return "GATE_RESULTS_INVALID"
        if any(item.get("passed") is not True or item.get("reason") != "PASS" for item in gates):
            return "GATE_NOT_PASSED"
        if decision.get("passed") != len(gates) or decision.get("blocked") != 0:
            return "GATE_SUMMARY_INVALID"
        if decision.get("authority_state") != "RUST_SHADOW":
            return "AUTHORITY_PRECONDITION_INVALID"
        if decision.get("v1_unchanged") is not True:
            return "V1_PRECONDITION_INVALID"
        if decision.get("production_mutations") != 0:
            return "PRODUCTION_MUTATION_DETECTED"
        return "AUTHORIZED"


@dataclass(frozen=True, slots=True)
class CanaryGuardrailPolicy:
    max_lag_ms: int
    max_freshness_ms: int
    max_cpu_percent: float
    max_rss_mb: float
    max_queue_depth: int
    hold_down_seconds: int

    def __post_init__(self) -> None:
        if any(value <= 0 for value in (
            self.max_lag_ms,
            self.max_freshness_ms,
            self.max_cpu_percent,
            self.max_rss_mb,
            self.max_queue_depth,
            self.hold_down_seconds,
        )):
            raise ValueError("canary guardrail thresholds must be positive")


@dataclass(frozen=True, slots=True)
class CanaryObservation:
    observed_at_ns: int
    semantic_mismatches: int = 0
    open_gaps: int = 0
    duplicate_external_writes: int = 0
    stale_writer_attempts: int = 0
    authority_ambiguities: int = 0
    durable_ack_failures: int = 0
    lag_ms: int = 0
    freshness_ms: int = 0
    cpu_percent: float = 0.0
    rss_mb: float = 0.0
    queue_depth: int = 0

    def __post_init__(self) -> None:
        values = (
            self.observed_at_ns,
            self.semantic_mismatches,
            self.open_gaps,
            self.duplicate_external_writes,
            self.stale_writer_attempts,
            self.authority_ambiguities,
            self.durable_ack_failures,
            self.lag_ms,
            self.freshness_ms,
            self.cpu_percent,
            self.rss_mb,
            self.queue_depth,
        )
        if any(value < 0 for value in values) or self.observed_at_ns <= 0:
            raise ValueError("canary observation values are invalid")


@dataclass(frozen=True, slots=True)
class CanaryGuardrailDecision:
    allowed: bool
    reason: str
    blocked_at_ns: int | None
    hold_until_ns: int | None


class CanaryGuardrailEngine:
    def __init__(self, policy: CanaryGuardrailPolicy) -> None:
        self.policy = policy
        self._blocked_at_ns: int | None = None
        self._hold_until_ns: int | None = None
        self._first_failure_reason: str | None = None

    def evaluate(self, observation: CanaryObservation) -> CanaryGuardrailDecision:
        if self._first_failure_reason is not None:
            return self._decision(False, "EXPLICIT_RESET_REQUIRED")
        reason = self._failure_reason(observation)
        if reason is not None:
            self._first_failure_reason = reason
            self._blocked_at_ns = observation.observed_at_ns
            self._hold_until_ns = (
                observation.observed_at_ns
                + self.policy.hold_down_seconds * 1_000_000_000
            )
            return self._decision(False, reason)
        return self._decision(True, "PASS")

    def reset_after_hold(self, observation: CanaryObservation) -> CanaryGuardrailDecision:
        if self._first_failure_reason is None:
            return self.evaluate(observation)
        assert self._hold_until_ns is not None
        if observation.observed_at_ns < self._hold_until_ns:
            return self._decision(False, "HOLD_DOWN_ACTIVE")
        reason = self._failure_reason(observation)
        if reason is not None:
            return self._decision(False, reason)
        self._blocked_at_ns = None
        self._hold_until_ns = None
        self._first_failure_reason = None
        return self._decision(True, "RESET_CONFIRMED")

    def _failure_reason(self, item: CanaryObservation) -> str | None:
        zero_tolerance = (
            (item.semantic_mismatches, "SEMANTIC_MISMATCH"),
            (item.open_gaps, "OPEN_GAP"),
            (item.duplicate_external_writes, "DUPLICATE_EXTERNAL_WRITE"),
            (item.stale_writer_attempts, "STALE_WRITER_ATTEMPT"),
            (item.authority_ambiguities, "AUTHORITY_AMBIGUITY"),
            (item.durable_ack_failures, "DURABLE_ACK_FAILURE"),
        )
        for value, reason in zero_tolerance:
            if value:
                return reason
        thresholds = (
            (item.lag_ms > self.policy.max_lag_ms, "LAG_THRESHOLD_EXCEEDED"),
            (item.freshness_ms > self.policy.max_freshness_ms, "FRESHNESS_THRESHOLD_EXCEEDED"),
            (item.cpu_percent > self.policy.max_cpu_percent, "CPU_THRESHOLD_EXCEEDED"),
            (item.rss_mb > self.policy.max_rss_mb, "RSS_THRESHOLD_EXCEEDED"),
            (item.queue_depth > self.policy.max_queue_depth, "QUEUE_THRESHOLD_EXCEEDED"),
        )
        return next((reason for failed, reason in thresholds if failed), None)

    def _decision(self, allowed: bool, reason: str) -> CanaryGuardrailDecision:
        return CanaryGuardrailDecision(
            allowed=allowed,
            reason=reason,
            blocked_at_ns=self._blocked_at_ns,
            hold_until_ns=self._hold_until_ns,
        )

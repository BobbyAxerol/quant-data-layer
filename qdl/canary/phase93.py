from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from qdl._compat import StrEnum
from typing import Any, Mapping

from qdl.canary.phase9 import ProductionCanaryAuthorizer
from qdl.certification.prerequisites import CandidateSlice


def _digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        char in "0123456789abcdef" for char in value
    )


def _valid_uuid(value: object) -> bool:
    try:
        uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _non_negative_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and value >= 0
    )


class HoldScope(StrEnum):
    TEST_REHEARSAL = "TEST_REHEARSAL"
    PRODUCTION = "PRODUCTION"


class HoldStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    PASSED = "PASSED"
    BLOCKED = "BLOCKED"


class ExpansionType(StrEnum):
    INSTRUMENT_PARTITION = "INSTRUMENT_PARTITION"
    BBO = "BBO"
    L2_BOOK = "L2_BOOK"
    BAR_LIFECYCLE = "BAR_LIFECYCLE"
    VENUE_MARKET = "VENUE_MARKET"


_EXPANSION_GATES: dict[ExpansionType, frozenset[str]] = {
    ExpansionType.INSTRUMENT_PARTITION: frozenset({
        "authority_handoff", "capacity_headroom", "exact_frame_parity",
        "partition_churn", "provider_authentic_source", "rollback",
        "source_capacity",
    }),
    ExpansionType.BBO: frozenset({
        "authority_handoff", "capacity_headroom", "coalescing_policy",
        "exact_frame_parity", "freshness", "ordering_reconnect",
        "provider_authentic_source", "quote_identity", "rollback",
    }),
    ExpansionType.L2_BOOK: frozenset({
        "authority_handoff", "capacity_headroom", "checksum",
        "exact_frame_parity", "lossless_backpressure",
        "provider_authentic_source", "resync", "rollback",
        "snapshot_delta_sequence",
    }),
    ExpansionType.BAR_LIFECYCLE: frozenset({
        "authority_handoff", "capacity_headroom", "close_time_semantics",
        "exact_frame_parity", "final_revision_lineage",
        "provider_authentic_source", "replay", "rollback",
    }),
    ExpansionType.VENUE_MARKET: frozenset({
        "adapter_capability", "authority_handoff", "capacity_headroom",
        "disaster_recovery", "entitlement", "exact_frame_parity",
        "instrument_identity", "provider_authentic_source",
        "provider_semantics", "rollback",
    }),
}


@dataclass(frozen=True, slots=True)
class PrimaryHoldPolicy:
    minimum_duration_seconds: int
    sample_interval_seconds: int
    max_sample_gap_seconds: int
    max_lag_ms: int
    max_freshness_ms: int
    max_queue_depth: int
    max_spool_bytes: int
    max_cpu_percent: float
    max_rss_mb: float

    def __post_init__(self) -> None:
        integers = (
            self.minimum_duration_seconds,
            self.sample_interval_seconds,
            self.max_sample_gap_seconds,
            self.max_lag_ms,
            self.max_freshness_ms,
            self.max_queue_depth,
            self.max_spool_bytes,
        )
        if any(not _positive_int(value) for value in integers):
            raise ValueError("hold policy integer thresholds must be positive")
        if self.max_sample_gap_seconds < self.sample_interval_seconds:
            raise ValueError("hold maximum sample gap cannot be below sample interval")
        if self.minimum_duration_seconds < self.max_sample_gap_seconds:
            raise ValueError("hold duration must cover one maximum sample gap")
        if self.max_cpu_percent <= 0 or self.max_cpu_percent > 100:
            raise ValueError("hold CPU threshold is invalid")
        if self.max_rss_mb <= 0:
            raise ValueError("hold RSS threshold is invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class PrimaryHoldIdentity:
    schema: str
    hold_id: str
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    started_at_ns: int
    required_until_ns: int
    policy_digest: str

    def __post_init__(self) -> None:
        if self.schema != "qdl.primary-hold.v1":
            raise ValueError("hold schema is invalid")
        if not _valid_uuid(self.hold_id) or not _valid_uuid(
            self.prerequisite_bundle_id
        ):
            raise ValueError("hold UUID identity is invalid")
        if not self.slice_id.strip() or not self.owner_id.strip():
            raise ValueError("hold owner/slice identity is required")
        if not _valid_digest(self.candidate_digest) or not _valid_digest(
            self.policy_digest
        ):
            raise ValueError("hold digest identity is invalid")
        if any(
            not _positive_int(value)
            for value in (
                self.authority_revision,
                self.lease_epoch,
                self.partition_plan_epoch,
                self.started_at_ns,
                self.required_until_ns,
            )
        ) or self.required_until_ns <= self.started_at_ns:
            raise ValueError("hold epoch/time identity is invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class PrimaryHoldObservation:
    schema: str
    observation_id: str
    hold_id: str
    slice_id: str
    candidate_digest: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    sequence: int
    observed_at_ns: int
    last_watermark: int
    semantic_mismatches: int = 0
    open_gaps: int = 0
    duplicate_external_writes: int = 0
    accepted_stale_writer_writes: int = 0
    authority_ambiguities: int = 0
    durable_ack_failures: int = 0
    projection_mismatches: int = 0
    consumer_checkpoint_regressions: int = 0
    unexplained_quality_failures: int = 0
    lag_ms: int = 0
    freshness_ms: int = 0
    queue_depth: int = 0
    spool_bytes: int = 0
    cpu_percent: float = 0.0
    rss_mb: float = 0.0
    registered_consumers: int = 0
    healthy_consumers: int = 0
    checkpoint_watermark: int = 0

    def __post_init__(self) -> None:
        if self.schema != "qdl.primary-hold-observation.v1":
            raise ValueError("hold observation schema is invalid")
        if not _valid_uuid(self.observation_id) or not _valid_uuid(self.hold_id):
            raise ValueError("hold observation UUID is invalid")
        if (
            not self.slice_id.strip()
            or not self.owner_id.strip()
            or not _valid_digest(self.candidate_digest)
        ):
            raise ValueError("hold observation identity is invalid")
        positive = (
            self.authority_revision,
            self.lease_epoch,
            self.partition_plan_epoch,
            self.sequence,
            self.observed_at_ns,
        )
        if any(not _positive_int(value) for value in positive):
            raise ValueError("hold observation epoch/sequence/time is invalid")
        integer_values = (
            self.last_watermark,
            self.semantic_mismatches,
            self.open_gaps,
            self.duplicate_external_writes,
            self.accepted_stale_writer_writes,
            self.authority_ambiguities,
            self.durable_ack_failures,
            self.projection_mismatches,
            self.consumer_checkpoint_regressions,
            self.unexplained_quality_failures,
            self.lag_ms,
            self.freshness_ms,
            self.queue_depth,
            self.spool_bytes,
            self.registered_consumers,
            self.healthy_consumers,
            self.checkpoint_watermark,
        )
        if any(not _non_negative_int(value) for value in integer_values) or any(
            not _non_negative_number(value)
            for value in (self.cpu_percent, self.rss_mb)
        ):
            raise ValueError("hold observation values must be non-negative")
        if self.healthy_consumers > self.registered_consumers:
            raise ValueError("healthy consumer count exceeds registry count")


@dataclass(frozen=True, slots=True)
class PrimaryHoldDecision:
    schema: str
    decision_id: str
    hold_id: str
    status: HoldStatus
    reason: str
    scope: HoldScope
    production_authorized: bool
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    policy_digest: str
    first_observed_at_ns: int | None
    last_observed_at_ns: int | None
    observation_count: int
    terminal_watermark: int | None
    decided_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.primary-hold-decision.v1":
            raise ValueError("hold decision schema is invalid")
        if not isinstance(self.status, HoldStatus) or not isinstance(
            self.scope, HoldScope
        ):
            raise ValueError("hold decision status/scope is invalid")
        if not _valid_uuid(self.decision_id) or not _valid_uuid(self.hold_id):
            raise ValueError("hold decision UUID is invalid")
        if (
            not self.reason.strip()
            or not self.slice_id.strip()
            or not self.owner_id.strip()
        ):
            raise ValueError("hold decision identity/reason is required")
        if not _valid_uuid(self.prerequisite_bundle_id):
            raise ValueError("hold decision prerequisite bundle is invalid")
        if not _valid_digest(self.candidate_digest) or not _valid_digest(
            self.policy_digest
        ):
            raise ValueError("hold decision digest is invalid")
        if any(
            not _positive_int(value)
            for value in (
                self.authority_revision,
                self.lease_epoch,
                self.partition_plan_epoch,
                self.decided_at_ns,
            )
        ) or not _non_negative_int(self.observation_count):
            raise ValueError("hold decision epoch/count/time is invalid")
        if self.production_authorized != (
            self.scope == HoldScope.PRODUCTION and self.status == HoldStatus.PASSED
        ):
            raise ValueError("hold decision production authorization is inconsistent")
        summary = (
            self.first_observed_at_ns,
            self.last_observed_at_ns,
            self.terminal_watermark,
        )
        if self.observation_count == 0 and any(value is not None for value in summary):
            raise ValueError("empty hold decision cannot expose observation state")
        if self.observation_count > 0 and (
            self.first_observed_at_ns is None
            or self.last_observed_at_ns is None
            or self.terminal_watermark is None
            or self.first_observed_at_ns <= 0
            or self.last_observed_at_ns < self.first_observed_at_ns
            or self.terminal_watermark < 0
        ):
            raise ValueError("hold decision observation summary is invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


class PrimaryHoldEvaluator:
    _ZERO_TOLERANCE = (
        ("semantic_mismatches", "SEMANTIC_MISMATCH"),
        ("open_gaps", "OPEN_GAP"),
        ("duplicate_external_writes", "DUPLICATE_EXTERNAL_WRITE"),
        ("accepted_stale_writer_writes", "ACCEPTED_STALE_WRITER_WRITE"),
        ("authority_ambiguities", "AUTHORITY_AMBIGUITY"),
        ("durable_ack_failures", "DURABLE_ACK_FAILURE"),
        ("projection_mismatches", "PROJECTION_MISMATCH"),
        ("consumer_checkpoint_regressions", "CONSUMER_CHECKPOINT_REGRESSION"),
        ("unexplained_quality_failures", "UNEXPLAINED_QUALITY_FAILURE"),
    )

    def __init__(
        self,
        *,
        identity: PrimaryHoldIdentity,
        policy: PrimaryHoldPolicy,
        scope: HoldScope,
    ) -> None:
        if identity.policy_digest != policy.digest:
            raise ValueError("hold identity does not bind the policy")
        duration = identity.required_until_ns - identity.started_at_ns
        if duration < policy.minimum_duration_seconds * 1_000_000_000:
            raise ValueError("hold identity duration is below policy")
        self.identity = identity
        self.policy = policy
        self.scope = scope
        self._observations: list[PrimaryHoldObservation] = []
        self._blocked_reason: str | None = None

    def observe(self, item: PrimaryHoldObservation) -> str:
        if self._blocked_reason is not None:
            return "HOLD_ALREADY_BLOCKED"
        reason = self._validate_observation(item)
        self._observations.append(item)
        if reason != "PASS":
            self._blocked_reason = reason
        return reason

    def _validate_observation(self, item: PrimaryHoldObservation) -> str:
        expected = (
            self.identity.hold_id,
            self.identity.slice_id,
            self.identity.candidate_digest,
            self.identity.owner_id,
            self.identity.authority_revision,
            self.identity.lease_epoch,
            self.identity.partition_plan_epoch,
        )
        actual = (
            item.hold_id,
            item.slice_id,
            item.candidate_digest,
            item.owner_id,
            item.authority_revision,
            item.lease_epoch,
            item.partition_plan_epoch,
        )
        if actual != expected:
            return "HOLD_AUTHORITY_IDENTITY_CHANGED"
        if item.sequence != len(self._observations) + 1:
            return "HOLD_SEQUENCE_NOT_CONTIGUOUS"
        previous_time = (
            self._observations[-1].observed_at_ns
            if self._observations
            else self.identity.started_at_ns
        )
        if item.observed_at_ns <= previous_time:
            return "HOLD_OBSERVATION_TIME_NOT_MONOTONIC"
        if (
            item.observed_at_ns - previous_time
            > self.policy.max_sample_gap_seconds * 1_000_000_000
        ):
            return "HOLD_OBSERVATION_GAP_EXCEEDED"
        if (
            self._observations
            and item.last_watermark < self._observations[-1].last_watermark
        ):
            return "HOLD_WATERMARK_REGRESSED"
        for field, reason in self._ZERO_TOLERANCE:
            if getattr(item, field) != 0:
                return reason
        thresholds = (
            (item.lag_ms > self.policy.max_lag_ms, "LAG_THRESHOLD_EXCEEDED"),
            (
                item.freshness_ms > self.policy.max_freshness_ms,
                "FRESHNESS_THRESHOLD_EXCEEDED",
            ),
            (
                item.queue_depth > self.policy.max_queue_depth,
                "QUEUE_THRESHOLD_EXCEEDED",
            ),
            (
                item.spool_bytes > self.policy.max_spool_bytes,
                "SPOOL_THRESHOLD_EXCEEDED",
            ),
            (
                item.cpu_percent > self.policy.max_cpu_percent,
                "CPU_THRESHOLD_EXCEEDED",
            ),
            (item.rss_mb > self.policy.max_rss_mb, "RSS_THRESHOLD_EXCEEDED"),
        )
        threshold_reason = next((reason for failed, reason in thresholds if failed), None)
        if threshold_reason is not None:
            return threshold_reason
        if item.registered_consumers <= 0:
            return "CONSUMER_REGISTRY_EMPTY"
        if item.healthy_consumers != item.registered_consumers:
            return "CONSUMER_NOT_HEALTHY"
        if item.checkpoint_watermark < item.last_watermark:
            return "CONSUMER_CHECKPOINT_BEHIND"
        return "PASS"

    def decision(self, *, decision_id: str, now_ns: int) -> PrimaryHoldDecision:
        if not _positive_int(now_ns):
            raise ValueError("hold decision clock is invalid")
        if self._observations and now_ns < self._observations[-1].observed_at_ns:
            raise ValueError("hold decision precedes the latest observation")
        status = HoldStatus.IN_PROGRESS
        reason = "HOLD_WINDOW_INCOMPLETE"
        if self._blocked_reason is not None:
            status = HoldStatus.BLOCKED
            reason = self._blocked_reason
        elif not self._observations:
            reason = "HOLD_OBSERVATION_MISSING"
        elif now_ns < self.identity.required_until_ns:
            reason = "HOLD_WINDOW_INCOMPLETE"
        elif self._observations[-1].observed_at_ns < self.identity.required_until_ns:
            reason = "HOLD_TERMINAL_OBSERVATION_MISSING"
        else:
            status = HoldStatus.PASSED
            reason = "PASS"
        first = self._observations[0] if self._observations else None
        last = self._observations[-1] if self._observations else None
        return PrimaryHoldDecision(
            schema="qdl.primary-hold-decision.v1",
            decision_id=decision_id,
            hold_id=self.identity.hold_id,
            status=status,
            reason=reason,
            scope=self.scope,
            production_authorized=(
                status == HoldStatus.PASSED and self.scope == HoldScope.PRODUCTION
            ),
            slice_id=self.identity.slice_id,
            candidate_digest=self.identity.candidate_digest,
            prerequisite_bundle_id=self.identity.prerequisite_bundle_id,
            owner_id=self.identity.owner_id,
            authority_revision=self.identity.authority_revision,
            lease_epoch=self.identity.lease_epoch,
            partition_plan_epoch=self.identity.partition_plan_epoch,
            policy_digest=self.identity.policy_digest,
            first_observed_at_ns=first.observed_at_ns if first else None,
            last_observed_at_ns=last.observed_at_ns if last else None,
            observation_count=len(self._observations),
            terminal_watermark=last.last_watermark if last else None,
            decided_at_ns=now_ns,
        )


@dataclass(frozen=True, slots=True)
class ConsumerCheckpoint:
    consumer_id: str
    requirement_digest: str
    contract_major: int
    applied_watermark: int
    checkpointed_watermark: int
    status: str
    migration_status: str
    rollback_ready: bool

    def __post_init__(self) -> None:
        if not self.consumer_id.strip() or not _valid_digest(self.requirement_digest):
            raise ValueError("consumer checkpoint identity is invalid")
        if not _positive_int(self.contract_major):
            raise ValueError("consumer contract major is invalid")
        if (
            not _non_negative_int(self.applied_watermark)
            or not _non_negative_int(self.checkpointed_watermark)
            or self.checkpointed_watermark > self.applied_watermark
        ):
            raise ValueError("consumer checkpoint watermark is invalid")
        if self.status != "READY" or self.migration_status != "COMPLETE":
            raise ValueError("consumer is not closure-ready")
        if self.rollback_ready is not True:
            raise ValueError("consumer rollback posture is not ready")


@dataclass(frozen=True, slots=True)
class ConsumerRegistrySnapshot:
    schema: str
    snapshot_id: str
    slice_id: str
    authority_revision: int
    checkpoints: tuple[ConsumerCheckpoint, ...]
    observed_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.consumer-registry-snapshot.v1":
            raise ValueError("consumer registry schema is invalid")
        if not _valid_uuid(self.snapshot_id) or not self.slice_id.strip():
            raise ValueError("consumer registry identity is invalid")
        if not _positive_int(self.authority_revision) or not _positive_int(
            self.observed_at_ns
        ):
            raise ValueError("consumer registry revision/time is invalid")
        if not isinstance(self.checkpoints, tuple):
            raise ValueError("consumer registry checkpoints must be immutable")
        ids = [item.consumer_id for item in self.checkpoints]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError("consumer registry must be non-empty and unique")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class AuthorityRegistrySnapshot:
    schema: str
    snapshot_id: str
    slice_id: str
    state: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    candidate_digest: str
    prerequisite_bundle_id: str
    current_watermark: int
    public_write_allowed: bool
    legacy_write_allowed: bool
    observed_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.authority-registry-snapshot.v1":
            raise ValueError("authority registry schema is invalid")
        if (
            not _valid_uuid(self.snapshot_id)
            or not _valid_uuid(self.prerequisite_bundle_id)
            or not self.slice_id.strip()
            or not self.owner_id.strip()
            or not _valid_digest(self.candidate_digest)
        ):
            raise ValueError("authority registry identity is invalid")
        if self.state != "RUST_PRIMARY":
            raise ValueError("authority registry is not Rust primary")
        if any(
            not _positive_int(value)
            for value in (
                self.authority_revision,
                self.lease_epoch,
                self.partition_plan_epoch,
                self.observed_at_ns,
            )
        ) or not _non_negative_int(self.current_watermark):
            raise ValueError("authority registry epoch/watermark/time is invalid")
        if not self.public_write_allowed or not self.legacy_write_allowed:
            raise ValueError("primary authority write flags are invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class RollbackRehearsalEvidence:
    schema: str
    rehearsal_id: str
    slice_id: str
    candidate_digest: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    rollback_manifest_digest: str
    reconciled_through_watermark: int
    rto_ms: float
    status: str
    production_scope: bool
    observed_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.rollback-rehearsal.v1":
            raise ValueError("rollback rehearsal schema is invalid")
        if (
            not _valid_uuid(self.rehearsal_id)
            or not self.slice_id.strip()
            or not self.owner_id.strip()
            or not _valid_digest(self.candidate_digest)
            or not _valid_digest(self.rollback_manifest_digest)
        ):
            raise ValueError("rollback rehearsal identity is invalid")
        if self.status != "PASS":
            raise ValueError("rollback rehearsal did not pass")
        if any(
            not _positive_int(value)
            for value in (
                self.authority_revision,
                self.lease_epoch,
                self.partition_plan_epoch,
                self.observed_at_ns,
                self.expires_at_ns,
            )
        ) or self.expires_at_ns <= self.observed_at_ns:
            raise ValueError("rollback rehearsal epoch/time is invalid")
        if not _non_negative_int(self.reconciled_through_watermark) or self.rto_ms <= 0:
            raise ValueError("rollback rehearsal watermark/RTO is invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class ClosureApproval:
    schema: str
    approval_id: str
    closure_id: str
    decision: str
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str
    hold_id: str
    hold_policy_digest: str
    operator: str
    change_ticket: str
    allow_close_rollback_window: bool
    repository_cleanup_approved: bool
    approved_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.rollback-window-closure-approval.v1":
            raise ValueError("closure approval schema is invalid")
        if any(
            not _valid_uuid(value)
            for value in (
                self.approval_id,
                self.closure_id,
                self.prerequisite_bundle_id,
                self.hold_id,
            )
        ):
            raise ValueError("closure approval UUID is invalid")
        if (
            self.decision != "APPROVE"
            or not self.slice_id.strip()
            or not self.operator.strip()
            or not self.change_ticket.strip()
            or not _valid_digest(self.candidate_digest)
            or not _valid_digest(self.hold_policy_digest)
        ):
            raise ValueError("closure approval identity/decision is invalid")
        if self.allow_close_rollback_window is not True:
            raise ValueError("closure approval does not allow window close")
        if self.repository_cleanup_approved:
            raise ValueError("window closure cannot approve repository cleanup")
        if (
            not _positive_int(self.approved_at_ns)
            or not _positive_int(self.expires_at_ns)
            or self.expires_at_ns <= self.approved_at_ns
        ):
            raise ValueError("closure approval time window is invalid")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class RollbackWindowClosure:
    schema: str
    closure_id: str
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    hold_decision_id: str
    hold_decision_digest: str
    consumer_registry_snapshot_id: str
    consumer_registry_digest: str
    authority_registry_snapshot_id: str
    authority_registry_digest: str
    rollback_rehearsal_id: str
    rollback_rehearsal_digest: str
    approval_id: str
    approval_digest: str
    operator: str
    change_ticket: str
    closed_at_ns: int
    production_authorized: bool

    def __post_init__(self) -> None:
        if self.schema != "qdl.rollback-window-closure.v1":
            raise ValueError("rollback closure schema is invalid")
        if any(
            not _valid_uuid(value)
            for value in (
                self.closure_id,
                self.prerequisite_bundle_id,
                self.hold_decision_id,
                self.consumer_registry_snapshot_id,
                self.authority_registry_snapshot_id,
                self.rollback_rehearsal_id,
                self.approval_id,
            )
        ):
            raise ValueError("rollback closure UUID is invalid")
        if (
            not self.slice_id.strip()
            or not self.owner_id.strip()
            or not self.operator.strip()
            or not self.change_ticket.strip()
        ):
            raise ValueError("rollback closure identity is incomplete")
        if any(
            not _valid_digest(value)
            for value in (
                self.candidate_digest,
                self.hold_decision_digest,
                self.consumer_registry_digest,
                self.authority_registry_digest,
                self.rollback_rehearsal_digest,
                self.approval_digest,
            )
        ):
            raise ValueError("rollback closure digest is invalid")
        if any(
            not _positive_int(value)
            for value in (
                self.authority_revision,
                self.lease_epoch,
                self.partition_plan_epoch,
                self.closed_at_ns,
            )
        ):
            raise ValueError("rollback closure epoch/time is invalid")
        if self.production_authorized is not True:
            raise ValueError("rollback closure must be production-authorized")

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class ClosureAuthorization:
    allowed: bool
    reason: str
    production_authorized: bool
    closure: RollbackWindowClosure | None


class ProductionClosureAuthorizer:
    def __init__(self, *, max_decision_age_seconds: int = 900) -> None:
        self._prerequisite_authorizer = ProductionCanaryAuthorizer(
            max_decision_age_seconds=max_decision_age_seconds
        )

    def authorize(
        self,
        *,
        candidate: CandidateSlice,
        prerequisite_decision: Mapping[str, Any],
        expected_bundle_id: str,
        primary_evidence: Mapping[str, Any] | None,
        hold_decision: PrimaryHoldDecision | None,
        consumer_registry: ConsumerRegistrySnapshot | None,
        authority_registry: AuthorityRegistrySnapshot | None,
        rollback_evidence: RollbackRehearsalEvidence | None,
        approval: ClosureApproval | None,
        now_ns: int,
    ) -> ClosureAuthorization:
        prerequisite = self._prerequisite_authorizer.authorize(
            candidate=candidate,
            decision=prerequisite_decision,
            expected_bundle_id=expected_bundle_id,
            now_ns=now_ns,
        )
        if not prerequisite.allowed:
            return ClosureAuthorization(False, prerequisite.reason, False, None)
        if primary_evidence is None:
            return ClosureAuthorization(False, "PRIMARY_EVIDENCE_MISSING", False, None)
        if hold_decision is None:
            return ClosureAuthorization(False, "PRIMARY_HOLD_MISSING", False, None)
        if consumer_registry is None or authority_registry is None:
            return ClosureAuthorization(False, "REGISTRY_SNAPSHOT_MISSING", False, None)
        if rollback_evidence is None:
            return ClosureAuthorization(False, "ROLLBACK_REHEARSAL_MISSING", False, None)
        if approval is None:
            return ClosureAuthorization(False, "CLOSURE_APPROVAL_MISSING", False, None)
        reason = self._validate_primary(candidate, expected_bundle_id, primary_evidence)
        if reason == "AUTHORIZED":
            reason = self._validate_hold(candidate, expected_bundle_id, hold_decision)
        if reason == "AUTHORIZED":
            reason = self._validate_registries(
                hold_decision,
                primary_evidence,
                consumer_registry,
                authority_registry,
                now_ns,
            )
        if reason == "AUTHORIZED":
            reason = self._validate_rollback(
                candidate,
                hold_decision,
                authority_registry,
                rollback_evidence,
                now_ns,
            )
        if reason == "AUTHORIZED":
            reason = self._validate_approval(
                candidate, expected_bundle_id, hold_decision, approval, now_ns
            )
        if reason != "AUTHORIZED":
            return ClosureAuthorization(False, reason, False, None)
        closure = RollbackWindowClosure(
            schema="qdl.rollback-window-closure.v1",
            closure_id=approval.closure_id,
            slice_id=hold_decision.slice_id,
            candidate_digest=hold_decision.candidate_digest,
            prerequisite_bundle_id=hold_decision.prerequisite_bundle_id,
            owner_id=hold_decision.owner_id,
            authority_revision=hold_decision.authority_revision,
            lease_epoch=hold_decision.lease_epoch,
            partition_plan_epoch=hold_decision.partition_plan_epoch,
            hold_decision_id=hold_decision.decision_id,
            hold_decision_digest=hold_decision.digest,
            consumer_registry_snapshot_id=consumer_registry.snapshot_id,
            consumer_registry_digest=consumer_registry.digest,
            authority_registry_snapshot_id=authority_registry.snapshot_id,
            authority_registry_digest=authority_registry.digest,
            rollback_rehearsal_id=rollback_evidence.rehearsal_id,
            rollback_rehearsal_digest=rollback_evidence.digest,
            approval_id=approval.approval_id,
            approval_digest=approval.digest,
            operator=approval.operator,
            change_ticket=approval.change_ticket,
            closed_at_ns=now_ns,
            production_authorized=True,
        )
        return ClosureAuthorization(True, "AUTHORIZED", True, closure)

    @staticmethod
    def _validate_primary(
        candidate: CandidateSlice, bundle_id: str, primary: Mapping[str, Any]
    ) -> str:
        if primary.get("schema") != "qdl.phase92.production-primary.v1":
            return "PRIMARY_EVIDENCE_SCHEMA_INVALID"
        if primary.get("status") != "PRODUCTION_PRIMARY_ACTIVE":
            return "PRIMARY_NOT_ACTIVE"
        if primary.get("production_authorized") is not True:
            return "PRIMARY_NOT_PRODUCTION_AUTHORIZED"
        if primary.get("slice_id") != candidate.payload["slice_id"]:
            return "PRIMARY_SLICE_MISMATCH"
        if primary.get("candidate_digest") != candidate.digest:
            return "PRIMARY_CANDIDATE_MISMATCH"
        if primary.get("prerequisite_bundle_id") != bundle_id:
            return "PRIMARY_BUNDLE_MISMATCH"
        authority = primary.get("authority")
        if not isinstance(authority, Mapping) or authority.get("state") != "RUST_PRIMARY":
            return "PRIMARY_AUTHORITY_INVALID"
        required = (
            "owner_id", "authority_revision", "lease_epoch",
            "partition_plan_epoch", "current_watermark",
        )
        if any(key not in authority for key in required):
            return "PRIMARY_AUTHORITY_INCOMPLETE"
        return "AUTHORIZED"

    @staticmethod
    def _validate_hold(
        candidate: CandidateSlice, bundle_id: str, hold: PrimaryHoldDecision
    ) -> str:
        if hold.status != HoldStatus.PASSED or hold.reason != "PASS":
            return "PRIMARY_HOLD_NOT_PASSED"
        if hold.scope != HoldScope.PRODUCTION or not hold.production_authorized:
            return "PRIMARY_HOLD_NOT_PRODUCTION"
        if hold.slice_id != candidate.payload["slice_id"]:
            return "PRIMARY_HOLD_SLICE_MISMATCH"
        if hold.candidate_digest != candidate.digest:
            return "PRIMARY_HOLD_CANDIDATE_MISMATCH"
        if hold.prerequisite_bundle_id != bundle_id:
            return "PRIMARY_HOLD_BUNDLE_MISMATCH"
        return "AUTHORIZED"

    @staticmethod
    def _validate_registries(
        hold: PrimaryHoldDecision,
        primary: Mapping[str, Any],
        consumer: ConsumerRegistrySnapshot,
        authority: AuthorityRegistrySnapshot,
        now_ns: int,
    ) -> str:
        if (consumer.slice_id, consumer.authority_revision) != (
            hold.slice_id,
            hold.authority_revision,
        ):
            return "CONSUMER_REGISTRY_IDENTITY_MISMATCH"
        if (
            authority.slice_id != hold.slice_id
            or authority.owner_id != hold.owner_id
            or authority.authority_revision != hold.authority_revision
            or authority.lease_epoch != hold.lease_epoch
            or authority.partition_plan_epoch != hold.partition_plan_epoch
            or authority.candidate_digest != hold.candidate_digest
            or authority.prerequisite_bundle_id != hold.prerequisite_bundle_id
        ):
            return "AUTHORITY_REGISTRY_IDENTITY_MISMATCH"
        max_age_ns = 300 * 1_000_000_000
        if any(
            observed > now_ns or now_ns - observed > max_age_ns
            for observed in (consumer.observed_at_ns, authority.observed_at_ns)
        ):
            return "REGISTRY_SNAPSHOT_STALE"
        primary_authority = primary.get("authority")
        if not isinstance(primary_authority, Mapping) or any(
            primary_authority.get(field) != expected
            for field, expected in (
                ("owner_id", authority.owner_id),
                ("authority_revision", authority.authority_revision),
                ("lease_epoch", authority.lease_epoch),
                ("partition_plan_epoch", authority.partition_plan_epoch),
                ("current_watermark", authority.current_watermark),
            )
        ):
            return "PRIMARY_AUTHORITY_REGISTRY_MISMATCH"
        terminal = hold.terminal_watermark
        if terminal is None or authority.current_watermark < terminal:
            return "AUTHORITY_REGISTRY_WATERMARK_BEHIND"
        if any(
            item.checkpointed_watermark < authority.current_watermark
            for item in consumer.checkpoints
        ):
            return "CONSUMER_CHECKPOINT_BEHIND"
        return "AUTHORIZED"

    @staticmethod
    def _validate_rollback(
        candidate: CandidateSlice,
        hold: PrimaryHoldDecision,
        authority: AuthorityRegistrySnapshot,
        rollback: RollbackRehearsalEvidence,
        now_ns: int,
    ) -> str:
        if not rollback.production_scope:
            return "ROLLBACK_REHEARSAL_NOT_PRODUCTION"
        if rollback.expires_at_ns <= now_ns:
            return "ROLLBACK_REHEARSAL_EXPIRED"
        if (
            rollback.slice_id != hold.slice_id
            or rollback.candidate_digest != hold.candidate_digest
            or rollback.owner_id != hold.owner_id
            or rollback.authority_revision != hold.authority_revision
            or rollback.lease_epoch != hold.lease_epoch
            or rollback.partition_plan_epoch != hold.partition_plan_epoch
        ):
            return "ROLLBACK_REHEARSAL_IDENTITY_MISMATCH"
        if (
            rollback.rollback_manifest_digest
            != candidate.payload["rollback_manifest_digest"]
        ):
            return "ROLLBACK_MANIFEST_MISMATCH"
        if rollback.reconciled_through_watermark < authority.current_watermark:
            return "ROLLBACK_REHEARSAL_WATERMARK_BEHIND"
        return "AUTHORIZED"

    @staticmethod
    def _validate_approval(
        candidate: CandidateSlice,
        bundle_id: str,
        hold: PrimaryHoldDecision,
        approval: ClosureApproval,
        now_ns: int,
    ) -> str:
        if approval.expires_at_ns <= now_ns or approval.approved_at_ns > now_ns:
            return "CLOSURE_APPROVAL_TIME_INVALID"
        if (
            approval.slice_id != candidate.payload["slice_id"]
            or approval.candidate_digest != candidate.digest
            or approval.prerequisite_bundle_id != bundle_id
            or approval.hold_id != hold.hold_id
            or approval.hold_policy_digest != hold.policy_digest
        ):
            return "CLOSURE_APPROVAL_IDENTITY_MISMATCH"
        return "AUTHORIZED"


@dataclass(frozen=True, slots=True)
class ExpansionManifest:
    schema: str
    expansion_id: str
    parent_slice_id: str
    parent_candidate_digest: str
    parent_closure_id: str
    parent_closure_digest: str
    expansion_type: ExpansionType
    candidate_digest: str
    scope_digest: str
    partition_plan_epoch: int
    required_gates: tuple[str, ...]
    status: str
    transitive_evidence_allowed: bool
    public_write_allowed: bool
    legacy_write_allowed: bool
    created_at_ns: int

    def __post_init__(self) -> None:
        if self.schema != "qdl.independent-expansion.v1":
            raise ValueError("expansion schema is invalid")
        if not isinstance(self.expansion_type, ExpansionType):
            raise ValueError("expansion type is invalid")
        if not _valid_uuid(self.expansion_id) or not _valid_uuid(
            self.parent_closure_id
        ):
            raise ValueError("expansion UUID identity is invalid")
        if not self.parent_slice_id.strip():
            raise ValueError("expansion parent slice is required")
        if any(
            not _valid_digest(value)
            for value in (
                self.parent_candidate_digest,
                self.parent_closure_digest,
                self.candidate_digest,
                self.scope_digest,
            )
        ):
            raise ValueError("expansion digest identity is invalid")
        if self.candidate_digest == self.parent_candidate_digest:
            raise ValueError("expansion requires a new candidate digest")
        if not _positive_int(self.partition_plan_epoch):
            raise ValueError("expansion partition plan epoch is invalid")
        if self.required_gates != tuple(sorted(_EXPANSION_GATES[self.expansion_type])):
            raise ValueError("expansion required gates are incomplete or transitive")
        if self.status != "INDEPENDENT_CERTIFICATION_REQUIRED":
            raise ValueError("expansion status cannot grant authority")
        if (
            self.transitive_evidence_allowed
            or self.public_write_allowed
            or self.legacy_write_allowed
        ):
            raise ValueError("expansion cannot inherit evidence or write authority")
        if not _positive_int(self.created_at_ns):
            raise ValueError("expansion creation time is invalid")

    @classmethod
    def plan(
        cls,
        *,
        expansion_id: str,
        parent: RollbackWindowClosure,
        expansion_type: ExpansionType,
        candidate_digest: str,
        scope_digest: str,
        partition_plan_epoch: int,
        created_at_ns: int,
    ) -> "ExpansionManifest":
        if (
            expansion_type == ExpansionType.INSTRUMENT_PARTITION
            and partition_plan_epoch <= parent.partition_plan_epoch
        ):
            raise ValueError("instrument expansion requires a newer partition epoch")
        return cls(
            schema="qdl.independent-expansion.v1",
            expansion_id=expansion_id,
            parent_slice_id=parent.slice_id,
            parent_candidate_digest=parent.candidate_digest,
            parent_closure_id=parent.closure_id,
            parent_closure_digest=parent.digest,
            expansion_type=expansion_type,
            candidate_digest=candidate_digest,
            scope_digest=scope_digest,
            partition_plan_epoch=partition_plan_epoch,
            required_gates=tuple(sorted(_EXPANSION_GATES[expansion_type])),
            status="INDEPENDENT_CERTIFICATION_REQUIRED",
            transitive_evidence_allowed=False,
            public_write_allowed=False,
            legacy_write_allowed=False,
            created_at_ns=created_at_ns,
        )

    @property
    def digest(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True, slots=True)
class DecommissionRequest:
    schema: str
    request_id: str
    runtime_id: str
    owned_slice_ids: tuple[str, ...]
    rollback_reference_ids: tuple[str, ...]
    consumer_dependency_ids: tuple[str, ...]
    all_replacement_windows_closed: bool
    repository_cleanup_approved: bool
    shared_knowledge_retained: bool

    def __post_init__(self) -> None:
        if self.schema != "qdl.runtime-decommission-request.v1":
            raise ValueError("decommission request schema is invalid")
        if not _valid_uuid(self.request_id) or not self.runtime_id.strip():
            raise ValueError("decommission request identity is invalid")
        for values in (
            self.owned_slice_ids,
            self.rollback_reference_ids,
            self.consumer_dependency_ids,
        ):
            if not isinstance(values, tuple):
                raise ValueError("decommission dependencies must be immutable")
            if any(not value.strip() for value in values) or len(values) != len(
                set(values)
            ):
                raise ValueError("decommission dependencies must be unique")


@dataclass(frozen=True, slots=True)
class DecommissionDecision:
    allowed: bool
    reason: str


def assess_decommission(request: DecommissionRequest) -> DecommissionDecision:
    if request.owned_slice_ids:
        return DecommissionDecision(False, "RUNTIME_STILL_OWNS_SLICES")
    if request.rollback_reference_ids:
        return DecommissionDecision(False, "RUNTIME_STILL_REQUIRED_FOR_ROLLBACK")
    if request.consumer_dependency_ids:
        return DecommissionDecision(False, "RUNTIME_HAS_CONSUMER_DEPENDENCIES")
    if not request.all_replacement_windows_closed:
        return DecommissionDecision(False, "REPLACEMENT_WINDOWS_NOT_CLOSED")
    if not request.repository_cleanup_approved:
        return DecommissionDecision(False, "REPOSITORY_CLEANUP_NOT_APPROVED")
    if not request.shared_knowledge_retained:
        return DecommissionDecision(False, "SHARED_KNOWLEDGE_REMOVAL_FORBIDDEN")
    return DecommissionDecision(True, "AUTHORIZED")

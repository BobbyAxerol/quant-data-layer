from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping

from qdl.canary.phase9 import ProductionCanaryAuthorizer
from qdl.certification.prerequisites import CandidateSlice


def _canonical_digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _valid_digest(value: str) -> bool:
    return len(value) == 64 and all(char in "0123456789abcdef" for char in value)


class PrimaryAuthorizationMode(StrEnum):
    PRODUCTION = "PRODUCTION"
    ISOLATED_REHEARSAL = "ISOLATED_REHEARSAL"


class HandoffDirection(StrEnum):
    PYTHON_TO_RUST = "PYTHON_TO_RUST"
    RUST_TO_PYTHON = "RUST_TO_PYTHON"


@dataclass(frozen=True, slots=True)
class PrimaryAuthorization:
    allowed: bool
    production_authorized: bool
    mode: PrimaryAuthorizationMode
    reason: str
    slice_id: str
    candidate_digest: str
    prerequisite_bundle_id: str | None


@dataclass(frozen=True, slots=True)
class TerminalOwnerCheckpoint:
    schema: str
    checkpoint_id: str
    slice_id: str
    owner_id: str
    authority_revision: int
    lease_epoch: int
    partition_plan_epoch: int
    source_session_id: str
    connection_generation: int
    terminal_watermark: int
    terminal_event_id: str
    terminal_payload_sha256: str
    candidate_digest: str
    committed_at_ns: int

    def __post_init__(self) -> None:
        try:
            uuid.UUID(self.checkpoint_id)
        except ValueError as error:
            raise ValueError("checkpoint_id must be a UUID") from error
        if self.schema != "qdl.terminal-owner-checkpoint.v1":
            raise ValueError("terminal checkpoint schema is invalid")
        text_fields = (
            self.slice_id,
            self.owner_id,
            self.source_session_id,
            self.terminal_event_id,
        )
        if any(not value.strip() for value in text_fields):
            raise ValueError("terminal checkpoint identity is incomplete")
        positive = (
            self.authority_revision,
            self.lease_epoch,
            self.partition_plan_epoch,
            self.connection_generation,
            self.committed_at_ns,
        )
        if any(value <= 0 for value in positive) or self.terminal_watermark < 0:
            raise ValueError("terminal checkpoint epoch/watermark is invalid")
        if not _valid_digest(self.terminal_payload_sha256):
            raise ValueError("terminal payload digest is invalid")
        if not _valid_digest(self.candidate_digest):
            raise ValueError("terminal candidate digest is invalid")

    @property
    def digest(self) -> str:
        return _canonical_digest(asdict(self))


@dataclass(frozen=True, slots=True)
class AcceptedHandoff:
    schema: str
    handoff_id: str
    direction: HandoffDirection
    checkpoint_digest: str
    slice_id: str
    old_owner_id: str
    new_owner_id: str
    expected_state: str
    new_state: str
    expected_authority_revision: int
    new_authority_revision: int
    expected_lease_epoch: int
    new_lease_epoch: int
    partition_plan_epoch: int
    terminal_watermark: int
    first_new_watermark: int
    overlap_start_watermark: int
    overlap_end_watermark: int
    old_event_count: int
    new_event_count: int
    semantic_mismatches: int
    open_gaps: int
    candidate_digest: str
    prerequisite_bundle_id: str
    approved_by: str
    approved_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.handoff_id, "handoff_id"),
            (self.prerequisite_bundle_id, "prerequisite_bundle_id"),
        ):
            try:
                uuid.UUID(value)
            except ValueError as error:
                raise ValueError(f"{label} must be a UUID") from error
        if self.schema != "qdl.accepted-authority-handoff.v1":
            raise ValueError("handoff schema is invalid")
        if any(
            not value.strip()
            for value in (
                self.slice_id,
                self.old_owner_id,
                self.new_owner_id,
                self.expected_state,
                self.new_state,
                self.approved_by,
            )
        ):
            raise ValueError("handoff identity is incomplete")
        expected_states = {
            HandoffDirection.PYTHON_TO_RUST: ("RUST_CANARY", "RUST_PRIMARY"),
            HandoffDirection.RUST_TO_PYTHON: (
                "ROLLBACK_PENDING",
                "PYTHON_PRIMARY",
            ),
        }
        if (self.expected_state, self.new_state) != expected_states[self.direction]:
            raise ValueError("handoff direction/state pair is invalid")
        if self.old_owner_id == self.new_owner_id:
            raise ValueError("handoff must change owner")
        if self.new_authority_revision != self.expected_authority_revision + 1:
            raise ValueError("handoff authority revision must advance exactly one")
        if self.new_lease_epoch <= self.expected_lease_epoch:
            raise ValueError("handoff owner requires a newer lease epoch")
        if self.partition_plan_epoch <= 0 or self.terminal_watermark < 0:
            raise ValueError("handoff plan/watermark is invalid")
        if self.first_new_watermark != self.terminal_watermark + 1:
            raise ValueError("handoff first watermark must equal terminal + 1")
        if (
            self.overlap_start_watermark < 0
            or self.overlap_start_watermark > self.overlap_end_watermark
            or self.overlap_end_watermark != self.terminal_watermark
        ):
            raise ValueError("handoff reconciliation range is invalid")
        if (
            self.old_event_count <= 0
            or self.old_event_count != self.new_event_count
            or self.semantic_mismatches != 0
            or self.open_gaps != 0
        ):
            raise ValueError("handoff reconciliation is not clean")
        if not _valid_digest(self.checkpoint_digest) or not _valid_digest(
            self.candidate_digest
        ):
            raise ValueError("handoff digest is invalid")
        if self.approved_at_ns <= 0 or self.expires_at_ns <= self.approved_at_ns:
            raise ValueError("handoff approval window is invalid")

    @property
    def digest(self) -> str:
        payload = asdict(self)
        payload["direction"] = self.direction.value
        return _canonical_digest(payload)


class ProductionPrimaryAuthorizer:
    """Fail-closed Phase 9.2 gate. It does not mutate authority."""

    def __init__(self, *, max_decision_age_seconds: int = 900) -> None:
        self._canary_authorizer = ProductionCanaryAuthorizer(
            max_decision_age_seconds=max_decision_age_seconds
        )

    @staticmethod
    def authorize_isolated_rehearsal(
        *, candidate: CandidateSlice
    ) -> PrimaryAuthorization:
        return PrimaryAuthorization(
            allowed=True,
            production_authorized=False,
            mode=PrimaryAuthorizationMode.ISOLATED_REHEARSAL,
            reason="ISOLATED_REHEARSAL_ONLY",
            slice_id=str(candidate.payload["slice_id"]),
            candidate_digest=candidate.digest,
            prerequisite_bundle_id=None,
        )

    def authorize(
        self,
        *,
        candidate: CandidateSlice,
        prerequisite_decision: Mapping[str, Any],
        canary_evidence: Mapping[str, Any],
        approval: Mapping[str, Any],
        expected_bundle_id: str,
        now_ns: int,
    ) -> PrimaryAuthorization:
        canary_auth = self._canary_authorizer.authorize(
            candidate=candidate,
            decision=prerequisite_decision,
            expected_bundle_id=expected_bundle_id,
            now_ns=now_ns,
        )
        reason = canary_auth.reason
        if canary_auth.allowed:
            reason = self._validate_canary(
                candidate=candidate,
                canary=canary_evidence,
                expected_bundle_id=expected_bundle_id,
                now_ns=now_ns,
            )
        if reason == "AUTHORIZED":
            reason = self._validate_approval(
                candidate=candidate,
                approval=approval,
                expected_bundle_id=expected_bundle_id,
                now_ns=now_ns,
            )
        return PrimaryAuthorization(
            allowed=reason == "AUTHORIZED",
            production_authorized=reason == "AUTHORIZED",
            mode=PrimaryAuthorizationMode.PRODUCTION,
            reason=reason,
            slice_id=str(candidate.payload["slice_id"]),
            candidate_digest=candidate.digest,
            prerequisite_bundle_id=expected_bundle_id or None,
        )

    @staticmethod
    def _validate_canary(
        *,
        candidate: CandidateSlice,
        canary: Mapping[str, Any],
        expected_bundle_id: str,
        now_ns: int,
    ) -> str:
        if canary.get("schema") != "qdl.phase91.rust-canary-certification.v1":
            return "CANARY_EVIDENCE_SCHEMA_INVALID"
        if canary.get("status") != "PRODUCTION_CANARY_HOLD_PASSED":
            return "CANARY_HOLD_NOT_PASSED"
        if canary.get("production_authorized") is not True:
            return "CANARY_NOT_PRODUCTION_AUTHORIZED"
        if canary.get("slice_id") != candidate.payload["slice_id"]:
            return "CANARY_SLICE_MISMATCH"
        if canary.get("candidate_digest") != candidate.digest:
            return "CANARY_CANDIDATE_MISMATCH"
        if canary.get("prerequisite_bundle_id") != expected_bundle_id:
            return "CANARY_BUNDLE_MISMATCH"
        if canary.get("python_v1_public_authority_unchanged") is not True:
            return "CANARY_V1_PRECONDITION_INVALID"
        if canary.get("production_mutations") != 0:
            return "CANARY_PRODUCTION_MUTATION_DETECTED"
        parity = canary.get("parity")
        broker = canary.get("broker")
        if not isinstance(parity, Mapping) or parity.get("semantic_mismatches") != 0:
            return "CANARY_PARITY_INVALID"
        if not isinstance(broker, Mapping):
            return "CANARY_BROKER_EVIDENCE_MISSING"
        if broker.get("final_authority") != "RUST_CANARY":
            return "CANARY_AUTHORITY_NOT_HELD"
        if broker.get("public_writes") != 0 or broker.get("legacy_writes") != 0:
            return "CANARY_EXTERNAL_WRITE_DETECTED"
        completed = canary.get("hold_completed_at_ns")
        if (
            not isinstance(completed, int)
            or isinstance(completed, bool)
            or completed <= 0
            or completed > now_ns
        ):
            return "CANARY_HOLD_TIME_INVALID"
        rollback_digest = canary.get("rollback_manifest_digest")
        if not isinstance(rollback_digest, str) or not _valid_digest(rollback_digest):
            return "CANARY_ROLLBACK_MANIFEST_INVALID"
        return "AUTHORIZED"

    @staticmethod
    def _validate_approval(
        *,
        candidate: CandidateSlice,
        approval: Mapping[str, Any],
        expected_bundle_id: str,
        now_ns: int,
    ) -> str:
        if approval.get("schema") != "qdl.primary-slice-approval.v1":
            return "PRIMARY_APPROVAL_SCHEMA_INVALID"
        if approval.get("decision") != "APPROVE":
            return "PRIMARY_NOT_APPROVED"
        if approval.get("slice_id") != candidate.payload["slice_id"]:
            return "PRIMARY_APPROVAL_SLICE_MISMATCH"
        if approval.get("candidate_digest") != candidate.digest:
            return "PRIMARY_APPROVAL_CANDIDATE_MISMATCH"
        if approval.get("prerequisite_bundle_id") != expected_bundle_id:
            return "PRIMARY_APPROVAL_BUNDLE_MISMATCH"
        if not str(approval.get("operator") or "").strip():
            return "PRIMARY_APPROVER_MISSING"
        if not str(approval.get("change_ticket") or "").strip():
            return "PRIMARY_CHANGE_TICKET_MISSING"
        if approval.get("max_partitions") != 1:
            return "PRIMARY_BLAST_RADIUS_INVALID"
        if approval.get("allow_disable_exact_python_slice") is not True:
            return "PRIMARY_PYTHON_HANDOFF_NOT_APPROVED"
        expires = approval.get("expires_at_ns")
        if not isinstance(expires, int) or isinstance(expires, bool) or expires <= now_ns:
            return "PRIMARY_APPROVAL_EXPIRED"
        return "AUTHORIZED"

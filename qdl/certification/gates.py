from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from qdl.domain.capabilities import CapabilityAvailability, VenueCapabilityProfile


class GateStatus(str, Enum):
    PASS = "PASS"
    BLOCKED = "BLOCKED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True, slots=True)
class CertificationGate:
    gate_id: str
    status: GateStatus
    evidence: str
    mandatory: bool = True

    def __post_init__(self) -> None:
        if not self.gate_id.strip() or not self.evidence.strip():
            raise ValueError("certification gates require an ID and evidence")
        if self.status is GateStatus.NOT_APPLICABLE and self.mandatory:
            raise ValueError("a mandatory gate cannot be not-applicable")


@dataclass(frozen=True, slots=True)
class CertificationReport:
    scope: str
    gates: tuple[CertificationGate, ...]

    def __post_init__(self) -> None:
        gate_ids = [gate.gate_id for gate in self.gates]
        if not self.scope.strip() or len(gate_ids) != len(set(gate_ids)):
            raise ValueError("certification scope and unique gate IDs are required")

    @property
    def production_eligible(self) -> bool:
        return bool(self.gates) and all(
            gate.status is GateStatus.PASS for gate in self.gates if gate.mandatory
        )

    @property
    def blockers(self) -> tuple[CertificationGate, ...]:
        return tuple(
            gate for gate in self.gates
            if gate.mandatory and gate.status is not GateStatus.PASS
        )


@dataclass(frozen=True, slots=True)
class AdapterEvidence:
    instrument_mapping: bool
    precision_preserved: bool
    sequence_semantics: bool
    reconnect_resubscribe: bool
    rate_limit: bool
    malformed_quarantine: bool
    duplicate_out_of_order_gap: bool
    canonical_schema: bool
    quality_state: bool
    source_policy: bool
    rollback: bool
    performance: bool
    telemetry_runbook: bool


@dataclass(frozen=True, slots=True)
class AdapterCertification:
    provider: str
    market: str
    feed: str
    report: CertificationReport


def _gate(name: str, passed: bool, evidence_ref: str) -> CertificationGate:
    return CertificationGate(
        name,
        GateStatus.PASS if passed else GateStatus.BLOCKED,
        evidence_ref,
    )


def certify_adapter(
    profile: VenueCapabilityProfile,
    *,
    feed: str,
    evidence: AdapterEvidence,
    evidence_prefix: str,
) -> AdapterCertification:
    capability = profile.capability(feed)
    if capability.availability is not CapabilityAvailability.AVAILABLE:
        report = CertificationReport(
            f"{profile.provider}/{profile.market}/{feed}",
            (CertificationGate(
                "capability",
                GateStatus.BLOCKED,
                f"{evidence_prefix}: {capability.availability.value} - "
                f"{capability.constraint or 'not approved'}",
            ),),
        )
        return AdapterCertification(profile.provider, profile.market, feed, report)

    checks: Iterable[tuple[str, bool]] = (
        ("capability", True),
        ("instrument_mapping", evidence.instrument_mapping),
        ("precision", evidence.precision_preserved),
        ("sequence_semantics", evidence.sequence_semantics),
        ("reconnect_resubscribe", evidence.reconnect_resubscribe),
        ("rate_limit", evidence.rate_limit),
        ("malformed_quarantine", evidence.malformed_quarantine),
        ("duplicate_out_of_order_gap", evidence.duplicate_out_of_order_gap),
        ("canonical_schema", evidence.canonical_schema),
        ("quality_state", evidence.quality_state),
        ("source_policy", evidence.source_policy),
        ("rollback", evidence.rollback),
        ("performance", evidence.performance),
        ("telemetry_runbook", evidence.telemetry_runbook),
    )
    report = CertificationReport(
        f"{profile.provider}/{profile.market}/{feed}",
        tuple(_gate(name, passed, f"{evidence_prefix}#{name}") for name, passed in checks),
    )
    return AdapterCertification(profile.provider, profile.market, feed, report)

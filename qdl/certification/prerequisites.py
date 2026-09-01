from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_SENSITIVE_DETAIL_NAMES = frozenset({
    "secret", "token", "password", "private_key", "private-key", "key_material",
})


def _sensitive_detail_key(value: object) -> bool:
    name = str(value).lower()
    return name in _SENSITIVE_DETAIL_NAMES or any(
        name.endswith(suffix)
        for suffix in ("_secret", "_token", "_password", "_private_key", "_key_material")
    )


def _contains_sensitive_detail(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            _sensitive_detail_key(key) or _contains_sensitive_detail(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_sensitive_detail(item) for item in value)
    return False


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class EvidenceScope(StrEnum):
    TEST = "TEST"
    LOCAL_REHEARSAL = "LOCAL_REHEARSAL"
    PRODUCTION = "PRODUCTION"
    INDEPENDENT_FAILURE_DOMAIN = "INDEPENDENT_FAILURE_DOMAIN"


_SCOPE_RANK = {scope: rank for rank, scope in enumerate(EvidenceScope)}


@dataclass(frozen=True)
class GatePolicy:
    gate_id: str
    minimum_scope: EvidenceScope
    max_age_seconds: int
    required_details: frozenset[str]
    candidate_bound: bool
    assertions: dict[str, Any]
    minimums: dict[str, float]
    maximums: dict[str, float]
    candidate_field_matches: dict[str, str]


@dataclass(frozen=True)
class PrerequisitePolicy:
    revision: int
    environment: str
    gates: tuple[GatePolicy, ...]

    @classmethod
    def load(cls, path: str | Path) -> "PrerequisitePolicy":
        payload = yaml.safe_load(Path(path).read_text())
        if not isinstance(payload, dict) or set(payload) != {
            "schema", "revision", "environment", "gates"
        }:
            raise ValueError("prerequisite policy fields are incomplete or unknown")
        if payload["schema"] != "qdl.production-prerequisite-policy.v1":
            raise ValueError("unsupported prerequisite policy schema")
        gates_raw = payload["gates"]
        if not isinstance(gates_raw, list) or not gates_raw:
            raise ValueError("prerequisite policy requires gates")
        gates: list[GatePolicy] = []
        seen: set[str] = set()
        for item in gates_raw:
            if not isinstance(item, dict) or set(item) != {
                "id", "minimum_scope", "max_age_seconds",
                "required_details", "candidate_bound", "assertions",
                "minimums", "maximums", "candidate_field_matches",
            }:
                raise ValueError("prerequisite gate fields are incomplete or unknown")
            gate_id = str(item["id"]).strip()
            if not gate_id or gate_id in seen:
                raise ValueError("prerequisite gate IDs must be non-empty and unique")
            seen.add(gate_id)
            max_age = int(item["max_age_seconds"])
            details = item["required_details"]
            if max_age <= 0 or not isinstance(details, list):
                raise ValueError("prerequisite gate age/details are invalid")
            mappings = tuple(item[key] for key in (
                "assertions", "minimums", "maximums", "candidate_field_matches"
            ))
            if not all(isinstance(value, dict) for value in mappings):
                raise ValueError("prerequisite gate constraints must be mappings")
            constrained = set().union(*(set(value) for value in mappings))
            if constrained - set(details):
                raise ValueError("prerequisite constraints must reference required details")
            gates.append(GatePolicy(
                gate_id=gate_id,
                minimum_scope=EvidenceScope(str(item["minimum_scope"]).upper()),
                max_age_seconds=max_age,
                required_details=frozenset(str(value) for value in details),
                candidate_bound=bool(item["candidate_bound"]),
                assertions=dict(item["assertions"]),
                minimums={key: float(value) for key, value in item["minimums"].items()},
                maximums={key: float(value) for key, value in item["maximums"].items()},
                candidate_field_matches={
                    str(key): str(value)
                    for key, value in item["candidate_field_matches"].items()
                },
            ))
        revision = int(payload["revision"])
        environment = str(payload["environment"]).strip().lower()
        if revision <= 0 or not environment:
            raise ValueError("prerequisite policy revision/environment are invalid")
        return cls(revision=revision, environment=environment, gates=tuple(gates))


@dataclass(frozen=True)
class CandidateSlice:
    payload: dict[str, Any]

    @classmethod
    def load(cls, path: str | Path) -> "CandidateSlice":
        payload = yaml.safe_load(Path(path).read_text())
        required = {
            "schema", "slice_id", "environment", "venue", "market",
            "product_type", "feed", "instrument_uids", "partition_plan_epoch",
            "partition_id", "schema_major", "authority_state", "owner_id",
            "lease_epoch", "artifact_image_digest", "sbom_digest",
            "contract_digest", "partition_plan_digest", "rollback_manifest_digest",
            "signature_identity", "normalizer_version", "adapter_version",
            "config_revision", "instrument_catalog_revision", "source_policy_revision",
            "public_write_allowed", "legacy_write_allowed",
        }
        if not isinstance(payload, dict) or set(payload) != required:
            raise ValueError("candidate slice fields are incomplete or unknown")
        if payload["schema"] != "qdl.authority-candidate.v1":
            raise ValueError("unsupported candidate slice schema")
        text_fields = required - {
            "instrument_uids", "partition_plan_epoch", "schema_major", "lease_epoch",
            "public_write_allowed", "legacy_write_allowed",
        }
        if any(not str(payload[field]).strip() for field in text_fields):
            raise ValueError("candidate slice identity fields are required")
        instruments = payload["instrument_uids"]
        if not isinstance(instruments, list) or not instruments or not all(
            isinstance(item, str) and item.strip() for item in instruments
        ) or len(set(instruments)) != len(instruments):
            raise ValueError("candidate slice instruments must be non-empty and unique")
        try:
            for instrument_uid in instruments:
                uuid.UUID(instrument_uid)
        except ValueError as exc:
            raise ValueError("candidate instrument UID is invalid") from exc
        if not isinstance(payload["public_write_allowed"], bool) or not isinstance(payload["legacy_write_allowed"], bool):
            raise ValueError("candidate write-authority flags must be booleans")
        if int(payload["partition_plan_epoch"]) <= 0 or int(payload["schema_major"]) <= 0:
            raise ValueError("candidate plan/schema epoch must be positive")
        if int(payload["lease_epoch"]) <= 0:
            raise ValueError("candidate lease epoch must be positive")
        if payload["authority_state"] != "RUST_SHADOW":
            raise ValueError("Phase 9.0-C candidate must remain RUST_SHADOW")
        if payload["public_write_allowed"] or payload["legacy_write_allowed"]:
            raise ValueError("Phase 9.0-C candidate cannot write public or legacy output")
        if not _IMAGE_DIGEST.fullmatch(str(payload["artifact_image_digest"])):
            raise ValueError("candidate image digest is invalid")
        for field in (
            "sbom_digest", "contract_digest", "partition_plan_digest",
            "rollback_manifest_digest",
        ):
            if not _SHA256.fullmatch(str(payload[field])):
                raise ValueError(f"candidate {field} is invalid")
        return cls(dict(payload))

    @property
    def digest(self) -> str:
        encoded = json.dumps(self.payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class EvidenceRecord:
    evidence_id: str
    gate_id: str
    environment: str
    scope: EvidenceScope
    status: str
    issuer: str
    observed_at_ns: int
    expires_at_ns: int
    artifact_path: str | None
    artifact_sha256: str | None
    details: dict[str, Any]

    @classmethod
    def from_mapping(cls, payload: Any) -> "EvidenceRecord":
        required = {
            "evidence_id", "gate_id", "environment", "scope", "status", "issuer",
            "observed_at_ns", "expires_at_ns", "artifact_path", "artifact_sha256",
            "details",
        }
        if not isinstance(payload, dict) or set(payload) != required:
            raise ValueError("prerequisite evidence fields are incomplete or unknown")
        details = payload["details"]
        if not isinstance(details, dict) or _contains_sensitive_detail(details):
            raise ValueError("prerequisite evidence details are invalid or sensitive")
        status = str(payload["status"]).upper()
        if status not in {"PASS", "BLOCKED"}:
            raise ValueError("prerequisite evidence status is invalid")
        artifact_path = payload["artifact_path"]
        artifact_sha = payload["artifact_sha256"]
        if status == "PASS" and (
            not isinstance(artifact_path, str)
            or Path(artifact_path).is_absolute()
            or ".." in Path(artifact_path).parts
            or not isinstance(artifact_sha, str)
            or not _SHA256.fullmatch(artifact_sha)
        ):
            raise ValueError("passing evidence requires a repository-relative artifact hash")
        observed = int(payload["observed_at_ns"])
        expires = int(payload["expires_at_ns"])
        if observed <= 0 or expires <= observed:
            raise ValueError("prerequisite evidence timestamps are invalid")
        identity = tuple(str(payload[key]).strip() for key in ("evidence_id", "gate_id", "environment", "issuer"))
        if not all(identity):
            raise ValueError("prerequisite evidence identity is required")
        return cls(
            evidence_id=identity[0], gate_id=identity[1], environment=identity[2].lower(),
            scope=EvidenceScope(str(payload["scope"]).upper()), status=status,
            issuer=identity[3], observed_at_ns=observed, expires_at_ns=expires,
            artifact_path=artifact_path, artifact_sha256=artifact_sha,
            details=dict(details),
        )


@dataclass(frozen=True)
class GateResult:
    gate_id: str
    passed: bool
    reason: str
    evidence_id: str | None
    observed_scope: str | None
    required_scope: str


@dataclass(frozen=True)
class PrerequisiteDecision:
    decision: str
    candidate_digest: str
    policy_revision: int
    results: tuple[GateResult, ...]

    def as_mapping(self) -> dict[str, Any]:
        return {
            "schema": "qdl.production-prerequisite-decision.v1",
            "decision": self.decision,
            "candidate_digest": self.candidate_digest,
            "policy_revision": self.policy_revision,
            "passed": sum(item.passed for item in self.results),
            "blocked": sum(not item.passed for item in self.results),
            "gates": [item.__dict__ for item in self.results],
        }


def load_inventory(path: str | Path) -> tuple[EvidenceRecord, ...]:
    payload = yaml.safe_load(Path(path).read_text())
    if not isinstance(payload, dict) or set(payload) != {"schema", "evidence"}:
        raise ValueError("prerequisite inventory fields are incomplete or unknown")
    if payload["schema"] != "qdl.production-prerequisite-inventory.v1":
        raise ValueError("unsupported prerequisite inventory schema")
    items = tuple(EvidenceRecord.from_mapping(item) for item in payload["evidence"])
    ids = [item.evidence_id for item in items]
    gates = [item.gate_id for item in items]
    if len(ids) != len(set(ids)) or len(gates) != len(set(gates)):
        raise ValueError("prerequisite evidence IDs and gate bindings must be unique")
    return items


def evaluate_prerequisites(
    policy: PrerequisitePolicy,
    candidate: CandidateSlice,
    evidence: tuple[EvidenceRecord, ...],
    *,
    repository_root: str | Path,
    now_ns: int,
) -> PrerequisiteDecision:
    if candidate.payload["environment"].lower() != policy.environment:
        raise ValueError("candidate environment does not match prerequisite policy")
    by_gate = {item.gate_id: item for item in evidence}
    unknown_gates = set(by_gate) - {gate.gate_id for gate in policy.gates}
    if unknown_gates:
        raise ValueError(f"unknown prerequisite evidence gates: {sorted(unknown_gates)}")
    root = Path(repository_root).resolve()
    results: list[GateResult] = []
    for gate in policy.gates:
        item = by_gate.get(gate.gate_id)
        reason = "PASS"
        if item is None:
            reason = "MISSING_EVIDENCE"
        elif item.status != "PASS":
            reason = "EVIDENCE_BLOCKED"
        elif item.environment != policy.environment:
            reason = "ENVIRONMENT_MISMATCH"
        elif _SCOPE_RANK[item.scope] < _SCOPE_RANK[gate.minimum_scope]:
            reason = "INSUFFICIENT_SCOPE"
        elif item.observed_at_ns > now_ns + 60_000_000_000:
            reason = "EVIDENCE_FROM_FUTURE"
        elif item.expires_at_ns <= now_ns:
            reason = "EVIDENCE_EXPIRED"
        elif now_ns - item.observed_at_ns > gate.max_age_seconds * 1_000_000_000:
            reason = "EVIDENCE_TOO_OLD"
        elif gate.required_details - set(item.details):
            reason = "DETAILS_INCOMPLETE"
        elif gate.candidate_bound and item.details.get("candidate_digest") != candidate.digest:
            reason = "CANDIDATE_DIGEST_MISMATCH"
        elif any(item.details.get(key) != expected for key, expected in gate.assertions.items()):
            reason = "ASSERTION_FAILED"
        elif any(
            not _is_number(item.details.get(key))
            or float(item.details[key]) < minimum
            for key, minimum in gate.minimums.items()
        ):
            reason = "MINIMUM_NOT_MET"
        elif any(
            not _is_number(item.details.get(key))
            or float(item.details[key]) > maximum
            for key, maximum in gate.maximums.items()
        ):
            reason = "MAXIMUM_EXCEEDED"
        elif any(
            item.details.get(detail_key) != candidate.payload.get(candidate_key)
            for detail_key, candidate_key in gate.candidate_field_matches.items()
        ):
            reason = "CANDIDATE_FIELD_MISMATCH"
        elif gate.gate_id == "exact_slice_approval" and int(item.details["hold_until_ns"]) <= now_ns:
            reason = "APPROVAL_HOLD_WINDOW_INVALID"
        else:
            artifact = (root / str(item.artifact_path)).resolve()
            if root not in artifact.parents or not artifact.is_file():
                reason = "ARTIFACT_MISSING"
            elif hashlib.sha256(artifact.read_bytes()).hexdigest() != item.artifact_sha256:
                reason = "ARTIFACT_DIGEST_MISMATCH"
        results.append(GateResult(
            gate_id=gate.gate_id,
            passed=reason == "PASS",
            reason=reason,
            evidence_id=item.evidence_id if item else None,
            observed_scope=item.scope.value if item else None,
            required_scope=gate.minimum_scope.value,
        ))
    return PrerequisiteDecision(
        decision="GO" if all(item.passed for item in results) else "NO_GO_EXTERNAL",
        candidate_digest=candidate.digest,
        policy_revision=policy.revision,
        results=tuple(results),
    )

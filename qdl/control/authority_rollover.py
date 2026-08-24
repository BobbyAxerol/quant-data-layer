from __future__ import annotations

"""Immutable, blocked-only authority candidate rollover packets.

A rollout may update a candidate/image only after the old authority is terminal
and fenced.  This module constructs and validates the operator packet; the
PostgreSQL function remains the sole mutation path and Kafka offsets are never
changed here.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import time
from typing import Any
import uuid


SCHEMA = "qdl.r1.authority-candidate-rollover.v1"
PROVENANCE_FIELDS = (
    "adapter_version",
    "artifact_image_digest",
    "candidate_digest",
    "config_revision",
    "contract_digest",
    "instrument_catalog_revision",
    "normalizer_version",
    "partition_plan_digest",
    "rollback_manifest_digest",
    "sbom_digest",
    "signature_identity",
    "source_policy_revision",
)
STATIC_SEMANTIC_FIELDS = (
    "environment",
    "venue",
    "market",
    "product_type",
    "feed",
    "partition_plan_epoch",
    "partition_id",
    "schema_major",
    "signature_identity",
    "contract_digest",
    "normalizer_version",
    "adapter_version",
    "config_revision",
    "instrument_catalog_revision",
    "source_policy_revision",
    "partition_plan_digest",
)


def _encoded(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_encoded(value)).hexdigest()


def _uuid(value: object, field: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a UUID") from error


def _sha256(value: object, field: str, *, image: bool = False) -> str:
    text = str(value)
    prefix = "sha256:" if image else ""
    raw = text.removeprefix(prefix) if prefix else text
    if (
        (image and not text.startswith(prefix))
        or len(raw) != 64
        or any(character not in "0123456789abcdef" for character in raw)
    ):
        raise ValueError(f"{field} must be a SHA-256 digest")
    return text


def _timestamp_ns(value: object, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} must be an integer timestamp") from error
    if parsed <= 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def _iso_timestamp_ns(value: object, field: str) -> int:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return int(parsed.astimezone(timezone.utc).timestamp() * 1_000_000_000)


def _provenance(raw: Mapping[str, Any]) -> dict[str, str]:
    if set(raw) != set(PROVENANCE_FIELDS):
        raise ValueError("candidate rollover provenance fields are incomplete or unknown")
    result = {field: str(raw[field]) for field in PROVENANCE_FIELDS}
    _sha256(result["candidate_digest"], "candidate_digest")
    _sha256(result["artifact_image_digest"], "artifact_image_digest", image=True)
    for field in (
        "sbom_digest",
        "contract_digest",
        "partition_plan_digest",
        "rollback_manifest_digest",
    ):
        _sha256(result[field], field)
    if any(not result[field].strip() for field in set(PROVENANCE_FIELDS) - {
        "candidate_digest", "artifact_image_digest", "sbom_digest", "contract_digest",
        "partition_plan_digest", "rollback_manifest_digest",
    }):
        raise ValueError("candidate rollover provenance text is incomplete")
    return result


@dataclass(frozen=True, slots=True)
class CandidateRolloverSlice:
    rollover_id: str
    slice_id: str
    expected_revision: int
    expected_owner_id: str
    expected_lease_epoch: int
    expected_partition_plan_epoch: int
    expected_partition_id: str
    expected_candidate_digest: str
    expected_artifact_image_digest: str
    new_owner_id: str
    new_lease_epoch: int
    new_provenance: Mapping[str, str]

    @classmethod
    def parse(cls, raw: Mapping[str, Any]) -> "CandidateRolloverSlice":
        required = {
            "rollover_id", "slice_id", "expected_revision", "expected_owner_id",
            "expected_lease_epoch", "expected_partition_plan_epoch",
            "expected_partition_id", "expected_candidate_digest",
            "expected_artifact_image_digest",
            "new_owner_id", "new_lease_epoch", "new_provenance",
        }
        if set(raw) != required:
            raise ValueError("candidate rollover slice fields are incomplete or unknown")
        expected_revision = int(raw["expected_revision"])
        expected_lease = int(raw["expected_lease_epoch"])
        expected_plan = int(raw["expected_partition_plan_epoch"])
        new_lease = int(raw["new_lease_epoch"])
        if (
            expected_revision < 1
            or expected_lease < 1
            or expected_plan < 1
            or new_lease <= expected_lease
            or not str(raw["slice_id"]).strip()
            or not str(raw["expected_partition_id"]).strip()
            or not str(raw["expected_owner_id"]).strip()
            or not str(raw["new_owner_id"]).strip()
        ):
            raise ValueError("candidate rollover slice identity/revision is invalid")
        return cls(
            rollover_id=_uuid(raw["rollover_id"], "rollover_id"),
            slice_id=str(raw["slice_id"]),
            expected_revision=expected_revision,
            expected_owner_id=str(raw["expected_owner_id"]),
            expected_lease_epoch=expected_lease,
            expected_partition_plan_epoch=expected_plan,
            expected_partition_id=str(raw["expected_partition_id"]),
            expected_candidate_digest=_sha256(
                raw["expected_candidate_digest"], "expected_candidate_digest"
            ),
            expected_artifact_image_digest=_sha256(
                raw["expected_artifact_image_digest"],
                "expected_artifact_image_digest",
                image=True,
            ),
            new_owner_id=str(raw["new_owner_id"]),
            new_lease_epoch=new_lease,
            new_provenance=_provenance(raw["new_provenance"]),
        )


@dataclass(frozen=True, slots=True)
class CandidateRolloverPacket:
    raw: Mapping[str, Any]
    packet_id: str
    candidate_digest: str
    candidate: Mapping[str, str]
    prerequisite_bundle: Mapping[str, Any]
    rollovers: tuple[CandidateRolloverSlice, ...]

    @classmethod
    def parse(
        cls, raw: Mapping[str, Any], *, now_ns: int | None = None
    ) -> "CandidateRolloverPacket":
        required = {
            "schema", "packet_id", "issued_at_ns", "expires_at_ns", "actor",
            "change_ticket", "candidate", "candidate_digest", "prerequisite_bundle",
            "rollovers",
        }
        if set(raw) != required or raw.get("schema") != SCHEMA:
            raise ValueError("candidate rollover packet schema/fields are invalid")
        now = time.time_ns() if now_ns is None else int(now_ns)
        issued = _timestamp_ns(raw["issued_at_ns"], "issued_at_ns")
        expires = _timestamp_ns(raw["expires_at_ns"], "expires_at_ns")
        if not issued <= now < expires:
            raise ValueError("candidate rollover packet approval window is inactive")
        if not str(raw["actor"]).strip() or not str(raw["change_ticket"]).strip():
            raise ValueError("candidate rollover actor/change ticket is required")
        candidate_digest = _sha256(raw["candidate_digest"], "candidate_digest")
        candidate = raw["candidate"]
        candidate_fields = {
            "rust_image_digest", "contract_digest", "partition_plan_digest",
            "promotion_scope_digest", "production_core_manifest_digest",
        }
        if not isinstance(candidate, Mapping) or set(candidate) != candidate_fields:
            raise ValueError("candidate rollover candidate descriptor is invalid")
        candidate = {key: str(candidate[key]) for key in candidate_fields}
        _sha256(candidate["rust_image_digest"], "candidate rust image", image=True)
        for field in candidate_fields - {"rust_image_digest"}:
            _sha256(candidate[field], f"candidate {field}")
        if _digest(candidate) != candidate_digest:
            raise ValueError("candidate rollover candidate digest differs from descriptor")
        bundle = raw["prerequisite_bundle"]
        if not isinstance(bundle, Mapping) or set(bundle) != {
            "bundle_id", "policy_revision", "decision", "evidence", "evidence_sha256",
            "issued_by", "issued_at", "expires_at",
        }:
            raise ValueError("candidate rollover prerequisite bundle is invalid")
        _uuid(bundle["bundle_id"], "bundle_id")
        if (
            int(bundle["policy_revision"]) < 1
            or bundle["decision"] != "GO"
            or not isinstance(bundle["evidence"], Mapping)
            or _digest(bundle["evidence"]) != _sha256(
                bundle["evidence_sha256"], "evidence_sha256"
            )
            or not str(bundle["issued_by"]).strip()
            or _iso_timestamp_ns(bundle["issued_at"], "bundle issued_at")
            >= _iso_timestamp_ns(bundle["expires_at"], "bundle expires_at")
            or _iso_timestamp_ns(bundle["expires_at"], "bundle expires_at") < expires
        ):
            raise ValueError("candidate rollover prerequisite bundle differs from packet")
        values = raw["rollovers"]
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise ValueError("candidate rollover rows must be a list")
        rollovers = tuple(CandidateRolloverSlice.parse(value) for value in values)
        if len(rollovers) != 12:
            raise ValueError("candidate rollover requires exactly twelve crypto slices")
        if len({item.rollover_id for item in rollovers}) != len(rollovers) or len(
            {item.slice_id for item in rollovers}
        ) != len(rollovers):
            raise ValueError("candidate rollover identities must be unique")
        if any(item.new_provenance["candidate_digest"] != candidate_digest for item in rollovers):
            raise ValueError("candidate rollover slice candidate differs from packet")
        if any(
            item.new_provenance["artifact_image_digest"] != candidate["rust_image_digest"]
            or item.new_provenance["contract_digest"] != candidate["contract_digest"]
            or item.new_provenance["partition_plan_digest"] != candidate["partition_plan_digest"]
            for item in rollovers
        ):
            raise ValueError("candidate rollover provenance differs from candidate descriptor")
        return cls(
            raw=dict(raw),
            packet_id=_uuid(raw["packet_id"], "packet_id"),
            candidate_digest=candidate_digest,
            candidate=candidate,
            prerequisite_bundle=dict(bundle),
            rollovers=rollovers,
        )

    @property
    def digest(self) -> str:
        return _digest(self.raw)

    @property
    def confirmation_token(self) -> str:
        return f"APPLY_R1_ROLLOVER_{self.digest[:16]}"

    def plan(self) -> dict[str, Any]:
        return {
            "schema": "qdl.r1.authority-candidate-rollover-plan.v1",
            "packet_id": self.packet_id,
            "packet_digest": self.digest,
            "candidate_digest": self.candidate_digest,
            "confirmation_token": self.confirmation_token,
            "slice_count": len(self.rollovers),
            "apply_requested": False,
            "kafka_offset_mutations": 0,
            "production_mutations": 0,
        }


def prepare_rollover_packet(
    bootstrap: Mapping[str, Any],
    current_rows: Sequence[Mapping[str, Any]],
    *,
    actor: str,
    change_ticket: str,
    issued_at_ns: int | None = None,
    ttl_seconds: int = 1_800,
) -> dict[str, Any]:
    """Build one exact blocked-only rollover packet from a fresh C40 candidate."""

    from scripts.phasec40_authority_bootstrap import validate_packet as validate_bootstrap

    now = time.time_ns() if issued_at_ns is None else int(issued_at_ns)
    validate_bootstrap(bootstrap, now_ns=now)
    if not actor.strip() or not change_ticket.strip() or not 60 <= ttl_seconds <= 86_400:
        raise ValueError("candidate rollover actor/ticket/TTL is invalid")
    candidate_digest = _sha256(bootstrap["candidate_digest"], "candidate_digest")
    bootstrap_by_slice = {str(item["slice_id"]): item for item in bootstrap["slices"]}
    if len(bootstrap_by_slice) != 12:
        raise ValueError("candidate rollover bootstrap scope is not exact")
    current_by_slice = {str(item.get("slice_id", "")): item for item in current_rows}
    if set(current_by_slice) != set(bootstrap_by_slice):
        raise ValueError("candidate rollover current authority scope differs from bootstrap")
    rollovers = []
    for slice_id in sorted(bootstrap_by_slice):
        current = current_by_slice[slice_id]
        candidate = bootstrap_by_slice[slice_id]
        if str(current.get("state")) != "BLOCKED":
            raise ValueError(f"candidate rollover requires BLOCKED authority: {slice_id}")
        for field in STATIC_SEMANTIC_FIELDS:
            if str(current.get(field)) != str(candidate.get(field)):
                raise ValueError(
                    f"candidate rollover cannot change source semantics field={field} slice={slice_id}"
                )
        provenance = _provenance({field: candidate[field] for field in PROVENANCE_FIELDS})
        if provenance["candidate_digest"] != candidate_digest:
            raise ValueError("candidate rollover bootstrap candidate differs from slice")
        old_candidate = _sha256(current.get("candidate_digest"), "expected_candidate_digest")
        old_image = _sha256(
            current.get("artifact_image_digest"), "expected_artifact_image_digest", image=True
        )
        if (
            old_candidate == provenance["candidate_digest"]
            or old_image == provenance["artifact_image_digest"]
        ):
            raise ValueError("candidate rollover requires a new candidate and immutable image")
        expected_revision = int(current["authority_revision"])
        expected_lease = int(current["lease_epoch"])
        rollover_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r1-rollover:{candidate_digest}:{slice_id}:{expected_revision}:{expected_lease}",
        ))
        rollovers.append({
            "rollover_id": rollover_id,
            "slice_id": slice_id,
            "expected_revision": expected_revision,
            "expected_owner_id": str(current["owner_id"]),
            "expected_lease_epoch": expected_lease,
            "expected_partition_plan_epoch": int(current["partition_plan_epoch"]),
            "expected_partition_id": str(current["partition_id"]),
            "expected_candidate_digest": old_candidate,
            "expected_artifact_image_digest": old_image,
            "new_owner_id": str(current["owner_id"]),
            "new_lease_epoch": expected_lease + 1,
            "new_provenance": provenance,
        })
    packet = {
        "schema": SCHEMA,
        "packet_id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"qdl-r1-rollover-packet:{candidate_digest}:{','.join(item['rollover_id'] for item in rollovers)}",
        )),
        "issued_at_ns": now,
        "expires_at_ns": now + ttl_seconds * 1_000_000_000,
        "actor": actor,
        "change_ticket": change_ticket,
        "candidate": dict(bootstrap["candidate"]),
        "candidate_digest": candidate_digest,
        "prerequisite_bundle": dict(bootstrap["prerequisite_bundle"]),
        "rollovers": rollovers,
    }
    CandidateRolloverPacket.parse(packet, now_ns=now)
    return packet

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


_SCHEMA = "qdl.c3.authority-cutover-packet.v1"
_PAIRS = {
    "SHADOW_VALIDATE": ("RUST_SHADOW", "VALIDATING"),
    "REVALIDATE": ("BLOCKED", "VALIDATING"),
    "CANARY": ("VALIDATING", "RUST_CANARY"),
    "PRIMARY": ("RUST_CANARY", "RUST_PRIMARY"),
    "BLOCK_CANARY": ("RUST_CANARY", "BLOCKED"),
    "BLOCK_PRIMARY": ("RUST_PRIMARY", "BLOCKED"),
    "ROLLBACK_PENDING": ("BLOCKED", "ROLLBACK_PENDING"),
    "PYTHON_RESTORE": ("ROLLBACK_PENDING", "PYTHON_PRIMARY"),
}
_HANDOFF_STAGES = {"PRIMARY", "PYTHON_RESTORE"}
_ACTIVE_STAGES = {"CANARY", "PRIMARY"}
_CLEAN_EVIDENCE_STAGES = _ACTIVE_STAGES | {"SHADOW_VALIDATE", "REVALIDATE"}


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _uuid(value: object, name: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except ValueError as error:
        raise ValueError(f"{name} must be a UUID") from error


def _hex(value: object, name: str, *, image: bool = False) -> str:
    text = str(value)
    prefix = "sha256:" if image else ""
    raw = text.removeprefix(prefix) if prefix else text
    if (image and not text.startswith(prefix)) or len(raw) != 64 or any(
        char not in "0123456789abcdef" for char in raw
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return text


def _exact(value: Mapping[str, Any], fields: set[str], name: str) -> None:
    if set(value) != fields:
        raise ValueError(f"{name} fields are incomplete or unknown")


@dataclass(frozen=True, slots=True)
class CutoverSlice:
    transition_id: str
    handoff_id: str | None
    slice_id: str
    expected_state: str
    expected_revision: int
    expected_owner_id: str
    expected_lease_epoch: int
    partition_plan_epoch: int
    new_state: str
    new_owner_id: str
    new_lease_epoch: int
    terminal_watermark: int | None
    prerequisite_bundle_id: str | None
    hold_until: str | None
    reason: str

    @classmethod
    def parse(cls, raw: Mapping[str, Any], *, stage: str) -> "CutoverSlice":
        _exact(
            raw,
            {
                "transition_id", "handoff_id", "slice_id", "expected_state",
                "expected_revision", "expected_owner_id", "expected_lease_epoch",
                "partition_plan_epoch", "new_state", "new_owner_id",
                "new_lease_epoch", "terminal_watermark",
                "prerequisite_bundle_id", "hold_until", "reason",
            },
            "cutover slice",
        )
        expected_pair = _PAIRS[stage]
        if (raw["expected_state"], raw["new_state"]) != expected_pair:
            raise ValueError("cutover slice state pair differs from packet stage")
        handoff_id = raw["handoff_id"]
        prerequisite = raw["prerequisite_bundle_id"]
        if stage in _HANDOFF_STAGES:
            handoff_id = _uuid(handoff_id, "handoff_id")
        elif handoff_id is not None:
            raise ValueError("non-handoff stage cannot carry handoff_id")
        if stage in _ACTIVE_STAGES:
            prerequisite = _uuid(prerequisite, "prerequisite_bundle_id")
            if raw["terminal_watermark"] is None or int(raw["terminal_watermark"]) < 0:
                raise ValueError("active Rust stage requires terminal watermark")
            if not str(raw["hold_until"] or "").strip():
                raise ValueError("active Rust stage requires hold_until")
        elif prerequisite is not None or raw["hold_until"] is not None:
            raise ValueError("inactive stage cannot carry prerequisite/hold")
        positive = (
            int(raw["expected_revision"]), int(raw["expected_lease_epoch"]),
            int(raw["partition_plan_epoch"]), int(raw["new_lease_epoch"]),
        )
        if any(value <= 0 for value in positive):
            raise ValueError("cutover slice revisions/epochs must be positive")
        if (
            raw["expected_owner_id"] != raw["new_owner_id"]
            and int(raw["new_lease_epoch"]) <= int(raw["expected_lease_epoch"])
        ):
            raise ValueError("authority owner change requires newer lease")
        if any(
            not str(value).strip()
            for value in (
                raw["slice_id"], raw["expected_owner_id"],
                raw["new_owner_id"], raw["reason"],
            )
        ):
            raise ValueError("cutover slice identity/reason is incomplete")
        return cls(
            transition_id=_uuid(raw["transition_id"], "transition_id"),
            handoff_id=handoff_id,
            slice_id=str(raw["slice_id"]),
            expected_state=str(raw["expected_state"]),
            expected_revision=int(raw["expected_revision"]),
            expected_owner_id=str(raw["expected_owner_id"]),
            expected_lease_epoch=int(raw["expected_lease_epoch"]),
            partition_plan_epoch=int(raw["partition_plan_epoch"]),
            new_state=str(raw["new_state"]),
            new_owner_id=str(raw["new_owner_id"]),
            new_lease_epoch=int(raw["new_lease_epoch"]),
            terminal_watermark=(
                int(raw["terminal_watermark"])
                if raw["terminal_watermark"] is not None else None
            ),
            prerequisite_bundle_id=prerequisite,
            hold_until=(
                str(raw["hold_until"]) if raw["hold_until"] is not None else None
            ),
            reason=str(raw["reason"]),
        )


@dataclass(frozen=True, slots=True)
class AuthorityCutoverPacket:
    raw: Mapping[str, Any]
    packet_id: str
    stage: str
    actor: str
    candidate_digest: str
    artifact_image_digest: str
    contract_digest: str
    partition_plan_digest: str
    route_manifest_digest: str
    slices: tuple[CutoverSlice, ...]

    @classmethod
    def parse(
        cls, raw: Mapping[str, Any], *, now_ns: int | None = None
    ) -> "AuthorityCutoverPacket":
        _exact(
            raw,
            {
                "schema", "packet_id", "stage", "issued_at_ns", "expires_at_ns",
                "actor", "change_ticket", "candidate_digest",
                "artifact_image_digest", "contract_digest",
                "partition_plan_digest", "route_manifest_digest",
                "consumer_route", "evidence", "slices",
            },
            "cutover packet",
        )
        if raw["schema"] != _SCHEMA or raw["stage"] not in _PAIRS:
            raise ValueError("cutover packet schema/stage is unsupported")
        current = time.time_ns() if now_ns is None else now_ns
        issued = int(raw["issued_at_ns"])
        expires = int(raw["expires_at_ns"])
        if issued <= 0 or expires <= issued or not issued <= current < expires:
            raise ValueError("cutover packet approval window is invalid")
        if not str(raw["actor"]).strip() or not str(raw["change_ticket"]).strip():
            raise ValueError("cutover operator/change ticket is required")
        route = raw["consumer_route"]
        evidence = raw["evidence"]
        if not isinstance(route, Mapping) or not isinstance(evidence, Mapping):
            raise ValueError("cutover route/evidence must be objects")
        _exact(
            route,
            {
                "consumer_id", "expected_route", "new_route", "rollback_route",
                "rollback_command",
            },
            "consumer route",
        )
        rollback = route["rollback_command"]
        if (
            route["consumer_id"] != "trading-system"
            or route["rollback_route"] != "V1"
            or not isinstance(rollback, Sequence)
            or isinstance(rollback, (str, bytes))
            or not rollback
            or any(not isinstance(item, str) or not item.strip() for item in rollback)
        ):
            raise ValueError("cutover Trading System rollback route is invalid")
        _exact(
            evidence,
            {
                "provider_provenance", "semantic_mismatches", "open_gaps",
                "duplicate_external_effects", "consumer_errors",
            },
            "cutover evidence",
        )
        evidence_counts = {}
        try:
            evidence_counts = {
                name: int(evidence[name])
                for name in (
                    "semantic_mismatches", "open_gaps",
                    "duplicate_external_effects", "consumer_errors",
                )
            }
        except (TypeError, ValueError) as error:
            raise ValueError("cutover evidence counts are invalid") from error
        if raw["stage"] in _CLEAN_EVIDENCE_STAGES:
            if (
                evidence["provider_provenance"] != "REAL"
                or any(value != 0 for value in evidence_counts.values())
            ):
                raise ValueError("cutover evidence is not clean/authentic")
        elif (
            evidence["provider_provenance"] not in {"REAL", "UNKNOWN"}
            or any(value < 0 for value in evidence_counts.values())
        ):
            raise ValueError("block/rollback cutover evidence is invalid")
        values = raw["slices"]
        if not isinstance(values, list) or not 1 <= len(values) <= 32:
            raise ValueError("cutover packet requires 1..32 slices")
        slices = tuple(
            CutoverSlice.parse(value, stage=str(raw["stage"])) for value in values
        )
        identities = [item.slice_id for item in slices]
        if len(identities) != len(set(identities)):
            raise ValueError("cutover packet slice IDs must be unique")
        packet = cls(
            raw=dict(raw),
            packet_id=_uuid(raw["packet_id"], "packet_id"),
            stage=str(raw["stage"]),
            actor=str(raw["actor"]),
            candidate_digest=_hex(raw["candidate_digest"], "candidate_digest"),
            artifact_image_digest=_hex(
                raw["artifact_image_digest"], "artifact_image_digest", image=True
            ),
            contract_digest=_hex(raw["contract_digest"], "contract_digest"),
            partition_plan_digest=_hex(
                raw["partition_plan_digest"], "partition_plan_digest"
            ),
            route_manifest_digest=_hex(
                raw["route_manifest_digest"], "route_manifest_digest"
            ),
            slices=slices,
        )
        return packet

    @property
    def digest(self) -> str:
        return _digest(self.raw)

    @property
    def confirmation_token(self) -> str:
        return f"APPLY_C3_{self.digest[:16]}"

    def plan(self) -> dict[str, Any]:
        return {
            "schema": "qdl.c3.authority-cutover-plan.v1",
            "packet_id": self.packet_id,
            "packet_digest": self.digest,
            "confirmation_token": self.confirmation_token,
            "stage": self.stage,
            "slice_count": len(self.slices),
            "slice_ids": [item.slice_id for item in self.slices],
            "apply_requested": False,
            "production_mutations": 0,
        }

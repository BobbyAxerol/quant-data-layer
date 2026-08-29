from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from qdl.query import AccessPurpose, ConsumerGrade, DataRequirement, FeedType


_DATA_PLANE_PERMISSIONS = frozenset({
    "instruments:read",
    "snapshot:read",
    "history:read",
    "status:read",
    "stream:read",
    "quality:read",
})


class MigrationState(StrEnum):
    REGISTERED = "REGISTERED"
    SHADOW = "SHADOW"
    ACCEPTED = "ACCEPTED"
    ACTIVE = "ACTIVE"
    ROLLED_BACK = "ROLLED_BACK"


class ConsumerRoute(StrEnum):
    V1 = "V1"
    V1_WITH_V2_SHADOW = "V1_WITH_V2_SHADOW"
    V2 = "V2"


_TRANSITIONS = {
    MigrationState.REGISTERED: frozenset({MigrationState.SHADOW}),
    MigrationState.SHADOW: frozenset({MigrationState.ACCEPTED, MigrationState.ROLLED_BACK}),
    MigrationState.ACCEPTED: frozenset({MigrationState.ACTIVE, MigrationState.ROLLED_BACK}),
    MigrationState.ACTIVE: frozenset({MigrationState.ROLLED_BACK}),
    MigrationState.ROLLED_BACK: frozenset({MigrationState.SHADOW}),
}


@dataclass(frozen=True)
class ConsumerQuotas:
    requests_per_minute: int
    max_batch_items: int
    max_warmup_rows: int
    max_streams: int
    max_buffer_events: int

    def __post_init__(self) -> None:
        values = (
            self.requests_per_minute,
            self.max_batch_items,
            self.max_warmup_rows,
            self.max_streams,
            self.max_buffer_events,
        )
        if any(value <= 0 for value in values):
            raise ValueError("consumer quotas must be positive")
        if self.max_batch_items > 100 or self.max_warmup_rows > 10_000:
            raise ValueError("consumer query quota exceeds the V2 service boundary")
        if self.max_buffer_events > 10_000:
            raise ValueError("consumer stream buffer quota exceeds the V2 service boundary")


@dataclass(frozen=True)
class ConsumerManifest:
    consumer_id: str
    owner: str
    subject: str
    environment: str
    manifest_revision: int
    sdk_major: int
    allowed_purposes: frozenset[AccessPurpose]
    allowed_permissions: frozenset[str]
    execution_dependency: str
    quotas: ConsumerQuotas
    requirements: tuple[DataRequirement, ...]
    rollback_contract: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.consumer_id, self.owner, self.subject, self.environment)
        ):
            raise ValueError("consumer manifest identity, subject and environment are required")
        if self.manifest_revision < 1:
            raise ValueError("consumer manifest revision must be positive")
        if self.sdk_major != 2:
            raise ValueError("V2 consumer manifest requires sdk_major=2")
        if not self.allowed_purposes or not self.allowed_permissions:
            raise ValueError("consumer manifest access policy cannot be empty")
        if self.allowed_permissions - _DATA_PLANE_PERMISSIONS:
            raise ValueError("consumer manifest contains an unknown data-plane permission")
        if self.execution_dependency not in {"FORBIDDEN", "PAPER_ONLY", "ALLOWED"}:
            raise ValueError("consumer execution dependency policy is invalid")
        if not self.requirements:
            raise ValueError("consumer manifest requires at least one data requirement")
        if self.rollback_contract not in {"V1", "V2"}:
            raise ValueError("rollback_contract must be V1 or V2")
        if len(self.manifest_sha256) != 64:
            raise ValueError("manifest SHA-256 is invalid")

    def requirement_allowed(self, requirement: DataRequirement) -> bool:
        return any(
            configured.instrument_uid == requirement.instrument_uid
            and configured.feed is requirement.feed
            and configured.interval == requirement.interval
            and configured.consumer_grade is requirement.consumer_grade
            and configured.source_policy_id == requirement.source_policy_id
            # Event-recency handling is an entitlement property, not a caller
            # preference.  A request cannot turn a strict manifest route into
            # an observed quiet-feed route, or remove its session SLA.
            and configured.event_recency_policy is requirement.event_recency_policy
            and (
                configured.max_session_liveness_ms
                == requirement.max_session_liveness_ms
            )
            for configured in self.requirements
        )

    def purpose_allowed(self, purpose: AccessPurpose) -> bool:
        return purpose in self.allowed_purposes

    def feed_scope_allowed(self, *, instrument_uid: str, feed: FeedType) -> bool:
        return any(
            item.instrument_uid == instrument_uid and item.feed is feed
            for item in self.requirements
        )

    @property
    def allowed_feeds(self) -> frozenset[FeedType]:
        return frozenset(item.feed for item in self.requirements)

    @property
    def allowed_grades(self) -> frozenset[ConsumerGrade]:
        return frozenset(item.consumer_grade for item in self.requirements)


class ConsumerManifestLoader:
    @staticmethod
    def load(path: str | Path) -> ConsumerManifest:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return ConsumerManifestLoader.from_mapping(payload)

    @staticmethod
    def from_mapping(payload: Any) -> ConsumerManifest:
        if not isinstance(payload, dict):
            raise ValueError("consumer manifest must be a mapping")
        if payload.get("apiVersion") != "qdl/v2" or payload.get("kind") != "DataRequirement":
            raise ValueError("consumer manifest apiVersion/kind is unsupported")
        metadata = payload.get("metadata")
        spec = payload.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise ValueError("consumer manifest metadata/spec are required")
        allowed_top = {"apiVersion", "kind", "metadata", "spec"}
        if set(payload) - allowed_top:
            raise ValueError("consumer manifest contains unknown top-level fields")
        if set(metadata) - {"id", "owner", "subject", "environment", "revision"}:
            raise ValueError("consumer manifest metadata contains unknown fields")
        if set(spec) - {
            "sdk_major",
            "rollback_contract",
            "execution_dependency",
            "permissions",
            "purposes",
            "quotas",
            "requirements",
        }:
            raise ValueError("consumer manifest spec contains unknown fields")
        requirements = spec.get("requirements")
        if not isinstance(requirements, list) or not 1 <= len(requirements) <= 100:
            raise ValueError("consumer manifest requires 1..100 requirements")
        permissions = spec.get("permissions")
        purposes = spec.get("purposes")
        quotas = spec.get("quotas")
        if not isinstance(permissions, list) or not permissions:
            raise ValueError("consumer manifest permissions are required")
        if not isinstance(purposes, list) or not purposes:
            raise ValueError("consumer manifest purposes are required")
        if not isinstance(quotas, dict):
            raise ValueError("consumer manifest quotas are required")
        allowed_quota_fields = {
            "requests_per_minute",
            "max_batch_items",
            "max_warmup_rows",
            "max_streams",
            "max_buffer_events",
        }
        if set(quotas) != allowed_quota_fields:
            raise ValueError("consumer manifest quota fields are incomplete or unknown")
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return ConsumerManifest(
            consumer_id=str(metadata.get("id", "")),
            owner=str(metadata.get("owner", "")),
            subject=str(metadata.get("subject", "")),
            environment=str(metadata.get("environment", "")).lower(),
            manifest_revision=int(metadata.get("revision", 0)),
            sdk_major=int(spec.get("sdk_major", 0)),
            allowed_purposes=frozenset(
                AccessPurpose(str(value).upper()) for value in purposes
            ),
            allowed_permissions=frozenset(str(value).lower() for value in permissions),
            execution_dependency=str(
                spec.get("execution_dependency", "FORBIDDEN")
            ).upper(),
            quotas=ConsumerQuotas(**{key: int(value) for key, value in quotas.items()}),
            requirements=tuple(DataRequirement.from_mapping(item) for item in requirements),
            rollback_contract=str(spec.get("rollback_contract", "V1")).upper(),
            manifest_sha256=hashlib.sha256(canonical).hexdigest(),
        )


class ConsumerManifestRegistry:
    """Immutable-revision lookup used by REST and gRPC data-plane guards."""

    def __init__(self, manifests: tuple[ConsumerManifest, ...] = ()) -> None:
        self._by_id: dict[str, ConsumerManifest] = {}
        self._by_subject: dict[tuple[str, str], ConsumerManifest] = {}
        for manifest in manifests:
            self.register(manifest)

    def register(self, manifest: ConsumerManifest) -> None:
        existing = self._by_id.get(manifest.consumer_id)
        if existing is not None and existing.manifest_sha256 != manifest.manifest_sha256:
            raise ValueError("consumer manifest changed without a governed revision")
        key = (manifest.environment, manifest.subject)
        subject_owner = self._by_subject.get(key)
        if subject_owner is not None and subject_owner.consumer_id != manifest.consumer_id:
            raise ValueError("workload subject is already bound to another consumer")
        self._by_id[manifest.consumer_id] = manifest
        self._by_subject[key] = manifest

    def by_id(self, consumer_id: str) -> ConsumerManifest:
        try:
            return self._by_id[consumer_id]
        except KeyError as error:
            raise KeyError(f"consumer manifest is not registered: {consumer_id}") from error

    def by_subject(self, *, environment: str, subject: str) -> ConsumerManifest:
        try:
            return self._by_subject[(environment.lower(), subject)]
        except KeyError as error:
            raise KeyError("workload subject has no registered consumer manifest") from error

    @property
    def count(self) -> int:
        return len(self._by_id)

    @property
    def revisions(self) -> tuple[tuple[str, int, str], ...]:
        return tuple(sorted(
            (item.consumer_id, item.manifest_revision, item.manifest_sha256)
            for item in self._by_id.values()
        ))


@dataclass(frozen=True)
class ConsumerMigration:
    consumer_id: str
    manifest_sha256: str
    state: MigrationState
    owner: str
    changed_at_ns: int
    reason: str


class ConsumerMigrationRegistry:
    """Fail-closed state machine; no consumer is activated by registration alone."""

    def __init__(self, *, clock_ns=time.time_ns):
        self._clock_ns = clock_ns
        self._items: dict[str, ConsumerMigration] = {}

    def register(self, manifest: ConsumerManifest, *, reason: str) -> ConsumerMigration:
        if not reason.strip():
            raise ValueError("migration registration reason is required")
        existing = self._items.get(manifest.consumer_id)
        if existing and existing.manifest_sha256 != manifest.manifest_sha256:
            raise ValueError("manifest changed without a new governed registration")
        if existing:
            return existing
        item = ConsumerMigration(
            manifest.consumer_id,
            manifest.manifest_sha256,
            MigrationState.REGISTERED,
            manifest.owner,
            self._clock_ns(),
            reason,
        )
        self._items[item.consumer_id] = item
        return item

    def transition(
        self,
        consumer_id: str,
        state: MigrationState,
        *,
        owner: str,
        reason: str,
    ) -> ConsumerMigration:
        current = self.get(consumer_id)
        if owner != current.owner:
            raise PermissionError("consumer migration owner mismatch")
        if state not in _TRANSITIONS[current.state]:
            raise ValueError(f"invalid migration transition: {current.state}->{state}")
        if not reason.strip():
            raise ValueError("migration transition reason is required")
        updated = ConsumerMigration(
            current.consumer_id,
            current.manifest_sha256,
            state,
            owner,
            self._clock_ns(),
            reason,
        )
        self._items[consumer_id] = updated
        return updated

    def get(self, consumer_id: str) -> ConsumerMigration:
        try:
            return self._items[consumer_id]
        except KeyError as error:
            raise KeyError(f"consumer migration is not registered: {consumer_id}") from error

    def route(self, consumer_id: str) -> ConsumerRoute:
        state = self.get(consumer_id).state
        if state in {MigrationState.REGISTERED, MigrationState.ROLLED_BACK}:
            return ConsumerRoute.V1
        if state in {MigrationState.SHADOW, MigrationState.ACCEPTED}:
            return ConsumerRoute.V1_WITH_V2_SHADOW
        return ConsumerRoute.V2


class UsageTelemetry:
    """Bounded aggregate telemetry; never records strategy parameters or payloads."""

    def __init__(self, *, max_consumers: int = 10_000):
        if max_consumers <= 0:
            raise ValueError("max_consumers must be positive")
        self._max_consumers = max_consumers
        self._usage: dict[tuple[str, int, str], dict[str, int]] = {}

    def record(self, *, consumer_id: str, sdk_major: int, contract: str, cursor_offset: int) -> None:
        if not consumer_id.strip() or sdk_major not in {1, 2} or cursor_offset < 0:
            raise ValueError("consumer telemetry identity/version/cursor is invalid")
        key = (consumer_id, sdk_major, contract)
        if key not in self._usage and len(self._usage) >= self._max_consumers:
            raise RuntimeError("consumer telemetry capacity exhausted")
        item = self._usage.setdefault(key, {"requests": 0, "last_cursor_offset": 0})
        item["requests"] += 1
        item["last_cursor_offset"] = max(item["last_cursor_offset"], cursor_offset)

    def snapshot(self) -> tuple[dict[str, int | str | bool], ...]:
        return tuple(
            {
                "consumer_id": consumer_id,
                "sdk_major": sdk_major,
                "contract": contract,
                "requests": values["requests"],
                "last_cursor_offset": values["last_cursor_offset"],
                "deprecated": sdk_major == 1,
            }
            for (consumer_id, sdk_major, contract), values in sorted(self._usage.items())
        )

    def deprecation_notices(self, owners: dict[str, str]) -> tuple[dict[str, str | int], ...]:
        notices = []
        for item in self.snapshot():
            if not item["deprecated"]:
                continue
            owner = owners.get(str(item["consumer_id"]))
            if not owner:
                raise ValueError(
                    f"deprecated consumer has no notification owner: {item['consumer_id']}"
                )
            notices.append({
                "consumer_id": str(item["consumer_id"]),
                "owner": owner,
                "contract": str(item["contract"]),
                "requests": int(item["requests"]),
                "action": "REGISTER_V2_MANIFEST_OR_APPROVE_V1_SUNSET_EXCEPTION",
            })
        return tuple(notices)

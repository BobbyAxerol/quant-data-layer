from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from qdl.query import DataRequirement


class MigrationState(StrEnum):
    REGISTERED = "REGISTERED"
    SHADOW = "SHADOW"
    ACCEPTED = "ACCEPTED"
    ACTIVE = "ACTIVE"
    ROLLED_BACK = "ROLLED_BACK"


_TRANSITIONS = {
    MigrationState.REGISTERED: frozenset({MigrationState.SHADOW}),
    MigrationState.SHADOW: frozenset({MigrationState.ACCEPTED, MigrationState.ROLLED_BACK}),
    MigrationState.ACCEPTED: frozenset({MigrationState.ACTIVE, MigrationState.ROLLED_BACK}),
    MigrationState.ACTIVE: frozenset({MigrationState.ROLLED_BACK}),
    MigrationState.ROLLED_BACK: frozenset({MigrationState.SHADOW}),
}


@dataclass(frozen=True)
class ConsumerManifest:
    consumer_id: str
    owner: str
    sdk_major: int
    requirements: tuple[DataRequirement, ...]
    rollback_contract: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not self.consumer_id.strip() or not self.owner.strip():
            raise ValueError("consumer manifest identity and owner are required")
        if self.sdk_major != 2:
            raise ValueError("Phase 5 consumer manifest requires sdk_major=2")
        if not self.requirements:
            raise ValueError("consumer manifest requires at least one data requirement")
        if self.rollback_contract not in {"V1", "V2"}:
            raise ValueError("rollback_contract must be V1 or V2")
        if len(self.manifest_sha256) != 64:
            raise ValueError("manifest SHA-256 is invalid")


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
        if set(metadata) - {"id", "owner"}:
            raise ValueError("consumer manifest metadata contains unknown fields")
        if set(spec) - {"sdk_major", "rollback_contract", "requirements"}:
            raise ValueError("consumer manifest spec contains unknown fields")
        requirements = spec.get("requirements")
        if not isinstance(requirements, list) or not 1 <= len(requirements) <= 100:
            raise ValueError("consumer manifest requires 1..100 requirements")
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return ConsumerManifest(
            consumer_id=str(metadata.get("id", "")),
            owner=str(metadata.get("owner", "")),
            sdk_major=int(spec.get("sdk_major", 0)),
            requirements=tuple(DataRequirement.from_mapping(item) for item in requirements),
            rollback_contract=str(spec.get("rollback_contract", "V1")).upper(),
            manifest_sha256=hashlib.sha256(canonical).hexdigest(),
        )


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

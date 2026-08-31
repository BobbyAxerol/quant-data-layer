from __future__ import annotations

"""Signed, generation-bound bootstrap cursors for bounded Phase 9.2 cores.

This module has no Kafka mutation primitive.  It only validates a supplied
per-partition tail map and signs it with an operator-provided HMAC key.  Kafka
consumer offsets continue to advance exclusively inside the Rust core's
transactional commits.
"""

from dataclasses import dataclass
import hashlib
import hmac
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any
import uuid


SIGNED_BOOTSTRAP_SCHEMA = "qdl.phase92.signed-bootstrap-cursor.v1"
BOOTSTRAP_PAYLOAD_SCHEMA = "qdl.phase92.bootstrap-cursor-payload.v1"


@dataclass(frozen=True, slots=True)
class BootstrapScope:
    consumer_group_id: str
    raw_topics: tuple[str, ...]
    promotion_scope_digest: str
    candidate_digest: str
    partition_plan_epoch: int

    def validate(self) -> None:
        if (
            not self.consumer_group_id.strip()
            or not self.raw_topics
            or len(set(self.raw_topics)) != len(self.raw_topics)
            or any(not topic.strip() for topic in self.raw_topics)
            or not _sha256(self.promotion_scope_digest)
            or not _sha256(self.candidate_digest)
            or self.partition_plan_epoch < 1
        ):
            raise ValueError("Phase 9.2 bootstrap scope is invalid")


@dataclass(frozen=True, slots=True)
class BootstrapPartition:
    topic: str
    partition: int
    offset: int

    def validate(self, raw_topics: frozenset[str]) -> None:
        if (
            self.topic not in raw_topics
            or self.partition < 0
            or self.offset < 0
            or self.offset > 2**63 - 1
        ):
            raise ValueError("Phase 9.2 bootstrap partition is invalid")


def build_signed_cursor(
    *,
    key_id: str,
    keyring: Mapping[str, str],
    scope: BootstrapScope,
    partitions: Sequence[BootstrapPartition],
    generation: int,
    issued_at_ns: int | None = None,
    ttl_seconds: int = 1_800,
    cursor_id: str | None = None,
) -> dict[str, str]:
    """Return a signed envelope compatible with the Rust Phase 9.2 core."""

    scope.validate()
    if not key_id.strip() or generation < 1 or not 60 <= ttl_seconds <= 86_400:
        raise ValueError("Phase 9.2 bootstrap key/generation/TTL is invalid")
    key = _key_bytes(keyring, key_id)
    if issued_at_ns is None:
        issued_at_ns = time.time_ns()
    if issued_at_ns <= 0:
        raise ValueError("Phase 9.2 bootstrap issuance time is invalid")
    if cursor_id is None:
        cursor_id = str(uuid.uuid4())
    try:
        parsed_cursor = uuid.UUID(cursor_id)
    except (ValueError, AttributeError) as error:
        raise ValueError("Phase 9.2 bootstrap cursor ID must be a UUID") from error
    if str(parsed_cursor) != cursor_id.lower():
        raise ValueError("Phase 9.2 bootstrap cursor ID must be canonical UUID")

    raw_topics = frozenset(scope.raw_topics)
    ordered = sorted(partitions, key=lambda item: (item.topic, item.partition))
    if not ordered:
        raise ValueError("Phase 9.2 bootstrap requires at least one partition")
    seen: set[tuple[str, int]] = set()
    for item in ordered:
        item.validate(raw_topics)
        identity = (item.topic, item.partition)
        if identity in seen:
            raise ValueError("Phase 9.2 bootstrap partitions must be unique")
        seen.add(identity)
    payload = {
        "schema": BOOTSTRAP_PAYLOAD_SCHEMA,
        "cursor_id": str(parsed_cursor),
        "generation": generation,
        "issued_at_ns": issued_at_ns,
        "expires_at_ns": issued_at_ns + ttl_seconds * 1_000_000_000,
        "consumer_group_id": scope.consumer_group_id,
        "raw_topics": sorted(scope.raw_topics),
        "promotion_scope_digest": scope.promotion_scope_digest,
        "candidate_digest": scope.candidate_digest,
        "partition_plan_epoch": scope.partition_plan_epoch,
        "partitions": [
            {"topic": item.topic, "partition": item.partition, "offset": item.offset}
            for item in ordered
        ],
    }
    payload_bytes = _encoded(payload)
    return {
        "schema": SIGNED_BOOTSTRAP_SCHEMA,
        "key_id": key_id,
        "payload_hex": payload_bytes.hex(),
        "signature_hex": hmac.new(key, payload_bytes, hashlib.sha256).hexdigest(),
    }


def verify_signed_cursor(
    envelope: Mapping[str, Any], *, keyring: Mapping[str, str], scope: BootstrapScope
) -> dict[str, Any]:
    """Strict local verifier used by the issuer tests and preflight tooling."""

    scope.validate()
    if set(envelope) != {"schema", "key_id", "payload_hex", "signature_hex"}:
        raise ValueError("Phase 9.2 signed bootstrap cursor fields are invalid")
    if envelope.get("schema") != SIGNED_BOOTSTRAP_SCHEMA:
        raise ValueError("Phase 9.2 signed bootstrap cursor schema is invalid")
    key_id = str(envelope.get("key_id") or "")
    key = _key_bytes(keyring, key_id)
    try:
        payload_bytes = bytes.fromhex(str(envelope["payload_hex"]))
        signature = bytes.fromhex(str(envelope["signature_hex"]))
    except ValueError as error:
        raise ValueError("Phase 9.2 signed bootstrap cursor is not hexadecimal") from error
    if len(signature) != 32 or not hmac.compare_digest(
        signature, hmac.new(key, payload_bytes, hashlib.sha256).digest()
    ):
        raise ValueError("Phase 9.2 signed bootstrap cursor signature is invalid")
    try:
        payload = json.loads(payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Phase 9.2 signed bootstrap payload is invalid") from error
    if not isinstance(payload, dict) or set(payload) != {
        "schema", "cursor_id", "generation", "issued_at_ns", "expires_at_ns",
        "consumer_group_id", "raw_topics", "promotion_scope_digest",
        "candidate_digest", "partition_plan_epoch", "partitions",
    }:
        raise ValueError("Phase 9.2 signed bootstrap payload fields are invalid")
    expected = {
        "consumer_group_id": scope.consumer_group_id,
        "raw_topics": sorted(scope.raw_topics),
        "promotion_scope_digest": scope.promotion_scope_digest,
        "candidate_digest": scope.candidate_digest,
        "partition_plan_epoch": scope.partition_plan_epoch,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise ValueError("Phase 9.2 signed bootstrap cursor scope mismatch")
    try:
        parsed = uuid.UUID(str(payload["cursor_id"]))
    except ValueError as error:
        raise ValueError("Phase 9.2 signed bootstrap cursor ID is invalid") from error
    if str(parsed) != str(payload["cursor_id"]).lower():
        raise ValueError("Phase 9.2 signed bootstrap cursor ID is noncanonical")
    if (
        not isinstance(payload["generation"], int)
        or payload["generation"] < 1
        or not isinstance(payload["issued_at_ns"], int)
        or not isinstance(payload["expires_at_ns"], int)
        or payload["issued_at_ns"] <= 0
        or payload["expires_at_ns"] <= payload["issued_at_ns"]
    ):
        raise ValueError("Phase 9.2 signed bootstrap payload timing is invalid")
    items = payload["partitions"]
    if not isinstance(items, list) or not items:
        raise ValueError("Phase 9.2 signed bootstrap partition list is invalid")
    parsed_partitions = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"topic", "partition", "offset"}:
            raise ValueError("Phase 9.2 signed bootstrap partition fields are invalid")
        parsed_partitions.append(BootstrapPartition(
            topic=str(item["topic"]), partition=item["partition"], offset=item["offset"]
        ))
    expected_topics = frozenset(scope.raw_topics)
    if len({(item.topic, item.partition) for item in parsed_partitions}) != len(parsed_partitions):
        raise ValueError("Phase 9.2 signed bootstrap partitions are duplicated")
    for item in parsed_partitions:
        if not isinstance(item.partition, int) or not isinstance(item.offset, int):
            raise ValueError("Phase 9.2 signed bootstrap partition types are invalid")
        item.validate(expected_topics)
    return payload


def envelope_sha256(envelope: Mapping[str, Any]) -> str:
    return hashlib.sha256(_encoded(dict(envelope))).hexdigest()


def _encoded(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _sha256(value: object) -> bool:
    text = str(value)
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _key_bytes(keyring: Mapping[str, str], key_id: str) -> bytes:
    encoded = keyring.get(key_id)
    if not key_id or not isinstance(encoded, str) or not encoded:
        raise ValueError("Phase 9.2 bootstrap signing key is unavailable")
    if any(character not in "0123456789abcdef" for character in encoded):
        raise ValueError("Phase 9.2 bootstrap signing key must be lowercase hexadecimal")
    try:
        result = bytes.fromhex(encoded)
    except ValueError as error:
        raise ValueError("Phase 9.2 bootstrap signing key is invalid") from error
    if len(result) < 32:
        raise ValueError("Phase 9.2 bootstrap signing key is shorter than 256 bits")
    return result

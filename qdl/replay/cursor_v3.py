"""Signed handoff cursor v3 (KN-1 K1.3): the Kafka-native recovery contract.

Query issues the token with a snapshot; the native Rust Stream verifies it and
re-signs one per delivered record. Both sides must therefore agree byte for
byte, so the body is a canonical JSON object whose rules are small enough to
implement identically in Python and Rust:

* keys sorted by byte order, no whitespace, ``,``/``:`` separators;
* values are strings drawn from ``_TOKEN_CHARSET`` (never escaped) or
  unsigned integers ``0 <= n < 2**63`` written in decimal, never floats;
* token = ``base64url(body) + "." + base64url(HMAC-SHA256(key, body))``
  without padding. Verification uses the received body bytes, not a
  re-serialization.

Claims (guide 18.5, invariant 29): environment, authenticated consumer,
normalized delivery-requirement digest, public schema major, canonical stream,
logical product key, snapshot identity, source coordinate (Kafka topic id,
physical partition, last applied canonical offset), partition-plan epoch,
source-policy and catalog revisions, route generation, issue/expiry.

Typed outcomes follow the SDK recovery contract: a token that is authentic but
belongs to another generation, route, policy/catalog revision, schema major or
an earlier cursor schema (v1/v2), or that has expired, is ``CursorExpired``
(gRPC OUT_OF_RANGE -> SDK fresh snapshot). A token that is malformed, forged,
signed with an unknown key, or bound to another consumer/environment/
requirement is ``CursorInvalid`` (INVALID_ARGUMENT). The SDK raises on the
latter, so nothing that a normal migration produces may land there.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, fields
import hashlib
import hmac
import json
import re
from typing import Any, Mapping

from qdl.transport.contracts import CursorExpired

SCHEMA_V3 = "qdl.handoff-cursor.v3"
LEGACY_SCHEMAS = frozenset({"qdl.handoff-cursor.v1", "qdl.handoff-cursor.v2"})
REQUIREMENT_DIGEST_SCHEMA = "qdl.requirement-digest.v1"
MAX_OFFSET = 2**63 - 1
# Always ``fullmatch``: ``re.match`` with ``$`` also accepts a trailing "\n".
_TOKEN_CHARSET = re.compile(r"[A-Za-z0-9._:/@|+=-]{1,256}")
_HEX64 = re.compile(r"[0-9a-f]{64}")


class CursorInvalid(ValueError):
    """Malformed, forged or foreign token; the SDK does not auto-recover."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"CURSOR_INVALID:{reason}" + (f":{detail}" if detail else ""))
        self.reason = reason


class CursorV3Expired(CursorExpired):
    """Authentic but no longer resumable here; the SDK takes a fresh snapshot."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"CURSOR_EXPIRED:{reason}" + (f":{detail}" if detail else ""))
        self.reason = reason


@dataclass(frozen=True)
class CursorV3Claims:
    key_id: str
    environment: str
    consumer_id: str
    requirement_digest: str
    schema_major: int
    stream: str
    product_key: str
    snapshot_id: str
    source_topic_id: str
    source_partition: int
    source_offset: int
    partition_plan_epoch: int
    source_policy_revision: int
    catalog_revision: int
    route_generation: str
    issued_at_ns: int
    expires_at_ns: int

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if item.type == "int":
                if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_OFFSET:
                    raise CursorInvalid("FIELD_RANGE", item.name)
            elif not isinstance(value, str) or not _TOKEN_CHARSET.fullmatch(value):
                raise CursorInvalid("FIELD_CHARSET", item.name)
        if not _HEX64.fullmatch(self.requirement_digest):
            raise CursorInvalid("FIELD_CHARSET", "requirement_digest")
        if self.expires_at_ns <= self.issued_at_ns:
            raise CursorInvalid("FIELD_RANGE", "expires_at_ns")

    def body(self) -> bytes:
        return canonical_body({"schema": SCHEMA_V3, **{f.name: getattr(self, f.name) for f in fields(self)}})


@dataclass(frozen=True)
class CursorV3Expectation:
    """What the verifying replica is configured to accept."""

    environment: str
    stream: str
    source_topic_id: str
    partition_plan_epoch: int
    source_policy_revision: int
    catalog_revision: int
    route_generation: str
    schema_major: int = 2


def canonical_body(value: Mapping[str, Any]) -> bytes:
    """Encode the restricted canonical JSON object (see module docstring)."""

    parts = []
    for key in sorted(value, key=lambda item: item.encode()):
        item = value[key]
        if isinstance(item, bool) or not isinstance(item, (int, str)):
            raise CursorInvalid("FIELD_TYPE", key)
        if isinstance(item, int):
            if not 0 <= item <= MAX_OFFSET:
                raise CursorInvalid("FIELD_RANGE", key)
            encoded = str(item)
        else:
            if not _TOKEN_CHARSET.fullmatch(item):
                raise CursorInvalid("FIELD_CHARSET", key)
            encoded = f'"{item}"'
        parts.append(f'"{key}":{encoded}')
    return ("{" + ",".join(parts) + "}").encode("ascii")


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value) or len(value) % 4 == 1:
        raise CursorInvalid("ENCODING")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except ValueError as error:
        raise CursorInvalid("ENCODING") from error
    if _b64(decoded) != value:
        # Non-canonical trailing bits would make one token spellable many ways.
        raise CursorInvalid("ENCODING")
    return decoded


def _reject_constant(name: str) -> Any:
    # NaN/Infinity are not JSON; Python accepts them by default, Rust does not.
    raise ValueError(name)


def requirement_digest(requirement: Any) -> str:
    """SHA-256 of the normalized *delivery* requirement.

    Covers every field that changes what a stream delivers or when it blocks;
    the historical horizon (``warmup_limit``/``warmup``) is excluded because a
    stream resumed after a different warmup size is the same subscription.
    Accepts ``qdl.query.contracts.DataRequirement`` (Query) and the same
    semantics parsed from the proto by the native Stream.
    """

    def text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if hasattr(value, "value"):
            return str(value.value)
        return str(value)

    lines = [
        REQUIREMENT_DIGEST_SCHEMA,
        f"instrument_uid={text(requirement.instrument_uid)}",
        f"feed={text(requirement.feed)}",
        f"interval={text(requirement.interval)}",
        f"consumer_grade={text(requirement.consumer_grade)}",
        f"source_policy_id={text(requirement.source_policy_id)}",
        f"max_freshness_ms={text(requirement.max_freshness_ms)}",
        f"event_recency_policy={text(requirement.event_recency_policy)}",
        f"max_session_liveness_ms={text(requirement.max_session_liveness_ms)}",
        f"require_full_coverage={text(requirement.require_full_coverage)}",
        f"require_final_bars={text(requirement.require_final_bars)}",
        f"stale_policy={text(requirement.stale_policy)}",
        f"gap_policy={text(requirement.gap_policy)}",
        f"recovery={text(requirement.recovery)}",
        f"bar_revision_policy={text(requirement.bar_revision_policy)}",
    ]
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


class SignedCursorV3Codec:
    """HMAC-SHA256 cursor v3 with key rotation and typed verification."""

    def __init__(self, keys: Mapping[str, bytes], *, active_key_id: str) -> None:
        if active_key_id not in keys:
            raise ValueError("active cursor-signing key is unavailable")
        if any(len(secret) < 32 for secret in keys.values()):
            raise ValueError("cursor-signing secrets must contain at least 256 bits")
        for key_id in keys:
            if not _TOKEN_CHARSET.fullmatch(key_id):
                raise ValueError("cursor key id has characters outside the token charset")
        self._keys = dict(keys)
        self.active_key_id = active_key_id

    def encode(self, claims: CursorV3Claims) -> str:
        if claims.key_id != self.active_key_id:
            raise ValueError("new cursors must use the active signing key")
        body = claims.body()
        signature = hmac.new(self._keys[claims.key_id], body, hashlib.sha256).digest()
        return f"{_b64(body)}.{_b64(signature)}"

    def verify(
        self,
        token: str,
        *,
        consumer_id: str,
        environment: str,
        requirement_digest_value: str,
        expected: CursorV3Expectation,
        now_ns: int,
    ) -> CursorV3Claims:
        if not isinstance(token, str) or token.count(".") != 1 or len(token) > 4096:
            raise CursorInvalid("ENCODING")
        encoded_body, encoded_signature = token.split(".")
        body = _unb64(encoded_body)
        signature = _unb64(encoded_signature)
        try:
            raw = json.loads(body.decode("utf-8"), parse_constant=_reject_constant)
        except (UnicodeDecodeError, ValueError) as error:
            raise CursorInvalid("ENCODING") from error
        if not isinstance(raw, dict):
            raise CursorInvalid("ENCODING")
        schema = raw.get("schema")
        # An array or object schema is unhashable: test the type before the
        # set lookup so it is refused as SCHEMA, never raised as TypeError.
        if isinstance(schema, str) and schema in LEGACY_SCHEMAS:
            # An earlier spool-offset cursor can never be interpreted as a
            # Kafka coordinate. Its only safe outcome is a fresh snapshot; a
            # forged legacy body gains nothing beyond that authenticated reset.
            raise CursorV3Expired("LEGACY_SCHEMA", str(schema))
        if schema != SCHEMA_V3:
            raise CursorInvalid("SCHEMA")
        key = self._keys.get(raw.get("key_id")) if isinstance(raw.get("key_id"), str) else None
        if key is None:
            raise CursorInvalid("UNKNOWN_KEY")
        if not hmac.compare_digest(signature, hmac.new(key, body, hashlib.sha256).digest()):
            raise CursorInvalid("SIGNATURE")
        names = {item.name for item in fields(CursorV3Claims)}
        if set(raw) != names | {"schema"}:
            raise CursorInvalid("FIELDS")
        claims = CursorV3Claims(**{name: raw[name] for name in names})
        if claims.body() != body:
            raise CursorInvalid("NON_CANONICAL")
        if claims.consumer_id != consumer_id:
            raise CursorInvalid("CONSUMER")
        if claims.environment != environment or environment != expected.environment:
            raise CursorInvalid("ENVIRONMENT")
        if claims.requirement_digest != requirement_digest_value:
            raise CursorInvalid("REQUIREMENT")
        for reason, actual, wanted in (
            ("SCHEMA_MAJOR", claims.schema_major, expected.schema_major),
            ("STREAM", claims.stream, expected.stream),
            ("TOPIC_GENERATION", claims.source_topic_id, expected.source_topic_id),
            ("PARTITION_PLAN", claims.partition_plan_epoch, expected.partition_plan_epoch),
            ("SOURCE_POLICY", claims.source_policy_revision, expected.source_policy_revision),
            ("CATALOG", claims.catalog_revision, expected.catalog_revision),
            ("ROUTE_GENERATION", claims.route_generation, expected.route_generation),
        ):
            if actual != wanted:
                raise CursorV3Expired(reason)
        if now_ns >= claims.expires_at_ns:
            raise CursorV3Expired("EXPIRED")
        return claims

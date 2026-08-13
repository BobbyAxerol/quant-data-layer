from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Mapping, Protocol, runtime_checkable

from qdl.transport.contracts import Cursor, CursorExpired, StoredEvent


class ReplayGapError(RuntimeError):
    """Durable offsets are not contiguous at the historical/live boundary."""


class SnapshotWatermarkMismatch(RuntimeError):
    """Historical snapshot and durable live watermark do not form one boundary."""


@runtime_checkable
class HandoffStore(Protocol):
    def high_watermark(self, stream: str, partition_key: str) -> int: ...

    def read(
        self,
        *,
        stream: str,
        partition_key: str,
        after: Cursor | None = None,
        limit: int = 100,
    ) -> list[StoredEvent]: ...

    def checkpoint(self, *, consumer_id: str, cursor: Cursor, ttl_seconds: int) -> None: ...

    def get_checkpoint(
        self, *, consumer_id: str, stream: str, partition_key: str
    ) -> Cursor | None: ...


@dataclass(frozen=True)
class SigningKeySet:
    active_key_id: str
    verification_keys: Mapping[str, bytes]

    def __post_init__(self) -> None:
        if self.active_key_id not in self.verification_keys:
            raise ValueError("active cursor-signing key is unavailable")
        if any(len(secret) < 32 for secret in self.verification_keys.values()):
            raise ValueError("cursor-signing secrets must contain at least 256 bits")


@runtime_checkable
class SigningKeyProvider(Protocol):
    def load(self) -> SigningKeySet: ...


class StaticSigningKeyProvider:
    """Test/local provider; production injects a secret-manager-backed provider."""

    def __init__(self, keys: SigningKeySet):
        self._keys = keys

    def load(self) -> SigningKeySet:
        return self._keys


@dataclass(frozen=True)
class HandoffGrant:
    consumer_id: str
    snapshot_id: str
    stream: str
    partition_key: str
    watermark_offset: int
    issued_at_ns: int
    expires_at_ns: int
    token: str


@dataclass(frozen=True)
class _TokenPayload:
    consumer_id: str
    snapshot_id: str
    stream: str
    partition_key: str
    watermark_offset: int
    issued_at_ns: int
    expires_at_ns: int
    key_id: str


class SignedHandoffCursorCodec:
    """HMAC cursor envelope with key rotation and strict scope validation."""

    def __init__(
        self,
        secrets: Mapping[str, bytes] | SigningKeyProvider,
        *,
        active_key_id: str | None = None,
        clock_ns=time.time_ns,
    ):
        if isinstance(secrets, SigningKeyProvider):
            if active_key_id is not None:
                raise ValueError("active_key_id belongs to the injected key provider")
            self._key_provider = secrets
        else:
            if active_key_id is None:
                raise ValueError("active_key_id is required for a static key mapping")
            self._key_provider = StaticSigningKeyProvider(
                SigningKeySet(active_key_id, dict(secrets))
            )
        self._key_provider.load()
        self._clock_ns = clock_ns

    def encode(self, payload: _TokenPayload) -> str:
        key_set = self._key_provider.load()
        if payload.key_id != key_set.active_key_id:
            raise ValueError("new handoff cursor must use the active signing key")
        body = json.dumps(
            {"schema": "qdl.handoff-cursor.v1", **payload.__dict__},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        signature = hmac.new(
            key_set.verification_keys[payload.key_id], body, hashlib.sha256
        ).digest()
        return f"{self._b64(body)}.{self._b64(signature)}"

    def decode(
        self,
        token: str,
        *,
        consumer_id: str,
        stream: str,
        partition_key: str,
    ) -> _TokenPayload:
        try:
            encoded_body, encoded_signature = token.split(".", 1)
            body = self._unb64(encoded_body)
            supplied_signature = self._unb64(encoded_signature)
            raw = json.loads(body)
            key_id = str(raw["key_id"])
            secret = self._key_provider.load().verification_keys[key_id]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid signed handoff cursor") from error
        expected_signature = hmac.new(secret, body, hashlib.sha256).digest()
        if not hmac.compare_digest(supplied_signature, expected_signature):
            raise ValueError("handoff cursor signature mismatch")
        if raw.get("schema") != "qdl.handoff-cursor.v1":
            raise ValueError("unsupported handoff cursor schema")
        payload = _TokenPayload(**{
            key: raw[key]
            for key in _TokenPayload.__dataclass_fields__
        })
        if (payload.consumer_id, payload.stream, payload.partition_key) != (
            consumer_id, stream, partition_key
        ):
            raise ValueError("handoff cursor scope mismatch")
        if self._clock_ns() >= payload.expires_at_ns:
            raise CursorExpired("signed handoff cursor expired")
        return payload

    @property
    def active_key_id(self) -> str:
        return self._key_provider.load().active_key_id

    @staticmethod
    def _b64(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")

    @staticmethod
    def _unb64(value: str) -> bytes:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class GapFreeHandoff:
    """Joins a historical snapshot watermark to durable live replay."""

    def __init__(
        self,
        spool: HandoffStore,
        codec: SignedHandoffCursorCodec,
        *,
        checkpoint_ttl_seconds: int = 3600,
        clock_ns=time.time_ns,
    ) -> None:
        if checkpoint_ttl_seconds <= 0:
            raise ValueError("checkpoint_ttl_seconds must be positive")
        self._spool = spool
        self._codec = codec
        self._checkpoint_ttl_seconds = checkpoint_ttl_seconds
        self._clock_ns = clock_ns

    def capture_watermark(self, *, stream: str, partition_key: str) -> Cursor:
        return Cursor(stream, partition_key, self._spool.high_watermark(stream, partition_key))

    def issue(
        self,
        *,
        consumer_id: str,
        snapshot_id: str,
        snapshot_watermark: Cursor,
        ttl_seconds: int,
    ) -> HandoffGrant:
        if not consumer_id.strip() or not snapshot_id.strip() or ttl_seconds <= 0:
            raise ValueError("consumer, snapshot and positive TTL are required")
        high = self._spool.high_watermark(
            snapshot_watermark.stream, snapshot_watermark.partition_key
        )
        if snapshot_watermark.offset > high:
            raise ValueError("snapshot watermark is ahead of durable live state")
        now_ns = self._clock_ns()
        payload = _TokenPayload(
            consumer_id=consumer_id,
            snapshot_id=snapshot_id,
            stream=snapshot_watermark.stream,
            partition_key=snapshot_watermark.partition_key,
            watermark_offset=snapshot_watermark.offset,
            issued_at_ns=now_ns,
            expires_at_ns=now_ns + ttl_seconds * 1_000_000_000,
            key_id=self._codec.active_key_id,
        )
        return HandoffGrant(**{
            key: getattr(payload, key)
            for key in HandoffGrant.__dataclass_fields__ if key != "token"
        }, token=self._codec.encode(payload))

    def replay(
        self,
        *,
        token: str,
        consumer_id: str,
        stream: str,
        partition_key: str,
        limit: int = 1000,
    ) -> list[StoredEvent]:
        if limit <= 0 or limit > 10_000:
            raise ValueError("replay limit must be between 1 and 10000")
        payload = self._codec.decode(
            token,
            consumer_id=consumer_id,
            stream=stream,
            partition_key=partition_key,
        )
        checkpoint = self._spool.get_checkpoint(
            consumer_id=consumer_id, stream=stream, partition_key=partition_key
        )
        start_offset = max(
            payload.watermark_offset,
            checkpoint.offset if checkpoint is not None else 0,
        )
        records = self._spool.read(
            stream=stream,
            partition_key=partition_key,
            after=Cursor(stream, partition_key, start_offset),
            limit=limit,
        )
        expected = start_offset + 1
        for record in records:
            if record.cursor.offset != expected:
                raise ReplayGapError(
                    f"expected durable offset {expected}, observed {record.cursor.offset}"
                )
            expected += 1
        return records

    def acknowledge(self, *, consumer_id: str, cursor: Cursor) -> None:
        self._spool.checkpoint(
            consumer_id=consumer_id,
            cursor=cursor,
            ttl_seconds=self._checkpoint_ttl_seconds,
        )

    def advance_token(
        self,
        *,
        token: str,
        consumer_id: str,
        cursor: Cursor,
        ttl_seconds: int,
    ) -> HandoffGrant:
        """Issue a resumable signed cursor after one event is safely processed."""

        payload = self._codec.decode(
            token,
            consumer_id=consumer_id,
            stream=cursor.stream,
            partition_key=cursor.partition_key,
        )
        if cursor.offset < payload.watermark_offset:
            raise ValueError("resume cursor cannot precede the snapshot watermark")
        return self.issue(
            consumer_id=consumer_id,
            snapshot_id=payload.snapshot_id,
            snapshot_watermark=cursor,
            ttl_seconds=ttl_seconds,
        )


@runtime_checkable
class HistoricalSnapshotView(Protocol):
    snapshot_id: str
    data_key: str
    source_cursor_start: str
    source_cursor_end: str


@runtime_checkable
class HistoricalSnapshotCatalog(Protocol):
    def current(self, dataset_id: str) -> HistoricalSnapshotView | None: ...


@dataclass(frozen=True)
class SnapshotHandoffBundle:
    dataset_id: str
    snapshot_id: str
    data_key: str
    snapshot_watermark: Cursor
    grant: HandoffGrant


class SnapshotHandoffCoordinator:
    """Validate one immutable history/live boundary before issuing a grant."""

    def __init__(self, catalog: HistoricalSnapshotCatalog, handoff: GapFreeHandoff):
        self._catalog = catalog
        self._handoff = handoff

    def prepare(
        self,
        *,
        dataset_id: str,
        consumer_id: str,
        stream: str,
        partition_key: str,
        ttl_seconds: int,
    ) -> SnapshotHandoffBundle:
        if not dataset_id.strip():
            raise ValueError("dataset_id is required")
        watermark = self._handoff.capture_watermark(
            stream=stream, partition_key=partition_key
        )
        snapshot = self._catalog.current(dataset_id)
        if snapshot is None:
            raise SnapshotWatermarkMismatch("historical snapshot is unavailable")
        try:
            snapshot_start = int(snapshot.source_cursor_start)
            snapshot_end = int(snapshot.source_cursor_end)
        except (TypeError, ValueError) as error:
            raise SnapshotWatermarkMismatch(
                "historical snapshot has a non-logical source cursor"
            ) from error
        if snapshot_start < 0 or snapshot_end < snapshot_start:
            raise SnapshotWatermarkMismatch("historical snapshot cursor range is invalid")
        if snapshot_end != watermark.offset:
            raise SnapshotWatermarkMismatch(
                "historical snapshot cursor end does not match durable live watermark"
            )
        grant = self._handoff.issue(
            consumer_id=consumer_id,
            snapshot_id=snapshot.snapshot_id,
            snapshot_watermark=watermark,
            ttl_seconds=ttl_seconds,
        )
        return SnapshotHandoffBundle(
            dataset_id=dataset_id,
            snapshot_id=snapshot.snapshot_id,
            data_key=snapshot.data_key,
            snapshot_watermark=watermark,
            grant=grant,
        )

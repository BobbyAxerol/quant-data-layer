from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, runtime_checkable


class StreamName(str, Enum):
    RAW = "md.raw.v1"
    CANONICAL = "md.canonical.v2"
    QUALITY = "md.quality.v1"
    QUARANTINE = "md.quarantine.v1"


class RetryClass(str, Enum):
    RETRYABLE = "RETRYABLE"
    NON_RETRYABLE = "NON_RETRYABLE"
    CAPACITY = "CAPACITY"


@dataclass(frozen=True)
class RetryDecision:
    classification: RetryClass
    reason: str
    retry_after_seconds: float = 0.0


class DurableTransportError(RuntimeError):
    """Base class for errors at the durable acceptance boundary."""


class BackpressureRequired(DurableTransportError):
    """The bounded bridge cannot accept another event without losing data."""


class EventIdCollision(DurableTransportError):
    """An event ID was reused for different immutable bytes."""


class CursorExpired(DurableTransportError):
    """A requested cursor predates the bridge retention horizon."""


class CheckpointRegression(DurableTransportError):
    """A consumer attempted to move its durable checkpoint backwards."""


@dataclass(frozen=True, order=True)
class Cursor:
    """Portable logical cursor; it intentionally exposes no broker offset type."""

    stream: str
    partition_key: str
    offset: int

    def __post_init__(self) -> None:
        if not self.stream.strip():
            raise ValueError("cursor stream is required")
        if not self.partition_key.strip():
            raise ValueError("cursor partition_key is required")
        if self.offset < 0:
            raise ValueError("cursor offset must be non-negative")

    def to_token(self) -> str:
        payload = json.dumps(
            {
                "offset": self.offset,
                "partition_key": self.partition_key,
                "schema": "qdl.cursor.v1",
                "stream": self.stream,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")

    @classmethod
    def from_token(cls, token: str) -> "Cursor":
        try:
            padding = "=" * (-len(token) % 4)
            payload = json.loads(base64.urlsafe_b64decode(token + padding))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid cursor token") from exc
        if payload.get("schema") != "qdl.cursor.v1":
            raise ValueError("unsupported cursor schema")
        return cls(
            stream=str(payload["stream"]),
            partition_key=str(payload["partition_key"]),
            offset=int(payload["offset"]),
        )


@dataclass(frozen=True)
class DurableEvent:
    stream: str
    partition_key: str
    event_id: bytes
    payload: bytes
    accepted_at_ns: int
    content_type: str = "application/x-protobuf"
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.stream.strip():
            raise ValueError("event stream is required")
        if not self.partition_key.strip():
            raise ValueError("event partition_key is required")
        if len(self.event_id) not in {16, 32}:
            raise ValueError("event_id must be 16 or 32 bytes")
        if not isinstance(self.payload, bytes) or not self.payload:
            raise ValueError("event payload must be non-empty bytes")
        if self.accepted_at_ns <= 0:
            raise ValueError("accepted_at_ns must be positive")
        if not self.content_type.strip():
            raise ValueError("content_type is required")


@dataclass(frozen=True)
class StoredEvent:
    event: DurableEvent
    cursor: Cursor
    committed_at_ns: int
    payload_sha256: str


@dataclass(frozen=True)
class AppendResult:
    cursor: Cursor
    committed_at_ns: int
    duplicate: bool
    payload_sha256: str


@runtime_checkable
class EventSink(Protocol):
    def append(self, event: DurableEvent) -> AppendResult: ...


@runtime_checkable
class BatchEventSink(EventSink, Protocol):
    def append_many(self, events: list[DurableEvent]) -> list[AppendResult]: ...


@runtime_checkable
class EventSource(Protocol):
    def read(
        self,
        *,
        stream: str,
        partition_key: str,
        after: Cursor | None = None,
        limit: int = 100,
    ) -> list[StoredEvent]: ...

    def checkpoint(
        self,
        *,
        consumer_id: str,
        cursor: Cursor,
        ttl_seconds: int,
    ) -> None: ...


def partition_key(*, instrument_uid: str, feed_type: str, source_id: str) -> str:
    """Keep one instrument/feed/source ordered without leaking broker partitions."""

    values = (instrument_uid.strip(), feed_type.strip().lower(), source_id.strip())
    if not all(values):
        raise ValueError("instrument_uid, feed_type and source_id are required")
    return "/".join(values)


def classify_transport_error(error: BaseException) -> RetryDecision:
    if isinstance(error, BackpressureRequired):
        return RetryDecision(RetryClass.CAPACITY, "bridge_capacity_exhausted")
    if isinstance(error, (EventIdCollision, ValueError, TypeError)):
        return RetryDecision(RetryClass.NON_RETRYABLE, "invalid_or_conflicting_event")
    return RetryDecision(RetryClass.RETRYABLE, "transient_transport_failure", 0.05)

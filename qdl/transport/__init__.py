"""Transport-neutral durability primitives for the QDL V2 shadow path."""

from qdl.transport.contracts import (
    AppendResult,
    BackpressureRequired,
    BatchEventSink,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    EventIdCollision,
    EventSink,
    EventSource,
    RetryClass,
    RetryDecision,
    PayloadCorruption,
    StoredEvent,
    StreamName,
)
from qdl.transport.publisher import DurablePublisher, PublisherState
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig, SpoolStats

__all__ = [
    "AppendResult",
    "BackpressureRequired",
    "BatchEventSink",
    "CheckpointRegression",
    "Cursor",
    "CursorExpired",
    "DurableEvent",
    "DurablePublisher",
    "EventIdCollision",
    "EventSink",
    "EventSource",
    "PublisherState",
    "PayloadCorruption",
    "RetryClass",
    "RetryDecision",
    "SQLiteDurableSpool",
    "SpoolConfig",
    "SpoolStats",
    "StoredEvent",
    "StreamName",
]

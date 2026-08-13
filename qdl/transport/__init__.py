"""Transport-neutral durability primitives for the QDL V2 shadow path."""

from qdl.transport.contracts import (
    AppendResult,
    BackpressureRequired,
    CheckpointRegression,
    Cursor,
    CursorExpired,
    DurableEvent,
    EventIdCollision,
    EventSink,
    EventSource,
    RetryClass,
    RetryDecision,
    StreamName,
)
from qdl.transport.publisher import DurablePublisher, PublisherState
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig, SpoolStats

__all__ = [
    "AppendResult",
    "BackpressureRequired",
    "CheckpointRegression",
    "Cursor",
    "CursorExpired",
    "DurableEvent",
    "DurablePublisher",
    "EventIdCollision",
    "EventSink",
    "EventSource",
    "PublisherState",
    "RetryClass",
    "RetryDecision",
    "SQLiteDurableSpool",
    "SpoolConfig",
    "SpoolStats",
    "StreamName",
]

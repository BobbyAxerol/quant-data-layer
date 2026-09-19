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
    FINAL_BAR_CLOSE_TIME_NS_HEADER,
    RetryClass,
    RetryDecision,
    PayloadCorruption,
    StoredEvent,
    StreamName,
)
from qdl.transport.publisher import DurablePublisher, PublisherState
from qdl.transport.sqlite_spool import (
    SQLiteDurableSpool,
    SpoolConfig,
    SpoolReadiness,
    SpoolStats,
)

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
    "FINAL_BAR_CLOSE_TIME_NS_HEADER",
    "PublisherState",
    "PayloadCorruption",
    "RetryClass",
    "RetryDecision",
    "SQLiteDurableSpool",
    "SpoolConfig",
    "SpoolReadiness",
    "SpoolStats",
    "StoredEvent",
    "StreamName",
]

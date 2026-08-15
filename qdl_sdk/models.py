from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from qdl.api_v2.models import MarketDataView, SnapshotResponse, WarmupResponse
from qdl.query.v2 import query_pb2


class Feed(StrEnum):
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    BAR = "BAR"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    MARK_INDEX_PRICE = "MARK_INDEX_PRICE"
    TICKER = "TICKER"


class Grade(StrEnum):
    EXECUTION = "EXECUTION"
    ALPHA = "ALPHA"
    RESEARCH = "RESEARCH"


class StalePolicy(StrEnum):
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class GapPolicy(StrEnum):
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class RecoveryPolicy(StrEnum):
    SNAPSHOT_AND_REPLAY = "SNAPSHOT_AND_REPLAY"
    FRESH_SNAPSHOT = "FRESH_SNAPSHOT"
    NONE = "NONE"


class BarRevisionPolicy(StrEnum):
    LATEST = "LATEST"
    INITIAL_ONLY = "INITIAL_ONLY"
    EMIT_REVISIONS = "EMIT_REVISIONS"


@dataclass(frozen=True)
class DataRequirement:
    instrument_uid: str
    feed: Feed
    consumer_grade: Grade
    source_policy_id: str
    interval: str | None = None
    warmup_limit: int = 0
    max_freshness_ms: int | None = None
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: StalePolicy = StalePolicy.BLOCK
    gap_policy: GapPolicy = GapPolicy.BLOCK
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip() or not self.source_policy_id.strip():
            raise ValueError("instrument_uid and source_policy_id are required")
        enum_fields = (
            (self.feed, Feed, "feed"),
            (self.consumer_grade, Grade, "consumer_grade"),
            (self.stale_policy, StalePolicy, "stale_policy"),
            (self.gap_policy, GapPolicy, "gap_policy"),
            (self.recovery, RecoveryPolicy, "recovery"),
            (self.bar_revision_policy, BarRevisionPolicy, "bar_revision_policy"),
        )
        for value, enum_type, field in enum_fields:
            if not isinstance(value, enum_type):
                raise TypeError(f"{field} must use the typed SDK enum")
        if not 0 <= self.warmup_limit <= 10_000:
            raise ValueError("warmup_limit must be between 0 and 10000")
        if self.max_freshness_ms is not None and self.max_freshness_ms <= 0:
            raise ValueError("max_freshness_ms must be positive")
        if self.feed is Feed.BAR and not self.interval:
            raise ValueError("bar requirement needs interval")
        if self.feed is not Feed.BAR and self.interval is not None:
            raise ValueError("interval is valid only for bar requirements")
        if self.consumer_grade is Grade.EXECUTION and (
            self.stale_policy is not StalePolicy.BLOCK
            or self.gap_policy is not GapPolicy.BLOCK
            or not self.require_full_coverage
        ):
            raise ValueError("execution-grade requirement cannot relax fail-closed policy")

    def query_params(self) -> dict[str, str | int | bool]:
        values: dict[str, str | int | bool | None] = {
            "feed": self.feed.value,
            "consumer_grade": self.consumer_grade.value,
            "source_policy_id": self.source_policy_id,
            "interval": self.interval,
            "limit": self.warmup_limit or None,
            "max_freshness_ms": self.max_freshness_ms,
            "require_full_coverage": self.require_full_coverage,
            "require_final_bars": self.require_final_bars,
            "stale_policy": self.stale_policy.value,
            "gap_policy": self.gap_policy.value,
            "recovery": self.recovery.value,
            "bar_revision_policy": self.bar_revision_policy.value,
        }
        return {key: value for key, value in values.items() if value is not None}

    def to_proto(self) -> query_pb2.DataRequirement:
        return query_pb2.DataRequirement(
            instrument_uid=self.instrument_uid,
            interval=self.interval or "",
            source_policy_id=self.source_policy_id,
            warmup_limit=self.warmup_limit,
            max_freshness_ms=self.max_freshness_ms or 0,
            require_full_coverage=self.require_full_coverage,
            require_final_bars=self.require_final_bars,
            feed_type=getattr(query_pb2, f"FEED_TYPE_{self.feed.value}"),
            grade=getattr(query_pb2, f"CONSUMER_GRADE_{self.consumer_grade.value}"),
            stale_policy_type=getattr(
                query_pb2, f"STALE_POLICY_{self.stale_policy.value}"
            ),
            gap_policy_type=getattr(query_pb2, f"GAP_POLICY_{self.gap_policy.value}"),
            recovery_policy=getattr(
                query_pb2, f"RECOVERY_POLICY_{self.recovery.value}"
            ),
            revision_policy=getattr(
                query_pb2, f"BAR_REVISION_POLICY_{self.bar_revision_policy.value}"
            ),
        )


@dataclass(frozen=True)
class StreamEvent:
    logical_offset: int
    resume_token: str
    event: Any

    def __post_init__(self) -> None:
        if self.logical_offset <= 0 or not self.resume_token:
            raise ValueError("stream event requires positive offset and signed resume token")


@dataclass(frozen=True)
class ControlEvent:
    code: str
    detail: str
    snapshot: WarmupResponse | None = None

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.detail.strip():
            raise ValueError("control event code/detail are required")

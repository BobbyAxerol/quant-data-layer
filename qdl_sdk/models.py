from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from qdl.query.v2 import query_pb2


_GRADES = frozenset({"EXECUTION", "ALPHA", "RESEARCH"})
_FEEDS = frozenset({
    "TRADE", "QUOTE", "BAR", "BOOK_SNAPSHOT", "BOOK_DELTA",
    "FUNDING_RATE", "OPEN_INTEREST", "MARK_INDEX_PRICE", "TICKER",
})


@dataclass(frozen=True)
class DataRequirement:
    instrument_uid: str
    feed: str
    consumer_grade: str
    source_policy_id: str
    interval: str | None = None
    warmup_limit: int = 0
    max_freshness_ms: int | None = None
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: str = "BLOCK"
    gap_policy: str = "BLOCK"
    recovery: str = "SNAPSHOT_AND_REPLAY"
    bar_revision_policy: str = "LATEST"

    def __post_init__(self) -> None:
        object.__setattr__(self, "feed", self.feed.upper())
        object.__setattr__(self, "consumer_grade", self.consumer_grade.upper())
        if not self.instrument_uid.strip() or not self.source_policy_id.strip():
            raise ValueError("instrument_uid and source_policy_id are required")
        if self.feed not in _FEEDS or self.consumer_grade not in _GRADES:
            raise ValueError("unsupported feed or consumer grade")
        if not 0 <= self.warmup_limit <= 10_000:
            raise ValueError("warmup_limit must be between 0 and 10000")
        if self.feed == "BAR" and not self.interval:
            raise ValueError("bar requirement needs interval")
        if self.feed != "BAR" and self.interval is not None:
            raise ValueError("interval is valid only for bar requirements")
        if self.consumer_grade == "EXECUTION" and (
            self.stale_policy != "BLOCK"
            or self.gap_policy != "BLOCK"
            or not self.require_full_coverage
        ):
            raise ValueError("execution-grade requirement cannot relax fail-closed policy")

    def query_params(self) -> dict[str, str | int | bool]:
        values: dict[str, str | int | bool | None] = {
            "feed": self.feed,
            "consumer_grade": self.consumer_grade,
            "source_policy_id": self.source_policy_id,
            "interval": self.interval,
            "limit": self.warmup_limit or None,
            "max_freshness_ms": self.max_freshness_ms,
        }
        return {key: value for key, value in values.items() if value is not None}

    def to_proto(self) -> query_pb2.DataRequirement:
        return query_pb2.DataRequirement(
            instrument_uid=self.instrument_uid,
            feed=self.feed,
            interval=self.interval or "",
            consumer_grade=self.consumer_grade,
            source_policy_id=self.source_policy_id,
            warmup_limit=self.warmup_limit,
            max_freshness_ms=self.max_freshness_ms or 0,
            require_full_coverage=self.require_full_coverage,
            require_final_bars=self.require_final_bars,
            stale_policy=self.stale_policy,
            gap_policy=self.gap_policy,
            recovery=self.recovery,
            bar_revision_policy=self.bar_revision_policy,
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
    snapshot: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.detail.strip():
            raise ValueError("control event code/detail are required")

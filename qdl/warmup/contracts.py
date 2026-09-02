from __future__ import annotations

from dataclasses import dataclass
from qdl._compat import StrEnum
from typing import Any, Mapping


class IntervalSourcePolicy(StrEnum):
    NATIVE_ONLY = "NATIVE_ONLY"
    NATIVE_OR_EXACT_RESAMPLE = "NATIVE_OR_EXACT_RESAMPLE"


@dataclass(frozen=True, slots=True)
class WarmupTimeRange:
    start_time_ns: int
    end_time_ns: int

    def __post_init__(self) -> None:
        if self.start_time_ns <= 0 or self.end_time_ns <= self.start_time_ns:
            raise ValueError("warmup time range must be positive and increasing")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "WarmupTimeRange":
        if set(value) != {"start_time_ns", "end_time_ns"}:
            raise ValueError("warmup time range fields are incomplete or unknown")
        return cls(int(value["start_time_ns"]), int(value["end_time_ns"]))

    def canonical_mapping(self) -> dict[str, int]:
        return {
            "start_time_ns": self.start_time_ns,
            "end_time_ns": self.end_time_ns,
        }


@dataclass(frozen=True, slots=True)
class WarmupSpecification:
    rows: int | None = None
    time_range: WarmupTimeRange | None = None
    interval_source_policy: IntervalSourcePolicy = (
        IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
    )
    max_cache_age_ms: int = 60_000
    deadline_ms: int = 20_000

    def __post_init__(self) -> None:
        if (self.rows is None) == (self.time_range is None):
            raise ValueError("warmup requires exactly one rows or time_range horizon")
        if self.rows is not None and not 1 <= self.rows <= 100_000:
            raise ValueError("warmup rows must be between 1 and 100000")
        if not isinstance(self.interval_source_policy, IntervalSourcePolicy):
            object.__setattr__(
                self,
                "interval_source_policy",
                IntervalSourcePolicy(str(self.interval_source_policy).upper()),
            )
        if not 0 <= self.max_cache_age_ms <= 86_400_000:
            raise ValueError("warmup max_cache_age_ms is outside bounds")
        if not 100 <= self.deadline_ms <= 120_000:
            raise ValueError("warmup deadline_ms must be between 100 and 120000")

    @classmethod
    def for_rows(
        cls,
        rows: int,
        *,
        interval_source_policy: IntervalSourcePolicy = (
            IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE
        ),
        max_cache_age_ms: int = 60_000,
        deadline_ms: int = 20_000,
    ) -> "WarmupSpecification":
        return cls(
            rows=rows,
            interval_source_policy=interval_source_policy,
            max_cache_age_ms=max_cache_age_ms,
            deadline_ms=deadline_ms,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "WarmupSpecification":
        allowed = {
            "rows",
            "time_range",
            "interval_source_policy",
            "max_cache_age_ms",
            "deadline_ms",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown warmup specification fields: {sorted(unknown)}")
        raw_range = value.get("time_range")
        return cls(
            rows=int(value["rows"]) if value.get("rows") is not None else None,
            time_range=(
                WarmupTimeRange.from_mapping(raw_range)
                if isinstance(raw_range, Mapping)
                else None
            ),
            interval_source_policy=IntervalSourcePolicy(
                str(
                    value.get(
                        "interval_source_policy",
                        IntervalSourcePolicy.NATIVE_OR_EXACT_RESAMPLE.value,
                    )
                ).upper()
            ),
            max_cache_age_ms=int(value.get("max_cache_age_ms", 60_000)),
            deadline_ms=int(value.get("deadline_ms", 20_000)),
        )

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "time_range": (
                self.time_range.canonical_mapping() if self.time_range else None
            ),
            "interval_source_policy": self.interval_source_policy.value,
            "max_cache_age_ms": self.max_cache_age_ms,
            "deadline_ms": self.deadline_ms,
        }

    def resolved_window(
        self,
        *,
        interval_ns: int,
        latest_closed_boundary_ns: int,
    ) -> tuple[int, int, int]:
        if interval_ns <= 0 or latest_closed_boundary_ns <= 0:
            raise ValueError("warmup interval and closed boundary must be positive")
        if self.rows is not None:
            end_ns = latest_closed_boundary_ns
            return end_ns - self.rows * interval_ns, end_ns, self.rows
        assert self.time_range is not None
        start_ns = self.time_range.start_time_ns
        end_ns = self.time_range.end_time_ns
        if end_ns > latest_closed_boundary_ns:
            raise ValueError("warmup time range includes an unfinished bar")
        duration = end_ns - start_ns
        if duration % interval_ns:
            raise ValueError("warmup time range is not aligned to the interval")
        rows = duration // interval_ns
        if not 1 <= rows <= 100_000:
            raise ValueError("warmup time range resolves outside row bounds")
        return start_ns, end_ns, rows

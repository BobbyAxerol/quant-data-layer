from __future__ import annotations

from dataclasses import dataclass

from qdl.domain.calendar import TradingCalendar
from qdl.quality.engine import FeedQualityState


@dataclass(frozen=True)
class CalendarAssessment:
    state: FeedQualityState
    reason: str
    outage: bool


def assess_bar_availability(
    calendar: TradingCalendar,
    *,
    expected_open_time_ns: int,
    observed_open_time_ns: int | None,
    sparse_allowed: bool,
) -> CalendarAssessment:
    if not calendar.is_open_ns(expected_open_time_ns):
        return CalendarAssessment(FeedQualityState.MARKET_CLOSED, "OUTSIDE_TRADING_SESSION", False)
    if observed_open_time_ns == expected_open_time_ns:
        return CalendarAssessment(FeedQualityState.LIVE, "EXPECTED_BAR_OBSERVED", False)
    if observed_open_time_ns is None and sparse_allowed:
        return CalendarAssessment(FeedQualityState.DEGRADED, "SPARSE_NO_EVENT", False)
    if observed_open_time_ns is not None and observed_open_time_ns < expected_open_time_ns:
        return CalendarAssessment(FeedQualityState.STALE, "LATE_OR_STALE_BAR", True)
    return CalendarAssessment(FeedQualityState.GAPPED, "EXPECTED_BAR_MISSING", True)

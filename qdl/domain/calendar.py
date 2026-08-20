from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True, order=True)
class MarketSession:
    opens_at: time
    closes_at: time
    name: str = "CONTINUOUS"

    def __post_init__(self) -> None:
        if self.closes_at <= self.opens_at:
            raise ValueError("market session close must follow open")


@dataclass(frozen=True)
class TradingCalendar:
    calendar_id: str
    timezone_name: str
    weekly_sessions: tuple[MarketSession, ...]
    trading_weekdays: frozenset[int] = frozenset({0, 1, 2, 3, 4})
    holidays: frozenset[date] = frozenset()
    special_sessions: dict[date, tuple[MarketSession, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.calendar_id.strip() or not self.weekly_sessions:
            raise ValueError("calendar identity and weekly sessions are required")
        ZoneInfo(self.timezone_name)
        if any(day < 0 or day > 6 for day in self.trading_weekdays):
            raise ValueError("trading weekdays must be in range 0..6")

    def sessions_for(self, trading_date: date) -> tuple[MarketSession, ...]:
        if trading_date in self.special_sessions:
            return self.special_sessions[trading_date]
        if trading_date in self.holidays or trading_date.weekday() not in self.trading_weekdays:
            return ()
        return self.weekly_sessions

    def session_at_ns(self, timestamp_ns: int) -> MarketSession | None:
        local = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc).astimezone(
            ZoneInfo(self.timezone_name)
        )
        local_time = local.timetz().replace(tzinfo=None)
        for session in self.sessions_for(local.date()):
            if session.opens_at <= local_time < session.closes_at:
                return session
        return None

    def is_open_ns(self, timestamp_ns: int) -> bool:
        return self.session_at_ns(timestamp_ns) is not None


def trading_calendar_for_id(calendar_id: str) -> TradingCalendar:
    """Resolve a governed session calendar from canonical instrument metadata."""
    if calendar_id.startswith("VN_"):
        return TradingCalendar(
            calendar_id=calendar_id,
            timezone_name="Asia/Ho_Chi_Minh",
            weekly_sessions=(
                MarketSession(time(9, 0), time(11, 30), "MORNING"),
                MarketSession(time(13, 0), time(14, 30), "AFTERNOON"),
                MarketSession(time(14, 45), time(14, 46), "ATC_CLOSE"),
            ),
        )
    raise ValueError(f"unknown governed trading calendar: {calendar_id}")

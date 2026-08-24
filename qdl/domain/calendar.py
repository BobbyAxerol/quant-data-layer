from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


_MAX_CALENDAR_SPAN_DAYS = 366 * 50


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

    def bar_opens_between_ns(
        self,
        *,
        start_ns: int,
        end_ns: int,
        interval_ns: int,
        max_rows: int | None = None,
    ) -> tuple[int, ...]:
        """Enumerate complete session bars in the half-open UTC range.

        Bar alignment is anchored at each governed session open, not at the
        Unix epoch. This is required for venues with lunch/ATC breaks and keeps
        a legitimate closed period from being reported as a data gap.
        """
        if start_ns <= 0 or end_ns <= start_ns or interval_ns <= 0:
            raise ValueError("calendar bar range and interval must be positive")
        if max_rows is not None and max_rows < 1:
            raise ValueError("calendar max_rows must be positive")
        zone = ZoneInfo(self.timezone_name)
        first_date = datetime.fromtimestamp(
            start_ns / 1_000_000_000, tz=timezone.utc
        ).astimezone(zone).date()
        last_date = datetime.fromtimestamp(
            (end_ns - 1) / 1_000_000_000, tz=timezone.utc
        ).astimezone(zone).date()
        if (last_date - first_date).days > _MAX_CALENDAR_SPAN_DAYS:
            raise ValueError("calendar bar range exceeds the bounded date span")
        current_date = first_date
        result: list[int] = []
        while current_date <= last_date:
            for session in self.sessions_for(current_date):
                session_open = self._local_time_ns(
                    current_date, session.opens_at, zone
                )
                session_close = self._local_time_ns(
                    current_date, session.closes_at, zone
                )
                cursor = session_open
                while cursor + interval_ns <= session_close:
                    if cursor >= start_ns and cursor + interval_ns <= end_ns:
                        result.append(cursor)
                        if max_rows is not None and len(result) > max_rows:
                            raise ValueError("calendar bar range exceeds the row bound")
                    cursor += interval_ns
            current_date += timedelta(days=1)
        return tuple(result)

    def previous_bar_opens_ns(
        self,
        *,
        end_ns: int,
        interval_ns: int,
        rows: int,
    ) -> tuple[int, ...]:
        """Return the newest complete governed session bars before `end_ns`."""
        if end_ns <= 0 or interval_ns <= 0 or rows < 1:
            raise ValueError("calendar history boundary, interval and rows are required")
        zone = ZoneInfo(self.timezone_name)
        current_date = datetime.fromtimestamp(
            (end_ns - 1) / 1_000_000_000, tz=timezone.utc
        ).astimezone(zone).date()
        result: list[int] = []
        # 100k one-minute bars need roughly one trading year. Five calendar
        # years is a finite guard against an invalid/empty governed calendar.
        for _ in range(366 * 5):
            day = self.bar_opens_between_ns(
                start_ns=self._local_time_ns(current_date, time.min, zone),
                end_ns=self._local_time_ns(
                    current_date + timedelta(days=1), time.min, zone
                ),
                interval_ns=interval_ns,
            )
            result.extend(value for value in reversed(day) if value + interval_ns <= end_ns)
            if len(result) >= rows:
                return tuple(sorted(result[:rows]))
            current_date -= timedelta(days=1)
        raise ValueError("calendar cannot resolve the requested historical row count")

    @staticmethod
    def _local_time_ns(day: date, value: time, zone: ZoneInfo) -> int:
        return int(datetime.combine(day, value, tzinfo=zone).timestamp() * 1_000_000_000)


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

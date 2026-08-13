from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SessionWindow:
    opens_at: time
    closes_at: time

    def __post_init__(self) -> None:
        if self.closes_at <= self.opens_at:
            raise ValueError("session window close must follow open")


@dataclass(frozen=True)
class BarRecord:
    instrument_uid: str
    instrument_id: str
    instrument_revision: int
    source_id: str
    source_role: str
    interval: str
    open_time_ns: int
    close_time_ns: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    trade_count: int
    is_final: bool
    revision: int
    origin: str
    normalizer_version: str
    config_revision: int
    raw_lineage_json: str
    timestamp_interpretation: str = "UTC"

    def __post_init__(self) -> None:
        if not all((self.instrument_uid, self.instrument_id, self.source_id, self.interval)):
            raise ValueError("bar identity is incomplete")
        if self.instrument_revision < 1 or self.config_revision < 1:
            raise ValueError("instrument/config revisions must be positive")
        if self.open_time_ns <= 0 or self.close_time_ns <= self.open_time_ns:
            raise ValueError("bar time range is invalid")
        if self.revision < 0 or self.trade_count < 0:
            raise ValueError("bar revision/trade_count cannot be negative")
        try:
            open_, high, low, close, volume = map(
                Decimal, (self.open, self.high, self.low, self.close, self.volume)
            )
        except InvalidOperation as error:
            raise ValueError("bar contains invalid decimal text") from error
        if high < max(open_, close) or low > min(open_, close) or high < low:
            raise ValueError("bar OHLC invariants failed")
        if volume < 0:
            raise ValueError("bar volume cannot be negative")

    @property
    def identity(self) -> tuple[str, str, str, int]:
        return self.instrument_uid, self.source_id, self.interval, self.open_time_ns

    def as_dict(self) -> dict:
        return asdict(self)


def select_revisions(records: list[BarRecord]) -> list[BarRecord]:
    selected: dict[tuple[str, str, str, int], BarRecord] = {}
    for record in records:
        current = selected.get(record.identity)
        if current is None or record.revision > current.revision:
            selected[record.identity] = record
        elif record.revision == current.revision and record != current:
            raise ValueError("same bar identity/revision has conflicting immutable content")
    return sorted(selected.values(), key=lambda item: (item.open_time_ns, item.identity))


def _session_bucket(
    instant: datetime,
    *,
    interval_minutes: int,
    sessions: tuple[SessionWindow, ...],
) -> tuple[datetime, datetime] | None:
    local_time = instant.timetz().replace(tzinfo=None)
    for session in sessions:
        if session.opens_at <= local_time < session.closes_at:
            session_start = datetime.combine(instant.date(), session.opens_at, tzinfo=instant.tzinfo)
            elapsed = int((instant - session_start).total_seconds() // 60)
            bucket = session_start + timedelta(minutes=(elapsed // interval_minutes) * interval_minutes)
            return bucket, min(
                bucket + timedelta(minutes=interval_minutes),
                datetime.combine(instant.date(), session.closes_at, tzinfo=instant.tzinfo),
            )
    return None


def aggregate_bars(
    records: list[BarRecord],
    *,
    output_interval: str,
    interval_minutes: int | None,
    timezone_name: str,
    sessions: tuple[SessionWindow, ...] = (),
) -> list[BarRecord]:
    if not records:
        return []
    zone = ZoneInfo(timezone_name)
    groups: dict[tuple, list[BarRecord]] = defaultdict(list)
    for record in select_revisions(records):
        instant = datetime.fromtimestamp(record.open_time_ns / 1_000_000_000, tz=timezone.utc).astimezone(zone)
        if output_interval == "1d":
            bucket_open = datetime.combine(instant.date(), time.min, tzinfo=zone)
            bucket_close = bucket_open + timedelta(days=1)
        else:
            if interval_minutes is None or interval_minutes <= 0 or not sessions:
                raise ValueError("intraday aggregation requires positive interval and sessions")
            bucket = _session_bucket(instant, interval_minutes=interval_minutes, sessions=sessions)
            if bucket is None:
                continue
            bucket_open, bucket_close = bucket
        groups[(record.instrument_uid, record.source_id, bucket_open)].append(record)

    result: list[BarRecord] = []
    for (_, _, bucket_open), rows in sorted(groups.items(), key=lambda item: item[0][2]):
        rows.sort(key=lambda item: item.open_time_ns)
        bucket_close = (
            bucket_open + timedelta(days=1)
            if output_interval == "1d"
            else _session_bucket(
                datetime.fromtimestamp(rows[0].open_time_ns / 1_000_000_000, tz=timezone.utc).astimezone(zone),
                interval_minutes=interval_minutes or 0,
                sessions=sessions,
            )[1]
        )
        result.append(replace(
            rows[0],
            interval=output_interval,
            open_time_ns=int(bucket_open.astimezone(timezone.utc).timestamp() * 1_000_000_000),
            close_time_ns=int(bucket_close.astimezone(timezone.utc).timestamp() * 1_000_000_000),
            open=rows[0].open,
            high=str(max(Decimal(row.high) for row in rows)),
            low=str(min(Decimal(row.low) for row in rows)),
            close=rows[-1].close,
            volume=str(sum((Decimal(row.volume) for row in rows), Decimal(0))),
            trade_count=sum(row.trade_count for row in rows),
            revision=max(row.revision for row in rows),
            origin="AGGREGATED",
            raw_lineage_json="[" + ",".join(row.raw_lineage_json for row in rows) + "]",
        ))
    return result

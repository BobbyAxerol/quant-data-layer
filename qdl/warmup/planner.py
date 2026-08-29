from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Iterable, Mapping

from qdl.adapters.intervals import (
    canonical_interval_ms,
    latest_closed_boundary_ms,
)
from qdl.demand import DemandFeed, ResolvedRequirement
from qdl.domain.calendar import trading_calendar_for_id
from qdl.warmup.contracts import IntervalSourcePolicy, WarmupSpecification


@dataclass(frozen=True, slots=True)
class ProviderChunkPolicy:
    max_rows: int
    max_concurrency: int
    requests_per_second: float

    def __post_init__(self) -> None:
        if self.max_rows < 1 or self.max_concurrency < 1 or self.requests_per_second <= 0:
            raise ValueError("provider chunk policy values must be positive")


@dataclass(frozen=True, slots=True)
class WarmupFetchChunk:
    chunk_id: str
    requirement_id: str
    provider: str
    venue: str
    market: str
    instrument_uid: str
    native_symbol: str
    source_interval: str
    start_time_ns: int
    end_time_ns: int
    rows: int


@dataclass(frozen=True, slots=True)
class WarmupPlanItem:
    requirement_id: str
    consumer_ids: tuple[str, ...]
    instrument_uid: str
    native_symbol: str
    provider: str
    venue: str
    market: str
    target_interval: str
    source_interval: str
    target_rows: int
    source_rows: int
    start_time_ns: int
    end_time_ns: int
    resample_factor: int
    chunks: tuple[WarmupFetchChunk, ...]


@dataclass(frozen=True, slots=True)
class WarmupPlan:
    demand_revision: int
    generated_at_ns: int
    items: tuple[WarmupPlanItem, ...]

    @property
    def source_request_count(self) -> int:
        return sum(len(item.chunks) for item in self.items)


class UniversalWarmupPlanner:
    """Compile resolved BAR demand into bounded provider fetch chunks."""

    DEFAULT_POLICIES = {
        "BINANCE": ProviderChunkPolicy(1000, 8, 8.0),
        "OKX": ProviderChunkPolicy(300, 4, 5.0),
        "VN_MARKETS": ProviderChunkPolicy(2000, 2, 2.0),
        "HNX": ProviderChunkPolicy(2000, 2, 2.0),
        "HOSE": ProviderChunkPolicy(2000, 2, 2.0),
    }

    def __init__(
        self,
        *,
        policies: Mapping[str, ProviderChunkPolicy] | None = None,
        session_calendar_ids: Mapping[str, str] | None = None,
    ) -> None:
        self.policies = {
            key.upper(): value for key, value in (policies or self.DEFAULT_POLICIES).items()
        }
        self.session_calendar_ids = dict(session_calendar_ids or {})

    def compile(
        self,
        requirements: Iterable[ResolvedRequirement],
        *,
        generated_at_ns: int,
        specifications: Mapping[str, WarmupSpecification] | None = None,
        demand_revision: int,
    ) -> WarmupPlan:
        if generated_at_ns <= 0 or demand_revision < 1:
            raise ValueError("warmup plan time and demand revision must be positive")
        overrides = specifications or {}
        items = []
        for resolved in sorted(requirements, key=lambda value: value.requirement_id):
            requirement = resolved.requirement
            if requirement.feed is not DemandFeed.BAR:
                continue
            if not resolved.capability.enabled or not resolved.capability.rest_history:
                raise ValueError(
                    f"BAR history capability unavailable: {resolved.requirement_id}"
                )
            spec = overrides.get(resolved.requirement_id) or requirement.warmup
            if spec is None:
                if requirement.warmup_limit < 1:
                    continue
                spec = WarmupSpecification.for_rows(requirement.warmup_limit)
            target_interval = requirement.interval or ""
            target_ms = canonical_interval_ms(target_interval)
            boundary_ns = latest_closed_boundary_ms(
                target_interval,
                generated_at_ns // 1_000_000,
                provider=resolved.capability.venue,
            ) * 1_000_000
            target_interval_ns = target_ms * 1_000_000
            calendar_id = self.session_calendar_ids.get(resolved.instrument_uid)
            target_opens: tuple[int, ...] | None = None
            if calendar_id and calendar_id != "CRYPTO_24X7":
                calendar = trading_calendar_for_id(calendar_id)
                if spec.rows is not None:
                    target_opens = calendar.previous_bar_opens_ns(
                        end_ns=generated_at_ns,
                        interval_ns=target_interval_ns,
                        rows=spec.rows,
                    )
                    start_ns = target_opens[0]
                    end_ns = target_opens[-1] + target_interval_ns
                else:
                    assert spec.time_range is not None
                    if spec.time_range.end_time_ns > boundary_ns:
                        raise ValueError("warmup time range includes an unfinished bar")
                    start_ns = spec.time_range.start_time_ns
                    end_ns = spec.time_range.end_time_ns
                    target_opens = calendar.bar_opens_between_ns(
                        start_ns=start_ns,
                        end_ns=end_ns,
                        interval_ns=target_interval_ns,
                    )
                    if not target_opens:
                        raise ValueError("warmup time range contains no governed session bars")
                target_rows = len(target_opens)
            else:
                start_ns, end_ns, target_rows = spec.resolved_window(
                    interval_ns=target_interval_ns,
                    latest_closed_boundary_ns=boundary_ns,
                )
            source_interval = self._source_interval(
                target_interval,
                resolved.capability.native_intervals,
                spec.interval_source_policy,
            )
            source_ms = canonical_interval_ms(source_interval)
            if target_ms % source_ms:
                raise ValueError("source interval does not divide target interval exactly")
            factor = target_ms // source_ms
            source_opens: tuple[int, ...] | None = None
            if target_opens is not None:
                source_opens = trading_calendar_for_id(calendar_id).bar_opens_between_ns(
                    start_ns=start_ns,
                    end_ns=end_ns,
                    interval_ns=source_ms * 1_000_000,
                )
                source_rows = len(source_opens)
                if source_rows != target_rows * factor:
                    raise ValueError(
                        "session calendar cannot form complete target-bar constituents"
                    )
            else:
                source_rows = target_rows * factor
            policy = self.policies.get(resolved.capability.venue)
            if policy is None:
                raise ValueError(
                    f"no provider chunk policy for {resolved.capability.venue}"
                )
            provider = resolved.capability.capability_id.split(":", 1)[0]
            chunks = self._chunks(
                resolved=resolved,
                provider=provider,
                source_interval=source_interval,
                source_interval_ns=source_ms * 1_000_000,
                start_ns=start_ns,
                end_ns=end_ns,
                rows=source_rows,
                max_rows=policy.max_rows,
                open_times=source_opens,
            )
            items.append(
                WarmupPlanItem(
                    requirement_id=resolved.requirement_id,
                    consumer_ids=resolved.consumer_ids,
                    instrument_uid=resolved.instrument_uid,
                    native_symbol=resolved.native_symbol,
                    provider=provider,
                    venue=resolved.capability.venue,
                    market=resolved.capability.market,
                    target_interval=target_interval,
                    source_interval=source_interval,
                    target_rows=target_rows,
                    source_rows=source_rows,
                    start_time_ns=start_ns,
                    end_time_ns=end_ns,
                    resample_factor=factor,
                    chunks=chunks,
                )
            )
        return WarmupPlan(demand_revision, generated_at_ns, tuple(items))

    @staticmethod
    def _source_interval(
        target: str,
        native_intervals: tuple[str, ...],
        policy: IntervalSourcePolicy,
    ) -> str:
        if target in native_intervals:
            return target
        if policy is IntervalSourcePolicy.NATIVE_ONLY:
            raise ValueError(f"{target} is not a provider-native interval")
        target_ms = canonical_interval_ms(target)
        candidates = sorted(
            (
                (canonical_interval_ms(value), value)
                for value in native_intervals
                if target_ms % canonical_interval_ms(value) == 0
            ),
            reverse=True,
        )
        if not candidates:
            raise ValueError(f"no native interval can exactly resample {target}")
        return candidates[0][1]

    @staticmethod
    def _chunks(
        *,
        resolved: ResolvedRequirement,
        provider: str,
        source_interval: str,
        source_interval_ns: int,
        start_ns: int,
        end_ns: int,
        rows: int,
        max_rows: int,
        open_times: tuple[int, ...] | None = None,
    ) -> tuple[WarmupFetchChunk, ...]:
        if open_times is not None:
            if len(open_times) != rows:
                raise ValueError("warmup session rows differ from planned rows")
            return UniversalWarmupPlanner._session_chunks(
                resolved=resolved,
                provider=provider,
                source_interval=source_interval,
                source_interval_ns=source_interval_ns,
                open_times=open_times,
                max_rows=max_rows,
            )
        result = []
        cursor = start_ns
        remaining = rows
        while remaining:
            count = min(remaining, max_rows)
            chunk_end = cursor + count * source_interval_ns
            raw_id = "|".join(
                (
                    resolved.requirement_id,
                    source_interval,
                    str(cursor),
                    str(chunk_end),
                )
            )
            result.append(
                WarmupFetchChunk(
                    chunk_id=hashlib.sha256(raw_id.encode()).hexdigest(),
                    requirement_id=resolved.requirement_id,
                    provider=provider,
                    venue=resolved.capability.venue,
                    market=resolved.capability.market,
                    instrument_uid=resolved.instrument_uid,
                    native_symbol=resolved.native_symbol,
                    source_interval=source_interval,
                    start_time_ns=cursor,
                    end_time_ns=chunk_end,
                    rows=count,
                )
            )
            cursor = chunk_end
            remaining -= count
        if cursor != end_ns:
            raise ValueError("warmup chunks do not cover the exact requested window")
        return tuple(result)

    @staticmethod
    def _session_chunks(
        *,
        resolved: ResolvedRequirement,
        provider: str,
        source_interval: str,
        source_interval_ns: int,
        open_times: tuple[int, ...],
        max_rows: int,
    ) -> tuple[WarmupFetchChunk, ...]:
        groups: list[list[int]] = []
        for value in open_times:
            if not groups or value != groups[-1][-1] + source_interval_ns:
                groups.append([value])
            else:
                groups[-1].append(value)
        result = []
        for group in groups:
            for offset in range(0, len(group), max_rows):
                values = group[offset:offset + max_rows]
                chunk_end = values[-1] + source_interval_ns
                raw_id = "|".join((
                    resolved.requirement_id,
                    source_interval,
                    str(values[0]),
                    str(chunk_end),
                ))
                result.append(WarmupFetchChunk(
                    chunk_id=hashlib.sha256(raw_id.encode()).hexdigest(),
                    requirement_id=resolved.requirement_id,
                    provider=provider,
                    venue=resolved.capability.venue,
                    market=resolved.capability.market,
                    instrument_uid=resolved.instrument_uid,
                    native_symbol=resolved.native_symbol,
                    source_interval=source_interval,
                    start_time_ns=values[0],
                    end_time_ns=chunk_end,
                    rows=len(values),
                ))
        reconstructed = tuple(
            value
            for chunk in result
            for value in range(
                chunk.start_time_ns,
                chunk.end_time_ns,
                source_interval_ns,
            )
        )
        if reconstructed != open_times or sum(item.rows for item in result) != len(
            open_times
        ):
            raise ValueError("warmup session chunks do not exactly cover planned rows")
        return tuple(result)

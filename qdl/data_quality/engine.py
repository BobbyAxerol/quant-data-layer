from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum


class ValidationLevel(IntEnum):
    INVALID = 0
    TRANSPORT = 1
    SOURCE = 2
    CANONICAL = 3
    EXECUTION_ELIGIBLE = 4


class FeedQualityState(StrEnum):
    STARTING = "STARTING"
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    GAPPED = "GAPPED"
    RESYNCING = "RESYNCING"
    STALE = "STALE"
    OFFLINE = "OFFLINE"
    MARKET_CLOSED = "MARKET_CLOSED"


@dataclass(frozen=True, order=True)
class FeedKey:
    source_id: str
    instrument_uid: str
    feed: str

    def __post_init__(self) -> None:
        if not self.source_id.strip() or not self.instrument_uid.strip() or not self.feed.strip():
            raise ValueError("source_id, instrument_uid and feed are required")


@dataclass(frozen=True)
class Observation:
    key: FeedKey
    event_id: bytes
    received_at_ns: int
    source_time_ns: int | None
    source_sequence: int | None
    transport_valid: bool = True
    source_valid: bool = True
    canonical_valid: bool = True
    source_authoritative: bool = True
    freshness_limit_ns: int | None = None

    def __post_init__(self) -> None:
        if len(self.event_id) not in {16, 32}:
            raise ValueError("event_id must be 16 or 32 bytes")
        if self.received_at_ns <= 0:
            raise ValueError("received_at_ns must be positive")
        if self.source_time_ns is not None and self.source_time_ns <= 0:
            raise ValueError("source_time_ns must be positive when present")


@dataclass(frozen=True)
class GapRecord:
    expected_sequence: int
    observed_sequence: int
    detected_at_ns: int
    resolved_at_ns: int | None = None


@dataclass
class _FeedState:
    state: FeedQualityState = FeedQualityState.STARTING
    last_sequence: int | None = None
    last_source_time_ns: int | None = None
    last_received_at_ns: int | None = None
    last_event_id: bytes | None = None
    gap: GapRecord | None = None
    duplicate_count: int = 0
    out_of_order_count: int = 0
    clock_regression_count: int = 0
    recent_event_ids: deque[bytes] = field(default_factory=deque)
    recent_event_set: set[bytes] = field(default_factory=set)


@dataclass(frozen=True)
class ObservationResult:
    level: ValidationLevel
    state: FeedQualityState
    flags: tuple[str, ...]
    executable: bool
    duplicate: bool
    expected_next_sequence: int | None
    gap: GapRecord | None


class FeedQualityLedger:
    """Per-source/instrument/feed continuity state with bounded dedup memory."""

    def __init__(self, *, dedup_capacity: int = 4096, clock_regression_tolerance_ns: int = 0):
        if dedup_capacity <= 0 or clock_regression_tolerance_ns < 0:
            raise ValueError("invalid quality ledger bounds")
        self._capacity = dedup_capacity
        self._clock_tolerance = clock_regression_tolerance_ns
        self._states: dict[FeedKey, _FeedState] = {}

    def observe(self, item: Observation) -> ObservationResult:
        state = self._states.setdefault(item.key, _FeedState())
        flags: list[str] = []
        if not item.transport_valid:
            return self._result(state, ValidationLevel.INVALID, ("TRANSPORT_INVALID",), False)
        if not item.source_valid:
            return self._result(state, ValidationLevel.TRANSPORT, ("SOURCE_INVALID",), False)
        if not item.canonical_valid:
            return self._result(state, ValidationLevel.SOURCE, ("CANONICAL_INVALID",), False)

        if item.event_id in state.recent_event_set:
            state.duplicate_count += 1
            return self._result(
                state, ValidationLevel.CANONICAL, ("DUPLICATE",), False, duplicate=True
            )

        self._remember(state, item.event_id)
        if item.source_time_ns is None:
            flags.append("SOURCE_TIME_MISSING")
        elif (
            state.last_source_time_ns is not None
            and item.source_time_ns + self._clock_tolerance < state.last_source_time_ns
        ):
            state.clock_regression_count += 1
            flags.extend(("OUT_OF_ORDER", "CLOCK_SKEW_SUSPECTED"))
            state.out_of_order_count += 1

        if item.source_sequence is None:
            flags.append("SEQUENCE_MISSING")
        elif state.last_sequence is not None:
            expected = state.last_sequence + 1
            if item.source_sequence < expected:
                if "OUT_OF_ORDER" not in flags:
                    flags.append("OUT_OF_ORDER")
                    state.out_of_order_count += 1
            elif item.source_sequence > expected:
                state.gap = GapRecord(expected, item.source_sequence, item.received_at_ns)
                state.state = FeedQualityState.GAPPED
                flags.extend(("SEQUENCE_GAP_BEFORE", "RESYNC_REQUIRED"))

        if item.source_sequence is not None and (
            state.last_sequence is None or item.source_sequence > state.last_sequence
        ):
            state.last_sequence = item.source_sequence
        if item.source_time_ns is not None and (
            state.last_source_time_ns is None or item.source_time_ns > state.last_source_time_ns
        ):
            state.last_source_time_ns = item.source_time_ns
        state.last_received_at_ns = item.received_at_ns
        state.last_event_id = item.event_id

        if item.freshness_limit_ns is not None and item.source_time_ns is not None:
            if item.received_at_ns - item.source_time_ns > item.freshness_limit_ns:
                flags.append("STALE")
                state.state = FeedQualityState.STALE
        if not item.source_authoritative:
            flags.append("SOURCE_REFERENCE_ONLY")

        blocked = state.state in {
            FeedQualityState.GAPPED,
            FeedQualityState.RESYNCING,
            FeedQualityState.STALE,
            FeedQualityState.OFFLINE,
        }
        executable = not flags and not blocked and item.source_authoritative
        if executable:
            state.state = FeedQualityState.LIVE
        elif state.state is FeedQualityState.STARTING:
            state.state = FeedQualityState.DEGRADED
        return self._result(
            state,
            ValidationLevel.EXECUTION_ELIGIBLE if executable else ValidationLevel.CANONICAL,
            tuple(flags),
            executable,
        )

    def begin_resync(self, key: FeedKey) -> None:
        state = self._states.setdefault(key, _FeedState())
        if state.gap is None:
            raise ValueError("cannot resync a feed without an open gap")
        state.state = FeedQualityState.RESYNCING

    def complete_resync(
        self, key: FeedKey, *, snapshot_sequence: int, source_time_ns: int, completed_at_ns: int
    ) -> None:
        state = self._states.setdefault(key, _FeedState())
        if state.gap is None:
            raise ValueError("cannot complete resync without an open gap")
        if snapshot_sequence < state.gap.observed_sequence:
            raise ValueError("resync snapshot does not cover the observed gap")
        state.gap = GapRecord(
            state.gap.expected_sequence,
            state.gap.observed_sequence,
            state.gap.detected_at_ns,
            completed_at_ns,
        )
        state.last_sequence = snapshot_sequence
        state.last_source_time_ns = source_time_ns
        state.last_received_at_ns = completed_at_ns
        state.state = FeedQualityState.LIVE

    def mark_state(self, key: FeedKey, state: FeedQualityState) -> None:
        self._states.setdefault(key, _FeedState()).state = state

    def snapshot(self, key: FeedKey) -> dict[str, int | str | None]:
        state = self._states.setdefault(key, _FeedState())
        return {
            "state": state.state.value,
            "last_sequence": state.last_sequence,
            "last_source_time_ns": state.last_source_time_ns,
            "last_received_at_ns": state.last_received_at_ns,
            "duplicate_count": state.duplicate_count,
            "out_of_order_count": state.out_of_order_count,
            "clock_regression_count": state.clock_regression_count,
            "gap_expected_sequence": state.gap.expected_sequence if state.gap else None,
            "gap_observed_sequence": state.gap.observed_sequence if state.gap else None,
            "gap_resolved_at_ns": state.gap.resolved_at_ns if state.gap else None,
        }

    def _remember(self, state: _FeedState, event_id: bytes) -> None:
        state.recent_event_ids.append(event_id)
        state.recent_event_set.add(event_id)
        while len(state.recent_event_ids) > self._capacity:
            state.recent_event_set.remove(state.recent_event_ids.popleft())

    @staticmethod
    def _result(
        state: _FeedState,
        level: ValidationLevel,
        flags: tuple[str, ...],
        executable: bool,
        *,
        duplicate: bool = False,
    ) -> ObservationResult:
        return ObservationResult(
            level=level,
            state=state.state,
            flags=flags,
            executable=executable,
            duplicate=duplicate,
            expected_next_sequence=(state.last_sequence + 1) if state.last_sequence is not None else None,
            gap=state.gap,
        )

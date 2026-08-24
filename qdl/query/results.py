from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from qdl.domain.instrument import InstrumentRecord, InstrumentRegistry
from qdl.query.contracts import (
    CoverageStatus,
    DataRequirement,
    FeedType,
    QueryProblem,
)
from qdl.query.lifecycle import BarLifecycle

if TYPE_CHECKING:
    from qdl.warmup.handoff import ResampleLineage


# A fresh provider snapshot has no durable canonical-log position to resume.
# Keeping one contract sentinel prevents edge adapters from fabricating a cursor.
NON_REPLAYABLE_STREAM_CURSOR = "PASS_THROUGH_NO_REPLAY"


class QueryBackendError(RuntimeError):
    def __init__(self, problem: QueryProblem) -> None:
        super().__init__(problem.detail)
        self.problem = problem


@dataclass(frozen=True)
class SourceMetadata:
    venue: str
    provider: str
    source_id: str
    source_role: str
    authoritative: bool

    def __post_init__(self) -> None:
        if not all(value.strip() for value in (self.venue, self.provider, self.source_id)):
            raise ValueError("source venue/provider/source_id are required")


@dataclass(frozen=True)
class QualityMetadata:
    state: str
    freshness_ms: int
    gap_open: bool
    complete: bool
    execution_eligible: bool
    policy_id: str
    flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.freshness_ms < 0:
            raise ValueError("freshness_ms cannot be negative")
        if not self.state.strip() or not self.policy_id.strip():
            raise ValueError("quality state and policy_id are required")


@dataclass(frozen=True)
class ContractMetadata:
    schema_digest: str
    contract_version: str
    normalizer_version: str
    adapter_version: str
    instrument_catalog_revision: int
    source_policy_revision: int
    authority_revision: int
    config_revision: int
    correlation_id: str

    def __post_init__(self) -> None:
        if len(self.schema_digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.schema_digest
        ):
            raise ValueError("contract schema digest must be lowercase SHA-256")
        if not all(
            value.strip()
            for value in (
                self.contract_version,
                self.normalizer_version,
                self.adapter_version,
                self.correlation_id,
            )
        ):
            raise ValueError("contract version and lineage identifiers are required")
        revisions = (
            self.instrument_catalog_revision,
            self.source_policy_revision,
            self.authority_revision,
            self.config_revision,
        )
        if any(value < 1 for value in revisions):
            raise ValueError("contract lineage revisions must be positive")


@dataclass(frozen=True)
class MarketDataItem:
    instrument_uid: str
    instrument_id: str
    instrument_revision: int
    feed: FeedType
    observed_at_ns: int
    payload: dict[str, Any]
    source: SourceMetadata
    quality: QualityMetadata
    contract: ContractMetadata
    interval: str | None = None
    cursor: str | None = None
    snapshot_id: str | None = None
    revision: int = 0
    watermark_offset: int = 0
    bar_lifecycle: BarLifecycle | None = None
    supersedes_event_id: str | None = None
    received_at_ns: int | None = None
    resample_lineage: "ResampleLineage | None" = None

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip() or not self.instrument_id.strip():
            raise ValueError("market-data instrument identity is required")
        if self.instrument_revision < 1 or self.observed_at_ns <= 0 or self.revision < 0:
            raise ValueError("market-data revision/time fields are invalid")
        if self.watermark_offset < 0:
            raise ValueError("market-data watermark_offset cannot be negative")
        if self.received_at_ns is not None and self.received_at_ns <= 0:
            raise ValueError("market-data received_at_ns must be positive")
        if self.feed is FeedType.BAR and not self.interval:
            raise ValueError("bar item requires interval")
        if self.feed is not FeedType.BAR and self.interval is not None:
            raise ValueError("interval is valid only for bar items")
        if self.feed is FeedType.BAR:
            if self.bar_lifecycle in {None, BarLifecycle.UNSPECIFIED}:
                raise ValueError("bar item requires an explicit lifecycle")
            is_final = self.payload.get("is_final")
            if self.bar_lifecycle is BarLifecycle.IN_PROGRESS and is_final is not False:
                raise ValueError("in-progress bar must declare is_final=false")
            if self.bar_lifecycle in {BarLifecycle.FINAL, BarLifecycle.REVISED}:
                if is_final is not True:
                    raise ValueError("final or revised bar must declare is_final=true")
            if self.bar_lifecycle is BarLifecycle.REVISED and not self.supersedes_event_id:
                raise ValueError("revised bar must identify the superseded event")
        elif (
            self.bar_lifecycle is not None
            or self.supersedes_event_id is not None
            or self.resample_lineage is not None
        ):
            raise ValueError("bar lifecycle metadata is valid only for bar items")


@dataclass(frozen=True)
class HistoryResult:
    items: tuple[MarketDataItem, ...]
    coverage: CoverageStatus
    snapshot_id: str
    stream_cursor: str
    watermark_offset: int
    data_as_of_ns: int

    def __post_init__(self) -> None:
        if not self.snapshot_id.strip() or not self.stream_cursor.strip():
            raise ValueError("history snapshot and stream cursor are required")
        if self.data_as_of_ns <= 0:
            raise ValueError("history data_as_of_ns must be positive")
        if self.watermark_offset < 0:
            raise ValueError("history watermark_offset cannot be negative")


@runtime_checkable
class MarketDataQueryBackend(Protocol):
    def latest(self, requirement: DataRequirement) -> MarketDataItem | None: ...

    def history(self, requirement: DataRequirement) -> HistoryResult | None: ...

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None: ...

    def open_gaps(self) -> tuple["GapRecord", ...]: ...


@dataclass(frozen=True)
class GapRecord:
    gap_id: str
    instrument_uid: str
    feed: FeedType
    source_id: str
    expected_sequence: str
    observed_sequence: str
    detected_at_ns: int

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.gap_id,
                self.instrument_uid,
                self.source_id,
                self.expected_sequence,
                self.observed_sequence,
            )
        ):
            raise ValueError("gap record identity and sequence fields are required")
        if self.detected_at_ns <= 0:
            raise ValueError("gap detection time must be positive")


class MemoryMarketDataBackend:
    """Deterministic shadow/test backend; production adapters implement the protocol."""

    def __init__(self) -> None:
        self._latest: dict[tuple[str, FeedType, str | None], MarketDataItem] = {}
        self._history: dict[tuple[str, FeedType, str | None], HistoryResult] = {}
        self._gaps: list[GapRecord] = []

    @staticmethod
    def key(requirement: DataRequirement) -> tuple[str, FeedType, str | None]:
        return requirement.instrument_uid, requirement.feed, requirement.interval

    def put_latest(self, requirement: DataRequirement, item: MarketDataItem) -> None:
        self._latest[self.key(requirement)] = item

    def put_history(self, requirement: DataRequirement, result: HistoryResult) -> None:
        self._history[self.key(requirement)] = result

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        return self._latest.get(self.key(requirement))

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        return self._history.get(self.key(requirement))

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        latest = self.latest(requirement)
        if latest is not None:
            return latest.quality
        history = self.history(requirement)
        return history.items[-1].quality if history and history.items else None

    def put_gap(self, gap: GapRecord) -> None:
        self._gaps.append(gap)

    def open_gaps(self) -> tuple[GapRecord, ...]:
        return tuple(sorted(self._gaps, key=lambda item: (item.detected_at_ns, item.gap_id)))


@dataclass(frozen=True)
class InstrumentPage:
    items: tuple[InstrumentRecord, ...]
    next_cursor: str | None


class InstrumentQuery:
    def __init__(self, registry: InstrumentRegistry):
        self._registry = registry

    def get(self, identity: str) -> InstrumentRecord:
        try:
            return self._registry.get(identity)
        except KeyError:
            return self._registry.get_by_id(identity)

    def list(self, *, cursor: str | None = None, limit: int = 100) -> InstrumentPage:
        if limit < 1 or limit > 500:
            raise ValueError("instrument page limit must be between 1 and 500")
        records = self._registry.list_records()
        start = 0
        if cursor:
            matches = [index for index, item in enumerate(records) if item.instrument_uid == cursor]
            if not matches:
                raise ValueError("instrument cursor is invalid")
            start = matches[0] + 1
        selected = records[start : start + limit]
        next_cursor = (
            selected[-1].instrument_uid if selected and start + limit < len(records) else None
        )
        return InstrumentPage(selected, next_cursor)

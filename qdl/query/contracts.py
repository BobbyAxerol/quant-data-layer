from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from qdl.warmup.contracts import WarmupSpecification


class CanonicalErrorCode(StrEnum):
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    INSTRUMENT_NOT_FOUND = "INSTRUMENT_NOT_FOUND"
    UNSUPPORTED_FEED = "UNSUPPORTED_FEED"
    SCHEMA_NOT_SUPPORTED = "SCHEMA_NOT_SUPPORTED"
    DATA_NOT_READY = "DATA_NOT_READY"
    DATA_STALE = "DATA_STALE"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    SOURCE_NOT_ALLOWED = "SOURCE_NOT_ALLOWED"
    SOURCE_NON_AUTHORITATIVE = "SOURCE_NON_AUTHORITATIVE"
    OPEN_SEQUENCE_GAP = "OPEN_SEQUENCE_GAP"
    CURSOR_EXPIRED = "CURSOR_EXPIRED"
    CURSOR_INVALID = "CURSOR_INVALID"
    RATE_LIMITED = "RATE_LIMITED"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    PARTIAL_RESULT = "PARTIAL_RESULT"
    CONFLICT = "CONFLICT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


LEGACY_ERROR_ALIASES = {
    "INVALID_REQUEST": CanonicalErrorCode.INVALID_ARGUMENT,
    "UNKNOWN_INSTRUMENT": CanonicalErrorCode.INSTRUMENT_NOT_FOUND,
    "UPSTREAM_UNAVAILABLE": CanonicalErrorCode.SOURCE_UNAVAILABLE,
    "SCHEMA_UNSUPPORTED": CanonicalErrorCode.SCHEMA_NOT_SUPPORTED,
}


class ConsumerGrade(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    EXECUTION = "EXECUTION"
    ALPHA = "ALPHA"
    RESEARCH = "RESEARCH"


class FeedType(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    BAR = "BAR"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    MARK_INDEX_PRICE = "MARK_INDEX_PRICE"
    TICKER = "TICKER"
    LONG_SHORT_RATIO = "LONG_SHORT_RATIO"
    TAKER_FLOW = "TAKER_FLOW"
    BASIS = "BASIS"
    CONTRACT_METADATA = "CONTRACT_METADATA"


METRIC_INTERVAL_FEEDS = frozenset(
    {
        FeedType.LONG_SHORT_RATIO,
        FeedType.TAKER_FLOW,
        FeedType.BASIS,
    }
)

# Open interest has two provider-valid shapes: an instantaneous snapshot with
# no sampling interval, and a bounded historical series with one.  Keeping it
# separate from the required metric-series set prevents a snapshot caller from
# inventing an interval while still allowing the historical contract to carry
# the provider cadence explicitly.
OPTIONAL_INTERVAL_FEEDS = frozenset({FeedType.OPEN_INTEREST})

# These products may satisfy an execution-grade requirement. Final BAR remains
# here for established execution-readiness compatibility; it is not a direct
# broker-price substitute. Reference metrics remain alpha/research inputs even
# when their observations are fresh and provider-authoritative.
EXECUTION_PRICE_VALIDATION_FEEDS = frozenset(
    {
        FeedType.TRADE,
        FeedType.QUOTE,
        FeedType.BAR,
        FeedType.BOOK_SNAPSHOT,
        FeedType.BOOK_DELTA,
        FeedType.MARK_INDEX_PRICE,
    }
)


class CoverageStatus(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    FULL = "FULL"
    PARTIAL = "PARTIAL"
    SNAPSHOT_ONLY = "SNAPSHOT_ONLY"
    UNAVAILABLE = "UNAVAILABLE"


class StalePolicy(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class GapPolicy(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class RecoveryPolicy(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    SNAPSHOT_AND_REPLAY = "SNAPSHOT_AND_REPLAY"
    FRESH_SNAPSHOT = "FRESH_SNAPSHOT"
    NONE = "NONE"


class BarRevisionPolicy(StrEnum):
    UNSPECIFIED = "UNSPECIFIED"
    LATEST = "LATEST"
    INITIAL_ONLY = "INITIAL_ONLY"
    EMIT_REVISIONS = "EMIT_REVISIONS"


@dataclass(frozen=True)
class QueryProblem:
    code: CanonicalErrorCode
    detail: str
    retryable: bool
    retry_after_ms: int | None = None

    def __post_init__(self) -> None:
        if not self.detail.strip():
            raise ValueError("query problem detail is required")
        if self.retry_after_ms is not None and self.retry_after_ms < 0:
            raise ValueError("retry_after_ms must be non-negative")


@dataclass(frozen=True)
class DataRequirement:
    instrument_uid: str
    feed: FeedType
    consumer_grade: ConsumerGrade
    source_policy_id: str
    interval: str | None = None
    warmup_limit: int = 0
    max_freshness_ms: int | None = None
    # ``max_freshness_ms`` remains the age of the last market event.  A sparse
    # TRADE feed may observe that bound without the provider session being
    # unhealthy, so the consumer can declare a separate session SLA and choose
    # whether event recency blocks this non-execution read.
    event_recency_policy: StalePolicy | None = None
    max_session_liveness_ms: int | None = None
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: StalePolicy = StalePolicy.BLOCK
    gap_policy: GapPolicy = GapPolicy.BLOCK
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST
    warmup: WarmupSpecification | None = None

    @classmethod
    def from_mapping(cls, value: dict[str, object]) -> "DataRequirement":
        allowed = set(cls.__dataclass_fields__)
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown data requirement fields: {sorted(unknown)}")
        converted = dict(value)
        enums = {
            "feed": FeedType,
            "consumer_grade": ConsumerGrade,
            "stale_policy": StalePolicy,
            "gap_policy": GapPolicy,
            "recovery": RecoveryPolicy,
            "bar_revision_policy": BarRevisionPolicy,
            "event_recency_policy": StalePolicy,
        }
        for field, enum_type in enums.items():
            if (
                field in converted
                and converted[field] is not None
                and not isinstance(converted[field], enum_type)
            ):
                converted[field] = enum_type(str(converted[field]).upper())
        if isinstance(converted.get("warmup"), dict):
            converted["warmup"] = WarmupSpecification.from_mapping(converted["warmup"])
        return cls(**converted)

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip():
            raise ValueError("instrument_uid is required")
        if not self.source_policy_id.strip():
            raise ValueError("source_policy_id is required")
        if not 0 <= self.warmup_limit <= 10_000:
            raise ValueError("warmup_limit must be between 0 and 10000")
        if self.warmup is not None and not isinstance(
            self.warmup, WarmupSpecification
        ):
            raise TypeError("warmup must use WarmupSpecification")
        if self.warmup is not None and self.warmup.rows is not None:
            if self.warmup.rows > 10_000:
                raise ValueError("public V2 warmup rows cannot exceed 10000")
            if self.warmup_limit not in {0, self.warmup.rows}:
                raise ValueError("warmup_limit conflicts with warmup.rows")
        if self.warmup is not None and self.warmup.time_range is not None:
            if self.warmup_limit != 0:
                raise ValueError("time-range warmup cannot also declare warmup_limit")
        if self.max_freshness_ms is not None and self.max_freshness_ms <= 0:
            raise ValueError("max_freshness_ms must be positive")
        if (
            self.max_session_liveness_ms is not None
            and self.max_session_liveness_ms <= 0
        ):
            raise ValueError("max_session_liveness_ms must be positive")
        if self.event_recency_policy is not None and not isinstance(
            self.event_recency_policy, StalePolicy
        ):
            raise TypeError("event_recency_policy must use StalePolicy")
        if (
            self.event_recency_policy is StalePolicy.UNSPECIFIED
            or (
                self.event_recency_policy is StalePolicy.OBSERVE
                and self.max_session_liveness_ms is None
            )
        ):
            raise ValueError(
                "observed event recency requires an explicit provider session SLA"
            )
        enum_values = (
            self.feed,
            self.consumer_grade,
            self.stale_policy,
            self.gap_policy,
            self.recovery,
            self.bar_revision_policy,
        )
        if any(value.value == "UNSPECIFIED" for value in enum_values):
            raise ValueError("UNSPECIFIED enum values are invalid at the V2 boundary")
        if self.feed is FeedType.BAR:
            if self.interval is None or not self.interval.strip():
                raise ValueError("bar requirements need an interval")
        elif self.feed in METRIC_INTERVAL_FEEDS:
            if self.interval is None or not self.interval.strip():
                raise ValueError("metric-series requirements need a sampling interval")
        elif self.feed in OPTIONAL_INTERVAL_FEEDS:
            if self.interval is not None and not self.interval.strip():
                raise ValueError("open-interest sampling interval cannot be blank")
        elif self.interval is not None:
            raise ValueError("interval is valid only for bar or metric-series requirements")
        if self.consumer_grade is ConsumerGrade.EXECUTION:
            if self.feed not in EXECUTION_PRICE_VALIDATION_FEEDS:
                raise ValueError(
                    "execution-grade requirements need an execution-price validation feed"
                )
            if self.stale_policy is not StalePolicy.BLOCK:
                raise ValueError("execution-grade stale policy must BLOCK")
            if self.gap_policy is not GapPolicy.BLOCK:
                raise ValueError("execution-grade gap policy must BLOCK")
            if not self.require_full_coverage:
                raise ValueError("execution-grade requirements need full coverage")

    @property
    def effective_event_recency_policy(self) -> StalePolicy:
        """Preserve legacy blocking semantics unless a route declares otherwise."""

        return self.event_recency_policy or self.stale_policy

    @property
    def warmup_specification(self) -> WarmupSpecification | None:
        if self.warmup is not None:
            return self.warmup
        if self.warmup_limit:
            return WarmupSpecification.for_rows(self.warmup_limit)
        return None


@dataclass(frozen=True)
class BatchRequirement:
    consumer_id: str
    requirements: tuple[DataRequirement, ...]
    require_all: bool = True

    def __post_init__(self) -> None:
        if not self.consumer_id.strip():
            raise ValueError("consumer_id is required")
        if not 1 <= len(self.requirements) <= 100:
            raise ValueError("batch requires between 1 and 100 items")
        identities = [(item.instrument_uid, item.feed, item.interval) for item in self.requirements]
        if len(identities) != len(set(identities)):
            raise ValueError("batch contains duplicate data requirements")
        if any(item.consumer_grade is ConsumerGrade.EXECUTION for item in self.requirements):
            if not self.require_all:
                raise ValueError("execution-grade batch must require all items")


def evaluate_requirement(
    requirement: DataRequirement,
    *,
    coverage: CoverageStatus,
    entitled: bool,
    available: bool,
    fresh: bool,
    authoritative: bool,
    gap_open: bool,
) -> QueryProblem | None:
    """Return the first stable fail-closed problem for a requirement."""

    if not entitled:
        return QueryProblem(
            CanonicalErrorCode.SOURCE_NOT_ALLOWED,
            "source entitlement or licensing policy denied this data product",
            False,
        )
    if not available or coverage is CoverageStatus.UNAVAILABLE:
        return QueryProblem(
            CanonicalErrorCode.DATA_NOT_READY,
            "required data is not available",
            True,
        )
    if requirement.consumer_grade is ConsumerGrade.EXECUTION and not authoritative:
        return QueryProblem(
            CanonicalErrorCode.SOURCE_NON_AUTHORITATIVE,
            "execution-grade data source is not authoritative",
            True,
        )
    if gap_open and requirement.gap_policy in {GapPolicy.BLOCK, GapPolicy.PAUSE}:
        return QueryProblem(
            CanonicalErrorCode.OPEN_SEQUENCE_GAP,
            "required feed has an unresolved sequence gap",
            True,
        )
    if not fresh and requirement.stale_policy in {StalePolicy.BLOCK, StalePolicy.PAUSE}:
        return QueryProblem(
            CanonicalErrorCode.DATA_STALE,
            "required data exceeds its freshness policy",
            True,
        )
    if requirement.require_full_coverage and coverage is not CoverageStatus.FULL:
        return QueryProblem(
            CanonicalErrorCode.PARTIAL_RESULT,
            "requirement requested full historical coverage",
            coverage is CoverageStatus.PARTIAL,
        )
    return None

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Literal

try:  # Python 3.11+; the released SDK contract supports Python 3.10.
    from enum import StrEnum
except ImportError:  # pragma: no cover - exercised by the Python 3.10 artifact gate.
    class StrEnum(str, Enum):
        def __str__(self) -> str:
            return self.value

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qdl.query.v2 import query_pb2


try:
    # The service and SDK share enum identity when the public query package is present.
    from qdl.query import BarLifecycle, FeedType
except ImportError:
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

    class BarLifecycle(StrEnum):
        UNSPECIFIED = "UNSPECIFIED"
        IN_PROGRESS = "IN_PROGRESS"
        FINAL = "FINAL"
        REVISED = "REVISED"
        CANCELLED = "CANCELLED"


# Concise public SDK spelling; the frozen wire component remains FeedType.
Feed = FeedType


class Grade(StrEnum):
    EXECUTION = "EXECUTION"
    ALPHA = "ALPHA"
    RESEARCH = "RESEARCH"


class StalePolicy(StrEnum):
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class GapPolicy(StrEnum):
    BLOCK = "BLOCK"
    PAUSE = "PAUSE"
    OBSERVE = "OBSERVE"


class RecoveryPolicy(StrEnum):
    SNAPSHOT_AND_REPLAY = "SNAPSHOT_AND_REPLAY"
    FRESH_SNAPSHOT = "FRESH_SNAPSHOT"
    NONE = "NONE"


class BarRevisionPolicy(StrEnum):
    LATEST = "LATEST"
    INITIAL_ONLY = "INITIAL_ONLY"
    EMIT_REVISIONS = "EMIT_REVISIONS"


class ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ProblemDetails(ClosedModel):
    type: str
    title: str
    status: int
    code: str
    detail: str
    request_id: str
    retryable: bool
    retry_after_ms: int | None = None
    instrument_uid: str | None = None
    quality_state: str | None = None


class DecimalValue(ClosedModel):
    coefficient: str = Field(pattern=r"^-?(0|[1-9][0-9]*)$")
    scale: int = Field(ge=-38, le=38)
    source_text: str = Field(min_length=1, max_length=128)


class TradeIdentityKind(StrEnum):
    NATIVE = "NATIVE"
    DERIVED_RAW_CAPTURE = "DERIVED_RAW_CAPTURE"


class QuantityUnit(StrEnum):
    BASE_ASSET = "BASE_ASSET"
    QUOTE_ASSET = "QUOTE_ASSET"
    CONTRACT = "CONTRACT"
    SHARE = "SHARE"


class SourceView(ClosedModel):
    venue: str = Field(min_length=1, max_length=40)
    provider: str = Field(min_length=1, max_length=80)
    source_id: str = Field(min_length=1, max_length=120)
    source_role: Literal["PRIMARY", "SECONDARY", "REFERENCE", "BACKFILL"]
    authoritative: bool


class QualityView(ClosedModel):
    state: Literal[
        "DISABLED", "STARTING", "CONNECTING", "SUBSCRIBING", "SYNCING",
        "LIVE", "DEGRADED", "GAPPED", "RESYNCING", "STALE", "OFFLINE",
        "HALTED", "MARKET_CLOSED",
    ]
    freshness_ms: int = Field(ge=0)
    gap_open: bool
    complete: bool
    execution_eligible: bool
    policy_id: str = Field(min_length=1, max_length=200)
    flags: list[str]


class ContractView(ClosedModel):
    schema_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_version: str = Field(min_length=1, max_length=40)
    normalizer_version: str = Field(min_length=1, max_length=80)
    adapter_version: str = Field(min_length=1, max_length=80)
    instrument_catalog_revision: int = Field(ge=1)
    source_policy_revision: int = Field(ge=1)
    authority_revision: int = Field(ge=1)
    config_revision: int = Field(ge=1)
    correlation_id: str = Field(min_length=1, max_length=200)


class TradePayload(ClosedModel):
    feed: Literal[Feed.TRADE] = Feed.TRADE
    native_trade_id: str = Field(min_length=1, max_length=200)
    price: DecimalValue
    quantity: DecimalValue
    quantity_unit: QuantityUnit
    aggressor_side: Literal["BUY", "SELL", "UNKNOWN"]
    identity_kind: TradeIdentityKind
    is_block_trade: bool = False
    is_buyer_maker: bool = False


class QuotePayload(ClosedModel):
    feed: Literal[Feed.QUOTE] = Feed.QUOTE
    bid_price: DecimalValue
    bid_quantity: DecimalValue
    ask_price: DecimalValue
    ask_quantity: DecimalValue
    quantity_unit: QuantityUnit
    level: int = Field(default=1, ge=1)


class BarPayload(ClosedModel):
    feed: Literal[Feed.BAR] = Feed.BAR
    interval: str = Field(min_length=1, max_length=20)
    open_time_ns: int = Field(gt=0)
    close_time_ns: int = Field(gt=0)
    open: DecimalValue
    high: DecimalValue
    low: DecimalValue
    close: DecimalValue
    volume: DecimalValue
    volume_unit: QuantityUnit
    base_volume: DecimalValue | None = None
    quote_volume: DecimalValue | None = None
    contract_volume: DecimalValue | None = None
    trade_count: int = Field(default=0, ge=0)
    lifecycle: BarLifecycle
    revision: int = Field(ge=0)
    origin: Literal["VENUE_NATIVE", "AGGREGATED", "BACKFILLED", "RECONCILED"]
    supersedes_event_id: str | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self):
        if self.lifecycle is BarLifecycle.UNSPECIFIED:
            raise ValueError("bar lifecycle cannot be UNSPECIFIED")
        if self.lifecycle is BarLifecycle.REVISED and not self.supersedes_event_id:
            raise ValueError("revised bar must identify the superseded event")
        if self.close_time_ns <= self.open_time_ns:
            raise ValueError("bar close time must be after open time")
        return self


class BookLevel(ClosedModel):
    side: Literal["BID", "ASK"]
    price: DecimalValue
    quantity: DecimalValue
    quantity_unit: QuantityUnit
    order_count: int = Field(default=0, ge=0)


class BookSnapshotPayload(ClosedModel):
    feed: Literal[Feed.BOOK_SNAPSHOT] = Feed.BOOK_SNAPSHOT
    native_sequence: str = Field(min_length=1, max_length=200)
    checksum: str | None = Field(default=None, max_length=200)
    levels: list[BookLevel]
    depth: int = Field(ge=1)


class BookDeltaPayload(ClosedModel):
    feed: Literal[Feed.BOOK_DELTA] = Feed.BOOK_DELTA
    native_sequence_start: str = Field(min_length=1, max_length=200)
    native_sequence_end: str = Field(min_length=1, max_length=200)
    snapshot_sequence: str = Field(min_length=1, max_length=200)
    checksum: str | None = Field(default=None, max_length=200)
    updates: list[BookLevel]
    reset: bool = False


class FundingRatePayload(ClosedModel):
    feed: Literal[Feed.FUNDING_RATE] = Feed.FUNDING_RATE
    rate: DecimalValue
    funding_time_ns: int = Field(gt=0)
    next_funding_time_ns: int | None = Field(default=None, gt=0)


class OpenInterestPayload(ClosedModel):
    feed: Literal[Feed.OPEN_INTEREST] = Feed.OPEN_INTEREST
    quantity: DecimalValue
    quantity_unit: QuantityUnit
    notional: DecimalValue | None = None


class MarkIndexPricePayload(ClosedModel):
    feed: Literal[Feed.MARK_INDEX_PRICE] = Feed.MARK_INDEX_PRICE
    mark_price: DecimalValue
    index_price: DecimalValue


class TickerPayload(ClosedModel):
    feed: Literal[Feed.TICKER] = Feed.TICKER
    last_price: DecimalValue
    last_quantity: DecimalValue | None = None
    open_24h: DecimalValue | None = None
    high_24h: DecimalValue | None = None
    low_24h: DecimalValue | None = None
    volume_24h: DecimalValue | None = None
    last_quantity_unit: QuantityUnit | None = None
    volume_24h_unit: QuantityUnit | None = None

    @model_validator(mode="after")
    def quantity_units_match_optional_values(self):
        if (self.last_quantity is None) != (self.last_quantity_unit is None):
            raise ValueError("ticker last quantity and unit must be present together")
        if (self.volume_24h is None) != (self.volume_24h_unit is None):
            raise ValueError("ticker 24h volume and unit must be present together")
        return self


MarketPayload = Annotated[
    TradePayload | QuotePayload | BarPayload | BookSnapshotPayload | BookDeltaPayload
    | FundingRatePayload | OpenInterestPayload | MarkIndexPricePayload | TickerPayload,
    Field(discriminator="feed"),
]


class MarketDataView(ClosedModel):
    instrument_uid: str
    instrument_id: str
    instrument_revision: int = Field(ge=1)
    feed: Feed
    interval: str | None
    observed_at_ns: int = Field(gt=0)
    revision: int = Field(ge=0)
    payload: MarketPayload
    source: SourceView
    quality: QualityView
    contract: ContractView
    cursor: str | None = None
    snapshot_id: str | None = None
    watermark_offset: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def feed_matches_payload(self):
        if self.payload.feed is not self.feed:
            raise ValueError("market-data envelope feed does not match its payload")
        if self.feed is Feed.BAR:
            if not self.interval or self.payload.interval != self.interval:
                raise ValueError("bar envelope and payload interval must match")
            if self.payload.revision != self.revision:
                raise ValueError("bar envelope and payload revision must match")
        elif self.interval is not None:
            raise ValueError("interval is valid only for bar market data")
        return self


class SnapshotResponse(ClosedModel):
    contract_schema: str = Field("qdl.marketdata.snapshot.v2", alias="schema")
    request_id: str
    data: MarketDataView


class InstrumentView(ClosedModel):
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    product_type: str
    canonical_symbol: str
    metadata_revision: int = Field(ge=1)
    asset_class: str
    native_symbol: str
    status: str


class InstrumentPageResponse(ClosedModel):
    contract_schema: str = Field("qdl.instruments.page.v2", alias="schema")
    items: list[InstrumentView]
    next_cursor: str | None = None


class InstrumentResponse(InstrumentView):
    contract_schema: str = Field("qdl.instrument.v2", alias="schema")


class WarmupResponse(ClosedModel):
    contract_schema: str = Field("qdl.marketdata.warmup.v2", alias="schema")
    request_id: str
    snapshot_id: str = Field(min_length=1)
    data_as_of_ns: int = Field(gt=0)
    stream_cursor: str = Field(min_length=1)
    watermark_offset: int = Field(ge=0)
    coverage: str
    count: int
    data: list[MarketDataView]


class BatchItemResponse(ClosedModel):
    instrument_uid: str
    status: str
    data: WarmupResponse | None = None
    problem: ProblemDetails | None = None


class BatchResponse(ClosedModel):
    contract_schema: str = Field("qdl.marketdata.batch.v2", alias="schema")
    request_id: str
    partial: bool
    success_count: int
    error_count: int
    results: list[BatchItemResponse]

    @model_validator(mode="after")
    def counts_match(self):
        if self.success_count + self.error_count != len(self.results):
            raise ValueError("batch counts do not match results")
        if self.partial != (self.error_count > 0):
            raise ValueError("batch partial flag does not match errors")
        return self


class ReadinessItemResponse(ClosedModel):
    instrument_uid: str
    status: str
    quality: QualityView | None = None
    problem: ProblemDetails | None = None


class ReadinessResponse(ClosedModel):
    contract_schema: str = Field("qdl.system-readiness.v2", alias="schema")
    request_id: str
    ready: bool
    authority: str = "V1"
    results: list[ReadinessItemResponse]


class FeedStatusResponse(ClosedModel):
    contract_schema: str = Field("qdl.feed-status.v2", alias="schema")
    instrument_uid: str
    feed: Feed
    quality: QualityView


class GapView(ClosedModel):
    gap_id: str
    instrument_uid: str
    feed: Feed
    source_id: str
    expected_sequence: str
    observed_sequence: str
    detected_at_ns: int


class GapListResponse(ClosedModel):
    contract_schema: str = Field("qdl.data-quality.gaps.v2", alias="schema")
    items: list[GapView]


class SystemReadinessSummary(ClosedModel):
    contract_schema: str = Field("qdl.system-readiness.v2", alias="schema")
    status: str
    authority: str
    v2_consumer_activation: str


@dataclass(frozen=True)
class DataRequirement:
    instrument_uid: str
    feed: Feed
    consumer_grade: Grade
    source_policy_id: str
    interval: str | None = None
    warmup_limit: int = 0
    max_freshness_ms: int | None = None
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: StalePolicy = StalePolicy.BLOCK
    gap_policy: GapPolicy = GapPolicy.BLOCK
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip() or not self.source_policy_id.strip():
            raise ValueError("instrument_uid and source_policy_id are required")
        if self.feed is Feed.UNSPECIFIED:
            raise ValueError("UNSPECIFIED feed is invalid at the V2 boundary")
        enum_fields = (
            (self.feed, Feed, "feed"),
            (self.consumer_grade, Grade, "consumer_grade"),
            (self.stale_policy, StalePolicy, "stale_policy"),
            (self.gap_policy, GapPolicy, "gap_policy"),
            (self.recovery, RecoveryPolicy, "recovery"),
            (self.bar_revision_policy, BarRevisionPolicy, "bar_revision_policy"),
        )
        for value, enum_type, field in enum_fields:
            if not isinstance(value, enum_type):
                raise TypeError(f"{field} must use the typed SDK enum")
        if not 0 <= self.warmup_limit <= 10_000:
            raise ValueError("warmup_limit must be between 0 and 10000")
        if self.max_freshness_ms is not None and self.max_freshness_ms <= 0:
            raise ValueError("max_freshness_ms must be positive")
        if self.feed is Feed.BAR and not self.interval:
            raise ValueError("bar requirement needs interval")
        if self.feed is not Feed.BAR and self.interval is not None:
            raise ValueError("interval is valid only for bar requirements")
        if self.consumer_grade is Grade.EXECUTION and (
            self.stale_policy is not StalePolicy.BLOCK
            or self.gap_policy is not GapPolicy.BLOCK
            or not self.require_full_coverage
        ):
            raise ValueError("execution-grade requirement cannot relax fail-closed policy")

    def query_params(self) -> dict[str, str | int | bool]:
        values: dict[str, str | int | bool | None] = {
            "feed": self.feed.value,
            "consumer_grade": self.consumer_grade.value,
            "source_policy_id": self.source_policy_id,
            "interval": self.interval,
            "limit": self.warmup_limit or None,
            "max_freshness_ms": self.max_freshness_ms,
            "require_full_coverage": self.require_full_coverage,
            "require_final_bars": self.require_final_bars,
            "stale_policy": self.stale_policy.value,
            "gap_policy": self.gap_policy.value,
            "recovery": self.recovery.value,
            "bar_revision_policy": self.bar_revision_policy.value,
        }
        return {key: value for key, value in values.items() if value is not None}

    def to_proto(self) -> query_pb2.DataRequirement:
        return query_pb2.DataRequirement(
            instrument_uid=self.instrument_uid,
            interval=self.interval or "",
            source_policy_id=self.source_policy_id,
            warmup_limit=self.warmup_limit,
            max_freshness_ms=self.max_freshness_ms or 0,
            require_full_coverage=self.require_full_coverage,
            require_final_bars=self.require_final_bars,
            feed_type=getattr(query_pb2, f"FEED_TYPE_{self.feed.value}"),
            grade=getattr(query_pb2, f"CONSUMER_GRADE_{self.consumer_grade.value}"),
            stale_policy_type=getattr(
                query_pb2, f"STALE_POLICY_{self.stale_policy.value}"
            ),
            gap_policy_type=getattr(query_pb2, f"GAP_POLICY_{self.gap_policy.value}"),
            recovery_policy=getattr(
                query_pb2, f"RECOVERY_POLICY_{self.recovery.value}"
            ),
            revision_policy=getattr(
                query_pb2, f"BAR_REVISION_POLICY_{self.bar_revision_policy.value}"
            ),
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
    snapshot: WarmupResponse | dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.detail.strip():
            raise ValueError("control event code/detail are required")

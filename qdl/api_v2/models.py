from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qdl.query import (
    BarLifecycle,
    BarRevisionPolicy,
    ConsumerGrade,
    FeedType,
    GapPolicy,
    RecoveryPolicy,
    StalePolicy,
)


class ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class RequirementModel(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    feed: FeedType
    consumer_grade: ConsumerGrade
    source_policy_id: str = Field(min_length=1, max_length=200)
    interval: str | None = Field(default=None, max_length=20)
    warmup_limit: int = Field(default=0, ge=0, le=10_000)
    max_freshness_ms: int | None = Field(default=None, gt=0, le=86_400_000)
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: StalePolicy = StalePolicy.BLOCK
    gap_policy: GapPolicy = GapPolicy.BLOCK
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST


class BatchRequirementModel(ClosedModel):
    consumer_id: str = Field(min_length=1, max_length=200)
    requirements: list[RequirementModel] = Field(min_length=1, max_length=100)
    require_all: bool = True


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
    feed: Literal[FeedType.TRADE] = FeedType.TRADE
    native_trade_id: str = Field(min_length=1, max_length=200)
    price: DecimalValue
    quantity: DecimalValue
    aggressor_side: Literal["BUY", "SELL"]
    is_block_trade: bool = False
    is_buyer_maker: bool = False


class QuotePayload(ClosedModel):
    feed: Literal[FeedType.QUOTE] = FeedType.QUOTE
    bid_price: DecimalValue
    bid_quantity: DecimalValue
    ask_price: DecimalValue
    ask_quantity: DecimalValue
    level: int = Field(default=1, ge=1)


class BarPayload(ClosedModel):
    feed: Literal[FeedType.BAR] = FeedType.BAR
    interval: str = Field(min_length=1, max_length=20)
    open_time_ns: int = Field(gt=0)
    close_time_ns: int = Field(gt=0)
    open: DecimalValue
    high: DecimalValue
    low: DecimalValue
    close: DecimalValue
    volume: DecimalValue
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
    order_count: int = Field(default=0, ge=0)


class BookSnapshotPayload(ClosedModel):
    feed: Literal[FeedType.BOOK_SNAPSHOT] = FeedType.BOOK_SNAPSHOT
    native_sequence: str = Field(min_length=1, max_length=200)
    checksum: str | None = Field(default=None, max_length=200)
    levels: list[BookLevel]
    depth: int = Field(ge=1)


class BookDeltaPayload(ClosedModel):
    feed: Literal[FeedType.BOOK_DELTA] = FeedType.BOOK_DELTA
    native_sequence_start: str = Field(min_length=1, max_length=200)
    native_sequence_end: str = Field(min_length=1, max_length=200)
    snapshot_sequence: str = Field(min_length=1, max_length=200)
    checksum: str | None = Field(default=None, max_length=200)
    updates: list[BookLevel]
    reset: bool = False


class FundingRatePayload(ClosedModel):
    feed: Literal[FeedType.FUNDING_RATE] = FeedType.FUNDING_RATE
    rate: DecimalValue
    funding_time_ns: int = Field(gt=0)
    next_funding_time_ns: int | None = Field(default=None, gt=0)


class OpenInterestPayload(ClosedModel):
    feed: Literal[FeedType.OPEN_INTEREST] = FeedType.OPEN_INTEREST
    quantity: DecimalValue
    notional: DecimalValue | None = None


class MarkIndexPricePayload(ClosedModel):
    feed: Literal[FeedType.MARK_INDEX_PRICE] = FeedType.MARK_INDEX_PRICE
    mark_price: DecimalValue
    index_price: DecimalValue


class TickerPayload(ClosedModel):
    feed: Literal[FeedType.TICKER] = FeedType.TICKER
    last_price: DecimalValue
    last_quantity: DecimalValue | None = None
    open_24h: DecimalValue | None = None
    high_24h: DecimalValue | None = None
    low_24h: DecimalValue | None = None
    volume_24h: DecimalValue | None = None


MarketPayload = Annotated[
    TradePayload | QuotePayload | BarPayload | BookSnapshotPayload | BookDeltaPayload
    | FundingRatePayload | OpenInterestPayload | MarkIndexPricePayload | TickerPayload,
    Field(discriminator="feed"),
]


class MarketDataView(ClosedModel):
    instrument_uid: str
    instrument_id: str
    instrument_revision: int = Field(ge=1)
    feed: FeedType
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
        if self.feed is FeedType.UNSPECIFIED or self.payload.feed is not self.feed:
            raise ValueError("market-data envelope feed does not match its payload")
        if self.feed is FeedType.BAR:
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
    feed: FeedType
    quality: QualityView


class GapView(ClosedModel):
    gap_id: str
    instrument_uid: str
    feed: FeedType
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

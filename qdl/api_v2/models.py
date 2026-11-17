from __future__ import annotations

from pydantic import Field, model_validator

from qdl.query import (
    BarRevisionPolicy,
    ConsumerGrade,
    FeedType,
    GapPolicy,
    RecoveryPolicy,
    StalePolicy,
)
from qdl_sdk.models import (
    BarPayload,
    BatchItemResponse,
    BatchResponse,
    BookDeltaPayload,
    BookLevel,
    BookSnapshotPayload,
    ClosedModel,
    ContractView,
    DecimalValue,
    FeedStatusResponse,
    FundingRatePayload,
    GapListResponse,
    GapView,
    InstrumentPageResponse,
    InstrumentResponse,
    InstrumentView,
    MarkIndexPricePayload,
    MarketDataView,
    OpenInterestPayload,
    ProblemDetails,
    QualityView,
    QuantityUnit,
    QuotePayload,
    ReadinessItemResponse,
    ReadinessResponse,
    ResponseModel,
    SnapshotResponse,
    SourceView,
    SystemReadinessSummary,
    TickerPayload,
    TradeIdentityKind,
    TradePayload,
    WarmupResponse,
    WarmupSpecification,
)
from qdl_sdk.reference import ReferenceBatchRequest, ReferenceBatchResponse


# The sealed V2 final-BAR catalog currently reaches a one-week interval with
# a 21-day freshness allowance. Keep the public boundary aligned with that
# governed policy while individual consumer manifests remain authoritative.
MAX_REQUIREMENT_FRESHNESS_MS = 21 * 86_400_000


class RequirementModel(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    feed: FeedType
    consumer_grade: ConsumerGrade
    source_policy_id: str = Field(min_length=1, max_length=200)
    interval: str | None = Field(default=None, max_length=20)
    warmup_limit: int = Field(default=0, ge=0, le=10_000)
    max_freshness_ms: int | None = Field(
        default=None,
        gt=0,
        le=MAX_REQUIREMENT_FRESHNESS_MS,
    )
    event_recency_policy: StalePolicy | None = None
    max_session_liveness_ms: int | None = Field(default=None, gt=0, le=86_400_000)
    require_full_coverage: bool = True
    require_final_bars: bool = True
    stale_policy: StalePolicy = StalePolicy.BLOCK
    gap_policy: GapPolicy = GapPolicy.BLOCK
    recovery: RecoveryPolicy = RecoveryPolicy.SNAPSHOT_AND_REPLAY
    bar_revision_policy: BarRevisionPolicy = BarRevisionPolicy.LATEST
    warmup: WarmupSpecification | None = None


class BatchRequirementModel(ClosedModel):
    consumer_id: str = Field(min_length=1, max_length=200)
    requirements: list[RequirementModel] = Field(min_length=1, max_length=100)
    require_all: bool = True


# B4 (DL v2.3.1): batched latest snapshots, one HTTP call (one quota unit) for
# many probe reads. Additive route and schemas; the single snapshot contract
# (``qdl.marketdata.snapshot.v2``) is unchanged and each item's ``data`` is that
# contract's ``data``.
SNAPSHOT_BATCH_SCHEMA = "qdl.marketdata.snapshot-batch.v1"


class SnapshotBatchRequestModel(ClosedModel):
    consumer_id: str = Field(min_length=1, max_length=200)
    # The manifest's ``max_batch_items`` is the binding bound (checked first);
    # this is the transport ceiling shared with the other batch routes.
    requirements: list[RequirementModel] = Field(min_length=1, max_length=100)


class SnapshotBatchItemResponse(ResponseModel):
    instrument_uid: str
    feed: FeedType
    status: str
    data: MarketDataView | None = None
    problem: ProblemDetails | None = None


class SnapshotBatchResponse(ResponseModel):
    contract_schema: str = Field(SNAPSHOT_BATCH_SCHEMA, alias="schema")
    request_id: str
    partial: bool
    success_count: int
    error_count: int
    results: list[SnapshotBatchItemResponse]

    @model_validator(mode="after")
    def counts_match(self):
        if self.success_count + self.error_count != len(self.results):
            raise ValueError("snapshot batch counts do not match results")
        if self.partial != (self.error_count > 0):
            raise ValueError("snapshot batch partial flag does not match errors")
        if any((item.data is None) == (item.problem is None) for item in self.results):
            raise ValueError("snapshot batch item needs exactly one of data or problem")
        return self

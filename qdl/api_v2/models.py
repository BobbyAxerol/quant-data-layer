from __future__ import annotations

from pydantic import Field

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
    warmup: WarmupSpecification | None = None


class BatchRequirementModel(ClosedModel):
    consumer_id: str = Field(min_length=1, max_length=200)
    requirements: list[RequirementModel] = Field(min_length=1, max_length=100)
    require_all: bool = True

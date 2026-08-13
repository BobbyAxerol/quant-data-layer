from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qdl.query import (
    BarRevisionPolicy,
    ConsumerGrade,
    FeedType,
    GapPolicy,
    RecoveryPolicy,
    StalePolicy,
)


class RequirementModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


class BatchRequirementModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_id: str = Field(min_length=1, max_length=200)
    requirements: list[RequirementModel] = Field(min_length=1, max_length=100)
    require_all: bool = True


class ProblemDetails(BaseModel):
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


class SourceView(BaseModel):
    venue: str
    provider: str
    source_id: str
    source_role: str
    authoritative: bool


class QualityView(BaseModel):
    state: str
    freshness_ms: int
    gap_open: bool
    complete: bool
    execution_eligible: bool
    policy_id: str
    flags: list[str]


class MarketDataView(BaseModel):
    instrument_uid: str
    instrument_id: str
    instrument_revision: int
    feed: str
    interval: str | None
    observed_at_ns: int
    revision: int
    payload: dict[str, Any]
    source: SourceView
    quality: QualityView
    cursor: str | None = None
    snapshot_id: str | None = None


class SnapshotResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    contract_schema: str = Field("qdl.marketdata.snapshot.v2", alias="schema")
    request_id: str
    data: MarketDataView


class WarmupResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    contract_schema: str = Field("qdl.marketdata.warmup.v2", alias="schema")
    request_id: str
    snapshot_id: str
    data_as_of_ns: int
    stream_cursor: str
    coverage: str
    count: int
    data: list[MarketDataView]


class BatchItemResponse(BaseModel):
    instrument_uid: str
    status: str
    data: WarmupResponse | None = None
    problem: ProblemDetails | None = None


class BatchResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

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


class ReadinessItemResponse(BaseModel):
    instrument_uid: str
    status: str
    quality: QualityView | None = None
    problem: ProblemDetails | None = None


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    contract_schema: str = Field("qdl.system-readiness.v2", alias="schema")
    request_id: str
    ready: bool
    authority: str = "V1"
    results: list[ReadinessItemResponse]

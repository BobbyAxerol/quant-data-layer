"""Typed V2 REST contract for bounded provider reference-data batches.

This is intentionally a query contract, not a broker/order model.  It carries
only catalog-bound identities, provider-authentic measurements and explicit
coverage/lineage.  Unknown or unavailable data remains typed rather than
becoming a numeric zero in an alpha dataframe.
"""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import Field, model_validator

from qdl_sdk.models import ClosedModel, DecimalValue, Grade, ProblemDetails


# Reference products may be deliberately less frequent than price data. The
# stable research manifest currently declares a three-day freshness window for
# low-frequency historical metrics; keep the public SDK aligned with that
# governed contract rather than rejecting an otherwise valid request locally.
MAX_REFERENCE_FRESHNESS_MS = 259_200_000


class _StringEnum(str, Enum):
    pass


class ReferenceProduct(_StringEnum):
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    LONG_SHORT_RATIO = "LONG_SHORT_RATIO"
    TAKER_FLOW = "TAKER_FLOW"
    MARK_INDEX_PRICE = "MARK_INDEX_PRICE"
    CONTRACT_METADATA = "CONTRACT_METADATA"
    BASIS = "BASIS"


class LongShortKind(_StringEnum):
    GLOBAL_ACCOUNT = "GLOBAL_ACCOUNT"
    TOP_ACCOUNT = "TOP_ACCOUNT"
    TOP_POSITION = "TOP_POSITION"


class MarkIndexKind(_StringEnum):
    MARK = "MARK"
    INDEX = "INDEX"
    BOTH = "BOTH"


class BasisSeries(_StringEnum):
    NATIVE = "NATIVE"
    CONTINUOUS = "CONTINUOUS"


class ReferenceRequirement(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    product: ReferenceProduct
    consumer_grade: Grade
    source_policy_id: str = Field(min_length=1, max_length=200)
    start_time_ns: int | None = Field(default=None, gt=0)
    end_time_ns: int | None = Field(default=None, gt=0)
    interval: str | None = Field(default=None, max_length=20)
    limit: int = Field(default=100, ge=1, le=10_000)
    page_size: int | None = Field(default=None, ge=1, le=10_000)
    max_pages: int = Field(default=20, ge=1, le=100)
    long_short_kind: LongShortKind | None = None
    mark_index_kind: MarkIndexKind = MarkIndexKind.BOTH
    basis_series: BasisSeries = BasisSeries.NATIVE
    basis_contract_type: str | None = Field(default=None, max_length=40)
    max_freshness_ms: int | None = Field(
        default=None,
        gt=0,
        le=MAX_REFERENCE_FRESHNESS_MS,
    )
    require_full_coverage: bool = True
    deadline_ms: int = Field(default=20_000, ge=100, le=120_000)

    @model_validator(mode="after")
    def valid_shape(self):
        execution_mark_snapshot = (
            self.consumer_grade is Grade.EXECUTION
            and self.product is ReferenceProduct.MARK_INDEX_PRICE
        )
        if self.consumer_grade is Grade.EXECUTION and not execution_mark_snapshot:
            raise ValueError(
                "only MARK_INDEX_PRICE may be requested as execution-grade reference data"
            )
        if (self.start_time_ns is None) != (self.end_time_ns is None):
            raise ValueError("reference start_time_ns and end_time_ns are required together")
        if self.start_time_ns is not None:
            if self.start_time_ns % 1_000_000 or self.end_time_ns % 1_000_000:
                raise ValueError("reference time range must align to whole milliseconds")
            if self.end_time_ns <= self.start_time_ns:
                raise ValueError("reference time range must increase")
        if self.interval is not None and not self.interval.strip():
            raise ValueError("reference interval cannot be blank")
        if self.page_size is not None and self.page_size > self.limit:
            raise ValueError("reference page_size cannot exceed limit")
        historical = self.start_time_ns is not None
        interval_products = {
            ReferenceProduct.LONG_SHORT_RATIO,
            ReferenceProduct.TAKER_FLOW,
            ReferenceProduct.BASIS,
        }
        if self.product in interval_products and not self.interval:
            raise ValueError("reference metric history requires interval")
        if self.product in interval_products and not historical:
            raise ValueError("reference metric history requires a bounded window")
        if self.product is ReferenceProduct.FUNDING_RATE and not historical:
            raise ValueError("funding-rate requires a bounded window")
        if self.product is ReferenceProduct.OPEN_INTEREST and historical and not self.interval:
            raise ValueError("historical open-interest requires interval")
        if self.product in {
            ReferenceProduct.MARK_INDEX_PRICE,
            ReferenceProduct.CONTRACT_METADATA,
        } and (historical or self.interval is not None):
            raise ValueError("reference snapshot product cannot declare history or interval")
        if execution_mark_snapshot and (
            self.limit != 1
            or self.page_size not in {None, 1}
            or self.max_pages != 1
            or not self.require_full_coverage
        ):
            raise ValueError(
                "execution MARK_INDEX_PRICE requires one complete snapshot row"
            )
        if self.product is ReferenceProduct.LONG_SHORT_RATIO:
            if self.long_short_kind is None:
                raise ValueError("long/short ratio requires long_short_kind")
        elif self.long_short_kind is not None:
            raise ValueError("long_short_kind only applies to long/short ratio")
        if self.product is not ReferenceProduct.MARK_INDEX_PRICE and self.mark_index_kind is not MarkIndexKind.BOTH:
            raise ValueError("mark_index_kind only applies to mark/index price")
        if self.product is ReferenceProduct.BASIS:
            selector = str(self.basis_contract_type or "").strip().upper()
            if selector not in {"PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"}:
                raise ValueError("basis requires a declared contract selector")
            if self.basis_series is BasisSeries.CONTINUOUS and selector == "PERPETUAL":
                raise ValueError("continuous basis requires a dated contract selector")
        elif self.basis_contract_type is not None or self.basis_series is not BasisSeries.NATIVE:
            raise ValueError("basis selector fields only apply to basis")
        return self


class ReferenceBatchRequest(ClosedModel):
    consumer_id: str = Field(min_length=1, max_length=200)
    requirements: list[ReferenceRequirement] = Field(min_length=1, max_length=100)
    require_all: bool = True

    @model_validator(mode="after")
    def no_duplicate_items(self):
        if len({item.consumer_grade for item in self.requirements}) != 1:
            raise ValueError("reference batch cannot mix consumer grades")
        identities = [
            (
                item.instrument_uid,
                item.product.value,
                item.interval,
                item.start_time_ns,
                item.end_time_ns,
                item.long_short_kind.value if item.long_short_kind else None,
                item.mark_index_kind.value,
                item.basis_series.value,
                item.basis_contract_type,
            )
            for item in self.requirements
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("reference batch contains duplicate requirements")
        return self


class ReferenceFieldView(ClosedModel):
    name: str = Field(min_length=1, max_length=160)
    value: DecimalValue
    unit: str = Field(min_length=1, max_length=80)


class ReferenceLineageView(ClosedModel):
    provider: str = Field(min_length=1, max_length=80)
    provider_endpoint: str = Field(min_length=1, max_length=240)
    source_role: str = Field(min_length=1, max_length=40)
    adapter_version: str = Field(min_length=1, max_length=120)
    capability_name: str = Field(min_length=1, max_length=120)
    capability_constraint: str | None = Field(default=None, max_length=240)


class ReferenceCoverageView(ClosedModel):
    requested_start_ms: int | None = Field(default=None, ge=0)
    requested_end_ms: int | None = Field(default=None, ge=0)
    observed_min_ms: int | None = Field(default=None, ge=0)
    observed_max_ms: int | None = Field(default=None, ge=0)
    complete_left: bool
    complete_right: bool
    truncated: bool
    terminal_reason: str = Field(min_length=1, max_length=160)

    @model_validator(mode="after")
    def ordered_window(self):
        if (
            self.requested_start_ms is not None
            and self.requested_end_ms is not None
            and self.requested_end_ms <= self.requested_start_ms
        ):
            raise ValueError("reference coverage request window must increase")
        if (
            self.observed_min_ms is not None
            and self.observed_max_ms is not None
            and self.observed_max_ms < self.observed_min_ms
        ):
            raise ValueError("reference coverage observation window is invalid")
        return self


class ReferenceObservationView(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    instrument_revision: int = Field(ge=1)
    product: ReferenceProduct
    observed_at_ns: int = Field(gt=0)
    fields: list[ReferenceFieldView] = Field(min_length=1)
    labels: dict[str, str] = Field(default_factory=dict)


class ReferenceDataResponse(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    product: ReferenceProduct
    status: Literal["OK", "MISSING", "UNAVAILABLE", "ERROR"]
    lineage: list[ReferenceLineageView] = Field(min_length=1)
    coverage: ReferenceCoverageView
    received_at_ns: int = Field(gt=0)
    observations: list[ReferenceObservationView] = Field(default_factory=list)
    error_code: str | None = Field(default=None, max_length=120)
    error_detail: str | None = Field(default=None, max_length=400)
    cache_hit: bool
    coalesced: bool

    @model_validator(mode="after")
    def status_matches_observations(self):
        if self.status == "OK" and not self.observations:
            raise ValueError("OK reference data requires observations")
        if self.status != "OK" and self.observations:
            raise ValueError("non-OK reference data cannot contain observations")
        if self.status in {"UNAVAILABLE", "ERROR"} and not self.error_code:
            raise ValueError("unavailable/error reference data requires error_code")
        return self


class ReferenceBatchItemResponse(ClosedModel):
    instrument_uid: str = Field(min_length=1, max_length=200)
    product: ReferenceProduct
    status: str = Field(min_length=1, max_length=80)
    data: ReferenceDataResponse | None = None
    problem: ProblemDetails | None = None


class ReferenceBatchResponse(ClosedModel):
    contract_schema: str = Field("qdl.reference.batch.v2", alias="schema")
    request_id: str = Field(min_length=1)
    partial: bool
    success_count: int = Field(ge=0)
    error_count: int = Field(ge=0)
    results: list[ReferenceBatchItemResponse]

    @model_validator(mode="after")
    def counts_match(self):
        if self.success_count + self.error_count != len(self.results):
            raise ValueError("reference batch counts do not match results")
        if self.partial != (self.error_count > 0):
            raise ValueError("reference batch partial flag does not match errors")
        return self

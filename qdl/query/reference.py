"""Reference-data query contracts bound to the V2 data requirement model.

The provider adapters deliberately accept :class:`ReferenceRequest` with an
``InstrumentRecord`` rather than a free-form exchange symbol.  This module is
the public-query bridge: it keeps that exact-record invariant while making the
same consumer manifest, source-policy and grade checks that protect BAR
warmups apply to reference data as well.

Reference data is an alpha/research input.  It is never an execution-grade
price source and it never cross-venue substitutes a missing provider product.
"""

from __future__ import annotations

from dataclasses import dataclass

from qdl.domain.instrument import InstrumentRecord
from qdl.query.contracts import ConsumerGrade, DataRequirement, FeedType
from qdl.reference.contracts import (
    BasisSeries,
    LongShortKind,
    MarkIndexKind,
    ReferenceProduct,
    ReferenceRequest,
)


_PRODUCT_FEED = {
    ReferenceProduct.FUNDING_RATE: FeedType.FUNDING_RATE,
    ReferenceProduct.OPEN_INTEREST: FeedType.OPEN_INTEREST,
    ReferenceProduct.LONG_SHORT_RATIO: FeedType.LONG_SHORT_RATIO,
    ReferenceProduct.TAKER_FLOW: FeedType.TAKER_FLOW,
    ReferenceProduct.MARK_INDEX_PRICE: FeedType.MARK_INDEX_PRICE,
    ReferenceProduct.CONTRACT_METADATA: FeedType.CONTRACT_METADATA,
    ReferenceProduct.BASIS: FeedType.BASIS,
}


def feed_for_reference_product(product: ReferenceProduct) -> FeedType:
    """Return the one canonical V2 feed identity for a reference product."""

    try:
        return _PRODUCT_FEED[ReferenceProduct(product)]
    except (KeyError, ValueError) as error:
        raise ValueError("reference product has no V2 feed mapping") from error


def _milliseconds(value_ns: int | None, field: str) -> int | None:
    if value_ns is None:
        return None
    if value_ns <= 0 or value_ns % 1_000_000:
        raise ValueError(f"{field} must be a positive whole millisecond timestamp")
    return value_ns // 1_000_000


@dataclass(frozen=True, slots=True)
class ReferenceDataRequirement:
    """One bounded reference-data request under a consumer's V2 manifest.

    Public input carries canonical ``instrument_uid`` and nanosecond times to
    remain aligned with the V2 API.  Provider adapters use milliseconds, so
    conversion is explicit and rejects lossy timestamps instead of rounding a
    window boundary silently.
    """

    instrument_uid: str
    product: ReferenceProduct
    consumer_grade: ConsumerGrade
    source_policy_id: str
    start_time_ns: int | None = None
    end_time_ns: int | None = None
    interval: str | None = None
    limit: int = 100
    page_size: int | None = None
    max_pages: int = 20
    long_short_kind: LongShortKind | None = None
    mark_index_kind: MarkIndexKind = MarkIndexKind.BOTH
    basis_series: BasisSeries = BasisSeries.NATIVE
    basis_contract_type: str | None = None
    max_freshness_ms: int | None = None
    require_full_coverage: bool = True
    deadline_ms: int = 20_000

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip() or not self.source_policy_id.strip():
            raise ValueError("reference instrument_uid and source_policy_id are required")
        if not isinstance(self.product, ReferenceProduct):
            raise TypeError("reference product must use ReferenceProduct")
        if not isinstance(self.consumer_grade, ConsumerGrade):
            raise TypeError("reference consumer_grade must use ConsumerGrade")
        if self.consumer_grade is ConsumerGrade.EXECUTION:
            raise ValueError("reference data cannot be requested as execution-grade data")
        if self.consumer_grade is ConsumerGrade.UNSPECIFIED:
            raise ValueError("reference consumer_grade cannot be UNSPECIFIED")
        if (self.start_time_ns is None) != (self.end_time_ns is None):
            raise ValueError("reference start_time_ns and end_time_ns are required together")
        start_ms = _milliseconds(self.start_time_ns, "reference start_time_ns")
        end_ms = _milliseconds(self.end_time_ns, "reference end_time_ns")
        if start_ms is not None and end_ms is not None and end_ms <= start_ms:
            raise ValueError("reference time window must increase")
        if self.interval is not None and not self.interval.strip():
            raise ValueError("reference interval cannot be blank")
        if self.max_freshness_ms is not None and self.max_freshness_ms <= 0:
            raise ValueError("reference max_freshness_ms must be positive")
        if not 100 <= self.deadline_ms <= 120_000:
            raise ValueError("reference deadline_ms must be between 100 and 120000")
        self._validate_shape()

    @property
    def feed(self) -> FeedType:
        return feed_for_reference_product(self.product)

    @property
    def data_requirement(self) -> DataRequirement:
        return DataRequirement(
            instrument_uid=self.instrument_uid,
            feed=self.feed,
            consumer_grade=self.consumer_grade,
            source_policy_id=self.source_policy_id,
            interval=self.interval,
            # Reference history has the same bounded-row cost characteristic as
            # BAR warmup.  Reuse the manifest quota rather than leaving a
            # provider pagination limit outside the access-control boundary.
            warmup_limit=self.limit,
            max_freshness_ms=self.max_freshness_ms,
            require_full_coverage=self.require_full_coverage,
            require_final_bars=False,
        )

    def to_reference_request(self, instrument: InstrumentRecord) -> ReferenceRequest:
        if instrument.instrument_uid != self.instrument_uid:
            raise ValueError("resolved reference instrument differs from the request")
        return ReferenceRequest(
            instrument=instrument,
            product=self.product,
            start_ms=_milliseconds(self.start_time_ns, "reference start_time_ns"),
            end_ms=_milliseconds(self.end_time_ns, "reference end_time_ns"),
            interval=self.interval,
            limit=self.limit,
            page_size=self.page_size,
            max_pages=self.max_pages,
            long_short_kind=self.long_short_kind,
            mark_index_kind=self.mark_index_kind,
            basis_series=self.basis_series,
            basis_contract_type=self.basis_contract_type,
        )

    def _validate_shape(self) -> None:
        """Mirror only the request-shape rules needed before catalog lookup.

        The actual :class:`ReferenceRequest` is still constructed from the
        registry record immediately before dispatch, so provider-bound identity
        is checked by the existing adapter contract.  This early validation
        prevents an invalid public batch from spending a catalog or provider
        lookup.
        """

        if not 1 <= self.limit <= 10_000:
            raise ValueError("reference request limit must be between 1 and 10000")
        if self.page_size is not None and not 1 <= self.page_size <= self.limit:
            raise ValueError("reference request page_size must be between 1 and limit")
        if not 1 <= self.max_pages <= 100:
            raise ValueError("reference request max_pages must be between 1 and 100")
        requires_interval = {
            ReferenceProduct.LONG_SHORT_RATIO,
            ReferenceProduct.TAKER_FLOW,
            ReferenceProduct.BASIS,
        }
        has_history = self.start_time_ns is not None
        if self.product in requires_interval and not str(self.interval or "").strip():
            raise ValueError(f"{self.product.value} requires a provider sampling interval")
        if self.product in requires_interval and not has_history:
            raise ValueError(f"{self.product.value} requires a bounded historical window")
        if self.product is ReferenceProduct.FUNDING_RATE and not has_history:
            raise ValueError("FUNDING_RATE requires a bounded historical window")
        if self.product is ReferenceProduct.OPEN_INTEREST and has_history and not str(self.interval or "").strip():
            raise ValueError("historical OPEN_INTEREST requires a provider sampling interval")
        if self.product in {ReferenceProduct.MARK_INDEX_PRICE, ReferenceProduct.CONTRACT_METADATA}:
            if has_history or self.interval is not None:
                raise ValueError(f"{self.product.value} is a snapshot request")
        if self.product is ReferenceProduct.LONG_SHORT_RATIO and self.long_short_kind is None:
            raise ValueError("LONG_SHORT_RATIO requires long_short_kind")
        if self.product is not ReferenceProduct.LONG_SHORT_RATIO and self.long_short_kind is not None:
            raise ValueError("long_short_kind only applies to LONG_SHORT_RATIO")
        if self.product is not ReferenceProduct.MARK_INDEX_PRICE and self.mark_index_kind is not MarkIndexKind.BOTH:
            raise ValueError("mark_index_kind only applies to MARK_INDEX_PRICE")
        if self.product is ReferenceProduct.BASIS:
            contract_type = str(self.basis_contract_type or "").strip().upper()
            if contract_type not in {"PERPETUAL", "CURRENT_QUARTER", "NEXT_QUARTER"}:
                raise ValueError("BASIS requires a declared provider contract selector")
            if self.basis_series is BasisSeries.CONTINUOUS and contract_type == "PERPETUAL":
                raise ValueError("continuous BASIS requires CURRENT_QUARTER or NEXT_QUARTER")
        elif self.basis_contract_type is not None or self.basis_series is not BasisSeries.NATIVE:
            raise ValueError("basis selector fields only apply to BASIS")


@dataclass(frozen=True, slots=True)
class ReferenceBatchRequirement:
    consumer_id: str
    requirements: tuple[ReferenceDataRequirement, ...]
    require_all: bool = True

    def __post_init__(self) -> None:
        if not self.consumer_id.strip():
            raise ValueError("reference batch consumer_id is required")
        if not 1 <= len(self.requirements) <= 100:
            raise ValueError("reference batch requires between 1 and 100 items")
        if len({item.consumer_grade for item in self.requirements}) != 1:
            raise ValueError("reference batch cannot mix consumer grades")
        keys = [
            (
                item.instrument_uid,
                item.product.value,
                item.interval,
                item.start_time_ns,
                item.end_time_ns,
                item.long_short_kind.value if item.long_short_kind else None,
                item.mark_index_kind.value,
                item.basis_series.value,
                str(item.basis_contract_type or "").strip().upper() or None,
            )
            for item in self.requirements
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("reference batch contains duplicate requirements")

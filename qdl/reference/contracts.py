from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from qdl.domain.capabilities import FeedCapability
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import InstrumentRecord, ProductType


class ReferenceProduct(str, Enum):
    FUNDING_RATE = "FUNDING_RATE"
    OPEN_INTEREST = "OPEN_INTEREST"
    LONG_SHORT_RATIO = "LONG_SHORT_RATIO"
    TAKER_FLOW = "TAKER_FLOW"
    MARK_INDEX_PRICE = "MARK_INDEX_PRICE"
    CONTRACT_METADATA = "CONTRACT_METADATA"
    BASIS = "BASIS"


class ReferenceStatus(str, Enum):
    OK = "OK"
    MISSING = "MISSING"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class LongShortKind(str, Enum):
    GLOBAL_ACCOUNT = "GLOBAL_ACCOUNT"
    TOP_ACCOUNT = "TOP_ACCOUNT"
    TOP_POSITION = "TOP_POSITION"


class MarkIndexKind(str, Enum):
    MARK = "MARK"
    INDEX = "INDEX"
    BOTH = "BOTH"


class BasisSeries(str, Enum):
    NATIVE = "NATIVE"
    CONTINUOUS = "CONTINUOUS"


class ReferenceProviderError(RuntimeError):
    """A provider reply cannot safely be interpreted as this request's data."""


class ReferenceProviderExhausted(RuntimeError):
    """A bounded retry policy was exhausted without a trustworthy reply."""

    def __init__(self, detail: str, *, retry_after_ms: int | None = None) -> None:
        super().__init__(detail)
        if retry_after_ms is not None and retry_after_ms < 0:
            raise ValueError("retry_after_ms must be non-negative")
        self.retry_after_ms = retry_after_ms


class ReferenceProviderRateLimited(ReferenceProviderExhausted):
    """A documented provider rate-limit signal reached a bounded adapter edge."""

    def __init__(
        self,
        detail: str,
        *,
        http_status: int | None,
        provider_code: int | None,
        retry_after_ms: int | None = None,
    ) -> None:
        if http_status not in {None, 418, 429} or provider_code not in {None, -1003}:
            raise ValueError("reference provider rate-limit signal is not recognized")
        if http_status is None and provider_code is None:
            raise ValueError("reference provider rate-limit signal is empty")
        super().__init__(detail, retry_after_ms=retry_after_ms)
        self.http_status = http_status
        self.provider_code = provider_code


class ReferenceUnavailable(RuntimeError):
    """A truthful capability/product combination has no provider result."""


@dataclass(frozen=True, slots=True)
class ReferenceField:
    """One present provider number with an explicit, non-coerced unit."""

    name: str
    value: CanonicalDecimal
    unit: str

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.unit.strip():
            raise ValueError("reference field name and unit are required")


@dataclass(frozen=True, slots=True)
class ReferenceLineage:
    provider: str
    provider_endpoint: str
    source_role: str
    adapter_version: str
    capability_name: str
    capability_constraint: str | None = None

    def __post_init__(self) -> None:
        if any(
            not value.strip()
            for value in (
                self.provider,
                self.provider_endpoint,
                self.source_role,
                self.adapter_version,
                self.capability_name,
            )
        ):
            raise ValueError("reference lineage identity is incomplete")


@dataclass(frozen=True, slots=True)
class ReferenceObservation:
    instrument_uid: str
    instrument_revision: int
    product: ReferenceProduct
    observed_at_ns: int
    fields: tuple[ReferenceField, ...]
    labels: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.instrument_uid.strip() or self.instrument_revision < 1:
            raise ValueError("reference observation instrument identity is invalid")
        if self.observed_at_ns <= 0:
            raise ValueError("reference observation time must be positive")
        if not self.fields:
            raise ValueError("reference observations require at least one present field")
        if len({field.name for field in self.fields}) != len(self.fields):
            raise ValueError("reference observation has duplicate field names")
        label_names = [name for name, _ in self.labels]
        if len(set(label_names)) != len(label_names):
            raise ValueError("reference observation has duplicate labels")
        if any(not name.strip() or not value.strip() for name, value in self.labels):
            raise ValueError("reference observation labels must be nonblank")


@dataclass(frozen=True, slots=True)
class ReferenceCoverage:
    requested_start_ms: int | None
    requested_end_ms: int | None
    observed_min_ms: int | None
    observed_max_ms: int | None
    complete_left: bool
    complete_right: bool
    truncated: bool
    terminal_reason: str

    def __post_init__(self) -> None:
        if self.requested_start_ms is not None and self.requested_start_ms < 0:
            raise ValueError("reference coverage start must be non-negative")
        if self.requested_end_ms is not None and self.requested_end_ms < 0:
            raise ValueError("reference coverage end must be non-negative")
        if (
            self.requested_start_ms is not None
            and self.requested_end_ms is not None
            and self.requested_end_ms <= self.requested_start_ms
        ):
            raise ValueError("reference coverage window must increase")
        if not self.terminal_reason.strip():
            raise ValueError("reference coverage terminal reason is required")


@dataclass(frozen=True, slots=True)
class ReferenceRequest:
    """A bounded request anchored to one versioned registry record.

    A request never accepts a free-form symbol. That prevents one batch caller
    from accidentally receiving a similarly named provider instrument after a
    dated-contract rollover or metadata revision.
    """

    instrument: InstrumentRecord
    product: ReferenceProduct
    start_ms: int | None = None
    end_ms: int | None = None
    interval: str | None = None
    limit: int = 100
    page_size: int | None = None
    max_pages: int = 20
    long_short_kind: LongShortKind | None = None
    mark_index_kind: MarkIndexKind = MarkIndexKind.BOTH
    basis_series: BasisSeries = BasisSeries.NATIVE
    basis_contract_type: str | None = None

    def __post_init__(self) -> None:
        if self.limit < 1 or self.limit > 10_000:
            raise ValueError("reference request limit must be between 1 and 10000")
        if self.page_size is not None and not 1 <= self.page_size <= self.limit:
            raise ValueError("reference request page_size must be between 1 and limit")
        if not 1 <= self.max_pages <= 100:
            raise ValueError("reference request max_pages must be between 1 and 100")
        if (self.start_ms is None) != (self.end_ms is None):
            raise ValueError("reference request start_ms and end_ms are required together")
        if self.start_ms is not None and (self.start_ms < 0 or self.end_ms <= self.start_ms):
            raise ValueError("reference request history window must increase from a non-negative start")

        requires_interval = {
            ReferenceProduct.LONG_SHORT_RATIO,
            ReferenceProduct.TAKER_FLOW,
            ReferenceProduct.BASIS,
        }
        if self.product in requires_interval and not str(self.interval or "").strip():
            raise ValueError(f"{self.product.value} requires a provider sampling interval")
        if self.product in requires_interval and self.start_ms is None:
            raise ValueError(f"{self.product.value} requires a bounded historical window")
        if self.product is ReferenceProduct.FUNDING_RATE and self.start_ms is None:
            raise ValueError("FUNDING_RATE requires a bounded historical window")
        if self.product is ReferenceProduct.OPEN_INTEREST and self.start_ms is not None:
            if not str(self.interval or "").strip():
                raise ValueError("historical OPEN_INTEREST requires a provider sampling interval")
        if self.product in {ReferenceProduct.MARK_INDEX_PRICE, ReferenceProduct.CONTRACT_METADATA}:
            if self.start_ms is not None or self.interval is not None:
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

    @property
    def provider_key(self) -> tuple[str, str]:
        return (self.instrument.identity.venue, self.instrument.identity.market)

    @property
    def is_history(self) -> bool:
        return self.start_ms is not None

    @property
    def cache_key(self) -> tuple[object, ...]:
        return (
            self.provider_key,
            self.instrument.instrument_uid,
            self.instrument.metadata_revision,
            self.instrument.native_symbol,
            self.product.value,
            self.start_ms,
            self.end_ms,
            self.interval,
            self.limit,
            self.page_size,
            self.max_pages,
            self.long_short_kind.value if self.long_short_kind else None,
            self.mark_index_kind.value,
            self.basis_series.value,
            str(self.basis_contract_type or "").upper() or None,
        )


@dataclass(frozen=True, slots=True)
class ReferenceFetch:
    observations: tuple[ReferenceObservation, ...]
    lineage: tuple[ReferenceLineage, ...]
    coverage: ReferenceCoverage

    def __post_init__(self) -> None:
        if not self.lineage:
            raise ValueError("reference fetch requires lineage")


@dataclass(frozen=True, slots=True)
class ReferenceBatchResult:
    request: ReferenceRequest
    status: ReferenceStatus
    capability: FeedCapability
    lineage: tuple[ReferenceLineage, ...]
    coverage: ReferenceCoverage
    received_at_ns: int
    observations: tuple[ReferenceObservation, ...] = ()
    error_code: str | None = None
    error_detail: str | None = None
    retry_after_ms: int | None = None
    cache_hit: bool = False
    coalesced: bool = False

    def __post_init__(self) -> None:
        if self.received_at_ns <= 0:
            raise ValueError("reference result received time must be positive")
        if not self.lineage:
            raise ValueError("reference result requires lineage")
        if self.status is ReferenceStatus.OK and not self.observations:
            raise ValueError("OK reference result requires observations")
        if self.status is not ReferenceStatus.OK and self.observations:
            raise ValueError("non-OK reference result cannot carry observations")
        if self.status is ReferenceStatus.ERROR and not self.error_code:
            raise ValueError("ERROR reference result requires error_code")
        if self.status is ReferenceStatus.UNAVAILABLE and not self.error_code:
            raise ValueError("UNAVAILABLE reference result requires error_code")
        if self.status is ReferenceStatus.MISSING and self.error_code is not None:
            raise ValueError("MISSING reference result cannot claim an error")
        if self.retry_after_ms is not None and self.retry_after_ms < 0:
            raise ValueError("reference retry_after_ms must be non-negative")


def decimal_field(name: str, value: object, unit: str) -> ReferenceField | None:
    """Return a field only when the provider actually supplied a numeric value."""

    if value is None:
        return None
    source_text = str(value).strip()
    if not source_text:
        return None
    try:
        decimal = CanonicalDecimal.from_text(source_text)
    except ValueError as error:
        raise ReferenceProviderError(
            f"reference field {name} has invalid provider decimal text"
        ) from error
    return ReferenceField(name=name, value=decimal, unit=unit)


def product_feed_name(product: ReferenceProduct) -> str:
    return {
        ReferenceProduct.FUNDING_RATE: "funding_rate",
        ReferenceProduct.OPEN_INTEREST: "open_interest",
        ReferenceProduct.LONG_SHORT_RATIO: "long_short_ratio",
        ReferenceProduct.TAKER_FLOW: "taker_flow",
        ReferenceProduct.MARK_INDEX_PRICE: "mark_index_price",
        ReferenceProduct.CONTRACT_METADATA: "contract_metadata",
        ReferenceProduct.BASIS: "basis",
    }[product]


def provider_lineage(
    *,
    provider: str,
    endpoint: str,
    capability_name: str,
    capability: FeedCapability,
    adapter_version: str,
) -> ReferenceLineage:
    return ReferenceLineage(
        provider=provider,
        provider_endpoint=endpoint,
        source_role="REFERENCE",
        adapter_version=adapter_version,
        capability_name=capability_name,
        capability_constraint=capability.constraint,
    )


def require_product(record: InstrumentRecord, *products: ProductType) -> None:
    if record.identity.product_type not in products:
        names = ", ".join(product.value for product in products)
        raise ReferenceUnavailable(
            f"{record.instrument_id} product type {record.identity.product_type.value} is not valid for {names}"
        )

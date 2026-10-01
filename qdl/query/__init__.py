"""Provider-neutral query contracts shared by REST, gRPC and SDK surfaces."""

from pathlib import Path
from pkgutil import extend_path


__path__ = extend_path(__path__, __name__)
_generated_package = (
    Path(__file__).resolve().parents[2] / "generated" / "python" / "qdl" / "query"
)
if _generated_package.is_dir():
    __path__.append(str(_generated_package))

from qdl.query.contracts import (
    BarRevisionPolicy,
    BatchRequirement,
    CanonicalErrorCode,
    ConsumerGrade,
    CoverageStatus,
    DataRequirement,
    EXECUTION_PRICE_VALIDATION_FEEDS,
    FeedType,
    GapPolicy,
    METRIC_INTERVAL_FEEDS,
    OPTIONAL_INTERVAL_FEEDS,
    QueryProblem,
    RecoveryPolicy,
    StalePolicy,
    STALE_REASON_EVENT_AGE,
    STALE_REASON_SESSION_LIVENESS,
    STALE_REASON_SESSION_STATE,
    STALE_REASONS,
    evaluate_requirement,
)
from qdl.query.entitlement import (
    AccessDecision,
    AccessPurpose,
    DataProduct,
    EntitlementGrant,
    EntitlementPolicy,
)
from qdl.query.lifecycle import BarLifecycle, DeliveryMode, delivery_mode
from qdl.query.reference import (
    ReferenceBatchRequirement,
    ReferenceDataRequirement,
    feed_for_reference_product,
)
from qdl.query.results import (
    ContractMetadata,
    GapRecord,
    HistoryResult,
    InstrumentPage,
    InstrumentQuery,
    MarketDataItem,
    MarketDataQueryBackend,
    MemoryMarketDataBackend,
    QualityMetadata,
    QueryBackendError,
    SourceMetadata,
)
from qdl.warmup.contracts import (
    IntervalSourcePolicy,
    WarmupSpecification,
    WarmupTimeRange,
)

__all__ = [
    "AccessDecision",
    "AccessPurpose",
    "BarLifecycle",
    "BarRevisionPolicy",
    "BatchRequirement",
    "CanonicalErrorCode",
    "ConsumerGrade",
    "ContractMetadata",
    "CoverageStatus",
    "DataProduct",
    "DataRequirement",
    "DeliveryMode",
    "EXECUTION_PRICE_VALIDATION_FEEDS",
    "EntitlementGrant",
    "EntitlementPolicy",
    "FeedType",
    "GapPolicy",
    "METRIC_INTERVAL_FEEDS",
    "OPTIONAL_INTERVAL_FEEDS",
    "QueryProblem",
    "ReferenceBatchRequirement",
    "ReferenceDataRequirement",
    "RecoveryPolicy",
    "StalePolicy",
    "delivery_mode",
    "STALE_REASONS",
    "STALE_REASON_EVENT_AGE",
    "STALE_REASON_SESSION_LIVENESS",
    "STALE_REASON_SESSION_STATE",
    "evaluate_requirement",
    "feed_for_reference_product",
    "HistoryResult",
    "GapRecord",
    "InstrumentPage",
    "InstrumentQuery",
    "MarketDataItem",
    "MarketDataQueryBackend",
    "MemoryMarketDataBackend",
    "QualityMetadata",
    "QueryBackendError",
    "SourceMetadata",
    "BatchItemResult",
    "BatchQueryResult",
    "QueryResult",
    "QueryServiceError",
    "ReferenceBatchItemResult",
    "ReferenceBatchQueryResult",
    "ReadinessItemResult",
    "ReadinessResult",
    "V2QueryService",
    "WarmupResult",
    "IntervalSourcePolicy",
    "WarmupSpecification",
    "WarmupTimeRange",
]


# SDK contracts also import this package; service loading must not create a cycle.
_SERVICE_EXPORTS = frozenset({
    "BatchItemResult", "BatchQueryResult", "QueryResult", "QueryServiceError",
    "ReferenceBatchItemResult", "ReferenceBatchQueryResult", "ReadinessItemResult",
    "ReadinessResult", "V2QueryService", "WarmupResult",
})


def __getattr__(name):
    if name not in _SERVICE_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module("qdl.query.service"), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | _SERVICE_EXPORTS)

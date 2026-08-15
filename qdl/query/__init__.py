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
    FeedType,
    GapPolicy,
    QueryProblem,
    RecoveryPolicy,
    StalePolicy,
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
    SourceMetadata,
)
from qdl.query.service import (
    BatchItemResult,
    BatchQueryResult,
    QueryResult,
    QueryServiceError,
    ReadinessItemResult,
    ReadinessResult,
    V2QueryService,
    WarmupResult,
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
    "EntitlementGrant",
    "EntitlementPolicy",
    "FeedType",
    "GapPolicy",
    "QueryProblem",
    "RecoveryPolicy",
    "StalePolicy",
    "delivery_mode",
    "evaluate_requirement",
    "HistoryResult",
    "GapRecord",
    "InstrumentPage",
    "InstrumentQuery",
    "MarketDataItem",
    "MarketDataQueryBackend",
    "MemoryMarketDataBackend",
    "QualityMetadata",
    "SourceMetadata",
    "BatchItemResult",
    "BatchQueryResult",
    "QueryResult",
    "QueryServiceError",
    "ReadinessItemResult",
    "ReadinessResult",
    "V2QueryService",
    "WarmupResult",
]

"""Universal demand control-plane contracts and topology planning."""

from pathlib import Path
from pkgutil import extend_path


__path__ = extend_path(__path__, __name__)
_generated_package = (
    Path(__file__).resolve().parents[2] / "generated" / "python" / "qdl" / "demand"
)
if _generated_package.is_dir():
    __path__.append(str(_generated_package))

from qdl.demand.contracts import (
    CapabilityAvailability,
    DataRequirement,
    DemandFeed,
    DemandLease,
    DemandPurpose,
    DemandState,
    DemandTransition,
    FeedCapability,
    demand_transition_allowed,
    ResolvedRequirement,
    UniverseSelector,
    UniverseSelectorKind,
)
from qdl.demand.resolver import (
    CapabilityRegistry,
    DemandLeaseRegistry,
    DemandManifest,
    DemandResolver,
    UniverseDefinition,
    UniverseMember,
    UniverseRegistry,
)
from qdl.demand.topology import (
    DemandTopology,
    DemandTopologyPlanner,
    SubscriptionAction,
    SubscriptionActionKind,
)
from qdl.demand.inventory import (
    ActiveDemandConvergence,
    ActiveDemandCompiler,
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    AdmissionBudget,
    AdmissionBudgetUsage,
    AdmissionPolicy,
    DemandInventoryReadiness,
    InventoryError,
    ProviderAdmission,
    admit_provider_metadata,
    converge_active_demand,
    parse_provider_metadata,
    source_requirement_for_admission,
)
from qdl.demand.liquid_crypto import (
    LiquidCryptoFeaturePolicy,
    LiquidCryptoFeatureSet,
    build_l2_feature_requirements,
    reference_feeds_for,
    select_liquid_crypto_feature_set,
)

__all__ = [
    "CapabilityAvailability",
    "DataRequirement",
    "DemandFeed",
    "DemandLease",
    "DemandPurpose",
    "DemandState",
    "DemandTransition",
    "FeedCapability",
    "demand_transition_allowed",
    "ResolvedRequirement",
    "UniverseSelector",
    "UniverseSelectorKind",
    "CapabilityRegistry",
    "DemandLeaseRegistry",
    "DemandManifest",
    "DemandResolver",
    "UniverseDefinition",
    "UniverseMember",
    "UniverseRegistry",
    "DemandTopology",
    "DemandTopologyPlanner",
    "SubscriptionAction",
    "SubscriptionActionKind",
    "ActiveDemandCompiler",
    "ActiveDemandInventory",
    "ActiveDemandSourceRegistry",
    "ActiveDemandConvergence",
    "AdmissionBudget",
    "AdmissionBudgetUsage",
    "AdmissionPolicy",
    "DemandInventoryReadiness",
    "InventoryError",
    "ProviderAdmission",
    "admit_provider_metadata",
    "converge_active_demand",
    "parse_provider_metadata",
    "source_requirement_for_admission",
    "LiquidCryptoFeaturePolicy",
    "LiquidCryptoFeatureSet",
    "build_l2_feature_requirements",
    "reference_feeds_for",
    "select_liquid_crypto_feature_set",
]

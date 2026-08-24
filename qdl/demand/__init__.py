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
]

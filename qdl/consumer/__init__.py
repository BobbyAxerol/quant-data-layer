"""Audited V2 consumer requirements and controlled migration state."""

from typing import TYPE_CHECKING, Any

from qdl.consumer.stable import (
    StableConsumerMigration,
    StableConsumerMigrationPlan,
    StablePrimaryConsumerRoute,
    StablePrimaryConsumerRoutePlan,
    primary_fallback_return_drill,
)
from qdl.consumer.manifest import (
    ConsumerManifest,
    ConsumerManifestLoader,
    ConsumerManifestRegistry,
    ConsumerMigration,
    ConsumerMigrationRegistry,
    ConsumerQuotas,
    ConsumerRoute,
    MigrationState,
    UsageTelemetry,
)
from qdl.consumer.realtime_route import (
    RealtimeRoute,
    RealtimeRouteDecision,
    RealtimeSliceHealth,
    RealtimeSliceObservation,
    evaluate_realtime_health,
    requirement_key,
    select_realtime_route,
)
from qdl.consumer.release import (
    ReleaseReadinessSummary,
    ReleaseResourceBudget,
    ReleaseRouteObservation,
    StableReleaseConsumerRoute,
    StableReleaseProductRoute,
    StableReleaseRoutePlan,
    V1FallbackReference,
    evaluate_release_readiness,
    is_explicit_v1_exclusion,
)
from qdl.consumer.universal_release import (
    ConsumerRouteBinding,
    ConsumerClassRule,
    FallbackRule,
    UniversalConsumerClass,
    UniversalReleaseCoverage,
    UniversalReleaseExclusion,
    UniversalReleaseManifest,
    UniversalReleasePolicy,
    UniversalReleaseProduct,
    UniversalResourceBudget,
    UniversalV1Rollback,
    build_universal_release_manifest,
)

if TYPE_CHECKING:
    from qdl.consumer.shadow import ManifestShadowConsumer, ShadowObservation

__all__ = [
    "ConsumerManifest",
    "ConsumerManifestLoader",
    "ConsumerManifestRegistry",
    "ConsumerMigration",
    "ConsumerMigrationRegistry",
    "ConsumerQuotas",
    "ConsumerRoute",
    "MigrationState",
    "StableConsumerMigration",
    "StableConsumerMigrationPlan",
    "StablePrimaryConsumerRoute",
    "StablePrimaryConsumerRoutePlan",
    "primary_fallback_return_drill",
    "ManifestShadowConsumer",
    "ShadowObservation",
    "UsageTelemetry",
    "RealtimeRoute",
    "RealtimeRouteDecision",
    "RealtimeSliceHealth",
    "RealtimeSliceObservation",
    "evaluate_realtime_health",
    "requirement_key",
    "select_realtime_route",
    "ReleaseReadinessSummary",
    "ReleaseResourceBudget",
    "ReleaseRouteObservation",
    "StableReleaseConsumerRoute",
    "StableReleaseProductRoute",
    "StableReleaseRoutePlan",
    "V1FallbackReference",
    "evaluate_release_readiness",
    "is_explicit_v1_exclusion",
    "ConsumerRouteBinding",
    "ConsumerClassRule",
    "FallbackRule",
    "UniversalConsumerClass",
    "UniversalReleaseCoverage",
    "UniversalReleaseExclusion",
    "UniversalReleaseManifest",
    "UniversalReleasePolicy",
    "UniversalReleaseProduct",
    "UniversalResourceBudget",
    "UniversalV1Rollback",
    "build_universal_release_manifest",
]


def __getattr__(name: str) -> Any:
    if name in {"ManifestShadowConsumer", "ShadowObservation"}:
        from qdl.consumer.shadow import ManifestShadowConsumer, ShadowObservation

        value = {
            "ManifestShadowConsumer": ManifestShadowConsumer,
            "ShadowObservation": ShadowObservation,
        }[name]
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

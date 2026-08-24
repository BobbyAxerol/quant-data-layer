"""Audited V2 consumer requirements and controlled migration state."""

from typing import TYPE_CHECKING, Any

from qdl.consumer.stable import (
    StableConsumerMigration,
    StableConsumerMigrationPlan,
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

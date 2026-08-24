from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable

from qdl.demand import (
    DemandLeaseRegistry,
    DemandManifest,
    DemandResolver,
    DemandState,
    DemandTopology,
    DemandTopologyPlanner,
    ResolvedRequirement,
    UniverseRegistry,
)
from qdl.runtime.stable_catalog import StableSourceCatalog


@dataclass(frozen=True, slots=True)
class DemandedSliceReadiness:
    requirement_id: str
    instrument_uid: str
    native_symbol: str
    feed: str
    interval: str | None
    state: DemandState
    capability_available: bool
    provisioned: bool
    execution_eligible: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class UniversalDemandRuntimePlan:
    schema: str
    demand_revision: int
    manifest_sha256: str
    universe_registry_sha256: str
    catalog_revision: int
    resolved: tuple[ResolvedRequirement, ...]
    topology: DemandTopology
    readiness: tuple[DemandedSliceReadiness, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.v2.universal-demand-runtime-plan.v1":
            raise ValueError("unsupported universal demand runtime plan schema")
        if min(self.demand_revision, self.catalog_revision) < 1:
            raise ValueError("universal runtime revisions must be positive")
        expected = {item.requirement_id for item in self.resolved}
        observed = {item.requirement_id for item in self.readiness}
        if expected != observed:
            raise ValueError("universal runtime readiness differs from resolved demand")
        if self.topology.demand_revision != self.demand_revision:
            raise ValueError("universal runtime topology revision differs from demand")

    @property
    def ready(self) -> bool:
        return all(
            item.state in {DemandState.LIVE, DemandState.MARKET_CLOSED}
            for item in self.readiness
        )

    @property
    def execution_ready(self) -> bool:
        execution_items = [
            item
            for item, resolved in zip(self.readiness, self.resolved, strict=True)
            if resolved.requirement.execution_grade
        ]
        return bool(execution_items) and all(item.execution_eligible for item in execution_items)

    def canonical_payload(self) -> dict:
        return {
            "schema": self.schema,
            "demand_revision": self.demand_revision,
            "manifest_sha256": self.manifest_sha256,
            "universe_registry_sha256": self.universe_registry_sha256,
            "catalog_revision": self.catalog_revision,
            "resolved": [
                {
                    "requirement_id": item.requirement_id,
                    "instrument_uid": item.instrument_uid,
                    "instrument_id": item.instrument_id,
                    "native_symbol": item.native_symbol,
                    "feed": item.requirement.feed.value,
                    "interval": item.requirement.interval,
                    "binding_id": item.binding_id,
                    "state": item.state.value,
                    "provisioned": item.provisioned,
                    "capability_id": item.capability.capability_id,
                    "consumer_ids": list(item.consumer_ids),
                    "effective_priority": item.effective_priority,
                }
                for item in self.resolved
            ],
            "topology": {
                "subscription_count": len(self.topology.subscriptions),
                "connection_count": self.topology.connection_count,
                "service_role_count": self.topology.service_role_count,
                "runtime_roles": [list(item) for item in self.topology.runtime_roles],
                "core_binding_ids": list(self.topology.core_binding_ids),
                "provisioning_required": list(self.topology.provisioning_required),
                "shards": [
                    {
                        "shard_id": shard.shard_id,
                        "venue": shard.venue,
                        "market": shard.market,
                        "feed": shard.feed.value,
                        "subscription_count": len(shard.subscriptions),
                    }
                    for shard in self.topology.shards
                ],
            },
            "readiness": [
                {
                    **asdict(item),
                    "state": item.state.value,
                }
                for item in self.readiness
            ],
        }

    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def write(self, path: str | Path) -> str:
        target = Path(path)
        if target.exists():
            raise FileExistsError("universal runtime plan output already exists")
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = self.canonical_payload() | {"sha256": self.digest()}
        target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        target.chmod(0o640)
        return str(target)


class UniversalDemandPlanner:
    """Compiles a manifest into a dynamic, non-container-per-symbol plan."""

    def __init__(
        self,
        *,
        catalog: StableSourceCatalog,
        universes: UniverseRegistry,
        max_subscriptions_per_connection: int,
    ) -> None:
        self.catalog = catalog
        self.universes = universes
        self.resolver = DemandResolver(catalog=catalog, universes=universes)
        self.topology = DemandTopologyPlanner(
            max_subscriptions_per_connection=max_subscriptions_per_connection
        )

    def compile(self, manifest: DemandManifest) -> UniversalDemandRuntimePlan:
        resolved = self.resolver.resolve_manifest(manifest)
        registry = DemandLeaseRegistry(clock_ns=lambda: 1_000_000_000)
        by_owner: dict[str, list[ResolvedRequirement]] = {}
        for item in resolved:
            for owner in item.consumer_ids:
                by_owner.setdefault(owner, []).append(item)
        for owner, items in sorted(by_owner.items()):
            registry.renew(owner, items, now_ns=1_000_000_000)
        active = registry.desired(now_ns=1_000_000_001)
        topology = self.topology.build(active, demand_revision=manifest.revision)
        readiness = tuple(self._readiness(item) for item in active)
        return UniversalDemandRuntimePlan(
            schema="qdl.v2.universal-demand-runtime-plan.v1",
            demand_revision=manifest.revision,
            manifest_sha256=manifest.sha256,
            universe_registry_sha256=self.universes.sha256,
            catalog_revision=self.catalog.catalog_revision,
            resolved=active,
            topology=topology,
            readiness=readiness,
        )

    @staticmethod
    def _readiness(item: ResolvedRequirement) -> DemandedSliceReadiness:
        state = item.state
        reason = None
        if state is DemandState.UNSUPPORTED:
            reason = item.capability.constraint or "capability is not available"
        elif not item.provisioned:
            reason = "capability is available but this catalog revision has no active binding"
        eligible = (
            item.requirement.execution_grade
            and item.capability.enabled
            and item.provisioned
            and state is DemandState.LIVE
        )
        return DemandedSliceReadiness(
            requirement_id=item.requirement_id,
            instrument_uid=item.instrument_uid,
            native_symbol=item.native_symbol,
            feed=item.requirement.feed.value,
            interval=item.requirement.interval,
            state=state,
            capability_available=item.capability.enabled,
            provisioned=item.provisioned,
            execution_eligible=eligible,
            reason=reason,
        )

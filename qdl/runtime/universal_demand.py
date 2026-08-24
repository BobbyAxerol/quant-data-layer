from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping

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
from qdl.warmup.planner import UniversalWarmupPlanner, WarmupPlan


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
class RealtimeDemandObservation:
    """Runtime facts for exactly one resolved demand slice.

    The observation intentionally carries only bounded operational metadata,
    not provider payload bytes. A broad-universe issue cannot contaminate a
    different requirement because callers must key observations by the
    resolved requirement ID.
    """

    available: bool
    source_age_ms: int | None
    receive_age_ms: int | None
    gap_count: int
    reconnect_count: int
    final_bar_available: bool = True
    market_closed: bool = False

    def __post_init__(self) -> None:
        for name, value in (
            ("source_age_ms", self.source_age_ms),
            ("receive_age_ms", self.receive_age_ms),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.gap_count < 0 or self.reconnect_count < 0:
            raise ValueError("gap_count and reconnect_count must be non-negative")
        if self.market_closed and self.available:
            raise ValueError("market-closed demand cannot claim live availability")


@dataclass(frozen=True, slots=True)
class ObservedDemandedSliceReadiness:
    requirement_id: str
    state: str
    execution_eligible: bool
    reason: str | None
    source_age_ms: int | None
    receive_age_ms: int | None
    gap_count: int
    reconnect_count: int


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

    def assess_realtime(
        self,
        observations: Mapping[str, RealtimeDemandObservation],
    ) -> tuple[ObservedDemandedSliceReadiness, ...]:
        """Evaluate health per resolved slice without broad-universe coupling."""
        expected = {item.requirement_id for item in self.resolved}
        if set(observations) != expected:
            raise ValueError("realtime observations differ from resolved demand")
        static = {item.requirement_id: item for item in self.readiness}
        result = []
        for resolved in self.resolved:
            requirement_id = resolved.requirement_id
            observation = observations[requirement_id]
            baseline = static[requirement_id]
            requirement = resolved.requirement
            state = "READY"
            reason = None
            if resolved.state is not DemandState.LIVE:
                state = resolved.state.value
                reason = baseline.reason or "demand is not live"
            elif observation.market_closed:
                state = "MARKET_CLOSED"
                reason = "market session is closed for this demanded slice"
            elif not observation.available:
                state = "NOT_READY"
                reason = "canonical source is not available for this demanded slice"
            elif requirement.require_final_bars and not observation.final_bar_available:
                state = "NOT_READY"
                reason = "latest BAR is not final for this demanded slice"
            elif observation.gap_count:
                state = "GAP"
                reason = "canonical source has an unresolved gap for this demanded slice"
            elif observation.source_age_ms is None:
                state = "STALE"
                reason = "canonical source age is unavailable"
            elif (
                requirement.max_freshness_ms is not None
                and observation.source_age_ms > requirement.max_freshness_ms
            ):
                state = "STALE"
                reason = "canonical source exceeds demanded freshness"
            result.append(
                ObservedDemandedSliceReadiness(
                    requirement_id=requirement_id,
                    state=state,
                    execution_eligible=(
                        baseline.execution_eligible and state == "READY"
                    ),
                    reason=reason,
                    source_age_ms=observation.source_age_ms,
                    receive_age_ms=observation.receive_age_ms,
                    gap_count=observation.gap_count,
                    reconnect_count=observation.reconnect_count,
                )
            )
        return tuple(result)

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
        self.warmup = UniversalWarmupPlanner(session_calendar_ids={
            item.instrument_uid: item.session_calendar_id
            for item in catalog.instruments
        })

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

    def compile_warmup(
        self,
        plan: UniversalDemandRuntimePlan,
        *,
        generated_at_ns: int,
    ) -> WarmupPlan:
        """Compile the active resolved BAR demand without changing topology IDs."""
        if plan.catalog_revision != self.catalog.catalog_revision:
            raise ValueError("warmup plan catalog revision differs from runtime plan")
        return self.warmup.compile(
            plan.resolved,
            generated_at_ns=generated_at_ns,
            demand_revision=plan.demand_revision,
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

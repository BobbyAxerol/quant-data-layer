"""Projection from admitted demand to one shared realtime runtime plan.

The Phase 11 control plane owns selector resolution, capability admission and
sharding.  This module deliberately does not rediscover symbols, invent a
second catalog, or create a role per alpha/symbol.  It turns only admitted,
selected TRADE/QUOTE/BAR/BOOK slices into the existing stable catalog/acquisition
contracts used by the Rust canonical core and provider edges.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from qdl.demand import (
    ActiveDemandConvergence,
    ActiveDemandInventory,
    DemandFeed,
    DemandPurpose,
    DemandState,
    InventoryError,
    ProviderAdmission,
    source_requirement_for_admission,
)
from qdl.ingestion.contracts import FeedType as IngestionFeedType, plan_shards
from qdl.query import ConsumerGrade, FeedType
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionCatalogBundle,
    ProductionDemand,
    ProductionDemandManifest,
)
from qdl.demand.topology import DemandTopology


_BOOK_FEEDS = frozenset({DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})
_REALTIME_FEEDS = frozenset({DemandFeed.TRADE, DemandFeed.QUOTE, DemandFeed.BAR, *_BOOK_FEEDS})
_PURPOSE_TO_GRADE = {
    DemandPurpose.EXECUTION: ConsumerGrade.EXECUTION,
    DemandPurpose.ALPHA: ConsumerGrade.ALPHA,
    DemandPurpose.RESEARCH: ConsumerGrade.RESEARCH,
    DemandPurpose.OBSERVABILITY: ConsumerGrade.RESEARCH,
}
_INGESTION_FEED = {
    DemandFeed.TRADE: IngestionFeedType.TRADE,
    DemandFeed.QUOTE: IngestionFeedType.BBO,
    DemandFeed.BAR: IngestionFeedType.BAR,
    DemandFeed.BOOK_SNAPSHOT: IngestionFeedType.BOOK,
    DemandFeed.BOOK_DELTA: IngestionFeedType.BOOK,
}
_GRADE_RANK = {
    ConsumerGrade.EXECUTION: 0,
    ConsumerGrade.ALPHA: 1,
    ConsumerGrade.RESEARCH: 2,
}


@dataclass(frozen=True, slots=True)
class UniversalRealtimePlan:
    """One dark catalog/acquisition projection for all admitted realtime demand.

    ``topology`` contains admitted native TRADE/BBO/BOOK subscriptions and,
    where a provider has passed final-bar admission, native final BAR
    subscriptions. A shared REST edge still owns historical bootstrap and
    recovery for every BAR, while it is the recurring final source only for
    ``PYTHON_REST`` BAR bindings. Snapshot/delta book aliases coalesce into one
    physical provider subscription; reference remains bounded on-demand data.
    """

    schema: str
    inventory_sha256: str
    demand: ProductionDemandManifest
    bundle: ProductionCatalogBundle
    topology: DemandTopology
    owners_by_binding: Mapping[str, tuple[str, ...]]
    final_bar_binding_ids: tuple[str, ...]
    final_bar_runtime_roles: tuple[tuple[str, str], ...]
    deferred_requirement_ids: tuple[str, ...]
    unsupported_requirement_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.v2.universal-realtime-plan.v1":
            raise ValueError("unsupported universal realtime plan schema")
        if len(self.inventory_sha256) != 64:
            raise ValueError("universal realtime inventory digest is invalid")
        binding_ids = {
            item["binding_id"] for item in self.bundle.source_catalog["bindings"]
        }
        if binding_ids != set(self.owners_by_binding):
            raise ValueError("universal realtime owners differ from catalog bindings")
        catalog_final_bar_ids = {
            item["binding_id"]
            for item in self.bundle.source_catalog["bindings"]
            if item["feed"] == FeedType.BAR.value
        }
        if set(self.final_bar_binding_ids) != catalog_final_bar_ids:
            raise ValueError("universal realtime final BAR bindings differ from catalog")
        if len(self.final_bar_binding_ids) != len(set(self.final_bar_binding_ids)):
            raise ValueError("universal realtime final BAR bindings must be unique")
        expected_bar_roles = {
            (item.venue, item.market)
            for item in self.demand.demands
            if item.feed is FeedType.BAR
        }
        if set(self.final_bar_runtime_roles) != expected_bar_roles:
            raise ValueError("universal realtime final BAR roles differ from demand")
        if len(self.final_bar_runtime_roles) != len(set(self.final_bar_runtime_roles)):
            raise ValueError("universal realtime final BAR roles must be unique")
        if self.topology.demand_revision != self.demand.revision:
            raise ValueError("universal realtime topology revision differs from demand")
        if self.topology.service_role_count > 4:
            raise ValueError("universal realtime plan grew an unexpected runtime role")

    @property
    def binding_count(self) -> int:
        return len(self.bundle.source_catalog["bindings"])

    @property
    def final_bar_binding_count(self) -> int:
        return len(self.final_bar_binding_ids)

    def report_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "provenance": "ADMITTED_ACTIVE_DEMAND_ONLY",
            "inventory_sha256": self.inventory_sha256,
            "binding_count": self.binding_count,
            "native_subscription_count": len(self.topology.subscriptions),
            "native_connection_count": self.topology.connection_count,
            "service_role_count": self.topology.service_role_count,
            "runtime_roles": [list(item) for item in self.topology.runtime_roles],
            "final_bar_binding_count": self.final_bar_binding_count,
            "final_bar_runtime_roles": [list(item) for item in self.final_bar_runtime_roles],
            "deferred_requirement_count": len(self.deferred_requirement_ids),
            "unsupported_requirement_count": len(self.unsupported_requirement_ids),
            "source_catalog_sha256": self.bundle.provenance["source_catalog_sha256"],
            "acquisition_plan_sha256": self.bundle.provenance["acquisition_plan_sha256"],
        }


@dataclass(frozen=True, slots=True)
class ProviderRealtimeBinding:
    """One authentic provider-edge binding from a universal realtime plan.

    This is intentionally data, not a worker specification. A shared Rust
    role multiplexes every ``RUST_NATIVE`` row for its venue/market; the shared
    Python final-bar edge polls only recurring ``PYTHON_REST`` BAR rows and
    handles bounded history recovery for every BAR. The typed
    projection is also the only input accepted by the bounded provider
    admission harness, preventing tests from silently falling back to
    reference symbols.
    """

    binding_id: str
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: FeedType
    interval: str | None
    source_id: str
    adapter_version: str
    normalizer_version: str
    stale_after_ms: int
    require_final_bar: bool
    mode: str
    provider_kind: str
    native_channel: str
    websocket_url: str | None
    business_websocket_url: str | None
    l2: Mapping[str, object] | None
    catalog_revision: int
    demand_revision: int

    def __post_init__(self) -> None:
        required = (
            self.binding_id,
            self.instrument_uid,
            self.instrument_id,
            self.venue,
            self.market,
            self.product_type,
            self.native_symbol,
            self.source_id,
            self.adapter_version,
            self.normalizer_version,
            self.provider_kind,
            self.native_channel,
        )
        if any(not value.strip() for value in required):
            raise ValueError("provider realtime binding identity is incomplete")
        if self.stale_after_ms <= 0 or min(self.catalog_revision, self.demand_revision) < 1:
            raise ValueError("provider realtime binding revisions/freshness are invalid")
        if self.feed is FeedType.BAR:
            if not self.require_final_bar or self.interval is None or self.l2 is not None:
                raise ValueError("final BAR binding is missing finality, interval or carries L2")
            if self.mode == "PYTHON_REST":
                if self.websocket_url is not None or self.business_websocket_url is not None:
                    raise ValueError("REST final BAR cannot carry a websocket endpoint")
                return
            if self.mode != "RUST_NATIVE" or not self.websocket_url:
                raise ValueError("native final BAR requires an admitted websocket lane")
            return
        if self.feed not in {FeedType.TRADE, FeedType.QUOTE, FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
            raise ValueError("provider realtime binding feed is unsupported")
        if (
            self.mode != "RUST_NATIVE"
            or self.require_final_bar
            or self.interval is not None
            or not self.websocket_url
        ):
            raise ValueError("native provider binding contract is invalid")
        if self.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
            if not isinstance(self.l2, Mapping):
                raise ValueError("BOOK provider binding has no L2 acquisition contract")
            return
        if self.l2 is not None:
            raise ValueError("non-BOOK provider binding cannot carry L2 acquisition")


def provider_realtime_bindings(
    plan: UniversalRealtimePlan,
) -> tuple[ProviderRealtimeBinding, ...]:
    """Project a dark plan into exact provider-edge bindings.

    No provider request, subscription, file write or runtime mutation occurs
    here. The projection proves that every provider test and later handoff
    uses the catalog's admitted instrument identity and acquisition mode.
    """
    source_by_id = {
        str(item["binding_id"]): item for item in plan.bundle.source_catalog["bindings"]
    }
    acquisition_by_id = {
        str(item["binding_id"]): item for item in plan.bundle.acquisition_plan["bindings"]
    }
    instrument_by_uid = {
        str(item["instrument_uid"]): item for item in plan.bundle.source_catalog["instruments"]
    }
    if set(source_by_id) != set(acquisition_by_id) or set(source_by_id) != set(plan.owners_by_binding):
        raise ValueError("provider realtime projection binding sets differ")
    result = []
    for binding_id in sorted(source_by_id):
        source = source_by_id[binding_id]
        acquisition = acquisition_by_id[binding_id]
        instrument_uid = str(source["instrument_uid"])
        try:
            instrument = instrument_by_uid[instrument_uid]
        except KeyError as error:
            raise ValueError("provider realtime binding references an undeclared instrument") from error
        source_config = source["source"]
        quality = source["quality"]
        result.append(
            ProviderRealtimeBinding(
                binding_id=binding_id,
                instrument_uid=instrument_uid,
                instrument_id=str(instrument["instrument_id"]),
                venue=str(instrument["venue"]),
                market=str(instrument["market"]),
                product_type=str(instrument["product_type"]),
                native_symbol=str(instrument["native_symbol"]),
                feed=FeedType(str(source["feed"])),
                interval=(str(source["interval"]) if source["interval"] is not None else None),
                source_id=str(source_config["source_id"]),
                adapter_version=str(source_config["adapter_version"]),
                normalizer_version=str(source_config["normalizer_version"]),
                stale_after_ms=int(quality["stale_after_ms"]),
                require_final_bar=bool(quality["require_final_bar"]),
                mode=str(acquisition["mode"]),
                provider_kind=str(acquisition["provider_kind"]),
                native_channel=str(acquisition["native_channel"]),
                websocket_url=(
                    str(acquisition["websocket_url"])
                    if acquisition["websocket_url"] is not None else None
                ),
                business_websocket_url=(
                    str(acquisition["business_websocket_url"])
                    if acquisition["business_websocket_url"] is not None else None
                ),
                l2=(dict(acquisition["l2"]) if acquisition.get("l2") is not None else None),
                catalog_revision=int(plan.bundle.source_catalog["catalog_revision"]),
                demand_revision=plan.demand.revision,
            )
        )
    if len(result) != plan.binding_count:
        raise ValueError("provider realtime projection lost an admitted binding")
    return tuple(result)


def build_universal_realtime_plan(
    *,
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    convergence: ActiveDemandConvergence,
    builder: ProductionCatalogBuilder,
) -> UniversalRealtimePlan:
    """Build the dark Phase 11.2 plan from exactly one admitted inventory.

    No provider request occurs here.  The `InstrumentRecord` instances are the
    immutable output of the one real metadata capture used by Phase 11.1.
    """
    if admission.inventory_sha256 != inventory.manifest_sha256:
        raise InventoryError("provider admission inventory digest does not match active demand")
    if convergence.inventory_sha256 != inventory.manifest_sha256:
        raise InventoryError("convergence inventory digest does not match active demand")

    readiness = {item.requirement_id: item for item in convergence.readiness}
    if len(readiness) != len(convergence.readiness):
        raise InventoryError("convergence readiness has duplicate admission IDs")
    admission_ids = {item.requirement_id for item in admission.rows}
    if set(readiness) != admission_ids:
        raise InventoryError("convergence readiness differs from provider admission")

    selected: dict[tuple[str, str, str, str, DemandFeed, str | None], tuple[ProductionDemand, set[str]]] = {}
    deferred: list[str] = []
    unsupported: list[str] = []
    for row in admission.rows:
        current = readiness[row.requirement_id]
        if row.state != "ADMITTED" or current.state is DemandState.UNSUPPORTED:
            unsupported.append(row.requirement_id)
            continue
        if current.state is not DemandState.WARMING:
            raise InventoryError(
                f"admitted active demand is not selected for realtime projection: {row.requirement_id}"
            )
        requirement = source_requirement_for_admission(inventory, row)
        if requirement.feed not in _REALTIME_FEEDS:
            deferred.append(row.requirement_id)
            continue
        try:
            record = admission.records[
                (row.venue, row.market, row.product_type, row.native_symbol)
            ]
        except KeyError as error:
            raise InventoryError(
                "admitted realtime demand has no authoritative instrument record: "
                f"{row.venue}/{row.market}/{row.product_type}/{row.native_symbol}"
            ) from error
        if record.instrument_uid != row.instrument_uid or record.instrument_id != row.instrument_id:
            raise InventoryError("admitted realtime identity differs from authoritative record")
        feed = FeedType(requirement.feed.value)
        grade = _PURPOSE_TO_GRADE[requirement.purpose]
        key = (
            row.venue,
            row.market,
            row.product_type,
            row.native_symbol,
            requirement.feed,
            requirement.interval,
        )
        book_fields = (
            {
                "depth_per_side": requirement.depth_levels,
                "max_freshness_ms": requirement.max_freshness_ms,
                "require_live": requirement.require_live,
            }
            if requirement.feed in _BOOK_FEEDS else {}
        )
        demand = ProductionDemand(
            consumer_id=requirement.consumer_id,
            consumer_grade=grade,
            venue=row.venue,
            market=row.market,
            product_type=row.product_type,
            native_symbol=row.native_symbol,
            feed=feed,
            interval=requirement.interval,
            source_policy_id=requirement.source_policy_id,
            **book_fields,
        )
        existing = selected.get(key)
        if existing is None:
            selected[key] = (demand, {requirement.consumer_id})
            continue
        existing_demand, owners = existing
        if existing_demand.source_policy_id != demand.source_policy_id:
            raise InventoryError("one realtime physical slice has conflicting source policies")
        owners.add(requirement.consumer_id)
        if _GRADE_RANK[demand.consumer_grade] < _GRADE_RANK[existing_demand.consumer_grade]:
            selected[key] = (
                ProductionDemand(
                    consumer_id=demand.consumer_id,
                    consumer_grade=demand.consumer_grade,
                    venue=demand.venue,
                    market=demand.market,
                    product_type=demand.product_type,
                    native_symbol=demand.native_symbol,
                    feed=demand.feed,
                    interval=demand.interval,
                    source_policy_id=demand.source_policy_id,
                    **(
                        {
                            "depth_per_side": demand.depth_per_side,
                            "max_freshness_ms": demand.max_freshness_ms,
                            "require_live": demand.require_live,
                        }
                        if demand.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}
                        else {}
                    ),
                ),
                owners,
            )

    if not selected:
        raise InventoryError("active demand has no admitted TRADE/QUOTE/BAR slice")
    demands = tuple(
        item[0]
        for _key, item in sorted(
            selected.items(),
            key=lambda pair: (
                pair[0][0], pair[0][1], pair[0][2], pair[0][3], pair[0][4].value,
                pair[0][5] or "",
            ),
        )
    )
    demand = ProductionDemandManifest(
        revision=inventory.revision,
        demands=demands,
        source_paths=(f"active-demand:{inventory.manifest_sha256}",),
    )
    records = []
    for item in demands:
        records.append(
            admission.records[(item.venue, item.market, item.product_type, item.native_symbol)]
        )
    bundle = builder.build_from_records(
        demand=demand,
        records=records,
        metadata_provenance={
            **dict(admission.metadata_sha256),
            "active_demand_inventory_sha256": inventory.manifest_sha256,
        },
    )
    acquisition_by_id = {
        str(item["binding_id"]): item for item in bundle.acquisition_plan["bindings"]
    }
    binding_owners: dict[str, tuple[str, ...]] = {}
    subscriptions = []
    final_bar_binding_ids: list[str] = []
    final_bar_runtime_roles: set[tuple[str, str]] = set()
    for item in demands:
        key = (
            item.venue,
            item.market,
            item.product_type,
            item.native_symbol,
            DemandFeed(item.feed.value),
            item.interval,
        )
        binding_id = builder._binding_id(item)
        binding_owners[binding_id] = tuple(sorted(selected[key][1]))
        if item.feed is FeedType.BAR:
            final_bar_binding_ids.append(binding_id)
            final_bar_runtime_roles.add((item.venue, item.market))
            try:
                acquisition = acquisition_by_id[binding_id]
            except KeyError as error:
                raise InventoryError("final BAR demand has no acquisition binding") from error
            mode = str(acquisition["mode"])
            if mode == "RUST_NATIVE":
                subscriptions.append(_subscription_from_demand(item))
            elif mode != "PYTHON_REST":
                raise InventoryError("final BAR acquisition mode is unsupported")
        else:
            subscriptions.append(_subscription_from_demand(item))
    subscriptions = sorted(set(subscriptions), key=lambda item: item.key)
    topology = DemandTopology(
        demand_revision=inventory.revision,
        subscriptions=tuple(subscriptions),
        shards=plan_shards(
            set(subscriptions),
            max_streams=convergence.policy.max_subscriptions_per_connection,
            config_revision=inventory.revision,
        ),
        runtime_roles=tuple(sorted({(item.venue, item.market) for item in subscriptions})),
        core_binding_ids=tuple(sorted(binding_owners)),
        provisioning_required=(),
    )
    return UniversalRealtimePlan(
        schema="qdl.v2.universal-realtime-plan.v1",
        inventory_sha256=inventory.manifest_sha256,
        demand=demand,
        bundle=bundle,
        topology=topology,
        owners_by_binding=dict(sorted(binding_owners.items())),
        final_bar_binding_ids=tuple(sorted(final_bar_binding_ids)),
        final_bar_runtime_roles=tuple(sorted(final_bar_runtime_roles)),
        deferred_requirement_ids=tuple(sorted(deferred)),
        unsupported_requirement_ids=tuple(sorted(unsupported)),
    )


def _subscription_from_demand(item: ProductionDemand):
    from qdl.ingestion.contracts import Subscription

    return Subscription(
        venue=item.venue,
        market=item.market,
        feed=_INGESTION_FEED[DemandFeed(item.feed.value)],
        native_symbol=item.native_symbol,
        interval=item.interval,
        priority=0,
    )

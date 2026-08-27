"""Source-only demand projection for one shared Rust L2 book plane.

The Phase 11 control plane already resolves universe membership, provider
admission, ownership, TTL and requested depth.  This module projects only
admitted L2 requirements into provider-neutral bindings and shared venue/market
shards.  It opens no socket, fetches no REST snapshot, writes no state and
never creates a worker/container per alpha or symbol.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Mapping

from qdl.demand import (
    ActiveDemandConvergence,
    ActiveDemandInventory,
    DemandFeed,
    DemandState,
    InventoryError,
    ProviderAdmission,
    source_requirement_for_admission,
)
from qdl.demand.topology import DemandTopology
from qdl.ingestion.contracts import FeedType as IngestionFeedType
from qdl.ingestion.contracts import Subscription, plan_shards


_BOOK_FEEDS = frozenset({DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})
_PLAN_SCHEMA = "qdl.v2.l2-demand-plan.v1"
_BINANCE_WS = {
    "USDM": "wss://fstream.binance.com/ws",
    "SPOT": "wss://stream.binance.com:9443/ws",
}
_BINANCE_REST_DEPTH = {
    "USDM": "https://fapi.binance.com/fapi/v1/depth",
    "SPOT": "https://api.binance.com/api/v3/depth",
}
_OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"


@dataclass(frozen=True, slots=True)
class L2ProviderBinding:
    """One physical book subscription shared by all declared consumers."""

    binding_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    instrument_uid: str
    instrument_id: str
    source_policy_id: str
    consumer_ids: tuple[str, ...]
    requirement_ids: tuple[str, ...]
    feeds: tuple[DemandFeed, ...]
    requested_depth_per_side: int
    stale_after_ms: int
    ttl_seconds: int
    require_live: bool
    execution_grade: bool
    provider_protocol: str
    native_channel: str
    websocket_url: str
    rest_snapshot_url: str | None

    def __post_init__(self) -> None:
        required = (
            self.binding_id,
            self.venue,
            self.market,
            self.product_type,
            self.native_symbol,
            self.instrument_uid,
            self.instrument_id,
            self.source_policy_id,
            self.provider_protocol,
            self.native_channel,
            self.websocket_url,
        )
        if any(not value.strip() for value in required):
            raise ValueError("L2 binding identity/protocol is incomplete")
        if (
            not self.consumer_ids
            or not self.requirement_ids
            or not self.feeds
            or len(self.consumer_ids) != len(set(self.consumer_ids))
            or len(self.requirement_ids) != len(set(self.requirement_ids))
        ):
            raise ValueError("L2 binding ownership is incomplete or duplicated")
        if set(self.feeds) - _BOOK_FEEDS:
            raise ValueError("L2 binding contains a non-book feed")
        if not 1 <= self.requested_depth_per_side <= 10_000:
            raise ValueError("L2 requested depth is outside safe bounds")
        if self.stale_after_ms <= 0 or not 30 <= self.ttl_seconds <= 3_600:
            raise ValueError("L2 freshness/TTL is outside bounds")
        if self.provider_protocol == "BINANCE_DIFF_DEPTH":
            if self.venue != "BINANCE" or self.market not in _BINANCE_WS:
                raise ValueError("Binance diff-depth binding market is invalid")
            if not self.native_channel.endswith("@depth@100ms"):
                raise ValueError("Binance L2 binding must use documented diff-depth channel")
            if self.rest_snapshot_url != _BINANCE_REST_DEPTH[self.market]:
                raise ValueError("Binance L2 binding must retain its REST snapshot anchor")
        elif self.provider_protocol == "OKX_PUBLIC_BOOKS":
            if self.venue != "OKX" or self.market not in {"SWAP", "FUTURES", "SPOT"}:
                raise ValueError("OKX public books binding market is invalid")
            if self.native_channel != "books" or self.rest_snapshot_url is not None:
                raise ValueError("OKX books must bootstrap only from its websocket snapshot")
        else:
            raise ValueError("L2 provider protocol is not certified")

    @property
    def subscription(self) -> Subscription:
        return Subscription(
            venue=self.venue,
            market=self.market,
            feed=IngestionFeedType.BOOK,
            native_symbol=self.native_symbol,
            priority=0,
        )

    def report_payload(self) -> dict[str, object]:
        return {
            "binding_id": self.binding_id,
            "venue": self.venue,
            "market": self.market,
            "product_type": self.product_type,
            "native_symbol": self.native_symbol,
            "instrument_uid": self.instrument_uid,
            "consumer_ids": list(self.consumer_ids),
            "requirement_ids": list(self.requirement_ids),
            "feeds": [item.value for item in self.feeds],
            "requested_depth_per_side": self.requested_depth_per_side,
            "stale_after_ms": self.stale_after_ms,
            "ttl_seconds": self.ttl_seconds,
            "require_live": self.require_live,
            "execution_grade": self.execution_grade,
            "provider_protocol": self.provider_protocol,
            "native_channel": self.native_channel,
            "rest_snapshot_required": self.rest_snapshot_url is not None,
        }


@dataclass(frozen=True, slots=True)
class L2DemandPlan:
    """Auditable, inactive L2 plan derived from one admitted inventory."""

    schema: str
    inventory_sha256: str
    demand_revision: int
    bindings: tuple[L2ProviderBinding, ...]
    topology: DemandTopology
    deferred_requirement_ids: tuple[str, ...]
    unsupported_requirement_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema != _PLAN_SCHEMA or len(self.inventory_sha256) != 64:
            raise ValueError("L2 demand plan schema or inventory identity is invalid")
        if self.demand_revision < 1 or not self.bindings:
            raise ValueError("L2 demand plan is empty or has an invalid revision")
        binding_ids = [item.binding_id for item in self.bindings]
        if len(binding_ids) != len(set(binding_ids)):
            raise ValueError("L2 demand plan binding IDs are not unique")
        expected_subscriptions = {item.subscription for item in self.bindings}
        if set(self.topology.subscriptions) != expected_subscriptions:
            raise ValueError("L2 topology differs from physical book bindings")
        if set(self.topology.core_binding_ids) != set(binding_ids):
            raise ValueError("L2 topology core bindings differ from L2 plan")
        if self.topology.service_role_count > len({(item.venue, item.market) for item in self.bindings}):
            raise ValueError("L2 topology added a non-demand runtime role")

    def report_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "provenance": "ADMITTED_ACTIVE_DEMAND_ONLY",
            "inventory_sha256": self.inventory_sha256,
            "demand_revision": self.demand_revision,
            "binding_count": len(self.bindings),
            "subscription_count": len(self.topology.subscriptions),
            "connection_count": self.topology.connection_count,
            "service_role_count": self.topology.service_role_count,
            "runtime_roles": [list(item) for item in self.topology.runtime_roles],
            "bindings": [item.report_payload() for item in self.bindings],
            "deferred_requirement_ids": list(self.deferred_requirement_ids),
            "unsupported_requirement_ids": list(self.unsupported_requirement_ids),
            "runtime_applied": False,
        }


def build_l2_demand_plan(
    *,
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    convergence: ActiveDemandConvergence,
) -> L2DemandPlan:
    """Compile all admitted BOOK requirements without acquiring a provider feed."""

    if (
        admission.inventory_sha256 != inventory.manifest_sha256
        or convergence.inventory_sha256 != inventory.manifest_sha256
    ):
        raise InventoryError("L2 plan inventory/admission/convergence digests differ")
    readiness = {item.requirement_id: item for item in convergence.readiness}
    if set(readiness) != {item.requirement_id for item in admission.rows}:
        raise InventoryError("L2 plan convergence rows differ from provider admission")

    selected: dict[
        tuple[str, str, str, str, str],
        list[tuple[object, object, object]],
    ] = {}
    deferred: list[str] = []
    unsupported: list[str] = []
    for row in admission.rows:
        requirement = source_requirement_for_admission(inventory, row)
        if requirement.feed not in _BOOK_FEEDS:
            continue
        readiness_row = readiness[row.requirement_id]
        if row.state != "ADMITTED" or readiness_row.state is DemandState.UNSUPPORTED:
            unsupported.append(row.requirement_id)
            continue
        if readiness_row.state is not DemandState.WARMING:
            raise InventoryError("admitted L2 demand is not in dark WARMING state")
        if requirement.depth_levels <= 0 or requirement.max_freshness_ms is None:
            raise InventoryError("L2 demand requires explicit positive depth and freshness")
        key = (
            row.venue,
            row.market,
            row.product_type,
            row.native_symbol,
            requirement.source_policy_id,
        )
        selected.setdefault(key, []).append((row, requirement, readiness_row))

    if not selected:
        raise InventoryError("no admitted L2 demand is available for a book plan")

    bindings = tuple(
        _binding_from_rows(key, rows, admission)
        for key, rows in sorted(selected.items())
    )
    subscriptions = tuple(sorted({item.subscription for item in bindings}, key=lambda item: item.key))
    topology = DemandTopology(
        demand_revision=inventory.revision,
        subscriptions=subscriptions,
        shards=plan_shards(
            set(subscriptions),
            max_streams=convergence.policy.max_subscriptions_per_connection,
            config_revision=inventory.revision,
        ),
        runtime_roles=tuple(sorted({(item.venue, item.market) for item in bindings})),
        core_binding_ids=tuple(sorted(item.binding_id for item in bindings)),
        provisioning_required=(),
    )
    return L2DemandPlan(
        schema=_PLAN_SCHEMA,
        inventory_sha256=inventory.manifest_sha256,
        demand_revision=inventory.revision,
        bindings=bindings,
        topology=topology,
        deferred_requirement_ids=tuple(sorted(deferred)),
        unsupported_requirement_ids=tuple(sorted(unsupported)),
    )


def _binding_from_rows(
    key: tuple[str, str, str, str, str],
    rows: Iterable[tuple[object, object, object]],
    admission: ProviderAdmission,
) -> L2ProviderBinding:
    venue, market, product_type, native_symbol, source_policy_id = key
    values = tuple(rows)
    first_row = values[0][0]
    try:
        record = admission.records[(venue, market, product_type, native_symbol)]
    except KeyError as error:
        raise InventoryError("admitted L2 row has no authoritative instrument record") from error
    if (
        first_row.instrument_uid != record.instrument_uid
        or first_row.instrument_id != record.instrument_id
    ):
        raise InventoryError("admitted L2 row identity differs from instrument record")
    if any(item[1].source_policy_id != source_policy_id for item in values):
        raise InventoryError("one physical L2 book has conflicting source policies")
    if any(
        item[0].instrument_uid != record.instrument_uid
        or item[0].instrument_id != record.instrument_id
        for item in values
    ):
        raise InventoryError("one physical L2 book has conflicting instrument identity")
    protocol, channel, websocket, rest_snapshot = _provider_edge(
        venue=venue,
        market=market,
        native_symbol=native_symbol,
    )
    feeds = tuple(sorted({item[1].feed for item in values}, key=lambda item: item.value))
    binding_id = "l2-{}-{}-{}".format(
        venue.lower(), market.lower(), re.sub(r"[^a-z0-9]+", "-", native_symbol.lower()).strip("-"),
    )
    return L2ProviderBinding(
        binding_id=binding_id,
        venue=venue,
        market=market,
        product_type=product_type,
        native_symbol=native_symbol,
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        source_policy_id=source_policy_id,
        consumer_ids=tuple(sorted({item[0].consumer_id for item in values})),
        requirement_ids=tuple(sorted({item[0].requirement_id for item in values})),
        feeds=feeds,
        requested_depth_per_side=max(item[1].depth_levels for item in values),
        stale_after_ms=min(int(item[1].max_freshness_ms) for item in values),
        ttl_seconds=max(item[1].ttl_seconds for item in values),
        require_live=any(item[1].require_live for item in values),
        execution_grade=any(item[1].execution_grade for item in values),
        provider_protocol=protocol,
        native_channel=channel,
        websocket_url=websocket,
        rest_snapshot_url=rest_snapshot,
    )


def _provider_edge(
    *,
    venue: str,
    market: str,
    native_symbol: str,
) -> tuple[str, str, str, str | None]:
    if venue == "BINANCE" and market in _BINANCE_WS:
        return (
            "BINANCE_DIFF_DEPTH",
            f"{native_symbol.lower()}@depth@100ms",
            _BINANCE_WS[market],
            _BINANCE_REST_DEPTH[market],
        )
    if venue == "OKX" and market in {"SWAP", "FUTURES", "SPOT"}:
        return "OKX_PUBLIC_BOOKS", "books", _OKX_PUBLIC_WS, None
    raise InventoryError(f"L2 provider edge is not certified: {venue}/{market}")

"""Deterministic materialization for the active Reference/L2 V2 product.

This control-plane module turns one admitted active-demand inventory into the
existing stable catalog, acquisition and consumer-manifest contracts.  It is
deliberately free of provider I/O and runtime mutation: callers supply the one
bounded provider-metadata result, and a separate reviewed packet may later
mount the rendered artifacts into the existing roles.

Reference products add only catalog instruments and least-privilege query
entitlements.  BOOK_SNAPSHOT/BOOK_DELTA remain distinct public products but
coalesce through the existing Rust L2 acquisition contract.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any, Callable, Mapping

import yaml

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.demand import (
    ActiveDemandConvergence,
    ActiveDemandInventory,
    DemandFeed,
    DemandState,
    InventoryError,
    ProviderAdmission,
    source_requirement_for_admission,
)
from qdl.query import ConsumerGrade, FeedType
from qdl.reference.batch import default_capability_resolver
from qdl.runtime.l2_demand import L2DemandPlan, build_l2_demand_plan
from qdl.runtime.production_catalog import ProductionCatalogBuilder
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope, StableAcquisitionPlan
from qdl.runtime.universal_realtime import UniversalRealtimePlan, build_universal_realtime_plan


REFERENCE_L2_CONSUMER_ID = "qdl.crypto.reference-l2.stable"
_BOOK_FEEDS = frozenset({DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})
_REFERENCE_FEED_MAP = {
    DemandFeed.FUNDING_RATE: FeedType.FUNDING_RATE,
    DemandFeed.OPEN_INTEREST: FeedType.OPEN_INTEREST,
    DemandFeed.LONG_SHORT_RATIO: FeedType.LONG_SHORT_RATIO,
    DemandFeed.TAKER_FLOW: FeedType.TAKER_FLOW,
    DemandFeed.MARK_PRICE: FeedType.MARK_INDEX_PRICE,
    DemandFeed.INDEX_PRICE: FeedType.MARK_INDEX_PRICE,
    DemandFeed.MARK_INDEX_PRICE: FeedType.MARK_INDEX_PRICE,
    DemandFeed.CONTRACT_METADATA: FeedType.CONTRACT_METADATA,
    DemandFeed.BASIS: FeedType.BASIS,
    DemandFeed.BOOK_SNAPSHOT: FeedType.BOOK_SNAPSHOT,
    DemandFeed.BOOK_DELTA: FeedType.BOOK_DELTA,
}


@dataclass(frozen=True, slots=True)
class ReferenceL2Materialization:
    """Fully validated source artifacts for one bounded Reference/L2 demand."""

    source_catalog: dict[str, Any]
    acquisition_plan: dict[str, Any]
    promotion_scope: dict[str, Any]
    consumer_manifest: dict[str, Any]
    realtime_plan: UniversalRealtimePlan
    l2_plan: L2DemandPlan
    summary: dict[str, Any]


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _without_revision(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    result = deepcopy(dict(value))
    result.pop(field, None)
    return result


def _validate_temporary(
    value: Mapping[str, Any],
    *,
    filename: str,
    loader: Callable[[Path], Any],
) -> Any:
    with tempfile.TemporaryDirectory(prefix="qdl-reference-l2-") as raw:
        path = Path(raw) / filename
        path.write_text(yaml.safe_dump(dict(value), sort_keys=False), encoding="utf-8")
        return loader(path)


def _source_key(item: Mapping[str, Any]) -> tuple[str, str, str | None, str]:
    source = item.get("source")
    if not isinstance(source, Mapping):
        raise ValueError("source binding has no lineage object")
    interval = item.get("interval")
    return (
        str(item["instrument_uid"]),
        str(item["feed"]),
        str(interval) if interval is not None else None,
        str(source["source_policy_id"]),
    )


def _managed_l2(item: Mapping[str, Any]) -> bool:
    source = item.get("source")
    if not isinstance(source, Mapping):
        return False
    return (
        str(item.get("feed")) in {FeedType.BOOK_SNAPSHOT.value, FeedType.BOOK_DELTA.value}
        and str(source.get("source_policy_id")) == "crypto_liquid_v2"
        and str(source.get("source_id", "")).endswith("-book-primary-v2")
    )


def _merge_source_bindings(
    *,
    existing: list[dict[str, Any]],
    generated: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str], set[str]]:
    """Replace only managed L2 bindings, preserving every unrelated cursor ID."""

    existing_by_key: dict[tuple[str, str, str | None, str], dict[str, Any]] = {}
    for item in existing:
        key = _source_key(item)
        if key in existing_by_key:
            raise ValueError("existing source catalog has duplicate requirement identity")
        existing_by_key[key] = item
    generated_by_key: dict[tuple[str, str, str | None, str], dict[str, Any]] = {}
    for item in generated:
        key = _source_key(item)
        if key in generated_by_key:
            raise ValueError("generated Reference/L2 catalog has duplicate requirement identity")
        generated_by_key[key] = item

    managed_old_ids = {
        str(item["binding_id"]) for item in existing if _managed_l2(item)
    }
    result: list[dict[str, Any]] = []
    remap: dict[str, str] = {}
    for key, generated_item in sorted(generated_by_key.items()):
        current = existing_by_key.get(key)
        if current is not None and not _managed_l2(current):
            raise ValueError(
                "Reference/L2 binding collides with an unrelated stable requirement: "
                + str(current["binding_id"])
            )
        # Keeping an existing equivalent binding ID/source ID is vital: it is
        # the cursor lineage, not an incidental configuration label.
        chosen = deepcopy(current if current is not None else generated_item)
        result.append(chosen)
        remap[str(generated_item["binding_id"])] = str(chosen["binding_id"])

    for item in existing:
        key = _source_key(item)
        if _managed_l2(item):
            continue
        if key in generated_by_key:
            raise ValueError("unmanaged stable binding collided during materialization")
        result.append(deepcopy(item))

    ids = [str(item["binding_id"]) for item in result]
    if len(ids) != len(set(ids)):
        raise ValueError("materialized source binding IDs are not unique")
    return sorted(result, key=lambda item: str(item["binding_id"])), remap, managed_old_ids


def _merge_acquisition_bindings(
    *,
    existing: list[dict[str, Any]],
    generated: list[dict[str, Any]],
    source_binding_ids: set[str],
    remap: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Keep acquisition immutable unless this compiler owns the L2 binding."""

    existing_by_id = {str(item["binding_id"]): item for item in existing}
    generated_by_id = {str(item["binding_id"]): item for item in generated}
    if len(existing_by_id) != len(existing) or len(generated_by_id) != len(generated):
        raise ValueError("acquisition binding IDs are duplicated")
    result: dict[str, dict[str, Any]] = {}
    for generated_id, target_id in remap.items():
        candidate = existing_by_id.get(target_id, generated_by_id[generated_id])
        chosen = deepcopy(candidate)
        chosen["binding_id"] = target_id
        result[target_id] = chosen
    for binding_id in source_binding_ids:
        if binding_id in result:
            continue
        try:
            result[binding_id] = deepcopy(existing_by_id[binding_id])
        except KeyError as error:
            raise ValueError(
                "stable source binding lacks an acquisition binding: " + binding_id
            ) from error
    if set(result) != source_binding_ids:
        raise ValueError("source/acquisition binding coverage differs after materialization")
    return sorted(result.values(), key=lambda item: str(item["binding_id"]))


def _manifest_requirement(
    *,
    row: object,
    requirement: object,
    instrument: object,
) -> dict[str, Any]:
    demand_feed = requirement.feed
    try:
        feed = _REFERENCE_FEED_MAP[demand_feed]
    except KeyError as error:
        raise InventoryError(f"Reference/L2 manifest has unsupported feed: {demand_feed}") from error
    if requirement.purpose.value != "RESEARCH" or requirement.execution_grade:
        raise InventoryError("Reference/L2 consumer must remain research/non-execution")
    if demand_feed is DemandFeed.OPEN_INTEREST and requirement.interval is not None:
        capability = default_capability_resolver(instrument).capability("open_interest")
        if not capability.rest_history:
            raise InventoryError(
                "Reference/L2 open-interest history is not certified for "
                f"{row.venue}/{row.market}; declare a fresh snapshot instead"
            )
    is_book = demand_feed in _BOOK_FEEDS
    return {
        "instrument_uid": str(row.instrument_uid),
        "feed": feed.value,
        "consumer_grade": ConsumerGrade.RESEARCH.value,
        "source_policy_id": requirement.source_policy_id,
        "interval": requirement.interval,
        "warmup_limit": int(requirement.warmup_limit),
        "max_freshness_ms": int(requirement.max_freshness_ms),
        "require_full_coverage": True,
        "require_final_bars": False,
        "stale_policy": "BLOCK",
        "gap_policy": "BLOCK",
        "recovery": "SNAPSHOT_AND_REPLAY" if is_book else "FRESH_SNAPSHOT",
        "bar_revision_policy": "LATEST",
    }


def _render_consumer_manifest(
    *,
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    existing: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Render one V2-only entitlement manifest from admitted identities."""

    merged: dict[tuple[str, str, str | None, str], dict[str, Any]] = {}
    for row in admission.rows:
        if row.consumer_id != REFERENCE_L2_CONSUMER_ID:
            continue
        if row.state != "ADMITTED" or row.instrument_uid is None:
            raise InventoryError("Reference/L2 consumer has a non-admitted provider identity")
        requirement = source_requirement_for_admission(inventory, row)
        try:
            instrument = admission.records[
                (row.venue, row.market, row.product_type, row.native_symbol)
            ]
        except KeyError as error:
            raise InventoryError(
                "Reference/L2 admitted row has no canonical instrument record"
            ) from error
        item = _manifest_requirement(
            row=row,
            requirement=requirement,
            instrument=instrument,
        )
        key = (
            item["instrument_uid"],
            item["feed"],
            item["interval"],
            item["source_policy_id"],
        )
        prior = merged.get(key)
        if prior is None:
            merged[key] = item
            continue
        # MARK_PRICE and INDEX_PRICE intentionally converge into the one public
        # MARK_INDEX_PRICE product.  Any future duplicate must be compatible.
        for field in (
            "consumer_grade", "source_policy_id", "require_full_coverage",
            "require_final_bars", "stale_policy", "gap_policy", "recovery",
            "bar_revision_policy",
        ):
            if prior[field] != item[field]:
                raise InventoryError("one public Reference/L2 entitlement has conflicting semantics")
        prior["warmup_limit"] = max(int(prior["warmup_limit"]), int(item["warmup_limit"]))
        prior["max_freshness_ms"] = min(
            int(prior["max_freshness_ms"]), int(item["max_freshness_ms"])
        )

    requirements = [
        item for _key, item in sorted(
            merged.items(),
            key=lambda pair: (pair[0][0], pair[0][1], pair[0][2] or "", pair[0][3]),
        )
    ]
    if not requirements:
        raise InventoryError("Reference/L2 consumer has no admitted entitlement")
    base = {
        "apiVersion": "qdl/v2",
        "kind": "DataRequirement",
        "metadata": {
            "id": REFERENCE_L2_CONSUMER_ID,
            "owner": "data-platform",
            "subject": "spiffe://qdl/paper/reference-l2-stable",
            "environment": "paper",
            "revision": 1,
        },
        "spec": {
            "sdk_major": 2,
            # There is no V1 equivalence claim for REST reference or L2 state.
            # A failed V2 route blocks rather than silently changes semantics.
            "rollback_contract": "V2",
            "execution_dependency": "FORBIDDEN",
            "permissions": [
                "instruments:read",
                "snapshot:read",
                "history:read",
                "status:read",
                "quality:read",
                "stream:read",
            ],
            "purposes": ["INTERNAL_RESEARCH"],
            "quotas": {
                "requests_per_minute": 120,
                "max_batch_items": 100,
                "max_warmup_rows": 10_000,
                "max_streams": 24,
                "max_buffer_events": 4_000,
            },
            "requirements": requirements,
        },
    }
    if existing is not None:
        previous = ConsumerManifestLoader.from_mapping(existing)
        if previous.consumer_id != REFERENCE_L2_CONSUMER_ID:
            raise ValueError("existing Reference/L2 manifest has the wrong consumer identity")
        comparison = deepcopy(base)
        comparison["metadata"]["revision"] = previous.manifest_revision
        if _canonical(comparison) == _canonical(existing):
            return deepcopy(dict(existing))
        base["metadata"]["revision"] = previous.manifest_revision + 1
    ConsumerManifestLoader.from_mapping(base)
    return base


def build_reference_l2_materialization(
    *,
    inventory: ActiveDemandInventory,
    admission: ProviderAdmission,
    convergence: ActiveDemandConvergence,
    current_catalog_document: Mapping[str, Any],
    current_acquisition_document: Mapping[str, Any],
    current_promotion_scope_document: Mapping[str, Any],
    current_consumer_manifest_document: Mapping[str, Any] | None = None,
) -> ReferenceL2Materialization:
    """Materialize the approved product from exactly one admitted inventory.

    The function is pure relative to Data Layer runtime: temporary files are
    used solely to execute existing strict YAML loaders.  It does not contact
    providers, write a file, create a route or alter authority.
    """

    if not admission.passed or not convergence.passed:
        raise InventoryError("Reference/L2 materialization requires fully admitted convergence")
    if admission.inventory_sha256 != inventory.manifest_sha256:
        raise InventoryError("Reference/L2 admission inventory digest differs")
    if convergence.inventory_sha256 != inventory.manifest_sha256:
        raise InventoryError("Reference/L2 convergence inventory digest differs")

    current_catalog = _validate_temporary(
        current_catalog_document,
        filename="catalog.yaml",
        loader=StableSourceCatalog.load,
    )
    current_acquisition = _validate_temporary(
        current_acquisition_document,
        filename="acquisition.yaml",
        loader=lambda path: StableAcquisitionPlan.load(path, catalog=current_catalog),
    )
    _validate_temporary(
        current_promotion_scope_document,
        filename="scope.yaml",
        loader=lambda path: AuthorityPromotionScope.load(path, catalog=current_catalog),
    )

    builder = ProductionCatalogBuilder(
        catalog_revision=current_catalog.catalog_revision,
        source_policy_revision=current_catalog.source_policy_revision,
        authority_revision=current_catalog.authority_revision,
    )
    realtime_plan = build_universal_realtime_plan(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        builder=builder,
    )
    l2_plan = build_l2_demand_plan(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
    )
    generated_source_bindings = list(realtime_plan.bundle.source_catalog["bindings"])
    generated_acquisition_bindings = list(realtime_plan.bundle.acquisition_plan["bindings"])
    if len(generated_source_bindings) != 24 or len(l2_plan.bindings) != 12:
        raise InventoryError("Reference/L2 demand no longer resolves to 24 logical / 12 physical books")
    if len(l2_plan.topology.subscriptions) != 12 or l2_plan.topology.service_role_count != 3:
        raise InventoryError("Reference/L2 L2 topology must use exactly three shared venue roles")

    source_result = deepcopy(dict(current_catalog_document))
    source_result["instruments"] = ProductionCatalogBuilder.merge_authoritative_instruments(
        records=admission.records.values(),
        previous_catalog=current_catalog,
    )
    source_result["bindings"], remap, managed_old_ids = _merge_source_bindings(
        existing=list(current_catalog_document["bindings"]),
        generated=generated_source_bindings,
    )
    source_changed = _canonical(
        _without_revision(source_result, "catalog_revision")
    ) != _canonical(_without_revision(current_catalog_document, "catalog_revision"))
    source_result["catalog_revision"] = (
        current_catalog.catalog_revision + 1 if source_changed else current_catalog.catalog_revision
    )
    next_catalog = _validate_temporary(
        source_result,
        filename="next-catalog.yaml",
        loader=StableSourceCatalog.load,
    )

    acquisition_result = deepcopy(dict(current_acquisition_document))
    acquisition_result["bindings"] = _merge_acquisition_bindings(
        existing=list(current_acquisition_document["bindings"]),
        generated=generated_acquisition_bindings,
        source_binding_ids={str(item["binding_id"]) for item in source_result["bindings"]},
        remap=remap,
    )
    acquisition_changed = _canonical(
        _without_revision(acquisition_result, "revision")
    ) != _canonical(_without_revision(current_acquisition_document, "revision"))
    acquisition_result["revision"] = (
        current_acquisition.revision + 1 if acquisition_changed else current_acquisition.revision
    )
    _validate_temporary(
        acquisition_result,
        filename="next-acquisition.yaml",
        loader=lambda path: StableAcquisitionPlan.load(path, catalog=next_catalog),
    )

    promotion_result = deepcopy(dict(current_promotion_scope_document))
    generated_scope_ids = set(remap.values())
    current_scope_ids = {str(item) for item in current_promotion_scope_document["binding_ids"]}
    promotion_result["binding_ids"] = sorted(
        (current_scope_ids - managed_old_ids) | generated_scope_ids
    )
    scope_changed = _canonical(
        _without_revision(promotion_result, "revision")
    ) != _canonical(_without_revision(current_promotion_scope_document, "revision"))
    promotion_result["revision"] = (
        int(current_promotion_scope_document["revision"]) + 1
        if scope_changed else int(current_promotion_scope_document["revision"])
    )
    _validate_temporary(
        promotion_result,
        filename="next-scope.yaml",
        loader=lambda path: AuthorityPromotionScope.load(path, catalog=next_catalog),
    )

    consumer_manifest = _render_consumer_manifest(
        inventory=inventory,
        admission=admission,
        existing=current_consumer_manifest_document,
    )
    parsed_consumer = ConsumerManifestLoader.from_mapping(consumer_manifest)
    if len(parsed_consumer.requirements) > 100:
        raise InventoryError("Reference/L2 entitlement exceeds the public V2 batch boundary")
    l2_requirements = [
        item for item in parsed_consumer.requirements
        if item.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}
    ]
    if len(l2_requirements) != 24:
        raise InventoryError("Reference/L2 manifest lost a logical BOOK product")

    summary = {
        "schema": "qdl.reference-l2-materialization.v1",
        "status": "SOURCE_ONLY_READY",
        "runtime_mutations": 0,
        "provider_requests": 0,
        "provider_writes": 0,
        "order_actions": 0,
        "inventory_sha256": inventory.manifest_sha256,
        "provider_metadata_sha256": dict(sorted(admission.metadata_sha256.items())),
        "catalog_revision": next_catalog.catalog_revision,
        "acquisition_revision": int(acquisition_result["revision"]),
        "promotion_scope_revision": int(promotion_result["revision"]),
        "consumer_manifest_revision": parsed_consumer.manifest_revision,
        "catalog_instrument_count": len(next_catalog.instruments),
        "catalog_binding_count": len(next_catalog.bindings),
        "reference_entitlement_count": len(parsed_consumer.requirements) - len(l2_requirements),
        "logical_l2_binding_count": len(l2_requirements),
        "physical_l2_book_count": len(l2_plan.bindings),
        "l2_shared_runtime_role_count": l2_plan.topology.service_role_count,
        "l2_runtime_roles": [list(item) for item in l2_plan.topology.runtime_roles],
        "managed_stale_l2_binding_count": len(managed_old_ids - generated_scope_ids),
        "source_changed": source_changed,
        "acquisition_changed": acquisition_changed,
        "promotion_scope_changed": scope_changed,
        "source_catalog_sha256": _sha256(source_result),
        "acquisition_plan_sha256": _sha256(acquisition_result),
        "promotion_scope_sha256": _sha256(promotion_result),
        "consumer_manifest_sha256": parsed_consumer.manifest_sha256,
    }
    return ReferenceL2Materialization(
        source_catalog=source_result,
        acquisition_plan=acquisition_result,
        promotion_scope=promotion_result,
        consumer_manifest=consumer_manifest,
        realtime_plan=realtime_plan,
        l2_plan=l2_plan,
        summary=summary,
    )

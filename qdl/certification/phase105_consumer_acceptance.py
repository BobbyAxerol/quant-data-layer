"""Manifest-bound paper scope for Phase 10.5-B no-order acceptance."""

from __future__ import annotations

from qdl.certification.phase103_consumer_acceptance import (
    ConsumerAcceptanceScope,
    build_manifest_acceptance_scope,
)
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


PHASE105_PAPER_CONSUMER_IDS = frozenset({
    "monitoring.multivenue.stable",
    "trading-system.paper.stable",
    "alpha.binance.paper.stable",
    "alpha.okx.paper.stable",
})


def build_release_consumer_acceptance_scope(
    release: StableReleaseRoutePlan,
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
) -> ConsumerAcceptanceScope:
    """Materialize exactly the release-manifest V2 paper products.

    The release contract owns routing. This helper only derives the public SDK
    read matrix from those routes, leaving `V1_PRIMARY` VN requirements outside
    the V2 probe and refusing any route/product drift.
    """
    selected = tuple(
        consumer
        for consumer in release.consumers
        if consumer.consumer_id in PHASE105_PAPER_CONSUMER_IDS
    )
    if frozenset(item.consumer_id for item in selected) != PHASE105_PAPER_CONSUMER_IDS:
        raise ValueError("Phase 10.5 paper consumers are incomplete")
    scope = build_manifest_acceptance_scope(
        (item.manifest_path for item in selected),
        catalog=catalog,
        acquisition=acquisition,
        expected_consumer_ids=PHASE105_PAPER_CONSUMER_IDS,
        schema="qdl.phase105.consumer-acceptance-scope.v1",
    )
    routes = {
        (consumer.consumer_id, product.requirement_key): product
        for consumer in selected
        for product in consumer.products
    }
    actual = {
        (product.consumer_id, requirement_key(product.requirement))
        for product in scope.products
    }
    expected = {
        identity
        for identity, product in routes.items()
        if product.route == "V2_PRIMARY"
    }
    if actual != expected:
        raise ValueError("Phase 10.5 acceptance products differ from V2 release routes")
    manifest_requirements = {
        consumer.consumer_id: {
            (item.instrument_uid, item.feed.value, item.interval): item
            for item in consumer.manifest.requirements
        }
        for consumer in selected
    }
    for excluded in scope.excluded:
        requirement = manifest_requirements[excluded.consumer_id].get((
            excluded.instrument_uid,
            excluded.feed.value,
            excluded.interval,
        ))
        product = (
            routes.get((excluded.consumer_id, requirement_key(requirement)))
            if requirement is not None
            else None
        )
        if product is None or product.route != "V1_PRIMARY":
            raise ValueError("Phase 10.5 V2 scope excluded an undeclared product")
    return scope

"""Manifest-driven scope for Phase 10.3 no-order V2 consumer acceptance.

The runtime probe deliberately owns no provider connection, service lifecycle,
broker state or route mutation.  It turns the governed consumer manifests and
the stable catalog into the exact products a later approved V2-primary packet
must prove to Trading System and alpha SDK identities.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Iterable

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.query import DataRequirement, FeedType, RecoveryPolicy
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


CRYPTO_VENUES = frozenset({"BINANCE", "OKX"})
PHASE103_FEEDS = frozenset({FeedType.TRADE, FeedType.QUOTE, FeedType.BAR})
PHASE103_CONSUMER_IDS = frozenset(
    {
        "trading-system.paper.stable",
        "alpha.binance.paper.stable",
    }
)


class DeliveryClass(StrEnum):
    """Whether a requirement has canonical replay or a fresh provider window."""

    DURABLE = "DURABLE"
    PROVIDER_PASS_THROUGH = "PROVIDER_PASS_THROUGH"


@dataclass(frozen=True, slots=True)
class AcceptanceProduct:
    consumer_id: str
    consumer_subject: str
    manifest_revision: int
    manifest_sha256: str
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    native_symbol: str
    provider: str
    feed: FeedType
    interval: str | None
    source_policy_id: str
    delivery: DeliveryClass
    binding_id: str | None
    requirement: DataRequirement

    @property
    def identity(self) -> tuple[str, str, str, str, str]:
        return (
            self.consumer_id,
            self.instrument_uid,
            self.feed.value,
            self.interval or "",
            self.source_policy_id,
        )

    def evidence(self) -> dict[str, object]:
        """Return compact, payload-free evidence suitable for an audit ledger."""
        return {
            "consumer_id": self.consumer_id,
            "manifest_revision": self.manifest_revision,
            "manifest_sha256": self.manifest_sha256,
            "instrument_uid": self.instrument_uid,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "market": self.market,
            "native_symbol": self.native_symbol,
            "provider": self.provider,
            "feed": self.feed.value,
            "interval": self.interval,
            "source_policy_id": self.source_policy_id,
            "delivery": self.delivery.value,
            "binding_id": self.binding_id,
        }


@dataclass(frozen=True, slots=True)
class ExcludedRequirement:
    consumer_id: str
    instrument_uid: str
    feed: FeedType
    interval: str | None
    reason: str

    def evidence(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "instrument_uid": self.instrument_uid,
            "feed": self.feed.value,
            "interval": self.interval,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ConsumerAcceptanceScope:
    schema: str
    catalog_revision: int
    acquisition_revision: int
    products: tuple[AcceptanceProduct, ...]
    excluded: tuple[ExcludedRequirement, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.phase103.consumer-acceptance-scope.v1":
            raise ValueError("consumer acceptance scope schema is unsupported")
        if min(self.catalog_revision, self.acquisition_revision) < 1:
            raise ValueError("consumer acceptance scope revisions must be positive")
        identities = [item.identity for item in self.products]
        if not identities or len(identities) != len(set(identities)):
            raise ValueError("consumer acceptance products must be non-empty and unique")
        if any(item.venue not in CRYPTO_VENUES for item in self.products):
            raise ValueError("consumer acceptance scope contains a non-crypto venue")
        if any(item.feed not in PHASE103_FEEDS for item in self.products):
            raise ValueError("consumer acceptance scope contains a later-phase feed")
        by_consumer: dict[str, list[AcceptanceProduct]] = {}
        for item in self.products:
            by_consumer.setdefault(item.consumer_id, []).append(item)
        if any(
            not any(item.delivery is DeliveryClass.DURABLE for item in products)
            for products in by_consumer.values()
        ):
            raise ValueError("every consumer acceptance scope requires durable realtime data")

    @property
    def sha256(self) -> str:
        payload = {
            "schema": self.schema,
            "catalog_revision": self.catalog_revision,
            "acquisition_revision": self.acquisition_revision,
            "products": [item.evidence() for item in self.products],
            "excluded": [item.evidence() for item in self.excluded],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def evidence(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "scope_sha256": self.sha256,
            "catalog_revision": self.catalog_revision,
            "acquisition_revision": self.acquisition_revision,
            "product_count": len(self.products),
            "durable_product_count": sum(
                item.delivery is DeliveryClass.DURABLE for item in self.products
            ),
            "pass_through_product_count": sum(
                item.delivery is DeliveryClass.PROVIDER_PASS_THROUGH
                for item in self.products
            ),
            "products": [item.evidence() for item in self.products],
            "excluded": [item.evidence() for item in self.excluded],
        }


def _manifest_paths(paths: Iterable[str | Path]) -> tuple[Path, ...]:
    values = tuple(Path(item) for item in paths)
    if not values:
        raise ValueError("at least one consumer manifest is required")
    if any(not item.is_file() for item in values):
        raise FileNotFoundError("consumer acceptance manifest is unavailable")
    if len({item.resolve() for item in values}) != len(values):
        raise ValueError("consumer acceptance manifest paths must be unique")
    return values


def _product_for_requirement(
    manifest: ConsumerManifest,
    requirement: DataRequirement,
    *,
    catalog: StableSourceCatalog,
    acquisition_by_id: dict[str, object],
) -> AcceptanceProduct | ExcludedRequirement:
    instrument = catalog.instrument_for(requirement.instrument_uid)
    identity = instrument.identity
    if identity.venue not in CRYPTO_VENUES:
        return ExcludedRequirement(
            consumer_id=manifest.consumer_id,
            instrument_uid=requirement.instrument_uid,
            feed=requirement.feed,
            interval=requirement.interval,
            reason="VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE",
        )
    if requirement.source_policy_id != "crypto_primary_v2":
        raise ValueError("crypto consumer requirement has an unapproved source policy")
    if requirement.feed not in PHASE103_FEEDS:
        raise ValueError("consumer requirement belongs to a later Phase 10 product")
    try:
        binding: StableSourceBinding = catalog.binding_for(requirement)
    except KeyError:
        if not (
            requirement.feed is FeedType.BAR
            and requirement.interval
            and requirement.recovery is RecoveryPolicy.FRESH_SNAPSHOT
        ):
            raise ValueError(
                "crypto consumer requirement has neither a durable binding nor "
                "an explicit provider pass-through policy"
            ) from None
        return AcceptanceProduct(
            consumer_id=manifest.consumer_id,
            consumer_subject=manifest.subject,
            manifest_revision=manifest.manifest_revision,
            manifest_sha256=manifest.manifest_sha256,
            instrument_uid=requirement.instrument_uid,
            instrument_id=instrument.instrument_id,
            venue=identity.venue,
            market=identity.market,
            native_symbol=instrument.native_symbol,
            provider=identity.venue,
            feed=requirement.feed,
            interval=requirement.interval,
            source_policy_id=requirement.source_policy_id,
            delivery=DeliveryClass.PROVIDER_PASS_THROUGH,
            binding_id=None,
            requirement=requirement,
        )
    acquisition = acquisition_by_id.get(binding.binding_id)
    if acquisition is None or not bool(getattr(acquisition, "enabled", False)):
        raise ValueError("durable consumer requirement resolves to a disabled acquisition")
    return AcceptanceProduct(
        consumer_id=manifest.consumer_id,
        consumer_subject=manifest.subject,
        manifest_revision=manifest.manifest_revision,
        manifest_sha256=manifest.manifest_sha256,
        instrument_uid=requirement.instrument_uid,
        instrument_id=instrument.instrument_id,
        venue=identity.venue,
        market=identity.market,
        native_symbol=instrument.native_symbol,
        provider=binding.provider,
        feed=requirement.feed,
        interval=requirement.interval,
        source_policy_id=requirement.source_policy_id,
        delivery=DeliveryClass.DURABLE,
        binding_id=binding.binding_id,
        requirement=requirement,
    )


def build_consumer_acceptance_scope(
    manifest_paths: Iterable[str | Path],
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
) -> ConsumerAcceptanceScope:
    """Build the exact Phase 10.3 crypto acceptance matrix from governed input.

    The catalog owns identity/lineage while the acquisition plan owns whether a
    canonical product is active. A future 15m alpha window is legal only when
    it states ``FRESH_SNAPSHOT`` explicitly; it is never promoted to durable
    replay by this helper.
    """
    manifests = tuple(
        ConsumerManifestLoader.load(path) for path in _manifest_paths(manifest_paths)
    )
    consumer_ids = frozenset(item.consumer_id for item in manifests)
    if len(consumer_ids) != len(manifests):
        raise ValueError("consumer acceptance manifests must have unique consumer IDs")
    if consumer_ids != PHASE103_CONSUMER_IDS:
        raise ValueError(
            "Phase 10.3 consumer acceptance requires exactly the Trading System "
            "and alpha workload manifests"
        )
    acquisition_by_id = {item.binding_id: item for item in acquisition.bindings}
    products: list[AcceptanceProduct] = []
    excluded: list[ExcludedRequirement] = []
    for manifest in manifests:
        for requirement in manifest.requirements:
            item = _product_for_requirement(
                manifest,
                requirement,
                catalog=catalog,
                acquisition_by_id=acquisition_by_id,
            )
            if isinstance(item, ExcludedRequirement):
                excluded.append(item)
            else:
                products.append(item)
    products.sort(key=lambda item: item.identity)
    excluded.sort(
        key=lambda item: (
            item.consumer_id,
            item.instrument_uid,
            item.feed.value,
            item.interval or "",
        )
    )
    return ConsumerAcceptanceScope(
        schema="qdl.phase103.consumer-acceptance-scope.v1",
        catalog_revision=catalog.catalog_revision,
        acquisition_revision=acquisition.revision,
        products=tuple(products),
        excluded=tuple(excluded),
    )

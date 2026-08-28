"""Manifest-bound V2-only acceptance scope for Reference and L2 products.

This is intentionally a small extension of the existing Phase-10.3 receipt
contract.  Reference rows stay fresh provider pass-through reads; books keep
their canonical snapshot/replay semantics.  It neither creates a route nor
permits a V1 fallback.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    DeliveryClass,
    _decimal_value,
)
from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.query import ConsumerGrade, DataRequirement, FeedType, RecoveryPolicy
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk import Grade
from qdl_sdk.reference import (
    BasisSeries,
    LongShortKind,
    ReferenceBatchResponse,
    ReferenceProduct,
    ReferenceRequirement,
)


REFERENCE_L2_CONSUMER_ID = "qdl.crypto.reference-l2.stable"
REFERENCE_FEEDS = frozenset({
    FeedType.FUNDING_RATE,
    FeedType.OPEN_INTEREST,
    FeedType.LONG_SHORT_RATIO,
    FeedType.TAKER_FLOW,
    FeedType.MARK_INDEX_PRICE,
    FeedType.CONTRACT_METADATA,
    FeedType.BASIS,
})
BOOK_FEEDS = frozenset({FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA})
_DAY_NS = 86_400_000_000_000
_FUNDING_NS = 8 * 3_600_000_000_000
_MILLISECOND_NS = 1_000_000
_FUNDING_SETTLEMENT_JITTER_NS = 60_000 * _MILLISECOND_NS
_REFERENCE_ACCEPTANCE_BATCH_SIZE = 12


@dataclass(frozen=True, slots=True)
class ReferenceAcceptanceProduct:
    consumer_id: str
    consumer_subject: str
    manifest_revision: int
    manifest_sha256: str
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    native_symbol: str
    requirement: DataRequirement
    sdk_requirement: ReferenceRequirement

    @property
    def identity(self) -> tuple[str, str, str, str, str]:
        return (
            self.consumer_id,
            self.instrument_uid,
            self.requirement.feed.value,
            self.requirement.interval or "",
            self.requirement.source_policy_id,
        )

    def evidence(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "manifest_revision": self.manifest_revision,
            "manifest_sha256": self.manifest_sha256,
            "instrument_uid": self.instrument_uid,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "market": self.market,
            "native_symbol": self.native_symbol,
            "feed": self.requirement.feed.value,
            "interval": self.requirement.interval,
            "source_policy_id": self.requirement.source_policy_id,
            "delivery": DeliveryClass.PROVIDER_PASS_THROUGH.value,
        }


@dataclass(frozen=True, slots=True)
class ReferenceL2AcceptanceScope:
    manifest_revision: int
    manifest_sha256: str
    catalog_revision: int
    acquisition_revision: int
    references: tuple[ReferenceAcceptanceProduct, ...]
    books: tuple[AcceptanceProduct, ...]

    def __post_init__(self) -> None:
        if len(self.references) != 55 or len(self.books) != 24:
            raise ValueError("Reference/L2 acceptance must contain exactly 55 reference and 24 book products")
        identities = [item.identity for item in self.references] + [item.identity for item in self.books]
        if len(identities) != len(set(identities)):
            raise ValueError("Reference/L2 acceptance contains duplicate product identities")

    @property
    def sha256(self) -> str:
        payload = {
            "schema": "qdl.reference-l2.consumer-acceptance.v1",
            "manifest_revision": self.manifest_revision,
            "manifest_sha256": self.manifest_sha256,
            "catalog_revision": self.catalog_revision,
            "acquisition_revision": self.acquisition_revision,
            "references": [item.evidence() for item in self.references],
            "books": [item.evidence() for item in self.books],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def _history_bounds(feed: FeedType, now_ns: int) -> tuple[int, int]:
    if now_ns <= _DAY_NS:
        raise ValueError("reference acceptance clock is invalid")
    if feed is FeedType.FUNDING_RATE:
        # Funding records are settlement observations. Binance may report the
        # raw settlement timestamp a few milliseconds either side of its clock
        # boundary, matching the adapter's existing 60-second coverage guard.
        settled = (now_ns // _FUNDING_NS) * _FUNDING_NS
        return settled - _FUNDING_NS, settled + _FUNDING_SETTLEMENT_JITTER_NS

    settled_day = (now_ns // _DAY_NS) * _DAY_NS
    if feed is FeedType.TAKER_FLOW:
        # Binance publishes daily taker flow at the next provider daily
        # boundary. Certify the latest known published completed period rather
        # than treating the not-yet-published next row as a partial history.
        return settled_day - 2 * _DAY_NS, settled_day - _DAY_NS - _MILLISECOND_NS
    if feed is FeedType.BASIS:
        # Binance documents these rows at period open. Request two fully
        # closed daily periods and exclude the current open period by one ms.
        return settled_day - 2 * _DAY_NS, settled_day - _MILLISECOND_NS

    # OI and long/short rows are documented at period end, so the latest
    # daily boundary is already the end of the last fully closed period.
    return settled_day - _DAY_NS, settled_day


def reference_acceptance_batches(
    products: tuple[ReferenceAcceptanceProduct, ...],
) -> tuple[tuple[ReferenceAcceptanceProduct, ...], ...]:
    """Split the real receipt without changing its complete product scope.

    Native Binance basis uses one Rust-admitted provider lane, so each request
    is intentionally isolated. Every other reference requirement stays in a
    bounded batch; no product is dropped or retried through another provider.
    """

    native_basis = tuple(
        product
        for product in products
        if product.venue == "BINANCE"
        and product.requirement.feed is FeedType.BASIS
        and product.sdk_requirement.basis_series is BasisSeries.NATIVE
    )
    ordinary = tuple(product for product in products if product not in native_basis)
    chunks = tuple(
        ordinary[offset:offset + _REFERENCE_ACCEPTANCE_BATCH_SIZE]
        for offset in range(0, len(ordinary), _REFERENCE_ACCEPTANCE_BATCH_SIZE)
    )
    return tuple((product,) for product in native_basis) + chunks


def _reference_request(requirement: DataRequirement, *, now_ns: int) -> ReferenceRequirement:
    try:
        product = ReferenceProduct(requirement.feed.value)
    except ValueError as error:
        raise ValueError("Reference/L2 product has no V2 reference mapping") from error
    values: dict[str, object] = {
        "instrument_uid": requirement.instrument_uid,
        "product": product,
        "consumer_grade": Grade.RESEARCH,
        "source_policy_id": requirement.source_policy_id,
        "max_freshness_ms": requirement.max_freshness_ms,
        "require_full_coverage": requirement.require_full_coverage,
        "deadline_ms": 60_000,
    }
    history_product = product in {
        ReferenceProduct.FUNDING_RATE,
        ReferenceProduct.LONG_SHORT_RATIO,
        ReferenceProduct.TAKER_FLOW,
        ReferenceProduct.BASIS,
    } or (
        product is ReferenceProduct.OPEN_INTEREST
        and requirement.interval is not None
    )
    if history_product:
        start_ns, end_ns = _history_bounds(requirement.feed, now_ns)
        values.update({
            "start_time_ns": start_ns,
            "end_time_ns": end_ns,
            "interval": requirement.interval,
            "limit": 3,
            "page_size": 3,
            "max_pages": 2,
        })
    if product is ReferenceProduct.LONG_SHORT_RATIO:
        values["long_short_kind"] = LongShortKind.GLOBAL_ACCOUNT
    if product is ReferenceProduct.BASIS:
        values["basis_series"] = BasisSeries.NATIVE
        values["basis_contract_type"] = "PERPETUAL"
    return ReferenceRequirement(**values)


def _scope_from_manifest(
    manifest: ConsumerManifest,
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    now_ns: int,
) -> ReferenceL2AcceptanceScope:
    if (
        manifest.consumer_id != REFERENCE_L2_CONSUMER_ID
        or manifest.rollback_contract != "V2"
        or manifest.execution_dependency != "FORBIDDEN"
        or manifest.environment != "paper"
    ):
        raise ValueError("Reference/L2 manifest identity or V2-only policy is invalid")
    acquisition_by_id = {item.binding_id: item for item in acquisition.bindings}
    references: list[ReferenceAcceptanceProduct] = []
    books: list[AcceptanceProduct] = []
    for requirement in manifest.requirements:
        if (
            requirement.source_policy_id != "crypto_liquid_v2"
            or requirement.consumer_grade is not ConsumerGrade.RESEARCH
            or requirement.feed not in REFERENCE_FEEDS | BOOK_FEEDS
        ):
            raise ValueError("Reference/L2 manifest requirement is outside its approved policy")
        instrument = catalog.instrument_for(requirement.instrument_uid)
        identity = instrument.identity
        if identity.venue not in {"BINANCE", "OKX"}:
            raise ValueError("Reference/L2 manifest has an unsupported venue")
        if requirement.feed in REFERENCE_FEEDS:
            if requirement.recovery is not RecoveryPolicy.FRESH_SNAPSHOT:
                raise ValueError("reference data must remain V2 fresh-provider pass-through")
            if (
                requirement.feed is FeedType.BASIS
                and identity.venue == "OKX"
                and identity.market == "SWAP"
            ):
                raise ValueError("OKX USDT swap basis must not cross USD future settlement")
            references.append(ReferenceAcceptanceProduct(
                consumer_id=manifest.consumer_id,
                consumer_subject=manifest.subject,
                manifest_revision=manifest.manifest_revision,
                manifest_sha256=manifest.manifest_sha256,
                instrument_uid=requirement.instrument_uid,
                instrument_id=instrument.instrument_id,
                venue=identity.venue,
                market=identity.market,
                native_symbol=instrument.native_symbol,
                requirement=requirement,
                sdk_requirement=_reference_request(requirement, now_ns=now_ns),
            ))
            continue
        if requirement.recovery is not RecoveryPolicy.SNAPSHOT_AND_REPLAY:
            raise ValueError("L2 book route must require snapshot and replay")
        try:
            binding = catalog.binding_for(requirement)
        except KeyError as error:
            raise ValueError("L2 book route has no canonical stable binding") from error
        active = acquisition_by_id.get(binding.binding_id)
        if active is None or not active.enabled:
            raise ValueError("L2 book route resolves to an inactive acquisition binding")
        books.append(AcceptanceProduct(
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
        ))
    references.sort(key=lambda item: item.identity)
    books.sort(key=lambda item: item.identity)
    return ReferenceL2AcceptanceScope(
        manifest_revision=manifest.manifest_revision,
        manifest_sha256=manifest.manifest_sha256,
        catalog_revision=catalog.catalog_revision,
        acquisition_revision=acquisition.revision,
        references=tuple(references),
        books=tuple(books),
    )


def build_reference_l2_acceptance_scope(
    manifest_path: str | Path,
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    now_ns: int,
) -> ReferenceL2AcceptanceScope:
    return _scope_from_manifest(
        ConsumerManifestLoader.load(manifest_path),
        catalog=catalog,
        acquisition=acquisition,
        now_ns=now_ns,
    )


def reference_evidence(
    product: ReferenceAcceptanceProduct,
    item,
    *,
    observed_at_ns: int,
) -> str:
    """Validate one typed V2 response and return a payload-free digest."""
    request = product.sdk_requirement
    if (
        item.instrument_uid != product.instrument_uid
        or item.product.value != request.product.value
        or item.status != "OK"
        or item.problem is not None
        or item.data is None
    ):
        raise ValueError("reference response identity or availability differs from demand")
    data = item.data
    if data.instrument_uid != product.instrument_uid or data.product.value != request.product.value:
        raise ValueError("reference payload identity differs from demand")
    if observed_at_ns - data.received_at_ns > (request.max_freshness_ms or 86_400_000) * 1_000_000:
        raise ValueError("reference response exceeds its governed freshness bound")
    coverage = data.coverage
    if request.require_full_coverage and (
        not coverage.complete_left or not coverage.complete_right or coverage.truncated
    ):
        raise ValueError("reference response is partial where full coverage is required")
    if not data.lineage or any(item.source_role != "REFERENCE" for item in data.lineage):
        raise ValueError("reference response has incomplete lineage")
    observations = []
    for observation in data.observations:
        if (
            observation.instrument_uid != product.instrument_uid
            or observation.product.value != request.product.value
            or not observation.fields
        ):
            raise ValueError("reference observation identity or fields differ from demand")
        fields = []
        for field in observation.fields:
            if not field.unit.strip():
                raise ValueError("reference field unit is blank")
            _decimal_value(field.value)
            fields.append((field.name, field.unit, field.value.coefficient, field.value.scale))
        observations.append((observation.observed_at_ns, tuple(fields)))
    if not observations:
        raise ValueError("reference response has no observations")
    payload = {
        "product": product.evidence(),
        "coverage": {
            "complete_left": coverage.complete_left,
            "complete_right": coverage.complete_right,
            "truncated": coverage.truncated,
            "terminal_reason": coverage.terminal_reason,
        },
        "lineage": [
            (item.provider, item.provider_endpoint, item.capability_name)
            for item in data.lineage
        ],
        "observations": observations,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def validate_reference_batch(
    products: tuple[ReferenceAcceptanceProduct, ...],
    response: ReferenceBatchResponse,
    *,
    observed_at_ns: int,
) -> tuple[str, ...]:
    if response.partial or response.error_count or response.success_count != len(products):
        raise ValueError("Reference/L2 V2 batch contains a typed provider failure")
    if len(response.results) != len(products):
        raise ValueError("Reference/L2 V2 batch cardinality differs from demand")
    return tuple(
        reference_evidence(product, item, observed_at_ns=observed_at_ns)
        for product, item in zip(products, response.results, strict=True)
    )

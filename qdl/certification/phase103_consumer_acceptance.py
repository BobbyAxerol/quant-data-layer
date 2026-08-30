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
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Iterable, Sequence

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.query import DataRequirement, FeedType, RecoveryPolicy, StalePolicy
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
    """The weakest source semantics a governed request is allowed to receive.

    A ``FRESH_SNAPSHOT`` BAR can use a non-authoritative provider window when
    its retained canonical partition is incomplete. If that partition is
    complete, the router may return the stronger durable primary result.
    """

    DURABLE = "DURABLE"
    PROVIDER_PASS_THROUGH = "PROVIDER_PASS_THROUGH"


def _allows_provider_snapshot(requirement: DataRequirement) -> bool:
    return (
        requirement.feed is FeedType.BAR
        and bool(requirement.interval)
        and requirement.recovery is RecoveryPolicy.FRESH_SNAPSHOT
    )


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
        if self.schema not in {
            "qdl.phase103.consumer-acceptance-scope.v1",
            "qdl.phase105.consumer-acceptance-scope.v1",
        }:
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
        if not _allows_provider_snapshot(requirement):
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
        delivery=(
            DeliveryClass.PROVIDER_PASS_THROUGH
            if _allows_provider_snapshot(requirement)
            else DeliveryClass.DURABLE
        ),
        binding_id=binding.binding_id,
        requirement=requirement,
    )


def build_manifest_acceptance_scope(
    manifest_paths: Iterable[str | Path],
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    expected_consumer_ids: frozenset[str],
    schema: str,
) -> ConsumerAcceptanceScope:
    """Build a governed crypto acceptance matrix from registered manifests.

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
    if consumer_ids != expected_consumer_ids:
        raise ValueError(
            "consumer acceptance requires exactly the governed consumer IDs"
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
        schema=schema,
        catalog_revision=catalog.catalog_revision,
        acquisition_revision=acquisition.revision,
        products=tuple(products),
        excluded=tuple(excluded),
    )


def build_consumer_acceptance_scope(
    manifest_paths: Iterable[str | Path],
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
) -> ConsumerAcceptanceScope:
    """Build the exact Phase 10.3 crypto acceptance matrix from governed input."""
    return build_manifest_acceptance_scope(
        manifest_paths,
        catalog=catalog,
        acquisition=acquisition,
        expected_consumer_ids=PHASE103_CONSUMER_IDS,
        schema="qdl.phase103.consumer-acceptance-scope.v1",
    )


def sdk_requirement(product: AcceptanceProduct):
    """Map one governed domain requirement to the public SDK without loss.

    Importing the SDK at this narrow boundary preserves the service-domain
    module's provider neutrality while ensuring the operator probe exercises
    the exact public client contract used by Trading System and alpha code.
    """
    from qdl_sdk import (
        BarRevisionPolicy as SdkBarRevisionPolicy,
        DataRequirement as SdkDataRequirement,
        Feed as SdkFeed,
        GapPolicy as SdkGapPolicy,
        Grade,
        RecoveryPolicy as SdkRecoveryPolicy,
        StalePolicy as SdkStalePolicy,
    )

    item = product.requirement
    return SdkDataRequirement(
        instrument_uid=item.instrument_uid,
        feed=SdkFeed(item.feed.value),
        consumer_grade=Grade(item.consumer_grade.value),
        source_policy_id=item.source_policy_id,
        interval=item.interval,
        warmup_limit=item.warmup_limit,
        max_freshness_ms=item.max_freshness_ms,
        event_recency_policy=(
            SdkStalePolicy(item.event_recency_policy.value)
            if item.event_recency_policy is not None
            else None
        ),
        max_session_liveness_ms=item.max_session_liveness_ms,
        require_full_coverage=item.require_full_coverage,
        require_final_bars=item.require_final_bars,
        stale_policy=SdkStalePolicy(item.stale_policy.value),
        gap_policy=SdkGapPolicy(item.gap_policy.value),
        recovery=SdkRecoveryPolicy(item.recovery.value),
        bar_revision_policy=SdkBarRevisionPolicy(item.bar_revision_policy.value),
    )


def _decimal_value(value) -> Decimal:
    """Verify that the public decimal's text and coefficient/scale agree."""
    try:
        text = Decimal(str(value.source_text))
        coefficient = Decimal(str(value.coefficient)).scaleb(-int(value.scale))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("V2 receipt contains an invalid canonical decimal") from error
    if not text.is_finite() or text != coefficient:
        raise ValueError("V2 receipt decimal text and coefficient/scale disagree")
    return text


def content_fingerprint(view) -> str:
    """Hash content that must agree between replicas, never retain payload."""
    payload = view.payload.model_dump(mode="json")
    value = {
        "instrument_uid": view.instrument_uid,
        "instrument_id": view.instrument_id,
        "feed": view.feed.value,
        "interval": view.interval,
        "revision": view.revision,
        "payload": payload,
        "provider": view.source.provider,
        "source_id": view.source.source_id,
        "source_role": view.source.source_role,
        "authoritative": view.source.authoritative,
        "policy_id": view.quality.policy_id,
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_payload(product: AcceptanceProduct, view) -> None:
    payload = view.payload
    feed = product.feed
    if feed is FeedType.TRADE:
        if _decimal_value(payload.price) <= 0 or _decimal_value(payload.quantity) <= 0:
            raise ValueError("V2 TRADE receipt contains a non-positive price or quantity")
        if not payload.native_trade_id:
            raise ValueError("V2 TRADE receipt has no native trade identity")
        return
    if feed is FeedType.QUOTE:
        bid_price = _decimal_value(payload.bid_price)
        ask_price = _decimal_value(payload.ask_price)
        if (
            bid_price <= 0
            or ask_price <= 0
            or bid_price > ask_price
            or _decimal_value(payload.bid_quantity) < 0
            or _decimal_value(payload.ask_quantity) < 0
        ):
            raise ValueError("V2 QUOTE receipt violates best-bid/best-offer invariants")
        return
    if feed is FeedType.BAR:
        open_price = _decimal_value(payload.open)
        high_price = _decimal_value(payload.high)
        low_price = _decimal_value(payload.low)
        close_price = _decimal_value(payload.close)
        volume = _decimal_value(payload.volume)
        if (
            low_price <= 0
            or volume < 0
            or not low_price <= open_price <= high_price
            or not low_price <= close_price <= high_price
        ):
            raise ValueError("V2 BAR receipt violates OHLCV domain invariants")
        if product.requirement.require_final_bars and payload.lifecycle.value not in {
            "FINAL",
            "REVISED",
        }:
            raise ValueError("V2 BAR receipt is not final")
        return
    if feed is FeedType.BOOK_SNAPSHOT:
        if (
            not payload.native_sequence
            or payload.depth < 1
            or not payload.levels
            or not payload.sequence_verified
            or payload.book_generation < 1
        ):
            raise ValueError("V2 BOOK_SNAPSHOT receipt lacks verified snapshot state")
        for level in payload.levels:
            if (
                _decimal_value(level.price) <= 0
                or _decimal_value(level.quantity) <= 0
                or not str(level.quantity_unit.value).strip()
            ):
                raise ValueError("V2 BOOK_SNAPSHOT receipt has an invalid level")
        return
    if feed is FeedType.BOOK_DELTA:
        if (
            not payload.native_sequence_start
            or not payload.native_sequence_end
            or not payload.snapshot_sequence
            or not payload.sequence_verified
            or payload.book_generation < 1
        ):
            raise ValueError("V2 BOOK_DELTA receipt lacks verified sequence state")
        for level in payload.updates:
            if (
                _decimal_value(level.price) <= 0
                or _decimal_value(level.quantity) < 0
                or not str(level.quantity_unit.value).strip()
            ):
                raise ValueError("V2 BOOK_DELTA receipt has an invalid update")
        return
    raise ValueError("Phase 10.3 receipt received an unsupported feed")


def validate_product_view(
    product: AcceptanceProduct,
    view,
    *,
    require_current_quality: bool = True,
) -> None:
    """Validate one governed view.

    Historical BAR warmup rows retain identity, provenance, gap and payload
    checks, but cannot individually satisfy a *current* freshness SLA. The
    latest closed BAR is the row that carries current/live/execution quality.
    """
    if (
        view.instrument_uid != product.instrument_uid
        or view.instrument_id != product.instrument_id
        or view.feed.value != product.feed.value
        or view.interval != product.interval
        or view.quality.policy_id != product.source_policy_id
    ):
        raise ValueError("V2 receipt identity, feed, interval or policy mismatches demand")
    if view.quality.gap_open or not view.quality.complete:
        raise ValueError("V2 receipt has an unresolved gap or incomplete coverage")
    requirement = product.requirement
    max_freshness_ms = requirement.max_freshness_ms
    observed_quiet_trade = (
        product.feed is FeedType.TRADE
        and requirement.effective_event_recency_policy is StalePolicy.OBSERVE
        and view.quality.event_recency_state == "STALE"
        and view.quality.provider_session_state == "LIVE"
    )
    if (
        require_current_quality
        and max_freshness_ms is not None
        and view.quality.freshness_ms > max_freshness_ms
        and not observed_quiet_trade
    ):
        raise ValueError("V2 receipt exceeds the governed freshness bound")
    if requirement.max_session_liveness_ms is not None and (
        view.quality.provider_session_state != "LIVE"
        or view.quality.provider_session_liveness_ms is None
        or view.quality.provider_session_liveness_ms
        > requirement.max_session_liveness_ms
    ):
        raise ValueError("V2 receipt provider session liveness differs from demand")
    if product.delivery is DeliveryClass.DURABLE:
        if (
            view.source.venue != product.venue
            or view.source.provider != product.provider
            or view.source.source_role != "PRIMARY"
            or not view.source.authoritative
            or (require_current_quality and view.quality.state != "LIVE")
        ):
            raise ValueError("durable V2 receipt is not authoritative and live")
        if (
            require_current_quality
            and requirement.consumer_grade.value == "EXECUTION"
            and not view.quality.execution_eligible
            and not observed_quiet_trade
        ):
            raise ValueError("execution-grade durable V2 receipt is not eligible")
    elif product.delivery is DeliveryClass.PROVIDER_PASS_THROUGH:
        if not _allows_provider_snapshot(product.requirement):
            raise ValueError("provider pass-through receipt has invalid authority semantics")
        if view.source.venue != product.venue:
            raise ValueError("provider pass-through receipt has an invalid venue")
        if product.binding_id is not None and view.source.provider != product.provider:
            raise ValueError("provider pass-through receipt has an invalid provider")
        durable_upgrade = (
            view.source.source_role == "PRIMARY" and view.source.authoritative
        )
        if durable_upgrade:
            if require_current_quality and view.quality.state != "LIVE":
                raise ValueError("durable upgrade receipt is not live")
        elif (
            view.source.source_role != "REFERENCE"
            or view.source.authoritative
            or view.quality.execution_eligible
        ):
            raise ValueError("provider pass-through receipt has invalid authority semantics")
    else:  # pragma: no cover - protected by DeliveryClass and scope construction.
        raise ValueError("V2 receipt has an unknown delivery class")
    _validate_payload(product, view)


def validate_replica_views(
    product: AcceptanceProduct,
    primary_view,
    secondary_view,
) -> tuple[str, str]:
    """Validate two typed query results without assuming live ticks are equal.

    A final BAR is immutable at its declared revision, so its canonical content
    hash must agree exactly. TRADE/QUOTE snapshots may advance between the two
    bounded requests; only their governed typed identity and quality contract
    must agree, while each content hash is recorded independently.
    """
    validate_product_view(product, primary_view)
    validate_product_view(product, secondary_view)
    primary_hash = content_fingerprint(primary_view)
    secondary_hash = content_fingerprint(secondary_view)
    if product.feed is FeedType.BAR and primary_hash != secondary_hash:
        raise ValueError("V2 query replicas diverged on a final BAR revision")
    return primary_hash, secondary_hash


def validate_resume_offsets(*, acknowledged_offset: int, resumed_offset: int) -> None:
    """A resumed durable stream must move forward, never replay stale state."""
    if acknowledged_offset < 0 or resumed_offset <= acknowledged_offset:
        raise ValueError("V2 stream cursor resume is not strictly increasing")


def compact_receipt_evidence(
    product: AcceptanceProduct,
    *,
    primary_hash: str,
    secondary_hash: str | None,
    primary_latency_ms: float,
    secondary_latency_ms: float | None = None,
    acknowledged_offset: int | None = None,
    resumed_offset: int | None = None,
) -> dict[str, object]:
    """Return audit-safe evidence; hash content and cursors rather than expose them."""
    if len(primary_hash) != 64 or (secondary_hash is not None and len(secondary_hash) != 64):
        raise ValueError("V2 receipt fingerprint is invalid")
    if primary_latency_ms < 0 or (secondary_latency_ms is not None and secondary_latency_ms < 0):
        raise ValueError("V2 receipt latency is invalid")
    if (acknowledged_offset is None) != (resumed_offset is None):
        raise ValueError("V2 receipt resume evidence is incomplete")
    if acknowledged_offset is not None:
        assert resumed_offset is not None
        validate_resume_offsets(
            acknowledged_offset=acknowledged_offset,
            resumed_offset=resumed_offset,
        )
    evidence = product.evidence()
    evidence.update(
        {
            "primary_content_sha256": primary_hash,
            "secondary_content_sha256": secondary_hash,
            "primary_latency_ms": round(primary_latency_ms, 3),
            "secondary_latency_ms": (
                round(secondary_latency_ms, 3)
                if secondary_latency_ms is not None
                else None
            ),
            "acknowledged_offset": acknowledged_offset,
            "resumed_offset": resumed_offset,
        }
    )
    return evidence


def warmup_content_fingerprint(rows: Sequence[object]) -> str:
    """Hash ordered immutable final-BAR rows for a cross-replica warmup check."""
    if not rows:
        raise ValueError("V2 BAR warmup is empty")
    values = [content_fingerprint(item) for item in rows]
    return hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode()
    ).hexdigest()


def validate_final_bar_warmup_windows(
    primary_rows: Sequence[object],
    secondary_rows: Sequence[object],
) -> dict[str, object]:
    """Prove replica parity across one bounded final-BAR window rollover.

    Query replicas can independently materialize a newly closed bar between
    sequential warmup requests.  The immutable overlap must still be exactly
    equal, and only a one-row opposite head/tail shift is acceptable.  This is
    receipt-only validation; it does not alter query or provider semantics.
    """
    primary = tuple(primary_rows)
    secondary = tuple(secondary_rows)
    if not primary or not secondary:
        raise ValueError("V2 BAR warmup is empty")
    if len(primary) != len(secondary):
        raise ValueError("V2 BAR warmup replica row counts differ")

    def by_open_time(
        rows: tuple[object, ...],
        label: str,
    ) -> tuple[tuple[int, ...], dict[int, object]]:
        ordered: list[int] = []
        indexed: dict[int, object] = {}
        for view in rows:
            open_time_ns = getattr(getattr(view, "payload", None), "open_time_ns", None)
            if (
                isinstance(open_time_ns, bool)
                or not isinstance(open_time_ns, int)
                or open_time_ns <= 0
            ):
                raise ValueError(f"V2 {label} BAR warmup has an invalid open time")
            if ordered and open_time_ns <= ordered[-1]:
                raise ValueError(f"V2 {label} BAR warmup is duplicate or out of order")
            ordered.append(open_time_ns)
            indexed[open_time_ns] = view
        return tuple(ordered), indexed

    primary_times, primary_by_open = by_open_time(primary, "primary")
    secondary_times, secondary_by_open = by_open_time(secondary, "secondary")
    common_times = tuple(item for item in primary_times if item in secondary_by_open)
    if not common_times:
        raise ValueError("V2 BAR warmup replicas have no immutable common window")

    primary_indexes = tuple(primary_times.index(item) for item in common_times)
    secondary_indexes = tuple(secondary_times.index(item) for item in common_times)

    def contiguous(indexes: tuple[int, ...]) -> bool:
        return indexes == tuple(range(indexes[0], indexes[-1] + 1))

    if not contiguous(primary_indexes) or not contiguous(secondary_indexes):
        raise ValueError("V2 BAR warmup replica overlap is not contiguous")

    primary_only_indexes = tuple(
        index for index, item in enumerate(primary_times) if item not in secondary_by_open
    )
    secondary_only_indexes = tuple(
        index for index, item in enumerate(secondary_times) if item not in primary_by_open
    )
    if not primary_only_indexes and not secondary_only_indexes:
        comparison = "EXACT"
    else:
        if (
            len(primary_only_indexes) != 1
            or len(secondary_only_indexes) != 1
            or len(common_times) != len(primary_times) - 1
        ):
            raise ValueError("V2 BAR warmup replica shift exceeds one final BAR")
        primary_index = primary_only_indexes[0]
        secondary_index = secondary_only_indexes[0]
        last_index = len(primary_times) - 1
        if (primary_index, secondary_index) not in {(0, last_index), (last_index, 0)}:
            raise ValueError("V2 BAR warmup replica shift is not an opposite window boundary")
        comparison = "SINGLE_FINAL_BAR_ROLLOVER"

    for open_time_ns in common_times:
        if content_fingerprint(primary_by_open[open_time_ns]) != content_fingerprint(
            secondary_by_open[open_time_ns]
        ):
            raise ValueError("V2 BAR warmup replicas differ on immutable common BAR content")

    common_rows = tuple(primary_by_open[item] for item in common_times)
    return {
        "comparison": comparison,
        "primary_content_sha256": warmup_content_fingerprint(primary),
        "secondary_content_sha256": warmup_content_fingerprint(secondary),
        "common_content_sha256": warmup_content_fingerprint(common_rows),
        "primary_row_count": len(primary),
        "secondary_row_count": len(secondary),
        "common_row_count": len(common_rows),
        "tail_skew_rows": len(primary_only_indexes),
    }

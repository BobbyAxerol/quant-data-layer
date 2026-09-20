#!/usr/bin/env python3
"""Bounded no-order C2 acceptance for the four Phase 10.5 paper consumers.

The probe reads V2 through the shared SDK, then performs a local route-selection
drill only for manifest-authorized V1 cached reads before returning to V2. It
does not install or mutate a deployed Trading System/alpha route controller.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import hashlib
import json
from math import ceil
import resource
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    DeliveryClass,
    content_fingerprint,
    sdk_requirement,
    validate_final_bar_warmup_windows,
    validate_product_view,
    validate_replica_views,
    warmup_content_fingerprint,
)
from qdl.certification.phase105_consumer_acceptance import (
    PHASE105_PAPER_CONSUMER_IDS,
    build_release_consumer_acceptance_scope,
)
from qdl.certification.reference_l2_acceptance import (
    ReferenceAcceptanceProduct,
    acceptance_transport_timeout_seconds,
    is_rust_admitted_native_basis,
    reference_acceptance_batches,
    reference_evidence,
    reference_quality,
    reference_request_for_requirement,
)
from qdl.certification.phase105_fallback import (
    PHASE105_PAPER_CONSUMER_ORDER,
    blocked_fallback_identities,
    build_fallback_return_receipt,
    build_v1_fallback_probes,
    validate_v1_fallback_payload,
    validate_v1_provenance,
    validate_v1_runtime_binding,
)
from qdl.adapters.intervals import canonical_interval_ms
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.query import ConsumerGrade, FeedType, StalePolicy
from qdl.certification.phase105_release_observations import compact_view_quality
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_consumer_receipt_acceptance import (
    C2StatusEvidenceError,
    _c2_requirement,
    _certify_product,
    _client,
    compact_feed_status,
    _identity,
    _query_product,
)
from scripts.phasec36_reference_l2_consumer_acceptance import (
    _reference_batch_until_terminal,
)
from qdl_sdk.errors import DataLayerError


IDENTITY_PREFIXES = {
    "monitoring.multivenue.stable": "monitoring",
    "trading-system.paper.stable": "trading",
    "alpha.binance.paper.stable": "alpha-binance",
    "alpha.okx.paper.stable": "alpha-okx",
}

# C2 is a bounded acceptance against the fixed stable reader pair. These are
# replica aliases, not leader names: the SDK must receive both so its existing
# UNAVAILABLE retry can reach the current lease owner.
_C2_STREAM_TARGETS = frozenset({
    "qdl-v2-stream-a:8210",
    "qdl-v2-stream-b:8210",
})
_MAX_REFERENCE_BATCH_CONCURRENCY = 4
_C2_REQUEST_QUOTA_FRACTION = 0.75
_C2_QUOTA_WINDOW_MARGIN_SECONDS = 0.05
_C2_CLOSING_REVALIDATION_MAX_SECONDS = 120.0
_STRICT_BAR_BATCH_SHAPES = (1, 8, 16, 32)


def _evidence_sha256(value: Mapping[str, object]) -> str:
    """Hash already payload-free evidence without retaining its value twice."""

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class IdentityFiles:
    certificate: str
    private_key: str
    jwt_private_key: str
    jwt_key_id: str


class C2OpeningCapacityError(RuntimeError):
    """Payload-free evidence when C2 exceeds its declared opening protocol."""

    def __init__(self, code: str, details: Mapping[str, object]) -> None:
        super().__init__(f"Phase 10.5 C2 opening capacity failure: {code}")
        self.evidence = {
            "schema": "qdl.phase105.c2-opening-capacity-failure.v1",
            "code": code,
            "details": dict(details),
            "payload_recorded": False,
        }


class C2ProductAcceptanceError(RuntimeError):
    """One compact, payload-free product failure for an operator C2 receipt."""

    def __init__(self, product: AcceptanceProduct, error: C2StatusEvidenceError) -> None:
        super().__init__(
            "Phase 10.5 V2 identity receipt failed "
            f"consumer={product.consumer_id} instrument={product.instrument_id} "
            f"feed={product.feed.value} interval={product.interval}"
        )
        self.evidence = {
            "schema": "qdl.phase105.c2-product-failure.v1",
            "product": product.evidence(),
            "replica": error.replica or "unknown",
            "error_code": error.code,
            "typed_status": error.status_evidence,
            "quality_sha256": _evidence_sha256(error.status_evidence),
            "payload_recorded": False,
        }


class C2ReferenceProductError(RuntimeError):
    """One compact, payload-free reference failure for an operator C2 receipt."""

    def __init__(
        self,
        product: ReferenceAcceptanceProduct,
        *,
        replica: str,
        error: ValueError,
    ) -> None:
        super().__init__(
            "Phase 10.5 V2 reference receipt failed "
            f"consumer={product.consumer_id} instrument={product.instrument_id} "
            f"feed={product.requirement.feed.value} replica={replica}"
        )
        self.evidence = {
            "schema": "qdl.phase105.c2-reference-product-failure.v1",
            "product": product.evidence(),
            "replica": replica,
            "error_type": type(error).__name__,
            "reason": str(error)[:240],
            "payload_recorded": False,
        }


class C2ClosingBatchError(RuntimeError):
    """Compact, payload-free evidence for a closing batch transport failure."""

    def __init__(
        self,
        *,
        consumer_id: str,
        replica: str,
        products: tuple[AcceptanceProduct, ...],
        error: Exception,
        status_observations: list[dict[str, object]],
        batch_item_problems: list[dict[str, object]] | None = None,
    ) -> None:
        if not products or any(item.consumer_id != consumer_id for item in products):
            raise ValueError("Phase 10.5 closing batch failure has an invalid consumer scope")
        digest = hashlib.sha256(
            json.dumps(
                [item.identity for item in products],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        super().__init__(
            "Phase 10.5 V2 closing batch failed "
            f"consumer={consumer_id} replica={replica} size={len(products)}"
        )
        self.evidence = {
            "schema": "qdl.phase105.c2-closing-batch-failure.v1",
            "consumer_id": consumer_id,
            "replica": replica,
            "batch_size": len(products),
            "batch_identity_sha256": digest,
            "transport_error": type(error).__name__,
            "transport_error_code": getattr(error, "code", None),
            "transport_retryable": bool(getattr(error, "retryable", False)),
            "transport_detail_sha256": hashlib.sha256(
                str(getattr(error, "detail", error)).encode()
            ).hexdigest(),
            "typed_status": status_observations,
            "batch_item_problems": list(batch_item_problems or ()),
            "payload_recorded": False,
        }


class C2BatchShapeError(RuntimeError):
    """Add batch-shape context to one bounded, payload-free V2 failure."""

    def __init__(
        self,
        error: Exception,
        *,
        stage: str,
        batch_shape: int,
        window_index: int,
        products: tuple[AcceptanceProduct, ...],
    ) -> None:
        if batch_shape < 1 or window_index < 0 or not products:
            raise ValueError("Phase 10.5 batch-shape failure context is invalid")
        if isinstance(error, C2ClosingBatchError):
            source = dict(error.evidence)
        else:
            source = {
                "consumer_id": products[0].consumer_id,
                "replica": "both",
                "batch_size": len(products),
                "batch_identity_sha256": _batch_identity_sha256(products),
                "transport_error": type(error).__name__,
                "transport_error_code": getattr(error, "code", None),
                "transport_retryable": bool(getattr(error, "retryable", False)),
                "transport_detail_sha256": hashlib.sha256(str(error).encode()).hexdigest(),
                "typed_status": [],
                "batch_item_problems": [],
                "payload_recorded": False,
            }
        super().__init__(
            "Phase 10.5 strict BAR batch-shape matrix failed "
            f"stage={stage} shape={batch_shape} window={window_index}"
        )
        self.evidence = {
            **source,
            "schema": "qdl.phase105.strict-bar-batch-shape-failure.v1",
            "stage": stage,
            "batch_shape": batch_shape,
            "window_index": window_index,
            "payload_recorded": False,
        }


class _C2ConsumerRequestPacer:
    """Keep the disposable C2 probe below one manifest's real REST quota.

    The stable data-plane limit is enforced by Redis in wall-clock minute
    buckets.  C2 shares an identity across both query replicas, so one local
    pacer must serialize every REST request for that identity.  It deliberately
    uses only 75% of the sealed quota, leaving headroom for an independently
    running paper consumer without changing its production allowance.
    """

    def __init__(
        self,
        requests_per_minute: int,
        *,
        safety_fraction: float = _C2_REQUEST_QUOTA_FRACTION,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        if requests_per_minute < 1:
            raise ValueError("C2 manifest request quota must be positive")
        if not 0.0 < safety_fraction < 1.0:
            raise ValueError("C2 request quota fraction must be between zero and one")
        self.requests_per_minute = requests_per_minute
        self.safe_requests_per_minute = max(
            1, int(requests_per_minute * safety_fraction)
        )
        self._seconds_per_request = 60.0 / self.safe_requests_per_minute
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._operation_counts = Counter()
        self._next_at: float | None = None
        self._request_count = 0
        self._wait_seconds = 0.0
        self._window_wait_seconds = 0.0

    async def wait_for_clean_window(self) -> float:
        """Start C2 only after the next shared Redis quota-minute boundary."""

        now = self._clock()
        target = (int(now // 60.0) + 1) * 60.0 + _C2_QUOTA_WINDOW_MARGIN_SECONDS
        wait_seconds = max(0.0, target - now)
        sleeper = asyncio.sleep if self._sleep is None else self._sleep
        if wait_seconds > 0:
            await sleeper(wait_seconds)
        async with self._lock:
            self._next_at = self._clock()
            self._window_wait_seconds += wait_seconds
        return wait_seconds

    async def acquire(self, operation: str = "UNSPECIFIED") -> None:
        """Reserve one real REST request without borrowing quota from a peer."""

        operation = str(operation).strip()
        if not operation:
            raise ValueError("C2 operation name is required")
        async with self._lock:
            now = self._clock()
            target = now if self._next_at is None else max(now, self._next_at)
            self._next_at = target + self._seconds_per_request
            self._request_count += 1
            self._operation_counts[operation] += 1
            wait_seconds = max(0.0, target - now)
            self._wait_seconds += wait_seconds
        sleeper = asyncio.sleep if self._sleep is None else self._sleep
        if wait_seconds > 0:
            await sleeper(wait_seconds)

    def evidence(self) -> dict[str, object]:
        return {
            "requests_per_minute": self.requests_per_minute,
            "c2_safe_requests_per_minute": self.safe_requests_per_minute,
            "c2_request_count": self._request_count,
            "c2_pacing_wait_seconds": round(self._wait_seconds, 3),
            "c2_clean_window_wait_seconds": round(self._window_wait_seconds, 3),
            "c2_operation_counts": dict(sorted(self._operation_counts.items())),
        }


class _PacedQueryTransport:
    """Acceptance-only adapter that charges every C2 REST call to one pacer."""

    def __init__(self, delegate, pacer: _C2ConsumerRequestPacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def _call(self, name: str, *args, **kwargs):
        operation = "REFERENCE_BATCH" if name == "reference_batch" else "QUERY_READ"
        await self._pacer.acquire(operation)
        return await getattr(self._delegate, name)(*args, **kwargs)

    async def warmup(self, *args, **kwargs):
        return await self._call("warmup", *args, **kwargs)

    async def warmup_batch(self, *args, **kwargs):
        return await self._call("warmup_batch", *args, **kwargs)

    async def reference_batch(self, *args, **kwargs):
        return await self._call("reference_batch", *args, **kwargs)

    async def snapshot(self, *args, **kwargs):
        return await self._call("snapshot", *args, **kwargs)

    async def feed_status(self, *args, **kwargs):
        return await self._call("feed_status", *args, **kwargs)

    async def instruments(self, *args, **kwargs):
        return await self._call("instruments", *args, **kwargs)

    async def instrument(self, *args, **kwargs):
        return await self._call("instrument", *args, **kwargs)

    async def close(self) -> None:
        await self._delegate.close()


class _PacedStreamTransport:
    """Charge each C2 stream open to the same per-identity request budget."""

    def __init__(self, delegate, pacer: _C2ConsumerRequestPacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def subscribe(self, *args, **kwargs):
        # `subscribe` is an async iterator. Reserving at iterator start covers
        # both the initial stream and every SDK reconnect without changing the
        # public stream contract.
        await self._pacer.acquire("STREAM_SUBSCRIBE")
        async for item in self._delegate.subscribe(*args, **kwargs):
            yield item

    async def close(self) -> None:
        await self._delegate.close()


def _paced_client_factory(pacer: _C2ConsumerRequestPacer):
    """Preserve the SDK contract while pacing C2 REST and stream opens only."""

    def create(identity, *, base_url, grpc_target, cursor_path, timeout_seconds):
        client = _client(
            identity,
            base_url=base_url,
            grpc_target=grpc_target,
            cursor_path=cursor_path,
            timeout_seconds=timeout_seconds,
        )
        client.query_transport = _PacedQueryTransport(client.query_transport, pacer)
        client.stream_transport = _PacedStreamTransport(client.stream_transport, pacer)
        return client

    return create


def _quota_pacers(
    release: StableReleaseRoutePlan,
    consumer_ids: tuple[str, ...],
) -> dict[str, _C2ConsumerRequestPacer]:
    routes = {item.consumer_id: item for item in release.consumers}
    if any(consumer_id not in routes for consumer_id in consumer_ids):
        raise ValueError("Phase 10.5 C2 consumer quota manifest is unavailable")
    return {
        consumer_id: _C2ConsumerRequestPacer(
            routes[consumer_id].manifest.quotas.requests_per_minute
        )
        for consumer_id in consumer_ids
    }


async def _wait_for_clean_quota_windows(
    pacers: dict[str, _C2ConsumerRequestPacer],
) -> float:
    """Align all governed identities with a fresh server-side quota minute."""

    if not pacers:
        raise ValueError("Phase 10.5 C2 requires at least one quota pacer")
    waits = await asyncio.gather(*(item.wait_for_clean_window() for item in pacers.values()))
    return max(waits)


async def _wait_for_minimum_observation(
    *,
    started_monotonic: float,
    observation_seconds: float,
) -> float:
    """Hold a real C2 observation window before full closing revalidation."""

    if observation_seconds <= 0:
        raise ValueError("C2 observation duration must be positive")
    remaining = started_monotonic + observation_seconds - time.monotonic()
    if remaining > 0:
        await asyncio.sleep(remaining)
    elapsed = time.monotonic() - started_monotonic
    if elapsed + 0.001 < observation_seconds:
        raise AssertionError("Phase 10.5 C2 observation ended before its declared duration")
    return elapsed


def _authority(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5 authority record cannot be read") from error
    if not isinstance(value, dict) or value.get("schema") != "qdl.authority-record.v1":
        raise ValueError("Phase 10.5 authority record is invalid")
    if value.get("mode") != "RUST_PRIMARY" or value.get("public_write_allowed") is not False:
        raise ValueError("Phase 10.5 identity acceptance requires fenced RUST_PRIMARY")
    return value


def _reference_batch_concurrency(observation_concurrency: int) -> int:
    """Keep C2 provider reads within the shared ReferenceBatch lane budget."""

    if observation_concurrency < 1:
        raise ValueError("C2 observation concurrency must be positive")
    return min(observation_concurrency, _MAX_REFERENCE_BATCH_CONCURRENCY)


def _reference_transport_timeout_seconds(
    products: tuple[ReferenceAcceptanceProduct, ...],
    *,
    generic_timeout_seconds: float,
) -> float:
    """Keep the disposable client alive through its declared provider contract.

    Durable query/stream reads retain the generic C2 timeout.  Reference
    batches carry their own bounded provider deadline, so a client must not
    cancel a valid request before that contract plus the response margin.
    """

    if not products:
        raise ValueError("C2 reference transport requires at least one product")
    declared_timeout_seconds = max(
        item.sdk_requirement.deadline_ms / 1_000 for item in products
    )
    return acceptance_transport_timeout_seconds(
        max(generic_timeout_seconds, declared_timeout_seconds)
    )


def _identity_files(args: argparse.Namespace) -> dict[str, IdentityFiles]:
    return _identity_files_for_consumers(args, tuple(IDENTITY_PREFIXES))


def _identity_files_for_consumers(
    args: argparse.Namespace,
    consumer_ids: tuple[str, ...],
) -> dict[str, IdentityFiles]:
    values: dict[str, IdentityFiles] = {}
    for consumer_id in consumer_ids:
        prefix = IDENTITY_PREFIXES[consumer_id]
        # argparse converts every option dash into an underscore in Namespace
        # attributes, while the public CLI intentionally keeps alpha-binance
        # and alpha-okx readable as dashed option names.
        attribute_prefix = prefix.replace("-", "_")
        raw_fields = (
            getattr(args, f"{attribute_prefix}_tls_certificate_file"),
            getattr(args, f"{attribute_prefix}_tls_private_key_file"),
            getattr(args, f"{attribute_prefix}_jwt_private_key_file"),
            getattr(args, f"{attribute_prefix}_jwt_key_id"),
        )
        if not all(item is not None and str(item) for item in raw_fields):
            raise ValueError(f"Phase 10.5 identity material is unavailable for {consumer_id}")
        fields = IdentityFiles(
            certificate=str(raw_fields[0]),
            private_key=str(raw_fields[1]),
            jwt_private_key=str(raw_fields[2]),
            jwt_key_id=str(raw_fields[3]),
        )
        if any(not Path(path).is_file() for path in (
            fields.certificate, fields.private_key, fields.jwt_private_key,
        )):
            raise ValueError(f"Phase 10.5 identity material is unavailable for {consumer_id}")
        values[consumer_id] = fields
    return values


def _consumer_ids(args: argparse.Namespace) -> tuple[str, ...]:
    selected = tuple(args.consumer_id or ())
    if not selected:
        return tuple(PHASE105_PAPER_CONSUMER_ORDER)
    if len(selected) != len(set(selected)) or any(
        item not in IDENTITY_PREFIXES for item in selected
    ):
        raise ValueError("Phase 10.5 C2 consumer selection is invalid")
    return tuple(
        consumer_id
        for consumer_id in PHASE105_PAPER_CONSUMER_ORDER
        if consumer_id in selected
    )


def _scope(args: argparse.Namespace, consumer_ids: tuple[str, ...]):
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    release = StableReleaseRoutePlan.load(args.release_routing, manifest_root=ROOT)
    scope = build_release_consumer_acceptance_scope(
        release, catalog=catalog, acquisition=acquisition, consumer_ids=consumer_ids
    )
    if {item.consumer_id for item in scope.products} != frozenset(consumer_ids):
        raise ValueError("Phase 10.5 V2 identity scope is incomplete")
    return scope, release


def _route_summary(release: StableReleaseRoutePlan, products: tuple[AcceptanceProduct, ...]) -> dict[str, int]:
    routes = {
        (consumer.consumer_id, product.requirement_key): product
        for consumer in release.consumers
        for product in consumer.products
    }
    summary = {"v1_fallback_declared": 0, "blocked_fallback_declared": 0}
    for product in products:
        route = routes[(product.consumer_id, requirement_key(product.requirement))]
        if route.fallback == "V1":
            summary["v1_fallback_declared"] += 1
        elif route.fallback == "BLOCKED":
            summary["blocked_fallback_declared"] += 1
        else:
            raise ValueError("Phase 10.5 V2 product has an invalid fallback route")
    return summary


def _timing_policy(product) -> dict[str, object]:
    """Classify a sealed requirement without changing its declared threshold.

    The product requirement remains the source of truth. This only prevents
    acceptance evidence from describing a continuity/dropout horizon as an
    execution reaction SLA, or a quiet session as a broken provider because no
    event was emitted.
    """

    requirement = product.requirement
    feed = requirement.feed.value
    event_policy = requirement.effective_event_recency_policy.value
    freshness_ms = requirement.max_freshness_ms
    session_ms = requirement.max_session_liveness_ms
    common: dict[str, object] = {
        "feed": feed,
        "event_recency_policy": event_policy,
        "declared_max_freshness_ms": freshness_ms,
        "declared_max_session_liveness_ms": session_ms,
    }
    if feed == "BAR":
        interval = requirement.interval
        if not interval or not requirement.require_final_bars:
            raise ValueError("Phase 10.5 final BAR timing requires interval and finality")
        interval_ms = canonical_interval_ms(interval)
        if freshness_ms is None or freshness_ms < interval_ms:
            raise ValueError("Phase 10.5 final BAR continuity horizon is below its interval")
        return {
            **common,
            "semantic_class": "FINAL_SCHEDULED",
            "finality_required": True,
            "interval_ms": interval_ms,
            "freshness_role": "CONTINUITY_DROPOUT_HORIZON",
            "close_to_usable_sla_ms": None,
            "required_fences": ["FINALITY", "GAP", "GENERATION", "COMPLETENESS"],
        }
    if feed == "MARK_INDEX_PRICE":
        # MARK/INDEX has two deliberately distinct V2 read contracts.  The
        # execution OBSERVE form is the current gateway live view, whose
        # provider session/component evidence makes a quiet market usable.
        # All other forms are bounded reference snapshots; they must prove
        # provider-observation freshness and lineage, but cannot invent a
        # stream session that the product did not declare.
        execution_live_view = (
            requirement.consumer_grade is ConsumerGrade.EXECUTION
            and requirement.effective_event_recency_policy is StalePolicy.OBSERVE
        )
        if execution_live_view:
            if session_ms is None:
                raise ValueError(
                    "Phase 10.5 execution MARK_INDEX_PRICE timing requires a session-liveness bound"
                )
            return {
                **common,
                "semantic_class": "QUIET_SESSION",
                "freshness_role": "EVENT_RECENCY_OBSERVED_NOT_ADMITTED_ALONE",
                "required_fences": [
                    "SESSION", "COMPONENT_CADENCE", "GAP", "GENERATION", "COMPLETENESS",
                ],
            }
        if freshness_ms is None:
            raise ValueError(
                "Phase 10.5 reference MARK_INDEX_PRICE timing requires a provider-observation bound"
            )
        return {
            **common,
            "semantic_class": "REFERENCE_SNAPSHOT",
            "freshness_role": "PROVIDER_OBSERVATION_AGE",
            "required_fences": ["IDENTITY", "LINEAGE", "COVERAGE", "FRESHNESS"],
        }
    if feed in {"TRADE", "BOOK_DELTA"}:
        if event_policy == "OBSERVE":
            if session_ms is None:
                raise ValueError(
                    f"Phase 10.5 observed {feed} timing requires a session-liveness bound"
                )
            return {
                **common,
                "semantic_class": "QUIET_SESSION",
                "freshness_role": "EVENT_RECENCY_OBSERVED_NOT_ADMITTED_ALONE",
                "required_fences": ["SESSION", "GAP", "GENERATION", "COMPLETENESS"],
            }
        if freshness_ms is None:
            raise ValueError(f"Phase 10.5 strict {feed} timing requires an event-age bound")
        if session_ms is None:
            # Some research-only strict routes deliberately do not declare a
            # numeric session SLA. They remain event-age/gap fenced and cannot
            # become a quiet execution route merely because a provider has not
            # emitted a trade during this probe.
            return {
                **common,
                "semantic_class": "STRICT_EVENT",
                "freshness_role": "EVENT_AGE",
                "session_contract": "NOT_DECLARED_STRICT_EVENT",
                "required_fences": ["EVENT_AGE", "GAP", "GENERATION", "COMPLETENESS"],
            }
        return {
            **common,
            "semantic_class": "STRICT_EVENT_WITH_SESSION",
            "freshness_role": "EVENT_AGE_AND_SESSION",
            "required_fences": ["EVENT_AGE", "SESSION", "GAP", "GENERATION", "COMPLETENESS"],
        }
    if feed == "QUOTE":
        if freshness_ms is None or session_ms is None:
            raise ValueError("Phase 10.5 QUOTE timing requires event-age and session bounds")
        return {
            **common,
            "semantic_class": (
                "QUIET_SESSION" if event_policy == "OBSERVE" else "STRICT_EVENT_WITH_SESSION"
            ),
            "freshness_role": "EVENT_AGE_OR_PROVIDER_ON_CHANGE_WITH_SESSION",
            "delivery_semantics": "ASSERT_FROM_TYPED_QUALITY",
            "required_fences": ["SESSION", "GAP", "GENERATION", "COMPLETENESS"],
        }
    if feed == "BOOK_SNAPSHOT":
        if freshness_ms is None:
            raise ValueError("Phase 10.5 BOOK_SNAPSHOT timing requires a baseline-age bound")
        return {
            **common,
            "semantic_class": "BOOK_BASELINE",
            "freshness_role": "SNAPSHOT_BASELINE_NOT_DELTA_EXECUTION_AGE",
            "required_fences": ["GAP", "GENERATION", "COMPLETENESS", "BOOK_VERIFICATION"],
        }
    return {
        **common,
        "semantic_class": "REFERENCE_CADENCE",
        "freshness_role": "PUBLISHED_VALUE_CADENCE",
        "required_fences": ["IDENTITY", "LINEAGE", "COVERAGE"],
    }


def _reference_product(
    product: AcceptanceProduct,
    *,
    now_ns: int,
) -> ReferenceAcceptanceProduct:
    """Build one on-demand request retaining the real consumer identity/grade."""

    if product.delivery is not DeliveryClass.ON_DEMAND:
        raise ValueError("Phase 10.5 reference product lost ON_DEMAND delivery")
    return ReferenceAcceptanceProduct(
        consumer_id=product.consumer_id,
        consumer_subject=product.consumer_subject,
        manifest_revision=product.manifest_revision,
        manifest_sha256=product.manifest_sha256,
        instrument_uid=product.instrument_uid,
        instrument_id=product.instrument_id,
        venue=product.venue,
        market=product.market,
        native_symbol=product.native_symbol,
        requirement=product.requirement,
        sdk_requirement=reference_request_for_requirement(
            product.requirement, now_ns=now_ns
        ),
    )


def _build_c2_opening_operation_plan(
    products: tuple[AcceptanceProduct, ...],
    release: StableReleaseRoutePlan,
    probes,
    consumer_ids: tuple[str, ...],
    *,
    generic_timeout_seconds: float,
    reference_now_ns: int | None = None,
) -> dict[str, object]:
    """Compile the C2 opening budget from the sealed SDK operation graph.

    The calculation deliberately follows the same helpers C2 later invokes.
    It does not inspect provider payloads or change a manifest.  Every durable
    product gets two initial Query reads plus an explicit two-session
    warmup/cursor handoff; provider pass-through gets only the two Query reads;
    reference batches and the one documented native-BASIS deferral are counted
    through their existing batching helper.
    """

    if not products or not consumer_ids:
        raise ValueError("C2 opening operation plan requires products and consumers")
    if generic_timeout_seconds <= 0:
        raise ValueError("C2 opening operation plan requires a positive timeout")
    selected = frozenset(consumer_ids)
    if len(selected) != len(consumer_ids):
        raise ValueError("C2 opening operation plan duplicates a consumer")
    if {item.consumer_id for item in products} != selected:
        raise ValueError("C2 opening operation plan consumer scope is incomplete")
    routes = {item.consumer_id: item for item in release.consumers}
    if not selected <= set(routes):
        raise ValueError("C2 opening operation plan lacks a release consumer")
    probe_counts = Counter(item.consumer_id for item in probes)
    if not set(probe_counts) <= selected:
        raise ValueError("C2 opening operation plan has an out-of-scope fallback probe")
    now_ns = time.time_ns() if reference_now_ns is None else reference_now_ns
    plans: dict[str, dict[str, object]] = {}
    for consumer_id in consumer_ids:
        consumer_products = tuple(
            item for item in products if item.consumer_id == consumer_id
        )
        on_demand = tuple(
            item for item in consumer_products
            if item.delivery is DeliveryClass.ON_DEMAND
        )
        streamed = tuple(
            item for item in consumer_products
            if item.delivery is not DeliveryClass.ON_DEMAND
        )
        durable = tuple(
            item for item in streamed if item.delivery is DeliveryClass.DURABLE
        )
        if any(
            item.delivery not in {
                DeliveryClass.DURABLE,
                DeliveryClass.PROVIDER_PASS_THROUGH,
                DeliveryClass.ON_DEMAND,
            }
            for item in consumer_products
        ):
            raise ValueError("C2 opening operation plan has an unknown delivery class")
        references = tuple(
            _reference_product(item, now_ns=now_ns) for item in on_demand
        )
        reference_batches = reference_acceptance_batches(references)
        native_basis_batches = tuple(
            batch for batch in reference_batches
            if len(batch) == 1 and is_rust_admitted_native_basis(batch[0])
        )
        if any(
            is_rust_admitted_native_basis(item)
            for batch in reference_batches
            if len(batch) != 1
            for item in batch
        ):
            raise AssertionError("C2 native BASIS batch lost its singleton boundary")
        reference_tail_timeout = (
            _reference_transport_timeout_seconds(
                references, generic_timeout_seconds=generic_timeout_seconds
            )
            if references
            else generic_timeout_seconds
        )
        native_basis_deferral_seconds = sum(
            2.0 * batch[0].sdk_requirement.deadline_ms / 1_000
            for batch in native_basis_batches
        )
        manifest = routes[consumer_id].manifest
        safe_rpm = max(
            1,
            int(
                manifest.quotas.requests_per_minute * _C2_REQUEST_QUOTA_FRACTION
            ),
        )
        budget = {
            # Every stream-capable product reads both Query replicas first;
            # durable products then do one warmup/snapshot per handoff side.
            "QUERY_READ": (
                2 * len(streamed)
                + 2 * len(durable)
                # V2 -> V1 -> V2 reads both replicas before and after fallback.
                + 4 * probe_counts[consumer_id]
            ),
            # Both Query replicas read every declared reference batch. Each
            # native-BASIS replica may use exactly one typed deferral retry.
            "REFERENCE_BATCH": (
                2 * len(reference_batches) + 2 * len(native_basis_batches)
            ),
            "STREAM_SUBSCRIBE": 2 * len(durable),
        }
        budget = {name: count for name, count in budget.items() if count}
        total = sum(budget.values())
        plans[consumer_id] = {
            "requests_per_minute": manifest.quotas.requests_per_minute,
            "safe_requests_per_minute": safe_rpm,
            "max_streams": manifest.quotas.max_streams,
            "opening_operation_budget": budget,
            "opening_total_operations": total,
            "opening_pacing_floor_seconds": round(
                max(0, total - 1) * 60.0 / safe_rpm, 3
            ),
            "native_basis_deferral_seconds": round(native_basis_deferral_seconds, 3),
            "tail_timeout_seconds": round(
                max(generic_timeout_seconds, reference_tail_timeout), 3
            ),
        }
    global_route_products = tuple(
        product
        for consumer in release.consumers
        for product in consumer.products
    )
    selected_route_products = tuple(
        product
        for consumer_id in consumer_ids
        for product in routes[consumer_id].products
    )
    selected_v2_identities = {
        (consumer_id, product.requirement_key)
        for consumer_id in consumer_ids
        for product in routes[consumer_id].products
        if product.route == "V2_PRIMARY"
    }
    actual_v2_identities = {
        (product.consumer_id, requirement_key(product.requirement))
        for product in products
    }
    if actual_v2_identities != selected_v2_identities:
        raise ValueError("C2 opening operation plan differs from V2 primary routes")
    pacing_floor = max(float(item["opening_pacing_floor_seconds"]) for item in plans.values())
    native_basis_deferral = sum(
        float(item["native_basis_deferral_seconds"]) for item in plans.values()
    )
    tail_timeout = max(float(item["tail_timeout_seconds"]) for item in plans.values())
    return {
        "schema": "qdl.phase105.c2-opening-operation-plan.v1",
        "global_release_route_count": len(global_route_products),
        "global_v2_primary_product_count": sum(
            product.route == "V2_PRIMARY" for product in global_route_products
        ),
        "global_v1_primary_route_count": sum(
            product.route == "V1_PRIMARY" for product in global_route_products
        ),
        "selected_release_route_count": len(selected_route_products),
        "selected_v2_primary_product_count": len(selected_v2_identities),
        "selected_v1_primary_excluded_count": sum(
            product.route == "V1_PRIMARY" for product in selected_route_products
        ),
        "product_count": len(products),
        "total_operations": sum(int(item["opening_total_operations"]) for item in plans.values()),
        "pacing_floor_seconds": round(pacing_floor, 3),
        "native_basis_deferral_seconds": round(native_basis_deferral, 3),
        "tail_timeout_seconds": round(tail_timeout, 3),
        # The last quota-admitted call still owns its declared typed timeout.
        "minimum_deadline_seconds": float(
            ceil(pacing_floor + native_basis_deferral + tail_timeout)
        ),
        "consumers": plans,
    }


def _effective_c2_opening_timeout_seconds(
    operation_plan: Mapping[str, object],
    requested_seconds: float | None,
) -> float:
    """Use the exact derived budget unless an operator declares a larger one."""

    minimum = float(operation_plan["minimum_deadline_seconds"])
    if requested_seconds is None:
        return minimum
    if requested_seconds < 1.0:
        raise C2OpeningCapacityError(
            "OPENING_TIMEOUT_NOT_POSITIVE",
            {
                "requested_seconds": requested_seconds,
            },
        )
    if requested_seconds < minimum:
        raise C2OpeningCapacityError(
            "OPENING_TIMEOUT_BELOW_DERIVED_MINIMUM",
            {
                "requested_seconds": requested_seconds,
                "minimum_deadline_seconds": minimum,
                "operation_plan": dict(operation_plan),
            },
        )
    return requested_seconds


async def _certify_references(
    products: tuple[AcceptanceProduct, ...],
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    deadline_monotonic: float,
    semaphore: asyncio.Semaphore,
    native_basis_semaphore: asyncio.Semaphore,
    client_factory,
) -> list[dict[str, object]]:
    """Read declared provider data through both V2 replicas, never V1/direct.

    Reference results are explicitly on-demand.  They carry no stream cursor;
    their evidence instead proves catalog identity, provider lineage, units,
    full coverage and provider-observation freshness for the exact consumer
    grade/identity that declares the requirement.
    """

    reference_products = tuple(
        _reference_product(item, now_ns=time.time_ns()) for item in products
    )
    if not reference_products:
        return []
    transport_timeout_seconds = _reference_transport_timeout_seconds(
        reference_products,
        generic_timeout_seconds=timeout_seconds,
    )

    async def read_replica(client, *, label: str):
        values: dict[tuple[str, str, str, str, str], tuple[str, float, int, int, dict[str, int | bool]]] = {}
        try:
            for batch in reference_acceptance_batches(reference_products):
                started = time.perf_counter()
                response, attempts, deferred_ms = await _reference_batch_for_c2(
                    client,
                    batch,
                    deadline_monotonic=deadline_monotonic,
                    semaphore=semaphore,
                    native_basis_semaphore=native_basis_semaphore,
                )
                latency_ms = (time.perf_counter() - started) * 1_000
                observed_at_ns = time.time_ns()
                values_for_batch = []
                for item, result in zip(batch, response.results, strict=True):
                    try:
                        content_hash = reference_evidence(
                            item, result, observed_at_ns=observed_at_ns,
                        )
                        quality = reference_quality(
                            item, result, observed_at_ns=observed_at_ns,
                        )
                    except ValueError as error:
                        raise C2ReferenceProductError(
                            item, replica=label, error=error,
                        ) from error
                    values_for_batch.append((item, content_hash, quality))
                for item, content_hash, quality in values_for_batch:
                    if item.identity in values:
                        raise AssertionError("Phase 10.5 reference batch duplicated a product")
                    values[item.identity] = (
                        content_hash,
                        latency_ms,
                        attempts,
                        deferred_ms,
                        quality,
                    )
        finally:
            await client.close()
        if len(values) != len(reference_products):
            raise AssertionError(f"Phase 10.5 {label} reference batch lost a product")
        return values

    primary = client_factory(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "reference-primary.json",
        timeout_seconds=transport_timeout_seconds,
    )
    secondary = client_factory(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "reference-secondary.json",
        timeout_seconds=transport_timeout_seconds,
    )
    primary_values, secondary_values = await asyncio.gather(
        read_replica(primary, label="primary"),
        read_replica(secondary, label="secondary"),
    )
    return [
        {
            **product.evidence(),
            "primary_content_sha256": primary_values[product.identity][0],
            "secondary_content_sha256": secondary_values[product.identity][0],
            "primary_latency_ms": round(primary_values[product.identity][1], 3),
            "secondary_latency_ms": round(secondary_values[product.identity][1], 3),
            "primary_provider_attempts": primary_values[product.identity][2],
            "secondary_provider_attempts": secondary_values[product.identity][2],
            "primary_provider_deferred_ms": primary_values[product.identity][3],
            "secondary_provider_deferred_ms": secondary_values[product.identity][3],
            "acknowledged_offset": None,
            "resumed_offset": None,
            "stream_handoff": "NOT_APPLICABLE",
            "release_quality": {
                "primary": primary_values[product.identity][4],
                "secondary": secondary_values[product.identity][4],
            },
            "quality_sha256": {
                "primary": _evidence_sha256(primary_values[product.identity][4]),
                "secondary": _evidence_sha256(secondary_values[product.identity][4]),
            },
            "timing_policy": _timing_policy(product),
        }
        for product in reference_products
    ]


def _v1_base_url(value: str) -> str:
    """Keep the forced fallback inside the existing V1 service boundary."""
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "data_layer"
        or parsed.port != 8100
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Phase 10.5 V1 fallback URL must be exactly http://data_layer:8100")
    return value.rstrip("/")


def _c2_grpc_targets(value: str) -> str:
    """Require both stable stream replicas for the C2 lease-failover receipt."""
    targets = tuple(item.strip() for item in value.split(",") if item.strip())
    if len(targets) != 2 or len(set(targets)) != 2 or set(targets) != _C2_STREAM_TARGETS:
        raise ValueError(
            "Phase 10.5 C2 requires exactly qdl-v2-stream-a:8210 and "
            "qdl-v2-stream-b:8210 as --grpc-target"
        )
    return ",".join(targets)


async def _v2_query_product(
    product: AcceptanceProduct,
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    client_factory,
) -> tuple[str, str | None, float, float | None]:
    primary = client_factory(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "fallback-query-primary.json",
        timeout_seconds=timeout_seconds,
    )
    secondary = client_factory(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "fallback-query-secondary.json",
        timeout_seconds=timeout_seconds,
    )
    try:
        return await _query_product(product, primary=primary, secondary=secondary)
    finally:
        await primary.close()
        await secondary.close()


async def _v1_fallback_return(
    product: AcceptanceProduct,
    probe,
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    v1_base_url: str,
    state_dir: Path,
    timeout_seconds: float,
    client_factory,
) -> dict[str, object]:
    """Read V2, make one allowed V1 cached read, then confirm V2 again."""
    before = await _v2_query_product(
        product,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir / "before",
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    import httpx

    started = time.perf_counter()
    async with httpx.AsyncClient(
        base_url=v1_base_url,
        timeout=httpx.Timeout(timeout_seconds),
        trust_env=False,
    ) as client:
        response = await client.get(probe.path, params=dict(probe.params))
        response.raise_for_status()
        payload = response.json()
    v1_latency_ms = (time.perf_counter() - started) * 1000
    details = validate_v1_fallback_payload(probe, payload)
    after = await _v2_query_product(
        product,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir / "after",
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    return {
        **details,
        "before_primary_content_sha256": before[0],
        "before_secondary_content_sha256": before[1],
        "after_primary_content_sha256": after[0],
        "after_secondary_content_sha256": after[1],
        "v1_request_latency_ms": round(v1_latency_ms, 3),
    }


def _chunks(values: tuple[AcceptanceProduct, ...], size: int):
    if size < 1:
        raise ValueError("Phase 10.5 C2 batch size must be positive")
    for offset in range(0, len(values), size):
        yield values[offset:offset + size]


def _closing_batches(products: tuple[AcceptanceProduct, ...], max_batch_items: int):
    """Keep hot reads out of history batches and bound head-of-line delay."""
    groups: dict[str, list[AcceptanceProduct]] = {}
    for product in products:
        groups.setdefault(product.feed.value, []).append(product)
    for feed, group in groups.items():
        size = max_batch_items if feed == "BAR" else min(max_batch_items, 8)
        yield from _chunks(tuple(group), size)


def _closing_requirement(product: AcceptanceProduct):
    """Keep closing current-state proof small without weakening the product.

    Opening C2 already proves the declared bounded BAR history, finality and
    signed stream handoff. Closing keeps up to two final BARs so reads spanning one
    candle close still have an immutable overlap. Each current tail remains
    strict, and the existing parity validator rejects larger window shifts.
    Every non-history policy field is retained unchanged.
    """

    requirement = _c2_requirement(sdk_requirement(product))
    if requirement.feed.value != "BAR":
        return requirement
    specification = requirement.warmup_specification
    if specification is None or specification.rows is None:
        raise ValueError("Phase 10.5 closing BAR requires a row-bounded warmup policy")
    rows = min(2, specification.rows)
    return replace(
        requirement,
        warmup_limit=rows,
        warmup=(
            requirement.warmup.model_copy(update={"rows": rows})
            if requirement.warmup is not None
            else None
        ),
    )


async def _closing_batch_problem_evidence(
    client,
    products: tuple[AcceptanceProduct, ...],
    *,
    error: Exception,
    status_observations: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Locate typed item failures without weakening execution batch semantics.

    A public execution-grade batch must use ``require_all=True``. The SDK then
    deliberately raises a generic ``PARTIAL_RESULT`` rather than returning a
    partially usable response. Only after that fail-closed batch result do we
    bisect it with the same strict batch API, then issue a single public
    ``warmup`` for each failing leaf to retain its server typed code. The all-
    pass path stays batched; this diagnostic never turns a partial response into
    usable execution data or records its payload.
    """

    if not isinstance(error, DataLayerError) or error.code != "PARTIAL_RESULT":
        return []
    quality_by_identity = {
        tuple(item["product_identity"]): item.get("quality_sha256")
        for item in status_observations
        if isinstance(item.get("product_identity"), list)
    }

    async def failed_leaves(
        group: tuple[AcceptanceProduct, ...],
    ) -> tuple[AcceptanceProduct, ...]:
        if len(group) == 1:
            return group
        midpoint = len(group) // 2
        children = (group[:midpoint], group[midpoint:])
        leaves: list[AcceptanceProduct] = []
        for child in children:
            try:
                await client.warmup_batch(
                    tuple(_closing_requirement(product) for product in child),
                    require_all=True,
                )
            except (httpx.HTTPError, TimeoutError, DataLayerError, ValueError):
                leaves.extend(await failed_leaves(child))
        return tuple(leaves)

    leaves = await failed_leaves(products)
    evidence: list[dict[str, object]] = []
    for product in leaves:
        try:
            await client.warmup(_closing_requirement(product))
        except DataLayerError as leaf_error:
            code = leaf_error.code
            retryable = leaf_error.retryable
            detail = leaf_error.detail
        except (httpx.HTTPError, TimeoutError, ValueError) as leaf_error:
            code = f"DIAGNOSTIC_{type(leaf_error).__name__.upper()}"
            retryable = False
            detail = str(leaf_error)
        else:
            code = "BATCH_FAILURE_NOT_REPRODUCED"
            retryable = True
            detail = "strict batch failure was not reproduced by its isolated V2 read"
        evidence.append({
            **product.evidence(),
            "problem_code": code,
            "retryable": retryable,
            "problem_detail_sha256": hashlib.sha256(detail.encode()).hexdigest(),
            "quality_sha256": quality_by_identity.get(product.identity),
        })
    return evidence


async def _closing_failure_status_observations(
    client,
    products: tuple[AcceptanceProduct, ...],
    *,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    """Capture bounded typed status after a failed closing batch, never payload."""

    timeout = min(5.0, timeout_seconds)
    observations: list[dict[str, object]] = []
    for product in products:
        try:
            status = await asyncio.wait_for(
                client.feed_status(_closing_requirement(product)),
                timeout=timeout,
            )
        except Exception as error:  # Diagnostic must not hide the primary failure.
            observations.append({
                **product.evidence(),
                "product_identity": list(product.identity),
                "status_transport_error": type(error).__name__,
                "quality_sha256": None,
            })
        else:
            quality = compact_feed_status(status)
            observations.append({
                **product.evidence(),
                "product_identity": list(product.identity),
                "quality": quality,
                "quality_sha256": _evidence_sha256(quality),
            })
    return observations


async def _closing_batch_revalidation(
    products: tuple[AcceptanceProduct, ...],
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    max_batch_items: int,
    client_factory,
) -> list[dict[str, object]]:
    """Re-read every durable/pass-through product through both V2 replicas.

    C2's opening proof already establishes signed cursor/reconnect per product.
    Closing needs a strict current view for every route, not a second identical
    stream storm.  `warmup:batch` keeps that full-scope check below the real
    per-identity request quota without weakening any product validation.
    """

    if not products:
        return []
    if not 1 <= max_batch_items <= 100:
        raise ValueError("Phase 10.5 C2 batch size exceeds the V2 contract")

    async def read_replica(base_url: str, *, label: str):
        client = client_factory(
            identity,
            base_url=base_url,
            grpc_target=grpc_target,
            cursor_path=state_dir / f"closing-{label}.json",
            timeout_seconds=timeout_seconds,
        )
        values: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
        try:
            for batch in _closing_batches(products, max_batch_items):
                requirements = tuple(_closing_requirement(item) for item in batch)
                started = time.perf_counter()
                try:
                    response = await client.warmup_batch(requirements, require_all=True)
                except (httpx.HTTPError, TimeoutError, DataLayerError, ValueError) as error:
                    status_observations = await _closing_failure_status_observations(
                        client,
                        batch,
                        timeout_seconds=timeout_seconds,
                    )
                    batch_item_problems = await _closing_batch_problem_evidence(
                        client,
                        batch,
                        error=error,
                        status_observations=status_observations,
                    )
                    raise C2ClosingBatchError(
                        consumer_id=batch[0].consumer_id,
                        replica=label,
                        products=batch,
                        error=error,
                        status_observations=status_observations,
                        batch_item_problems=batch_item_problems,
                    ) from error
                latency_ms = (time.perf_counter() - started) * 1_000
                if len(response.results) != len(batch):
                    raise AssertionError("Phase 10.5 closing V2 batch cardinality differs")
                if response.partial:
                    raise AssertionError(
                        "Phase 10.5 public strict warmup batch returned partial"
                    )
                observed_at_ns = time.time_ns()
                for product, item in zip(batch, response.results, strict=True):
                    if item.data is None or not item.data.data:
                        raise AssertionError("Phase 10.5 closing V2 batch returned no product data")
                    history = tuple(item.data.data)
                    for view in history[:-1]:
                        validate_product_view(
                            product, view, require_current_quality=False
                        )
                    latest = history[-1]
                    validate_product_view(product, latest)
                    if product.identity in values:
                        raise AssertionError("Phase 10.5 closing V2 batch duplicated a product")
                    values[product.identity] = {
                        "history": history,
                        "latest": latest,
                        "latency_ms": latency_ms,
                        "quality": compact_view_quality(
                            latest, observed_at_ns=observed_at_ns
                        ),
                    }
        finally:
            await client.close()
        if len(values) != len(products):
            raise AssertionError("Phase 10.5 closing V2 batch lost a product")
        return values

    primary_values, secondary_values = await asyncio.gather(
        read_replica(primary_url, label="primary"),
        read_replica(secondary_url, label="secondary"),
    )
    evidence: list[dict[str, object]] = []
    for product in products:
        primary = primary_values[product.identity]
        secondary = secondary_values[product.identity]
        bar_alignment: dict[str, object] | None = None
        if product.feed.value == "BAR":
            primary_hash = warmup_content_fingerprint(primary["history"])
            secondary_hash = warmup_content_fingerprint(secondary["history"])
            if product.delivery is DeliveryClass.DURABLE:
                bar_alignment = validate_final_bar_warmup_windows(
                    primary["history"], secondary["history"]
                )
                primary_hash = str(bar_alignment["primary_content_sha256"])
                secondary_hash = str(bar_alignment["secondary_content_sha256"])
            else:
                validate_replica_views(product, primary["latest"], secondary["latest"])
        else:
            primary_hash, secondary_hash = validate_replica_views(
                product, primary["latest"], secondary["latest"]
            )
        item_evidence = {
            **product.evidence(),
            "primary_content_sha256": primary_hash,
            "secondary_content_sha256": secondary_hash,
            "primary_latency_ms": round(float(primary["latency_ms"]), 3),
            "secondary_latency_ms": round(float(secondary["latency_ms"]), 3),
            "release_quality": {
                "primary": primary["quality"],
                "secondary": secondary["quality"],
            },
            "quality_sha256": {
                "primary": _evidence_sha256(primary["quality"]),
                "secondary": _evidence_sha256(secondary["quality"]),
            },
            "timing_policy": _timing_policy(product),
            "closing_read": "BATCH_V2_PRIMARY",
        }
        if bar_alignment is not None:
            item_evidence["bar_replica_alignment"] = bar_alignment
        evidence.append(item_evidence)
    return evidence


async def _reference_batch_for_c2(
    client,
    batch: tuple[ReferenceAcceptanceProduct, ...],
    *,
    deadline_monotonic: float,
    semaphore: asyncio.Semaphore,
    native_basis_semaphore: asyncio.Semaphore,
):
    """Respect Rust's one native-BASIS lane across all C2 identities/replicas."""

    native_basis = tuple(item for item in batch if is_rust_admitted_native_basis(item))
    if native_basis and len(native_basis) != len(batch):
        raise AssertionError("Phase 10.5 native BASIS batch mixes provider lanes")
    if native_basis:
        if len(native_basis) != 1:
            raise AssertionError("Phase 10.5 native BASIS batch must be singleton")
        async with native_basis_semaphore:
            async with semaphore:
                return await _reference_batch_until_terminal(
                    client,
                    batch,
                    deadline_monotonic=deadline_monotonic,
                )
    async with semaphore:
        return await _reference_batch_until_terminal(
            client,
            batch,
            deadline_monotonic=deadline_monotonic,
        )


async def _closing_revalidate_consumer(
    consumer_id: str,
    products: tuple[AcceptanceProduct, ...],
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    deadline_monotonic: float,
    max_batch_items: int,
    reference_semaphore: asyncio.Semaphore,
    native_basis_semaphore: asyncio.Semaphore,
    client_factory,
) -> list[dict[str, object]]:
    stream_products = tuple(
        item for item in products if item.delivery is not DeliveryClass.ON_DEMAND
    )
    reference_products = tuple(
        item for item in products if item.delivery is DeliveryClass.ON_DEMAND
    )
    stream_task = asyncio.create_task(_closing_batch_revalidation(
        stream_products,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir / "stream",
        timeout_seconds=timeout_seconds,
        max_batch_items=max_batch_items,
        client_factory=client_factory,
    ))
    reference_task = asyncio.create_task(_certify_references(
        reference_products,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir / "references",
        timeout_seconds=timeout_seconds,
        deadline_monotonic=deadline_monotonic,
        semaphore=reference_semaphore,
        native_basis_semaphore=native_basis_semaphore,
        client_factory=client_factory,
    ))
    stream_results, reference_results = await _gather_or_cancel((stream_task, reference_task))
    if len(stream_results) != len(stream_products) or len(reference_results) != len(reference_products):
        raise AssertionError("Phase 10.5 closing V2 scope cardinality differs")
    return [*stream_results, *reference_results]


def _batch_identity_sha256(products: tuple[AcceptanceProduct, ...]) -> str:
    """Fingerprint an exact batch without persisting product data or payloads."""

    return hashlib.sha256(
        json.dumps(
            [item.identity for item in products],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _manifest_maximum_bar_batch(
    products: tuple[AcceptanceProduct, ...],
    *,
    max_batch_items: int,
) -> tuple[AcceptanceProduct, ...]:
    """Return the exact first manifest-maximum BAR partition."""

    if not 1 <= max_batch_items <= 100:
        raise ValueError("Phase 10.5 batch-shape maximum exceeds the V2 contract")
    bar_batches = tuple(
        batch
        for batch in _closing_batches(products, max_batch_items)
        if batch and batch[0].feed is FeedType.BAR
    )
    exact = next((batch for batch in bar_batches if len(batch) == max_batch_items), None)
    if exact is None:
        raise ValueError("Phase 10.5 batch-shape matrix lacks a manifest-maximum BAR partition")
    return exact


def _largest_bar_batch(
    products: tuple[AcceptanceProduct, ...],
    *,
    max_batch_items: int,
) -> tuple[AcceptanceProduct, ...]:
    """Return the largest declared BAR batch for a collocated consumer lane."""

    if not 1 <= max_batch_items <= 100:
        raise ValueError("Phase 10.5 collocation batch maximum exceeds the V2 contract")
    batches = tuple(
        batch
        for batch in _closing_batches(products, max_batch_items)
        if batch and batch[0].feed is FeedType.BAR
    )
    if not batches:
        raise ValueError("Phase 10.5 collocation has no BAR partition")
    return max(batches, key=len)


def _strict_bar_batch_windows(
    products: tuple[AcceptanceProduct, ...],
    *,
    max_batch_items: int,
) -> tuple[tuple[int, int, tuple[AcceptanceProduct, ...]], ...]:
    """Select boundary windows plus the exact maximum BAR partition.

    The existing failed receipt already bisected every member of the selected
    maximum batch. This matrix diagnoses batch cardinality and queue position,
    so smaller shapes test deterministic first/last windows while the maximum
    shape always exercises every item of the exact manifest partition.
    """

    exact = _manifest_maximum_bar_batch(
        products, max_batch_items=max_batch_items
    )
    windows: list[tuple[int, int, tuple[AcceptanceProduct, ...]]] = []
    for shape in (*_STRICT_BAR_BATCH_SHAPES, max_batch_items):
        if shape > len(exact):
            continue
        candidates = (exact[:shape],) if shape == len(exact) else (exact[:shape], exact[-shape:])
        seen: set[str] = set()
        for candidate in candidates:
            digest = _batch_identity_sha256(candidate)
            if digest in seen:
                continue
            seen.add(digest)
            windows.append((shape, len(seen) - 1, candidate))
    if not windows or windows[-1][0] != max_batch_items:
        raise AssertionError("Phase 10.5 batch-shape matrix omitted the maximum BAR batch")
    return tuple(windows)


def _batch_shape_observation(
    products: tuple[AcceptanceProduct, ...],
    observations: list[dict[str, object]],
    *,
    batch_shape: int,
    window_index: int,
) -> dict[str, object]:
    """Reduce a validated dual-replica batch to bounded diagnostic evidence."""

    expected = {item.identity for item in products}
    actual = {
        (
            str(item.get("consumer_id")),
            str(item.get("instrument_uid")),
            str(item.get("feed")),
            str(item.get("interval") or ""),
            str(item.get("source_policy_id")),
        )
        for item in observations
    }
    if actual != expected or len(observations) != len(expected):
        raise AssertionError("Phase 10.5 batch-shape result differs from its exact manifest partition")
    primary_latency = sorted(float(item["primary_latency_ms"]) for item in observations)
    secondary_latency = sorted(float(item["secondary_latency_ms"]) for item in observations)
    quality = [
        {
            "identity": item.identity,
            "primary": observation["quality_sha256"]["primary"],
            "secondary": observation["quality_sha256"]["secondary"],
            "primary_content": observation["primary_content_sha256"],
            "secondary_content": observation["secondary_content_sha256"],
        }
        for item, observation in zip(products, observations, strict=True)
    ]
    return {
        "batch_shape": batch_shape,
        "window_index": window_index,
        "batch_size": len(products),
        "batch_identity_sha256": _batch_identity_sha256(products),
        "primary_latency_ms": {
            "p50": round(primary_latency[max(0, ceil(len(primary_latency) * 0.50) - 1)], 3),
            "p95": round(primary_latency[max(0, ceil(len(primary_latency) * 0.95) - 1)], 3),
        },
        "secondary_latency_ms": {
            "p50": round(secondary_latency[max(0, ceil(len(secondary_latency) * 0.50) - 1)], 3),
            "p95": round(secondary_latency[max(0, ceil(len(secondary_latency) * 0.95) - 1)], 3),
        },
        "quality_content_sha256": hashlib.sha256(
            json.dumps(quality, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "payload_recorded": False,
    }


async def _strict_bar_batch_shape_matrix(
    products: tuple[AcceptanceProduct, ...],
    *,
    identity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    max_batch_items: int,
    client_factory,
    revalidate=None,
) -> list[dict[str, object]]:
    """Exercise exact strict BAR shapes without stream/fallback/provider reads."""

    runner = _closing_batch_revalidation if revalidate is None else revalidate
    evidence: list[dict[str, object]] = []
    for shape, window_index, window in _strict_bar_batch_windows(
        products, max_batch_items=max_batch_items
    ):
        try:
            observations = await runner(
                window,
                identity=identity,
                primary_url=primary_url,
                secondary_url=secondary_url,
                grpc_target=grpc_target,
                state_dir=state_dir / f"shape-{shape}-{window_index}",
                timeout_seconds=timeout_seconds,
                max_batch_items=shape,
                client_factory=client_factory,
            )
        except Exception as error:
            raise C2BatchShapeError(
                error,
                stage="ISOLATED",
                batch_shape=shape,
                window_index=window_index,
                products=window,
            ) from error
        evidence.append(_batch_shape_observation(
            window,
            observations,
            batch_shape=shape,
            window_index=window_index,
        ))
    return evidence


async def _strict_bar_collocation_matrix(
    scope,
    release: StableReleaseRoutePlan,
    *,
    consumer_ids: tuple[str, ...],
    identities: Mapping[str, object],
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    client_factories: Mapping[str, Callable],
    preferred_consumer_id: str,
    revalidate=None,
) -> dict[str, object]:
    """Measure deterministic local-lane saturation without hiding its threshold."""

    runner = _closing_batch_revalidation if revalidate is None else revalidate
    routes = {item.consumer_id: item for item in release.consumers}
    selected: list[tuple[str, int, tuple[AcceptanceProduct, ...]]] = []
    not_applicable: list[dict[str, object]] = []
    for consumer_id in consumer_ids:
        route = routes.get(consumer_id)
        if route is None:
            raise ValueError("Phase 10.5 collocation route is unavailable")
        stream_products = tuple(
            item for item in scope.products
            if item.consumer_id == consumer_id and item.delivery is not DeliveryClass.ON_DEMAND
        )
        if not any(item.feed is FeedType.BAR for item in stream_products):
            not_applicable.append({
                "consumer_id": consumer_id,
                "status": "NOT_APPLICABLE_NO_DURABLE_BAR",
                "read_actions": 0,
                "payload_recorded": False,
            })
            continue
        batch = _largest_bar_batch(
            stream_products,
            max_batch_items=route.manifest.quotas.max_batch_items,
        )
        selected.append((consumer_id, len(batch), batch))

    selected.sort(key=lambda item: (item[0] != preferred_consumer_id, item[0]))
    if selected and selected[0][0] != preferred_consumer_id:
        raise ValueError("Phase 10.5 preferred BAR consumer is not entitled")

    async def run_one(consumer_id: str, shape: int, batch: tuple[AcceptanceProduct, ...]):
        try:
            observations = await runner(
                batch,
                identity=identities[consumer_id],
                primary_url=primary_url,
                secondary_url=secondary_url,
                grpc_target=grpc_target,
                state_dir=state_dir / consumer_id.replace(".", "-"),
                timeout_seconds=timeout_seconds,
                max_batch_items=shape,
                client_factory=client_factories[consumer_id],
            )
        except Exception as error:
            raise C2BatchShapeError(
                error,
                stage="COLLOCATED",
                batch_shape=shape,
                window_index=0,
                products=batch,
            ) from error
        return _batch_shape_observation(
            batch,
            observations,
            batch_shape=shape,
            window_index=0,
        )

    waves: list[dict[str, object]] = []
    for parallel_lanes in range(1, len(selected) + 1):
        lane = tuple(selected[:parallel_lanes])
        try:
            executed = list(await _gather_or_cancel(tuple(
                asyncio.create_task(run_one(consumer_id, shape, batch))
                for consumer_id, shape, batch in lane
            )))
        except C2BatchShapeError as error:
            error.evidence["completed_collocation_waves"] = waves
            error.evidence["failed_parallel_lanes"] = parallel_lanes
            raise
        waves.append({
            "parallel_lanes": parallel_lanes,
            "consumer_ids": [consumer_id for consumer_id, _shape, _batch in lane],
            "observations": executed,
            "payload_recorded": False,
        })
    return {
        "waves": waves,
        "not_applicable": not_applicable,
        "payload_recorded": False,
    }


async def _read_plane_preflight(
    scope,
    release: StableReleaseRoutePlan,
    *,
    consumer_ids: tuple[str, ...],
    identities: Mapping[str, object],
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    deadline_monotonic: float,
    concurrency: int,
    client_factories: Mapping[str, Callable],
) -> list[dict[str, object]]:
    """Read every V2 route twice without opening a stream or fallback path.

    This is deliberately a release-candidate debugger, not a substitute for
    C2: it exercises the same sealed SDK requirements and both query replicas,
    but omits cursor/reconnect, fallback and the 300-second observation. It
    catches materialization/quality/replica faults before a full C2 spends its
    manifest-derived opening window.
    """

    release_consumers = {item.consumer_id: item for item in release.consumers}
    reference_semaphore = asyncio.Semaphore(_reference_batch_concurrency(concurrency))
    native_basis_semaphore = asyncio.Semaphore(1)

    async def revalidate_consumer(consumer_id: str) -> list[dict[str, object]]:
        products = tuple(item for item in scope.products if item.consumer_id == consumer_id)
        route = release_consumers.get(consumer_id)
        if not products or route is None:
            raise ValueError("Phase 10.5 pre-C2 consumer route is unavailable")
        return await _closing_revalidate_consumer(
            consumer_id,
            products,
            identity=identities[consumer_id],
            primary_url=primary_url,
            secondary_url=secondary_url,
            grpc_target=grpc_target,
            state_dir=state_dir / consumer_id.replace(".", "-"),
            timeout_seconds=timeout_seconds,
            deadline_monotonic=deadline_monotonic,
            max_batch_items=route.manifest.quotas.max_batch_items,
            reference_semaphore=reference_semaphore,
            native_basis_semaphore=native_basis_semaphore,
            client_factory=client_factories[consumer_id],
        )

    groups = await _gather_or_cancel(tuple(
        asyncio.create_task(revalidate_consumer(consumer_id))
        for consumer_id in consumer_ids
    ))
    return [item for group in groups for item in group]


def _read_plane_preflight_receipt(
    *,
    scope,
    release: StableReleaseRoutePlan,
    consumer_ids: tuple[str, ...],
    observations: list[dict[str, object]],
    authority_revision: object,
    elapsed_seconds: float,
    quota_window_wait_seconds: float,
    pacers: Mapping[str, _C2ConsumerRequestPacer],
) -> dict[str, object]:
    """Return compact proof that the fast matrix covered the exact route set."""

    expected = {
        (item.consumer_id, item.instrument_uid, item.feed.value, item.interval or "",
         item.source_policy_id)
        for item in scope.products
    }
    actual = {
        (
            str(item.get("consumer_id")),
            str(item.get("instrument_uid")),
            str(item.get("feed")),
            str(item.get("interval") or ""),
            str(item.get("source_policy_id")),
        )
        for item in observations
    }
    if actual != expected or len(observations) != len(expected):
        raise AssertionError("Phase 10.5 pre-C2 read-plane scope differs from release routes")
    timing_classes = Counter()
    for item in observations:
        timing_policy = item.get("timing_policy")
        if not isinstance(timing_policy, Mapping):
            raise AssertionError("Phase 10.5 pre-C2 timing policy is missing")
        semantic_class = timing_policy.get("semantic_class")
        if not isinstance(semantic_class, str) or not semantic_class:
            raise AssertionError("Phase 10.5 pre-C2 timing semantic class is invalid")
        timing_classes[semantic_class] += 1
    feed_counts = Counter(str(item[2]) for item in actual)
    consumer_counts = Counter(str(item[0]) for item in actual)
    primary_latency = sorted(
        float(item["primary_latency_ms"])
        for item in observations
        if isinstance(item.get("primary_latency_ms"), (int, float))
    )
    secondary_latency = sorted(
        float(item["secondary_latency_ms"])
        for item in observations
        if isinstance(item.get("secondary_latency_ms"), (int, float))
    )

    def percentile(values: list[float], fraction: float) -> float | None:
        if not values:
            return None
        index = min(len(values) - 1, max(0, ceil(len(values) * fraction) - 1))
        return round(values[index], 3)

    return {
        "schema": "qdl.phase105.v2-read-plane-preflight.v1",
        "status": "PASS_READ_PLANE_PREFLIGHT",
        "mode": "READ_PLANE_ONLY_NO_STREAM_NO_FALLBACK",
        "release_route_plan_sha256": release.digest,
        "authority_revision": authority_revision,
        "scope_sha256": scope.sha256,
        "product_count": len(observations),
        "consumer_counts": dict(sorted(consumer_counts.items())),
        "feed_counts": dict(sorted(feed_counts.items())),
        "timing_class_counts": dict(sorted(timing_classes.items())),
        "replica_read_count": 2,
        "primary_batch_latency_ms": {
            "p50": percentile(primary_latency, 0.50),
            "p95": percentile(primary_latency, 0.95),
            "p99": percentile(primary_latency, 0.99),
        },
        "secondary_batch_latency_ms": {
            "p50": percentile(secondary_latency, 0.50),
            "p95": percentile(secondary_latency, 0.95),
            "p99": percentile(secondary_latency, 0.99),
        },
        "quota_window_wait_seconds": round(quota_window_wait_seconds, 3),
        "quota_budget": {
            consumer_id: pacer.evidence()
            for consumer_id, pacer in sorted(pacers.items())
        },
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "payload_recorded": False,
        "elapsed_seconds": round(elapsed_seconds, 3),
    }


async def _run_consumer_groups(
    consumer_ids: tuple[str, ...],
    run_group,
) -> tuple[tuple[list[dict[str, object]], list[dict[str, object]]], ...]:
    """Start every governed C2 group before awaiting the ordered results.

    A final-BAR reconnect can legitimately wait through the next close. The
    C2-wide deadline is meaningful only if independent consumer groups observe
    that bounded wait concurrently, not one group after another.
    """
    tasks = tuple(asyncio.create_task(run_group(consumer_id)) for consumer_id in consumer_ids)
    return await _gather_or_cancel(tasks)


async def _gather_or_cancel(tasks):
    """Drain sibling work before a caller removes its scoped cursor state."""
    try:
        return tuple(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def run(args: argparse.Namespace) -> dict[str, object]:
    authority = _authority(args.authority_record)
    consumer_ids = _consumer_ids(args)
    scope, release = _scope(args, consumer_ids)
    timing_profiles = {
        product.identity: _timing_policy(product) for product in scope.products
    }
    if len(timing_profiles) != len(scope.products):
        raise AssertionError("Phase 10.5 timing policy duplicated a release product")
    files = _identity_files_for_consumers(args, consumer_ids)
    v1_base_url = _v1_base_url(args.v1_base_url)
    grpc_target = _c2_grpc_targets(args.grpc_target)
    try:
        v1_provenance_raw = json.loads(args.v1_provenance.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5 V1 provenance cannot be read") from error
    v1_provenance = validate_v1_provenance(release, v1_provenance_raw)
    try:
        v1_runtime_binding_raw = json.loads(args.v1_runtime_binding.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.5 V1 runtime binding cannot be read") from error
    v1_runtime_binding = validate_v1_runtime_binding(v1_provenance, v1_runtime_binding_raw)
    probes = build_v1_fallback_probes(
        release,
        catalog=StableSourceCatalog.load(args.catalog),
        products=scope.products,
        consumer_ids=consumer_ids,
    )
    opening_operation_plan = _build_c2_opening_operation_plan(
        scope.products,
        release,
        probes,
        consumer_ids,
        generic_timeout_seconds=args.timeout_seconds,
    )
    opening_consumer_plans = opening_operation_plan["consumers"]
    if not isinstance(opening_consumer_plans, dict):
        raise AssertionError("C2 opening operation plan has invalid consumers")
    if any(
        args.concurrency > int(item["max_streams"])
        for item in opening_consumer_plans.values()
    ):
        raise C2OpeningCapacityError(
            "OPENING_CONCURRENCY_EXCEEDS_MANIFEST_STREAM_QUOTA",
            {
                "requested_concurrency": args.concurrency,
                "operation_plan": opening_operation_plan,
            },
        )
    opening_timeout_seconds = _effective_c2_opening_timeout_seconds(
        opening_operation_plan, args.opening_timeout_seconds
    )
    products_by_identity = {
        (item.consumer_id, requirement_key(item.requirement)): item for item in scope.products
    }
    from qdl_sdk import WorkloadTlsConfig

    identities = {}
    for product in scope.products:
        if product.consumer_id in identities:
            continue
        material = files[product.consumer_id]
        identities[product.consumer_id] = _identity(
            product=product,
            certificate_file=material.certificate,
            private_key_file=material.private_key,
            jwt_private_key_file=material.jwt_private_key,
            jwt_key_id=material.jwt_key_id,
            tls_ca_file=args.tls_ca_file,
            issuer=args.issuer,
            audience=args.audience,
        )
        if not isinstance(identities[product.consumer_id].tls, WorkloadTlsConfig):
            raise AssertionError("Phase 10.5 identity did not build workload TLS")

    process_started = time.process_time()
    temporary = Path(tempfile.mkdtemp(prefix="qdl-phase105-v2-identity-"))
    product_semaphore = asyncio.Semaphore(args.concurrency)
    reference_semaphore = asyncio.Semaphore(
        _reference_batch_concurrency(args.concurrency)
    )
    native_basis_semaphore = asyncio.Semaphore(1)
    pacers = _quota_pacers(release, consumer_ids)
    client_factories = {
        consumer_id: _paced_client_factory(pacer)
        for consumer_id, pacer in pacers.items()
    }
    release_consumers = {item.consumer_id: item for item in release.consumers}

    if args.batch_shape_matrix:
        consumer_id = args.batch_shape_consumer_id
        if consumer_id not in consumer_ids:
            raise ValueError("Phase 10.5 batch-shape consumer is outside the selected scope")
        route = release_consumers.get(consumer_id)
        if route is None:
            raise ValueError("Phase 10.5 batch-shape consumer route is unavailable")
        selected_products = tuple(
            item for item in scope.products
            if item.consumer_id == consumer_id and item.delivery is not DeliveryClass.ON_DEMAND
        )
        if not selected_products:
            raise ValueError("Phase 10.5 batch-shape consumer has no durable products")
        quota_window_wait_seconds = await _wait_for_clean_quota_windows(pacers)
        matrix_started = time.monotonic()
        try:
            isolated = await _strict_bar_batch_shape_matrix(
                selected_products,
                identity=identities[consumer_id],
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=grpc_target,
                state_dir=temporary / "strict-bar-batch" / "isolated",
                timeout_seconds=args.timeout_seconds,
                max_batch_items=route.manifest.quotas.max_batch_items,
                client_factory=client_factories[consumer_id],
            )
            try:
                collocated = await _strict_bar_collocation_matrix(
                    scope,
                    release,
                    consumer_ids=consumer_ids,
                    identities=identities,
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=grpc_target,
                    state_dir=temporary / "strict-bar-batch" / "collocated",
                    timeout_seconds=args.timeout_seconds,
                    client_factories=client_factories,
                    preferred_consumer_id=consumer_id,
                )
            except C2BatchShapeError as error:
                error.evidence["isolated"] = isolated
                raise
        finally:
            shutil.rmtree(temporary, ignore_errors=True)
        exact_max = next(
            item for item in isolated
            if item["batch_shape"] == route.manifest.quotas.max_batch_items
        )
        return {
            "schema": "qdl.phase105.strict-bar-batch-shape.v1",
            "status": "PASS_STRICT_BAR_BATCH_SHAPE",
            "mode": "READ_PLANE_ONLY_NO_STREAM_NO_FALLBACK",
            "release_route_plan_sha256": release.digest,
            "authority_revision": authority.get("revision"),
            "scope_sha256": scope.sha256,
            "consumer_id": consumer_id,
            "manifest_max_batch_items": route.manifest.quotas.max_batch_items,
            "exact_maximum_batch_identity_sha256": exact_max["batch_identity_sha256"],
            "isolated": isolated,
            "collocated": collocated,
            "quota_window_wait_seconds": round(quota_window_wait_seconds, 3),
            "quota_budget": {
                item_consumer_id: pacer.evidence()
                for item_consumer_id, pacer in sorted(pacers.items())
            },
            "provider_connections": 0,
            "order_actions": 0,
            "cursor_directory_removed": True,
            "payload_recorded": False,
            "elapsed_seconds": round(time.monotonic() - matrix_started, 3),
        }

    if args.read_plane_preflight:
        # Use the same per-identity 75% quota guard as C2, but only for the
        # batched current read plane. There is no cursor, stream, fallback or
        # 300-second observation in this diagnostic mode.
        quota_window_wait_seconds = await _wait_for_clean_quota_windows(pacers)
        preflight_started = time.monotonic()
        try:
            observations = await asyncio.wait_for(
                _read_plane_preflight(
                    scope,
                    release,
                    consumer_ids=consumer_ids,
                    identities=identities,
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=grpc_target,
                    state_dir=temporary / "read-plane-preflight",
                    timeout_seconds=args.timeout_seconds,
                    deadline_monotonic=preflight_started + opening_timeout_seconds,
                    concurrency=args.concurrency,
                    client_factories=client_factories,
                ),
                timeout=opening_timeout_seconds,
            )
        finally:
            shutil.rmtree(temporary, ignore_errors=True)
        return _read_plane_preflight_receipt(
            scope=scope,
            release=release,
            consumer_ids=consumer_ids,
            observations=observations,
            authority_revision=authority.get("revision"),
            elapsed_seconds=time.monotonic() - preflight_started,
            quota_window_wait_seconds=quota_window_wait_seconds,
            pacers=pacers,
        )

    quota_window_wait_seconds = await _wait_for_clean_quota_windows(pacers)
    started = time.monotonic()
    opening_deadline = started + opening_timeout_seconds
    print(json.dumps({
        "stage": "C2_OPENING_OPERATION_PLAN",
        "operator_timeout_seconds": args.opening_timeout_seconds,
        "effective_timeout_seconds": opening_timeout_seconds,
        "plan": opening_operation_plan,
    }, sort_keys=True, separators=(",", ":")), file=sys.stderr, flush=True)

    async def certify(product: AcceptanceProduct) -> dict[str, object]:
        async with product_semaphore:
            try:
                return await _certify_product(
                    product,
                    identity=identities[product.consumer_id],
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=grpc_target,
                    state_dir=temporary,
                    timeout_seconds=args.timeout_seconds,
                    stream_open_timeout_seconds=opening_timeout_seconds,
                    client_factory=client_factories[product.consumer_id],
                )
            except C2StatusEvidenceError as error:
                raise C2ProductAcceptanceError(product, error) from error
            except C2OpeningCapacityError:
                raise
            except asyncio.CancelledError:
                print(json.dumps({"stage": "C2_ACTIVE_PRODUCT_CANCELLED",
                                  "identity": product.identity}), file=sys.stderr, flush=True)
                raise
            except Exception as error:
                raise RuntimeError(
                    "Phase 10.5 V2 identity receipt failed "
                    f"consumer={product.consumer_id} instrument={product.instrument_id} "
                    f"feed={product.feed.value} interval={product.interval}"
                ) from error

    async def certify_consumer(
        consumer_id: str,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        consumer_products = tuple(
            item for item in scope.products if item.consumer_id == consumer_id
        )
        if not consumer_products:
            raise ValueError(f"Phase 10.5 consumer has no V2 products: {consumer_id}")
        stream_products = tuple(
            item for item in consumer_products
            if item.delivery is not DeliveryClass.ON_DEMAND
        )
        reference_products = tuple(
            item for item in consumer_products
            if item.delivery is DeliveryClass.ON_DEMAND
        )
        product_tasks = tuple(
            asyncio.create_task(certify(product)) for product in consumer_products
            if product.delivery is not DeliveryClass.ON_DEMAND
        )
        reference_task = asyncio.create_task(_certify_references(
            reference_products,
            identity=identities[consumer_id],
            primary_url=args.primary_url,
            secondary_url=args.secondary_url,
            grpc_target=grpc_target,
            state_dir=temporary / consumer_id.replace(".", "-") / "references",
            timeout_seconds=args.timeout_seconds,
            deadline_monotonic=opening_deadline,
            semaphore=reference_semaphore,
            native_basis_semaphore=native_basis_semaphore,
            client_factory=client_factories[consumer_id],
        ))
        task_results = await _gather_or_cancel((*product_tasks, reference_task))
        ordered = tuple(task_results[:len(product_tasks)])
        reference_results = task_results[-1]
        if len(ordered) != len(stream_products) or len(reference_results) != len(reference_products):
            raise AssertionError("Phase 10.5 consumer result cardinality differs from scope")
        fallback_details: list[dict[str, object]] = []
        for probe in (item for item in probes if item.consumer_id == consumer_id):
            product = products_by_identity[probe.identity]
            fallback_details.append(await _v1_fallback_return(
                product,
                probe,
                identity=identities[consumer_id],
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=grpc_target,
                v1_base_url=v1_base_url,
                state_dir=temporary / consumer_id.replace(".", "-"),
                timeout_seconds=args.timeout_seconds,
                client_factory=client_factories[consumer_id],
            ))
        return [*ordered, *reference_results], fallback_details

    async def certify_ordered() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        groups = await _run_consumer_groups(
            consumer_ids, certify_consumer
        )
        results: list[dict[str, object]] = []
        fallback_details: list[dict[str, object]] = []
        for consumer_results, consumer_fallbacks in groups:
            results.extend(consumer_results)
            fallback_details.extend(consumer_fallbacks)
        return results, fallback_details

    async def closing_revalidation_ordered(deadline_monotonic: float) -> list[dict[str, object]]:
        async def revalidate_consumer(consumer_id: str) -> list[dict[str, object]]:
            consumer_products = tuple(
                item for item in scope.products if item.consumer_id == consumer_id
            )
            route = release_consumers.get(consumer_id)
            if route is None:
                raise ValueError("Phase 10.5 closing consumer route is unavailable")
            return await _closing_revalidate_consumer(
                consumer_id,
                consumer_products,
                identity=identities[consumer_id],
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=grpc_target,
                state_dir=temporary / consumer_id.replace(".", "-") / "closing",
                timeout_seconds=args.timeout_seconds,
                deadline_monotonic=deadline_monotonic,
                max_batch_items=route.manifest.quotas.max_batch_items,
                reference_semaphore=reference_semaphore,
                native_basis_semaphore=native_basis_semaphore,
                client_factory=client_factories[consumer_id],
            )

        groups = await _gather_or_cancel(tuple(
            asyncio.create_task(revalidate_consumer(consumer_id))
            for consumer_id in consumer_ids
        ))
        results: list[dict[str, object]] = []
        for consumer_results in groups:
            results.extend(consumer_results)
        return results

    try:
        # Opening proves warmup/cursor/reconnect/fallback for every product.
        # The clock for the true 300-second observation starts only after that
        # full proof is complete. Closing rechecks every route with batch V2
        # reads; it deliberately does not create a second stream storm.
        opening_started = time.monotonic()
        try:
            initial_results, initial_fallback_details = await asyncio.wait_for(
                certify_ordered(), timeout=opening_timeout_seconds
            )
        except TimeoutError as error:
            raise C2OpeningCapacityError(
                "OPENING_DEADLINE_EXCEEDED",
                {
                    "effective_timeout_seconds": opening_timeout_seconds,
                    "operation_plan": opening_operation_plan,
                    "quota_budget": {
                        consumer_id: pacer.evidence()
                        for consumer_id, pacer in sorted(pacers.items())
                    },
                },
            ) from error
        opening_seconds = time.monotonic() - opening_started
        print(json.dumps({"stage": "C2_OPENING_PASS", "products": len(initial_results),
                          "seconds": round(opening_seconds, 3)}), file=sys.stderr, flush=True)
        observation_started = time.monotonic()
        observation_seconds = await _wait_for_minimum_observation(
            started_monotonic=observation_started,
            observation_seconds=args.observation_seconds,
        )
        closing_started = time.monotonic()
        print(json.dumps({"stage": "C2_OBSERVATION_COMPLETE", "seconds": observation_seconds}),
              file=sys.stderr, flush=True)
        closing_results = await asyncio.wait_for(
            closing_revalidation_ordered(closing_started + args.closing_timeout_seconds),
            timeout=args.closing_timeout_seconds,
        )
        closing_seconds = time.monotonic() - closing_started
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    elapsed_seconds = time.monotonic() - started
    cpu_seconds = max(0.0, time.process_time() - process_started)
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_bytes = int(max_rss) * 1024
    if elapsed_seconds > (
        opening_timeout_seconds
        + args.observation_seconds
        + args.closing_timeout_seconds
    ):
        raise AssertionError("Phase 10.5 identity acceptance exceeded its bounded windows")
    initial_by_identity = {
        (
            item["consumer_id"], item["instrument_uid"], item["feed"],
            item["interval"] or "", item["source_policy_id"],
        ): item
        for item in initial_results
    }
    closing_by_identity = {
        (
            item["consumer_id"], item["instrument_uid"], item["feed"],
            item["interval"] or "", item["source_policy_id"],
        ): item
        for item in closing_results
    }
    if set(initial_by_identity) != set(closing_by_identity) or len(initial_by_identity) != len(scope.products):
        raise AssertionError("Phase 10.5 identity scope changed during the observation window")
    for identity_key, item in initial_by_identity.items():
        item["timing_policy"] = timing_profiles[identity_key]
        item["closing_v2_read"] = closing_by_identity[identity_key]
    route_summary = _route_summary(release, scope.products)
    return {
        "schema": "qdl.phase105.v2-identity-acceptance.v1",
        "status": "PASS_V2_DATA_PLANE_ONLY",
        "release_route_plan_sha256": release.digest,
        "authority_revision": authority.get("revision"),
        "scope_sha256": scope.sha256,
        "product_count": len(initial_results),
        "durable_product_count": sum(
            item.delivery is DeliveryClass.DURABLE for item in scope.products
        ),
        "products": initial_results,
        "route_contract": {
            **route_summary,
            "v1_fallback_observed": True,
            "route_selection_probe_only": True,
            "blocked_v1_requests": 0,
            "blocked_route_count": len(
                blocked_fallback_identities(release, consumer_ids=consumer_ids)
            ),
        },
        "v1_provenance": v1_provenance,
        "v1_runtime_binding": v1_runtime_binding,
        "fallback_details": initial_fallback_details,
        "fallback_drill": build_fallback_return_receipt(
            release, probes, consumer_ids=consumer_ids
        ),
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "observation_seconds_requested": args.observation_seconds,
        "observation_seconds_actual": round(observation_seconds, 3),
        "opening_product_count": len(initial_results),
        "opening_operation_plan": opening_operation_plan,
        "opening_timeout_seconds_operator": args.opening_timeout_seconds,
        "opening_timeout_seconds_effective": opening_timeout_seconds,
        "closing_product_count": len(closing_results),
        "opening_seconds_actual": round(opening_seconds, 3),
        "closing_seconds_actual": round(closing_seconds, 3),
        "quota_window_wait_seconds": round(quota_window_wait_seconds, 3),
        "quota_budget": {
            consumer_id: pacer.evidence()
            for consumer_id, pacer in sorted(pacers.items())
        },
        "secret_values_recorded": False,
        "test_provenance": False,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "release_capture": {
            "captured_at_ms": int(time.time() * 1000),
            "cpu_millicores": round((cpu_seconds / max(elapsed_seconds, 0.001)) * 1000),
            "rss_bytes": rss_bytes,
        },
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    value.add_argument("--acquisition", type=Path, default=ROOT / "config/v2/stable-acquisition-bindings.yaml")
    value.add_argument("--release-routing", type=Path, default=ROOT / "config/v2/stable-v2-release-routing.yaml")
    value.add_argument("--authority-record", type=Path, required=True)
    value.add_argument("--primary-url", required=True)
    value.add_argument("--secondary-url", required=True)
    value.add_argument("--grpc-target", required=True)
    value.add_argument("--v1-base-url", required=True)
    value.add_argument("--v1-provenance", type=Path, required=True)
    value.add_argument("--v1-runtime-binding", type=Path, required=True)
    value.add_argument("--tls-ca-file", required=True)
    value.add_argument(
        "--consumer-id",
        action="append",
        choices=tuple(IDENTITY_PREFIXES),
        help="Optional bounded C2 subset; omit for the existing four-consumer scope.",
    )
    for prefix in IDENTITY_PREFIXES.values():
        value.add_argument(f"--{prefix}-tls-certificate-file", type=Path)
        value.add_argument(f"--{prefix}-tls-private-key-file", type=Path)
        value.add_argument(f"--{prefix}-jwt-private-key-file", type=Path)
        value.add_argument(f"--{prefix}-jwt-key-id")
    value.add_argument("--issuer", default="https://identity.qdl.stable.internal")
    value.add_argument("--audience", default="qdl-v2-stable")
    value.add_argument("--timeout-seconds", type=float, default=15.0)
    value.add_argument("--concurrency", type=int, default=4)
    value.add_argument("--observation-seconds", type=float, default=300.0)
    value.add_argument(
        "--read-plane-preflight",
        action="store_true",
        help=(
            "Batch-read every selected V2 route through both replicas before a "
            "full C2; it never opens streams, drills V1 fallback or observes data."
        ),
    )
    value.add_argument(
        "--batch-shape-matrix",
        action="store_true",
        help=(
            "Run the bounded strict local-BAR batch-shape/collocation matrix "
            "before the full all-scope read-plane preflight."
        ),
    )
    value.add_argument(
        "--batch-shape-consumer-id",
        choices=tuple(IDENTITY_PREFIXES),
        default="alpha.okx.paper.stable",
        help="Governed consumer whose manifest-maximum BAR batch is diagnosed.",
    )
    value.add_argument(
        "--opening-timeout-seconds", type=float,
        help=(
            "Optional operator timeout at or above the manifest-derived opening "
            "budget. When omitted, C2 uses that exact derived deadline."
        ),
    )
    value.add_argument(
        "--closing-timeout-seconds", type=float,
        default=_C2_CLOSING_REVALIDATION_MAX_SECONDS,
        help="Bound for the full-scope batch V2 closing revalidation.",
    )
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    if not 1 <= args.concurrency <= 8:
        raise SystemExit("--concurrency must be between 1 and 8")
    if not 30.0 <= args.observation_seconds <= 300.0:
        raise SystemExit("--observation-seconds must be between 30 and 300")
    if args.batch_shape_matrix and args.read_plane_preflight:
        raise SystemExit("--batch-shape-matrix and --read-plane-preflight are exclusive")
    if args.opening_timeout_seconds is not None and args.opening_timeout_seconds < 1.0:
        raise SystemExit("--opening-timeout-seconds must be positive")
    if not 30.0 <= args.closing_timeout_seconds <= 300.0:
        raise SystemExit("--closing-timeout-seconds must be between 30 and 300")
    try:
        result = asyncio.run(run(args))
    except C2OpeningCapacityError as error:
        print(json.dumps({
            "schema": "qdl.phase105.v2-identity-acceptance.v1",
            "status": "FAIL_OPENING_CAPACITY",
            "failure": error.evidence,
            "order_actions": 0,
            "payload_recorded": False,
        }, sort_keys=True, separators=(",", ":")))
        return 1
    except (
        C2ProductAcceptanceError,
        C2ReferenceProductError,
        C2ClosingBatchError,
        C2BatchShapeError,
    ) as error:
        print(json.dumps({
            "schema": "qdl.phase105.v2-identity-acceptance.v1",
            "status": "FAIL_TYPED_STATUS",
            "failure": error.evidence,
            "order_actions": 0,
            "payload_recorded": False,
        }, sort_keys=True, separators=(",", ":")))
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

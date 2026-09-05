#!/usr/bin/env python3
"""Bounded no-order C2 acceptance for the four Phase 10.5 paper consumers.

The probe reads V2 through the shared SDK, then performs a local route-selection
drill only for manifest-authorized V1 cached reads before returning to V2. It
does not install or mutate a deployed Trading System/alpha route controller.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import resource
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable
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
from qdl.consumer import StableReleaseRoutePlan, requirement_key
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
_C2_OPENING_TIMEOUT_SECONDS = 900.0
_C2_CLOSING_REVALIDATION_MAX_SECONDS = 120.0


@dataclass(frozen=True, slots=True)
class IdentityFiles:
    certificate: str
    private_key: str
    jwt_private_key: str
    jwt_key_id: str


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
            "typed_status": status_observations,
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

    async def acquire(self) -> None:
        """Reserve one real REST request without borrowing quota from a peer."""

        async with self._lock:
            now = self._clock()
            target = now if self._next_at is None else max(now, self._next_at)
            self._next_at = target + self._seconds_per_request
            self._request_count += 1
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
        }


class _PacedQueryTransport:
    """Acceptance-only adapter that charges every C2 REST call to one pacer."""

    def __init__(self, delegate, pacer: _C2ConsumerRequestPacer) -> None:
        self._delegate = delegate
        self._pacer = pacer

    async def _call(self, name: str, *args, **kwargs):
        await self._pacer.acquire()
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
        await self._pacer.acquire()
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
                hashes = tuple(
                    reference_evidence(item, result, observed_at_ns=observed_at_ns)
                    for item, result in zip(batch, response.results, strict=True)
                )
                for item, result, content_hash in zip(batch, response.results, hashes, strict=True):
                    if item.identity in values:
                        raise AssertionError("Phase 10.5 reference batch duplicated a product")
                    values[item.identity] = (
                        content_hash,
                        latency_ms,
                        attempts,
                        deferred_ms,
                        reference_quality(item, result, observed_at_ns=observed_at_ns),
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


def _closing_status_representatives(
    products: tuple[AcceptanceProduct, ...],
) -> tuple[AcceptanceProduct, ...]:
    """Keep transport-failure evidence bounded to one identity per feed."""

    by_feed: dict[str, AcceptanceProduct] = {}
    for product in products:
        by_feed.setdefault(product.feed.value, product)
    return tuple(by_feed[feed] for feed in sorted(by_feed))


async def _closing_failure_status_observations(
    client,
    products: tuple[AcceptanceProduct, ...],
    *,
    timeout_seconds: float,
) -> list[dict[str, object]]:
    """Capture bounded typed status after a failed closing batch, never payload."""

    timeout = min(5.0, timeout_seconds)
    observations: list[dict[str, object]] = []
    for product in _closing_status_representatives(products):
        try:
            status = await asyncio.wait_for(
                client.feed_status(_closing_requirement(product)),
                timeout=timeout,
            )
        except Exception as error:  # Diagnostic must not hide the primary failure.
            observations.append({
                **product.evidence(),
                "status_transport_error": type(error).__name__,
            })
        else:
            observations.append({
                **product.evidence(),
                "quality": compact_feed_status(status),
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
                except (httpx.HTTPError, TimeoutError, DataLayerError) as error:
                    status_observations = await _closing_failure_status_observations(
                        client,
                        batch,
                        timeout_seconds=timeout_seconds,
                    )
                    raise C2ClosingBatchError(
                        consumer_id=batch[0].consumer_id,
                        replica=label,
                        products=batch,
                        error=error,
                        status_observations=status_observations,
                    ) from error
                latency_ms = (time.perf_counter() - started) * 1_000
                if response.partial or len(response.results) != len(batch):
                    raise AssertionError("Phase 10.5 closing V2 batch cardinality differs")
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
    quota_window_wait_seconds = await _wait_for_clean_quota_windows(pacers)
    started = time.monotonic()
    opening_deadline = started + args.opening_timeout_seconds

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
                    stream_open_timeout_seconds=args.opening_timeout_seconds,
                    client_factory=client_factories[product.consumer_id],
                )
            except C2StatusEvidenceError as error:
                raise C2ProductAcceptanceError(product, error) from error
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
        initial_results, initial_fallback_details = await asyncio.wait_for(
            certify_ordered(), timeout=args.opening_timeout_seconds
        )
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
        args.opening_timeout_seconds
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
        "--opening-timeout-seconds", type=float, default=_C2_OPENING_TIMEOUT_SECONDS,
        help="Bound for the full quota-paced opening proof before observation starts.",
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
    if not 60.0 <= args.opening_timeout_seconds <= 1_800.0:
        raise SystemExit("--opening-timeout-seconds must be between 60 and 1800")
    if not 30.0 <= args.closing_timeout_seconds <= 300.0:
        raise SystemExit("--closing-timeout-seconds must be between 30 and 300")
    try:
        result = asyncio.run(run(args))
    except (C2ProductAcceptanceError, C2ClosingBatchError) as error:
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

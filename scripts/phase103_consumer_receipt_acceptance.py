#!/usr/bin/env python3
"""No-order Phase 10.3 V2 consumer-receipt acceptance.

This tool is intentionally an operator probe, never an ingest, execution or
provider client. It obtains all data only through the public V2 query/stream
SDK surfaces using the governed Trading System and alpha workload identities.
Run it only after the separate RUST_PRIMARY handoff packet is approved.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    DeliveryClass,
    build_consumer_acceptance_scope,
    compact_receipt_evidence,
    content_fingerprint,
    sdk_requirement,
    validate_product_view,
    validate_replica_views,
    validate_resume_offsets,
    warmup_content_fingerprint,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_prepare_shared_primary_packet import (
    validate_prepared_shared_primary_bundle,
)
from qdl_sdk import (
    AsyncDataLayerClient,
    ControlEvent,
    FileCursorStore,
    GrpcStreamTransport,
    RestQueryTransport,
    RotatingJwtCredentialProvider,
    StreamEvent,
    WorkloadTlsConfig,
    market_data_view_from_stream,
)


DEFAULT_CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
DEFAULT_TRADING_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"
DEFAULT_ALPHA_MANIFEST = ROOT / "consumers/stable/alpha-binance-paper.yaml"


@dataclass(frozen=True, slots=True)
class WorkloadIdentity:
    consumer_id: str
    subject: str
    manifest_revision: int
    tls: WorkloadTlsConfig
    credential: RotatingJwtCredentialProvider


def _identity(
    *,
    product: AcceptanceProduct,
    certificate_file: str,
    private_key_file: str,
    jwt_private_key_file: str,
    jwt_key_id: str,
    tls_ca_file: str,
    issuer: str,
    audience: str,
) -> WorkloadIdentity:
    return WorkloadIdentity(
        consumer_id=product.consumer_id,
        subject=product.consumer_subject,
        manifest_revision=product.manifest_revision,
        tls=WorkloadTlsConfig(
            tls_ca_file,
            certificate_file,
            private_key_file,
        ),
        credential=RotatingJwtCredentialProvider(
            private_key_file=jwt_private_key_file,
            key_id=jwt_key_id,
            algorithm="RS256",
            issuer=issuer,
            audience=audience,
            subject=product.consumer_subject,
            environment="paper",
            roles=("historical_reader", "market_data_reader", "stream_consumer"),
            venues=("BINANCE", "OKX"),
            consumer_manifest_revision=product.manifest_revision,
            lifetime_seconds=300,
            refresh_before_seconds=60,
        ),
    )


def _client(
    identity: WorkloadIdentity,
    *,
    base_url: str,
    grpc_target: str,
    cursor_path: Path,
    timeout_seconds: float,
) -> AsyncDataLayerClient:
    return AsyncDataLayerClient(
        query_transport=RestQueryTransport(
            base_url,
            timeout_seconds=timeout_seconds,
            credential_provider=identity.credential,
            tls=identity.tls,
        ),
        stream_transport=GrpcStreamTransport(
            grpc_target,
            tls=identity.tls,
            credential_provider=identity.credential,
        ),
        consumer_id=identity.consumer_id,
        cursor_store=FileCursorStore(cursor_path),
        max_buffer_events=64,
        max_reconnect_attempts=2,
    )


def _cursor_path(state_dir: Path, product: AcceptanceProduct) -> Path:
    identity = "|".join(product.identity).encode()
    return state_dir / f"{hashlib.sha256(identity).hexdigest()}.json"


async def _next_data(session, *, timeout_seconds: float) -> tuple[StreamEvent, list[str]]:
    controls: list[str] = []
    for _ in range(8):
        item = await asyncio.wait_for(session.__anext__(), timeout=timeout_seconds)
        if isinstance(item, ControlEvent):
            controls.append(item.code)
            continue
        if isinstance(item, StreamEvent):
            return item, controls
        raise AssertionError("V2 SDK stream emitted an unknown event type")
    raise AssertionError("V2 SDK stream emitted controls without market data")


async def _query_product(
    product: AcceptanceProduct,
    *,
    primary: AsyncDataLayerClient,
    secondary: AsyncDataLayerClient,
) -> tuple[str, str | None, float, float | None]:
    requirement = sdk_requirement(product)
    primary_started = time.perf_counter()
    if product.feed.value == "BAR":
        primary_response = await primary.warmup(requirement)
        primary_latency_ms = (time.perf_counter() - primary_started) * 1000
        for view in primary_response.data:
            validate_product_view(product, view)
        primary_hash = warmup_content_fingerprint(primary_response.data)
    else:
        primary_response = await primary.snapshot(requirement)
        primary_latency_ms = (time.perf_counter() - primary_started) * 1000
        validate_product_view(product, primary_response.data)
        primary_hash = content_fingerprint(primary_response.data)

    secondary_started = time.perf_counter()
    if product.feed.value == "BAR":
        secondary_response = await secondary.warmup(requirement)
        secondary_latency_ms = (time.perf_counter() - secondary_started) * 1000
        for view in secondary_response.data:
            validate_product_view(product, view)
        secondary_hash = warmup_content_fingerprint(secondary_response.data)
        if product.delivery is DeliveryClass.DURABLE and primary_hash != secondary_hash:
            raise AssertionError("V2 query replicas diverged on a final BAR warmup")
        validate_replica_views(
            product,
            primary_response.data[-1],
            secondary_response.data[-1],
        )
    else:
        secondary_response = await secondary.snapshot(requirement)
        secondary_latency_ms = (time.perf_counter() - secondary_started) * 1000
        primary_hash, secondary_hash = validate_replica_views(
            product,
            primary_response.data,
            secondary_response.data,
        )
    return primary_hash, secondary_hash, primary_latency_ms, secondary_latency_ms


async def _stream_resume(
    product: AcceptanceProduct,
    *,
    identity: WorkloadIdentity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
) -> tuple[int | None, int | None, tuple[str, ...]]:
    if product.delivery is not DeliveryClass.DURABLE:
        return None, None, ()
    cursor_path = _cursor_path(state_dir, product)
    requirement = sdk_requirement(product)
    first_client = _client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
    )
    try:
        async with first_client.warmup_then_stream(requirement) as session:
            first, first_controls = await _next_data(
                session,
                timeout_seconds=timeout_seconds,
            )
            first_view = market_data_view_from_stream(
                first,
                template=session.warmup.data[-1],
                requirement=requirement,
            )
            validate_product_view(product, first_view)
            session.acknowledge(first)
            first_offset = first.logical_offset
    finally:
        await first_client.close()

    resumed_client = _client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
    )
    try:
        async with resumed_client.warmup_then_stream(
            requirement,
            resume_restored_state=True,
        ) as session:
            resumed, resumed_controls = await _next_data(
                session,
                timeout_seconds=timeout_seconds,
            )
            resumed_view = market_data_view_from_stream(
                resumed,
                template=session.warmup.data[-1],
                requirement=requirement,
            )
            validate_product_view(product, resumed_view)
            validate_resume_offsets(
                acknowledged_offset=first_offset,
                resumed_offset=resumed.logical_offset,
            )
            session.acknowledge(resumed)
            return first_offset, resumed.logical_offset, tuple(first_controls + resumed_controls)
    finally:
        await resumed_client.close()


async def _certify_product(
    product: AcceptanceProduct,
    *,
    identity: WorkloadIdentity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
) -> dict[str, object]:
    primary = _client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-primary.json",
        timeout_seconds=timeout_seconds,
    )
    secondary = _client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-secondary.json",
        timeout_seconds=timeout_seconds,
    )
    try:
        primary_hash, secondary_hash, primary_ms, secondary_ms = await _query_product(
            product,
            primary=primary,
            secondary=secondary,
        )
    finally:
        await primary.close()
        await secondary.close()
    acknowledged, resumed, controls = await _stream_resume(
        product,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir,
        timeout_seconds=timeout_seconds,
    )
    result = compact_receipt_evidence(
        product,
        primary_hash=primary_hash,
        secondary_hash=secondary_hash,
        primary_latency_ms=primary_ms,
        secondary_latency_ms=secondary_ms,
        acknowledged_offset=acknowledged,
        resumed_offset=resumed,
    )
    result["control_codes"] = list(controls)
    return result


@contextmanager
def _cursor_directory(path: str | None) -> Iterator[Path]:
    if path is None:
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as value:
            yield Path(value)
        return
    directory = Path(path).expanduser().resolve()
    if directory.exists():
        raise ValueError("Phase 10.3 cursor directory must not already exist")
    directory.mkdir(mode=0o700, parents=True)
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


def _identities(args: argparse.Namespace, products: tuple[AcceptanceProduct, ...]):
    by_consumer: dict[str, AcceptanceProduct] = {}
    for product in products:
        by_consumer.setdefault(product.consumer_id, product)
    return {
        "trading-system.paper.stable": _identity(
            product=by_consumer["trading-system.paper.stable"],
            certificate_file=args.trading_tls_certificate_file,
            private_key_file=args.trading_tls_private_key_file,
            jwt_private_key_file=args.trading_jwt_private_key_file,
            jwt_key_id=args.trading_jwt_key_id,
            tls_ca_file=args.tls_ca_file,
            issuer=args.issuer,
            audience=args.audience,
        ),
        "alpha.binance.paper.stable": _identity(
            product=by_consumer["alpha.binance.paper.stable"],
            certificate_file=args.alpha_tls_certificate_file,
            private_key_file=args.alpha_tls_private_key_file,
            jwt_private_key_file=args.alpha_jwt_private_key_file,
            jwt_key_id=args.alpha_jwt_key_id,
            tls_ca_file=args.tls_ca_file,
            issuer=args.issuer,
            audience=args.audience,
        ),
    }


def _validated_packet(packet_path: Path, runtime_dir: Path) -> dict[str, object]:
    try:
        packet = json.loads(packet_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.3 handoff packet cannot be read") from error
    if not isinstance(packet, dict):
        raise ValueError("Phase 10.3 handoff packet root is invalid")
    bundle = validate_prepared_shared_primary_bundle(packet, runtime_dir=runtime_dir)
    if bundle.get("status") != "PASS" or packet.get("authority", {}).get("mode") != "RUST_PRIMARY":
        raise ValueError("Phase 10.3 handoff packet is not an active RUST_PRIMARY bundle")
    return packet


async def run(args: argparse.Namespace) -> dict[str, object]:
    packet = _validated_packet(args.handoff_packet, args.runtime_dir)
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    scope = build_consumer_acceptance_scope(
        (args.trading_manifest, args.alpha_manifest),
        catalog=catalog,
        acquisition=acquisition,
    )
    identities = _identities(args, scope.products)
    semaphore = asyncio.Semaphore(args.concurrency)

    async def certify(product: AcceptanceProduct) -> dict[str, object]:
        async with semaphore:
            return await _certify_product(
                product,
                identity=identities[product.consumer_id],
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=args.grpc_target,
                state_dir=state_dir,
                timeout_seconds=args.timeout_seconds,
            )

    with _cursor_directory(args.cursor_dir) as state_dir:
        results = await asyncio.gather(*(certify(product) for product in scope.products))
    return {
        "schema": "qdl.phase103.consumer-receipt-acceptance.v1",
        "status": "PASS",
        "expected_authority": "RUST_PRIMARY",
        "packet_sha256": packet["packet_sha256"],
        "scope_sha256": scope.sha256,
        "catalog_revision": scope.catalog_revision,
        "acquisition_revision": scope.acquisition_revision,
        "product_count": len(results),
        "durable_product_count": sum(
            item.delivery is DeliveryClass.DURABLE for item in scope.products
        ),
        "pass_through_product_count": sum(
            item.delivery is DeliveryClass.PROVIDER_PASS_THROUGH
            for item in scope.products
        ),
        "excluded": [item.evidence() for item in scope.excluded],
        "products": results,
        "cursor_directory_removed": True,
        "provider_connections": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    value.add_argument("--acquisition", type=Path, default=DEFAULT_ACQUISITION)
    value.add_argument("--trading-manifest", type=Path, default=DEFAULT_TRADING_MANIFEST)
    value.add_argument("--alpha-manifest", type=Path, default=DEFAULT_ALPHA_MANIFEST)
    value.add_argument("--primary-url", required=True)
    value.add_argument("--secondary-url", required=True)
    value.add_argument("--grpc-target", required=True)
    value.add_argument("--handoff-packet", type=Path, required=True)
    value.add_argument("--runtime-dir", type=Path, required=True)
    value.add_argument("--tls-ca-file", required=True)
    value.add_argument("--trading-tls-certificate-file", required=True)
    value.add_argument("--trading-tls-private-key-file", required=True)
    value.add_argument("--trading-jwt-private-key-file", required=True)
    value.add_argument("--trading-jwt-key-id", required=True)
    value.add_argument("--alpha-tls-certificate-file", required=True)
    value.add_argument("--alpha-tls-private-key-file", required=True)
    value.add_argument("--alpha-jwt-private-key-file", required=True)
    value.add_argument("--alpha-jwt-key-id", required=True)
    value.add_argument("--issuer", default="https://identity.qdl.stable.internal")
    value.add_argument("--audience", default="qdl-v2-stable")
    value.add_argument("--timeout-seconds", type=float, default=15.0)
    value.add_argument("--concurrency", type=int, default=4)
    value.add_argument("--cursor-dir")
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    if not 1 <= args.concurrency <= 8:
        raise SystemExit("--concurrency must be between 1 and 8")
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

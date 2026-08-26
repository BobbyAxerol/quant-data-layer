#!/usr/bin/env python3
"""Bounded no-order C2 acceptance for the four Phase 10.5 paper consumers.

The probe reads V2 through the shared SDK, then performs a local route-selection
drill only for manifest-authorized V1 cached reads before returning to V2. It
does not install or mutate a deployed Trading System/alpha route controller.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    DeliveryClass,
)
from qdl.certification.phase105_consumer_acceptance import (
    PHASE105_PAPER_CONSUMER_IDS,
    build_release_consumer_acceptance_scope,
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
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_consumer_receipt_acceptance import (
    _certify_product,
    _client,
    _identity,
    _query_product,
)


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


@dataclass(frozen=True, slots=True)
class IdentityFiles:
    certificate: str
    private_key: str
    jwt_private_key: str
    jwt_key_id: str


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


def _identity_files(args: argparse.Namespace) -> dict[str, IdentityFiles]:
    values: dict[str, IdentityFiles] = {}
    for consumer_id, prefix in IDENTITY_PREFIXES.items():
        # argparse converts every option dash into an underscore in Namespace
        # attributes, while the public CLI intentionally keeps alpha-binance
        # and alpha-okx readable as dashed option names.
        attribute_prefix = prefix.replace("-", "_")
        fields = IdentityFiles(
            certificate=str(getattr(args, f"{attribute_prefix}_tls_certificate_file")),
            private_key=str(getattr(args, f"{attribute_prefix}_tls_private_key_file")),
            jwt_private_key=str(getattr(args, f"{attribute_prefix}_jwt_private_key_file")),
            jwt_key_id=str(getattr(args, f"{attribute_prefix}_jwt_key_id")),
        )
        if not fields.jwt_key_id or any(not Path(path).is_file() for path in (
            fields.certificate, fields.private_key, fields.jwt_private_key,
        )):
            raise ValueError(f"Phase 10.5 identity material is unavailable for {consumer_id}")
        values[consumer_id] = fields
    return values


def _scope(args: argparse.Namespace):
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    release = StableReleaseRoutePlan.load(args.release_routing, manifest_root=ROOT)
    scope = build_release_consumer_acceptance_scope(
        release, catalog=catalog, acquisition=acquisition
    )
    if {item.consumer_id for item in scope.products} != PHASE105_PAPER_CONSUMER_IDS:
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
) -> tuple[str, str | None, float, float | None]:
    primary = _client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "fallback-query-primary.json",
        timeout_seconds=timeout_seconds,
    )
    secondary = _client(
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
    )
    return {
        **details,
        "before_primary_content_sha256": before[0],
        "before_secondary_content_sha256": before[1],
        "after_primary_content_sha256": after[0],
        "after_secondary_content_sha256": after[1],
        "v1_request_latency_ms": round(v1_latency_ms, 3),
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
    return tuple(await asyncio.gather(*tasks))


async def run(args: argparse.Namespace) -> dict[str, object]:
    authority = _authority(args.authority_record)
    scope, release = _scope(args)
    files = _identity_files(args)
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
        release, catalog=StableSourceCatalog.load(args.catalog), products=scope.products
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

    started = time.monotonic()
    process_started = time.process_time()
    temporary = Path(tempfile.mkdtemp(prefix="qdl-phase105-v2-identity-"))
    semaphore = asyncio.Semaphore(args.concurrency)

    async def certify(product: AcceptanceProduct) -> dict[str, object]:
        async with semaphore:
            try:
                return await _certify_product(
                    product,
                    identity=identities[product.consumer_id],
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=grpc_target,
                    state_dir=temporary,
                    timeout_seconds=args.timeout_seconds,
                )
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
        ordered = await asyncio.gather(*(certify(product) for product in consumer_products))
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
            ))
        return list(ordered), fallback_details

    async def certify_ordered() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        groups = await _run_consumer_groups(
            tuple(PHASE105_PAPER_CONSUMER_ORDER), certify_consumer
        )
        results: list[dict[str, object]] = []
        fallback_details: list[dict[str, object]] = []
        for consumer_results, consumer_fallbacks in groups:
            results.extend(consumer_results)
            fallback_details.extend(consumer_fallbacks)
        return results, fallback_details

    try:
        results, fallback_details = await asyncio.wait_for(
            certify_ordered(), timeout=args.observation_seconds
        )
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    elapsed_seconds = time.monotonic() - started
    cpu_seconds = max(0.0, time.process_time() - process_started)
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_bytes = int(max_rss) * 1024
    if elapsed_seconds > args.observation_seconds:
        raise AssertionError("Phase 10.5 identity observation exceeded its bounded window")
    route_summary = _route_summary(release, scope.products)
    return {
        "schema": "qdl.phase105.v2-identity-acceptance.v1",
        "status": "PASS_V2_DATA_PLANE_ONLY",
        "release_route_plan_sha256": release.digest,
        "authority_revision": authority.get("revision"),
        "scope_sha256": scope.sha256,
        "product_count": len(results),
        "durable_product_count": sum(
            item.delivery is DeliveryClass.DURABLE for item in scope.products
        ),
        "products": results,
        "route_contract": {
            **route_summary,
            "v1_fallback_observed": True,
            "route_selection_probe_only": True,
            "blocked_v1_requests": 0,
            "blocked_route_count": len(blocked_fallback_identities(release)),
        },
        "v1_provenance": v1_provenance,
        "v1_runtime_binding": v1_runtime_binding,
        "fallback_details": fallback_details,
        "fallback_drill": build_fallback_return_receipt(release, probes),
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
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
    for prefix in IDENTITY_PREFIXES.values():
        value.add_argument(f"--{prefix}-tls-certificate-file", type=Path, required=True)
        value.add_argument(f"--{prefix}-tls-private-key-file", type=Path, required=True)
        value.add_argument(f"--{prefix}-jwt-private-key-file", type=Path, required=True)
        value.add_argument(f"--{prefix}-jwt-key-id", required=True)
    value.add_argument("--issuer", default="https://identity.qdl.stable.internal")
    value.add_argument("--audience", default="qdl-v2-stable")
    value.add_argument("--timeout-seconds", type=float, default=15.0)
    value.add_argument("--concurrency", type=int, default=4)
    value.add_argument("--observation-seconds", type=float, default=300.0)
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    if not 1 <= args.concurrency <= 8:
        raise SystemExit("--concurrency must be between 1 and 8")
    if not 30.0 <= args.observation_seconds <= 300.0:
        raise SystemExit("--observation-seconds must be between 30 and 300")
    print(json.dumps(asyncio.run(run(args)), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

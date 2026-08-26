#!/usr/bin/env python3
"""Bounded no-order V2 identity acceptance for the four Phase 10.5 consumers.

This checks the actual V2 query/stream boundary only.  It does not claim a V1
fallback transition because the deployed Trading System/alpha adapters have not
yet installed a versioned V2-to-V1 route controller.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

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
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_consumer_receipt_acceptance import _certify_product, _identity


IDENTITY_PREFIXES = {
    "monitoring.multivenue.stable": "monitoring",
    "trading-system.paper.stable": "trading",
    "alpha.binance.paper.stable": "alpha-binance",
    "alpha.okx.paper.stable": "alpha-okx",
}


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


async def run(args: argparse.Namespace) -> dict[str, object]:
    authority = _authority(args.authority_record)
    scope, release = _scope(args)
    files = _identity_files(args)
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
                    grpc_target=args.grpc_target,
                    state_dir=temporary,
                    timeout_seconds=args.timeout_seconds,
                )
            except Exception as error:
                raise RuntimeError(
                    "Phase 10.5 V2 identity receipt failed "
                    f"consumer={product.consumer_id} instrument={product.instrument_id} "
                    f"feed={product.feed.value} interval={product.interval}"
                ) from error

    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(certify(product) for product in scope.products)),
            timeout=args.observation_seconds,
        )
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    elapsed_seconds = time.monotonic() - started
    if elapsed_seconds > args.observation_seconds:
        raise AssertionError("Phase 10.5 identity observation exceeded its bounded window")
    route_summary = _route_summary(release, scope.products)
    return {
        "schema": "qdl.phase105.v2-identity-acceptance.v1",
        "status": "PASS_V2_DATA_PLANE_ONLY",
        "authority_revision": authority.get("revision"),
        "scope_sha256": scope.sha256,
        "product_count": len(results),
        "durable_product_count": sum(
            item.delivery is DeliveryClass.DURABLE for item in scope.products
        ),
        "products": results,
        "route_contract": {
            **route_summary,
            "v1_fallback_observed": False,
            "reason": "CONSUMER_ROUTE_CONTROLLER_NOT_DEPLOYED",
        },
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "secret_values_recorded": False,
        "elapsed_seconds": round(elapsed_seconds, 3),
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

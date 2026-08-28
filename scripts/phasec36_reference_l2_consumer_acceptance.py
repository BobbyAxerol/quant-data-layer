#!/usr/bin/env python3
"""Run the bounded V2-only Reference/L2 consumer acceptance receipt.

The process is deliberately disposable: it uses the existing paper identity,
the public V2 SDK and only supplied V2 query/stream endpoints. It never reads
V1, connects directly to a venue, or has execution configuration.
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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import DeliveryClass
from qdl.certification.reference_l2_acceptance import (
    build_reference_l2_acceptance_scope,
    validate_reference_batch,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from scripts.phase103_consumer_receipt_acceptance import (
    _certify_product,
    _client,
    _identity,
)


def _write_receipt(path: Path, receipt: dict[str, object]) -> None:
    target = path.expanduser().resolve()
    if target.exists():
        raise ValueError("Reference/L2 receipt path already exists")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    target.chmod(0o600)


async def _certify_references(scope, *, identity, args, state_dir: Path) -> list[dict[str, object]]:
    primary = _client(
        identity,
        base_url=args.primary_url,
        grpc_target=args.grpc_target,
        cursor_path=state_dir / "reference-primary.json",
        timeout_seconds=args.timeout_seconds,
    )
    secondary = _client(
        identity,
        base_url=args.secondary_url,
        grpc_target=args.grpc_target,
        cursor_path=state_dir / "reference-secondary.json",
        timeout_seconds=args.timeout_seconds,
    )
    try:
        primary_started = time.perf_counter()
        primary_response = await primary.reference_batch(
            [item.sdk_requirement for item in scope.references], require_all=True
        )
        primary_latency_ms = (time.perf_counter() - primary_started) * 1_000
        primary_hashes = validate_reference_batch(
            scope.references,
            primary_response,
            observed_at_ns=time.time_ns(),
        )

        secondary_started = time.perf_counter()
        secondary_response = await secondary.reference_batch(
            [item.sdk_requirement for item in scope.references], require_all=True
        )
        secondary_latency_ms = (time.perf_counter() - secondary_started) * 1_000
        secondary_hashes = validate_reference_batch(
            scope.references,
            secondary_response,
            observed_at_ns=time.time_ns(),
        )
    finally:
        await primary.close()
        await secondary.close()
    return [
        {
            **product.evidence(),
            "primary_content_sha256": primary_hash,
            "secondary_content_sha256": secondary_hash,
            "primary_latency_ms": round(primary_latency_ms, 3),
            "secondary_latency_ms": round(secondary_latency_ms, 3),
            "v1_fallback_attempted": False,
        }
        for product, primary_hash, secondary_hash in zip(
            scope.references, primary_hashes, secondary_hashes, strict=True
        )
    ]


async def run(args: argparse.Namespace) -> dict[str, object]:
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    scope = build_reference_l2_acceptance_scope(
        args.manifest,
        catalog=catalog,
        acquisition=acquisition,
        now_ns=time.time_ns(),
    )
    identity = _identity(
        product=scope.books[0],
        certificate_file=str(args.tls_certificate_file),
        private_key_file=str(args.tls_private_key_file),
        jwt_private_key_file=str(args.jwt_private_key_file),
        jwt_key_id=args.jwt_key_id,
        tls_ca_file=str(args.tls_ca_file),
        issuer=args.issuer,
        audience=args.audience,
    )
    started = time.monotonic()
    process_started = time.process_time()
    temporary = Path(tempfile.mkdtemp(prefix="qdl-reference-l2-acceptance-"))
    semaphore = asyncio.Semaphore(args.concurrency)

    async def certify_book(product):
        async with semaphore:
            details = await _certify_product(
                product,
                identity=identity,
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=args.grpc_target,
                state_dir=temporary,
                timeout_seconds=args.timeout_seconds,
            )
            if product.delivery is not DeliveryClass.DURABLE:
                raise AssertionError("Reference/L2 book route lost durable delivery")
            return details

    try:
        references, books = await asyncio.wait_for(
            asyncio.gather(
                _certify_references(scope, identity=identity, args=args, state_dir=temporary),
                asyncio.gather(*(certify_book(product) for product in scope.books)),
            ),
            timeout=args.observation_seconds,
        )
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    elapsed_seconds = time.monotonic() - started
    if elapsed_seconds > args.observation_seconds:
        raise AssertionError("Reference/L2 observation exceeded its bounded window")
    cpu_seconds = max(0.0, time.process_time() - process_started)
    receipt = {
        "schema": "qdl.reference-l2.consumer-acceptance.v1",
        "status": "PASS_V2_DATA_PLANE_ONLY",
        "scope_sha256": scope.sha256,
        "manifest_revision": scope.manifest_revision,
        "manifest_sha256": scope.manifest_sha256,
        "catalog_revision": scope.catalog_revision,
        "acquisition_revision": scope.acquisition_revision,
        "reference_product_count": len(references),
        "durable_book_product_count": len(books),
        "reference_products": references,
        "book_products": books,
        "provider_connections": 0,
        "v1_requests": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "secret_values_recorded": False,
        "test_provenance": False,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "resource": {
            "cpu_millicores": round((cpu_seconds / max(elapsed_seconds, 0.001)) * 1_000),
            "rss_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1_024,
        },
    }
    if args.receipt_path is not None:
        _write_receipt(args.receipt_path, receipt)
    return receipt


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    value.add_argument("--acquisition", type=Path, default=ROOT / "config/v2/stable-acquisition-bindings.yaml")
    value.add_argument("--manifest", type=Path, default=ROOT / "consumers/stable/reference-l2-stable.yaml")
    value.add_argument("--primary-url", required=True)
    value.add_argument("--secondary-url", required=True)
    value.add_argument("--grpc-target", required=True)
    value.add_argument("--tls-ca-file", type=Path, required=True)
    value.add_argument("--tls-certificate-file", type=Path, required=True)
    value.add_argument("--tls-private-key-file", type=Path, required=True)
    value.add_argument("--jwt-private-key-file", type=Path, required=True)
    value.add_argument("--jwt-key-id", required=True)
    value.add_argument("--issuer", default="https://identity.qdl.stable.internal")
    value.add_argument("--audience", default="qdl-v2-stable")
    value.add_argument("--timeout-seconds", type=float, default=15.0)
    value.add_argument("--concurrency", type=int, default=4)
    value.add_argument("--observation-seconds", type=float, default=300.0)
    value.add_argument("--receipt-path", type=Path)
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

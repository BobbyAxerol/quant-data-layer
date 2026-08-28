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
    acceptance_transport_timeout_seconds,
    build_reference_l2_acceptance_scope,
    reference_acceptance_batches,
    validate_reference_batch,
)
from qdl.query import FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk.reference import BasisSeries
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


def _native_basis_cooldown_ms(batch, response) -> int | None:
    """Recognize only Rust's one typed native-BASIS cooldown response.

    This is receipt behavior, not provider retry policy: the query service
    remains fail-closed, and no other product/error becomes retryable here.
    """

    if len(batch) != 1:
        return None
    product = batch[0]
    if (
        product.venue != "BINANCE"
        or product.requirement.feed is not FeedType.BASIS
        or product.sdk_requirement.basis_series is not BasisSeries.NATIVE
        or getattr(response, "partial", None) is not True
        or getattr(response, "success_count", None) != 0
        or getattr(response, "error_count", None) != 1
        or len(getattr(response, "results", ())) != 1
    ):
        return None
    item = response.results[0]
    problem = getattr(item, "problem", None)
    data = getattr(item, "data", None)
    retry_after_ms = getattr(problem, "retry_after_ms", None)
    if (
        getattr(item, "status", None) != "ERROR"
        or getattr(problem, "code", None) != "SOURCE_UNAVAILABLE"
        or getattr(problem, "retryable", None) is not True
        or not isinstance(retry_after_ms, int)
        or retry_after_ms <= 0
        or getattr(data, "status", None) != "ERROR"
        or getattr(data, "error_code", None) != "PROVIDER_RETRY_EXHAUSTED"
    ):
        return None
    return retry_after_ms


async def _reference_batch_until_terminal(
    client,
    batch,
    *,
    deadline_monotonic: float,
    clock=time.monotonic,
    sleep=asyncio.sleep,
):
    """Run one V2 batch with at most one explicit native-BASIS deferral."""

    attempts = 0
    deferred_ms = 0
    while True:
        attempts += 1
        response = await client.reference_batch(
            [item.sdk_requirement for item in batch], require_all=False
        )
        retry_after_ms = _native_basis_cooldown_ms(batch, response)
        if retry_after_ms is None or attempts >= 2:
            return response, attempts, deferred_ms
        remaining_ms = int((deadline_monotonic - clock()) * 1_000)
        if retry_after_ms + 250 >= remaining_ms:
            raise AssertionError(
                "native BASIS cooldown exceeds the bounded Reference/L2 acceptance deadline"
            )
        await sleep(retry_after_ms / 1_000)
        deferred_ms += retry_after_ms


async def _certify_references(
    scope,
    *,
    identity,
    args,
    state_dir: Path,
    transport_timeout_seconds: float,
    deadline_monotonic: float,
) -> list[dict[str, object]]:
    primary = _client(
        identity,
        base_url=args.primary_url,
        grpc_target=args.grpc_target,
        cursor_path=state_dir / "reference-primary.json",
        timeout_seconds=transport_timeout_seconds,
    )
    secondary = _client(
        identity,
        base_url=args.secondary_url,
        grpc_target=args.grpc_target,
        cursor_path=state_dir / "reference-secondary.json",
        timeout_seconds=transport_timeout_seconds,
    )
    try:
        async def certify(client):
            values = {}
            for batch in reference_acceptance_batches(scope.references):
                started = time.perf_counter()
                response, attempts, deferred_ms = await _reference_batch_until_terminal(
                    client,
                    batch,
                    deadline_monotonic=deadline_monotonic,
                )
                latency_ms = (time.perf_counter() - started) * 1_000
                hashes = validate_reference_batch(
                    batch,
                    response,
                    observed_at_ns=time.time_ns(),
                )
                for product, content_hash in zip(batch, hashes, strict=True):
                    if product.identity in values:
                        raise AssertionError("Reference/L2 receipt batch duplicated a product")
                    values[product.identity] = (content_hash, latency_ms, attempts, deferred_ms)
            if len(values) != len(scope.references):
                raise AssertionError("Reference/L2 receipt batch lost a product")
            return values

        primary_results = await certify(primary)
        secondary_results = await certify(secondary)
    finally:
        await primary.close()
        await secondary.close()
    return [
        {
            **product.evidence(),
            "primary_content_sha256": primary_results[product.identity][0],
            "secondary_content_sha256": secondary_results[product.identity][0],
            "primary_latency_ms": round(primary_results[product.identity][1], 3),
            "secondary_latency_ms": round(secondary_results[product.identity][1], 3),
            "primary_provider_attempts": primary_results[product.identity][2],
            "secondary_provider_attempts": secondary_results[product.identity][2],
            "primary_provider_deferred_ms": primary_results[product.identity][3],
            "secondary_provider_deferred_ms": secondary_results[product.identity][3],
            "v1_fallback_attempted": False,
        }
        for product in scope.references
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
    transport_timeout_seconds = acceptance_transport_timeout_seconds(args.timeout_seconds)

    async def certify_book(product):
        async with semaphore:
            details = await _certify_product(
                product,
                identity=identity,
                primary_url=args.primary_url,
                secondary_url=args.secondary_url,
                grpc_target=args.grpc_target,
                state_dir=temporary,
                timeout_seconds=transport_timeout_seconds,
            )
            if product.delivery is not DeliveryClass.DURABLE:
                raise AssertionError("Reference/L2 book route lost durable delivery")
            return details

    try:
        references, books = await asyncio.wait_for(
            asyncio.gather(
                _certify_references(
                    scope,
                    identity=identity,
                    args=args,
                    state_dir=temporary,
                    transport_timeout_seconds=transport_timeout_seconds,
                    deadline_monotonic=started + args.observation_seconds,
                ),
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

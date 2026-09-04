#!/usr/bin/env python3
"""Read only the active execution-L2 books through both V2 query replicas.

This is a preflight, not an ingest or execution client.  It derives the exact
physical book scope from the execution demand document, reads typed status and
one public V2 snapshot from each query replica, then writes compact evidence
without retaining market levels, prices, credentials or cursors.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    build_manifest_acceptance_scope,
    sdk_requirement,
    validate_product_view,
)
from qdl.query import FeedType
from qdl.runtime.execution_l2 import execution_l2_materialization_plan
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl_sdk.errors import DataLayerError
from scripts.phase103_consumer_receipt_acceptance import (
    _client,
    _identity,
    compact_feed_status,
)


DEFAULT_CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
DEFAULT_EXECUTION_DEMAND = ROOT / "config/v2/stable-crypto-demand.yaml"
DEFAULT_TRADING_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"
EXPECTED_CONSUMER_ID = "trading-system.paper.stable"


def execution_book_products(
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    execution_demand: Path,
    trading_manifest: Path,
) -> tuple[AcceptanceProduct, ...]:
    """Join the Trading System manifest to all derived execution L2 sources."""

    plan = execution_l2_materialization_plan(
        demand_path=execution_demand,
        catalog=catalog,
        acquisition=acquisition,
    )
    scope = build_manifest_acceptance_scope(
        (trading_manifest,),
        catalog=catalog,
        acquisition=acquisition,
        expected_consumer_ids=frozenset({EXPECTED_CONSUMER_ID}),
        schema="qdl.phase105.consumer-acceptance-scope.v1",
        requirement_filter=lambda item: item.feed is FeedType.BOOK_SNAPSHOT,
    )
    source_by_binding = {item.binding_id: item.source_id for item in catalog.bindings}
    products = tuple(
        item
        for item in scope.products
        if item.binding_id is not None
        and source_by_binding.get(item.binding_id) in plan.source_ids
    )
    actual_ids = {
        source_by_binding[item.binding_id]
        for item in products
        if item.binding_id is not None
    }
    if actual_ids != set(plan.source_ids) or len(products) != len(plan.source_ids):
        raise ValueError("Trading System execution L2 matrix differs from the declared demand")
    return tuple(sorted(products, key=lambda item: (item.venue, item.native_symbol)))


def compact_book_snapshot(view: object) -> dict[str, object]:
    """Keep readiness evidence, never book levels or price/quantity payloads."""

    payload = getattr(view, "payload", None)
    source = getattr(view, "source", None)
    quality = getattr(view, "quality", None)
    fields = {
        "source_id": getattr(source, "source_id", None),
        "book_generation": getattr(payload, "book_generation", None),
        "sequence_verified": getattr(payload, "sequence_verified", None),
        "native_sequence": getattr(payload, "native_sequence", None),
        "depth": getattr(payload, "depth", None),
        "revision": getattr(view, "revision", None),
        "watermark_offset": getattr(view, "watermark_offset", None),
        "received_at_ns": getattr(view, "received_at_ns", None),
        "event_age_ms": getattr(quality, "freshness_ms", None),
        "gap_open": getattr(quality, "gap_open", None),
        "complete": getattr(quality, "complete", None),
        "execution_eligible": getattr(quality, "execution_eligible", None),
    }
    if (
        not isinstance(fields["source_id"], str)
        or not fields["source_id"]
        or not isinstance(fields["book_generation"], int)
        or fields["book_generation"] < 0
        or not isinstance(fields["sequence_verified"], bool)
        or not isinstance(fields["native_sequence"], str)
        or not fields["native_sequence"]
        or not isinstance(fields["depth"], int)
        or fields["depth"] < 1
        or not isinstance(fields["revision"], int)
        or fields["revision"] < 0
        or not isinstance(fields["watermark_offset"], int)
        or fields["watermark_offset"] < 0
        or not isinstance(fields["received_at_ns"], int)
        or fields["received_at_ns"] < 1
        or not isinstance(fields["event_age_ms"], int)
        or fields["event_age_ms"] < 0
        or not isinstance(fields["gap_open"], bool)
        or not isinstance(fields["complete"], bool)
        or not isinstance(fields["execution_eligible"], bool)
    ):
        raise ValueError("execution L2 snapshot evidence has invalid typed fields")
    return {**fields, "payload_recorded": False}


def ready_book_row(row: Mapping[str, object]) -> bool:
    """Return true only for a fully verified execution-grade compact row."""

    status = row.get("typed_status")
    snapshot = row.get("snapshot")
    if not isinstance(status, Mapping) or not isinstance(snapshot, Mapping):
        return False
    quality = status.get("quality")
    return bool(
        isinstance(quality, Mapping)
        and quality.get("state") == "LIVE"
        and quality.get("complete") is True
        and quality.get("gap_open") is False
        and quality.get("execution_eligible") is True
        and snapshot.get("sequence_verified") is True
        and isinstance(snapshot.get("book_generation"), int)
        and int(snapshot["book_generation"]) >= 1
        and isinstance(snapshot.get("depth"), int)
        and int(snapshot["depth"]) >= 100
        and snapshot.get("complete") is True
        and snapshot.get("gap_open") is False
        and snapshot.get("execution_eligible") is True
    )


def replica_parity(primary: Mapping[str, object], secondary: Mapping[str, object]) -> bool:
    """Compare invariant identity/quality fields; native sequence may advance."""

    for field in (
        "instrument_uid",
        "venue",
        "market",
        "native_symbol",
        "feed",
        "source_policy_id",
        "source_id",
        "depth",
    ):
        if primary.get(field) != secondary.get(field):
            return False
    return ready_book_row(primary) and ready_book_row(secondary)


async def _read_one(
    product: AcceptanceProduct,
    *,
    label: str,
    base_url: str,
    grpc_target: str,
    identity,
    state_dir: Path,
    timeout_seconds: float,
) -> dict[str, object]:
    requirement = sdk_requirement(product)
    client = _client(
        identity,
        base_url=base_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / f"{label}-{product.instrument_uid}.cursor",
        timeout_seconds=timeout_seconds,
    )
    source_id = ""
    result: dict[str, object] = {
        "typed_status": None,
        "status_error": None,
        "snapshot": None,
        "snapshot_error": None,
    }
    try:
        try:
            status = await client.feed_status(requirement)
            status_evidence = compact_feed_status(status)
        except DataLayerError as error:
            result["status_error"] = {"code": error.code, "detail": error.detail}
            return {
                "replica": label,
                "instrument_uid": product.instrument_uid,
                "instrument_id": product.instrument_id,
                "venue": product.venue,
                "market": product.market,
                "native_symbol": product.native_symbol,
                "feed": product.feed.value,
                "source_policy_id": product.source_policy_id,
                "source_id": source_id,
                **result,
                "payload_recorded": False,
            }
        result["typed_status"] = status_evidence
        try:
            response = await client.snapshot(requirement)
            view = response.data
            validate_product_view(product, view)
            snapshot = compact_book_snapshot(view)
            source_id = str(snapshot["source_id"])
            result["snapshot"] = snapshot
        except DataLayerError as error:
            result["snapshot_error"] = {"code": error.code, "detail": error.detail}
        except ValueError as error:
            result["snapshot_error"] = {"code": "INVALID_VIEW", "detail": str(error)}
    finally:
        await client.close()
    return {
        "replica": label,
        "instrument_uid": product.instrument_uid,
        "instrument_id": product.instrument_id,
        "venue": product.venue,
        "market": product.market,
        "native_symbol": product.native_symbol,
        "feed": product.feed.value,
        "source_policy_id": product.source_policy_id,
        "source_id": source_id,
        **result,
        "payload_recorded": False,
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    products = execution_book_products(
        catalog=catalog,
        acquisition=acquisition,
        execution_demand=args.execution_demand,
        trading_manifest=args.trading_manifest,
    )
    identity = _identity(
        product=products[0],
        certificate_file=str(args.tls_certificate_file),
        private_key_file=str(args.tls_private_key_file),
        jwt_private_key_file=str(args.jwt_private_key_file),
        jwt_key_id=args.jwt_key_id,
        tls_ca_file=str(args.tls_ca_file),
        issuer=args.issuer,
        audience=args.audience,
    )
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="qdl-execution-l2-status-") as raw:
        state_dir = Path(raw)
        rows = []
        for product in products:
            primary, secondary = await asyncio.gather(
                _read_one(
                    product,
                    label="primary",
                    base_url=args.primary_url,
                    grpc_target=args.grpc_target,
                    identity=identity,
                    state_dir=state_dir,
                    timeout_seconds=args.timeout_seconds,
                ),
                _read_one(
                    product,
                    label="secondary",
                    base_url=args.secondary_url,
                    grpc_target=args.grpc_target,
                    identity=identity,
                    state_dir=state_dir,
                    timeout_seconds=args.timeout_seconds,
                ),
            )
            rows.append({
                "instrument_uid": product.instrument_uid,
                "instrument_id": product.instrument_id,
                "venue": product.venue,
                "market": product.market,
                "native_symbol": product.native_symbol,
                "source_policy_id": product.source_policy_id,
                "primary": primary,
                "secondary": secondary,
                "replica_parity": replica_parity(primary, secondary),
            })
    ready = all(
        row["replica_parity"]
        and ready_book_row(row["primary"])
        and ready_book_row(row["secondary"])
        for row in rows
    )
    return {
        "schema": "qdl.phase105.execution-l2-status-matrix.v1",
        "status": "PASS" if ready else "FAIL",
        "consumer_id": EXPECTED_CONSUMER_ID,
        "book_count": len(rows),
        "replica_count": 2,
        "rows": rows,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "payload_recorded": False,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    value.add_argument("--acquisition", type=Path, default=DEFAULT_ACQUISITION)
    value.add_argument("--execution-demand", type=Path, default=DEFAULT_EXECUTION_DEMAND)
    value.add_argument("--trading-manifest", type=Path, default=DEFAULT_TRADING_MANIFEST)
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
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

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
    """Join the Trading System manifest to every execution L2 source pair."""

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
        requirement_filter=lambda item: item.feed in {
            FeedType.BOOK_SNAPSHOT,
            FeedType.BOOK_DELTA,
        },
    )
    source_by_binding = {item.binding_id: item.source_id for item in catalog.bindings}
    products = tuple(
        item
        for item in scope.products
        if item.binding_id is not None
        and source_by_binding.get(item.binding_id) in plan.source_ids
    )
    by_source: dict[str, set[FeedType]] = {}
    for item in products:
        assert item.binding_id is not None
        source_id = source_by_binding[item.binding_id]
        by_source.setdefault(source_id, set()).add(item.feed)
    if set(by_source) != set(plan.source_ids) or any(
        feeds != {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}
        for feeds in by_source.values()
    ):
        raise ValueError("Trading System execution L2 matrix differs from the declared demand")
    if len(products) != len(plan.source_ids) * 2:
        raise ValueError("Trading System execution L2 matrix has an incomplete source pair")
    return tuple(sorted(
        products,
        key=lambda item: (item.venue, item.native_symbol, item.feed.value),
    ))


def compact_book_view(view: object) -> dict[str, object]:
    """Keep L2 readiness evidence, never levels, prices or quantities."""

    payload = getattr(view, "payload", None)
    source = getattr(view, "source", None)
    quality = getattr(view, "quality", None)
    feed = getattr(payload, "feed", None)
    feed_value = getattr(feed, "value", feed)
    fields = {
        "source_id": getattr(source, "source_id", None),
        "feed": feed_value,
        "book_generation": getattr(payload, "book_generation", None),
        "sequence_verified": getattr(payload, "sequence_verified", None),
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
        or feed_value not in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
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
        raise ValueError("execution L2 view evidence has invalid typed fields")
    if feed_value == "BOOK_SNAPSHOT":
        depth = getattr(payload, "depth", None)
        native_sequence = getattr(payload, "native_sequence", None)
        if (
            not isinstance(depth, int)
            or depth < 1
            or not isinstance(native_sequence, str)
            or not native_sequence
        ):
            raise ValueError("execution L2 snapshot evidence is incomplete")
        return {
            **fields,
            "depth": depth,
            "sequence_present": True,
            "payload_recorded": False,
        }
    sequence_fields = (
        getattr(payload, "native_sequence_start", None),
        getattr(payload, "native_sequence_end", None),
        getattr(payload, "snapshot_sequence", None),
    )
    reset = getattr(payload, "reset", None)
    if (
        not all(isinstance(value, str) and value for value in sequence_fields)
        or not isinstance(reset, bool)
    ):
        raise ValueError("execution L2 delta evidence is incomplete")
    return {
        **fields,
        "sequence_present": True,
        "reset": reset,
        "payload_recorded": False,
    }


def ready_book_row(row: Mapping[str, object]) -> bool:
    """Return true only for a fully verified execution-grade compact row."""

    status = row.get("typed_status")
    view = row.get("view")
    if not isinstance(status, Mapping) or not isinstance(view, Mapping):
        return False
    quality = status.get("quality")
    flags = status.get("flags")
    if not isinstance(flags, list) or any(
        not isinstance(flag, str) for flag in flags
    ):
        return False
    session_unhealthy = any(flag.startswith("SOURCE_SESSION_") for flag in flags)
    common_ready = bool(
        isinstance(quality, Mapping)
        and quality.get("state") == "LIVE"
        and quality.get("complete") is True
        and quality.get("gap_open") is False
        and not session_unhealthy
        and view.get("sequence_verified") is True
        and isinstance(view.get("book_generation"), int)
        and int(view["book_generation"]) >= 1
        and view.get("complete") is True
        and view.get("gap_open") is False
        and view.get("sequence_present") is True
    )
    if not common_ready:
        return False
    if row.get("feed") == "BOOK_SNAPSHOT":
        return bool(
            quality.get("execution_eligible") is True
            and view.get("execution_eligible") is True
            and isinstance(view.get("depth"), int)
            and int(view["depth"]) >= 100
        )
    return bool(
        row.get("feed") == "BOOK_DELTA"
        and view.get("reset") is False
        and quality.get("provider_session_state") == "LIVE"
        and isinstance(quality.get("provider_session_liveness_ms"), int)
        and quality["provider_session_liveness_ms"] >= 0
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


def source_pair_ready(rows: tuple[Mapping[str, object], ...]) -> bool:
    """A physical L2 source is usable only as one verified snapshot/delta pair."""

    if len(rows) != 2 or {row.get("feed") for row in rows} != {
        "BOOK_SNAPSHOT", "BOOK_DELTA"
    }:
        return False
    source_ids = {row.get("source_id") for row in rows}
    generations = {
        row.get("view", {}).get("book_generation")
        for row in rows
        if isinstance(row.get("view"), Mapping)
    }
    return (
        len(source_ids) == 1
        and None not in source_ids
        and len(generations) == 1
        and all(ready_book_row(row) for row in rows)
    )


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
        "view": None,
        "view_error": None,
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
            compact_view = compact_book_view(view)
            source_id = str(compact_view["source_id"])
            result["view"] = compact_view
        except DataLayerError as error:
            result["view_error"] = {"code": error.code, "detail": error.detail}
        except ValueError as error:
            result["view_error"] = {"code": "INVALID_VIEW", "detail": str(error)}
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


async def _run_round(args: argparse.Namespace) -> dict[str, object]:
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
                "feed": product.feed.value,
                "source_policy_id": product.source_policy_id,
                "primary": primary,
                "secondary": secondary,
                "replica_parity": replica_parity(primary, secondary),
            })
    by_replica_source: dict[str, dict[str, list[Mapping[str, object]]]] = {
        "primary": {}, "secondary": {},
    }
    for row in rows:
        for label in ("primary", "secondary"):
            replica = row[label]
            assert isinstance(replica, Mapping)
            source_id = replica.get("source_id")
            if isinstance(source_id, str) and source_id:
                by_replica_source[label].setdefault(source_id, []).append(replica)
    source_pair_results = {
        label: {
            source_id: source_pair_ready(tuple(source_rows))
            for source_id, source_rows in sorted(by_source.items())
        }
        for label, by_source in by_replica_source.items()
    }
    ready = all(
        row["replica_parity"]
        and ready_book_row(row["primary"])
        and ready_book_row(row["secondary"])
        for row in rows
    ) and all(
        all(result.values()) for result in source_pair_results.values()
    ) and all(
        len(by_source) == len(products) // 2
        for by_source in by_replica_source.values()
    )
    return {
        "schema": "qdl.phase105.execution-l2-status-matrix.v1",
        "status": "PASS" if ready else "FAIL",
        "consumer_id": EXPECTED_CONSUMER_ID,
        "book_count": len(rows) // 2,
        "book_product_count": len(rows),
        "replica_count": 2,
        "source_pair_results": source_pair_results,
        "rows": rows,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "payload_recorded": False,
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    """Require a short stable read window, not one lucky post-reconnect read."""

    rounds: list[dict[str, object]] = []
    for index in range(args.rounds):
        round_result = await _run_round(args)
        rounds.append(round_result)
        if index + 1 < args.rounds:
            await asyncio.sleep(args.period_seconds)
    final = dict(rounds[-1])
    final["status"] = "PASS" if all(
        item["status"] == "PASS" for item in rounds
    ) else "FAIL"
    final["round_count"] = args.rounds
    final["ready_round_count"] = sum(
        item["status"] == "PASS" for item in rounds
    )
    final["period_seconds"] = args.period_seconds
    final["round_summaries"] = [
        {
            "round": index + 1,
            "status": item["status"],
            "elapsed_seconds": item["elapsed_seconds"],
            "failed_product_identities": [
                {
                    "venue": row["venue"],
                    "native_symbol": row["native_symbol"],
                    "feed": row["primary"].get("feed"),
                }
                for row in item["rows"]
                if not row["replica_parity"]
            ],
        }
        for index, item in enumerate(rounds)
    ]
    return final


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
    value.add_argument("--rounds", type=int, default=3)
    value.add_argument("--period-seconds", type=float, default=2.0)
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    if not 1 <= args.rounds <= 10:
        raise SystemExit("--rounds must be between 1 and 10")
    if not 0.5 <= args.period_seconds <= 15.0:
        raise SystemExit("--period-seconds must be between 0.5 and 15")
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

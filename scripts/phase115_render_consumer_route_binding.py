#!/usr/bin/env python3
"""Render one sealed Phase-11.5 consumer route binding from a local artifact.

This is deliberately pure control-plane tooling: it reads an existing universal
release manifest/preflight artifact and writes one caller-named JSON document.
It never opens a provider connection, starts a role, changes a route, or writes
to Kafka, Redis, SQLite, V1, Trading System or an alpha.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.consumer.universal_release import (
    ConsumerRouteBinding,
    UniversalReleaseManifest,
    UniversalReleaseProduct,
    UniversalConsumerClass,
    UniversalReleasePolicy,
)
from qdl.consumer.release import StableReleaseRoutePlan
from qdl.consumer.realtime_route import requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog


def binding_from_stable_release(path: Path, root: Path, consumer_id: str) -> ConsumerRouteBinding:
    """Project an already admitted stable release; never infer extra entitlements."""
    plan = StableReleaseRoutePlan.load(path, manifest_root=root)
    consumer = next((c for c in plan.consumers if c.consumer_id == consumer_id), None)
    if consumer is None:
        raise ValueError("stable release has no requested consumer")
    policy = UniversalReleasePolicy.load(root / "config/v2/universal-release-policy.yaml", manifest_root=root)
    catalog = StableSourceCatalog.load(plan.source_catalog.path)
    routes = {r.requirement_key: r for r in consumer.products}
    products = []
    consumer_class = UniversalConsumerClass.TRADING_SYSTEM
    if not consumer_id.startswith("trading-system."):
        raise ValueError("stable renderer currently supports the Trading System consumer only")
    for requirement in consumer.manifest.requirements:
        key = requirement_key(requirement)
        route = routes[key]
        if route.route == "V1_PRIMARY":
            continue
        instrument = catalog.instrument_for(requirement.instrument_uid)
        identity = instrument.identity
        feed = requirement.feed.value
        plane = "L2" if feed in {"BOOK_SNAPSHOT", "BOOK_DELTA"} else (
            "REALTIME" if feed in {"TRADE", "QUOTE", "BAR"} else "REFERENCE"
        )
        rule = None
        if route.fallback == "V1":
            rule = next((r.rule_id for r in policy.fallback_rules if (
                r.venue, r.market, r.product_type, r.feed, r.interval
            ) == (identity.venue, identity.market, identity.product_type.value,
                  feed, requirement.interval)), None)
            if rule is None:
                raise ValueError("stable V1 route has no matching compatibility rule")
        products.append(UniversalReleaseProduct(
            consumer_id=consumer_id, consumer_class=consumer_class,
            requirement_id=hashlib.sha256(f"{consumer_id}:{key}".encode()).hexdigest(),
            instrument_uid=identity.instrument_uid, instrument_id=identity.instrument_id,
            venue=identity.venue, market=identity.market,
            product_type=identity.product_type.value, native_symbol=instrument.native_symbol,
            feed=feed, interval=requirement.interval,
            source_policy_id=requirement.source_policy_id, provider_plane=plane,
            max_freshness_ms=requirement.max_freshness_ms,
            require_final_bars=requirement.require_final_bars, require_live=True,
            execution_grade=requirement.consumer_grade.value == "EXECUTION",
            route=route.route, fallback=route.fallback, fallback_rule_id=rule,
            blocked_reason=route.reason if route.fallback == "BLOCKED" else None,
        ))
    # The existing portable binding contract also accepts a stable routing
    # generation digest, as used by compile_alpha_deployment_bindings.py.
    return ConsumerRouteBinding(
        consumer_id=consumer_id, consumer_class=consumer_class,
        release_revision=plan.revision, universal_manifest_sha256=plan.digest,
        policy_sha256=policy.policy_sha256,
        capability_matrix_sha256=plan.capability_matrix.sha256,
        capability_matrix_revision=plan.capability_matrix.revision,
        inventory_sha256=plan.crypto_demand.sha256, v1_rollback=policy.v1_rollback,
        independent_v1_venues=("DNSE",),
        products=tuple(sorted(products, key=lambda p: p.requirement_id)),
        consumer_manifest_revision=consumer.manifest.manifest_revision,
    )


def _read_mapping(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"release artifact is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"release artifact is not JSON: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError("release artifact must be a JSON object")
    return value


def _manifest_from_artifact(value: Mapping[str, Any]) -> UniversalReleaseManifest:
    raw_manifest = value.get("release_manifest", value)
    if not isinstance(raw_manifest, Mapping):
        raise ValueError("release artifact has no canonical release_manifest")
    manifest = UniversalReleaseManifest.from_canonical_mapping(raw_manifest)
    summary = value.get("release_summary")
    if summary is not None:
        if (
            not isinstance(summary, Mapping)
            or summary.get("schema") != "qdl.phase115.universal-release-preflight.v1"
            or summary.get("status") != "PREPARED"
            or summary.get("manifest_sha256") != manifest.digest
        ):
            raise ValueError("release preflight summary does not bind the canonical manifest")
    return manifest


def _consumer_manifest_revision(path: Path, *, consumer_id: str) -> int:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"consumer manifest is missing: {path}") from error
    if not isinstance(value, Mapping):
        raise ValueError("consumer manifest must be a mapping")
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ValueError("consumer manifest metadata is missing")
    if str(metadata.get("id", "")).strip() != consumer_id:
        raise ValueError("consumer manifest id differs from requested consumer")
    revision = metadata.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ValueError("consumer manifest revision must be a positive integer")
    return revision


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--release-artifact", type=Path)
    inputs.add_argument("--stable-release-routing", type=Path)
    parser.add_argument("--manifest-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--consumer-id", required=True)
    parser.add_argument(
        "--consumer-manifest",
        type=Path,
        help=(
            "Optional canonical consumer manifest. When supplied, render a "
            "generation-bound v2 binding that seals metadata.revision."
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--independent-v1-venue",
        action="append",
        default=["DNSE"],
        help="Explicit V1-only venue retained outside this V2 release (repeatable).",
    )
    args = parser.parse_args(argv)
    source = args.release_artifact or args.stable_release_routing
    if source.resolve() == args.output.resolve():
        raise ValueError("release artifact and binding output must be different files")
    if args.stable_release_routing:
        binding = binding_from_stable_release(source, args.manifest_root, str(args.consumer_id))
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(binding.canonical_mapping(), indent=2, sort_keys=True) + "\n")
        print(json.dumps({"product_count": len(binding.products), "binding_sha256": binding.binding_sha256,
                          "consumer_manifest_revision": binding.consumer_manifest_revision,
                          "runtime_mutations": 0, "order_actions": 0}))
        return 0
    manifest = _manifest_from_artifact(_read_mapping(source))
    consumer_id = str(args.consumer_id)
    consumer_manifest_revision = (
        _consumer_manifest_revision(args.consumer_manifest, consumer_id=consumer_id)
        if args.consumer_manifest is not None
        else None
    )
    binding = ConsumerRouteBinding.from_manifest(
        manifest,
        consumer_id=consumer_id,
        independent_v1_venues=tuple(str(item) for item in args.independent_v1_venue),
        consumer_manifest_revision=consumer_manifest_revision,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(binding.canonical_mapping(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": "RENDERED_SOURCE_ONLY",
        "consumer_id": binding.consumer_id,
        "release_revision": binding.release_revision,
        "universal_manifest_sha256": binding.universal_manifest_sha256,
        "binding_sha256": binding.binding_sha256,
        "consumer_manifest_revision": binding.consumer_manifest_revision,
        "product_count": len(binding.products),
        "runtime_mutations": 0,
        "order_actions": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

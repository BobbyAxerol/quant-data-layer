#!/usr/bin/env python3
"""Materialize the approved five-perp Reference/L2 V2 source artifacts.

The command performs exactly three bounded, read-only instrument-metadata
requests when ``--provider-metadata`` is supplied: Binance USD-M, OKX SWAP
and OKX FUTURES.  It never saves those raw replies.  Default mode is a dry
run; ``--apply`` requires an explicit confirmation and atomically replaces
only the four generated source documents.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

import yaml


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.demand import (
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    DemandManifest,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.runtime.reference_l2_materializer import build_reference_l2_materialization
from scripts.phase111_active_demand_inventory import fetch_provider_metadata


CONFIRMATION = "MATERIALIZE_REFERENCE_L2"


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"YAML document must be an object: {path}")
    return payload


def _inventory(demand_path: Path) -> ActiveDemandInventory:
    manifest = DemandManifest.load_many((demand_path,))
    digest = hashlib.sha256(demand_path.read_bytes()).hexdigest()
    return ActiveDemandInventory(
        revision=manifest.revision,
        requirements=manifest.requirements,
        source_documents=(),
        candidates=(),
        exclusions=(),
        input_sha256=digest,
    )


def _yaml_bytes(value: Mapping[str, Any]) -> bytes:
    return yaml.safe_dump(dict(value), sort_keys=False, allow_unicode=False).encode("utf-8")


def _atomic_write_many(values: tuple[tuple[Path, Mapping[str, Any]], ...]) -> None:
    """Stage every source document before replacing any target.

    Each target gets an ``os.replace`` in the same filesystem.  The command
    holds no runtime lock because it is source-only and must be run only from a
    reviewed worktree/CI transaction, never against a live mounted directory.
    """

    staged: list[tuple[Path, Path]] = []
    try:
        for path, value in values:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            temporary.write_bytes(_yaml_bytes(value))
            staged.append((path, temporary))
        for path, temporary in staged:
            os.replace(temporary, path)
    finally:
        for _path, temporary in staged:
            temporary.unlink(missing_ok=True)


def run(
    *,
    demand: Path,
    source_registry: Path,
    catalog: Path,
    acquisition: Path,
    promotion_scope: Path,
    consumer_manifest: Path,
    timeout_seconds: float,
    attempts: int,
    provider_payloads: Mapping[tuple[str, str], object] | None = None,
):
    inventory = _inventory(demand)
    payloads = dict(provider_payloads) if provider_payloads is not None else fetch_provider_metadata(
        inventory,
        timeout_seconds=timeout_seconds,
        attempts=attempts,
    )
    admission = admit_provider_metadata(inventory, payloads)
    registry = ActiveDemandSourceRegistry.load(source_registry)
    convergence = converge_active_demand(inventory, admission, registry.admission_policy)
    existing_manifest = _load_yaml(consumer_manifest) if consumer_manifest.exists() else None
    return build_reference_l2_materialization(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        current_catalog_document=_load_yaml(catalog),
        current_acquisition_document=_load_yaml(acquisition),
        current_promotion_scope_document=_load_yaml(promotion_scope),
        current_consumer_manifest_document=existing_manifest,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", type=Path, default=ROOT / "config/v2/stable-reference-l2-demand.yaml")
    parser.add_argument("--source-registry", type=Path, default=ROOT / "config/v2/active-demand-source-registry.yaml")
    parser.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    parser.add_argument("--acquisition", type=Path, default=ROOT / "config/v2/stable-acquisition-bindings.yaml")
    parser.add_argument("--promotion-scope", type=Path, default=ROOT / "config/v2/stable-authority-promotion-scope.yaml")
    parser.add_argument("--consumer-manifest", type=Path, default=ROOT / "consumers/stable/reference-l2-stable.yaml")
    parser.add_argument("--provider-metadata", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if not 1.0 <= args.timeout_seconds <= 30.0:
        raise SystemExit("--timeout-seconds must be between 1 and 30")
    if not 1 <= args.attempts <= 5:
        raise SystemExit("--attempts must be between 1 and 5")
    if not args.provider_metadata:
        raise SystemExit("--provider-metadata is required; fixture data cannot materialize source artifacts")
    if args.apply and args.confirm != CONFIRMATION:
        raise SystemExit(f"--apply requires --confirm {CONFIRMATION}")

    materialized = run(
        demand=args.demand,
        source_registry=args.source_registry,
        catalog=args.catalog,
        acquisition=args.acquisition,
        promotion_scope=args.promotion_scope,
        consumer_manifest=args.consumer_manifest,
        timeout_seconds=args.timeout_seconds,
        attempts=args.attempts,
    )
    if args.apply:
        _atomic_write_many((
            (args.catalog, materialized.source_catalog),
            (args.acquisition, materialized.acquisition_plan),
            (args.promotion_scope, materialized.promotion_scope),
            (args.consumer_manifest, materialized.consumer_manifest),
        ))
    print(json.dumps({
        **materialized.summary,
        "status": "APPLIED" if args.apply else "DRY_RUN",
        "provider_requests": 3,
        "raw_provider_payload_persisted": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

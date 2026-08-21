#!/usr/bin/env python3
"""Refresh the runtime configs of an existing stable bundle in place.

``phaseb_prepare_stable_candidate.py`` mints a *new* bundle: it refuses a
non-empty output directory and generates a fresh cursor signing key, ingest
secret and two database passwords. Running it against a bundle that is already
serving traffic would invalidate every signed consumer cursor and break the
already-initialised authority database.

This tool is the safe counterpart for a catalog or acquisition change. It
regenerates only ``<bundle>/runtime/*.json`` and never reads or writes
``stable.env`` or ``identities/``, so cursor keys, workload identities and
database credentials are preserved exactly.

It is a dry run unless ``--apply`` is passed. Applying does not disturb running
roles: every config is bind mounted as a file, so a container keeps the inode it
started with until it is explicitly recreated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_production_core_bundle,
    write_stable_runtime_bundle,
)

PRESERVED = ("stable.env", "identities")


def _digests(directory: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.iterdir())
        if path.is_file()
    }


def _classify(before: dict[str, str], after: dict[str, str]) -> dict[str, list[str]]:
    return {
        "added": sorted(set(after) - set(before)),
        "removed": sorted(set(before) - set(after)),
        "changed": sorted(
            name for name in set(before) & set(after) if before[name] != after[name]
        ),
        "unchanged": sorted(
            name for name in set(before) & set(after) if before[name] == after[name]
        ),
    }


def refresh(
    *,
    bundle_dir: Path,
    rust_image_id: str,
    source_catalog: Path,
    acquisition_plan: Path,
    promotion_scope_path: Path,
    partition_plan_epoch: int,
    apply: bool,
    clock=time.time_ns,
) -> dict[str, object]:
    bundle_dir = bundle_dir.resolve()
    runtime_dir = bundle_dir / "runtime"
    if not (bundle_dir / "stable.env").is_file() or not runtime_dir.is_dir():
        raise FileNotFoundError(
            "refusing to refresh: expected an existing bundle with stable.env and "
            f"runtime/ at {bundle_dir}"
        )
    if not rust_image_id.startswith("sha256:") or len(rust_image_id) != 71:
        raise ValueError("rust image ID must be an immutable SHA-256 reference")

    catalog = StableSourceCatalog.load(source_catalog)
    acquisition = StableAcquisitionPlan.load(acquisition_plan, catalog=catalog)
    promotion_scope = AuthorityPromotionScope.load(
        promotion_scope_path, catalog=catalog
    )
    authority = stable_authority_record(
        rust_image_digest=rust_image_id,
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=acquisition_plan.read_bytes(),
        effective_at_ns=clock(),
    )

    # A dry run must not write inside a bundle that is serving traffic, so it
    # stages elsewhere. An apply stages next to the target because the swap
    # relies on an atomic rename within one filesystem.
    if apply:
        staging = bundle_dir / f".runtime-staging-{clock()}"
        if staging.exists():
            raise FileExistsError(f"staging directory already exists: {staging}")
        cleanup_root = staging
    else:
        cleanup_root = Path(tempfile.mkdtemp(prefix="qdl-runtime-refresh-"))
        staging = cleanup_root / "runtime"
    before = _digests(runtime_dir)
    try:
        write_stable_runtime_bundle(
            staging, catalog=catalog, acquisition=acquisition, authority=authority
        )
        write_production_core_bundle(
            staging,
            catalog=catalog,
            acquisition=acquisition,
            promotion_scope=promotion_scope,
            raw_authority=authority,
            partition_plan_epoch=partition_plan_epoch,
        )
        after = _digests(staging)
        backup_dir = None
        if apply:
            backup_dir = bundle_dir / f"runtime.backup-{clock()}"
            runtime_dir.rename(backup_dir)
            staging.rename(runtime_dir)
    finally:
        if cleanup_root.exists():
            shutil.rmtree(cleanup_root)

    for name in PRESERVED:
        if not (bundle_dir / name).exists():
            raise RuntimeError(f"refresh removed a preserved artifact: {name}")

    return {
        "schema": "qdl.v2.stable-runtime-refresh-result.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "bundle_dir": str(bundle_dir),
        "catalog_revision": catalog.catalog_revision,
        "acquisition_revision": acquisition.revision,
        "promotion_scope_revision": promotion_scope.revision,
        "promotion_binding_count": len(promotion_scope.binding_ids),
        "partition_plan_epoch": partition_plan_epoch,
        "preserved": list(PRESERVED),
        "backup_dir": str(backup_dir) if backup_dir else None,
        "files": _classify(before, after),
        "digests": after,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--rust-image-id", required=True)
    parser.add_argument(
        "--source-catalog", type=Path,
        default=ROOT / "config/v2/stable-source-bindings.yaml",
    )
    parser.add_argument(
        "--acquisition-plan", type=Path,
        default=ROOT / "config/v2/stable-acquisition-bindings.yaml",
    )
    parser.add_argument(
        "--promotion-scope", type=Path,
        default=ROOT / "config/v2/stable-authority-promotion-scope.yaml",
    )
    parser.add_argument("--partition-plan-epoch", type=int, default=1)
    parser.add_argument(
        "--apply", action="store_true",
        help="write the refreshed configs; omit to report the diff only",
    )
    args = parser.parse_args()
    result = refresh(
        bundle_dir=args.bundle_dir,
        rust_image_id=args.rust_image_id,
        source_catalog=args.source_catalog,
        acquisition_plan=args.acquisition_plan,
        promotion_scope_path=args.promotion_scope,
        partition_plan_epoch=args.partition_plan_epoch,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

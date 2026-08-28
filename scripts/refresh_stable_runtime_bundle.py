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
import os
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
    validate_shared_authority_record,
    write_production_core_bundle,
    write_stable_runtime_bundle,
)

PRESERVED = ("stable.env", "identities")

# Edge roles persist a checkpoint that pins the catalog and acquisition
# revisions they were started with, and refuse to restore state when either
# moves. A refresh that bumps a revision therefore strands every such role
# until its checkpoint is reconciled, which surfaces as the role exiting on
# startup rather than as a failure of this tool.
CHECKPOINT_GLOB = "*-edge.json"
CHECKPOINT_PINNED_FIELDS = ("catalog_revision", "acquisition_revision")


def _checkpoint_reports(
    state_dir: Path | None, *, catalog_revision: int, acquisition_revision: int
) -> list[dict[str, object]]:
    """Report edge checkpoints that this refresh would strand."""
    if state_dir is None:
        return []
    expected = {
        "catalog_revision": catalog_revision,
        "acquisition_revision": acquisition_revision,
    }
    if not state_dir.is_dir():
        return [{
            "path": str(state_dir),
            "compatible": False,
            "reason": "state directory does not exist or is not a directory",
        }]
    try:
        candidates = sorted(state_dir.glob(CHECKPOINT_GLOB))
    except OSError as error:
        return [{
            "path": str(state_dir),
            "compatible": False,
            "reason": f"state directory is unreadable: {error}",
        }]
    if not candidates:
        # An empty result is ambiguous: it can mean no role keeps state, or
        # that the directory is unreadable to this process. Say which.
        readable = os.access(state_dir, os.R_OK | os.X_OK)
        return [{
            "path": str(state_dir),
            "compatible": readable,
            "reason": (
                "no edge checkpoint found"
                if readable
                else "state directory is not readable by this process, so "
                     "checkpoints could not be inspected"
            ),
        }]
    reports: list[dict[str, object]] = []
    for path in candidates:
        entry: dict[str, object] = {"path": str(path), "compatible": False}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            entry["reason"] = f"unreadable: {error}"
            reports.append(entry)
            continue
        if not isinstance(payload, dict):
            entry["reason"] = "checkpoint is not an object"
            reports.append(entry)
            continue
        drift = {
            field: {"checkpoint": payload.get(field), "refreshed": expected[field]}
            for field in CHECKPOINT_PINNED_FIELDS
            if payload.get(field) != expected[field]
        }
        binding_ids = payload.get("binding_ids")
        entry["binding_count"] = len(binding_ids) if isinstance(binding_ids, list) else None
        if drift:
            entry["reason"] = "pinned revision differs from the refreshed bundle"
            entry["drift"] = drift
        else:
            entry["compatible"] = True
        reports.append(entry)
    return reports


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


def _load_preserved_authority(runtime_dir: Path) -> tuple[dict[str, object], bytes]:
    """Load the current authority exactly when a config-only refresh is requested."""
    path = runtime_dir / "authority.json"
    try:
        encoded = path.read_bytes()
        decoded = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("preserved authority is unreadable") from error
    if not isinstance(decoded, dict):
        raise ValueError("preserved authority is not an object")
    try:
        validate_shared_authority_record(decoded)
    except ValueError as error:
        raise ValueError("preserved authority is invalid") from error
    return dict(decoded), encoded


def refresh(
    *,
    bundle_dir: Path,
    rust_image_id: str,
    source_catalog: Path,
    acquisition_plan: Path,
    promotion_scope_path: Path,
    partition_plan_epoch: int,
    apply: bool,
    state_dir: Path | None = None,
    preserve_authority: bool = False,
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
    authority_bytes: bytes | None = None
    if preserve_authority:
        authority, authority_bytes = _load_preserved_authority(runtime_dir)
    else:
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
            staging,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
            promotion_scope=promotion_scope,
        )
        write_production_core_bundle(
            staging,
            catalog=catalog,
            acquisition=acquisition,
            promotion_scope=promotion_scope,
            raw_authority=authority,
            partition_plan_epoch=partition_plan_epoch,
        )
        if authority_bytes is not None:
            authority_path = staging / "authority.json"
            authority_path.write_bytes(authority_bytes)
            if authority_path.read_bytes() != authority_bytes:
                raise RuntimeError("preserved authority bytes changed during refresh")
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

    checkpoints = _checkpoint_reports(
        state_dir,
        catalog_revision=catalog.catalog_revision,
        acquisition_revision=acquisition.revision,
    )
    stranded = [item for item in checkpoints if not item["compatible"]]
    return {
        "schema": "qdl.v2.stable-runtime-refresh-result.v1",
        "checkpoints": checkpoints,
        "stranded_checkpoints": len(stranded),
        "status": "APPLIED" if apply else "DRY_RUN",
        "bundle_dir": str(bundle_dir),
        "catalog_revision": catalog.catalog_revision,
        "acquisition_revision": acquisition.revision,
        "promotion_scope_revision": promotion_scope.revision,
        "promotion_binding_count": len(promotion_scope.binding_ids),
        "partition_plan_epoch": partition_plan_epoch,
        "requested_rust_image_id": rust_image_id,
        "authority_bytes_preserved": authority_bytes is not None,
        "authority_sha256": hashlib.sha256(
            authority_bytes if authority_bytes is not None else json.dumps(
                authority, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest(),
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
        "--state-dir", type=Path,
        help="mounted runtime state directory holding edge checkpoints; when "
             "given, checkpoints stranded by this refresh are reported",
    )
    parser.add_argument(
        "--preserve-authority",
        action="store_true",
        help="retain the validated existing authority.json bytes during a config-only refresh",
    )
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
        state_dir=args.state_dir,
        preserve_authority=args.preserve_authority,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["stranded_checkpoints"]:
        print(
            f"WARNING: {result['stranded_checkpoints']} edge checkpoint(s) pin an "
            "older revision and will refuse to restore state. Move each stranded "
            "checkpoint aside before restarting its role; the edge then "
            "bootstraps a fresh history instead of exiting on startup.",
            file=sys.stderr,
        )
    if args.state_dir is None:
        print(
            "NOTE: --state-dir was not given, so edge checkpoints were not "
            "checked for revision drift.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

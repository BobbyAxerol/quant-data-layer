#!/usr/bin/env python3
"""Compile KN-5 producer maps offline; preserve writer/partition fencing revision.

Run in the packaged candidate, with the previous authority mounted read-only.
No broker, cache or source configuration is changed by this command.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record, write_stable_runtime_bundle


def compile_runtime(old: Path, out: Path, rust_image: str):
    if out.exists():
        raise ValueError("refusing to overwrite runtime directory")
    catalog_path = Path("/app/config/v2/stable-source-bindings.yaml")
    acquisition_path = Path("/app/config/v2/stable-acquisition-bindings.yaml")
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    previous = json.loads((old / "authority.json").read_text())
    assert previous["mode"] == "RUST_PRIMARY"
    # Same existing authority, group and six-partition fence; only artifact
    # provenance changes. This is not a new writer or a new authority grant.
    authority = stable_authority_record(
        rust_image_digest=rust_image, capability_manifest=Path("/app/config/v2/stable-capabilities.yaml"),
        contract=Path("/app/contracts/proto/qdl/marketdata/v2/market_data.proto"),
        partition_plan=acquisition_path.read_bytes(), effective_at_ns=previous["effective_at_ns"],
        mode=previous["mode"], revision=previous["revision"], slice_id=previous["slice_id"],
        approved_by=previous["approved_by"],
    )
    hashes = write_stable_runtime_bundle(out, catalog=catalog, acquisition=acquisition, authority=authority)
    for source in (catalog_path, acquisition_path):
        shutil.copyfile(source, out / source.name)
    policy = json.loads(Path("/app/config/v2/provider-admission-policy-kn-bar-edge-v1.json").read_text())
    reference = json.loads(Path("/app/config/v2/provider-admission-policy-kn-reference-v1.json").read_text())
    policy["lanes"].extend(reference["lanes"])
    policy["revision"] = 3
    (out / "admission-policy.json").write_text(json.dumps(policy, sort_keys=True) + "\n")
    summary = {"catalog_revision": catalog.catalog_revision, "acquisition_revision": acquisition.revision,
        "bindings": len(catalog.bindings), "core_bindings": len(json.loads((out / "core.json").read_text())["core"]["bindings"]),
        "authority_revision_unchanged": previous["revision"] == authority["revision"],
        "raw_topic_unchanged": acquisition.raw_topic, "canonical_topic": acquisition.canonical_topic,
        "rust_image": rust_image, "files": hashes,
        "admission_policy_sha256": hashlib.sha256((out / "admission-policy.json").read_bytes()).hexdigest()}
    (out / "compile-receipt.json").write_text(json.dumps(summary, indent=2))
    for path in out.iterdir():
        if path.is_file():
            path.chmod(0o644)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rust-image", required=True)
    args = parser.parse_args()
    print(json.dumps(compile_runtime(args.old, args.out, args.rust_image)))

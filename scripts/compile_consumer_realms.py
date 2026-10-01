#!/usr/bin/env python3
"""Compile explicit sandbox/live read manifests from approved stable scopes.

No credentials, activation or runtime mutation. The source paper manifests stay
unchanged; output belongs in a versioned runtime packet, not the source tree.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader


def compile_realm(payload: dict, realm: str) -> dict:
    source = ConsumerManifestLoader.from_mapping(payload)
    if source.environment != "paper" or realm not in {"sandbox", "live"}:
        raise ValueError("explicit paper -> sandbox/live public-market mapping required")
    approved = {
        "trading-system.paper.stable": "PAPER_ONLY",
        "alpha.binance.paper.stable": "FORBIDDEN",
        "alpha.okx.paper.stable": "FORBIDDEN",
    }
    if approved.get(source.consumer_id) != source.execution_dependency:
        raise ValueError("only the three owner-approved stable consumer scopes may be mapped")
    if source.consumer_id.count(".paper.") != 1 or not source.subject.startswith("spiffe://qdl/paper/"):
        raise ValueError("stable consumer identity convention mismatch")
    result = copy.deepcopy(payload)
    metadata = result["metadata"]
    metadata.update(id=source.consumer_id.replace(".paper.", "." + realm + "."),
                    subject=source.subject.replace("spiffe://qdl/paper/", "spiffe://qdl/" + realm + "/", 1),
                    environment=realm, revision=1)
    # Alpha research permission is NOT promoted into direct Risk authority.
    result["spec"]["execution_dependency"] = (
        "ALLOWED" if source.execution_dependency == "PAPER_ONLY" else "FORBIDDEN"
    )
    # VN stays on its separately deferred V1 path. This packet authorizes only
    # the existing crypto policies; future policies require explicit inclusion.
    crypto_policies = {"crypto_primary_v2", "crypto_liquid_v2"}
    result["spec"]["requirements"] = [
        row for row in result["spec"]["requirements"]
        if row["source_policy_id"] in crypto_policies
    ]
    target = ConsumerManifestLoader.from_mapping(result)
    expected = tuple(row for row in source.requirements if row.source_policy_id in crypto_policies)
    if target.requirements != expected or target.quotas != source.quotas:
        raise AssertionError("realm mapping must preserve approved crypto scope and quotas")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        raise ValueError("new output directory required; no overwrite of sealed manifests")
    outputs = []
    for path in args.source:
        payload = yaml.safe_load(path.read_text())
        for realm in ("sandbox", "live"):
            mapped = compile_realm(payload, realm)
            parsed = ConsumerManifestLoader.from_mapping(mapped)
            outputs.append((parsed.consumer_id + ".yaml", mapped, parsed))
    if len({name for name, _, _ in outputs}) != len(outputs):
        raise ValueError("duplicate target consumer")
    args.output_dir.mkdir(parents=True, mode=0o750)
    receipt = []
    for name, mapped, parsed in outputs:
        (args.output_dir / name).write_text(yaml.safe_dump(mapped, sort_keys=False))
        receipt.append(dict(consumer_id=parsed.consumer_id, environment=parsed.environment,
            subject=parsed.subject, manifest_revision=parsed.manifest_revision,
            manifest_sha256=parsed.manifest_sha256, requirements=len(parsed.requirements)))
    print(json.dumps({"status": "COMPILED_NOT_ACTIVATED", "market_storage_realm": "paper",
                      "order_permissions": False, "manifests": receipt}, indent=2))


if __name__ == "__main__":
    main()

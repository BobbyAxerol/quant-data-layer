#!/usr/bin/env python3
"""Create a typed, bounded admission for an R1 Rust canary.

The admission binds a newly built immutable image to its source/release artifact
and to fresh, read-only real-provider reference parity.  It intentionally does
*not* claim that the candidate image has emitted live output yet.  That proof
is collected only after the authority reaches ``RUST_CANARY`` and is mandatory
for R2 primary handoff.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
from typing import Any
import uuid


SCHEMA = "qdl.r1.pre-canary-admission.v1"
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{7,64}\Z")


def _encoded(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_encoded(value)).hexdigest()



def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _inspect_image(image_id: str) -> Mapping[str, Any]:
    completed = subprocess.run(
        ["docker", "image", "inspect", image_id],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], Mapping):
        raise ValueError("Docker image inspect must return one object")
    return value[0]


def _require_digest(value: object, field: str) -> str:
    text = str(value)
    if not _DIGEST.fullmatch(text):
        raise ValueError(f"{field} must be an immutable SHA-256 image digest")
    return text


def _require_sha(value: object, field: str) -> str:
    text = str(value)
    if not _SHA256.fullmatch(text):
        raise ValueError(f"{field} must be a SHA-256 digest")
    return text


def _candidate_artifact(value: Mapping[str, Any]) -> dict[str, str]:
    required = {
        "schema", "status", "source_commit", "rust_image_digest",
        "rollback_rust_image_digest", "promotion_scope_digest", "contract_sha256",
        "partition_plan_sha256", "sbom_sha256", "rollback_manifest_sha256",
        "public_write_allowed", "legacy_write_allowed",
    }
    if set(value) != required or value.get("schema") != "qdl.r1.release-artifact-manifest.v1":
        raise ValueError("R1 release artifact schema/fields are invalid")
    if value.get("status") != "PRE_CANARY" or value.get("public_write_allowed") is not False or value.get("legacy_write_allowed") is not False:
        raise ValueError("R1 release artifact is not fenced for pre-canary")
    source_commit = str(value["source_commit"])
    if not _COMMIT.fullmatch(source_commit):
        raise ValueError("R1 release artifact source commit is invalid")
    result = {
        "source_commit": source_commit,
        "rust_image_digest": _require_digest(value["rust_image_digest"], "candidate image"),
        "rollback_rust_image_digest": _require_digest(value["rollback_rust_image_digest"], "rollback image"),
        "promotion_scope_digest": _require_sha(value["promotion_scope_digest"], "promotion scope"),
        "contract_sha256": _require_sha(value["contract_sha256"], "contract"),
        "partition_plan_sha256": _require_sha(value["partition_plan_sha256"], "partition plan"),
        "sbom_sha256": _require_sha(value["sbom_sha256"], "SBOM"),
        "rollback_manifest_sha256": _require_sha(value["rollback_manifest_sha256"], "rollback manifest"),
    }
    if result["rust_image_digest"] == result["rollback_rust_image_digest"]:
        raise ValueError("R1 candidate and rollback images must differ")
    return result


def _reference_parity(value: Mapping[str, Any], *, artifact: Mapping[str, str], now_ns: int) -> dict[str, Any]:
    if value.get("schema") != "qdl.c40.live-core-parity.v1" or value.get("status") != "PASS":
        raise ValueError("R1 reference parity must be a passing C40 live parity report")
    if value.get("provider_provenance") != "REAL" or int(value.get("production_mutations", -1)) != 0:
        raise ValueError("R1 reference parity must be read-only real-provider evidence")
    captured = int(value.get("captured_at_ns", 0))
    if captured <= 0 or captured > now_ns or now_ns - captured > 1_800_000_000_000:
        raise ValueError("R1 reference parity is not fresh enough for canary admission")
    observed = _require_digest(value.get("candidate_image_digest"), "reference runtime image")
    if observed == artifact["rust_image_digest"]:
        raise ValueError("R1 pre-canary reference must not claim candidate runtime parity")
    if value.get("scope_digest") != artifact["promotion_scope_digest"]:
        raise ValueError("R1 reference parity scope differs from candidate release")
    if int(value.get("slice_count", 0)) != 12 or int(value.get("sample_count", 0)) < 96:
        raise ValueError("R1 reference parity must cover twelve slices with at least eight samples each")
    if int(value.get("semantic_mismatches", -1)) != 0 or int(value.get("invalid_provenance", -1)) != 0:
        raise ValueError("R1 reference parity contains semantic/provenance failures")
    slices = value.get("slices")
    if not isinstance(slices, list) or len(slices) != 12 or not all(isinstance(item, Mapping) for item in slices):
        raise ValueError("R1 reference parity slice evidence is incomplete")
    if any(int(item.get("samples", 0)) < 8 or int(item.get("semantic_mismatches", -1)) != 0 or int(item.get("invalid_provenance", -1)) != 0 for item in slices):
        raise ValueError("R1 reference parity slice evidence is not clean")
    source_commit = str(value.get("source_commit", ""))
    if not _COMMIT.fullmatch(source_commit):
        raise ValueError("R1 reference parity source commit is invalid")
    return {
        "reference_runtime_image_digest": observed,
        "reference_source_commit": source_commit,
        "reference_parity_sha256": _digest(value),
        "reference_captured_at_ns": captured,
        "sample_count": int(value["sample_count"]),
    }


def _candidate_image(value: Mapping[str, Any], *, artifact: Mapping[str, str]) -> dict[str, str]:
    image_id = _require_digest(value.get("Id"), "Docker image ID")
    if image_id != artifact["rust_image_digest"]:
        raise ValueError("Docker image digest differs from the R1 release artifact")
    config = value.get("Config")
    if not isinstance(config, Mapping):
        raise ValueError("Docker image config is unavailable")
    labels = config.get("Labels") or {}
    if not isinstance(labels, Mapping):
        raise ValueError("Docker image labels are unavailable")
    if str(labels.get("org.opencontainers.image.revision", "")) != artifact["source_commit"]:
        raise ValueError("Docker image revision differs from the R1 release artifact")
    if str(labels.get("io.qdl.authority.default", "")) != "RUST_SHADOW":
        raise ValueError("R1 image default authority must remain RUST_SHADOW")
    if str(config.get("User", "")) != "10001:10001":
        raise ValueError("R1 image must run non-root")
    public = {
        "id": image_id,
        "revision": str(labels["org.opencontainers.image.revision"]),
        "authority_default": str(labels["io.qdl.authority.default"]),
        "user": str(config["User"]),
    }
    return {
        "candidate_image_digest": image_id,
        "candidate_image_inspect_sha256": _digest(public),
    }


def prepare_admission(
    *,
    release_artifact: Mapping[str, Any],
    reference_parity: Mapping[str, Any],
    image_inspect: Mapping[str, Any],
    now_ns: int | None = None,
    ttl_seconds: int = 1_800,
) -> dict[str, Any]:
    now = time.time_ns() if now_ns is None else int(now_ns)
    if not 300 <= ttl_seconds <= 7_200:
        raise ValueError("R1 pre-canary admission TTL must be 300..7200 seconds")
    artifact = _candidate_artifact(release_artifact)
    reference = _reference_parity(reference_parity, artifact=artifact, now_ns=now)
    candidate = _candidate_image(image_inspect, artifact=artifact)
    return {
        "schema": SCHEMA,
        "status": "PASS",
        "issued_at_ns": now,
        "expires_at_ns": now + ttl_seconds * 1_000_000_000,
        "provider_provenance": "REAL",
        "production_mutations": 0,
        "execution_state_changed": False,
        "semantic_mismatches": 0,
        "open_gaps": 0,
        "duplicate_external_effects": 0,
        "consumer_errors": 0,
        "candidate_runtime_parity_status": "PENDING_R1_CANARY",
        "candidate_source_commit": artifact["source_commit"],
        "candidate_image_digest": candidate["candidate_image_digest"],
        "candidate_image_inspect_sha256": candidate["candidate_image_inspect_sha256"],
        "rollback_rust_image_digest": artifact["rollback_rust_image_digest"],
        "promotion_scope_digest": artifact["promotion_scope_digest"],
        "contract_sha256": artifact["contract_sha256"],
        "partition_plan_sha256": artifact["partition_plan_sha256"],
        "release_artifact_sha256": _digest(release_artifact),
        "sbom_sha256": artifact["sbom_sha256"],
        "rollback_manifest_sha256": artifact["rollback_manifest_sha256"],
        **reference,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-artifact", type=Path, required=True)
    parser.add_argument("--reference-parity", type=Path, required=True)
    parser.add_argument("--candidate-image-id", required=True)
    parser.add_argument("--ttl-seconds", type=int, default=1_800)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError("R1 pre-canary admission output already exists")
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    admission = prepare_admission(
        release_artifact=_load(args.release_artifact),
        reference_parity=_load(args.reference_parity),
        image_inspect=_inspect_image(args.candidate_image_id),
        ttl_seconds=args.ttl_seconds,
    )
    staging = output.parent / f".{output.name}.staging-{uuid.uuid4().hex}"
    staging.write_text(json.dumps(admission, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    staging.chmod(0o640)
    staging.replace(output)
    print(json.dumps({
        "schema": admission["schema"],
        "status": admission["status"],
        "candidate_runtime_parity_status": admission["candidate_runtime_parity_status"],
        "candidate_image_digest": admission["candidate_image_digest"],
        "reference_runtime_image_digest": admission["reference_runtime_image_digest"],
        "expires_at_ns": admission["expires_at_ns"],
        "production_mutations": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

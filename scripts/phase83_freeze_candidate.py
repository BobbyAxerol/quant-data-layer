#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import tempfile
import urllib.request

from qdl.certification import verify_release_bundle, write_release_bundle


ROOT = pathlib.Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "upgrade/evidence"
RELEASE_DIR = EVIDENCE / "phase8-release"


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digest_files(paths: list[pathlib.Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = str(path.relative_to(ROOT)).encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def docker_json(*arguments: str) -> dict:
    result = subprocess.run(
        ["docker", *arguments], text=True, capture_output=True, check=True, timeout=30
    )
    return json.loads(result.stdout)


def inspect_payload(path: pathlib.Path | None, *docker_arguments: str) -> dict:
    payload = json.loads(path.read_text()) if path is not None else docker_json(
        *docker_arguments
    )
    if not isinstance(payload, list) or len(payload) != 1:
        raise RuntimeError("Docker inspect evidence must contain exactly one object")
    return payload[0]


def write_json(path: pathlib.Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--image-inspect-json", type=pathlib.Path)
    parser.add_argument("--runtime-inspect-json", type=pathlib.Path)
    parser.add_argument("--v1-health-status", type=int)
    args = parser.parse_args()
    RELEASE_DIR.mkdir(parents=True, exist_ok=True)
    for path in RELEASE_DIR.iterdir():
        if path.is_file():
            path.unlink()
    image = inspect_payload(args.image_inspect_json, "image", "inspect", args.image)
    if f"sha256:{image['Id'].removeprefix('sha256:')}" not in args.image_ref:
        raise RuntimeError("image ref does not match inspected immutable image ID")
    labels = image.get("Config", {}).get("Labels", {}) or {}
    if labels.get("org.opencontainers.image.revision") != args.git_sha:
        raise RuntimeError("candidate image revision label differs from Git SHA")
    if labels.get("io.qdl.authority.default") != "RUST_SHADOW":
        raise RuntimeError("candidate image does not default to RUST_SHADOW")
    capabilities = sorted((ROOT / "config/phase8/capabilities").glob("*.yaml"))
    contracts = sorted((ROOT / "contracts/proto").rglob("*.proto"))
    plan = ROOT / "config/phase8/candidate-partition-plan.json"
    authority = EVIDENCE / "phase8-authority-rehearsal.json"
    real_capture = EVIDENCE / "captures/phase8-real-provider-frames.json.gz"
    candidate_path = RELEASE_DIR / "candidate-slice.json"
    candidate = {
        "schema": "qdl.phase8.candidate-slice.v1",
        "release": args.release,
        "git_sha": args.git_sha,
        "image_ref": args.image_ref,
        "image_created": image.get("Created"),
        "image_architecture": image.get("Architecture"),
        "image_os": image.get("Os"),
        "image_labels": labels,
        "authority": "RUST_SHADOW",
        "slice_id": "BINANCE:USDM:TRADE:BTCUSDT",
        "capability_manifest_digest": digest_files(capabilities),
        "contract_digest": digest_files(contracts),
        "partition_plan_digest": sha256_file(plan),
        "authority_rehearsal_digest": sha256_file(authority),
        "real_capture_bundle_digest": sha256_file(real_capture),
        "public_write_allowed": False,
        "legacy_write_allowed": False,
        "phase9_authority_approval_required": True,
    }
    write_json(candidate_path, candidate)

    container = inspect_payload(
        args.runtime_inspect_json, "inspect", "data_layer_service"
    )
    compose_paths = [ROOT / "docker-compose.yml"]
    rollback_path = RELEASE_DIR / "python-v1-rollback.json"
    rollback = {
        "schema": "qdl.phase8.python-rollback.v1",
        "slice_id": candidate["slice_id"],
        "restore_authority": "PYTHON_V1_PRIMARY",
        "container_name": "data_layer_service",
        "container_image_id": container["Image"],
        "container_image_reference": container["Config"]["Image"],
        "container_started_at": container["State"]["StartedAt"],
        "compose_artifacts": [
            {"path": str(path.relative_to(ROOT)), "sha256": sha256_file(path)}
            for path in compose_paths
        ],
        "v1_health_url": "http://127.0.0.1:8100/v1/health",
        "expected_v1_health": 200,
        "rollback_order": [
            "persist RUST_SHADOW authority at a higher revision",
            "verify Rust public and legacy writes are fenced",
            "verify Python V1 health and demanded feed freshness",
            "retain durable shadow offsets for incident replay",
        ],
        "destructive_actions": [],
    }
    write_json(rollback_path, rollback)
    health_status = args.v1_health_status
    if health_status is None:
        with urllib.request.urlopen(rollback["v1_health_url"], timeout=10) as response:
            health_status = response.status
    if health_status != 200:
        raise RuntimeError("V1 is not healthy while freezing rollback manifest")

    with tempfile.TemporaryDirectory(prefix="qdl-phase83-signing-") as directory:
        private_key = pathlib.Path(directory) / "private.pem"
        public_key = RELEASE_DIR / "attestation-public.pem"
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:3072", "-out", str(private_key)],
            check=True, capture_output=True, timeout=60,
        )
        subprocess.run(
            ["openssl", "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)],
            check=True, capture_output=True, timeout=30,
        )
        write_release_bundle(
            ROOT, RELEASE_DIR, release=args.release, git_sha=args.git_sha,
            image_ref=args.image_ref, authority="SHADOW", signing_key=private_key,
        )
        verify_release_bundle(ROOT, RELEASE_DIR, verification_key=public_key)
        provenance_path = RELEASE_DIR / "artifact-provenance.json"
        provenance = {
            "schema": "qdl.phase8.artifact-provenance.v1",
            "release": args.release,
            "git_sha": args.git_sha,
            "image_ref": args.image_ref,
            "candidate_slice_sha256": sha256_file(candidate_path),
            "python_rollback_sha256": sha256_file(rollback_path),
            "release_manifest_sha256": sha256_file(RELEASE_DIR / "release-manifest.json"),
            "sbom_sha256": sha256_file(RELEASE_DIR / "sbom.spdx.json"),
            "signature_scheme": "RSA-3072-SHA256",
            "private_key_retained": False,
            "registry_signature_admission": False,
            "authority": "RUST_SHADOW",
        }
        write_json(provenance_path, provenance)
        signature = RELEASE_DIR / "artifact-provenance.sig"
        subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", str(private_key), "-out", str(signature), str(provenance_path)],
            check=True, capture_output=True, timeout=30,
        )
        subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(public_key), "-signature", str(signature), str(provenance_path)],
            check=True, capture_output=True, timeout=30,
        )
    if any(path.name == "private.pem" for path in RELEASE_DIR.iterdir()):
        raise RuntimeError("private signing key leaked into release evidence")
    print(json.dumps({
        "status": "PASS", "release": args.release, "image_ref": args.image_ref,
        "candidate_sha256": sha256_file(candidate_path),
        "rollback_sha256": sha256_file(rollback_path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

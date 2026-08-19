#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import shutil
import subprocess
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)


ROOT = Path(__file__).resolve().parents[1]


def image_id(reference: str) -> str:
    completed = subprocess.run(
        ["docker", "image", "inspect", reference, "--format", "{{.Id}}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    value = completed.stdout.strip()
    if not value.startswith("sha256:") or len(value) != 71:
        raise RuntimeError("Docker image did not return an immutable SHA-256 ID")
    return value


def copy_client_identity(source: Path, destination: Path, principal: str) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    for source_name, target_name in (
        ("ca.crt", "ca.crt"),
        (f"{principal}.crt", "client.crt"),
        (f"{principal}.key", "client.key"),
    ):
        origin = source / source_name
        if not origin.is_file():
            raise FileNotFoundError(f"stable TLS source is unavailable: {origin}")
        shutil.copyfile(origin, destination / target_name)
    for item in destination.iterdir():
        item.chmod(0o440)


def prepare_candidate(
    *,
    rust_image: str,
    python_image: str,
    cert_dir: Path,
    output_dir: Path,
) -> dict[str, object]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError("stable candidate output directory must be empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir = output_dir / "runtime"
    identities_dir = output_dir / "identities"
    identities_dir.mkdir()
    rust_digest = image_id(rust_image)
    python_digest = image_id(python_image)
    catalog = StableSourceCatalog.load(
        ROOT / "config/v2/stable-source-bindings.yaml"
    )
    acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    authority = stable_authority_record(
        rust_image_digest=rust_digest,
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=acquisition_path.read_bytes(),
        effective_at_ns=__import__("time").time_ns(),
    )
    bundle_digests = write_stable_runtime_bundle(
        runtime_dir,
        catalog=catalog,
        acquisition=acquisition,
        authority=authority,
    )
    for role, principal in (
        ("producer", "phase8-producer"),
        ("core", "phase8-core"),
        ("projector", "phase8-consumer"),
    ):
        copy_client_identity(cert_dir, identities_dir / role, principal)

    schema_digest = hashlib.sha256(
        (ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto").read_bytes()
    ).hexdigest()
    ingest_secret = secrets.token_urlsafe(48)
    cursor_secret = secrets.token_urlsafe(48)
    jwt_secret = secrets.token_urlsafe(48)
    values = {
        "QDL_STABLE_SCHEMA_DIGEST": schema_digest,
        "QDL_STABLE_INTERNAL_INGEST_SECRET": ingest_secret,
        "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps(
            {"stable-k1": cursor_secret}, separators=(",", ":")
        ),
        "QDL_STABLE_JWT_KEYS_JSON": json.dumps(
            {"stable-jwt-k1": jwt_secret}, separators=(",", ":")
        ),
        "QDL_STABLE_PYTHON_IMAGE": python_digest,
        "QDL_STABLE_RUST_IMAGE": rust_digest,
        "QDL_STABLE_CERT_DIR": str(cert_dir.resolve()),
        "QDL_STABLE_PROJECTOR_CERT_DIR": str((identities_dir / "projector").resolve()),
        "QDL_STABLE_CORE_CERT_DIR": str((identities_dir / "core").resolve()),
        "QDL_STABLE_PRODUCER_CERT_DIR": str((identities_dir / "producer").resolve()),
        "QDL_STABLE_RUNTIME_DIR": str(runtime_dir.resolve()),
    }
    env_path = output_dir / "stable.env"
    env_path.write_text(
        "".join(
            f"{key}='{value}'\n" if value.startswith("{") else f"{key}={value}\n"
            for key, value in values.items()
        ),
        encoding="utf-8",
    )
    env_path.chmod(0o600)
    manifest = {
        "schema": "qdl.v2.stable-candidate-bundle.v1",
        "contract_version": "2.0.0",
        "authority": "RUST_SHADOW",
        "cutover_authorized": False,
        "rust_image_id": rust_digest,
        "python_image_id": python_digest,
        "runtime_digests": bundle_digests,
        "catalog_revision": catalog.catalog_revision,
        "acquisition_revision": acquisition.revision,
        "consumer_count": 5,
        "secret_values_recorded": False,
    }
    manifest_path = output_dir / "candidate-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest_path.chmod(0o640)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-image", required=True)
    parser.add_argument("--python-image", required=True)
    parser.add_argument("--cert-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = prepare_candidate(
        rust_image=args.rust_image,
        python_image=args.python_image,
        cert_dir=args.cert_dir,
        output_dir=args.output_dir,
    )
    print(json.dumps({
        "status": "PASS",
        "contract_version": manifest["contract_version"],
        "authority": manifest["authority"],
        "cutover_authorized": manifest["cutover_authorized"],
        "runtime_files": len(manifest["runtime_digests"]),
        "secret_values_recorded": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

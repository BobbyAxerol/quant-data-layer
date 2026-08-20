#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import shutil
import subprocess
import sys
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


def copy_server_identity(source: Path, destination: Path, principal: str) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    for source_name, target_name in (
        ("ca.crt", "ca.crt"),
        (f"{principal}.crt", "server.crt"),
        (f"{principal}.key", "server.key"),
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
    consumer_network: str,
    rust_image_id: str | None = None,
    python_image_id: str | None = None,
    host_cert_dir: Path | None = None,
    host_output_dir: Path | None = None,
) -> dict[str, object]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError("stable candidate output directory must be empty")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", consumer_network) is None:
        raise ValueError("stable consumer network name is invalid")
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir = output_dir / "runtime"
    identities_dir = output_dir / "identities"
    identities_dir.mkdir()
    rust_digest = rust_image_id or image_id(rust_image)
    python_digest = python_image_id or image_id(python_image)
    for value in (rust_digest, python_digest):
        if not value.startswith("sha256:") or len(value) != 71:
            raise ValueError("stable candidate image ID must be SHA-256")
    catalog = StableSourceCatalog.load(
        ROOT / "config/v2/stable-source-bindings.yaml"
    )
    acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    promotion_scope = AuthorityPromotionScope.load(
        ROOT / "config/v2/stable-authority-promotion-scope.yaml",
        catalog=catalog,
    )
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
    bundle_digests.update(write_production_core_bundle(
        runtime_dir,
        catalog=catalog,
        acquisition=acquisition,
        promotion_scope=promotion_scope,
        raw_authority=authority,
        partition_plan_epoch=1,
    ))
    for role, principal in (
        ("producer", "phase8-producer"),
        ("core", "phase8-core"),
        ("projector", "phase8-consumer"),
        ("authority-dispatcher", "stable-authority-dispatcher"),
        ("trading-system", "stable-trading-system"),
    ):
        copy_client_identity(cert_dir, identities_dir / role, principal)
    copy_server_identity(cert_dir, identities_dir / "query", "stable-query")
    copy_server_identity(cert_dir, identities_dir / "stream", "stable-stream")
    jwt_identity_dir = identities_dir / "trading-system-jwt"
    jwt_identity_dir.mkdir(parents=True, exist_ok=False)
    for source_name, target_name in (
        ("stable-trading-system-jwt.key", "private.key"),
        ("stable-trading-system-jwt.public.pem", "public.pem"),
    ):
        origin = cert_dir / source_name
        if not origin.is_file():
            raise FileNotFoundError(f"stable JWT source is unavailable: {origin}")
        shutil.copyfile(origin, jwt_identity_dir / target_name)
    for item in jwt_identity_dir.iterdir():
        item.chmod(0o440)

    schema_digest = hashlib.sha256(
        (ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto").read_bytes()
    ).hexdigest()
    ingest_secret = secrets.token_urlsafe(48)
    cursor_secret = secrets.token_urlsafe(48)
    control_db_password = secrets.token_urlsafe(32)
    dispatcher_db_password = secrets.token_urlsafe(32)
    jwt_public_key = (jwt_identity_dir / "public.pem").read_text(encoding="utf-8")
    compose_cert_dir = (host_cert_dir or cert_dir).resolve()
    compose_output_dir = (host_output_dir or output_dir).resolve()
    values = {
        "QDL_STABLE_SCHEMA_DIGEST": schema_digest,
        "QDL_STABLE_CONSUMER_NETWORK": consumer_network,
        "QDL_STABLE_INTERNAL_INGEST_SECRET": ingest_secret,
        "QDL_STABLE_CURSOR_KEYS_JSON": json.dumps(
            {"stable-k1": cursor_secret}, separators=(",", ":")
        ),
        "QDL_STABLE_JWT_KEYS_JSON": json.dumps(
            {"stable-trading-system-rs256-v1": jwt_public_key},
            separators=(",", ":"),
        ),
        "QDL_STABLE_PYTHON_IMAGE": python_digest,
        "QDL_STABLE_RUST_IMAGE": rust_digest,
        "QDL_STABLE_CERT_DIR": str(compose_cert_dir),
        "QDL_STABLE_PROJECTOR_CERT_DIR": str(
            compose_output_dir / "identities/projector"
        ),
        "QDL_STABLE_AUTHORITY_CERT_DIR": str(
            compose_output_dir / "identities/authority-dispatcher"
        ),
        "QDL_STABLE_CORE_CERT_DIR": str(compose_output_dir / "identities/core"),
        "QDL_STABLE_PRODUCER_CERT_DIR": str(
            compose_output_dir / "identities/producer"
        ),
        "QDL_STABLE_QUERY_CERT_DIR": str(compose_output_dir / "identities/query"),
        "QDL_STABLE_STREAM_CERT_DIR": str(compose_output_dir / "identities/stream"),
        "QDL_STABLE_TRADING_SYSTEM_CERT_DIR": str(
            compose_output_dir / "identities/trading-system"
        ),
        "QDL_STABLE_TRADING_SYSTEM_JWT_PRIVATE_KEY": str(
            compose_output_dir / "identities/trading-system-jwt/private.key"
        ),
        "QDL_STABLE_CONTROL_DB_PASSWORD": control_db_password,
        "QDL_STABLE_DISPATCHER_DB_PASSWORD": dispatcher_db_password,
        "QDL_STABLE_CONTROL_DB_DSN": (
            "postgresql://qdl_authority_dispatcher:"
            f"{dispatcher_db_password}@stable_authority_db:5432/qdl_authority"
        ),
        "QDL_STABLE_CONTROL_ADMIN_DSN": (
            "postgresql://qdl_authority:"
            f"{control_db_password}@stable_authority_db:5432/qdl_authority"
        ),
        "QDL_STABLE_RUNTIME_DIR": str(compose_output_dir / "runtime"),
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
        "authority_promotion_scope_revision": promotion_scope.revision,
        "authority_promotion_scope_digest": promotion_scope.digest(),
        "authority_promotion_binding_count": len(promotion_scope.binding_ids),
        "consumer_network": consumer_network,
        "consumer_count": 5,
        "workload_mtls": True,
        "workload_identity_count": 4,
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
    parser.add_argument("--consumer-network", required=True)
    parser.add_argument("--rust-image-id")
    parser.add_argument("--python-image-id")
    parser.add_argument("--host-cert-dir", type=Path)
    parser.add_argument("--host-output-dir", type=Path)
    args = parser.parse_args()
    manifest = prepare_candidate(
        rust_image=args.rust_image,
        python_image=args.python_image,
        cert_dir=args.cert_dir,
        output_dir=args.output_dir,
        consumer_network=args.consumer_network,
        rust_image_id=args.rust_image_id,
        python_image_id=args.python_image_id,
        host_cert_dir=args.host_cert_dir,
        host_output_dir=args.host_output_dir,
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

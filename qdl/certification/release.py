from __future__ import annotations

import hashlib
import json
import re
import subprocess
import time
import tomllib
from pathlib import Path
from typing import Any


_SHA = re.compile(r"^[0-9a-f]{7,64}$")
_IMMUTABLE_IMAGE = re.compile(r"^[^\s]+@sha256:[0-9a-f]{64}$")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _lock_packages(path: Path, ecosystem: str) -> list[dict[str, str]]:
    if not path.exists():
        return []
    content = tomllib.loads(path.read_text(encoding="utf-8"))
    packages = []
    for package in content.get("package", []):
        name = str(package.get("name") or "").strip()
        version = str(package.get("version") or "").strip()
        if name and version:
            packages.append({"name": name, "version": version, "ecosystem": ecosystem})
    return packages


def build_spdx(repo: Path, *, release: str) -> dict[str, Any]:
    packages = {
        (item["ecosystem"], item["name"], item["version"]): item
        for item in (
            _lock_packages(repo / "poetry.lock", "pypi")
            + _lock_packages(repo / "Cargo.lock", "cargo")
        )
    }
    namespace_seed = json.dumps(sorted(packages), separators=(",", ":")).encode()
    namespace = hashlib.sha256(namespace_seed).hexdigest()
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"quant-data-layer-{release}",
        "documentNamespace": f"https://bobbyaxerol.github.io/qdl/sbom/{release}/{namespace}",
        "creationInfo": {
            "created": "1970-01-01T00:00:00Z",
            "creators": ["Tool: qdl-release-bundle"],
        },
        "packages": [
            {
                "SPDXID": f"SPDXRef-{item['ecosystem']}-{index}",
                "name": item["name"],
                "versionInfo": item["version"],
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "supplier": "NOASSERTION",
                "externalRefs": [{
                    "referenceCategory": "PACKAGE-MANAGER",
                    "referenceType": "purl",
                    "referenceLocator": (
                        f"pkg:{item['ecosystem']}/{item['name']}@{item['version']}"
                    ),
                }],
            }
            for index, item in enumerate(
                (packages[key] for key in sorted(packages)), start=1
            )
        ],
    }


def _artifact_paths(repo: Path) -> tuple[Path, ...]:
    paths = (
        repo / "contracts/baseline/qdl-v2-phase1.binpb",
        repo / "contracts/v1/openapi.snapshot.json",
        repo / "contracts/v1/public-surface.snapshot.json",
        repo / "contracts/v1/redis-payload-shapes.snapshot.json",
        repo / "contracts/v2/openapi.snapshot.json",
        repo / "pyproject.toml",
        repo / "poetry.lock",
        repo / "Cargo.lock",
        repo / "Dockerfile",
        repo / "Dockerfile.qdl-core",
        repo / "Dockerfile.phase8-rust",
        repo / "config/phase8/broker-topology.yaml",
        repo / "config/phase8/candidate-partition-plan.json",
        repo / "config/phase8/capabilities/binance-usdm-trade.yaml",
        repo / "config/phase8/capabilities/okx-swap-trade.yaml",
        repo / "config/phase8/capabilities/dnse-vn-bar.yaml",
        repo / "config/phase8/capabilities/deribit-option-book-fixture.yaml",
        repo / "contracts/proto/qdl/provider/v1/raw_provider.proto",
        repo / "contracts/proto/qdl/marketdata/v2/market_data.proto",
    )
    missing = [str(path.relative_to(repo)) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"release artifacts are missing: {missing}")
    return paths


def write_release_bundle(
    repo: Path,
    output_dir: Path,
    *,
    release: str,
    git_sha: str,
    image_ref: str,
    authority: str = "SHADOW",
    signing_key: Path | None = None,
) -> dict[str, Any]:
    if not release.strip() or not _SHA.fullmatch(git_sha.lower()):
        raise ValueError("release and a hexadecimal Git SHA are required")
    if not _IMMUTABLE_IMAGE.fullmatch(image_ref):
        raise ValueError("image_ref must use an immutable sha256 digest")
    if authority not in {"SHADOW", "CANARY", "PRIMARY"}:
        raise ValueError("unsupported authority state")
    output_dir.mkdir(parents=True, exist_ok=True)
    sbom_path = output_dir / "sbom.spdx.json"
    sbom_path.write_text(
        json.dumps(build_spdx(repo, release=release), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts = [
        {
            "path": str(path.relative_to(repo)),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        for path in _artifact_paths(repo)
    ]
    artifacts.append({
        "path": sbom_path.name,
        "sha256": sha256_file(sbom_path),
        "size_bytes": sbom_path.stat().st_size,
    })
    manifest = {
        "schema": "qdl.release-manifest.v1",
        "release": release,
        "git_sha": git_sha.lower(),
        "image_ref": image_ref,
        "authority": authority,
        "generated_at_ns": time.time_ns(),
        "artifacts": artifacts,
    }
    manifest_path = output_dir / "release-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if signing_key is not None:
        subprocess.run(
            [
                "openssl", "dgst", "-sha256", "-sign", str(signing_key),
                "-out", str(output_dir / "release-manifest.sig"), str(manifest_path),
            ],
            check=True,
            capture_output=True,
        )
    return manifest


def verify_release_bundle(
    repo: Path,
    output_dir: Path,
    *,
    verification_key: Path | None = None,
) -> dict[str, Any]:
    manifest_path = output_dir / "release-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != "qdl.release-manifest.v1":
        raise ValueError("unsupported release manifest schema")
    for artifact in manifest["artifacts"]:
        path = (
            output_dir / artifact["path"]
            if artifact["path"] == "sbom.spdx.json"
            else repo / artifact["path"]
        )
        if not path.is_file() or sha256_file(path) != artifact["sha256"]:
            raise ValueError(f"release artifact checksum mismatch: {artifact['path']}")
    if verification_key is not None:
        subprocess.run(
            [
                "openssl", "dgst", "-sha256", "-verify", str(verification_key),
                "-signature", str(output_dir / "release-manifest.sig"), str(manifest_path),
            ],
            check=True,
            capture_output=True,
        )
    return manifest

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import zipfile


ROOT = Path(__file__).resolve().parents[1]
NAME = "qdl-sdk"
NORMALIZED_NAME = "qdl_sdk"
VERSION = "2.0.0"
DIST_INFO = f"{NORMALIZED_NAME}-{VERSION}.dist-info"
DEPENDENCIES = (
    "grpcio>=1.70.0,<2.0.0",
    "httpx>=0.28.0,<1.0.0",
    "protobuf>=6.31.1,<7.0.0",
    "pydantic>=2.0.0,<3.0.0",
)
FIXED_TIMESTAMP = (2020, 1, 1, 0, 0, 0)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def record_digest(value: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(value).digest()).rstrip(b"=")
    return f"sha256={digest.decode('ascii')}"


def digest_paths(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = path.relative_to(ROOT).as_posix().encode()
        content = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def release_files() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for path in sorted((ROOT / "qdl_sdk").glob("*.py")):
        files[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    readme = ROOT / "qdl_sdk/README.md"
    files[readme.relative_to(ROOT).as_posix()] = readme.read_bytes()
    for path in sorted((ROOT / "generated/python/qdl").rglob("*.py")):
        archive = path.relative_to(ROOT / "generated/python").as_posix()
        files[archive] = path.read_bytes()
    forbidden = (b"qdl.api_v2", b"qdl.runtime", b"app.")
    sdk_sources = b"\n".join(
        value
        for name, value in files.items()
        if name.startswith("qdl_sdk/") and name.endswith(".py")
    )
    found = [token.decode() for token in forbidden if token in sdk_sources]
    if found:
        raise RuntimeError(f"SDK imports service internals: {found}")
    if not any(name == "qdl/query/v2/query_pb2.py" for name in files):
        raise RuntimeError("generated query contract is missing from SDK artifact")
    return files


def metadata() -> bytes:
    requires = "".join(f"Requires-Dist: {dependency}\n" for dependency in DEPENDENCIES)
    return (
        "Metadata-Version: 2.3\n"
        f"Name: {NAME}\n"
        f"Version: {VERSION}\n"
        "Summary: Typed provider-neutral client for Quant Data Layer V2\n"
        "Author-email: BobbyAxerol <vugioan11022002@gmail.com>\n"
        "License: MIT\n"
        "Requires-Python: >=3.10\n"
        f"{requires}"
        "Description-Content-Type: text/markdown\n\n"
        "# Quant Data Layer SDK\n"
    ).encode()


def write_zip_entry(archive: zipfile.ZipFile, name: str, content: bytes) -> None:
    info = zipfile.ZipInfo(name, FIXED_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    archive.writestr(info, content, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def build_wheel(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    wheel_name = f"{NORMALIZED_NAME}-{VERSION}-py3-none-any.whl"
    wheel_path = output_dir / wheel_name
    files = release_files()
    license_content = (ROOT / "LICENSE").read_bytes()
    files[f"{DIST_INFO}/METADATA"] = metadata()
    files[f"{DIST_INFO}/WHEEL"] = (
        "Wheel-Version: 1.0\n"
        "Generator: qdl-sdk-release/1\n"
        "Root-Is-Purelib: true\n"
        "Tag: py3-none-any\n"
    ).encode()
    files[f"{DIST_INFO}/top_level.txt"] = b"qdl\nqdl_sdk\n"
    files[f"{DIST_INFO}/licenses/LICENSE"] = license_content

    rows: list[list[str]] = []
    for name in sorted(files):
        value = files[name]
        rows.append([name, record_digest(value), str(len(value))])
    record_name = f"{DIST_INFO}/RECORD"
    rows.append([record_name, "", ""])
    stream = io.StringIO(newline="")
    csv.writer(stream, lineterminator="\n").writerows(rows)
    files[record_name] = stream.getvalue().encode()

    temporary = wheel_path.with_suffix(".whl.tmp")
    with zipfile.ZipFile(temporary, "w") as archive:
        for name in sorted(files):
            write_zip_entry(archive, name, files[name])
    os.replace(temporary, wheel_path)

    contract_paths = sorted((ROOT / "contracts/proto").rglob("*.proto"))
    source_paths = sorted((ROOT / "qdl_sdk").glob("*.py"))
    manifest = {
        "schema": "qdl.sdk.release.v1",
        "name": NAME,
        "version": VERSION,
        "wheel": wheel_path.name,
        "wheel_sha256": sha256_bytes(wheel_path.read_bytes()),
        "sdk_source_digest": digest_paths(source_paths),
        "generated_contract_digest": digest_paths(contract_paths),
        "python_requires": ">=3.10",
        "dependencies": list(DEPENDENCIES),
        "contains_service_internals": False,
        "reproducible_timestamp": "2020-01-01T00:00:00Z",
    }
    manifest_path = output_dir / f"{NORMALIZED_NAME}-{VERSION}.release.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{hashlib.sha256(wheel_path.read_bytes()).hexdigest()[:32]}",
        "version": 1,
        "metadata": {
            "component": {
                "type": "library",
                "name": NAME,
                "version": VERSION,
                "hashes": [{"alg": "SHA-256", "content": manifest["wheel_sha256"]}],
            }
        },
        "components": [
            {"type": "library", "name": item.split(">=")[0], "version": item}
            for item in DEPENDENCIES
        ],
    }
    sbom_path = output_dir / f"{NORMALIZED_NAME}-{VERSION}.cdx.json"
    sbom_path.write_text(json.dumps(sbom, indent=2, sort_keys=True) + "\n")
    return {**manifest, "manifest": str(manifest_path), "sbom": str(sbom_path)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build_wheel(args.output_dir.resolve()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

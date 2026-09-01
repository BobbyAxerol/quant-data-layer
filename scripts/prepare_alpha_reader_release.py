#!/usr/bin/env python3
"""Seal a secret-free reader release from config-derived alpha bindings."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping
import uuid

from qdl.consumer.universal_release import ConsumerRouteBinding


CONFIRM = "PREPARE_QDL_ALPHA_READER_RELEASE"
SCHEMA = "qdl.v2.alpha-reader-release.v1"
READER_SERVICES = (
    "query_v2_1",
    "query_v2_2",
    "stream_v2_active",
    "stream_v2_passive",
)
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SOURCE_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_RELEASE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{2,120}\Z")
_BINDING_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,180}\.binding\.json\Z")
_IMAGE_REFERENCE = re.compile(r"qdl-v2-python:[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class ReleasePreparationError(ValueError):
    """Raised when a release input cannot be sealed safely."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _load_mapping(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ReleasePreparationError(f"{label} is missing") from error
    except json.JSONDecodeError as error:
        raise ReleasePreparationError(f"{label} is not valid JSON") from error
    if not isinstance(value, dict):
        raise ReleasePreparationError(f"{label} must be a JSON object")
    return value


def _require_digest(value: object, label: str) -> str:
    result = str(value or "").strip().lower()
    if not _DIGEST.fullmatch(result):
        raise ReleasePreparationError(f"{label} must be a SHA-256 digest")
    return result


def _validate_inventory(path: Path) -> tuple[dict[str, Any], str]:
    inventory = _load_mapping(path, "inventory")
    expected = {
        "schema", "revision", "registry_path", "registry_sha256", "deployments", "inventory_sha256",
    }
    if set(inventory) != expected or inventory.get("schema") != "execution-alpha.data-requirements.v1":
        raise ReleasePreparationError("inventory schema or fields are invalid")
    reported = _require_digest(inventory.get("inventory_sha256"), "inventory_sha256")
    unsigned = dict(inventory)
    unsigned.pop("inventory_sha256")
    if _digest(unsigned) != reported:
        raise ReleasePreparationError("inventory checksum differs")
    if not isinstance(inventory.get("deployments"), list) or not inventory["deployments"]:
        raise ReleasePreparationError("inventory deployments are required")
    return inventory, reported


def _validate_report(path: Path, *, inventory_sha256: str) -> dict[str, Any]:
    report = _load_mapping(path, "compilation report")
    expected = {
        "schema", "contract_version", "inventory_sha256", "catalog_sha256",
        "reference_manifest_sha256", "release_routing_sha256", "policy_sha256",
        "deployments", "compilation_sha256",
    }
    if set(report) != expected or report.get("schema") != "qdl.v2.alpha-deployment-binding-compilation.v1":
        raise ReleasePreparationError("compilation report schema or fields are invalid")
    if _require_digest(report.get("inventory_sha256"), "report inventory_sha256") != inventory_sha256:
        raise ReleasePreparationError("compilation report inventory checksum differs")
    reported = _require_digest(report.get("compilation_sha256"), "compilation_sha256")
    unsigned = dict(report)
    unsigned.pop("compilation_sha256")
    if _digest(unsigned) != reported:
        raise ReleasePreparationError("compilation report checksum differs")
    deployments = report.get("deployments")
    if not isinstance(deployments, list):
        raise ReleasePreparationError("compilation report deployments are invalid")
    return report


def _canonical_binding(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    try:
        canonical = ConsumerRouteBinding.from_canonical_mapping(value).canonical_mapping()
    except (KeyError, TypeError, ValueError) as error:
        raise ReleasePreparationError(f"{label} is invalid") from error
    if dict(value) != canonical:
        raise ReleasePreparationError(f"{label} is not canonical")
    return canonical


def _binding_digests(report: Mapping[str, Any]) -> set[str]:
    result: set[str] = set()
    for deployment in report["deployments"]:
        if not isinstance(deployment, Mapping):
            raise ReleasePreparationError("compilation deployment is invalid")
        if deployment.get("status") != "ADMITTED":
            continue
        binding = deployment.get("binding")
        if not isinstance(binding, Mapping):
            raise ReleasePreparationError("admitted deployment binding is missing")
        _canonical_binding(binding, "admitted deployment binding")
        digest = _require_digest(binding.get("binding_sha256"), "binding_sha256")
        if digest in result:
            raise ReleasePreparationError("admitted deployment binding is duplicated")
        result.add(digest)
    if not result:
        raise ReleasePreparationError("compilation report has no admitted bindings")
    return result


def _validate_binding_files(directory: Path, expected_digests: set[str]) -> tuple[Path, ...]:
    if not directory.is_dir():
        raise ReleasePreparationError("binding directory is missing")
    files = tuple(sorted(directory.glob("*.binding.json")))
    if len(files) != len(expected_digests):
        raise ReleasePreparationError("binding file count differs from compilation report")
    observed: set[str] = set()
    for path in files:
        if not _BINDING_NAME.fullmatch(path.name):
            raise ReleasePreparationError("binding filename is unsafe")
        binding = _load_mapping(path, f"binding {path.name}")
        _canonical_binding(binding, "binding file")
        digest = _require_digest(binding.get("binding_sha256"), "binding file binding_sha256")
        if digest in observed:
            raise ReleasePreparationError("binding file digest is duplicated")
        observed.add(digest)
    if observed != expected_digests:
        raise ReleasePreparationError("binding files differ from compilation report")
    return files


def _validate_output(path: Path, *, inventory: Path, bindings: Path, report: Path) -> Path:
    output = path.resolve()
    if not _RELEASE_NAME.fullmatch(output.name):
        raise ReleasePreparationError("output release directory name is unsafe")
    if output.exists():
        raise FileExistsError("output release directory already exists")
    if not output.parent.is_dir():
        raise FileNotFoundError("output release parent must already exist")
    for source in (inventory.resolve(), bindings.resolve(), report.resolve()):
        if output == source or source in output.parents:
            raise ReleasePreparationError("output release directory must not contain an input artifact")
    return output


def _render_override(image_reference: str) -> bytes:
    return (
        "services:\n"
        + "".join(f"  {service}:\n    image: {image_reference}\n" for service in READER_SERVICES)
    ).encode("utf-8")


def _write_json(path: Path, value: Mapping[str, Any]) -> bytes:
    rendered = json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.write_bytes(rendered)
    path.chmod(0o640)
    return rendered


def prepare_alpha_reader_release(
    *,
    inventory_path: Path,
    bindings_dir: Path,
    report_path: Path,
    output_dir: Path,
    source_revision: str,
    image_reference: str,
    image_id: str,
    rollback_image_reference: str,
    rollback_image_id: str,
    apply: bool,
) -> dict[str, object]:
    """Validate and atomically seal only public alpha-reader release artifacts."""

    if not _SOURCE_REVISION.fullmatch(source_revision):
        raise ReleasePreparationError("source revision must be a full lowercase Git SHA")
    if not _IMAGE_REFERENCE.fullmatch(image_reference):
        raise ReleasePreparationError("image reference is not a canonical reader tag")
    if not _IMAGE_REFERENCE.fullmatch(rollback_image_reference):
        raise ReleasePreparationError("rollback image reference is not a canonical reader tag")
    if not _IMAGE_ID.fullmatch(image_id) or not _IMAGE_ID.fullmatch(rollback_image_id):
        raise ReleasePreparationError("candidate and rollback image IDs must be immutable sha256 digests")
    if image_id == rollback_image_id:
        raise ReleasePreparationError("candidate image must differ from rollback image")

    inventory, inventory_sha256 = _validate_inventory(inventory_path)
    report = _validate_report(report_path, inventory_sha256=inventory_sha256)
    binding_files = _validate_binding_files(bindings_dir, _binding_digests(report))
    output = _validate_output(
        output_dir,
        inventory=inventory_path,
        bindings=bindings_dir,
        report=report_path,
    )
    input_hashes = {
        "inventory.json": _sha256_bytes(inventory_path.read_bytes()),
        "compilation-report.json": _sha256_bytes(report_path.read_bytes()),
        **{
            f"bindings/{path.name}": _sha256_bytes(path.read_bytes())
            for path in binding_files
        },
    }
    manifest_without_digest: dict[str, object] = {
        "schema": SCHEMA,
        "source_revision": source_revision,
        "image_reference": image_reference,
        "image_id": image_id,
        "rollback_image_reference": rollback_image_reference,
        "rollback_image_id": rollback_image_id,
        "services": list(READER_SERVICES),
        "inventory_sha256": inventory_sha256,
        "compilation_sha256": report["compilation_sha256"],
        "binding_count": len(binding_files),
        "input_sha256": input_hashes,
        "secret_values_recorded": False,
        "runtime_mutations": 0,
        "order_actions": 0,
    }
    manifest = {**manifest_without_digest, "manifest_sha256": _digest(manifest_without_digest)}
    result: dict[str, object] = {
        "status": "APPLIED" if apply else "DRY_RUN",
        "output_dir": str(output),
        "manifest_sha256": manifest["manifest_sha256"],
        "binding_count": len(binding_files),
        "runtime_mutations": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
    }
    if not apply:
        return result

    staging = output.parent / f".{output.name}.staging-{uuid.uuid4().hex}"
    old_umask = os.umask(0o077)
    try:
        staging.mkdir(mode=0o700)
        (staging / "bindings").mkdir(mode=0o750)
        (staging / "inventory.json").write_bytes(inventory_path.read_bytes())
        (staging / "inventory.json").chmod(0o640)
        (staging / "compilation-report.json").write_bytes(report_path.read_bytes())
        (staging / "compilation-report.json").chmod(0o640)
        for source in binding_files:
            destination = staging / "bindings" / source.name
            shutil.copyfile(source, destination)
            destination.chmod(0o640)
        override = staging / "reader-image.override.yml"
        override.write_bytes(_render_override(image_reference))
        override.chmod(0o640)
        rollback_override = staging / "reader-rollback.override.yml"
        rollback_override.write_bytes(_render_override(rollback_image_reference))
        rollback_override.chmod(0o640)
        _write_json(staging / "release-manifest.json", manifest)
        staging.rename(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        os.umask(old_umask)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--bindings-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--image-reference", required=True)
    parser.add_argument("--image-id", required=True)
    parser.add_argument("--rollback-image-reference", required=True)
    parser.add_argument("--rollback-image-id", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise RuntimeError(f"--apply requires --confirm {CONFIRM}")
    result = prepare_alpha_reader_release(
        inventory_path=args.inventory,
        bindings_dir=args.bindings_dir,
        report_path=args.report,
        output_dir=args.output_dir,
        source_revision=args.source_revision,
        image_reference=args.image_reference,
        image_id=args.image_id,
        rollback_image_reference=args.rollback_image_reference,
        rollback_image_id=args.rollback_image_id,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

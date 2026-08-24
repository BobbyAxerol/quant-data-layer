#!/usr/bin/env python3
"""Prepare an isolated R1 bundle without rotating live V2 identities.

``phaseb_prepare_stable_candidate.py`` mints a new authority database
credential and workload identities, so it cannot prepare a canary against the
already-running authority control plane. This tool requires the explicit active
env, copies only its sealed bundle-local identity/certificate material into a
new private release directory, writes fresh runtime JSON for one immutable Rust
image, and creates a new Phase 9.2 signing
key/group. It never changes the source bundle, database, Kafka, Redis, or a
running Compose service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sys
import time
from typing import Callable
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_production_core_bundle,
    write_stable_runtime_bundle,
)


CONFIRM = "PREPARE_QDL_R1_RELEASE_BUNDLE"
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_GROUP = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{2,180}\Z")
_REQUIRED_ENV = frozenset((
    "QDL_STABLE_RUST_IMAGE",
    "QDL_STABLE_RUNTIME_DIR",
    "QDL_STABLE_CURSOR_KEYS_JSON",
    "QDL_STABLE_INTERNAL_INGEST_SECRET",
    "QDL_STABLE_CONTROL_DB_DSN",
    "QDL_STABLE_CONTROL_ADMIN_DSN",
    "QDL_STABLE_CORE_CERT_DIR",
))
_BUNDLE_PATH_ENV = frozenset((
    "QDL_STABLE_CERT_DIR",
    "QDL_STABLE_PROJECTOR_CERT_DIR",
    "QDL_STABLE_AUTHORITY_CERT_DIR",
    "QDL_STABLE_CORE_CERT_DIR",
    "QDL_STABLE_PRODUCER_CERT_DIR",
    "QDL_STABLE_QUERY_CERT_DIR",
    "QDL_STABLE_STREAM_CERT_DIR",
    "QDL_STABLE_TRADING_SYSTEM_CERT_DIR",
    "QDL_STABLE_TRADING_SYSTEM_JWT_PRIVATE_KEY",
    "QDL_STABLE_ALPHA_BINANCE_CERT_DIR",
    "QDL_STABLE_ALPHA_BINANCE_JWT_PRIVATE_KEY",
))
_MATERIAL_PREFIXES = ("identities", "cert-material")


def _parse_env(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise ValueError(f"stable env line has no '=': {line!r}")
        key, value = stripped.split("=", 1)
        if not key or not key.replace("_", "").isalnum():
            raise ValueError(f"stable env variable is invalid: {key!r}")
        value = value.strip()
        if len(value) >= 2 and value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return lines, values


def _render_env_value(value: str) -> str:
    if not value or any(char.isspace() for char in value) or value.startswith(("{", "[", "#")):
        if "'" in value:
            raise ValueError("R1 bundle env value contains an unsupported single quote")
        return f"'{value}'"
    return value


def _rewrite_env(lines: list[str], overrides: dict[str, str | None]) -> str:
    pending = dict(overrides)
    rendered: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            rendered.append(line)
            continue
        key = stripped.split("=", 1)[0]
        if key in pending:
            replacement = pending.pop(key)
            if replacement is not None:
                rendered.append(f"{key}={_render_env_value(replacement)}")
        else:
            rendered.append(line)
    for key in sorted(pending):
        if pending[key] is not None:
            rendered.append(f"{key}={_render_env_value(pending[key] or '')}")
    return "\n".join(rendered) + "\n"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _material_dirs(source: Path) -> tuple[Path, ...]:
    result = tuple(sorted(
        item for item in source.iterdir()
        if item.is_dir() and item.name.startswith(_MATERIAL_PREFIXES)
    ))
    if not result or not (source / "identities").is_dir():
        raise FileNotFoundError("source bundle must retain identities/ material")
    return result


def _material_tree_sha256(source: Path, directories: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for directory in directories:
        root = directory.relative_to(source).as_posix().encode("utf-8")
        digest.update(len(root).to_bytes(4, "big"))
        digest.update(root)
        for item in sorted(directory.rglob("*")):
            if not item.is_file():
                continue
            relative = item.relative_to(source).as_posix().encode("utf-8")
            digest.update(len(relative).to_bytes(4, "big"))
            digest.update(relative)
            content = item.read_bytes()
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
    return digest.hexdigest()


def _relative_to_bundle(
    value: str,
    *,
    source: Path,
    output: Path,
    material_names: frozenset[str],
    variable_name: str,
) -> str:
    candidate = Path(value)
    try:
        relative = candidate.resolve(strict=False).relative_to(source)
    except ValueError:
        return value
    if not relative.parts or relative.parts[0] not in material_names:
        raise ValueError(
            f"R1 bundle-local path for {variable_name} is not sealed release material"
        )
    return str(output / relative)


def _runtime_digests(runtime: Path) -> dict[str, str]:
    return {
        item.name: _sha256(item)
        for item in sorted(runtime.iterdir())
        if item.is_file()
    }


def prepare_release_bundle(
    *,
    source_bundle: Path,
    output_bundle: Path,
    rust_image_id: str,
    apply: bool,
    source_env: Path,
    group_id: str | None = None,
    key_factory: Callable[[int], str] = secrets.token_hex,
    clock: Callable[[], int] = time.time_ns,
) -> dict[str, object]:
    source = source_bundle.resolve()
    output = output_bundle.resolve()
    if source == output or not (source / "stable.env").is_file() or not (source / "identities").is_dir():
        raise FileNotFoundError("source bundle must contain stable.env and identities/ and differ from output")
    if source_env is None:
        raise ValueError("R1 source env must be explicitly provided")
    env_path = source_env.resolve()
    try:
        env_path.relative_to(source)
    except ValueError as error:
        raise ValueError("R1 source env must remain inside the source bundle") from error
    if not env_path.is_file():
        raise FileNotFoundError(f"R1 source env is unavailable: {env_path}")
    if output.exists():
        raise FileExistsError("R1 output bundle already exists; refusing to overwrite it")
    if not output.parent.is_dir():
        raise FileNotFoundError(f"R1 output parent does not exist: {output.parent}")
    if not _DIGEST.fullmatch(rust_image_id):
        raise ValueError("R1 Rust image must be an immutable sha256 digest")

    lines, env = _parse_env(env_path)
    material_dirs = _material_dirs(source)
    missing = sorted(name for name in _REQUIRED_ENV if not env.get(name, "").strip())
    if missing:
        raise ValueError("source bundle is missing required stable env values: " + ",".join(missing))
    image_suffix = rust_image_id.removeprefix("sha256:")[:12]
    effective_group = group_id or f"qdl-v2-production-core-r1-{image_suffix}"
    if not _GROUP.fullmatch(effective_group) or effective_group.endswith("-phase92-raw"):
        raise ValueError("R1 bootstrap group ID is invalid")
    key_id = f"phase92-r1-{image_suffix}"
    key = key_factory(32)
    if not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key):
        raise ValueError("R1 bootstrap key factory must produce 256-bit lowercase hex")

    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition = StableAcquisitionPlan.load(
        ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
    )
    scope = AuthorityPromotionScope.load(
        ROOT / "config/v2/stable-authority-promotion-scope.yaml", catalog=catalog
    )
    authority = stable_authority_record(
        rust_image_digest=rust_image_id,
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=(ROOT / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
        effective_at_ns=clock(),
    )
    overrides = {
        "QDL_STABLE_RUST_IMAGE": rust_image_id,
        "QDL_STABLE_RUNTIME_DIR": str(output / "runtime"),
        "QDL_PHASE92_BOOTSTRAP_CURSOR_KEYS_JSON": json.dumps({key_id: key}, separators=(",", ":")),
        "QDL_PHASE92_BOOTSTRAP_CURSOR_ACTIVE_KEY_ID": key_id,
        "QDL_PHASE92_BOOTSTRAP_GROUP_ID": effective_group,
        # A release starts from explicit current env/image selection. Retaining
        # a historical c39/c40 override would silently re-pin stale images.
        "QDL_STABLE_COMPOSE_OVERRIDE": None,
    }
    material_names = frozenset(item.name for item in material_dirs)
    for name in _BUNDLE_PATH_ENV:
        if value := env.get(name):
            overrides[name] = _relative_to_bundle(
                value,
                source=source,
                output=output,
                material_names=material_names,
                variable_name=name,
            )

    result: dict[str, object] = {
        "schema": "qdl.r1.release-bundle.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "source_bundle": str(source),
        "source_env": str(env_path.relative_to(source)),
        "source_env_sha256": _sha256(env_path),
        "source_material_sha256": _material_tree_sha256(source, material_dirs),
        "source_material_directories": [item.name for item in material_dirs],
        "output_bundle": str(output),
        "rust_image_digest": rust_image_id,
        "previous_rust_image_digest": env["QDL_STABLE_RUST_IMAGE"],
        "bootstrap_group_id": effective_group,
        "bootstrap_key_id": key_id,
        "bootstrap_key_sha256": hashlib.sha256(key.encode("ascii")).hexdigest(),
        "promotion_binding_count": len(scope.binding_ids),
        "promotion_scope_digest": scope.digest(),
        "source_mutations": 0,
        "production_mutations": 0,
        "secret_values_recorded": False,
    }
    if not apply:
        return result

    staging = output.parent / f".{output.name}.staging-{uuid.uuid4().hex}"
    old_umask = os.umask(0o077)
    try:
        staging.mkdir(mode=0o700)
        for directory in material_dirs:
            shutil.copytree(directory, staging / directory.name)
        runtime = staging / "runtime"
        runtime.mkdir(mode=0o700)
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
            promotion_scope=scope,
        )
        write_production_core_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            promotion_scope=scope,
            raw_authority=authority,
            partition_plan_epoch=1,
        )
        (staging / "stable.env").write_text(_rewrite_env(lines, overrides), encoding="utf-8")
        (staging / "stable.env").chmod(0o600)
        result["runtime_digests"] = _runtime_digests(runtime)
        manifest = staging / "release-manifest.json"
        manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        manifest.chmod(0o640)
        staging.rename(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        os.umask(old_umask)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bundle", type=Path, required=True)
    parser.add_argument("--output-bundle", type=Path, required=True)
    parser.add_argument("--rust-image-id", required=True)
    parser.add_argument("--source-env", type=Path, required=True, help="explicit active env file inside source bundle")
    parser.add_argument("--group-id")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    if args.apply and args.confirm != CONFIRM:
        raise RuntimeError(f"--apply requires --confirm {CONFIRM}")
    result = prepare_release_bundle(
        source_bundle=args.source_bundle,
        output_bundle=args.output_bundle,
        rust_image_id=args.rust_image_id,
        source_env=args.source_env,
        group_id=args.group_id,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

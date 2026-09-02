#!/usr/bin/env python3
"""Refresh exactly the three active V2 Rust-core configs.

The stable successor runtime is a role-config directory, not a generic bundle.
This tool deliberately rewrites only ``core.json``, ``core-002.json`` and
``core-003.json`` after proving that their sole semantic change is the declared
L2 materialized-snapshot cadence. It also advances only the immutable Rust image
selector in the sibling Compose environment file.  All prior bytes are kept in
one private rollback directory before the atomic replacements occur.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    STABLE_CORE_DEDUP_CAPACITY,
    StableAcquisitionPlan,
    validate_shared_authority_record,
    write_stable_runtime_bundle,
)


CONFIRM = "REFRESH_QDL_V2_RUST_CORE_RUNTIME"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
CORE_FILES = ("core.json", "core-002.json", "core-003.json")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_HOT_L2_SOURCE_IDS = frozenset({
    "binance-usdm-ethusdt-book-primary-v2",
    "okx-swap-eth-usdt-swap-book-primary-v2",
})
_HOT_L2_MATERIALIZATION_INTERVAL_MS = 1_000
_PREVIOUS_STABLE_CORE_DEDUP_CAPACITY = 1_000_000


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_json(path: Path, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{field} is unreadable: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object: {path}")
    return value


def _load_authority(runtime_dir: Path) -> tuple[dict[str, Any], bytes]:
    path = runtime_dir / "authority.json"
    try:
        encoded = path.read_bytes()
    except OSError as error:
        raise ValueError(f"active authority is unreadable: {path}") from error
    try:
        authority = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("active authority is invalid JSON") from error
    if not isinstance(authority, dict):
        raise ValueError("active authority is not an object")
    try:
        validate_shared_authority_record(authority)
    except ValueError as error:
        raise ValueError("active authority is invalid") from error
    return dict(authority), encoded


def _bindings(value: Mapping[str, Any], *, field: str) -> tuple[dict[str, Any], ...]:
    core = value.get("core")
    bindings = core.get("bindings") if isinstance(core, Mapping) else None
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} lacks core bindings")
    result: list[dict[str, Any]] = []
    source_ids: set[str] = set()
    for item in bindings:
        if not isinstance(item, dict):
            raise ValueError(f"{field} contains a non-object binding")
        source_id = item.get("source_id")
        if not isinstance(source_id, str) or not source_id or source_id in source_ids:
            raise ValueError(f"{field} contains an invalid/duplicate source_id")
        source_ids.add(source_id)
        result.append(item)
    return tuple(result)


def _without_bindings_and_dedup_capacity(value: Mapping[str, Any]) -> tuple[dict[str, Any], int]:
    result = dict(value)
    core = result.get("core")
    if not isinstance(core, Mapping):
        raise ValueError("core config lacks core object")
    normalized_core = dict(core)
    normalized_core.pop("bindings", None)
    dedup_capacity = normalized_core.pop("dedup_capacity", None)
    if not isinstance(dedup_capacity, int):
        raise ValueError("core config lacks integer dedup capacity")
    result["core"] = normalized_core
    return result, dedup_capacity


def _validate_only_materialized_snapshot_interval(
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    file_name: str,
) -> dict[str, Any]:
    active_without, active_dedup_capacity = _without_bindings_and_dedup_capacity(active)
    expected_without, expected_dedup_capacity = _without_bindings_and_dedup_capacity(expected)
    if active_without != expected_without:
        raise ValueError(f"{file_name} changes a non-binding core field")
    if (
        active_dedup_capacity not in {
            _PREVIOUS_STABLE_CORE_DEDUP_CAPACITY,
            STABLE_CORE_DEDUP_CAPACITY,
        }
        or expected_dedup_capacity != STABLE_CORE_DEDUP_CAPACITY
    ):
        raise ValueError(f"{file_name} has an invalid bounded dedup transition")
    before = _bindings(active, field=f"active {file_name}")
    after = _bindings(expected, field=f"expected {file_name}")
    before_ids = [str(item["source_id"]) for item in before]
    after_ids = [str(item["source_id"]) for item in after]
    if before_ids != after_ids:
        raise ValueError(f"{file_name} changes binding order or membership")

    l2_sources: list[str] = []
    for current, refreshed in zip(before, after, strict=True):
        source_id = str(current["source_id"])
        if current.get("l2") is None:
            if current != refreshed:
                raise ValueError(f"{file_name} changes non-L2 binding {source_id}")
            continue
        current_without = dict(current)
        refreshed_without = dict(refreshed)
        current_l2 = current_without.get("l2")
        refreshed_l2 = refreshed_without.get("l2")
        if not isinstance(current_l2, Mapping) or not isinstance(refreshed_l2, Mapping):
            raise ValueError(f"{file_name} has invalid L2 contract: {source_id}")
        current_l2 = dict(current_l2)
        refreshed_l2 = dict(refreshed_l2)
        old_provider_refresh = current_l2.pop("snapshot_refresh_seconds", None)
        new_provider_refresh = refreshed_l2.pop("snapshot_refresh_seconds", None)
        old_materialization = current_l2.pop("materialized_snapshot_interval_ms", None)
        new_materialization = refreshed_l2.pop("materialized_snapshot_interval_ms", None)
        current_without["l2"] = current_l2
        refreshed_without["l2"] = refreshed_l2
        if current_without != refreshed_without:
            raise ValueError(f"{file_name} changes L2 binding fields beyond cadence: {source_id}")
        if old_provider_refresh != new_provider_refresh or new_provider_refresh != 30:
            raise ValueError(f"{file_name} changes provider refresh cadence: {source_id}")
        if source_id in _HOT_L2_SOURCE_IDS:
            if (
                old_materialization not in {None, _HOT_L2_MATERIALIZATION_INTERVAL_MS}
                or new_materialization != _HOT_L2_MATERIALIZATION_INTERVAL_MS
            ):
                raise ValueError(f"{file_name} has invalid hot materialization transition: {source_id}")
        elif old_materialization is not None or new_materialization is not None:
            raise ValueError(f"{file_name} changes undeclared materialization cadence: {source_id}")
        l2_sources.append(source_id)
    if not l2_sources:
        raise ValueError(f"{file_name} has no L2 binding to refresh")
    if not _HOT_L2_SOURCE_IDS.issubset(l2_sources):
        raise ValueError(f"{file_name} lacks a declared hot materialization binding")
    return {
        "file": file_name,
        "l2_source_ids": sorted(l2_sources),
        "materialized_snapshot_interval_ms": _HOT_L2_MATERIALIZATION_INTERVAL_MS,
        "hot_l2_source_ids": sorted(_HOT_L2_SOURCE_IDS),
        "dedup_capacity": {
            "before": active_dedup_capacity,
            "after": expected_dedup_capacity,
        },
    }


def _render_expected(
    *,
    authority: Mapping[str, Any],
    catalog_path: Path,
    acquisition_path: Path,
) -> dict[str, bytes]:
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    with tempfile.TemporaryDirectory(prefix="qdl-core-runtime-render-") as raw:
        runtime = Path(raw) / "runtime"
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        return {
            name: (runtime / name).read_bytes()
            for name in CORE_FILES
        }


def _replace_rust_image(environment: bytes, *, expected_old: str, new: str) -> bytes:
    try:
        text = environment.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("rollout environment is not UTF-8") from error
    matches = [
        index
        for index, line in enumerate(text.splitlines(keepends=True))
        if line.rstrip("\r\n").startswith("QDL_STABLE_RUST_IMAGE=")
    ]
    if len(matches) != 1:
        raise ValueError("rollout environment must contain exactly one Rust image selector")
    lines = text.splitlines(keepends=True)
    current = lines[matches[0]].rstrip("\r\n").split("=", 1)[1]
    if current != expected_old:
        raise ValueError("rollout environment Rust image does not match expected active digest")
    newline = "\r\n" if lines[matches[0]].endswith("\r\n") else "\n"
    lines[matches[0]] = f"QDL_STABLE_RUST_IMAGE={new}{newline}"
    return "".join(lines).encode("utf-8")


def _write_atomic(path: Path, content: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.qdl-refresh-{os.getpid()}-{time.time_ns()}")
    try:
        with open(temporary, "xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def refresh(
    *,
    runtime_dir: Path,
    rollout_env: Path,
    active_rust_image: str,
    new_rust_image: str,
    output_dir: Path | None,
    apply: bool,
    catalog_path: Path = ROOT / "config/v2/stable-source-bindings.yaml",
    acquisition_path: Path = ROOT / "config/v2/stable-acquisition-bindings.yaml",
    state_root: Path = DEFAULT_STATE_ROOT,
) -> dict[str, Any]:
    runtime_dir = runtime_dir.resolve()
    rollout_env = rollout_env.resolve()
    state_root = state_root.resolve()
    if not runtime_dir.is_dir():
        raise ValueError("active runtime directory is invalid")
    if not rollout_env.is_file():
        raise ValueError("active rollout environment is invalid")
    if _DIGEST.fullmatch(active_rust_image) is None or _DIGEST.fullmatch(new_rust_image) is None:
        raise ValueError("Rust image selectors must be immutable SHA-256 digests")
    if apply:
        if output_dir is None:
            raise ValueError("apply requires an output directory")
        output_dir = output_dir.resolve()
        if output_dir.exists() or state_root not in (output_dir, *output_dir.parents):
            raise ValueError("output directory must be a new private QDL state path")

    authority, authority_bytes = _load_authority(runtime_dir)
    expected_bytes = _render_expected(
        authority=authority,
        catalog_path=catalog_path,
        acquisition_path=acquisition_path,
    )
    active_bytes = {name: (runtime_dir / name).read_bytes() for name in CORE_FILES}
    changes = [
        _validate_only_materialized_snapshot_interval(
            _read_json(runtime_dir / name, field=f"active {name}"),
            json.loads(expected_bytes[name]),
            file_name=name,
        )
        for name in CORE_FILES
    ]
    active_env = rollout_env.read_bytes()
    next_env = _replace_rust_image(
        active_env, expected_old=active_rust_image, new=new_rust_image
    )
    image_selector_changed = active_env != next_env
    result: dict[str, Any] = {
        "schema": "qdl.v2.rust-core-runtime-refresh.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "runtime_dir": str(runtime_dir),
        "rollout_env": str(rollout_env),
        "authority_sha256": _sha256(authority_bytes),
        "authority_bytes_preserved": True,
        "active_rust_image": active_rust_image,
        "new_rust_image": new_rust_image,
        "image_selector_changed": image_selector_changed,
        "changes": changes,
        "files": {
            name: {"before": _sha256(active_bytes[name]), "after": _sha256(expected_bytes[name])}
            for name in CORE_FILES
        },
        "rollout_env_sha256": {"before": _sha256(active_env), "after": _sha256(next_env)},
        "production_mutations": 0 if not apply else len(CORE_FILES) + int(image_selector_changed),
    }
    if not apply:
        return result

    assert output_dir is not None
    rollback_dir = output_dir / "rollback"
    output_dir.mkdir(mode=0o700, parents=True)
    rollback_dir.mkdir(mode=0o700)
    try:
        for name, content in active_bytes.items():
            backup = rollback_dir / name
            backup.write_bytes(content)
            backup.chmod(0o640)
        environment_backup = rollback_dir / "rollout.env"
        environment_backup.write_bytes(active_env)
        environment_backup.chmod(0o600)

        applied: list[Path] = []
        try:
            for name in CORE_FILES:
                target = runtime_dir / name
                _write_atomic(target, expected_bytes[name], mode=0o644)
                applied.append(target)
            if image_selector_changed:
                _write_atomic(rollout_env, next_env, mode=0o600)
                applied.append(rollout_env)
        except Exception:
            for target in reversed(applied):
                backup = rollback_dir / ("rollout.env" if target == rollout_env else target.name)
                _write_atomic(backup_target := target, backup.read_bytes(), mode=0o600 if target == rollout_env else 0o644)
            raise

        result["rollback_dir"] = str(rollback_dir)
        receipt = output_dir / "receipt.json"
        receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        receipt.chmod(0o640)
        return result
    except Exception:
        # Rollback copies are intentionally retained if an apply failed after
        # writing them; they are the exact bounded recovery material.
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--rollout-env", type=Path, required=True)
    parser.add_argument("--active-rust-image", required=True)
    parser.add_argument("--new-rust-image", required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    result = refresh(
        runtime_dir=args.runtime_dir,
        rollout_env=args.rollout_env,
        active_rust_image=args.active_rust_image,
        new_rust_image=args.new_rust_image,
        output_dir=args.output_dir,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

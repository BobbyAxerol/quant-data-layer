#!/usr/bin/env python3
"""Materialize the declared additive L2 mappings in the active Rust cores.

This is deliberately narrower than a generic stable-runtime refresh.  It
updates only the three mounted realtime-core JSON files and only admits the
six already-declared perpetual BOOK mappings required to complete the
five-liquid V2 demand scope.  Existing binding semantics stay byte-for-byte
equivalent apart from their known catalog-revision metadata.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
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
    StableAcquisitionPlan,
    validate_shared_authority_record,
    write_stable_runtime_bundle,
)


CONFIRM = "REFRESH_QDL_V2_L2_CORE_RUNTIME"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
CORE_FILES = ("core.json", "core-002.json", "core-003.json")
DECLARED_ADDITIVE_BOOK_SOURCE_IDS = frozenset({
    "binance-usdm-bnbusdt-book-primary-v2",
    "binance-usdm-dogeusdt-book-primary-v2",
    "binance-usdm-solusdt-book-primary-v2",
    "okx-swap-bnb-usdt-swap-book-primary-v2",
    "okx-swap-doge-usdt-swap-book-primary-v2",
    "okx-swap-sol-usdt-swap-book-primary-v2",
})
_COMMON_METADATA_DRIFT = frozenset({"instrument_catalog_revision"})


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _read_json(path: Path, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{field} is unreadable: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def _bindings(value: Mapping[str, Any], *, field: str) -> tuple[dict[str, Any], ...]:
    core = value.get("core")
    bindings = core.get("bindings") if isinstance(core, Mapping) else None
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} lacks core bindings")
    result: list[dict[str, Any]] = []
    source_ids: set[str] = set()
    for item in bindings:
        if not isinstance(item, dict):
            raise ValueError(f"{field} has a non-object binding")
        source_id = item.get("source_id")
        if not isinstance(source_id, str) or not source_id or source_id in source_ids:
            raise ValueError(f"{field} has an invalid/duplicate source_id")
        source_ids.add(source_id)
        result.append(item)
    return tuple(result)


def _without_catalog_revision(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    for field in _COMMON_METADATA_DRIFT:
        result.pop(field, None)
    return result


def _render_expected(
    *,
    authority: Mapping[str, Any],
    catalog_path: Path,
    acquisition_path: Path,
) -> dict[str, dict[str, Any]]:
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    with tempfile.TemporaryDirectory(prefix="qdl-l2-core-render-") as raw:
        runtime = Path(raw) / "runtime"
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        return {
            name: _read_json(runtime / name, field=f"generated {name}")
            for name in CORE_FILES
        }


def _validate_and_render(
    *,
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    file_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if active.get("authority") != expected.get("authority"):
        raise ValueError(f"{file_name} changes authority")
    active_bindings = _bindings(active, field=f"active {file_name}")
    expected_bindings = _bindings(expected, field=f"expected {file_name}")
    active_by_id = {str(item["source_id"]): item for item in active_bindings}
    expected_by_id = {str(item["source_id"]): item for item in expected_bindings}
    unknown = sorted(active_by_id.keys() - expected_by_id.keys())
    if unknown:
        raise ValueError(f"{file_name} contains bindings absent from current catalog: {unknown}")

    for source_id, current in active_by_id.items():
        generated = expected_by_id[source_id]
        if _without_catalog_revision(current) != _without_catalog_revision(generated):
            raise ValueError(f"{file_name} has semantic drift for {source_id}")

    added_ids = expected_by_id.keys() - active_by_id.keys()
    if added_ids != DECLARED_ADDITIVE_BOOK_SOURCE_IDS:
        raise ValueError(
            f"{file_name} additive BOOK scope differs from the approved declaration: "
            f"{sorted(added_ids)}"
        )
    additions = [
        expected_by_id[str(item["source_id"])]
        for item in expected_bindings
        if str(item["source_id"]) in added_ids
    ]
    for item in additions:
        source_id = str(item["source_id"])
        if (
            not isinstance(item.get("l2"), Mapping)
            or item.get("require_final_bar") is not False
            or item.get("sequence_policy") != "CONTIGUOUS"
            or item.get("product_type") != "PERPETUAL"
            or item.get("provider_kind") not in {"binance_usdm_book", "okx_book"}
        ):
            raise ValueError(f"{file_name} has an invalid declared L2 addition: {source_id}")

    result = copy.deepcopy(dict(active))
    core = result.get("core")
    if not isinstance(core, dict):
        raise ValueError(f"{file_name} lacks mutable core configuration")
    core["bindings"] = [*copy.deepcopy(active_bindings), *copy.deepcopy(additions)]
    return result, {
        "file": file_name,
        "before_binding_count": len(active_bindings),
        "after_binding_count": len(core["bindings"]),
        "added_book_source_ids": sorted(added_ids),
        "added_book_symbols": sorted(str(item["native_symbol"]) for item in additions),
    }


def _atomic_replace(path: Path, content: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.qdl-l2-core-{os.getpid()}-{time.time_ns()}")
    try:
        with temporary.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def refresh(
    *,
    runtime_dir: Path,
    output_dir: Path | None,
    apply: bool,
    catalog_path: Path = ROOT / "config/v2/stable-source-bindings.yaml",
    acquisition_path: Path = ROOT / "config/v2/stable-acquisition-bindings.yaml",
    state_root: Path = DEFAULT_STATE_ROOT,
) -> dict[str, Any]:
    runtime_dir = runtime_dir.resolve()
    state_root = state_root.resolve()
    if not runtime_dir.is_dir():
        raise ValueError("runtime directory is invalid")
    if apply:
        if output_dir is None:
            raise ValueError("apply requires an output directory")
        output_dir = output_dir.resolve()
        if output_dir.exists() or state_root not in (output_dir, *output_dir.parents):
            raise ValueError("output directory must be a new private QDL state path")

    authority_path = runtime_dir / "authority.json"
    authority_bytes = authority_path.read_bytes()
    authority = _read_json(authority_path, field="active authority")
    try:
        validate_shared_authority_record(authority)
    except ValueError as error:
        raise ValueError("active authority is invalid") from error
    expected_by_file = _render_expected(
        authority=authority,
        catalog_path=catalog_path,
        acquisition_path=acquisition_path,
    )

    pending: dict[str, tuple[Path, bytes, bytes, int, dict[str, Any]]] = {}
    for file_name in CORE_FILES:
        path = runtime_dir / file_name
        active_bytes = path.read_bytes()
        rendered, change = _validate_and_render(
            active=_read_json(path, field=f"active {file_name}"),
            expected=expected_by_file[file_name],
            file_name=file_name,
        )
        pending[file_name] = (
            path,
            active_bytes,
            _canonical_bytes(rendered),
            path.stat().st_mode & 0o777,
            change,
        )

    files = {
        name: {
            "before_sha256": _sha256(active),
            "after_sha256": _sha256(expected),
            "before_mode": oct(mode),
            "changed": active != expected,
            **change,
        }
        for name, (_path, active, expected, mode, change) in pending.items()
    }
    if not all(item["changed"] for item in files.values()):
        raise ValueError("all three core files must require the declared additive mapping")
    result: dict[str, Any] = {
        "schema": "qdl.v2.l2-core-runtime-refresh.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "runtime_dir": str(runtime_dir),
        "authority_sha256": _sha256(authority_bytes),
        "authority_bytes_preserved": True,
        "files": files,
        "production_mutations": 0 if not apply else len(CORE_FILES),
    }
    if not apply:
        return result

    assert output_dir is not None
    rollback_dir = output_dir / "rollback"
    output_dir.mkdir(parents=True, mode=0o700)
    rollback_dir.mkdir(mode=0o700)
    for file_name, (_path, active, _expected, mode, _change) in pending.items():
        backup = rollback_dir / file_name
        backup.write_bytes(active)
        backup.chmod(mode)

    applied: list[str] = []
    try:
        for file_name, (path, _active, expected, mode, _change) in pending.items():
            _atomic_replace(path, expected, mode=mode)
            applied.append(file_name)
    except Exception:
        for file_name in reversed(applied):
            path, active, _expected, mode, _change = pending[file_name]
            _atomic_replace(path, active, mode=mode)
        raise

    result["rollback_dir"] = str(rollback_dir)
    receipt = output_dir / "receipt.json"
    receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    receipt.chmod(0o640)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    print(json.dumps(refresh(
        runtime_dir=args.runtime_dir,
        output_dir=args.output_dir,
        apply=args.apply,
    ), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

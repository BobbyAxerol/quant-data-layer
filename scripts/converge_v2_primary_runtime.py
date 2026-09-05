#!/usr/bin/env python3
"""Converge a legacy partial V2 runtime bundle to the canonical compiler output.

The initial Rust-primary packet intentionally mounted a compact realtime
subset.  The current V2 catalog and acquisition plan contain the approved
multi-venue demand set, but a config-only replay of the old packet leaves
declared routes without their physical ingestion/core bindings.  This tool is
the narrow, reusable repair: it regenerates five already-mounted JSON files
from the canonical compiler while preserving the active authority byte-for-byte.

It is dry-run by default.  ``--apply`` atomically replaces only three core
configs and two native-ingestor configs after writing exact rollback bytes to a
new private state directory.  It never talks to a provider, Docker, Kafka,
Redis, SQLite, V1, Trading System, an alpha or an order path.
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


CONFIRM = "CONVERGE_QDL_V2_PRIMARY_RUNTIME"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
CORE_FILES = ("core.json", "core-002.json", "core-003.json")
INGESTOR_FILES = ("ingestor-binance-usdm.json", "ingestor-okx-swap.json")
RUNTIME_FILES = CORE_FILES + INGESTOR_FILES
_ALLOWED_CORE_DEDUP = frozenset({1_000_000, 100_000})
_EXPECTED_CORE_DEDUP = 100_000
_LINEAGE_FIELDS = frozenset({"instrument_catalog_revision", "instrument_revision"})
_INGESTOR_ADDITIVE_FIELDS = frozenset({
    "config_revision",
    "session_liveness_dir",
    "session_liveness_write_interval_ms",
})
_FIVE_LIQUID_PERPETUAL_BOOK_IDS = frozenset({
    "binance-usdm-btcusdt-book-primary-v2",
    "binance-usdm-ethusdt-book-primary-v2",
    "binance-usdm-solusdt-book-primary-v2",
    "binance-usdm-dogeusdt-book-primary-v2",
    "binance-usdm-bnbusdt-book-primary-v2",
    "okx-swap-btc-usdt-swap-book-primary-v2",
    "okx-swap-eth-usdt-swap-book-primary-v2",
    "okx-swap-sol-usdt-swap-book-primary-v2",
    "okx-swap-doge-usdt-swap-book-primary-v2",
    "okx-swap-bnb-usdt-swap-book-primary-v2",
})
_FIVE_LIQUID_BOOK_IDS_BY_INGESTOR = {
    "ingestor-binance-usdm.json": frozenset({
        "binance-usdm-btcusdt-book-primary-v2",
        "binance-usdm-ethusdt-book-primary-v2",
        "binance-usdm-solusdt-book-primary-v2",
        "binance-usdm-dogeusdt-book-primary-v2",
        "binance-usdm-bnbusdt-book-primary-v2",
    }),
    "ingestor-okx-swap.json": frozenset({
        "okx-swap-btc-usdt-swap-book-primary-v2",
        "okx-swap-eth-usdt-swap-book-primary-v2",
        "okx-swap-sol-usdt-swap-book-primary-v2",
        "okx-swap-doge-usdt-swap-book-primary-v2",
        "okx-swap-bnb-usdt-swap-book-primary-v2",
    }),
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(value), indent=2, sort_keys=True) + "\n").encode("utf-8")


def _read_json(path: Path, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{field} is unreadable: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def _without(value: Mapping[str, Any], *fields: str) -> dict[str, Any]:
    result = copy.deepcopy(dict(value))
    for field in fields:
        result.pop(field, None)
    return result


def _binding_map(
    bindings: object,
    *,
    key_field: str,
    field: str,
) -> dict[str, dict[str, Any]]:
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} bindings are invalid")
    result: dict[str, dict[str, Any]] = {}
    for item in bindings:
        if not isinstance(item, dict):
            raise ValueError(f"{field} has a non-object binding")
        key = item.get(key_field)
        if not isinstance(key, str) or not key or key in result:
            raise ValueError(f"{field} has an invalid/duplicate {key_field}")
        result[key] = dict(item)
    return result


def _lineage_equal(active: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return _without(active, *_LINEAGE_FIELDS) == _without(expected, *_LINEAGE_FIELDS)


def _expected_runtime(
    *,
    authority: Mapping[str, Any],
    catalog_path: Path,
    acquisition_path: Path,
) -> dict[str, dict[str, Any]]:
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    with tempfile.TemporaryDirectory(prefix="qdl-primary-runtime-converge-") as raw:
        generated = Path(raw) / "runtime"
        write_stable_runtime_bundle(
            generated,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        return {
            name: _read_json(generated / name, field=f"generated {name}")
            for name in RUNTIME_FILES
        }


def _validate_core(
    *,
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    file_name: str,
) -> dict[str, Any]:
    active_outer = _without(active, "core")
    expected_outer = _without(expected, "core")
    if active_outer != expected_outer:
        raise ValueError(f"{file_name} changes non-core runtime configuration")
    active_core = active.get("core")
    expected_core = expected.get("core")
    if not isinstance(active_core, Mapping) or not isinstance(expected_core, Mapping):
        raise ValueError(f"{file_name} lacks core configuration")
    active_core_static = _without(active_core, "bindings", "dedup_capacity")
    expected_core_static = _without(expected_core, "bindings", "dedup_capacity")
    if active_core_static != expected_core_static:
        raise ValueError(f"{file_name} changes non-binding core configuration")
    active_dedup = active_core.get("dedup_capacity")
    expected_dedup = expected_core.get("dedup_capacity")
    if (
        not isinstance(active_dedup, int)
        or active_dedup not in _ALLOWED_CORE_DEDUP
        or expected_dedup != _EXPECTED_CORE_DEDUP
    ):
        raise ValueError(f"{file_name} has an unsupported dedup transition")

    active_bindings = _binding_map(
        active_core.get("bindings"), key_field="source_id", field=f"active {file_name}"
    )
    expected_bindings = _binding_map(
        expected_core.get("bindings"), key_field="source_id", field=f"expected {file_name}"
    )
    unknown = sorted(active_bindings.keys() - expected_bindings.keys())
    if unknown:
        raise ValueError(f"{file_name} has bindings absent from canonical catalog: {unknown}")
    drift = sorted(
        source_id
        for source_id, binding in active_bindings.items()
        if not _lineage_equal(binding, expected_bindings[source_id])
    )
    if drift:
        raise ValueError(f"{file_name} has retained binding semantic drift: {drift}")
    added = sorted(expected_bindings.keys() - active_bindings.keys())
    missing_liquid_books = sorted(
        _FIVE_LIQUID_PERPETUAL_BOOK_IDS - expected_bindings.keys()
    )
    if missing_liquid_books:
        raise ValueError(f"{file_name} lacks five-liquid perpetual L2 scope: {missing_liquid_books}")
    return {
        "before_binding_count": len(active_bindings),
        "after_binding_count": len(expected_bindings),
        "added_binding_count": len(added),
        "added_five_liquid_book_source_ids": sorted(
            _FIVE_LIQUID_PERPETUAL_BOOK_IDS & set(added)
        ),
        "retained_lineage_update_count": sum(
            active_bindings[source_id] != expected_bindings[source_id]
            for source_id in active_bindings
        ),
        "dedup_capacity": {"before": active_dedup, "after": expected_dedup},
    }


def _validate_ingestor(
    *,
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    file_name: str,
) -> dict[str, Any]:
    active_static = _without(active, "bindings", *_INGESTOR_ADDITIVE_FIELDS)
    expected_static = _without(expected, "bindings", *_INGESTOR_ADDITIVE_FIELDS)
    if active_static != expected_static:
        raise ValueError(f"{file_name} changes non-binding runtime configuration")
    unexpected_removed = sorted(
        set(active.keys()) - set(expected.keys()) - {"config_revision"}
    )
    if unexpected_removed:
        raise ValueError(f"{file_name} has unsupported active fields: {unexpected_removed}")
    expected_liveness = {
        field: expected.get(field)
        for field in ("session_liveness_dir", "session_liveness_write_interval_ms")
    }
    if (
        not isinstance(expected_liveness["session_liveness_dir"], str)
        or not expected_liveness["session_liveness_dir"]
        or expected_liveness["session_liveness_write_interval_ms"] != 1_000
    ):
        raise ValueError(f"{file_name} lacks bounded session-liveness configuration")

    active_bindings = _binding_map(
        active.get("bindings"), key_field="subscription_id", field=f"active {file_name}"
    )
    expected_bindings = _binding_map(
        expected.get("bindings"), key_field="subscription_id", field=f"expected {file_name}"
    )
    unknown = sorted(active_bindings.keys() - expected_bindings.keys())
    if unknown:
        raise ValueError(f"{file_name} has bindings absent from canonical catalog: {unknown}")
    drift = sorted(
        subscription_id
        for subscription_id, binding in active_bindings.items()
        if not _lineage_equal(binding, expected_bindings[subscription_id])
    )
    if drift:
        raise ValueError(f"{file_name} has retained binding semantic drift: {drift}")
    added = sorted(expected_bindings.keys() - active_bindings.keys())
    book_ids = {
        item["subscription_id"]
        for item in expected_bindings.values()
        if item.get("feed") == "BOOK"
    }
    required_books = _FIVE_LIQUID_BOOK_IDS_BY_INGESTOR[file_name]
    if not required_books <= book_ids:
        missing = sorted(required_books - book_ids)
        raise ValueError(f"{file_name} lacks five-liquid native book scope: {missing}")
    return {
        "before_binding_count": len(active_bindings),
        "after_binding_count": len(expected_bindings),
        "added_binding_count": len(added),
        "added_book_subscription_ids": sorted(book_ids & set(added)),
        "retained_lineage_update_count": sum(
            active_bindings[subscription_id] != expected_bindings[subscription_id]
            for subscription_id in active_bindings
        ),
        "config_revision": {
            "before": active.get("config_revision"),
            "after": expected.get("config_revision"),
        },
        "session_liveness_write_interval_ms": expected_liveness[
            "session_liveness_write_interval_ms"
        ],
    }


def _atomic_replace(path: Path, content: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.qdl-primary-converge-{os.getpid()}-{time.time_ns()}")
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


def converge(
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
    expected = _expected_runtime(
        authority=authority,
        catalog_path=catalog_path,
        acquisition_path=acquisition_path,
    )

    pending: dict[str, tuple[Path, bytes, bytes, int, dict[str, Any]]] = {}
    for file_name in CORE_FILES:
        path = runtime_dir / file_name
        active_bytes = path.read_bytes()
        active = _read_json(path, field=f"active {file_name}")
        change = _validate_core(active=active, expected=expected[file_name], file_name=file_name)
        pending[file_name] = (
            path, active_bytes, _canonical_bytes(expected[file_name]),
            path.stat().st_mode & 0o777, change,
        )
    for file_name in INGESTOR_FILES:
        path = runtime_dir / file_name
        active_bytes = path.read_bytes()
        active = _read_json(path, field=f"active {file_name}")
        change = _validate_ingestor(active=active, expected=expected[file_name], file_name=file_name)
        pending[file_name] = (
            path, active_bytes, _canonical_bytes(expected[file_name]),
            path.stat().st_mode & 0o777, change,
        )

    files = {
        name: {
            "before_sha256": _sha256(active),
            "after_sha256": _sha256(rendered),
            "before_mode": oct(mode),
            "changed": active != rendered,
            **change,
        }
        for name, (_path, active, rendered, mode, change) in pending.items()
    }
    if not all(item["changed"] for item in files.values()):
        raise ValueError("all five runtime files must require canonical convergence")
    result: dict[str, Any] = {
        "schema": "qdl.v2.primary-runtime-convergence.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "runtime_dir": str(runtime_dir),
        "authority_sha256": _sha256(authority_bytes),
        "authority_bytes_preserved": True,
        "files": files,
        "production_mutations": 0 if not apply else len(RUNTIME_FILES),
    }
    if not apply:
        return result

    assert output_dir is not None
    rollback_dir = output_dir / "rollback"
    output_dir.mkdir(parents=True, mode=0o700)
    rollback_dir.mkdir(mode=0o700)
    for file_name, (_path, active, _rendered, mode, _change) in pending.items():
        backup = rollback_dir / file_name
        backup.write_bytes(active)
        backup.chmod(mode)

    applied: list[str] = []
    try:
        for file_name, (path, _active, rendered, mode, _change) in pending.items():
            _atomic_replace(path, rendered, mode=mode)
            applied.append(file_name)
    except Exception:
        for file_name in reversed(applied):
            path, active, _rendered, mode, _change = pending[file_name]
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
    print(json.dumps(converge(
        runtime_dir=args.runtime_dir,
        output_dir=args.output_dir,
        apply=args.apply,
    ), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

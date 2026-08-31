#!/usr/bin/env python3
"""Safely materialize additive realtime L2 bindings for the shared ingestors.

The stable BAR edge owns every final BAR bootstrap and recurring provider poll.
This compiler intentionally refreshes only the physical realtime inputs of the
two existing shared Rust ingestors: TRADE, QUOTE and coalesced BOOK.  It never
turns catalog BAR rows into native subscriptions, changes authority, creates a
symbol worker, or edits a Compose environment file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
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
)


CONFIRM = "REFRESH_QDL_V2_NATIVE_INGESTOR_RUNTIME"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
TARGETS = {
    "binance-usdm": "ingestor-binance-usdm.json",
    "okx-swap": "ingestor-okx-swap.json",
}
REALTIME_FEEDS = frozenset({"TRADE", "QUOTE", "BOOK"})


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(value), indent=2, sort_keys=True, separators=(",", ": "))
        + "\n"
    ).encode("utf-8")


def _read_json(path: Path, *, field: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{field} is unreadable: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def _binding_key(binding: Mapping[str, Any]) -> tuple[str, str]:
    feed = binding.get("feed")
    subscription = binding.get("subscription_id")
    if not isinstance(feed, str) or feed not in REALTIME_FEEDS:
        raise ValueError("native ingestor binding feed is invalid")
    if not isinstance(subscription, str) or not subscription:
        raise ValueError("native ingestor binding subscription_id is invalid")
    return feed, subscription


def _binding_map(
    bindings: object,
    *,
    field: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} bindings are invalid")
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for item in bindings:
        if not isinstance(item, dict):
            raise ValueError(f"{field} binding is not an object")
        key = _binding_key(item)
        if key in result:
            raise ValueError(f"{field} contains duplicate physical subscription")
        result[key] = dict(item)
    return result


def _without_catalog_revision(binding: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(binding)
    result.pop("instrument_catalog_revision", None)
    return result


def _non_binding_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result.pop("bindings", None)
    result.pop("config_revision", None)
    return result


def _expected_config(
    *,
    lane: str,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    generated = acquisition.native_ingestor_configs(catalog=catalog, authority=authority)
    payload = generated.get(lane)
    if not isinstance(payload, dict):
        raise ValueError(f"catalog has no native runtime lane: {lane}")
    bindings = payload.get("bindings")
    if not isinstance(bindings, list):
        raise ValueError(f"generated {lane} bindings are invalid")
    unsupported = {
        item.get("feed")
        for item in bindings
        if isinstance(item, dict) and item.get("feed") not in REALTIME_FEEDS | {"BAR"}
    }
    if unsupported:
        raise ValueError(f"generated {lane} has unsupported native feed(s): {sorted(unsupported)}")
    realtime = [dict(item) for item in bindings if isinstance(item, dict) and item.get("feed") in REALTIME_FEEDS]
    if not realtime:
        raise ValueError(f"generated {lane} has no realtime bindings")
    result = dict(payload)
    result["bindings"] = realtime
    return result


def _validate_transition(
    *,
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    target: str,
) -> dict[str, Any]:
    if _non_binding_fields(active) != _non_binding_fields(expected):
        raise ValueError(f"{target} changes a non-binding runtime field")

    active_bindings = _binding_map(active.get("bindings"), field=f"active {target}")
    expected_bindings = _binding_map(expected.get("bindings"), field=f"expected {target}")
    active_by_feed = {
        feed: {key: item for key, item in active_bindings.items() if key[0] == feed}
        for feed in REALTIME_FEEDS
    }
    expected_by_feed = {
        feed: {key: item for key, item in expected_bindings.items() if key[0] == feed}
        for feed in REALTIME_FEEDS
    }

    for feed in ("TRADE", "QUOTE"):
        if set(active_by_feed[feed]) != set(expected_by_feed[feed]):
            raise ValueError(f"{target} changes {feed} subscription membership")
        for key in active_by_feed[feed]:
            if _without_catalog_revision(active_by_feed[feed][key]) != _without_catalog_revision(expected_by_feed[feed][key]):
                raise ValueError(f"{target} changes {feed} binding contract")

    if not set(active_by_feed["BOOK"]).issubset(expected_by_feed["BOOK"]):
        raise ValueError(f"{target} removes an existing BOOK subscription")
    for key, active_binding in active_by_feed["BOOK"].items():
        expected_binding = expected_by_feed["BOOK"][key]
        if _without_catalog_revision(active_binding) != _without_catalog_revision(expected_binding):
            raise ValueError(f"{target} changes an existing BOOK binding contract")

    added_books = [
        item
        for key, item in expected_by_feed["BOOK"].items()
        if key not in active_by_feed["BOOK"]
    ]
    return {
        "before_binding_count": len(active_bindings),
        "after_binding_count": len(expected_bindings),
        "before_config_revision": active.get("config_revision"),
        "after_config_revision": expected.get("config_revision"),
        "before_book_symbols": sorted(
            str(item["native_symbol"]) for item in active_by_feed["BOOK"].values()
        ),
        "after_book_symbols": sorted(
            str(item["native_symbol"]) for item in expected_by_feed["BOOK"].values()
        ),
        "added_book_symbols": sorted(str(item["native_symbol"]) for item in added_books),
        "added_book_subscription_ids": sorted(
            str(item["subscription_id"]) for item in added_books
        ),
    }


def _atomic_replace(path: Path, content: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.qdl-native-ingestor-{os.getpid()}-{time.time_ns()}")
    try:
        with open(temporary, "xb") as handle:
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

    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    pending: dict[str, tuple[Path, bytes, bytes, int, dict[str, Any]]] = {}
    for lane, file_name in TARGETS.items():
        path = runtime_dir / file_name
        active_bytes = path.read_bytes()
        active = _read_json(path, field=f"active {file_name}")
        expected = _expected_config(
            lane=lane,
            catalog=catalog,
            acquisition=acquisition,
            authority=authority,
        )
        change = _validate_transition(active=active, expected=expected, target=file_name)
        expected_bytes = _canonical_bytes(expected)
        pending[file_name] = (
            path,
            active_bytes,
            expected_bytes,
            path.stat().st_mode & 0o777,
            change,
        )

    files = {
        name: {
            "before_sha256": _sha256(active),
            "after_sha256": _sha256(expected),
            "changed": active != expected,
            **change,
        }
        for name, (_path, active, expected, _mode, change) in pending.items()
    }
    result: dict[str, Any] = {
        "schema": "qdl.v2.native-ingestor-runtime-refresh.v1",
        "status": "APPLIED" if apply else "DRY_RUN",
        "runtime_dir": str(runtime_dir),
        "authority_sha256": _sha256(authority_bytes),
        "authority_bytes_preserved": True,
        "catalog_revision": catalog.catalog_revision,
        "acquisition_revision": acquisition.revision,
        "files": files,
        "production_mutations": 0 if not apply else sum(
            1 for value in files.values() if value["changed"]
        ),
    }
    if not apply:
        return result

    assert output_dir is not None
    rollback_dir = output_dir / "rollback"
    output_dir.mkdir(parents=True, mode=0o700)
    rollback_dir.mkdir(mode=0o700)
    try:
        for file_name, (_path, active, _expected, _mode, _change) in pending.items():
            backup = rollback_dir / file_name
            backup.write_bytes(active)
            backup.chmod(0o640)

        applied: list[str] = []
        try:
            for file_name, (path, _active, expected, mode, _change) in pending.items():
                if files[file_name]["changed"]:
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
    except Exception:
        # Retain the exact backup if an apply starts but does not finish.
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")
    result = refresh(
        runtime_dir=args.runtime_dir,
        output_dir=args.output_dir,
        apply=args.apply,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

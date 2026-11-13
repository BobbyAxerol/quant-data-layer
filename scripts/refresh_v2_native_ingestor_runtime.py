#!/usr/bin/env python3
"""Safely materialize additive realtime bindings for the shared ingestors.

The stable BAR edge owns every final BAR bootstrap and recurring provider poll.
This compiler intentionally refreshes only the physical realtime inputs of the
two existing shared Rust ingestors: TRADE, QUOTE, MARK_INDEX and coalesced BOOK. It never
turns catalog BAR rows into native subscriptions, changes authority, creates a
symbol worker, or edits a Compose environment file.

The refresh is strictly additive (v2.2.6):

* every active binding is kept byte for byte, including its materialized
  ``instrument_catalog_revision``. The Rust core quarantines a raw record whose
  catalog revision differs from its own binding (``qdl-realtime-core`` lib.rs
  ``instrument catalog revision mismatch``), so rematerializing a retained
  binding would quarantine it for as long as the ingestor and the three cores
  are not switched at the same instant. New bindings carry the current catalog
  revision, and the cores receive the same additions;
* active BAR bindings (native OKX bars) are retained verbatim and never
  generated here;
* a realtime binding may be added, never removed or changed;
* non-binding runtime fields the generator does not produce (for example the
  operator-installed ``book_resync``) are preserved; a field both produce must
  be equal; ``config_revision`` advances to the acquisition revision only
  when a binding is added, and a file without additions is left untouched;
* OKX lane names are positional (``okx-<service>-<index>``) and the L2 book
  core compares bare connection generations, so a refresh that would move an
  existing lane to another feed (renumbering) is refused.
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
from qdl.runtime.core_binding_identity import native_ingestor_binding_identity


CONFIRM = "REFRESH_QDL_V2_NATIVE_INGESTOR_RUNTIME"
DEFAULT_STATE_ROOT = Path("/home/bobby/.local/state/qdl-v2")
TARGETS = {
    "binance-usdm": "ingestor-binance-usdm.json",
    "okx-swap": "ingestor-okx-swap.json",
}
REALTIME_FEEDS = frozenset({"TRADE", "QUOTE", "MARK_INDEX", "BOOK"})


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


RETAINED_ONLY_FEEDS = frozenset({"BAR"})
# partition_feed_lanes() order in rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs
LANE_FEED_ORDER = ("BOOK", "BAR", "TRADE", "QUOTE", "MARK_INDEX")
OKX_PUBLIC_CHANNELS = frozenset({"trades", "bbo-tbt", "books", "mark-price", "index-tickers"})


def _binding_key(binding: Mapping[str, Any]) -> tuple[str, ...]:
    feed = binding.get("feed")
    if not isinstance(feed, str) or feed not in REALTIME_FEEDS | RETAINED_ONLY_FEEDS:
        raise ValueError("native ingestor binding feed is invalid")
    # OKX MARK_INDEX is a paired physical input under one logical source ID;
    # the native channel is part of the physical subscription identity.
    return native_ingestor_binding_identity(binding, field="native ingestor binding")


def _binding_map(
    bindings: object,
    *,
    field: str,
) -> dict[tuple[str, ...], dict[str, Any]]:
    if not isinstance(bindings, list) or not bindings:
        raise ValueError(f"{field} bindings are invalid")
    result: dict[tuple[str, ...], dict[str, Any]] = {}
    for item in bindings:
        if not isinstance(item, dict):
            raise ValueError(f"{field} binding is not an object")
        key = _binding_key(item)
        if key in result:
            raise ValueError(f"{field} contains duplicate physical subscription")
        result[key] = dict(item)
    return result


def _without_catalog_revision(binding: Mapping[str, Any]) -> dict[str, Any]:
    """A binding without its lineage fields (catalogue and, since DL v2.3.0 2d, config revision)."""
    result = dict(binding)
    result.pop("instrument_catalog_revision", None)
    result.pop("config_revision", None)
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


def _okx_lanes(config: Mapping[str, Any]) -> list[tuple[str, str]]:
    """(lane name, feed) in the Rust ingestor's order: public then business,
    feed lanes Book/Bar/Trade/Quote/MarkIndex, chunked by the per-connection cap."""
    cap = config.get("max_subscriptions_per_connection", 100)
    if not isinstance(cap, int) or not 0 < cap <= 1024:
        raise ValueError("max_subscriptions_per_connection is invalid")
    lanes: list[tuple[str, str]] = []
    bindings = [item for item in config.get("bindings") or [] if isinstance(item, dict)]
    for service, members in (
        ("public", [b for b in bindings if b.get("native_channel") in OKX_PUBLIC_CHANNELS]),
        ("business", [b for b in bindings if b.get("native_channel") not in OKX_PUBLIC_CHANNELS]),
    ):
        index = 0
        for feed in LANE_FEED_ORDER:
            count = sum(1 for b in members if b.get("feed") == feed)
            for _ in range(0, count, cap):
                index += 1
                lanes.append((f"okx-{service}-{index:03d}", feed))
    return lanes


def _validate_transition(
    *,
    active: Mapping[str, Any],
    expected: Mapping[str, Any],
    target: str,
    pin_config_lineage: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (successor config, change summary) for one ingestor file.

    ``pin_config_lineage`` (DL v2.3.0 ingestor binaries only; older ones refuse
    the per-binding field) stamps every retained binding with the config
    revision it was acquired at, so raising the file's ``config_revision`` for
    additions does not change the contract lineage of untouched bindings.
    """
    active_fields = _non_binding_fields(active)
    expected_fields = _non_binding_fields(expected)
    if set(expected_fields) - set(active_fields):
        raise ValueError(f"{target} changes a non-binding runtime field")
    if any(active_fields[key] != value for key, value in expected_fields.items()):
        raise ValueError(f"{target} changes a non-binding runtime field")

    active_bindings = _binding_map(active.get("bindings"), field=f"active {target}")
    expected_bindings = _binding_map(expected.get("bindings"), field=f"expected {target}")
    for key, active_binding in active_bindings.items():
        if key[0] in RETAINED_ONLY_FEEDS:
            continue
        expected_binding = expected_bindings.get(key)
        if expected_binding is None:
            raise ValueError(f"{target} removes an existing {key[0]} subscription")
        if _without_catalog_revision(active_binding) != _without_catalog_revision(expected_binding):
            raise ValueError(f"{target} changes an existing {key[0]} binding contract")

    added = [item for key, item in expected_bindings.items() if key not in active_bindings]
    successor = dict(active)
    if added:
        revision = expected.get("config_revision")
        if not isinstance(revision, int) or revision <= int(active.get("config_revision") or 0):
            raise ValueError(f"{target} additions require a higher config_revision")
        previous = int(active.get("config_revision") or 0)
        retained = [dict(item) for item in active["bindings"]]
        if pin_config_lineage:
            for item in retained:
                item.setdefault("config_revision", previous)
        successor["bindings"] = retained + [dict(item) for item in added]
        successor["config_revision"] = revision
    if target == TARGETS["okx-swap"]:
        before, after = _okx_lanes(active), _okx_lanes(successor)
        if after[: len(before)] != before:
            raise ValueError(f"{target} renames an existing OKX lane (generation fence): {before} -> {after}")
    by_feed = lambda items: {
        feed: sorted({str(item["native_symbol"]) for item in items if item.get("feed") == feed})
        for feed in sorted(REALTIME_FEEDS)
    }
    added_by_feed = {feed: symbols for feed, symbols in by_feed(added).items() if symbols}
    change = {
        "before_binding_count": len(active_bindings),
        "after_binding_count": len(active_bindings) + len(added),
        "before_config_revision": active.get("config_revision"),
        "after_config_revision": successor.get("config_revision"),
        "added_binding_count": len(added),
        "added_symbols_by_feed": added_by_feed,
        "added_book_symbols": added_by_feed.get("BOOK", []),
        "added_subscription_ids": sorted({str(item["subscription_id"]) for item in added}),
        "retained_bindings_byte_identical": True,
    }
    if target == TARGETS["okx-swap"]:
        change["okx_lanes_before"] = [name for name, _feed in _okx_lanes(active)]
        change["okx_lanes_after"] = [name for name, _feed in _okx_lanes(successor)]
    return successor, change


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
        successor, change = _validate_transition(active=active, expected=expected, target=file_name)
        expected_bytes = _canonical_bytes(successor) if change["added_binding_count"] else active_bytes
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
            "before_mode": oct(mode),
            "changed": active != expected,
            **change,
        }
        for name, (_path, active, expected, mode, change) in pending.items()
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
        for file_name, (_path, active, _expected, mode, _change) in pending.items():
            backup = rollback_dir / file_name
            backup.write_bytes(active)
            backup.chmod(mode)

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

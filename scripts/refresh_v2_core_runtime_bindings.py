#!/usr/bin/env python3
"""Render additive successors of the three active V2 Rust-core configs (v2.2.6).

Purpose: when the catalog/acquisition plan gains realtime bindings (QUOTE, MARK_INDEX_PRICE, TRADE or BOOK), the
cores must know them before the ingestor emits them (``strict_subscription_scope``). This tool generates the core
bindings with the repository's ``StableAcquisitionPlan.core_config`` (all acquired bindings, the selection that
reproduces the deployed cores exactly) and writes, for each active core file, a successor that is

* every active binding byte for byte, including its materialized ``instrument_catalog_revision`` (the core
  quarantines a raw record whose catalog revision differs from its binding, ``qdl-realtime-core`` lib.rs
  "instrument catalog revision mismatch"; the ingestor refresh keeps retained revisions the same way), plus
* only the generated bindings the active file does not have, at the current catalog revision.

It refuses a removed binding, a retained binding whose contract (other than the catalog revision) differs, a changed
non-binding field (authority, topics, batch settings), or cores that would receive different additions.

Boundary: reads the repository config and the active core files; writes new files only under a new ``--output-dir``
(exclusive create). It never edits the mounted files, an image selector or a Compose file; the window mounts the
output directory. The narrower ``refresh_v2_l2_core_runtime.py`` (BOOK only, in place) and
``refresh_v2_rust_core_runtime.py`` (snapshot cadence only) are unchanged.

  python -B scripts/refresh_v2_core_runtime_bindings.py --core A.json --core B.json --core C.json --output-dir NEW
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(value), indent=2, sort_keys=True) + "\n").encode("utf-8")


def _identity(binding: Mapping[str, Any]) -> tuple[str, str, str | None]:
    # One OKX MARK_INDEX source has several physical inputs; the physical symbol tells them apart.
    return (str(binding["source_id"]), str(binding["native_channel"]), binding.get("physical_native_symbol"))


def _without_catalog_revision(binding: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(binding)
    result.pop("instrument_catalog_revision", None)
    return result


def successor(
    active: Mapping[str, Any],
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    name: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    core = active.get("core")
    if not isinstance(core, Mapping) or not isinstance(core.get("bindings"), list):
        raise ValueError(f"{name} is not a realtime-core config")
    generated = acquisition.core_config(catalog=catalog, authority=active["authority"])
    generated_core = generated["core"]
    if {k: v for k, v in generated_core.items() if k != "bindings"} != {
            k: v for k, v in core.items() if k != "bindings"}:
        raise ValueError(f"{name} changes a non-binding core field")
    active_map = {}
    for item in core["bindings"]:
        key = _identity(item)
        if key in active_map:
            raise ValueError(f"{name} has a duplicate core binding {key}")
        active_map[key] = item
    generated_map = {_identity(item): item for item in generated_core["bindings"]}
    for key, item in active_map.items():
        if key not in generated_map:
            raise ValueError(f"{name} would remove core binding {key}")
        if _without_catalog_revision(item) != _without_catalog_revision(generated_map[key]):
            raise ValueError(f"{name} changes the contract of core binding {key}")
    added = [dict(item) for key, item in generated_map.items() if key not in active_map]
    result = json.loads(json.dumps(active))
    result["core"]["bindings"] = [dict(item) for item in core["bindings"]] + added
    return result, added


def render(*, cores: list[Path], output_dir: Path,
           catalog_path: Path = ROOT / "config/v2/stable-source-bindings.yaml",
           acquisition_path: Path = ROOT / "config/v2/stable-acquisition-bindings.yaml") -> dict[str, Any]:
    if len(cores) != len({path.name for path in cores}) or not cores:
        raise ValueError("core files must be given once each, with distinct names")
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    pending, added_sets, files = [], set(), {}
    for path in cores:
        raw = path.read_bytes()
        doc, added = successor(json.loads(raw), catalog=catalog, acquisition=acquisition, name=path.name)
        added_sets.add(tuple(sorted(json.dumps(item, sort_keys=True) for item in added)))
        out = _canonical_bytes(doc)
        pending.append((path.name, out))
        kinds: dict[str, int] = {}
        for item in added:
            kinds[item["provider_kind"]] = kinds.get(item["provider_kind"], 0) + 1
        files[path.name] = {"before_sha256": _sha256(raw), "after_sha256": _sha256(out),
                            "retained": len(doc["core"]["bindings"]) - len(added), "added": len(added),
                            "added_by_provider_kind": dict(sorted(kinds.items())),
                            "added_catalog_revision": catalog.catalog_revision if added else None}
    if len(added_sets) != 1:
        raise ValueError("cores would receive different additions")
    output_dir.mkdir(parents=True, exist_ok=False, mode=0o750)
    for name, out in pending:
        with open(output_dir / name, "xb") as handle:
            handle.write(out)
    result = {"schema": "qdl.v2.core-runtime-bindings-refresh.v1", "status": "RENDERED_NOT_MOUNTED",
              "catalog_revision": catalog.catalog_revision, "acquisition_revision": acquisition.revision,
              "output_dir": str(output_dir), "files": files, "production_mutations": 0}
    (output_dir / "receipt.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--core", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(render(cores=args.core, output_dir=args.output_dir), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Source-only, idempotent retirement of expired research book entitlements.

Metadata is retained for historical interpretation. No replacement contract is
inferred from a ticker or date. Runtime bundles are never edited by this tool.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml

BOOKS = {"BOOK_SNAPSHOT", "BOOK_DELTA"}


def retire(catalog, acquisition, scope, manifests, *, as_of_ns):
    if type(as_of_ns) is not int or as_of_ns <= 0:
        raise ValueError("positive explicit as_of_ns required")
    expired = {i["instrument_uid"]: i for i in catalog["instruments"]
               if i.get("expiry_time_ns") is not None and int(i["expiry_time_ns"]) <= as_of_ns}
    targets = [b for b in catalog["bindings"]
               if b["instrument_uid"] in expired and b["feed"] in BOOKS]
    ids = {b["binding_id"] for b in targets}
    uids = {b["instrument_uid"] for b in targets}
    output_manifests = deepcopy(manifests)
    for name, manifest in output_manifests.items():
        requirements = manifest["spec"]["requirements"]
        removing = [r for r in requirements if r["instrument_uid"] in uids and r["feed"] in BOOKS]
        if any(r["consumer_grade"] != "RESEARCH" for r in removing):
            raise ValueError(f"expired execution/alpha demand requires separate migration: {name}")
        if removing:
            manifest["spec"]["requirements"] = [r for r in requirements if r not in removing]
            manifest["metadata"]["revision"] += 1
    c, a, s = deepcopy(catalog), deepcopy(acquisition), deepcopy(scope)
    for document, field, revision in ((c, "bindings", "catalog_revision"),
                                       (a, "bindings", "revision")):
        kept = [b for b in document[field] if b["binding_id"] not in ids]
        if len(kept) != len(document[field]):
            document[field] = kept
            document[revision] += 1
    kept = [i for i in s["binding_ids"] if i not in ids]
    if kept != s["binding_ids"]:
        s["binding_ids"] = kept
        s["revision"] += 1
    receipt = {"as_of_ns": as_of_ns, "retired_binding_ids": sorted(ids),
               "metadata_preserved": c["instruments"] == catalog["instruments"],
               "replacement_contracts_added": 0,
               "retired_instruments": [{"instrument_uid": uid,
                   "native_symbol": expired[uid]["native_symbol"],
                   "expiry_time_ns": expired[uid]["expiry_time_ns"]} for uid in sorted(uids)]}
    return c, a, s, output_manifests, receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--as-of-ns", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    if not (root / "AGENTS.md").is_file() or not (root / "qdl").is_dir():
        raise SystemExit("source checkout required, never a runtime bundle")
    paths = [root / "config/v2" / name for name in (
        "stable-source-bindings.yaml", "stable-acquisition-bindings.yaml",
        "stable-authority-promotion-scope.yaml")]
    manifest_paths = sorted((root / "consumers/stable").glob("*.yaml"))
    manifests = {str(p.relative_to(root)): yaml.safe_load(p.read_text()) for p in manifest_paths}
    manifests = {k:v for k,v in manifests.items() if "requirements" in v.get("spec", {})}
    original = [yaml.safe_load(p.read_text()) for p in paths]
    c, a, s, updated, receipt = retire(*original, manifests, as_of_ns=args.as_of_ns)
    changes = [(p, value) for p, old, value in zip(paths, original, (c,a,s), strict=True) if old != value]
    changes += [(root / k, v) for k, v in updated.items() if v != manifests[k]]
    from scripts.phasec36_materialize_reference_l2 import _atomic_write_many, _yaml_bytes
    from scripts.phase115c_materialize_active_native_bars import _update_release_route
    route_path = root / "config/v2/stable-v2-release-routing.yaml"
    route = yaml.safe_load(route_path.read_text())
    next_route = _update_release_route(route, summary={
        "source_catalog_sha256": hashlib.sha256(_yaml_bytes(c)).hexdigest(),
        "catalog_revision": c["catalog_revision"],
        "demand_sha256": route["crypto_demand"]["sha256"],
        "demand_revision": route["crypto_demand"]["revision"],
    })
    if next_route != route:
        changes.append((route_path, next_route))
    if args.apply:
        _atomic_write_many(tuple(changes))
    print(json.dumps({**receipt, "applied": args.apply,
                      "changed_files": [str(p.relative_to(root)) for p,_ in changes]}, sort_keys=True))


if __name__ == "__main__":
    main()

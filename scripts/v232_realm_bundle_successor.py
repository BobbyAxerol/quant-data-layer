#!/usr/bin/env python3
"""DL v2.3.2 rollout P2: the realm (and kn) bundle successor that adds `delivery_semantics`, nothing else.

Purpose: v2.3.2 Streams mark opted-in ON_CHANGE QUOTE records DELIVERY_ON_CHANGE from the gateway bundle's
`delivery_semantics` field (`kn_gateway_bundle.py`). The live realm must not be re-rendered from older declaration
inputs: stage C r8 (2026-11-17) is the current declaration. This tool takes the deployed r8 bundle as the base,
re-compiles only its `catalog` from the same r8 catalogue with the v2.3.2 compiler (exactly how
`scripts/dl_declaration.py build_bundles` replaces the catalogue), keeps every manifest byte-equal, and refuses unless
the only difference is `delivery_semantics: ON_CHANGE` added to QUOTE bindings.

Boundary: reads files, writes one output file and prints a JSON gate report. No runtime access.

  python -B scripts/v232_realm_bundle_successor.py --base <deployed bundle.json> \
      --catalog <r8 stable-source-bindings.yaml> --out <new bundle.json>
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import kn_gateway_bundle  # noqa: E402

ADDED_FIELD = "delivery_semantics"


def successor(base: dict, compiled_catalog: dict) -> tuple[dict, dict]:
    """The successor body and the gate report; raises ValueError on any difference beyond the added field."""
    if kn_gateway_bundle.bundle_sha256(base) != base.get("sha256"):
        raise ValueError("base bundle hash does not verify")
    old = {b["binding_id"]: b for b in base["catalog"]["bindings"]}
    new = {b["binding_id"]: b for b in compiled_catalog["bindings"]}
    if set(old) != set(new):
        raise ValueError(f"binding sets differ: -{len(set(old) - set(new))} +{len(set(new) - set(old))}")
    for key in ("catalog_revision", "source_policy_revision", "canonical_stream"):
        if base["catalog"].get(key) != compiled_catalog.get(key):
            raise ValueError(f"catalog {key} differs")
    marked, by_feed = [], {}
    for binding_id, entry in new.items():
        rest = {k: v for k, v in entry.items() if k != ADDED_FIELD}
        if rest != old[binding_id]:
            raise ValueError(f"binding {binding_id} differs beyond {ADDED_FIELD}")
        if ADDED_FIELD in entry:
            if entry[ADDED_FIELD] != "ON_CHANGE" or entry["feed"] != "QUOTE":
                raise ValueError(f"binding {binding_id}: {ADDED_FIELD}={entry[ADDED_FIELD]} on {entry['feed']}")
            if ADDED_FIELD in old[binding_id]:
                continue
            marked.append(binding_id)
            key = f"{entry['venue']}/{entry['market']}"
            by_feed[key] = by_feed.get(key, 0) + 1
    body = {k: v for k, v in base.items() if k != "sha256"}
    body["catalog"] = {**base["catalog"], "bindings": compiled_catalog["bindings"]}
    body["sha256"] = kn_gateway_bundle.bundle_sha256(body)
    report = {"base_sha256": base["sha256"], "sha256": body["sha256"], "bindings": len(new),
              "marked_on_change": len(marked), "marked_by_venue_market": dict(sorted(by_feed.items())),
              "manifests_unchanged": body["manifests"] == base["manifests"], "status": "PASS"}
    return body, report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--universe-dir", type=Path, default=None)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    base = json.loads(args.base.read_text())
    compiled = kn_gateway_bundle.compile_bundle(environment=base["environment"], catalog_path=args.catalog,
                                                manifest_paths=[], universe_dir=args.universe_dir)
    try:
        body, report = successor(base, compiled["catalog"])
    except ValueError as error:
        print(json.dumps({"status": "FAIL", "reason": str(error)}))
        return 2
    args.out.write_bytes((json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode())
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

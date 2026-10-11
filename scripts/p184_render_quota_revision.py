#!/usr/bin/env python3
"""Render a consumer manifest revision that only RAISES quotas (P18.4 D3 window B: alpha capacity100 candidate).

Takes the deployed manifest YAML and writes a new file in which only `metadata.revision` (+1) and the named quotas
(`requests_per_minute`, `max_streams`, upward only) change. It refuses anything else and validates the result with the
existing ConsumerManifestLoader. The caller then compiles the gateway bundle with scripts/kn_gateway_bundle.py and
proves parity, as in D1. Pure file transform, never overwrites, no runtime access."""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

import yaml

from qdl.consumer.manifest import ConsumerManifestLoader

QUOTA_KEYS = frozenset({"requests_per_minute", "max_streams"})


def render(manifest: dict, quotas: dict[str, int]) -> dict:
    if not quotas or set(quotas) - QUOTA_KEYS:
        raise ValueError("only requests_per_minute/max_streams may change")
    current = manifest["spec"]["quotas"]
    if any(type(v) is not int or v <= current[k] for k, v in quotas.items()):
        raise ValueError("quota change must raise the named quotas")
    target = copy.deepcopy(manifest)
    target["metadata"]["revision"] += 1
    target["spec"]["quotas"].update(quotas)
    ConsumerManifestLoader.from_mapping(target)
    diff = _diff(manifest, target)
    expected = {("metadata", "revision")} | {("spec", "quotas", k) for k in quotas}
    if set(diff) != expected:
        raise ValueError(f"unexpected manifest difference: {sorted(diff)}")
    return target


def _diff(a, b, path=()):
    if isinstance(a, dict) and isinstance(b, dict):
        out = {}
        for key in set(a) | set(b):
            out.update(_diff(a.get(key), b.get(key), path + (key,)))
        return out
    return {} if a == b else {path: (a, b)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--requests-per-minute", type=int)
    parser.add_argument("--max-streams", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    quotas = {k: v for k, v in (("requests_per_minute", args.requests_per_minute),
                                ("max_streams", args.max_streams)) if v is not None}
    source = yaml.safe_load(args.manifest.read_text())
    target = render(source, quotas)
    with args.output.open("x") as handle:
        handle.write(yaml.safe_dump(target, sort_keys=False))
    parsed = ConsumerManifestLoader.from_mapping(target)
    print(json.dumps({"consumer_id": parsed.consumer_id, "revision": parsed.manifest_revision,
                      "quotas": target["spec"]["quotas"], "manifest_sha256": parsed.manifest_sha256}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

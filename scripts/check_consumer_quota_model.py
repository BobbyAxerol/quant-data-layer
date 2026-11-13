#!/usr/bin/env python3
"""Offline gate: refuse an EXECUTION consumer manifest whose quotas are below its modelled request/stream demand.

Purpose: the v2.2.6 stage A window (2026-11-13) failed on `RATE_LIMITED: consumer request quota is exhausted` because
Paper r12 granted 4,500 requests/min for a modelled steady demand of about 6,000/min. This gate runs on every
EXECUTION-grade manifest under consumers/stable (or on the paths given) before a manifest is committed or mounted.
The model and its measured calibration are in qdl/consumer/quota_model.py.

Boundary: reads manifest YAML files; prints one JSON verdict; exit 0 PASS, 1 FAIL. No runtime access.

  python3 -B scripts/check_consumer_quota_model.py [manifest.yaml ...]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.consumer.quota_model import is_execution_manifest, model_manifest, quota_shortfall  # noqa: E402


def check(paths: list[Path]) -> dict:
    results = {}
    for path in paths:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not is_execution_manifest(doc):
            continue
        short = quota_shortfall(doc)
        results[str(path)] = {"status": "FAIL" if short else "PASS",
                              "quotas": doc["spec"]["quotas"], **(short or {"model": model_manifest(doc).evidence()})}
    status = "FAIL" if any(r["status"] == "FAIL" for r in results.values()) else "PASS"
    return {"schema": "qdl.v2.consumer-quota-gate.v1", "status": status, "manifests": results}


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    paths = [Path(a) for a in args] or sorted((ROOT / "consumers/stable").glob("*.yaml"))
    verdict = check(paths)
    print(json.dumps(verdict, indent=2, sort_keys=True))
    return 0 if verdict["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

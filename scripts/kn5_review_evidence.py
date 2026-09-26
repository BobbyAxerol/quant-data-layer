"""Derive an honest KN review receipt without modifying archived evidence.

Offline only. Latency is in ms; no gate, deployment or release is authorized.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path


def distribution(values):
    ordered = sorted(float(v) for v in values)
    if any(not math.isfinite(v) or v < 0 for v in ordered):
        raise ValueError("latency samples must be finite nonnegative milliseconds")
    n = len(ordered)
    pick = lambda q: round(ordered[math.ceil(q * n) - 1], 3) if n else None
    return {"n": n, "p50_ms": pick(.5), "p95_ms": pick(.95) if n >= 20 else None,
            "p99_ms": pick(.99) if n >= 100 else None, "max_ms": pick(1)}


def qualify_summary(value):
    result = deepcopy(value)
    n = result["n"]
    if n < 100:
        result["p99"] = None
    if n < 20:
        result["p95"] = None
    result["unit"] = "ms"
    result["basis"] = "archived summary, success-only; raw samples unavailable here"
    return result


def http_outcome(row):
    result = dict(row)
    if "data-quality/gaps" in str(row.get("operation")):
        complete = row.get("http") == 200 and row.get("code") is None
        result.update(status="COMPLETE" if complete else "PARTIAL_NOT_COMPLETE",
                      scan_complete=complete,
                      bounded_fail_closed=row.get("http") in {206, 409, 503})
    return result


def build_report(matrix, stage, consumer):
    snapshot = next(r for r in consumer["results"] if r.get("phase") == "A_snapshots_per_replica")
    feeds = {}
    for feed, row in snapshot["by_feed"].items():
        feeds[feed] = {"attempts": row["reads"], "usable": row["reads"] - row["failed"],
                      "refused": row["failed"],
                      "usable_ratio": (row["reads"] - row["failed"]) / row["reads"],
                      "timings": {k: qualify_summary(v) for k, v in row.items() if isinstance(v, dict) and "n" in v}}
    return {"schema": "qdl.kn5.predeploy-review.v1", "status": "CORRECTED_ARCHIVED_EVIDENCE_NOT_NEW_CERTIFICATE",
            "unit": "ms", "http": [http_outcome(r) for r in matrix["sections"]["http"]],
            "stage_sdk_latency": {k: distribution(v) for k, v in stage["latency_series"].items()},
            "history": matrix["sections"].get("history", []),
            "batch": matrix["sections"].get("batch", []),
            "consumer_snapshot": {"attempts": snapshot["reads"], "refused": snapshot["failed"],
                                  "products": snapshot["products"], "products_ok_on_both_replicas": snapshot["products_ok_on_both_replicas"],
                                  "feeds": feeds, "per_binding_replica": snapshot["per_binding_replica"],
                                  "boundary": snapshot["timing_basis"]},
            "interpretation": {
                "refused_trade": "Session LIVE does not make an old last trade an eligible execution price. No freshness relaxation or quote masquerading as TRADE.",
                "market_orders": "Explicit fresh QUOTE/L2 reference plus Risk read-back, not a candle close or old trade; fills remain venue-authoritative.",
                "cold_latency": "History and universe batch completion are separate from hot execution-price reads. Existing small samples are not p99 or whole-universe capacity.",
                "end_to_end": "KN4 TS stream evidence ends at projector callback, not completed TS Redis write. Actual consumer cache-write latency remains to be measured in KN5.",
                "runtime": "No deploy/release or order authority is granted by this report."}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    names = ("kn4-matrix-200716/receipt.json", "stage-35-064645/receipt.json", "cmatrix-065612/ts60.json")
    paths = [args.evidence_root / name for name in names]
    if args.output.exists() or args.output.resolve() in {p.resolve() for p in paths}:
        raise ValueError("output must be a new additive receipt")
    encoded = [p.read_bytes() for p in paths]
    report = build_report(*(json.loads(raw) for raw in encoded))
    report["inputs"] = [{"path": str(p), "sha256": hashlib.sha256(raw).hexdigest()} for p, raw in zip(paths, encoded)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                      "status": report["status"], "runtime_mutations": 0}))

if __name__ == "__main__":
    main()

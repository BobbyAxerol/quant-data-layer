#!/usr/bin/env python3
"""Freeze the P18.4 stage-100 budget from a baseline (owner rule 2026-10-09), BEFORE any candidate load.

Inputs:
- the certified v2.1.1 target budget (latency classes, request/stream/startup gates);
- the baseline stage-50 target receipt (cold reads 2500/5000 per venue);
- the G4 observer summaries of the baseline windows.

Output: config/v2/p184-stage100-budget.json (schema qdl.p184.stage100-budget.v1):
- P18.4 stages 20/50/100;
- G5 COLD_<rows> limit = max(baseline cold ms over both venues) x 1.5;
- G4 table: candidate p95 limit = baseline p95 x 1.2 per feed/venue(/interval);
- TS demanded routes 64.
Pure file transform: reads JSON, writes one new file (never overwrites), no runtime access."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from qdl.certification.phase3_consumer_load import P184_STAGE_MIX, P184_STAGE_SECONDS, load_p184_stage100_budget

G5_FACTOR = 1.5
G4_FACTOR = 1.2


def freeze(base: dict, receipt: dict, g4_summaries: list[dict], *, ts_routes: int, sources: dict) -> dict:
    cold = [item for item in receipt["cold"] if not item.get("error") and isinstance(item.get("ms"), (int, float))]
    shapes = {}
    for rows in (2500, 5000):
        values = [item["ms"] for item in cold if item.get("rows") == rows]
        if not values:
            raise ValueError(f"baseline has no successful COLD_{rows} read; cannot freeze G5")
        shapes[f"COLD_{rows}"] = {"p95": round(max(values) * G5_FACTOR, 1), "baseline_max_ms": max(values),
                                  "baseline_n": len(values), "rule": "baseline max x 1.5 (n per venue small)"}
    g4 = {}
    for summary in g4_summaries:
        for key, value in summary.items():
            vis = value.get("visibility_ms")
            if not vis:
                continue
            g4.setdefault(key, []).append(vis["p95"])
    g4_table = {key: {"baseline_p95_ms_windows": values, "baseline_p95_ms": max(values),
                      "candidate_p95_limit_ms": round(max(values) * G4_FACTOR, 1)}
                for key, values in sorted(g4.items())}
    budget = {
        **{k: v for k, v in base.items() if k not in {"schema", "frozen_at", "stages", "source", "note"}},
        "schema": "qdl.p184.stage100-budget.v1",
        "frozen_before_candidate": True,
        "note": "Frozen from the baseline before any candidate load (owner 2026-10-09). A missed limit is an open exit "
                "to optimize, never a threshold edited after results.",
        "stages": {str(k): {"mix": list(P184_STAGE_MIX[k]), "seconds": P184_STAGE_SECONDS[k]} for k in P184_STAGE_MIX},
        "g5_shape_limits_ms": shapes,
        "g4_delivery_visibility": {"rule": "candidate p95 <= baseline p95 x 1.2 per feed/venue(/interval); "
                                           "read_committed observation is a visibility upper bound",
                                   "table": g4_table},
        "trading_system": {**base["trading_system"], "demanded_routes": ts_routes},
        "sources": sources,
    }
    load_p184_stage100_budget(budget)
    return budget


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", type=Path, default=Path("config/v2/v211-target-acceptance-budget.json"))
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--g4", type=Path, nargs="+", required=True, help="observer --summarize JSON files")
    parser.add_argument("--ts-routes", type=int, default=64)
    parser.add_argument("--output", type=Path, default=Path("config/v2/p184-stage100-budget.json"))
    args = parser.parse_args()
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    sources = {"base": digest(args.base), "receipt": digest(args.receipt), "g4": [digest(p) for p in args.g4]}
    budget = freeze(json.loads(args.base.read_text()), json.loads(args.receipt.read_text()),
                    [json.loads(p.read_text()) for p in args.g4], ts_routes=args.ts_routes, sources=sources)
    with args.output.open("x") as handle:
        json.dump(budget, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "g5": budget["g5_shape_limits_ms"],
                      "g4_rows": len(budget["g4_delivery_visibility"]["table"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

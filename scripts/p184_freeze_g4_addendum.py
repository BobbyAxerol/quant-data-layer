#!/usr/bin/env python3
"""Freeze the P18.4 stage-100 G4 addendum from a dedicated baseline run, BEFORE any candidate (owner 2026-10-11).

Purpose. The frozen budget config/v2/p184-stage100-budget.json has 17 G4 rows; candidate campaigns also observe bar
rows (5m, 15m, 1h, 2h, 4h, 6h) and the four OKX_INVERSE rows that have no frozen baseline, so G4 stayed NOT_MEASURED.
This tool writes a SEPARATE file, config/v2/p184-stage100-g4-addendum.json (schema qdl.p184.stage100-g4-addendum.v1),
once (open "x", never overwrites). The original budget stays byte-identical.

Row set: the owner's closed set (scripts/p184_gate_campaign.py G4_ADDENDUM_OWNER_ROWS) minus the keys already in the
frozen table. Rule per row, from the baseline observer windows only:
- bar 1h/2h/4h/6h: REPORT_ONLY (never gate, never fail, never pass the gate);
- otherwise only windows with >= 5 samples count; none -> REPORT_ONLY ("baseline < 5 samples per window");
- else GATED: candidate_max_limit_ms = max over those windows of the window max x 1.2 (max, not p95).

Input: a campaign directory produced by `p184_gate_campaign.py run --baseline` (campaign.json role=baseline and the
completion marker baseline.json). Any other directory, a candidate campaign above all, is refused: measured candidate
numbers never become a baseline. The baseline must have run with the same frozen budget file (sha256).

Pure file transform: reads JSON, writes one new file, no runtime access.
usage: p184_freeze_g4_addendum.py --baseline-campaign DIR [--budget PATH] [--output PATH]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import p184_gate_campaign as gc  # noqa: E402
from scripts.p184_resp_readonly import read_jsonl  # noqa: E402


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def baseline_windows(campaign_dir: Path) -> tuple[dict, list[dict], dict]:
    """(campaign.json, [{'index', 'summary', 'inverse_rows'}], source hashes); refuses anything but a finished
    role=baseline campaign."""
    d = Path(campaign_dir)
    campaign = gc.load_json(d / "campaign.json")
    if not isinstance(campaign, dict):
        raise ValueError(f"{d}: no campaign.json")
    if campaign.get("role") != "baseline":
        raise ValueError(f"{d}: not a baseline campaign (role={campaign.get('role')!r}); candidate measurements never "
                         "become a baseline")
    marker = gc.load_json(d / "baseline.json")
    if not isinstance(marker, dict) or marker.get("role") != "baseline":
        raise ValueError(f"{d}: baseline run not complete (no baseline.json)")
    windows, sources = [], {"campaign.json": sha256(d / "campaign.json"), "baseline.json": sha256(d / "baseline.json")}
    for item in campaign.get("g4_windows", []):
        summary_path = d / f"g4-{item['index']}-summary.json"
        summary = gc.load_json(summary_path)
        if not isinstance(summary, dict):
            continue
        obs = d / f"g4-{item['index']}" / "observations.jsonl"
        inverse = gc.g4_inverse_rows(read_jsonl(obs)) if obs.exists() else {}
        sources[summary_path.name] = sha256(summary_path)
        if obs.exists():
            sources[f"g4-{item['index']}/observations.jsonl"] = sha256(obs)
        windows.append({"index": item["index"], "boundary": item.get("boundary"), "summary": summary,
                        "inverse_rows": inverse})
    if not windows:
        raise ValueError(f"{d}: no G4 window summary")
    return campaign, windows, sources


def freeze_rows(windows: list[dict], table: dict) -> dict:
    rows = {}
    for key in gc.G4_ADDENDUM_OWNER_ROWS:
        if key in table:
            continue
        stats = [gc.g4_window_stats(w, key) for w in windows]
        n = [v.get("n", 0) for v in stats]
        maxes = [v.get("max") if v.get("n") else None for v in stats]
        row = {"baseline_n_windows": n, "baseline_max_ms_windows": maxes}
        qualified = [m for m, k in zip(maxes, n) if k >= gc.G4_ADDENDUM_MIN_SAMPLES and m is not None]
        if gc.g4_row_interval(key) in gc.G4_REPORT_ONLY_INTERVALS:
            rows[key] = {**row, "status": "REPORT_ONLY", "reason": "interval report-only by owner rule"}
        elif not qualified:
            rows[key] = {**row, "status": "REPORT_ONLY",
                         "reason": f"baseline < {gc.G4_ADDENDUM_MIN_SAMPLES} samples per window"}
        else:
            rows[key] = {**row, "status": "GATED", "baseline_max_ms": max(qualified),
                         "candidate_max_limit_ms": round(max(qualified) * gc.G4_ADDENDUM_FACTOR, 1)}
    return rows


def freeze(campaign_dir: Path, budget_path: Path, *, now: float | None = None) -> dict:
    budget_path = Path(budget_path).resolve()
    budget = json.loads(budget_path.read_text())
    table = (budget.get("g4_delivery_visibility") or {}).get("table") or {}
    if not table or not budget.get("frozen_before_candidate"):
        raise ValueError("budget has no frozen G4 table")
    campaign, windows, sources = baseline_windows(campaign_dir)
    budget_sha = sha256(budget_path)
    if campaign.get("budget_sha256") != budget_sha:
        raise ValueError("the baseline ran with a different budget file than the one the addendum extends")
    now = time.time() if now is None else now
    addendum = {
        "schema": gc.G4_ADDENDUM_SCHEMA,
        "frozen_before_candidate": True,
        "frozen_at_unix": now,
        "frozen_at": dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": "Owner decision 2026-10-11. Rows outside the frozen G4 table, from a dedicated baseline run only. A "
                "missed limit is an open exit to optimize, never a threshold edited after results.",
        "base_budget_path": str(budget_path.relative_to(ROOT) if budget_path.is_relative_to(ROOT) else budget_path),
        "base_budget_sha256": budget_sha,
        "rule": {"statistic": "max", "factor": gc.G4_ADDENDUM_FACTOR,
                 "min_samples_per_window": gc.G4_ADDENDUM_MIN_SAMPLES,
                 "report_only_intervals": list(gc.G4_REPORT_ONLY_INTERVALS),
                 "text": f"GATED rows: candidate window max <= baseline max x {gc.G4_ADDENDUM_FACTOR}, scored on "
                         f"windows with >= {gc.G4_ADDENDUM_MIN_SAMPLES} samples; no such window -> NOT_MEASURED. "
                         "REPORT_ONLY rows (bar 1h/2h/4h/6h, or a baseline without a window of >= "
                         f"{gc.G4_ADDENDUM_MIN_SAMPLES} samples) never pass, fail or block G4."},
        "excluded_already_in_frozen_table": [k for k in gc.G4_ADDENDUM_OWNER_ROWS if k in table],
        "rows": freeze_rows(windows, table),
        "baseline": {"campaign_dir": str(Path(campaign_dir).resolve()), "role": campaign["role"],
                     "sessions": campaign.get("sessions"), "started_unix": campaign.get("started_unix"),
                     "runtime_identity": campaign.get("runtime_identity"), "source_sha": campaign.get("source_sha"),
                     "tool_sha256": campaign.get("tool_sha256"),
                     "windows": [{"index": w["index"], "boundary": w["boundary"]} for w in windows]},
        "sources": sources,
    }
    problems = gc.validate_g4_addendum(addendum, table)
    if problems:
        raise ValueError("; ".join(problems))
    return addendum


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline-campaign", type=Path, required=True,
                        help="directory of `p184_gate_campaign.py run --baseline`")
    parser.add_argument("--budget", type=Path, default=ROOT / "config/v2/p184-stage100-budget.json")
    parser.add_argument("--output", type=Path, default=gc.G4_ADDENDUM_PATH)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise SystemExit(f"{args.output} exists; the addendum is frozen once")
    addendum = freeze(args.baseline_campaign, args.budget)
    with args.output.open("x") as handle:
        json.dump(addendum, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "rows": {k: v["status"] for k, v in addendum["rows"].items()}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

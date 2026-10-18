#!/usr/bin/env python3
"""Freeze P18.4 stage-100 G4 amendment 1 (one row, mark_index_price|OKX|) BEFORE the candidate run (owner 2026-10-18).

Purpose. The frozen G4 table (config/v2/p184-stage100-budget.json) scores mark_index_price|OKX| on read_committed
visibility = observer receipt - source_event_time. That row is a PAIRED mark+index event (rust/qdl-realtime-core
process_mark_index): source_event_time = min(mark.ts, index.ts) and received_at_ns = the OLDEST component receipt, so
the composite measures how long the OKX index stayed unchanged (pushed only on change) plus the internal path
(RCA: DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md commit 44fed4d). The owner approved ONE versioned amendment: this row
is scored on the same-event internal split, Kafka CreateTime - newest component receipt, against a baseline
RECOMPUTED for that split from the frozen baseline run's own per-event records, under the existing rule shapes:
- p95 (frozen-table shape): candidate window p95 <= max over baseline windows of the window p95 x 1.2;
- max (addendum shape): candidate window max <= max over baseline windows (>= 5 samples) of the window max x 1.2.
Composite value, component ts gap and a venue-direct probe are report-only.

Proof that the input IS the frozen baseline (else refused): the four g4-N-summary.json sha256 equal the budget's
sources.g4 list, and the composite p95 recomputed from the raw records with the observer's v1-v3 summary
(scripts/p184_g4_observer.py _summarize_records) equals the frozen table's baseline_p95_ms_windows exactly. Then the
split of the SAME records (paired_split(): source_sequence mark_ts:index_ts:mark_rx:index_rx) gives the baseline;
any record whose split does not parse, lacks CreateTime or has published_at != newest receipt, or a window with
< 5 records, refuses the freeze (insufficient baseline: STOP, nothing written).

Pure file transform (reads JSON/JSONL and `git show <base-ref>:<path>` for the old apparatus hashes); writes one new
file once (open "x"). Old budget, addendum and receipts are not touched.
usage: p184_freeze_g4_amendment.py --baseline-dir DIR [--budget P] [--addendum P] [--output P] [--base-ref REF]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import p184_g4_observer as observer  # noqa: E402
from scripts import p184_gate_campaign as gc  # noqa: E402

AMENDMENT_ID = "G4-A1"
DECISION = {
    "decision_owner": "Bobby (owner)",
    "approval_reference": "AskUserQuestion 2026-10-18 ~06:45Z, option \"Amendment co bang chung + 1 run (Recommended)\": "
                          "versioned G4 amendment for ONE row, then one clean final stage-100",
    "rca_reference": "DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md commit 44fed4d (2026-10-18 06:20-06:45Z read-only RCA)",
    "codex_guidance": "/home/bobby/.worktrees/ts-codex-p184-review/upgrade/"
                      "p184-idr112-113-provider-gates-review-20261018.md section \"Gate Amendment Options\"",
}
APPARATUS = {  # pinned file -> repo path; old = at --base-ref, new = the working file frozen with this amendment
    "gate_campaign_tool": "scripts/p184_gate_campaign.py",
    "g4_observer": "scripts/p184_g4_observer.py",
    "okx_mark_index_probe": "scripts/p184_g4_okx_mark_index_probe.py",
    "freeze_tool": "scripts/p184_freeze_g4_amendment.py",
    "g4_addendum": "config/v2/p184-stage100-g4-addendum.json",
    "budget": "config/v2/p184-stage100-budget.json",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git_sha256(ref: str, path: str) -> str | None:
    out = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT, capture_output=True)
    return hashlib.sha256(out.stdout).hexdigest() if out.returncode == 0 else None


def q(values, f):
    """The v1-v3 exact nearest rank of the frozen baseline (observer _summarize_records)."""
    values = sorted(values)
    return round(values[min(len(values) - 1, int(f * len(values)))], 1) if values else None


def window_split(lines: list[str], row: str) -> dict:
    """Per-event split of one baseline window: composite (as the frozen summary), internal split, component gap."""
    composite = observer._summarize_records(lines).get(row, {}).get("visibility_ms") or {}
    internal, gap, receipt_gap = [], [], []
    unparsed = no_create_time = mismatch = records = 0
    for line in lines:
        item = json.loads(line)
        if item.get("kind") != "canonical_observation" or observer._row_key(item) != row:
            continue
        records += 1
        identity = item.get("identity") or {}
        split = observer.paired_split(identity.get("source_sequence"), item.get("producer_timestamp_ms"),
                                      item.get("published_at_ns"))
        if split is None:
            unparsed += 1
            continue
        mismatch += split["published_mismatch"]
        if split[gc.G4_AMENDMENT_QUANTITY] is None:
            no_create_time += 1
        else:
            internal.append(split[gc.G4_AMENDMENT_QUANTITY])
        gap.append(split["component_ts_gap_ms"])
        receipt_gap.append(split["component_receipt_gap_ms"])

    def block(v):
        return {"n": len(v), "p50": q(v, .5), "p95": q(v, .95), "max": round(max(v), 1) if v else None,
                "negative": sum(1 for x in v if x < 0)}
    return {"records": records, "unparsed": unparsed, "no_create_time": no_create_time, "published_mismatch": mismatch,
            "composite_visibility_ms": composite, "internal": block(internal), "component_ts_gap_ms": block(gap),
            "component_receipt_gap_ms": block(receipt_gap)}


def recompute_baseline(baseline_dir: Path, budget: dict) -> dict:
    """The amended row's baseline from the frozen baseline run's per-event records (refuses anything else)."""
    row = gc.G4_AMENDMENT_ROW
    table = (budget.get("g4_delivery_visibility") or {}).get("table") or {}
    if row not in table or not budget.get("frozen_before_candidate"):
        raise ValueError(f"budget has no frozen G4 row {row}")
    frozen_row = table[row]
    d = Path(baseline_dir)
    expected = list((budget.get("sources") or {}).get("g4") or [])
    summaries = sorted(d.glob("g4-*-summary.json"), key=lambda p: int(p.name.split("-")[1]))
    if not expected or [sha256(p) for p in summaries] != expected:
        raise ValueError(f"{d}: G4 summaries are not the budget's frozen baseline (sources.g4)")
    windows, sources = [], {}
    for index, summary in enumerate(summaries, start=1):
        obs = d / f"g4-{index}" / "observations.jsonl"
        lines = [line for line in obs.read_text().splitlines() if line.strip()]
        sources[summary.name] = sha256(summary)
        sources[f"g4-{index}/observations.jsonl"] = sha256(obs)
        split = window_split(lines, row)
        frozen_p95 = frozen_row["baseline_p95_ms_windows"][index - 1]
        if split["composite_visibility_ms"].get("p95") != frozen_p95:
            raise ValueError(f"window {index}: recomputed composite p95 {split['composite_visibility_ms'].get('p95')} "
                             f"!= frozen {frozen_p95}: not the gated population")
        if split["unparsed"] or split["no_create_time"] or split["published_mismatch"]:
            raise ValueError(f"window {index}: insufficient per-event baseline data (unparsed {split['unparsed']}, "
                             f"no CreateTime {split['no_create_time']}, published mismatch {split['published_mismatch']})")
        if split["internal"]["n"] < gc.G4_AMENDMENT_MIN_SAMPLES:
            raise ValueError(f"window {index}: {split['internal']['n']} records < {gc.G4_AMENDMENT_MIN_SAMPLES}")
        windows.append({"index": index, **split})
    p95s = [w["internal"]["p95"] for w in windows]
    maxes = [w["internal"]["max"] for w in windows]
    return {"frozen_row": frozen_row, "windows": windows, "p95s": p95s, "maxes": maxes, "sources": sources,
            "limits": {"candidate_p95_limit_ms": round(max(p95s) * gc.G4_AMENDMENT_FACTOR, 1),
                       "candidate_max_limit_ms": round(max(maxes) * gc.G4_AMENDMENT_FACTOR, 1)}}


def freeze(baseline_dir: Path, budget_path: Path, addendum_path: Path, *, base_ref: str = "44fed4d",
           now: float | None = None) -> dict:
    row = gc.G4_AMENDMENT_ROW
    d = Path(baseline_dir)
    base = recompute_baseline(d, json.loads(Path(budget_path).read_text()))
    frozen_row, windows, p95s, maxes, sources = (base[k] for k in ("frozen_row", "windows", "p95s", "maxes", "sources"))
    now = time.time() if now is None else now
    pinned = {name: {"path": path, "old_sha256": git_sha256(base_ref, path), "new_sha256": sha256(ROOT / path)}
              for name, path in APPARATUS.items()}
    addendum_sha = sha256(addendum_path)
    if pinned["g4_addendum"]["new_sha256"] != addendum_sha or pinned["budget"]["new_sha256"] != sha256(budget_path):
        raise ValueError("--budget/--addendum differ from the repository files the campaign records")
    if (pinned["g4_addendum"]["old_sha256"] != addendum_sha
            or pinned["budget"]["old_sha256"] != pinned["budget"]["new_sha256"]):
        raise ValueError("budget or addendum changed since the base ref: this amendment never edits them")
    return {
        "schema": gc.G4_AMENDMENT_SCHEMA, "amendment_id": AMENDMENT_ID, "version": 1, "row": row,
        "frozen_before_candidate": True, "frozen_at_unix": now,
        "frozen_at": dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **DECISION,
        "scope": {"applies_to": [row], "unchanged": "every other G4 row (frozen table and addendum), every other gate "
                  "G1-G14, every threshold, the sampler, the observer population and the window schedule",
                  "old_receipts": "immutable: a campaign that did not record this amendment's sha256 at run time is "
                  "scored exactly as before (053803Z / 135719Z and earlier keep their verdicts)"},
        "base_budget_path": APPARATUS["budget"], "base_budget_sha256": pinned["budget"]["new_sha256"],
        "base_addendum_path": APPARATUS["g4_addendum"], "base_addendum_sha256": addendum_sha,
        "rule": {
            "quantity": gc.G4_AMENDMENT_QUANTITY, "factor": gc.G4_AMENDMENT_FACTOR,
            "max_min_samples_per_window": gc.G4_AMENDMENT_MIN_SAMPLES,
            "definition": "per admitted event of the row (unchanged v2 sampler population): Kafka CreateTime (ms, "
                          "librdkafka at the core's transactional send) - max(mark_receipt_ns, index_receipt_ns) from "
                          "source_sequence fields 3-4 (observer v6 paired_split)",
            "text": f"PASS needs, in every window with samples: internal-split p95 <= candidate_p95_limit_ms "
                    f"(BUCKET_UPPER_BOUND p95; a bucket straddling the limit is unresolved = NOT_MEASURED) and, on "
                    f"windows with >= {gc.G4_AMENDMENT_MIN_SAMPLES} samples, max <= candidate_max_limit_ms; a "
                    "decisive excess in any window = FAIL; plus, per window, a covering venue probe (attribution "
                    "EXTERNAL_VENUE_COMPONENT_CADENCE); a window without one is UNKNOWN, which is not EXTERNAL -> "
                    "NOT_MEASURED. Events without a parsable split, without CreateTime, or with published_at != "
                    "newest receipt -> NOT_MEASURED. No window with samples -> NOT_MEASURED.",
            "report_only": ["composite visibility (the frozen-table quantity) per window, with the frozen limit and "
                            "whether it would exceed it, all outliers kept (top-5 capsule over every accepted event)",
                            "component ts gap |mark.ts - index.ts|, component receipt gap, per-component venue -> DL, "
                            "older component, internal-split outlier capsule",
                            "venue-direct OKX mark-price/index-tickers probe per G4 window "
                            "(scripts/p184_g4_okx_mark_index_probe.py) with the host clock bound (chronyc)"]},
        "baseline": {
            "campaign_dir": str(d.resolve()), "role": "frozen G4 table baseline (budget sources.g4)",
            "method": "same records the frozen table was computed from: composite p95 recomputed per window with "
                      "observer _summarize_records equals baseline_p95_ms_windows exactly; the internal split of those "
                      "same records, exact nearest rank (index int(0.95 n)), max exact",
            "frozen_table_row": frozen_row, "windows": windows,
            "baseline_p95_ms_windows": p95s, "baseline_max_ms_windows": maxes,
            "baseline_p95_ms": max(p95s), "baseline_max_ms": max(maxes)},
        "limits": base["limits"],
        "pinned": {**pinned, "base_ref": base_ref,
                   "note": "old = at the pre-amendment base ref, new = frozen with this amendment; the scorer applies "
                           "the amendment only when the campaign tool sha256 equals gate_campaign_tool.new_sha256"},
        "sources": sources,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline-dir", type=Path, required=True, help="the frozen G4 table baseline run")
    parser.add_argument("--budget", type=Path, default=ROOT / APPARATUS["budget"])
    parser.add_argument("--addendum", type=Path, default=gc.G4_ADDENDUM_PATH)
    parser.add_argument("--output", type=Path, default=gc.G4_AMENDMENT_PATH)
    parser.add_argument("--base-ref", default="44fed4d")
    args = parser.parse_args(argv)
    if args.output.exists():
        raise SystemExit(f"{args.output} exists; the amendment is frozen once")
    amendment = freeze(args.baseline_dir, args.budget, args.addendum, base_ref=args.base_ref)
    problems = gc.validate_g4_amendment(amendment, json.loads(args.budget.read_text())["g4_delivery_visibility"]["table"])
    if problems:
        raise SystemExit("; ".join(problems))
    with args.output.open("x") as handle:
        json.dump(amendment, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "sha256": sha256(args.output), "limits": amendment["limits"],
                      "baseline_p95_ms_windows": amendment["baseline"]["baseline_p95_ms_windows"],
                      "baseline_max_ms_windows": amendment["baseline"]["baseline_max_ms_windows"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

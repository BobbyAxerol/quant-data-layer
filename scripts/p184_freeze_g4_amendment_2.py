#!/usr/bin/env python3
"""Freeze P18.4 stage-100 G4 amendment 2 ("B1": bar >= 8h REPORT_ONLY) BEFORE the candidate run (owner 2026-10-18).

Purpose. RCA 53eb546 (DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md): the G4 observer keys bar rows by whatever interval
closed inside a window, so a boundary window at 00/08/12/16Z shows bar|BINANCE|8h (and 12h/1d/... at their closes),
rows no baseline ever crossed; the frozen rule makes them "observed rows without a frozen baseline" -> G4 NOT_MEASURED
by construction. The owner chose (AskUserQuestion 2026-10-18 ~08:50Z, "bar >= 8h REPORT_ONLY"): such rows are
REPORT_ONLY, the same class as the addendum's bar 1h-6h (scripts/p184_gate_campaign.py G4_REPORT_ONLY_INTERVALS). No
threshold changes; the frozen table, the addendum and amendment 1 are not edited.

Amendment 1 applies only to the campaign tool sha256 it pins; this change edits that tool, so amendment 2 also
re-pins amendment 1 (unchanged file, recorded by sha256) to the new tool. The scorer applies both only to a campaign
that recorded them at run time (frozen before it started); every older campaign scores byte-identically.

Pure file transform (reads the repository files and `git show <base-ref>:<path>` for the old hashes); writes one new
file once (open "x").
usage: p184_freeze_g4_amendment_2.py [--output P] [--base-ref REF]
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

from scripts import p184_gate_campaign as gc  # noqa: E402

DECISION = {
    "decision_owner": "Bobby (owner)",
    "approval_reference": "AskUserQuestion 2026-10-18 ~08:50Z (one question, three answers): B1 \"bar >= 8h "
                          "REPORT_ONLY\" (with A1 \"dung chung bound\" and OKX books \"Reconnect + probe; tai dien thi "
                          "limit\")",
    "rca_reference": "DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md commit 53eb546 (2026-10-18 08:26-08:36Z read-only RCA, "
                     "item B)",
}
APPARATUS = {  # pinned file -> repo path; old = at --base-ref, new = the working file frozen with this amendment
    "gate_campaign_tool": "scripts/p184_gate_campaign.py",
    "g4_observer": "scripts/p184_g4_observer.py",
    "okx_mark_index_probe": "scripts/p184_g4_okx_mark_index_probe.py",
    "okx_books_probe": "scripts/p184_g4_okx_books_probe.py",
    "driver": "scripts/phase3_consumer_load_acceptance.py",
    "reference_verifier": "qdl/certification/reference_l2_acceptance.py",
    "freeze_tool": "scripts/p184_freeze_g4_amendment_2.py",
    "g4_amendment_1": "config/v2/p184-stage100-g4-amendment-1.json",
    "g4_addendum": "config/v2/p184-stage100-g4-addendum.json",
    "budget": "config/v2/p184-stage100-budget.json",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git_sha256(ref: str, path: str) -> str | None:
    out = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT, capture_output=True)
    return hashlib.sha256(out.stdout).hexdigest() if out.returncode == 0 else None


def freeze(*, base_ref: str, now: float | None = None) -> dict:
    now = time.time() if now is None else now
    pinned = {name: {"path": path, "old_sha256": git_sha256(base_ref, path), "new_sha256": sha256(ROOT / path)}
              for name, path in APPARATUS.items()}
    for name in ("budget", "g4_addendum", "g4_amendment_1"):
        if pinned[name]["old_sha256"] != pinned[name]["new_sha256"]:
            raise ValueError(f"{name} changed since the base ref: this amendment never edits it")
    amendment_1 = json.loads((ROOT / APPARATUS["g4_amendment_1"]).read_text())
    table = json.loads((ROOT / APPARATUS["budget"]).read_text())["g4_delivery_visibility"]["table"]
    if gc.validate_g4_amendment(amendment_1, table):
        raise ValueError("amendment 1 is not valid against the frozen table")
    long_in_table = sorted(k for k in table if gc.g4_long_bar_row(k))
    addendum_rows = json.loads((ROOT / APPARATUS["g4_addendum"]).read_text()).get("rows") or {}
    long_in_addendum = sorted(k for k in addendum_rows if gc.g4_long_bar_row(k))
    if long_in_table or long_in_addendum:
        raise ValueError(f"bar >= 8h rows already have a frozen baseline: {long_in_table + long_in_addendum}")
    return {
        "schema": gc.G4_AMENDMENT_2_SCHEMA, "amendment_id": gc.G4_AMENDMENT_2_ID, "version": 1,
        "frozen_before_candidate": True, "frozen_at_unix": now,
        "frozen_at": dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **DECISION,
        "rule": {"feed": "bar", "report_only_min_interval_ms": gc.G4_B1_MIN_INTERVAL_MS,
                 "applies_to": "rows observed without a frozen baseline (not in the table, not in the addendum)",
                 "threshold_changes": [],
                 "text": "A G4 row bar|VENUE|INTERVAL whose fixed interval is >= 8h and that is neither in the frozen "
                         "table nor in the addendum is REPORT_ONLY (never passes, never fails, never blocks), the "
                         "class of the addendum's bar 1h/2h/4h/6h. Every other row, gate, threshold and the window "
                         "schedule are unchanged."},
        "scope": {"unchanged": "frozen G4 table and its p95 rule, the addendum rows and limits, amendment 1's row and "
                               "limits, every other gate G1-G14, the sampler, the observer population, the schedule",
                  "old_receipts": "immutable: a campaign that did not record this amendment's sha256 at run time is "
                                  "scored exactly as before"},
        "base_budget_path": APPARATUS["budget"], "base_budget_sha256": pinned["budget"]["new_sha256"],
        "base_addendum_path": APPARATUS["g4_addendum"], "base_addendum_sha256": pinned["g4_addendum"]["new_sha256"],
        "inherits": {"g4_amendment_1_path": APPARATUS["g4_amendment_1"],
                     "g4_amendment_1_sha256": pinned["g4_amendment_1"]["new_sha256"],
                     "g4_amendment_1_tool_pin": ((amendment_1.get("pinned") or {}).get("gate_campaign_tool") or {})
                     .get("new_sha256"),
                     "note": "amendment 1 (file unchanged) is re-pinned to gate_campaign_tool.new_sha256 below"},
        "pinned": {**pinned, "base_ref": base_ref,
                   "note": "old = at the pre-amendment base ref, new = frozen with this amendment; the scorer applies "
                           "the amendment (and amendment 1 through it) only when the campaign tool sha256 equals "
                           "gate_campaign_tool.new_sha256"},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=gc.G4_AMENDMENT_2_PATH)
    parser.add_argument("--base-ref", required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise SystemExit(f"{args.output} exists; the amendment is frozen once")
    amendment = freeze(base_ref=args.base_ref)
    problems = gc.validate_g4_amendment_2(amendment)
    if problems:
        raise SystemExit("; ".join(problems))
    with args.output.open("x") as handle:
        json.dump(amendment, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "sha256": sha256(args.output),
                      "gate_campaign_tool": amendment["pinned"]["gate_campaign_tool"],
                      "g4_amendment_1": amendment["pinned"]["g4_amendment_1"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

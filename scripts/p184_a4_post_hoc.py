#!/usr/bin/env python3
"""P18.4 stage-100 amendment A4: a POST-HOC owner decision, applied to one named, already scored run.

Owner decisions (AskUserQuestion 2026-10-18 ~14:45Z, after run p184-final-campaign-20261018T123222Z was scored):
* G4, rows book_delta / book_snapshot / quote |OKX_INVERSE| ("M1: chấm theo p95 pooled (Recommended)"): each row is
  gated by the frozen p95 limit of its pooled OKX row (book_delta|OKX| 289.1, book_snapshot|OKX| 270.1, quote|OKX|
  283.3 ms) with the frozen-table p95 rule (bucket lower edge over the limit = FAIL, a straddled bucket = unresolved),
  on windows with >= 20 samples. The window maximum is REPORT_ONLY. Every admitted event above the replaced addendum
  max limit (282.5 / 300.7 / 322.6 ms) must be retained by observer v7 (threshold <= that limit, no overflow, every
  admitted sample seen, the window maximum among the retained); otherwise the row is FAIL (UNKNOWN = FAIL).
* G2, row MARK_INDEX_PRICE|BINANCE ("Nới max lên 1200 ms (Recommended)"): max <= 1200 ms (0.6 x the TS 2000 ms
  use-time budget) instead of max < 1000 ms; p95 <= 500 ms unchanged.
Both are labelled POST_HOC_OWNER_DECISION. The amendment file is config/v2/p184-stage100-a4-ts-eligibility.json;
the rationale is upgrade/evidence/releases/v2.2.4/a4-post-hoc-ts-eligibility.md.

Why this is a separate module and the frozen scorer (scripts/p184_gate_campaign.py) is NOT edited: A3 pins the sha256
of the scoring code it runs with (p184_gate_campaign.A3_CODE includes the campaign tool) and is not applied when that
code differs, and A3 re-pins G4-A1/G4-A2 to the same tool. Editing the scorer would therefore drop A1-A3 from any
rescore of 123222Z. Instead this module runs the unchanged build_scorecard twice: once as is (it must reproduce the
stored scorecard.json byte for byte) and once with score_mark and score_g4 wrapped so that ONLY the four rows above
are re-judged. Every other gate must come out byte-identical to the first pass, or nothing is written.

Applicability: only the campaign named in the amendment, only when its stored scorecard.json has the pinned sha256,
the frozen scorer has the pinned sha256, A3 was applied in the stored scorecard, the protocol was met, G6 and G9 PASS
with 0 data-path eligibility losses. `--force-report-only` scores another receipt with the same rule for comparison;
its output says REPORT_ONLY and is never written next to a receipt.

Boundary: read-only on the campaign directory except `--write`, which creates (open "x", never overwrites)
<campaign>/scorecard.a4-post-hoc.json and its .sha256. No container, Redis, Kafka or runtime action.
usage: p184_a4_post_hoc.py --campaign-dir DIR [--amendment PATH] [--write | --force-report-only]
"""
from __future__ import annotations

import argparse
import contextlib
import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import p184_gate_campaign as gc  # noqa: E402
from scripts.p184_resp_readonly import read_jsonl  # noqa: E402

A4_PATH = ROOT / "config/v2/p184-stage100-a4-ts-eligibility.json"
A4_SCHEMA = "qdl.p184.stage100-a4.v1"
A4_ID = "A4"
A4_LABEL = "POST_HOC_OWNER_DECISION"
OUTPUT_NAME = "scorecard.a4-post-hoc.json"
OUTPUT_SCHEMA = "qdl.p184.gate-campaign-scorecard.a4-post-hoc.v1"
G2_ROW = "MARK_INDEX_PRICE|BINANCE"
G2_MAX_LE_MS = 1200.0
G2_P95_LE_MS = 500.0
G4_MIN_SAMPLES = 20
# A4 row -> (pooled frozen-table row, its frozen p95 limit, the replaced addendum max limit).
G4_ROWS = {
    "book_delta|OKX_INVERSE|": ("book_delta|OKX|", 289.1, 282.5),
    "book_snapshot|OKX_INVERSE|": ("book_snapshot|OKX|", 270.1, 300.7),
    "quote|OKX_INVERSE|": ("quote|OKX|", 283.3, 322.6),
}
SCOPED_GATES = ("G2", "G4")


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def validate_a4(amendment) -> list[str]:
    """Problems that keep A4 from being applied (empty list = the file states exactly the owner decision)."""
    if not isinstance(amendment, dict) or amendment.get("schema") != A4_SCHEMA:
        return ["unknown A4 schema"]
    problems = []
    if amendment.get("amendment_id") != A4_ID or amendment.get("label") != A4_LABEL \
            or amendment.get("post_hoc") is not True:
        problems.append("A4 id, label or post_hoc flag differs from the owner decision")
    if not amendment.get("decision_owner") or not amendment.get("frozen_at_unix"):
        problems.append("A4 is not frozen with a decision owner")
    g2 = ((amendment.get("rules") or {}).get("G2") or {})
    if (g2.get("row"), g2.get("max_le_ms"), g2.get("p95_le_ms")) != (G2_ROW, G2_MAX_LE_MS, G2_P95_LE_MS):
        problems.append("A4 G2 rule differs from the owner decision")
    g4 = ((amendment.get("rules") or {}).get("G4") or {})
    rows = {key: (spec.get("pooled_row"), spec.get("p95_limit_ms"), spec.get("replaced_addendum_max_limit_ms"))
            for key, spec in (g4.get("rows") or {}).items()}
    if rows != G4_ROWS or g4.get("min_samples_per_window") != G4_MIN_SAMPLES or g4.get("max") != "REPORT_ONLY":
        problems.append("A4 G4 rule differs from the owner decision")
    applies = amendment.get("applies_to") or {}
    if not (applies.get("campaign_id") and applies.get("scorecard_sha256") and applies.get("scorer_sha256")):
        problems.append("A4 does not pin the run, its scorecard and the frozen scorer")
    return problems


def retention_problems(retention, *, n: int, window_max, limit: float) -> list[str]:
    """Observer v7 exceedance retention of one row-window whose max is over `limit` (the replaced addendum limit)."""
    if not retention:
        return ["no exceedance retention for the row (observer < v7): UNKNOWN"]
    problems = []
    if retention.get("threshold_ms") is None or retention["threshold_ms"] > limit:
        problems.append("retention threshold above the replaced limit")
    if retention.get("overflow"):
        problems.append(f"{retention['overflow']} exceedances beyond the retention cap")
    if retention.get("admitted_seen") != n:
        problems.append(f"retention saw {retention.get('admitted_seen')} admitted samples, the window has {n}")
    events = [e for e in retention.get("events") or [] if (e.get("visibility_ms") or 0) > limit]
    top = retention.get("max_event") or {}
    ident = lambda e: (e.get("topic"), e.get("partition"), e.get("offset"))  # noqa: E731
    if not events or not top or ident(top) not in {ident(e) for e in events} \
            or abs((top.get("visibility_ms") or 0) - (window_max or 0)) > 0.001:
        problems.append("the window maximum is not among the retained events")
    return problems


def _retained(retention, limit: float) -> list[dict]:
    keep = ("topic", "partition", "offset", "source_event_ns", "received_at_ns", "producer_timestamp_ms",
            "observer_receipt_wall_ns", "visibility_ms")
    return [{k: e.get(k) for k in keep} for e in (retention or {}).get("events") or []
            if (e.get("visibility_ms") or 0) > limit]


def score_a4_g4_row(frozen_row: dict, windows: list[dict], table: dict, retention: list[dict]) -> dict:
    """One OKX_INVERSE row under A4: pooled-row p95 rule (n >= 20), max report-only, retained outliers."""
    key = frozen_row["row"]
    pooled, limit, replaced = G4_ROWS[key]
    stats = [gc.g4_window_stats(w, key) for w in windows]
    n = [s.get("n", 0) for s in stats]
    p95 = [s.get("p95") if s.get("n") else None for s in stats]
    lower = [(s.get("p95_bucket_lower") if s.get("percentile_basis") == "BUCKET_UPPER_BOUND" else s.get("p95"))
             if s.get("n") else None for s in stats]
    maxes = [s.get("max") if s.get("n") else None for s in stats]
    scored = [k >= G4_MIN_SAMPLES for k in n]
    over = [sc and x is not None and x > limit and lo is not None and lo > limit
            for sc, x, lo in zip(scored, p95, lower)]
    unresolved = [sc and x is not None and x > limit and not o for sc, x, o in zip(scored, p95, over)]
    problems, outliers = [], []
    frozen_limit = (table.get(pooled) or {}).get("candidate_p95_limit_ms")
    if frozen_limit != limit:
        problems.append(f"pooled row {pooled} frozen p95 limit {frozen_limit} != A4 {limit}")
    if frozen_row.get("limit_max_ms") != replaced:
        problems.append(f"addendum max limit {frozen_row.get('limit_max_ms')} != A4 replaced {replaced}")
    for i, m in enumerate(maxes):
        held = retention[i].get(key) if i < len(retention) else None
        if m is not None and m > replaced:
            problems += [f"window {i + 1}: {p}" for p in retention_problems(held, n=n[i], window_max=m, limit=replaced)]
        outliers += [{"window": i + 1, **e} for e in _retained(held, replaced)]
    if problems and any(p.startswith("pooled row") or p.startswith("addendum max") for p in problems):
        verdict = "NOT_MEASURED"
    elif not any(scored):
        verdict = "NOT_MEASURED"
    elif any(over) or problems:
        verdict = "FAIL"
    elif any(unresolved):
        verdict = "NOT_MEASURED"
    else:
        verdict = "PASS"
    return {"row": key, "basis": "a4_post_hoc", "amendment_id": A4_ID, "label": A4_LABEL, "post_hoc": True,
            "pooled_row": pooled, "limit_p95_ms": limit, "min_samples_per_window": G4_MIN_SAMPLES,
            "n_per_window": n, "scored_windows": scored, "p95_basis": "BUCKET_UPPER_BOUND",
            "p95_ms_per_window": p95, "p95_bucket_lower_ms_per_window": lower,
            **({"unresolved_windows": unresolved} if any(unresolved) else {}),
            "max_ms_per_window": maxes, "max_report_only": True, "replaced_addendum_max_limit_ms": replaced,
            "retained_over_replaced_limit": outliers, "problems": problems, "verdict": verdict,
            **({"a3": frozen_row["a3"]} if frozen_row.get("a3") else {}),  # keeps the G4 A3 attempted/rescued lists
            "frozen_rule_row": frozen_row}


def a4_score_g4(original, retention: list[dict]):
    """Wrap the frozen score_g4: only the A4 rows are replaced; the gate verdict is re-derived from the same causes."""
    def score(windows, table, addendum=None, amendment=None, amendment2=None, a3_rescue=None):
        verdict, rows, why = original(windows, table, addendum, amendment, amendment2, a3_rescue=a3_rescue)
        rows = [score_a4_g4_row(r, windows, table, retention) if r.get("row") in G4_ROWS and r.get("basis") ==
                "addendum" else r for r in rows]
        failed = [r["row"] for r in rows if r["verdict"] == "FAIL"]
        other = []
        for clause in why:
            if clause.startswith("over limit: "):
                continue
            if clause.startswith("addendum rows without a window of"):  # an A4 row is no longer an addendum row
                head, _, keys = clause.partition(": ")
                keys = [k for k in keys.split(", ") if k not in G4_ROWS]
                clause = f"{head}: {', '.join(keys)}" if keys else ""
            if clause:
                other.append(clause)
        thin = [r["row"] for r in rows if r.get("basis") == "a4_post_hoc" and r["verdict"] == "NOT_MEASURED"]
        reasons = ([f"over limit: {', '.join(failed)}"] if failed else []) + other + (
            [f"A4 rows not measured: {', '.join(thin)}"] if thin else [])
        if failed:
            return "FAIL", rows, reasons
        if other or thin:
            return "NOT_MEASURED", rows, reasons
        return "PASS", rows, []
    return score


def a4_score_mark(original):
    """Wrap the frozen score_mark: only MARK_INDEX_PRICE|BINANCE is re-judged (p95 <= 500, max <= 1200 ms)."""
    def score(mark):
        _, rows, _ = original(mark)
        rows = [dict(r) for r in rows]
        for r in rows:
            if r["feed_venue"] == G2_ROW:
                inside = (r.get("n") and r.get("max") is not None and r.get("p95") is not None
                          and r["max"] <= G2_MAX_LE_MS and r["p95"] <= G2_P95_LE_MS)
                r["a4"] = {"amendment_id": A4_ID, "label": A4_LABEL, "post_hoc": True,
                           "rule": f"p95 <= {G2_P95_LE_MS} ms and max <= {G2_MAX_LE_MS} ms",
                           "observed_frozen_rule": r.get("observed")}
                r["observed"] = "INSIDE" if inside else "OUTSIDE"
        present = {r["feed_venue"] for r in rows}
        missing = [f"MARK|{v}" for v in gc.VENUES if f"MARK_INDEX_PRICE|{v}" not in present]
        outside = [f"MARK|{r['feed_venue'].split('|')[1]}" for r in rows if r.get("observed") == "OUTSIDE"]
        if outside:
            return "FAIL", rows, [f"outside: {', '.join(outside)}"]
        if missing:
            return "NOT_MEASURED", rows, [f"no data: {', '.join(missing)}"]
        return "PASS", rows, []
    return score


@contextlib.contextmanager
def patched(**functions):
    saved = {name: getattr(gc, name) for name in functions}
    try:
        for name, fn in functions.items():
            setattr(gc, name, fn)
        yield
    finally:
        for name, fn in saved.items():
            setattr(gc, name, fn)


def load_retention(d: Path, campaign: dict) -> list[dict]:
    """Per G4 window (campaign order): {A4 row: observer v7 exceedance retention or None}."""
    from scripts import p184_g4_observer as observer
    out = []
    for item in campaign.get("g4_windows", []):
        obs = d / f"g4-{item['index']}" / "observations.jsonl"
        aggregate = (observer.last_aggregate(read_jsonl(obs)) if obs.exists() else None) or {}
        rows = aggregate.get("inverse_rows") or {}
        out.append({key: (rows.get(key) or {}).get("exceedances") for key in G4_ROWS})
    return out


def applicability(d: Path, stored: dict | None, frozen: dict, amendment: dict) -> list[str]:
    problems = []
    applies = amendment.get("applies_to") or {}
    if d.name != applies.get("campaign_id"):
        problems.append(f"A4 names {applies.get('campaign_id')}, not {d.name}")
    if gc.sha256_file(d / "scorecard.json") != applies.get("scorecard_sha256"):
        problems.append("stored scorecard.json differs from the one A4 pins")
    if gc.sha256_file(ROOT / "scripts/p184_gate_campaign.py") != applies.get("scorer_sha256"):
        problems.append("frozen scorer differs from the one A4 pins")
    if stored is None or canonical(stored) != canonical(frozen):
        problems.append("the frozen scorer does not reproduce the stored scorecard.json")
    if not (frozen.get("a3") or {}).get("applied"):
        problems.append("A3 was not applied in the stored scorecard")
    gates = {g["id"]: g for g in frozen.get("gates", [])}
    if not frozen.get("protocol_met"):
        problems.append("protocol not met")
    if (gates.get("G6") or {}).get("verdict") != "PASS":
        problems.append("G6 (TS D3 execution eligibility) is not PASS")
    g9 = gates.get("G9") or {}
    if g9.get("verdict") != "PASS" or (g9.get("measured") or {}).get("eligibility_losses_data_path") != 0:
        problems.append("G9 is not PASS with 0 data-path eligibility losses")
    return problems


def rescore(campaign_dir, amendment_path=A4_PATH, *, force_report_only: bool = False) -> dict:
    """{'applied', 'report_only', 'problems', 'frozen', 'card'}; card is None when A4 is not applicable."""
    d = Path(campaign_dir)
    amendment = gc.load_json(Path(amendment_path))
    problems = validate_a4(amendment) if amendment is not None else ["unreadable A4"]
    frozen = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    if problems:
        return {"applied": False, "report_only": force_report_only, "problems": problems, "frozen": frozen,
                "card": None}
    stored = gc.load_json(d / "scorecard.json")
    gate_problems = applicability(d, stored, frozen, amendment)
    if gate_problems and not force_report_only:
        return {"applied": False, "report_only": False, "problems": gate_problems, "frozen": frozen, "card": None}
    campaign = gc.load_json(d / "campaign.json") or {}
    with patched(score_mark=a4_score_mark(gc.score_mark),
                 score_g4=a4_score_g4(gc.score_g4, load_retention(d, campaign))):
        card = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    frozen_gates = {g["id"]: g for g in frozen["gates"]}
    drift = [g["id"] for g in card["gates"]
             if g["id"] not in SCOPED_GATES and canonical(g) != canonical(frozen_gates.get(g["id"]))]
    if drift:
        raise RuntimeError(f"A4 changed gates outside its scope: {drift}")
    source = {"path": str(Path(amendment_path)), "sha256": gc.sha256_file(Path(amendment_path))}
    for gate in card["gates"]:
        if gate["id"] in SCOPED_GATES:
            gate["rule"] += ("; A4 (POST_HOC_OWNER_DECISION): MARK_INDEX_PRICE|BINANCE max <= 1200 ms, p95 <= 500 ms"
                             if gate["id"] == "G2" else
                             "; A4 (POST_HOC_OWNER_DECISION): book_delta / book_snapshot / quote |OKX_INVERSE| "
                             "gated by the pooled OKX row p95 limit (289.1 / 270.1 / 283.3 ms) on windows with >= 20 "
                             "samples, max report-only, every event above the replaced addendum limit retained")
            gate["a4"] = {**source, "amendment_id": A4_ID, "label": A4_LABEL, "post_hoc": True,
                          "verdict_without_a4": frozen_gates[gate["id"]]["verdict"],
                          "reason_without_a4": frozen_gates[gate["id"]]["reason"]}
    card["schema"] = OUTPUT_SCHEMA
    card["a4"] = {**source, "amendment_id": A4_ID, "label": A4_LABEL, "post_hoc": True,
                  "report_only": bool(gate_problems), "problems": gate_problems,
                  "scorecard_without_a4_sha256": gc.sha256_file(d / "scorecard.json"),
                  "scorer_sha256": gc.sha256_file(ROOT / "scripts/p184_gate_campaign.py"),
                  "rescore_tool_sha256": gc.sha256_file(Path(__file__)),
                  "summary_without_a4": frozen["summary"],
                  "statement": "A4 was adopted after this run was scored; the verdicts without A4 stay the record of "
                               "the frozen rule and are shown next to the A4 verdicts"}
    return {"applied": not gate_problems, "report_only": bool(gate_problems), "problems": gate_problems,
            "frozen": frozen, "card": card}


def write_receipt(d: Path, card: dict) -> tuple[Path, str]:
    path = Path(d) / OUTPUT_NAME
    text = json.dumps(card, indent=2, sort_keys=True) + "\n"
    with path.open("x") as out:
        out.write(text)
    digest = gc.sha256_file(path)
    with (Path(d) / (OUTPUT_NAME + ".sha256")).open("x") as out:
        out.write(f"{digest}  {OUTPUT_NAME}\n")
    return path, digest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--campaign-dir", type=Path, required=True)
    parser.add_argument("--amendment", type=Path, default=A4_PATH)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="create scorecard.a4-post-hoc.json (+ .sha256), never overwrite")
    mode.add_argument("--force-report-only", action="store_true", help="score another receipt for comparison only")
    args = parser.parse_args(argv)
    result = rescore(args.campaign_dir, args.amendment, force_report_only=args.force_report_only)
    card = result["card"]
    report = {"campaign": args.campaign_dir.name, "applied": result["applied"], "report_only": result["report_only"],
              "problems": result["problems"], "summary_without_a4": result["frozen"]["summary"],
              "summary_with_a4": card["summary"] if card else None}
    if args.write:
        if not result["applied"]:
            print(json.dumps(report, indent=1))
            return 2
        path, digest = write_receipt(args.campaign_dir, card)
        report.update(written=str(path), sha256=digest)
    print(json.dumps(report, indent=1))
    return 0 if card else 2


if __name__ == "__main__":
    raise SystemExit(main())

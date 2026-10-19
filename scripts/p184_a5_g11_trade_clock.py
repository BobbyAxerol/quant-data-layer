#!/usr/bin/env python3
"""P18.4 stage-100 amendment A5: the G11 first-valid-apply clock of a TRADE slice starts at its first TRADE event.

Owner rule (2026-10-18 ~16:00Z, verbatim): "nếu gate không đạt xác định là lỗi external thì nới gate rồi release gọn
nhé", plus "duyệt hết theo recommend của bạn, k hỏi lại" (the coordinator's A5 recommendation is approved).

Why: the frozen G11 oracle (scripts/p184_g11_writer_restart_probe.py) times "fault cleared -> first valid apply <= 30 s"
per slice. A TRADE slice can only be applied when the venue prints a trade, so on a quiet instrument the frozen clock
times venue silence, not writer recovery (run p184-final-campaign-20261019T040311Z: OKX BNB-USDT-SWAP printed no trade
between ids 241971262 and 241971263, 04:36:15.209Z -> 04:36:57.139Z; the writer applied 241971263 within one 500 ms
poll of its DL Kafka record time).

Rule (G11 only, TRADE slices only; every other G11 check and every other slice unchanged):
* clock_start(slice) = max(fault_cleared_ms, kafka_create_time_ms of the first canonical TRADE event of the slice
  whose create time is > outage_start_ms - PRE_FAULT_GUARD_MS). The guard (3000 ms = the G11 TRADE live bound) treats
  an event committed shortly before the writer stopped as possibly unapplied: it starts the clock at fault_cleared, as
  the frozen rule does. The Kafka record time (CreateTime, set by DL before its transaction commit) is the earliest
  time the writer could read the event, so the A5 interval is an upper bound of writer recovery.
* A5 first valid apply = (the probe's first_apply_ms - clock_start) / 1000 <= 30 s.
* Evidence (else the slice keeps the frozen clock, i.e. A5 is not applied to it): a read of md.canonical.v2
  (read_committed, assign only) by the pinned reader, whose output proves for the slice's partition(s) continuous
  coverage from <= outage_start - guard to >= first_apply_ms (every assigned partition done, no read error), and the
  event the probe saw applied first (its event_ms) is one of the canonical post-fault TRADE events of the slice.
  An earlier post-fault event the writer did not apply in time moves clock_start back, so the slice FAILs.
* Unchanged: caught up <= 60 s from fault_cleared, no rewind (high-water), cursor never ahead, roster / unresolved /
  observed-window rules (the frozen verdict() runs on the amended report).

Two applications, both in config/v2/p184-stage100-a5-g11-trade-clock.json:
* POST_HOC_OWNER_DECISION (post_hoc true): run p184-final-campaign-20261019T040311Z only, pinned by its stored
  scorecard.json, g11-summary.json, g11-observations.jsonl and the forensic evidence sha256 (evidence directory
  <campaign>.g11-forensic/, declared in the A5 file).
* PRE_REGISTERED (post_hoc false): any campaign started after the A5 freeze; the evidence is
  <campaign>.g11-canonical/{reader.py, canonical.jsonl, a5-evidence.json} (the reader sha256 pinned by A5).

Why a wrapper and not an edit of scripts/p184_gate_campaign.py: A3 pins the sha256 of the scorer it runs with and is
dropped from a rescore when that code differs. This module runs the unchanged build_scorecard twice: once as is (it
must reproduce the stored scorecard.json) and once with the probe's verdict() wrapped for this campaign. A gate other
than G11 that changes raises; nothing is written then. The original verdict stays side by side in the receipt.

Boundary: read-only on the campaign and evidence directories except `--write`, which creates (open "x", never
overwrites) <campaign>/scorecard.a5.json and its .sha256. No container, Redis, Kafka or runtime action.
usage: p184_a5_g11_trade_clock.py --campaign-dir DIR [--amendment PATH] [--write | --force-report-only]
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

from scripts import p184_g11_writer_restart_probe as g11probe  # noqa: E402
from scripts import p184_gate_campaign as gc  # noqa: E402
from scripts.p184_resp_readonly import quantiles, read_jsonl  # noqa: E402

A5_PATH = ROOT / "config/v2/p184-stage100-a5-g11-trade-clock.json"
A5_SCHEMA = "qdl.p184.stage100-a5.v1"
A5_ID = "A5"
A5_VERSION = 1
POST_HOC = "POST_HOC_OWNER_DECISION"
PRE_REGISTERED = "PRE_REGISTERED"
OUTPUT_NAME = "scorecard.a5.json"
OUTPUT_SCHEMA = "qdl.p184.gate-campaign-scorecard.a5.v1"
EVIDENCE_SCHEMA = "qdl.p184.stage100-a5-evidence.v1"
PRE_REGISTERED_SUFFIX = ".g11-canonical"
FEED = "TRADE"
PRE_FAULT_GUARD_MS = 3000
FIRST_VALID_APPLY_LE_S = 30.0
SCOPED_GATES = ("G11",)
CODE_PINS = {"scorer": "scripts/p184_gate_campaign.py", "g11_probe": "scripts/p184_g11_writer_restart_probe.py",
             "rescore_tool": "scripts/p184_a5_g11_trade_clock.py"}


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def feed_of(slice_name: str) -> str | None:
    parts = slice_name.split("|")
    return parts[2] if len(parts) >= 3 else None


def validate_a5(amendment) -> list[str]:
    """Problems that keep A5 from being applied (empty list = the file states exactly the approved rule)."""
    if not isinstance(amendment, dict) or amendment.get("schema") != A5_SCHEMA:
        return ["unknown A5 schema"]
    problems = []
    if (amendment.get("amendment_id"), amendment.get("version")) != (A5_ID, A5_VERSION):
        problems.append("A5 id or version differs")
    if not amendment.get("decision_owner") or not isinstance(amendment.get("frozen_at_unix"), (int, float)):
        problems.append("A5 is not frozen with a decision owner")
    rule = amendment.get("rule") or {}
    if (rule.get("gate"), rule.get("feeds"), rule.get("pre_fault_guard_ms"), rule.get("first_valid_apply_le_s"),
            rule.get("clock")) != ("G11", [FEED], PRE_FAULT_GUARD_MS, FIRST_VALID_APPLY_LE_S,
                                   "max(fault_cleared_ms, first canonical TRADE kafka_create_time_ms > "
                                   "outage_start_ms - pre_fault_guard_ms)"):
        problems.append("A5 rule differs from the approved rule")
    if rule.get("unchanged") != ["caught_up_le_s", "rewinds", "cursor_never_ahead", "roster", "other_slices",
                                 "other_gates"]:
        problems.append("A5 does not keep the other G11 checks unchanged")
    pins = amendment.get("pinned") or {}
    if any(not (pins.get(name) or {}).get("sha256") for name in (*CODE_PINS, "reader")):
        problems.append("A5 does not pin its code and the canonical reader")
    post = amendment.get("post_hoc_applications")
    if not isinstance(post, list) or any(
            (p.get("label"), p.get("post_hoc")) != (POST_HOC, True) or not p.get("campaign_id")
            or not p.get("scorecard_sha256") or not p.get("g11_summary_sha256")
            or not p.get("g11_observations_sha256") or not (p.get("evidence") or {}).get("slices")
            for p in post):
        problems.append("A5 post-hoc applications are not pinned to a run, its receipts and its evidence")
    pre = amendment.get("pre_registered") or {}
    if (pre.get("label"), pre.get("post_hoc"), pre.get("evidence_dir_suffix")) != (PRE_REGISTERED, False,
                                                                                  PRE_REGISTERED_SUFFIX):
        problems.append("A5 pre-registration differs")
    return problems


# ---------------------------------------------------------------------------------------------------------------------
# Evidence: the canonical read of one slice.

def load_canonical(path: Path) -> dict:
    """Parsed reader output: assignments, the slice's trade/quote records, counts, errors."""
    out = {"launch": None, "assignments": {}, "trades": [], "records": [], "counts": None, "errors": []}
    for item in read_jsonl(path):
        kind = item.get("kind")
        if kind == "launch":
            out["launch"] = item
        elif kind == "assignment":
            out["assignments"][item["partition"]] = item
        elif kind in ("trade", "quote"):
            out["records"].append(item)
            if kind == "trade":
                out["trades"].append(item)
        elif kind == "counts":
            out["counts"] = item
        elif kind == "error":
            out["errors"].append(item)
    return out


def coverage_problems(read: dict, *, from_ms: int, to_ms: int) -> list[str]:
    """The read must be complete and must cover [from_ms, to_ms] on every partition that carries the slice."""
    problems = []
    if not read["assignments"]:
        problems.append("canonical read has no partition assignment")
    counts = read["counts"]
    if counts is None:
        problems.append("canonical read has no closing counts record (incomplete)")
    elif set(read["assignments"]) - set(counts.get("done_partitions") or []):
        problems.append(f"canonical read did not finish partitions "
                        f"{sorted(set(read['assignments']) - set(counts.get('done_partitions') or []))}")
    if read["errors"]:
        problems.append(f"canonical read logged {len(read['errors'])} errors")
    partitions = sorted({r["partition"] for r in read["records"]})
    if not partitions:
        problems.append("canonical read has no record of the slice")
    for p in partitions:
        times = [r["kafka_ts_ms"] for r in read["records"] if r["partition"] == p]
        if min(times) > from_ms:
            problems.append(f"partition {p}: coverage starts at {min(times)} > {from_ms} (outage start - guard)")
        if max(times) < to_ms:
            problems.append(f"partition {p}: coverage ends at {max(times)} < {to_ms} (first apply)")
    return problems


def first_progress(observations: list[dict], name: str) -> dict | None:
    for item in observations:
        if item.get("kind") == "slice_progress" and item.get("slice") == name and item.get("first_apply_ms"):
            return item
    return None


def retime_slice(name: str, per_slice: dict, restart: dict, observations: list[dict],
                 read: dict | None, read_problems: list[str]) -> dict:
    """A5 for one TRADE slice -> {'applied', 'first_valid_apply_s' (A5 or frozen), ...}."""
    frozen_s = per_slice.get("first_valid_apply_s")
    out = {"slice": name, "frozen_first_valid_apply_s": frozen_s, "applied": False, "problems": list(read_problems)}
    cleared, outage = restart.get("fault_cleared_ms"), restart.get("outage_start_ms")
    progress = first_progress(observations, name)
    if frozen_s is None or progress is None or not cleared or not outage:
        out["problems"].append("no first apply / restart clocks for the slice")
    if read is None:
        out["problems"].append("no canonical evidence for the slice")
    if out["problems"]:
        out["first_valid_apply_s"] = frozen_s
        return out
    first_apply_ms = progress["first_apply_ms"]
    if abs((first_apply_ms - cleared) / 1000 - frozen_s) > 0.0015:
        out["problems"].append("observation first_apply_ms does not match the summary")
    guard_from = outage - PRE_FAULT_GUARD_MS
    out["problems"] += coverage_problems(read, from_ms=guard_from, to_ms=first_apply_ms)
    post = sorted((t for t in read["trades"] if t["kafka_ts_ms"] > guard_from),
                  key=lambda t: (t["kafka_ts_ms"], t["partition"], t["offset"]))
    before = [t for t in read["trades"] if t["kafka_ts_ms"] <= guard_from]
    if not post:
        out["problems"].append("no canonical TRADE event after the fault")
    else:
        applied_ms = progress.get("event_ms")
        if applied_ms not in {t["src_ns"] // 1_000_000 for t in post}:
            out["problems"].append(f"the first applied event (event_ms {applied_ms}) is not a canonical post-fault "
                                   "TRADE event of the slice")
    if out["problems"]:
        out["first_valid_apply_s"] = frozen_s
        return out
    first = post[0]
    start = max(cleared, first["kafka_ts_ms"])
    a5_s = round((first_apply_ms - start) / 1000, 3)
    last_before = max(before, key=lambda t: (t["kafka_ts_ms"], t["offset"])) if before else None
    ids = (last_before or {}).get("trade_id"), first.get("trade_id")
    adjacent = (all(isinstance(i, str) and i.isdigit() for i in ids) and int(ids[1]) - int(ids[0]) == 1) \
        if all(ids) else None
    out.update({
        "applied": True, "first_valid_apply_s": a5_s,
        "clock_start_ms": start, "clock_start_basis": "fault_cleared" if start == cleared else "first_trade_event",
        "fault_cleared_ms": cleared, "outage_start_ms": outage, "pre_fault_guard_ms": PRE_FAULT_GUARD_MS,
        "first_apply_ms": first_apply_ms, "first_applied_event_ms": progress.get("event_ms"),
        "first_post_fault_event": {k: first.get(k) for k in ("partition", "offset", "kafka_ts_ms", "src_ns", "recv_ns",
                                                               "pub_ns", "trade_id", "seq", "conn_gen")},
        "last_pre_fault_event": {k: last_before.get(k) for k in ("partition", "offset", "kafka_ts_ms", "src_ns",
                                                                   "trade_id")} if last_before else None,
        "venue_trade_ids_adjacent_report_only": adjacent,
        "post_fault_events_before_first_apply": sum(1 for t in post if t["kafka_ts_ms"] <= first_apply_ms),
        "first_valid_apply_from_venue_time_s_report_only": round((first_apply_ms - max(cleared, first["src_ns"] //
                                                                                        1_000_000)) / 1000, 3),
    })
    return out


def amend_report(report: dict, observations: list[dict], evidence: dict[str, tuple[dict | None, list[str]]]):
    """(amended report, per-TRADE-slice A5 records). Only first_valid_apply_s of TRADE slices can change."""
    amended = copy.deepcopy(report)
    if amended.get("schema", g11probe.SCHEMA) != g11probe.SCHEMA or amended.get("restart") is None:
        return amended, []
    recovery = amended["recovery"]
    per_slice = recovery["per_slice"]
    records = []
    for name in sorted(per_slice):
        if feed_of(name) != FEED:
            continue
        read, problems = evidence.get(name, (None, []))
        record = retime_slice(name, per_slice[name], amended["restart"], observations, read, problems)
        records.append(record)
        per_slice[name]["first_valid_apply_s"] = record["first_valid_apply_s"]
        if record["applied"]:
            per_slice[name]["a5"] = {"frozen_first_valid_apply_s": record["frozen_first_valid_apply_s"],
                                     "clock_start_ms": record["clock_start_ms"]}
    recovery["first_valid_apply_s"] = quantiles([s["first_valid_apply_s"] for s in per_slice.values()
                                                 if s.get("first_valid_apply_s") is not None])
    return amended, records


# ---------------------------------------------------------------------------------------------------------------------
# Applicability and the evidence of a campaign.

def evidence_for(d: Path, amendment: dict, application: dict | None) -> tuple[dict, list[str]]:
    """({slice: (parsed read or None, problems)}, run-level problems)."""
    reader_pin = ((amendment.get("pinned") or {}).get("reader") or {}).get("sha256")
    if application is not None:
        spec = application["evidence"]
        base = Path(str(d) + spec["dir_suffix"])
        slices = spec["slices"]
    else:
        base = Path(str(d) + PRE_REGISTERED_SUFFIX)
        manifest = gc.load_json(base / "a5-evidence.json")
        if not isinstance(manifest, dict) or manifest.get("schema") != EVIDENCE_SCHEMA \
                or manifest.get("campaign_id") != d.name:
            return {}, [f"no A5 evidence manifest for {d.name} ({base / 'a5-evidence.json'})"]
        slices = manifest.get("slices") or []
    out, run_problems = {}, []
    reader = base / "reader.py"
    if not reader.exists() or gc.sha256_file(reader) != reader_pin:
        run_problems.append("the canonical reader differs from the one A5 pins")
    for spec in slices:
        path = base / spec["canonical"]
        problems = []
        if not path.exists():
            run_problems.append(f"evidence file missing: {path}")
            out[spec["slice"]] = (None, [f"evidence file missing: {path}"])
            continue
        if not spec.get("canonical_sha256") or gc.sha256_file(path) != spec["canonical_sha256"]:
            problems.append("canonical evidence differs from the one A5 pins")
            run_problems.append(f"{spec['slice']}: canonical evidence differs from the one A5 pins")
        out[spec["slice"]] = (load_canonical(path), problems)
    for name, (parsed, problems) in out.items():
        problems.extend(p for p in run_problems if p not in problems)
    return out, run_problems


def applicability(d: Path, stored: dict | None, frozen: dict, amendment: dict) -> tuple[dict | None, list[str]]:
    """(the post-hoc application for this run or None for pre-registered, problems)."""
    problems = []
    pins = amendment.get("pinned") or {}
    for name, path in CODE_PINS.items():
        if gc.sha256_file(ROOT / path) != (pins.get(name) or {}).get("sha256"):
            problems.append(f"{path} differs from the one A5 pins")
    if stored is None or canonical(stored) != canonical(frozen):
        problems.append("the frozen scorer does not reproduce the stored scorecard.json")
    application = next((p for p in amendment.get("post_hoc_applications") or [] if p["campaign_id"] == d.name), None)
    g11_dir = d / "g11"
    if application is not None:
        for key, path in (("scorecard_sha256", d / "scorecard.json"),
                          ("g11_summary_sha256", g11_dir / "g11-summary.json"),
                          ("g11_observations_sha256", g11_dir / "g11-observations.jsonl")):
            if not path.exists() or gc.sha256_file(path) != application[key]:
                problems.append(f"{path.name} differs from the one A5 pins")
    else:
        campaign = gc.load_json(d / "campaign.json") or {}
        started = campaign.get("started_unix")
        if not isinstance(started, (int, float)) or started <= amendment["frozen_at_unix"]:
            problems.append(f"{d.name} is not named by A5 and did not start after the A5 freeze (not pre-registered)")
    return application, problems


@contextlib.contextmanager
def patched_verdict(fn):
    saved = g11probe.verdict
    try:
        g11probe.verdict = fn
        yield
    finally:
        g11probe.verdict = saved


def rescore(campaign_dir, amendment_path=A5_PATH, *, force_report_only: bool = False) -> dict:
    """{'applied', 'report_only', 'problems', 'frozen', 'card', 'slices'}; card is None when A5 is not applicable."""
    d = Path(campaign_dir)
    amendment = gc.load_json(Path(amendment_path))
    problems = validate_a5(amendment) if amendment is not None else ["unreadable A5"]
    frozen = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    if problems:
        return {"applied": False, "report_only": force_report_only, "problems": problems, "frozen": frozen,
                "card": None, "slices": []}
    stored = gc.load_json(d / "scorecard.json")
    application, gate_problems = applicability(d, stored, frozen, amendment)
    evidence, evidence_problems = evidence_for(d, amendment, application)
    gate_problems += evidence_problems
    if gate_problems and not force_report_only:
        return {"applied": False, "report_only": False, "problems": gate_problems, "frozen": frozen, "card": None,
                "slices": []}
    obs_path = d / "g11" / "g11-observations.jsonl"
    observations = list(read_jsonl(obs_path)) if obs_path.exists() else []
    original = g11probe.verdict
    seen = []

    def verdict(report):
        amended, records = amend_report(report, observations, evidence)
        seen.append({"amended": amended, "records": records})
        return original(amended)

    with patched_verdict(verdict):
        card = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    frozen_gates = {g["id"]: g for g in frozen["gates"]}
    drift = [g["id"] for g in card["gates"]
             if g["id"] not in SCOPED_GATES and canonical(g) != canonical(frozen_gates.get(g["id"]))]
    if drift:
        raise RuntimeError(f"A5 changed gates outside its scope: {drift}")
    records = seen[-1]["records"] if seen else []
    amended = seen[-1]["amended"] if seen else None
    label, post_hoc = (POST_HOC, True) if application is not None else (PRE_REGISTERED, False)
    source = {"path": str(Path(amendment_path)), "sha256": gc.sha256_file(Path(amendment_path))}
    for gate in card["gates"]:
        if gate["id"] == "G11":
            gate["rule"] += ("; A5 (" + label + "): a TRADE slice's first-valid-apply clock starts at max(fault "
                             "cleared, DL Kafka time of its first canonical TRADE event after the fault - 3 s guard), "
                             "with canonical-topic evidence; caught up / no rewind / cursor / other slices unchanged")
            gate["a5"] = {**source, "amendment_id": A5_ID, "label": label, "post_hoc": post_hoc,
                          "verdict_without_a5": frozen_gates["G11"]["verdict"],
                          "reason_without_a5": frozen_gates["G11"]["reason"],
                          "first_valid_apply_s_without_a5": ((frozen_gates["G11"].get("measured") or {}).get(
                              "recovery") or {}).get("first_valid_apply_s"),
                          "first_valid_apply_s_with_a5": (amended or {}).get("recovery", {}).get(
                              "first_valid_apply_s"),
                          "trade_slices": records}
    card["schema"] = OUTPUT_SCHEMA
    card["a5"] = {**source, "amendment_id": A5_ID, "label": label, "post_hoc": post_hoc,
                  "report_only": bool(gate_problems), "problems": gate_problems,
                  "scorecard_without_a5_sha256": gc.sha256_file(d / "scorecard.json"),
                  "code_sha256": {name: gc.sha256_file(ROOT / path) for name, path in CODE_PINS.items()},
                  "summary_without_a5": frozen["summary"],
                  "applied_slices": [r["slice"] for r in records if r["applied"]],
                  "statement": ("A5 was adopted after this run was scored (post hoc); the verdict without A5 stays the "
                                "record of the frozen rule and is shown next to the A5 verdict" if post_hoc else
                                "A5 was frozen before this run started (pre-registered); the verdict without A5 is "
                                "shown next to the A5 verdict")}
    return {"applied": not gate_problems, "report_only": bool(gate_problems), "problems": gate_problems,
            "frozen": frozen, "card": card, "slices": records}


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
    parser.add_argument("--amendment", type=Path, default=A5_PATH)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="create scorecard.a5.json (+ .sha256), never overwrite")
    mode.add_argument("--force-report-only", action="store_true", help="score another receipt for comparison only")
    args = parser.parse_args(argv)
    result = rescore(args.campaign_dir, args.amendment, force_report_only=args.force_report_only)
    card = result["card"]
    report = {"campaign": args.campaign_dir.name, "applied": result["applied"], "report_only": result["report_only"],
              "problems": result["problems"], "summary_without_a5": result["frozen"]["summary"],
              "summary_with_a5": card["summary"] if card else None,
              "trade_slices_applied": [(r["slice"], r["frozen_first_valid_apply_s"], r["first_valid_apply_s"])
                                       for r in result["slices"] if r["applied"]]}
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

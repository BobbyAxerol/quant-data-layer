#!/usr/bin/env python3
"""P18.4 stage-100 amendment A4 v2: the A4 rules PRE-REGISTERED for the final stage-100 run, bound to its runtime.

Owner decisions:
* AskUserQuestion 2026-10-18 ~16:40Z, "Duyệt A4 chốt trước (Recommended)": pre-register A4 v2 (post_hoc=false) for the
  final stage-100 run, frozen before load, pinned to the run's runtime identity, every other gate unchanged, the verdict
  without A4 reported next to the verdict with A4, NO external exemption beyond A3.
* The rule content is exactly the A4 the owner approved at ~14:45Z (config/v2/p184-stage100-a4-ts-eligibility.json, A4
  v1, which stays the post-hoc decision for run p184-final-campaign-20261018T123222Z only):
  - G4 book_delta / book_snapshot / quote |OKX_INVERSE|: gated by the pooled OKX row p95 limit (289.1 / 270.1 / 283.3
    ms) on windows with >= 20 samples, the window max REPORT_ONLY, every admitted event above the replaced addendum
    max limit (282.5 / 300.7 / 322.6 ms) retained by observer v7 (else FAIL);
  - G2 MARK_INDEX_PRICE|BINANCE: max <= 1200 ms, p95 <= 500 ms unchanged.
  The rule functions are the A4 v1 ones (scripts/p184_a4_post_hoc.py, sha256 pinned); this module only relabels their
  rows (label PRE_REGISTERED, post_hoc false, version 2), so A4 v1 and A4 v2 cannot judge the same data differently.

Why a wrapper and not an edit of scripts/p184_gate_campaign.py: A3 pins the sha256 of the scorer it runs with (A3_CODE
includes the campaign tool; the preflight refuses a run whose working apparatus differs, and the score drops A3, and
with it the re-pinned G4-A1/G4-A2, when the tool differs). An edited scorer would therefore remove A1-A3 from the final
run. The scorer stays byte-identical; A4 v2 runs it unchanged (it must reproduce the stored scorecard.json) and once
more with score_mark / score_g4 wrapped, and refuses to write when a gate outside G2/G4 changes.

Runtime-identity pin (consistent with A3's, but without digests that do not exist yet): A3 pins hard-coded image ids
taken at its freeze, and the campaign records the live ones (campaign.json a3_runtime_images, runtime_identity) at
preflight. The final run follows the rustls + resource roll, so its image ids are unknown when A4 v2 is frozen.
A4 v2 is therefore bound to the run by two receipts written next to the campaign directory by this tool:
  1. `bind` (the launcher's FIRST command, before the guard and `p184_gate_campaign.py run`): refuses unless the A4 v2
     file is valid and frozen, the code pins match the working files and the campaign directory does not exist yet;
     records (open "x") <campaign>.a4-v2-binding.json with the A4 v2 sha256, the code sha256s, the image id of every
     A3 runtime role + the writer (the same docker inspect .Image as gc.a3_runtime_images()), the writer identity in
     the preflight's shape, and the cgroup caps (NanoCpus / CpuQuota / CpuPeriod / CpusetCpus / Memory / MemorySwap) of
     those roles and kafka1-3.
  2. `close` (after the campaign wrote scorecard.json): records <campaign>.a4-v2-close.json with the binding sha256 and
     the same images and caps read again.
A4 v2 applies only when: A4 v2 frozen_at < binding created < campaign started_unix; the A4 v2 file is unchanged since
the binding; the campaign's preflight-recorded runtime_identity and a3_runtime_images equal the binding's; the close
images and caps equal the binding's (no roll or cap change during the run; the G11 writer restart keeps the image and
caps); the scorer, the A4 rule module, this tool and the observer have the pinned sha256 (the campaign tool_sha256
included); the campaign's budget / addendum / G4-A1 / G4-A2 sha256 are the pinned ones; the campaign is a 100-session
candidate not excluded by name (123222Z is); the frozen scorer reproduces the stored scorecard.json; A3 was applied;
protocol met; G6 PASS; G9 PASS with 0 data-path eligibility losses. Anything missing -> not applied (strict scoring is
the record). A3 itself is not pinned by sha256: its hard-coded runtime_images must be re-frozen for the rolled images,
and A4 v2 requires only that the campaign applied the A3 it recorded (reported in the receipt).

Boundary: `bind` / `close` only run read-only `docker inspect` and create their own receipt; `score --write` creates
(open "x", never overwrites) <campaign>/scorecard.a4-v2.json and its .sha256. No container, Redis, Kafka or runtime
action; the stored scorecard.json is never written.
usage: p184_a4_preregistered.py bind --campaign-dir DIR | close --campaign-dir DIR |
       score --campaign-dir DIR [--write | --force-report-only]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import p184_a4_post_hoc as rules  # noqa: E402
from scripts import p184_gate_campaign as gc  # noqa: E402

A4V2_PATH = ROOT / "config/v2/p184-stage100-a4-v2-preregistered.json"
A4V2_SCHEMA = "qdl.p184.stage100-a4.v2"
A4V2_VERSION = 2
A4_ID = "A4"
LABEL = "PRE_REGISTERED"
BASIS = "a4_v2_preregistered"
BINDING_SCHEMA = "qdl.p184.stage100-a4-v2-binding.v1"
CLOSE_SCHEMA = "qdl.p184.stage100-a4-v2-close.v1"
BINDING_SUFFIX = ".a4-v2-binding.json"
CLOSE_SUFFIX = ".a4-v2-close.json"
OUTPUT_NAME = "scorecard.a4-v2.json"
OUTPUT_SCHEMA = "qdl.p184.gate-campaign-scorecard.a4-v2.v1"
SELECTION = "RUN_BOUND_BEFORE_LOAD"
# Code whose sha256 A4 v2 pins (working file at bind and at score time, and the campaign's tool_sha256 for the scorer).
CODE_PINS = {"scorer": "scripts/p184_gate_campaign.py", "a4_rules": "scripts/p184_a4_post_hoc.py",
             "rescore_tool": "scripts/p184_a4_preregistered.py", "g4_observer": "scripts/p184_g4_observer.py"}
# A4 v2 pin -> the sha256 field the campaign records at run time.
CONFIG_PINS = {"budget": "budget_sha256", "g4_addendum": "g4_addendum_sha256",
               "g4_amendment_1": "g4_amendment_sha256", "g4_amendment_2": "g4_amendment_2_sha256"}
RESOURCE_KEYS = ("NanoCpus", "CpuQuota", "CpuPeriod", "CpusetCpus", "Memory", "MemorySwap")
RESOURCE_ROLES = gc.A3_RUNTIME_ROLES + ("kafka1", "kafka2", "kafka3")
SCOPED_GATES = rules.SCOPED_GATES


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def binding_path_for(d: Path) -> Path:
    return Path(str(Path(d)) + BINDING_SUFFIX)


def close_path_for(d: Path) -> Path:
    return Path(str(Path(d)) + CLOSE_SUFFIX)


def code_sha256() -> dict:
    return {name: gc.sha256_file(ROOT / path) for name, path in CODE_PINS.items()}


def validate_a4v2(amendment) -> list[str]:
    """Problems that keep A4 v2 from being applied (empty list = the file states exactly the owner decisions)."""
    if not isinstance(amendment, dict) or amendment.get("schema") != A4V2_SCHEMA:
        return ["unknown A4 v2 schema"]
    problems = []
    if (amendment.get("amendment_id"), amendment.get("version"), amendment.get("label"),
            amendment.get("post_hoc")) != (A4_ID, A4V2_VERSION, LABEL, False):
        problems.append("A4 v2 id, version, label or post_hoc flag differs from the owner decision")
    if not amendment.get("decision_owner") or not isinstance(amendment.get("frozen_at_unix"), (int, float)):
        problems.append("A4 v2 is not frozen with a decision owner")
    if amendment.get("external_exemptions") != []:
        problems.append("A4 v2 states an external exemption (the owner decision adds none beyond A3)")
    g2 = (amendment.get("rules") or {}).get("G2") or {}
    if (g2.get("row"), g2.get("max_le_ms"), g2.get("p95_le_ms")) != (rules.G2_ROW, rules.G2_MAX_LE_MS,
                                                                       rules.G2_P95_LE_MS):
        problems.append("A4 v2 G2 rule differs from the owner decision")
    g4 = (amendment.get("rules") or {}).get("G4") or {}
    rows = {key: (spec.get("pooled_row"), spec.get("p95_limit_ms"), spec.get("replaced_addendum_max_limit_ms"))
            for key, spec in (g4.get("rows") or {}).items()}
    if rows != rules.G4_ROWS or g4.get("min_samples_per_window") != rules.G4_MIN_SAMPLES \
            or g4.get("max") != "REPORT_ONLY":
        problems.append("A4 v2 G4 rule differs from the owner decision")
    applies = amendment.get("applies_to") or {}
    if (applies.get("selection"), applies.get("role"), applies.get("sessions")) != (SELECTION, "candidate", 100) \
            or not isinstance(applies.get("excluded_campaigns"), list):
        problems.append("A4 v2 does not bind to a 100-session candidate run before load")
    pins = amendment.get("pinned") or {}
    if any(not (pins.get(name) or {}).get("sha256") for name in (*CODE_PINS, *CONFIG_PINS)):
        problems.append("A4 v2 does not pin its code and configuration")
    return problems


def _resources(state: dict | None) -> dict | None:
    if not state:
        return None
    host = state.get("HostConfig") or {}
    return {key: host.get(key) for key in RESOURCE_KEYS}


def capture_identity(writer: str = gc.WRITER) -> dict:
    """Read-only docker inspect: image id per A3 runtime role + writer (as gc.a3_runtime_images()), the writer
    identity in the preflight's runtime_identity shape, and the cgroup caps of the A3 roles, kafka1-3 and the writer."""
    images = gc.a3_runtime_images() if writer == gc.WRITER else {
        **{k: v for k, v in gc.a3_runtime_images().items() if k != "writer"},
        "writer": (gc.docker_inspect(writer) or {}).get("Image")}
    state = gc.docker_inspect(writer)
    identity = ({"writer_image": state["Image"], "writer_started": state["State"]["StartedAt"],
                 "writer_restarts": state["RestartCount"]} if state else None)
    resources = {role: _resources(gc.docker_inspect(f"qdl_v2_stable_candidate-{role}-1")) for role in RESOURCE_ROLES}
    resources["writer"] = _resources(state)
    return {"runtime_images": images, "runtime_identity": identity, "runtime_resources": resources,
            "writer_running": bool(state and state["State"].get("Running"))}


def _write_new(path: Path, value: dict) -> str:
    with Path(path).open("x") as out:
        out.write(json.dumps(value, indent=1, sort_keys=True) + "\n")
    return gc.sha256_file(Path(path))


def bind(campaign_dir, *, amendment_path=A4V2_PATH, writer: str = gc.WRITER, identity: dict | None = None,
         now: float | None = None) -> tuple[Path, dict]:
    """Pre-load binding receipt <campaign>.a4-v2-binding.json; raises (nothing written) when A4 v2 cannot bind."""
    d = Path(campaign_dir).resolve()
    now = time.time() if now is None else now
    amendment = gc.load_json(Path(amendment_path))
    problems = validate_a4v2(amendment) if amendment is not None else ["unreadable A4 v2"]
    if not problems:
        if not amendment["frozen_at_unix"] < now:
            problems.append("A4 v2 is not frozen before the binding")
        if d.name in amendment["applies_to"]["excluded_campaigns"]:
            problems.append(f"A4 v2 excludes {d.name}")
        current = code_sha256()
        pinned = {name: (amendment["pinned"].get(name) or {}).get("sha256") for name in CODE_PINS}
        if current != pinned:
            problems.append(f"working code differs from the A4 v2 pins: "
                            f"{sorted(k for k in CODE_PINS if current[k] != pinned[k])}")
    if d.exists():
        problems.append(f"{d} exists: the binding must precede the campaign")
    identity = capture_identity(writer) if identity is None else identity
    missing = sorted(k for k, v in (identity.get("runtime_images") or {}).items() if not v)
    if missing or not identity.get("runtime_identity") or not identity.get("writer_running"):
        problems.append(f"runtime identity incomplete (missing images {missing}, writer running "
                        f"{identity.get('writer_running')})")
    if problems:
        raise RuntimeError("A4 v2 not bound: " + "; ".join(problems))
    binding = {"schema": BINDING_SCHEMA, "campaign_id": d.name, "campaign_dir": str(d), "created_unix": now,
               "created_at": dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
               "a4_v2": {"path": str(Path(amendment_path).resolve()), "sha256": gc.sha256_file(Path(amendment_path)),
                         "frozen_at_unix": amendment["frozen_at_unix"]},
               "code_sha256": code_sha256(), "writer": writer,
               **{k: identity[k] for k in ("runtime_images", "runtime_identity", "runtime_resources")},
               "statement": "written before the campaign directory existed, i.e. before preflight and load"}
    path = binding_path_for(d)
    _write_new(path, binding)
    return path, binding


def close(campaign_dir, *, identity: dict | None = None, now: float | None = None) -> tuple[Path, dict]:
    """Post-run receipt <campaign>.a4-v2-close.json (images and caps read again); raises when there is no binding or
    the campaign has not written scorecard.json yet."""
    d = Path(campaign_dir).resolve()
    bpath = binding_path_for(d)
    binding = gc.load_json(bpath)
    if not isinstance(binding, dict) or binding.get("schema") != BINDING_SCHEMA:
        raise RuntimeError(f"A4 v2 close: no binding at {bpath}")
    if not (d / "scorecard.json").exists():
        raise RuntimeError("A4 v2 close: the campaign has not written scorecard.json")
    identity = capture_identity(binding.get("writer") or gc.WRITER) if identity is None else identity
    now = time.time() if now is None else now
    record = {"schema": CLOSE_SCHEMA, "campaign_id": d.name, "binding_sha256": gc.sha256_file(bpath),
              "captured_unix": now, "runtime_images": identity.get("runtime_images"),
              "runtime_resources": identity.get("runtime_resources"),
              "runtime_identity_after": identity.get("runtime_identity")}
    path = close_path_for(d)
    _write_new(path, record)
    return path, record


def applicability(d: Path, campaign: dict, stored: dict | None, frozen: dict, amendment: dict, amendment_path: Path,
                  binding: dict | None, binding_path: Path, close_record: dict | None) -> list[str]:
    problems = []
    if not isinstance(binding, dict) or binding.get("schema") != BINDING_SCHEMA:
        return [f"no A4 v2 binding at {binding_path} (A4 v2 is bound before load or not at all)"]
    if binding.get("campaign_id") != d.name:
        problems.append(f"the binding names {binding.get('campaign_id')}, not {d.name}")
    if gc.sha256_file(amendment_path) != (binding.get("a4_v2") or {}).get("sha256"):
        problems.append("A4 v2 changed after the binding")
    created, started = binding.get("created_unix"), campaign.get("started_unix")
    if not (isinstance(created, (int, float)) and isinstance(started, (int, float))
            and amendment["frozen_at_unix"] < created < started):
        problems.append("not frozen_at < binding < campaign start (A4 v2 must be frozen and bound before the run)")
    applies = amendment["applies_to"]
    if d.name in applies["excluded_campaigns"]:
        problems.append(f"A4 v2 excludes {d.name} (A4 v1 post-hoc is its decision)")
    if campaign.get("role") != applies["role"] or campaign.get("sessions") != applies["sessions"]:
        problems.append("campaign is not a 100-session candidate")
    if campaign.get("runtime_identity") != binding.get("runtime_identity"):
        problems.append("writer identity at preflight differs from the binding")
    if not campaign.get("a3_runtime_images") or campaign.get("a3_runtime_images") != binding.get("runtime_images"):
        problems.append("runtime images at preflight differ from the binding")
    if not isinstance(close_record, dict) or close_record.get("schema") != CLOSE_SCHEMA:
        problems.append("no A4 v2 close receipt (images and caps after the run unknown)")
    else:
        if close_record.get("binding_sha256") != gc.sha256_file(binding_path) or close_record.get(
                "campaign_id") != d.name:
            problems.append("the close receipt is not the one of this binding")
        if close_record.get("runtime_images") != binding.get("runtime_images"):
            problems.append("runtime images changed during the run")
        if close_record.get("runtime_resources") != binding.get("runtime_resources"):
            problems.append("runtime cgroup caps changed during the run")
    pins = amendment["pinned"]
    current = code_sha256()
    for name in CODE_PINS:
        if not (current[name] == pins[name]["sha256"] == (binding.get("code_sha256") or {}).get(name)):
            problems.append(f"{CODE_PINS[name]} differs from the A4 v2 pin (now or at bind)")
    if campaign.get("tool_sha256") != pins["scorer"]["sha256"]:
        problems.append("campaign tool differs from the scorer A4 v2 pins")
    for name, field in CONFIG_PINS.items():
        if campaign.get(field) != pins[name]["sha256"]:
            problems.append(f"campaign {field} differs from the A4 v2 pin")
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


def _relabel(row: dict) -> dict:
    return {**row, "basis": BASIS, "amendment_id": A4_ID, "amendment_version": A4V2_VERSION, "label": LABEL,
            "post_hoc": False}


def v2_score_g4(retention: list[dict]):
    inner = rules.a4_score_g4(gc.score_g4, retention)

    def score(*args, **kwargs):
        verdict, rows, why = inner(*args, **kwargs)
        return verdict, [_relabel(r) if r.get("basis") == "a4_post_hoc" else r for r in rows], why
    return score


def v2_score_mark():
    inner = rules.a4_score_mark(gc.score_mark)

    def score(mark):
        verdict, rows, why = inner(mark)
        rows = [{**r, "a4": {**r["a4"], "amendment_version": A4V2_VERSION, "label": LABEL, "post_hoc": False}}
                if r.get("a4") else r for r in rows]
        return verdict, rows, why
    return score


def rescore(campaign_dir, *, amendment_path: Path | None = None, binding_path: Path | None = None,
            close_path: Path | None = None, force_report_only: bool = False) -> dict:
    """{'applied', 'report_only', 'problems', 'frozen', 'card'}; card is None when A4 v2 is not applicable (and not
    forced report-only). Paths default to the campaign's sibling receipts and the A4 v2 file the binding recorded."""
    d = Path(campaign_dir)
    binding_path = binding_path_for(d) if binding_path is None else Path(binding_path)
    close_path = close_path_for(d) if close_path is None else Path(close_path)
    binding = gc.load_json(binding_path) if binding_path.exists() else None
    if amendment_path is None:
        recorded = ((binding or {}).get("a4_v2") or {}).get("path")
        amendment_path = Path(recorded) if recorded else A4V2_PATH
    amendment_path = Path(amendment_path)
    amendment = gc.load_json(amendment_path)
    frozen = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    problems = validate_a4v2(amendment) if amendment is not None else ["unreadable A4 v2"]
    if problems:
        return {"applied": False, "report_only": force_report_only, "problems": problems, "frozen": frozen,
                "card": None}
    campaign = gc.load_json(d / "campaign.json") or {}
    stored = gc.load_json(d / "scorecard.json")
    close_record = gc.load_json(close_path) if close_path.exists() else None
    gate_problems = applicability(d, campaign, stored, frozen, amendment, amendment_path, binding, binding_path,
                                  close_record)
    if gate_problems and not force_report_only:
        return {"applied": False, "report_only": False, "problems": gate_problems, "frozen": frozen, "card": None}
    with rules.patched(score_mark=v2_score_mark(), score_g4=v2_score_g4(rules.load_retention(d, campaign))):
        card = json.loads(json.dumps(gc.build_scorecard(d), default=str))
    frozen_gates = {g["id"]: g for g in frozen["gates"]}
    drift = [g["id"] for g in card["gates"]
             if g["id"] not in SCOPED_GATES and canonical(g) != canonical(frozen_gates.get(g["id"]))]
    if drift:
        raise RuntimeError(f"A4 v2 changed gates outside its scope: {drift}")
    source = {"path": str(amendment_path), "sha256": gc.sha256_file(amendment_path)}
    tag = {"amendment_id": A4_ID, "amendment_version": A4V2_VERSION, "label": LABEL, "post_hoc": False}
    for gate in card["gates"]:
        if gate["id"] in SCOPED_GATES:
            gate["rule"] += ("; A4 v2 (PRE_REGISTERED): MARK_INDEX_PRICE|BINANCE max <= 1200 ms, p95 <= 500 ms"
                             if gate["id"] == "G2" else
                             "; A4 v2 (PRE_REGISTERED): book_delta / book_snapshot / quote |OKX_INVERSE| gated by the "
                             "pooled OKX row p95 limit (289.1 / 270.1 / 283.3 ms) on windows with >= 20 samples, max "
                             "report-only, every event above the replaced addendum limit retained")
            gate["a4"] = {**source, **tag, "verdict_without_a4": frozen_gates[gate["id"]]["verdict"],
                          "reason_without_a4": frozen_gates[gate["id"]]["reason"]}
    card["schema"] = OUTPUT_SCHEMA
    card["a4"] = {**source, **tag, "report_only": bool(gate_problems), "problems": gate_problems,
                  "external_exemptions": [],
                  "binding": {"path": str(binding_path), "sha256": gc.sha256_file(binding_path)},
                  "close": {"path": str(close_path), "sha256": gc.sha256_file(close_path)},
                  "frozen_at_unix": amendment["frozen_at_unix"],
                  "bound_at_unix": (binding or {}).get("created_unix"),
                  "campaign_started_unix": campaign.get("started_unix"),
                  "runtime_images": (binding or {}).get("runtime_images"),
                  "runtime_identity": (binding or {}).get("runtime_identity"),
                  "a3_recorded": {"path": campaign.get("a3_path"), "sha256": campaign.get("a3_sha256"),
                                  "applied": bool((frozen.get("a3") or {}).get("applied"))},
                  "scorecard_without_a4_sha256": gc.sha256_file(d / "scorecard.json"),
                  "code_sha256": code_sha256(), "summary_without_a4": frozen["summary"],
                  "statement": "A4 v2 was frozen and bound to this run's runtime identity before the run started; the "
                               "verdicts without A4 are the frozen-rule record and are shown next to the A4 verdicts"}
    return {"applied": not gate_problems, "report_only": bool(gate_problems), "problems": gate_problems,
            "frozen": frozen, "card": card}


def write_receipt(d: Path, card: dict) -> tuple[Path, str]:
    path = Path(d) / OUTPUT_NAME
    with path.open("x") as out:
        out.write(json.dumps(card, indent=2, sort_keys=True) + "\n")
    digest = gc.sha256_file(path)
    with (Path(d) / (OUTPUT_NAME + ".sha256")).open("x") as out:
        out.write(f"{digest}  {OUTPUT_NAME}\n")
    return path, digest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("bind", help="first launcher command: freeze the run binding before the campaign exists")
    b.add_argument("--campaign-dir", type=Path, required=True)
    b.add_argument("--amendment", type=Path, default=A4V2_PATH)
    b.add_argument("--writer", default=gc.WRITER)
    c = sub.add_parser("close", help="after the campaign wrote scorecard.json: images and caps read again")
    c.add_argument("--campaign-dir", type=Path, required=True)
    s = sub.add_parser("score")
    s.add_argument("--campaign-dir", type=Path, required=True)
    mode = s.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="create scorecard.a4-v2.json (+ .sha256), never overwrite")
    mode.add_argument("--force-report-only", action="store_true", help="score another receipt for comparison only")
    args = parser.parse_args(argv)
    try:
        if args.command == "bind":
            path, binding = bind(args.campaign_dir, amendment_path=args.amendment, writer=args.writer)
            print(json.dumps({"binding": str(path), "sha256": gc.sha256_file(path),
                              "runtime_images": binding["runtime_images"]}, indent=1))
            return 0
        if args.command == "close":
            path, record = close(args.campaign_dir)
            print(json.dumps({"close": str(path), "sha256": gc.sha256_file(path)}, indent=1))
            return 0
    except (RuntimeError, FileExistsError) as exc:  # a receipt is never overwritten
        print(json.dumps({"refused": str(exc)}), file=sys.stderr)
        return 2
    result = rescore(args.campaign_dir, force_report_only=args.force_report_only)
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

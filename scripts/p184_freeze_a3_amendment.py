#!/usr/bin/env python3
"""Freeze P18.4 stage-100 amendment A3 (external attribution of G2 tails, G4 samples, G9 losses) BEFORE the run.

Owner decision: AskUserQuestion 2026-10-18 ~10:10Z, "Amendment A3 quy nguyen nhan (Recommended)": a frozen, versioned
external-attribution amendment and ONE final stage-100. Codex reviews folded in: p184-idr112-113-provider-gates-review-
20261018.md "Gate Amendment Options" and p184-release-closure-options-20261018.md sections 1-4 (same-event evidence,
UNKNOWN is not EXTERNAL, retained exceedances, joined lane timelines, recovery budgets, frozen stop rules).

What it writes (once, open "x"): config/v2/p184-stage100-a3-external-attribution.json with the rule text, the limits
(per-row G4 internal-segment limits recomputed from the addendum's frozen BASELINE campaign: baseline max x 1.2, the
addendum method - never from a candidate), the caps and recovery budget, the stop rules, the workload, the runtime
images it pins (docker inspect, read-only), the profile image, and old/new sha256 of every apparatus file (old = at
--base-ref). A3 re-pins amendments 1 and 2 (unchanged files) to the new campaign tool. Read-only otherwise.
usage: p184_freeze_a3_amendment.py --base-ref REF [--output P]
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

from scripts import p184_a3_attribution as a3  # noqa: E402
from scripts import p184_gate_campaign as gc  # noqa: E402
from scripts.p184_resp_readonly import read_jsonl  # noqa: E402

DECISION = {
    "decision_owner": "Bobby (owner)",
    "approval_reference": "AskUserQuestion 2026-10-18 ~10:10Z, option \"Amendment A3 quy nguyen nhan (Recommended)\": "
                          "frozen versioned external-attribution amendment A3 + one final stage-100",
    "codex_guidance": ["/home/bobby/.worktrees/ts-codex-p184-review/upgrade/p184-idr112-113-provider-gates-review-"
                       "20261018.md section \"Gate Amendment Options\"",
                       "/home/bobby/.worktrees/ts-codex-p184-review/upgrade/p184-release-closure-options-20261018.md "
                       "sections 1-4"],
    "rationale_doc": "upgrade/evidence/releases/v2.2.4/a3-external-attribution.md",
}
APPARATUS = {
    "gate_campaign_tool": "scripts/p184_gate_campaign.py",
    "a3_attribution": "scripts/p184_a3_attribution.py",
    "a3_evidence": "scripts/p184_a3_evidence.py",
    "g4_observer": "scripts/p184_g4_observer.py",
    "okx_mark_index_probe": "scripts/p184_g4_okx_mark_index_probe.py",
    "okx_books_probe": "scripts/p184_g4_okx_books_probe.py",
    "binance_probe": "scripts/p184_g4_binance_probe.py",
    "driver": "scripts/phase3_consumer_load_acceptance.py",
    "driver_bootstrap": "scripts/phase3_consumer_load_bootstrap.sh",
    "reference_verifier": "qdl/certification/reference_l2_acceptance.py",
    "g9_g10_sampler": "scripts/p184_g9_g10_writer_counters.py",
    "trace_transit": "scripts/p184_trace_feed_transit.py",
    "mark_trace_transit": "scripts/p184_g2_mark_trace_transit.py",
    "foreign_activity_guard": "scripts/p184_foreign_activity_guard.py",
    "codex_pause_watch": "scripts/p184_codex_pause_watch.py",
    "host_measurement_lock": "scripts/host_measurement_lock.py",
    "freeze_tool": "scripts/p184_freeze_a3_amendment.py",
    "g4_amendment_2": "config/v2/p184-stage100-g4-amendment-2.json",
    "g4_amendment_1": "config/v2/p184-stage100-g4-amendment-1.json",
    "g4_addendum": "config/v2/p184-stage100-g4-addendum.json",
    "budget": "config/v2/p184-stage100-budget.json",
}
UNCHANGED = ("budget", "g4_addendum", "g4_amendment_1", "g4_amendment_2")
FACTOR = 1.2   # the addendum's baseline factor (G4_ADDENDUM_FACTOR)
FIXED_LIMITS = {
    "probe_healthy_ms": 150.0,             # = p184_g4_okx_books_probe.HEALTHY_MAX_MS (fresh connection on time)
    "lane_late_margin_ms": 50.0,           # production lane later than the same event on the fresh lane by >= this
    "reset_pad_s": 2.0,                    # docker log timestamp vs fault instant, both sides
    "episode_merge_s": 60.0,               # faults of one lane <= 60 s apart are one interruption episode
    "recovery_budget_s": 60.0,             # owner: recovery > 60 s -> FAIL
    "max_external_fraction": 0.01,         # owner: EXTERNAL > 1 % of a row's samples -> FAIL
    "max_external_episodes_per_venue": 3,  # more interruption episodes per venue in one load -> FAIL (G9)
    "ingestor_runq_wait_ms_max_thread": 50.0,  # worst ingestor thread run-queue wait per second
    "rx_persist_samples": 1,               # any sample with unread bytes on an ingestor socket (review finding 5)
    "clock_bracket_s": 60.0,               # host clock samples (every 30 s) must bracket the unit within 60 s
    "host_clock_bound_ms": 10.0,           # chrony |offset| + dispersion + delay/2 on both sides
    "core_raw_age_ms_max": 500.0,          # DL receipt -> core normalize, every core line of the recovery seconds
    "gateway_dispatch_lag_ms_max": 500.0,  # canonical CreateTime -> gateway dispatch, highest delta bucket bound
    "gateway_lag_bounds_ms": [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000],  # qdl-stream-gateway hub.rs:317
}


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def git_sha256(ref: str, path: str) -> str | None:
    out = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=ROOT, capture_output=True)
    return hashlib.sha256(out.stdout).hexdigest() if out.returncode == 0 else None


def baseline_segment_limits(baseline_dir: Path) -> tuple[dict, dict]:
    """Per non-bar G4 row: baseline max over the baseline windows of the same-event split (ingest -> producer
    timestamp, producer timestamp -> observer receipt) x FACTOR. Rows without baseline samples get none (A3 UNKNOWN)."""
    maxima, sources = {}, {}
    for obs in sorted(Path(baseline_dir).glob("g4-*/observations.jsonl")):
        split = gc.g4_window_receipt(read_jsonl(obs))["produce_split"]
        sources[str(obs.relative_to(baseline_dir))] = sha256(obs)
        for key, row in split.items():
            if key.startswith("bar|") or key == gc.G4_AMENDMENT_ROW:
                # bars: REST edges batch by design; the paired OKX mark+index row: its received_at is the OLDEST
                # component receipt (venue cadence inside), amendment 1 governs it. No limit -> A3 UNKNOWN.
                continue
            m = maxima.setdefault(key, {"rc": None, "cc": None, "n": 0})
            for name, field in (("rc", "ingest_to_producer_ts_ms"), ("cc", "producer_ts_to_receipt_ms")):
                value = (row.get(field) or {}).get("max")
                if value is not None:
                    m[name] = value if m[name] is None else max(m[name], value)
            m["n"] += int((row.get("ingest_to_producer_ts_ms") or {}).get("n") or 0)
    limits = {key: {"receive_to_canonical_ms": round(m["rc"] * FACTOR, 1),
                    "canonical_to_consumer_ms": round(m["cc"] * FACTOR, 1),
                    "baseline_max_ms": {"receive_to_canonical": m["rc"], "canonical_to_consumer": m["cc"]},
                    "baseline_samples": m["n"]}
              for key, m in sorted(maxima.items()) if m["rc"] is not None and m["cc"] is not None and m["n"]}
    return limits, sources


def profile_image(profile_path: Path) -> dict:
    profile = json.loads(Path(profile_path).read_text())
    image = profile.get("image")
    image_id = subprocess.run(["docker", "image", "inspect", image, "--format", "{{.Id}}"], capture_output=True,
                              text=True, timeout=30, check=True).stdout.strip()
    return {"profile_path": str(profile_path), "profile_sha256": sha256(profile_path), "image": image,
            "image_id": image_id}


def freeze(*, base_ref: str, now: float | None = None, runtime_images=None, profile=None) -> dict:
    now = time.time() if now is None else now
    pinned = {name: {"path": path, "old_sha256": git_sha256(base_ref, path), "new_sha256": sha256(ROOT / path)}
              for name, path in APPARATUS.items()}
    for name in UNCHANGED:
        if pinned[name]["old_sha256"] != pinned[name]["new_sha256"]:
            raise ValueError(f"{name} changed since the base ref: A3 never edits it")
    addendum = json.loads((ROOT / APPARATUS["g4_addendum"]).read_text())
    baseline_dir = Path(addendum["baseline"]["campaign_dir"])
    segments, segment_sources = baseline_segment_limits(baseline_dir)
    budget = json.loads((ROOT / APPARATUS["budget"]).read_text())
    table = budget["g4_delivery_visibility"]["table"]
    images = gc.a3_runtime_images() if runtime_images is None else runtime_images
    if any(value is None for value in images.values()):
        raise ValueError(f"runtime role without a running container: {[k for k, v in images.items() if v is None]}")
    return {
        "schema": gc.A3_SCHEMA, "amendment_id": gc.A3_ID, "version": 1,
        "frozen_before_candidate": True, "frozen_at_unix": now,
        "frozen_at": dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **DECISION,
        "scope": {
            "gates": ["G2", "G4", "G9"],
            "G2": "latency tails: in scope, but NO G2 unit is attributable in this version: the native writer's slices "
                  "carry no per-sample receive/canonical/dispatch clock and the Python MARK records have "
                  "envelope_received_at NOT_MEASURED (Query reference basis); every G2 tail is UNKNOWN = FAIL as "
                  "before (reported with the lane episodes)",
            "G4": "a failing row-window (frozen-table p95 rule or GATED addendum max rule) is re-scored when every "
                  "ADMITTED event above the row limit is retained (observer v7, no overflow, the window maximum "
                  "among them) and EXTERNAL; amendment 1's row keeps its own rule; REPORT_ONLY rows unchanged",
            "G9": "data-path eligibility losses only; demanded == session-ready, v1 fallback and internal-caused "
                  "losses stay strict",
            "strict_unchanged": ["G1", "G3", "G5", "G6", "G7", "G8", "G10", "G11", "G9 demanded==session-ready",
                                 "G9 v1 fallback", "G9 internal-caused losses", "every threshold"],
            "inherited_not_rerun": {"G12": "PASS on the final image set (inherited)",
                                    "G13": "76/76 on writer 2deb90b0 (DL e80e575 / TS 8d481760, inherited)"},
            "old_receipts": "immutable: a campaign that did not record this file's sha256 at run time scores exactly "
                            "as before (053803Z / 135719Z / 074624Z / 092255Z); the invalid 092255Z receipt is kept"},
        "classes": {
            "EXTERNAL_SESSION_UPSTREAM": "the SAME venue event (channel, symbol, venue id) on a fresh connection was "
                                         "on time (<= probe_healthy_ms) while the production lane received it >= "
                                         "lane_late_margin_ms later",
            "EXTERNAL_VENUE_WIDE": "NOT assigned in v1: a late fresh probe can be the probe's own scheduling on this "
                                   "host and no probe-side scheduler evidence is recorded (review finding 1)",
            "EXTERNAL_VENUE_RESET": "a G4 event in flight across a complete VENUE-originated session-fault episode of "
                                    "its own lane (OKX cause EOF/CLOSE_FRAME/READ_ERROR/HEARTBEAT_TIMEOUT/NOTICE*, "
                                    "Binance peer reset/close or heartbeat failure; no DL-initiated end such as "
                                    "STALENESS_HANDOVER/BOOK_RESYNC/WRITE_ERROR or connect failure in the episode)",
            "EXTERNAL_SESSION_INTERRUPTION": "a session-type G9 loss (TRADE_SESSION_UNVERIFIED / "
                                             "EXECUTION_CONTEXT_NOT_EXECUTION_ELIGIBLE) whose onset lies between the "
                                             "lane's last good frame and its first venue fault, writer workers LIVE and "
                                             "local/DL-path checks clean from the onset to restoration, restored within "
                                             "recovery_budget_s counted from the outage start",
            "UNKNOWN": "anything else, including a different probe event in the same second (supporting only), "
                       "missing coverage, retention overflow, an ambiguous lane: counts as FAIL"},
        "conditions": {
            "same_event_or_same_seconds": "probe evidence joins on the venue id DL keeps as source_sequence; lane "
                                          "faults on the event's/slice's own lane in its seconds (+- reset_pad_s)",
            "internal_segments": "G4: Kafka CreateTime - DL receipt <= row limit receive_to_canonical_ms, observer "
                                 "receipt - CreateTime <= canonical_to_consumer_ms, receipt->observer total <= the "
                                 "row limit; G9: core raw_age and gateway dispatch lag within limits over the "
                                 "recovery seconds",
            "local_checks": "the ingestor of the unit: cgroup throttled periods 0, worst thread run-queue wait <= "
                            "limit, no socket sample with unread bytes, continuous coverage (gap <= 2 s) and no "
                            "sampler error; samples ~100 ms apart cannot exclude a shorter stall nor frames read into "
                            "user space but processed late (stated, not over-claimed); another lane of the same "
                            "ingestor reading (progress interval after each fault)",
            "clock": "host clock bound sampled every 30 s by the A3 sampler; samples within clock_bracket_s on both "
                     "sides of the unit, each <= host_clock_bound_ms",
            "lane_timeline": "last good frame (progress last_frame_age), fault event (heartbeat_failed / disconnected "
                             "/ connect_failed), first frame of a newer generation (progress), eligibility restoration "
                             "(writer heartbeat, ~30 s resolution); canonical coordinate and consumer ACK per event "
                             "are not recorded and replaced by the DL-path checks and the restoration sample",
            "caps": "per G4 row AND per window: EXTERNAL units <= max_external_fraction x the admitted samples (so a "
                    "p95 failure, >= 5 % of a window over the limit, is never rescued); "
                    "G9: EXTERNAL losses <= max_external_fraction x demanded x readable samples and <= "
                    "max_external_episodes_per_venue episodes per venue; any recovery > recovery_budget_s -> FAIL",
            "retention": "EXTERNAL units stay in every distribution and in the receipt (count, durations, recovery); "
                         "verdicts are recomputed on the remaining units with the unchanged thresholds"},
        "limits": {**FIXED_LIMITS, "g4_internal_segment_ms_by_row": segments},
        "limits_sources": {"baseline_campaign": str(baseline_dir), "factor": FACTOR,
                           "baseline_observations_sha256": segment_sources,
                           "note": "per-row internal limits from the addendum's frozen baseline run only (never a "
                                   "candidate); OKX inverse rows use their pooled OKX row"},
        "attribution_module": {"schema": a3.A3_SCHEMA, "path": APPARATUS["a3_attribution"],
                               "sha256": pinned["a3_attribution"]["new_sha256"]},
        "workload": {"sessions": 100, "load_seconds": gc.LOAD_SECONDS, "budget_sha256": pinned["budget"]["new_sha256"],
                     "g4_schedule_version": gc.G4_SCHEDULE_VERSION, "g4_rows_frozen_table": len(table),
                     "client_smoke": "target-matrix (exact client image, mounts, user, bootstrap) before any load"},
        "stop_rules": [
            "one declared run (a NEW run after the invalid 092255Z, not its infrastructure rerun)",
            "exact-client smoke failure, unresolved A1 allowance or a preflight problem: refused, no load",
            "an apparatus fault in the client (ImportError/NameError/SyntaxError) aborts the driver at once",
            "a foreign job before or early in the load window (guard TAINTED): one rerun allowed (infra fault)",
            "any other failure is a result: no rerun until green",
            "runtime images must equal runtime_images below (else A3 is not applied and scoring is strict)"],
        "runtime_images": {"images": images, "basis": "docker inspect .Image at freeze time (read-only)"},
        "profile": profile,
        "base_budget_sha256": pinned["budget"]["new_sha256"],
        "base_addendum_sha256": pinned["g4_addendum"]["new_sha256"],
        "inherits": {"g4_amendment_1_sha256": pinned["g4_amendment_1"]["new_sha256"],
                     "g4_amendment_2_sha256": pinned["g4_amendment_2"]["new_sha256"],
                     "note": "amendments 1 and 2 (files unchanged) are re-pinned to gate_campaign_tool.new_sha256"},
        "pinned": {**pinned, "base_ref": base_ref,
                   "note": "old = at the pre-amendment base ref, new = frozen with this amendment; the scorer applies "
                           "A3 (and amendments 1/2 through it) only when the campaign tool sha256 equals "
                           "gate_campaign_tool.new_sha256"},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=gc.A3_PATH)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--base-profile", type=Path, default=gc.TOOLS / "profile-stage100-rc4.json")
    args = parser.parse_args(argv)
    if args.output.exists():
        raise SystemExit(f"{args.output} exists; the amendment is frozen once")
    amendment = freeze(base_ref=args.base_ref, profile=profile_image(args.base_profile))
    problems = gc.validate_a3(amendment)
    if problems:
        raise SystemExit("; ".join(problems))
    with args.output.open("x") as handle:
        json.dump(amendment, handle, indent=1, sort_keys=True)
        handle.write("\n")
    print(json.dumps({"output": str(args.output), "sha256": sha256(args.output),
                      "gate_campaign_tool": amendment["pinned"]["gate_campaign_tool"],
                      "rows_with_segment_limits": len(amendment["limits"]["g4_internal_segment_ms_by_row"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

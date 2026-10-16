#!/usr/bin/env python3
"""P18.4 gates G9 (readiness) and G10 (TRADE integrity): writer heartbeat sampler plus writer log counters (read-only).

G9 source: the writer heartbeat `service:heartbeat:market_data:*` on TRADING_REDIS_URL (redis_service DB 0), sampled
every --sample-seconds. Each readable sample records demanded / session-ready / execution-ready / quiet / unhealthy
counts, v1_fallback_count, v2_blocked/v2_error, the reported unhealthy rows (state, error_code, and for a slot without
a valid worker report its worker / worker_reason), the per-slot worker_liveness (LIVE, REPORT_EXPIRED,
REPORT_INVALID_OR_MISSING, PROCESS_NOT_ALIVE, ...; diagnostic, so a slot-wide WORKER_UNAVAILABLE is attributable) and, when the writer
image carries TS fb09a1b (purpose readiness), pricing/continuity/signal counts and the bounded `not_ready` rows with
their reason_code. An unreadable or stale heartbeat (last_seen older than 45 s;
the writer publishes about every 30 s, stage100m max age 30.0 s) is counted UNKNOWN, never READY.
A heartbeat key that changes (writer restart: new instance id) is re-scanned. The sampler reads faster than the
writer publishes, so G9 scores distinct heartbeat snapshots (key_tail + publication time, equal content): one snapshot
read three times counts once; every snapshot is still scored and an unreadable/stale read is always counted.

Eligibility losses (G9): a slice that is in `not_ready` in a sample but was not in the previous readable sample is
one loss event, classified by reason_code. TRADE_EVENT_STALE (a quiet TRADE whose provider session is live) is the
only legitimate quiet refusal; it is counted and reported, not gated. Every other reason is a data-path loss. Without
purpose fields (writer older than fb09a1b) the loss count is NOT_MEASURED; execution_ready drops are reported only as
a diagnostic.

G10 source: there is no dedicated TRADE integrity counter in the writer or SDK (checked: TS
adapters/market_data/data_layer_v2.py, services/market_data/*, qdl_sdk). The typed signals that do exist are its log
lines, read incrementally from the host-mounted writer log files (market_data_worker-*.log, market_data_service.log):
* `Data Layer V2 slice disconnected ... feed=TRADE ... reason=<Error> code=<CODE>`:
  CursorExpiredError / CURSOR_EXPIRED / CURSOR_INVALID -> replay_expired; SlowConsumerError (gRPC RESOURCE_EXHAUSTED,
  qdl_sdk/transport.py) -> overflow; ContinuityError or a GAP / OPEN_SEQUENCE code -> gap; anything else -> other;
* `data layer slice control code=<CODE> ... feed=TRADE`: SNAPSHOT_REPLACED -> gap (state rebuilt, continuity broken);
  RECONNECTED -> resumed (committed-cursor resume, not a gap; counted).
Duplicates applied come from the delivery trace (scripts/p184_trace_feed_transit.py duplicate_applied_event_id), not
from here. A silent offset gap that the Data Layer does not signal is not observable from these sources; the proposed
minimal additive counter is in the campaign report.

G10 native typed counters (Codex review SR2): the TS native (Rust) writer logs neither line above. Its typed TRADE
integrity counters (TS rust/crates/market-data-writer/src/telemetry.rs StreamIntegrity -> health.rs INTEGRITY_NAMES:
reconnects, snapshot_replaced, sequence_resets, gaps, continuity_errors, cursor_expired_errors, slow_consumer_errors,
offset_regressions, replayed_events, applied, refreshed, duplicates_rejected, duplicates_applied; the Python bridge's
StreamIntegrityCounters use the same names) reach the service heartbeat as details.trade_integrity, POOLED over the
live worker epochs (TS services/market_data/fixed_workers.py health_snapshot, trade_integrity_scope
{counter_scope: current_live_worker_epochs, workers_reporting, workers_expected}). The heartbeat carries no per-worker
epoch, so a worker replacement is only visible as a slot that is not LIVE, an incomplete worker roster or a pooled
counter that goes down. The sampler keeps the pooled counters per sample; summarize_g10_native() takes deltas between
consecutive samples of ONE unbroken activation stretch only (never subtracts pooled totals across a replacement) and
requires the load window to be bracketed by such samples (full-window coverage). Any break, gap, missing name or
invalid value is NOT_MEASURED for a PASS; a failure delta seen before a break still FAILs.

Boundary: read-only GET/SCAN on the heartbeat Redis; reads log files the host already owns read access to; bounded by
--seconds and --max-bytes. usage: p184_g9_g10_writer_counters.py --seconds N --log-dir DIR --out DIR
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_resp_readonly import (  # noqa: E402
    install_stop_signals, pause, stopped,
    BoundedJsonl, ReadOnlyRedis, container_ip, read_jsonl, write_json_exclusive,
)
from qdl.certification.phase3_consumer_load import distinct_heartbeat_reads, heartbeat_identity  # noqa: E402

LEGIT_QUIET = frozenset({"TRADE_EVENT_STALE"})
HEARTBEAT_PATTERN = "service:heartbeat:market_data:*"
COUNT_FIELDS = ("demanded_v2_slices", "session_ready_v2_slices", "execution_ready_v2_slices",
                "quiet_session_v2_slices", "unhealthy_v2_slices", "v1_fallback_count", "v2_blocked_count",
                "v2_error_count", "continuity_valid", "continuity_required", "pricing_eligible", "pricing_required",
                "signal_data_usable", "signal_required")
UNHEALTHY_FIELDS = ("provider", "symbol", "feed", "interval", "state", "error_code")
WORKER_FIELDS = ("worker", "worker_reason")  # only on rows of a slot without a valid worker report
# Native/Python typed TRADE counters (TS health.rs INTEGRITY_NAMES; adapters/market_data/data_layer_v2.py
# StreamIntegrityCounters). G10 classes; derived names (gap_events, replay_expired, overflow_upper_bound) are not used.
NATIVE_FAIL_CLASSES = {
    "gaps": ("gaps", "continuity_errors", "snapshot_replaced", "sequence_resets"),
    "replay_expired": ("cursor_expired_errors",),
    "overflow": ("slow_consumer_errors",),
    "duplicates_applied": ("duplicates_applied",),
}
# Delivered offset at/below the previous one with no reset in between: an unexplained ordering anomaly, never a PASS.
NATIVE_BLOCKING = ("offset_regressions",)
NATIVE_REPORT_ONLY = ("reconnects", "replayed_events", "applied", "refreshed", "duplicates_rejected")
NATIVE_REQUIRED = tuple(n for names in NATIVE_FAIL_CLASSES.values() for n in names) + NATIVE_BLOCKING + NATIVE_REPORT_ONLY
NATIVE_SCOPE = "current_live_worker_epochs"
LOG_STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3}) ")
DISCONNECT = re.compile(r"slice disconnected venue=(\S+) symbol=(\S+) feed=(\S+) interval=(\S+) reason=(\w+)"
                        r"(?: code=([A-Z_]+))?")
CONTROL = re.compile(r"slice control code=([A-Z_]+) venue=(\S+) symbol=(\S+) feed=(\S+)")


def heartbeat_sample(payload: dict | None, now_s: float) -> dict:
    if not isinstance(payload, dict) or not isinstance(payload.get("details"), dict):
        return {"readable": False}
    details = payload["details"]
    sample = {"readable": True, "status": payload.get("status"),
              "age_s": round(now_s - float(payload.get("last_seen_unix") or 0), 1),
              # Publication time: with key_tail it identifies the heartbeat snapshot (one snapshot counts once).
              **({"last_seen_unix": payload["last_seen_unix"]}
                 if type(payload.get("last_seen_unix")) in (int, float) else {}),
              **{key: details.get(key) for key in COUNT_FIELDS if key in details},
              "unhealthy": [{**{k: row.get(k) for k in UNHEALTHY_FIELDS}, **{k: row[k] for k in WORKER_FIELDS if k in row}}
                            for row in details.get("reported_unhealthy_slices", [])[:64] if isinstance(row, dict)],
              "purpose": "purpose_readiness_schema" in details and "not_ready" in details}
    liveness = details.get("worker_liveness")
    if isinstance(liveness, dict):
        sample["worker_liveness"] = {str(slot): str(state) for slot, state in list(liveness.items())[:64]}
    if "trade_integrity" in details:
        counters = bounded_counters(details.get("trade_integrity"))
        sample["trade_integrity"] = counters
        if counters is None:
            sample["trade_integrity_invalid"] = True
    scope = details.get("trade_integrity_scope")
    if isinstance(scope, dict):
        sample["trade_integrity_scope"] = {k: scope.get(k) for k in ("counter_scope", "workers_reporting",
                                                                      "workers_expected")}
    if sample["purpose"]:
        sample["not_ready"] = [{k: row.get(k) for k in ("provider", "symbol", "feed", "interval", "reason_code",
                                                         "component_age_ms")}
                               for row in details.get("not_ready", [])[:64] if isinstance(row, dict)]
        sample["not_ready_unreported"] = details.get("not_ready_unreported")
    if "purpose_readiness_error" in details:
        sample["purpose_error"] = details["purpose_readiness_error"]
    return sample


def bounded_counters(value) -> dict | None:
    """A typed counter map (str -> int >= 0, at most 64 names) or None: no default, no coercion (bool is not int)."""
    if not isinstance(value, dict) or len(value) > 64:
        return None
    if any(not isinstance(k, str) or not 1 <= len(k) <= 96 or type(v) is not int or not 0 <= v <= 2**63 - 1
           for k, v in value.items()):
        return None
    return dict(value)


def slice_key(row: dict) -> str:
    return "|".join(str(row.get(k)) for k in ("provider", "symbol", "feed", "interval"))


def _read_identity(row: dict, stale_after_s: float):
    """Heartbeat snapshot identity of one read; None (always counted) for an unreadable or stale read, so a writer
    that stopped publishing is never hidden behind its last snapshot."""
    sample = row.get("sample") or {}
    if not sample.get("readable") or type(sample.get("age_s")) not in (int, float) or sample["age_s"] > stale_after_s:
        return None
    return heartbeat_identity(at_ns=row.get("at_ns"), age_s=sample.get("age_s"),
                              last_seen_unix=sample.get("last_seen_unix"), key=row.get("key_tail"))


def _read_content(row: dict):
    return {k: v for k, v in (row.get("sample") or {}).items() if k != "age_s"}


def summarize_g9(samples, *, from_ns=None, to_ns=None, stale_after_s=45.0) -> dict:
    """G9 over distinct heartbeat snapshots: the sampler reads faster than the writer publishes, so repeated reads
    of one snapshot (same key_tail and publication time, equal content) count once. Every snapshot is still scored."""
    reads = [s for s in samples if s.get("kind") == "heartbeat"
             and (from_ns is None or s["at_ns"] >= from_ns) and (to_ns is None or s["at_ns"] <= to_ns)]
    rows = distinct_heartbeat_reads(reads, lambda row: _read_identity(row, stale_after_s), _read_content)
    unknown = [s for s in rows if not s["sample"]["readable"] or s["sample"]["age_s"] > stale_after_s]
    good = [s["sample"] for s in rows if s not in unknown]
    mismatch = [s for s in good if s.get("demanded_v2_slices") != s.get("session_ready_v2_slices")]
    session_causes = Counter(":".join(str(row[k]) for k in ("feed", "state", "error_code", "worker_reason") if k in row)
                             for s in mismatch for row in s["unhealthy"])
    # Per-slot worker report liveness (diagnostic): proves e.g. REPORT_EXPIRED behind WORKER_UNAVAILABLE rows.
    not_live = Counter(state for s in good for state in (s.get("worker_liveness") or {}).values() if state != "LIVE")
    not_live_samples = sum(1 for s in good if any(v != "LIVE" for v in (s.get("worker_liveness") or {}).values()))
    purpose = bool(good) and all(s["purpose"] for s in good)
    losses, legit, previous = Counter(), Counter(), None
    loss_rows = []
    for s in good:
        if not s["purpose"]:
            previous = None
            continue
        current = {slice_key(row): row for row in s.get("not_ready", [])}
        if previous is not None:
            for key, row in current.items():
                if key not in previous:
                    cause = f"{row.get('feed')}:{row.get('reason_code')}"
                    if row.get("reason_code") in LEGIT_QUIET:
                        legit[cause] += 1
                    else:
                        losses[cause] += 1
                        if len(loss_rows) < 100:
                            loss_rows.append(row)
        previous = current
    fallback = [s.get("v1_fallback_count") for s in good if isinstance(s.get("v1_fallback_count"), int)]
    demanded = sorted({s.get("demanded_v2_slices") for s in good})
    exec_ready = [s.get("execution_ready_v2_slices") for s in good if isinstance(s.get("execution_ready_v2_slices"), int)]
    return {
        "samples": len(rows), "sampler_reads": len(reads), "duplicate_heartbeat_reads": len(reads) - len(rows),
        "unknown_samples": len(unknown), "readable_samples": len(good),
        "demanded_values": demanded,
        "demanded_ne_session_ready_samples": len(mismatch),
        "session_loss_causes": dict(session_causes),
        "worker_liveness_present": bool(good) and all("worker_liveness" in s for s in good),
        "worker_not_live_samples": not_live_samples,
        "worker_not_live_causes": dict(not_live),
        "purpose_fields_present": purpose,
        "eligibility_losses_data_path": sum(losses.values()) if purpose else None,
        "eligibility_loss_causes": dict(losses) if purpose else None,
        "eligibility_loss_rows": loss_rows,
        "legit_quiet_refusals": dict(legit) if purpose else None,
        "v1_fallback_delta": (max(fallback) - min(fallback)) if fallback else None,
        "v1_fallback_max": max(fallback) if fallback else None,
        "execution_ready_min": min(exec_ready) if exec_ready else None,
        "execution_ready_max": max(exec_ready) if exec_ready else None,
        "purpose_errors": sum(1 for s in good if "purpose_error" in s),
    }


def verdict_g9(summary: dict, *, expected_demanded: int | None) -> tuple[str, list[str]]:
    reasons = []
    if summary["demanded_ne_session_ready_samples"]:
        reasons.append(f"demanded != session-ready in {summary['demanded_ne_session_ready_samples']} samples")
    if summary["eligibility_losses_data_path"]:
        reasons.append(f"{summary['eligibility_losses_data_path']} data-path eligibility losses")
    if summary["v1_fallback_max"]:
        reasons.append(f"v1_fallback_count {summary['v1_fallback_max']}")
    if reasons:
        return "FAIL", reasons
    gaps = []
    if not summary["readable_samples"]:
        gaps.append("no readable heartbeat sample")
    if summary["unknown_samples"]:
        gaps.append(f"{summary['unknown_samples']} unreadable/stale heartbeat samples")
    if not summary["purpose_fields_present"]:
        gaps.append("heartbeat has no purpose readiness (writer older than TS fb09a1b): losses not countable")
    if expected_demanded is not None and summary["demanded_values"] != [expected_demanded]:
        gaps.append(f"demanded {summary['demanded_values']} != final manifest {expected_demanded}")
    if summary["purpose_errors"]:
        gaps.append(f"{summary['purpose_errors']} purpose readiness evaluation errors")
    return ("NOT_MEASURED", gaps) if gaps else ("PASS", [])


def classify_line(line: str):
    """Return (stamp 'YYYY-MM-DD HH:MM:SS', category, detail) for a TRADE integrity line, else None."""
    stamp = LOG_STAMP.match(line)
    if not stamp:
        return None
    match = DISCONNECT.search(line)
    if match:
        feed, reason, code = match.group(3), match.group(5), match.group(6) or ""
        if feed != "TRADE":
            return stamp.group(1), "other_feed_disconnect", f"{feed}:{reason}:{code}"
        if reason == "CursorExpiredError" or code in {"CURSOR_EXPIRED", "CURSOR_INVALID"}:
            category = "replay_expired"
        elif reason == "SlowConsumerError":
            category = "overflow"
        elif reason == "ContinuityError" or "GAP" in code or "OPEN_SEQUENCE" in code:
            category = "gap"
        else:
            category = "trade_disconnect_other"
        return stamp.group(1), category, f"{match.group(1)}:{match.group(2)}:{reason}:{code}"
    match = CONTROL.search(line)
    if match and match.group(4) == "TRADE":
        code = match.group(1)
        category = "gap" if code == "SNAPSHOT_REPLACED" else "resumed" if code == "RECONNECTED" else "control_other"
        return stamp.group(1), category, f"{match.group(2)}:{match.group(3)}:{code}"
    return None


class LogTail:
    """Incremental reader over a fixed set of files, starting at their size at construction."""

    def __init__(self, paths):
        self.offsets = {}
        for path in paths:
            try:
                self.offsets[Path(path)] = Path(path).stat().st_size
            except OSError:
                self.offsets[Path(path)] = None
        self.unreadable = 0

    def read_new(self):
        lines = []
        for path, offset in list(self.offsets.items()):
            try:
                size = path.stat().st_size
                if offset is None or size < offset:
                    offset = 0  # created or truncated after start
                with path.open("rb") as handle:
                    handle.seek(offset)
                    data = handle.read(min(size - offset, 64 * 1024 * 1024))
                # Only complete lines; a partial last line is read next time.
                cut = data.rfind(b"\n") + 1
                self.offsets[path] = offset + cut
                lines.extend(data[:cut].decode(errors="replace").splitlines())
            except OSError:
                self.unreadable += 1
        return lines


def summarize_g10(samples, *, from_s=None, to_s=None) -> dict:
    counts, codes, unreadable, polls = Counter(), Counter(), 0, 0
    examples = []
    for s in samples:
        if s.get("kind") == "log_poll":
            polls += 1
            unreadable += s.get("unreadable", 0)
        if s.get("kind") != "log_event":
            continue
        if from_s is not None and s["stamp"] < from_s or to_s is not None and s["stamp"] > to_s:
            continue
        counts[s["category"]] += 1
        codes[f"{s['category']}|{s['detail'].split(':', 2)[-1]}"] += 1
        if s["category"] in {"gap", "replay_expired", "overflow"} and len(examples) < 50:
            examples.append(s)
    return {"log_polls": polls, "log_unreadable": unreadable,
            "trade_gap": counts["gap"], "trade_replay_expired": counts["replay_expired"],
            "trade_overflow": counts["overflow"], "trade_resumed": counts["resumed"],
            "trade_disconnect_other": counts["trade_disconnect_other"], "other_feed_disconnects": counts[
                "other_feed_disconnect"], "codes": dict(codes), "examples": examples}


def _native_sample_problem(row: dict, stale_after_s: float) -> str | None:
    """Why one heartbeat sample cannot carry the pooled typed counters of an unbroken activation, else None."""
    sample = row.get("sample") or {}
    if not sample.get("readable"):
        return "heartbeat unreadable"
    if type(sample.get("age_s")) not in (int, float) or sample["age_s"] > stale_after_s:
        return f"heartbeat stale (age {sample.get('age_s')} s)"
    counters = sample.get("trade_integrity")
    if counters is None:
        return "trade_integrity invalid" if sample.get("trade_integrity_invalid") else "no trade_integrity"
    missing = [name for name in NATIVE_REQUIRED if name not in counters]
    if missing:
        return f"trade_integrity without {missing}"
    scope = sample.get("trade_integrity_scope") or {}
    reporting, expected = scope.get("workers_reporting"), scope.get("workers_expected")
    if scope.get("counter_scope") != NATIVE_SCOPE:
        return f"counter scope {scope.get('counter_scope')!r} (expected {NATIVE_SCOPE})"
    if type(reporting) is not int or type(expected) is not int or expected < 1 or reporting != expected:
        return f"workers reporting {reporting} of {expected}"
    liveness = sample.get("worker_liveness")
    if not isinstance(liveness, dict) or not liveness:
        return "no worker_liveness"
    if any(state != "LIVE" for state in liveness.values()):
        return "worker not LIVE: " + ",".join(sorted(f"{k}={v}" for k, v in liveness.items() if v != "LIVE"))
    return None


def summarize_g10_native(samples, *, from_ns=None, to_ns=None, stale_after_s=45.0, max_gap_s=45.0) -> dict:
    """G10 native typed counters from the pooled heartbeat trade_integrity (see module doc, SR2).

    The window [from_ns, to_ns] must be bracketed by usable samples (the last one at/before from_ns and the first one
    at/after to_ns) with every sample in between usable, consecutive samples <= max_gap_s apart and the heartbeat key
    unchanged. Deltas are summed only between consecutive usable samples whose every counter is non-decreasing (one
    activation stretch); a decrease is a replacement (counters restarted) and breaks coverage instead of being
    subtracted. complete=False means the deltas are a lower bound: they can FAIL G10, never PASS it."""
    rows = sorted((s for s in samples if s.get("kind") == "heartbeat" and type(s.get("at_ns")) is int),
                  key=lambda s: s["at_ns"])
    out = {"basis": "service heartbeat details.trade_integrity pooled over the live worker epochs (TS fixed_workers "
                    "health_snapshot); deltas between consecutive samples of one unbroken activation stretch",
           "present": any("trade_integrity" in (s.get("sample") or {}) for s in rows),
           "limits": "no per-worker epoch in the heartbeat: a replacement is detected only by a non-LIVE slot, an "
                     "incomplete worker roster or a decreasing pooled counter; counts between the last sample and a "
                     "replacement are lost (that is a coverage break, never zero)",
           "breaks": [], "deltas": {}, "complete": False}
    if from_ns is None or to_ns is None or to_ns <= from_ns:
        out["breaks"].append("no load window")
        return out
    before = [i for i, s in enumerate(rows) if s["at_ns"] <= from_ns]
    after = [i for i, s in enumerate(rows) if s["at_ns"] >= to_ns]
    first = before[-1] if before else 0
    last = after[0] if after else len(rows) - 1
    if not before:
        out["breaks"].append("no heartbeat sample at or before the window start")
    if not after:
        out["breaks"].append("no heartbeat sample at or after the window end")
    bracket = rows[first:last + 1] if rows else []
    out.update(samples_in_bracket=len(bracket), bracket_from_ns=bracket[0]["at_ns"] if bracket else None,
               bracket_to_ns=bracket[-1]["at_ns"] if bracket else None)
    deltas = {name: 0 for name in NATIVE_REQUIRED}
    previous = None
    for row in bracket:
        problem = _native_sample_problem(row, stale_after_s)
        if problem is not None:
            if len(out["breaks"]) < 50:
                out["breaks"].append(f"{row['at_ns']}: {problem}")
            else:
                out["breaks_truncated"] = True
            previous = None
            continue
        if previous is not None:
            gap_s = (row["at_ns"] - previous["at_ns"]) / 1e9
            counters, base = row["sample"]["trade_integrity"], previous["sample"]["trade_integrity"]
            if gap_s > max_gap_s:
                out["breaks"].append(f"{row['at_ns']}: {gap_s:.1f} s since the previous usable sample")
            elif row.get("key_tail") != previous.get("key_tail"):
                out["breaks"].append(f"{row['at_ns']}: heartbeat key changed (writer instance replaced)")
            elif any(counters[name] < base[name] for name in NATIVE_REQUIRED):
                down = sorted(n for n in NATIVE_REQUIRED if counters[n] < base[n])
                out["breaks"].append(f"{row['at_ns']}: pooled counters decreased {down} (worker replaced)")
            else:
                for name in NATIVE_REQUIRED:
                    deltas[name] += counters[name] - base[name]
        previous = row
    out["deltas"] = deltas
    out["complete"] = bool(bracket) and not out["breaks"]
    out["failures"] = {cls: sum(deltas[n] for n in names) for cls, names in NATIVE_FAIL_CLASSES.items()}
    out["blocking"] = {name: deltas[name] for name in NATIVE_BLOCKING}
    out["report_only"] = {name: deltas[name] for name in NATIVE_REPORT_ONLY}
    return out


def run(args) -> Path:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    evidence = BoundedJsonl(out / "g9-g10-samples.jsonl", args.max_bytes)
    host = args.host or container_ip(args.container, args.network)
    redis = ReadOnlyRedis(host, args.port, args.db)
    log_dir = Path(args.log_dir)
    tail = LogTail(sorted(log_dir.glob("market_data_worker-*.log")) + [log_dir / "market_data_service.log"])
    evidence.write({"kind": "scope", "heartbeat_redis": {"container": args.container, "host": host, "db": args.db},
                    "log_dir": str(log_dir), "files": [str(p) for p in tail.offsets]})
    key = None
    deadline = time.monotonic() + args.seconds
    next_log = time.monotonic()
    try:
        while time.monotonic() < deadline and not stopped():
            tick = time.monotonic()
            payload = None
            try:
                if key is None:
                    keys = redis.scan(HEARTBEAT_PATTERN, limit=16)
                    key = keys[0] if len(keys) == 1 else None
                    if len(keys) > 1:
                        evidence.write({"kind": "heartbeat_keys_ambiguous", "count": len(keys)})
                raw = redis.call("GET", key) if key else None
                payload = json.loads(raw) if raw else None
                if raw is None:
                    key = None  # instance changed (writer restart): re-scan next tick
            except Exception as error:
                key = None
                evidence.write({"kind": "heartbeat_error", "at_ns": time.time_ns(), "error": type(error).__name__})
            evidence.write({"kind": "heartbeat", "at_ns": time.time_ns(), "key_tail": (key or "")[-24:],
                            "sample": heartbeat_sample(payload, time.time())})
            if tick >= next_log:
                next_log += args.log_seconds
                before = tail.unreadable
                for line in tail.read_new():
                    item = classify_line(line)
                    if item:
                        stamp, category, detail = item
                        evidence.write({"kind": "log_event", "stamp": stamp, "category": category, "detail": detail})
                evidence.write({"kind": "log_poll", "at_ns": time.time_ns(), "unreadable": tail.unreadable - before})
            pause(max(0.0, args.sample_seconds - (time.monotonic() - tick)))
    finally:
        redis.close()
        evidence.close()
    samples = list(read_jsonl(out / "g9-g10-samples.jsonl"))
    g9 = summarize_g9(samples)
    beats = [s["at_ns"] for s in samples if s.get("kind") == "heartbeat"]
    native = summarize_g10_native(samples, from_ns=beats[0], to_ns=beats[-1]) if len(beats) > 1 else None
    write_json_exclusive(out / "g9-g10-summary.json", {"schema": "qdl.p184.g9-g10-writer-counters.v2",
                                                       "g9": g9, "g9_verdict_whole_run": verdict_g9(g9, expected_demanded=None),
                                                       "g10": summarize_g10(samples),
                                                       "g10_native_whole_run": native})
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--container", default="redis_service", help="TRADING_REDIS_URL host (heartbeats)")
    parser.add_argument("--network", default="executor_network")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=0)
    parser.add_argument("--log-dir", required=True, help="host path mounted at the writer's /app/logs")
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--sample-seconds", type=float, default=10.0)
    parser.add_argument("--log-seconds", type=float, default=15.0)
    parser.add_argument("--max-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    install_stop_signals()
    if not 1 <= args.sample_seconds <= 60 or not 0 < args.seconds <= 4 * 3600:
        parser.error("sample-seconds must be 1..60 and seconds 0..14400")
    print(json.dumps({"output": str(run(args))}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

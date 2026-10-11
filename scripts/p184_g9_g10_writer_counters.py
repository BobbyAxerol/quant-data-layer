#!/usr/bin/env python3
"""P18.4 gates G9 (readiness) and G10 (TRADE integrity): writer heartbeat sampler plus writer log counters (read-only).

G9 source: the writer heartbeat `service:heartbeat:market_data:*` on TRADING_REDIS_URL (redis_service DB 0), sampled
every --sample-seconds. Each readable sample records demanded / session-ready / execution-ready / quiet / unhealthy
counts, v1_fallback_count, v2_blocked/v2_error, the reported unhealthy rows (state, error_code) and, when the writer
image carries TS fb09a1b (purpose readiness), pricing/continuity/signal counts and the bounded `not_ready` rows with
their reason_code. An unreadable or stale heartbeat (last_seen older than 45 s;
the writer publishes about every 30 s, stage100m max age 30.0 s) is counted UNKNOWN, never READY.
A heartbeat key that changes (writer restart: new instance id) is re-scanned.

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

LEGIT_QUIET = frozenset({"TRADE_EVENT_STALE"})
HEARTBEAT_PATTERN = "service:heartbeat:market_data:*"
COUNT_FIELDS = ("demanded_v2_slices", "session_ready_v2_slices", "execution_ready_v2_slices",
                "quiet_session_v2_slices", "unhealthy_v2_slices", "v1_fallback_count", "v2_blocked_count",
                "v2_error_count", "continuity_valid", "continuity_required", "pricing_eligible", "pricing_required",
                "signal_data_usable", "signal_required")
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
              **{key: details.get(key) for key in COUNT_FIELDS if key in details},
              "unhealthy": [{k: row.get(k) for k in ("provider", "symbol", "feed", "interval", "state", "error_code")}
                            for row in details.get("reported_unhealthy_slices", [])[:64] if isinstance(row, dict)],
              "purpose": "purpose_readiness_schema" in details and "not_ready" in details}
    if sample["purpose"]:
        sample["not_ready"] = [{k: row.get(k) for k in ("provider", "symbol", "feed", "interval", "reason_code",
                                                         "component_age_ms")}
                               for row in details.get("not_ready", [])[:64] if isinstance(row, dict)]
        sample["not_ready_unreported"] = details.get("not_ready_unreported")
    if "purpose_readiness_error" in details:
        sample["purpose_error"] = details["purpose_readiness_error"]
    return sample


def slice_key(row: dict) -> str:
    return "|".join(str(row.get(k)) for k in ("provider", "symbol", "feed", "interval"))


def summarize_g9(samples, *, from_ns=None, to_ns=None, stale_after_s=45.0) -> dict:
    rows = [s for s in samples if s.get("kind") == "heartbeat"
            and (from_ns is None or s["at_ns"] >= from_ns) and (to_ns is None or s["at_ns"] <= to_ns)]
    unknown = [s for s in rows if not s["sample"]["readable"] or s["sample"]["age_s"] > stale_after_s]
    good = [s["sample"] for s in rows if s not in unknown]
    mismatch = [s for s in good if s.get("demanded_v2_slices") != s.get("session_ready_v2_slices")]
    session_causes = Counter(f"{row['feed']}:{row['state']}:{row['error_code']}"
                             for s in mismatch for row in s["unhealthy"])
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
        "samples": len(rows), "unknown_samples": len(unknown), "readable_samples": len(good),
        "demanded_values": demanded,
        "demanded_ne_session_ready_samples": len(mismatch),
        "session_loss_causes": dict(session_causes),
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
    write_json_exclusive(out / "g9-g10-summary.json", {"schema": "qdl.p184.g9-g10-writer-counters.v1",
                                                       "g9": g9, "g9_verdict_whole_run": verdict_g9(g9, expected_demanded=None),
                                                       "g10": summarize_g10(samples)})
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

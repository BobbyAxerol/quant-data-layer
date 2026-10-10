#!/usr/bin/env python3
"""P18.4 gate G11: TS writer restart recovery, measured around a restart the coordinator triggers (read-only).

The probe never restarts anything. It arms, then waits (<= --wait-seconds) for the writer container's
State.StartedAt to change (`docker restart`; a recreate with a new container id is also detected by name). It records
the four G11 timestamps separately (gate table rev 2, Codex review 3):
* outage_start = State.FinishedAt of the old process (Docker's exit time), cross-checked by the last poll that saw
  the old StartedAt;
* fault_cleared = the new State.StartedAt with State.Running true;
* first_valid_apply per slice = first poll after fault_cleared at which the slice's TS cache entry was rewritten
  (value digest changed, or the key TTL jumped back up) with a parsable event time not older than the last
  pre-outage event time;
* caught_up per slice = first poll after fault_cleared whose cached event age is inside the live bound (TRADE 3000 ms,
  QUOTE/MARK/BOOK 2000 ms, BAR: newest closed bar closed <= interval + 5 s ago).
and checks: no rewind (event time and, for TRADE/QUOTE, the applied offset of the same generation never go backwards
across the restart) and cursor never ahead (each committed SDK cursor offset of a TRADE/QUOTE slice <= the applied
offset in TS Redis read after it; the cursor files are read with a read-only `docker exec ... python -c` that prints
only key -> offset, never the resume tokens).

Keys per slice (TS services/market_data/cache_keys.py; the market Redis is TRADING_MARKET_REDIS_URL, default
redis_marketdata DB 1): TRADE/QUOTE `execution_trade|execution_quote:<id>` (+ `applied:trade|quote:<id>` offset and
generation); BOOK_SNAPSHOT/BOOK_DELTA/MARK `<kind>:<id>:ordering-v1` marker (observed_at_ms) plus PTTL of the
`<kind>:<id>` view (the ~40 KB book body is never read); BAR `last_bar:<id>:<interval>`.
Gate (NEW owner bound, frozen table): fault cleared -> first valid apply per slice <= 30 s; -> caught up <= 60 s;
no rewind; cursor never ahead.

Boundary: GET/MGET/PTTL/SCAN only; docker inspect (read-only) and one read-only docker exec per cursor sample.
usage: p184_g11_writer_restart_probe.py --binding FILE --out DIR [--wait-seconds 900] [--observe-seconds 180]
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_resp_readonly import (  # noqa: E402
    install_stop_signals, pause, stopped,
    BoundedJsonl, ReadOnlyRedis, container_ip, quantiles, write_json_exclusive,
)

GATE = {"first_valid_apply_le_s": 30.0, "caught_up_le_s": 60.0}
LIVE_BOUND_MS = {"TRADE": 3000, "QUOTE": 2000, "MARK_INDEX_PRICE": 2000, "BOOK_SNAPSHOT": 2000, "BOOK_DELTA": 2000}
KIND = {"TRADE": "execution_trade", "QUOTE": "execution_quote", "BOOK_SNAPSHOT": "execution_book_snapshot",
        "BOOK_DELTA": "execution_book_delta", "MARK_INDEX_PRICE": "execution_mark_index_price"}
UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
CURSOR_DUMP = (
    "import glob,json\n"
    "out={}\n"
    "for p in glob.glob('/app/state/data-layer-v2/cursors/fixed-workers-v1/count-*-worker-*/*.json'):\n"
    "    try:\n"
    "        for k,v in json.load(open(p)).get('items',{}).items(): out[k]=int(v['offset'])\n"
    "    except Exception: pass\n"
    "print(json.dumps(out))\n"
)


def parse_docker_time(value: str) -> int | None:
    """Docker RFC3339 with nanoseconds -> unix ms; the zero time means 'never'."""
    if not value or value.startswith("0001-"):
        return None
    head, _, frac = value.rstrip("Z").partition(".")
    base = dt.datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    return int(base.timestamp() * 1000) + int((frac + "000")[:3] or 0)


def demanded_slices(binding: dict) -> list[dict]:
    return sorted(({"venue": p["venue"], "native": p["native_symbol"], "feed": p["feed"],
                    "interval": p.get("interval"), "uid": p["instrument_uid"]}
                   for p in binding.get("products", [])), key=lambda r: (r["venue"], r["native"], r["feed"]))


def identities_from_keys(keys) -> dict[tuple[str, str], str]:
    """(venue, venue_symbol) -> canonical id 'VENUE:PRODUCT:venue_symbol', from execution/last_bar key names."""
    out = {}
    for key in keys:
        parts = key.split(":")
        if (len(parts) >= 7 and parts[:3] == ["cache", "market", "v2"]
                and (parts[3].startswith("execution_") or parts[3] == "last_bar")):
            venue, product, symbol = parts[4], parts[5], parts[6]
            out[(venue, symbol)] = f"{venue}:{product}:{symbol}"
    return out


def slice_keys(item: dict, identity: str) -> dict:
    name = "|".join(str(item[k]) for k in ("venue", "native", "feed", "interval"))
    if item["feed"] == "BAR":
        key = f"cache:market:v2:last_bar:{identity}:{item['interval']}"
        return {"name": name, "value": key, "ttl": key, "applied": None}
    kind = KIND[item["feed"]]
    if item["feed"] in ("TRADE", "QUOTE"):
        key = f"cache:market:v2:{kind}:{identity}"
        return {"name": name, "value": key, "ttl": key,
                "applied": f"cache:market:v2:applied:{item['feed'].lower()}:{identity}"}
    return {"name": name, "value": f"cache:market:v2:{kind}:{identity}:ordering-v1",
            "ttl": f"cache:market:v2:{kind}:{identity}", "applied": None}


def observe(feed: str, raw, applied_raw, pttl, interval: str | None) -> dict:
    """Reduce one slice read to the fields the analysis needs (no payload values kept)."""
    obs = {"digest": hashlib.sha256(raw).hexdigest()[:16] if raw else None, "pttl": pttl,
           "event_ms": None, "offset": None, "generation": None, "closed": None}
    if raw:
        try:
            value = json.loads(raw)
            if feed == "BAR":
                obs["event_ms"] = int(value["timestamp_ms"]) + int(interval[:-1]) * UNIT_MS[interval[-1]]
                obs["closed"] = value.get("is_closed") is True
            else:
                obs["event_ms"] = int(value["observed_at_ms"])
        except (ValueError, KeyError, TypeError):
            pass
    if applied_raw:
        try:
            applied = json.loads(applied_raw)
            obs["offset"], obs["generation"] = int(applied["offset"]), str(applied["generation"])[:16]
        except (ValueError, KeyError, TypeError):
            pass
    return obs


class SliceRecovery:
    def __init__(self, name: str, feed: str, interval: str | None, poll_ms: int):
        self.name, self.feed, self.interval, self.poll_ms = name, feed, interval, poll_ms
        self.pre = None           # last observation before the outage
        self.prev = None          # previous observation
        self.first_apply_ms = None
        self.caught_up_ms = None
        self.rewinds = []

    def _rewritten(self, obs) -> bool:
        if self.prev is None:
            return False
        if obs["digest"] != self.prev["digest"]:
            return True
        return (obs["pttl"] is not None and self.prev["pttl"] is not None and obs["pttl"] >= 0
                and obs["pttl"] > self.prev["pttl"] + self.poll_ms + 500)

    def _live(self, obs, now_ms) -> bool:
        if obs["event_ms"] is None:
            return False
        if self.feed == "BAR":
            step = int(self.interval[:-1]) * UNIT_MS[self.interval[-1]]
            return bool(obs["closed"]) and now_ms - obs["event_ms"] <= step + 5000
        return now_ms - obs["event_ms"] <= LIVE_BOUND_MS[self.feed]

    def update(self, now_ms: int, obs: dict, *, phase: str) -> None:
        """phase: PRE (before the outage), DOWN (outage, not cleared), POST (after fault cleared)."""
        if phase == "PRE":
            self.pre = obs
        elif phase == "POST":
            base = self.pre or {}
            if base.get("event_ms") is not None and obs["event_ms"] is not None and obs["event_ms"] < base["event_ms"]:
                self.rewinds.append({"at_ms": now_ms, "kind": "event_time", "before": base["event_ms"],
                                     "after": obs["event_ms"]})
            if (base.get("offset") is not None and obs["offset"] is not None
                    and base.get("generation") == obs["generation"] and obs["offset"] < base["offset"]):
                self.rewinds.append({"at_ms": now_ms, "kind": "applied_offset", "before": base["offset"],
                                     "after": obs["offset"]})
            if self.first_apply_ms is None and self._rewritten(obs) and obs["event_ms"] is not None and (
                    base.get("event_ms") is None or obs["event_ms"] >= base["event_ms"]):
                self.first_apply_ms = now_ms
            if self.caught_up_ms is None and self.first_apply_ms is not None and self._live(obs, now_ms):
                self.caught_up_ms = now_ms
        self.prev = obs


def cursor_check(cursors: dict[str, int], applied: dict[str, int], uid_slice: dict[str, str]) -> dict:
    """cursors: 'consumer|uid|FEED|interval|policy' -> offset; applied: slice name -> applied offset (read after)."""
    ahead, compared, unmatched = [], 0, 0
    for key, offset in cursors.items():
        parts = key.split("|")
        if len(parts) < 3 or parts[2] not in ("TRADE", "QUOTE"):
            continue
        name = uid_slice.get(f"{parts[1]}|{parts[2]}")
        if name is None or applied.get(name) is None:
            unmatched += 1
            continue
        compared += 1
        if offset > applied[name]:
            ahead.append({"slice": name, "cursor": offset, "applied": applied[name]})
    return {"compared": compared, "unmatched": unmatched, "ahead": ahead}


def recovery_summary(slices: dict, cleared_ms: int | None) -> dict:
    first = {n: (s.first_apply_ms - cleared_ms) / 1000 for n, s in slices.items()
             if cleared_ms and s.first_apply_ms is not None}
    caught = {n: (s.caught_up_ms - cleared_ms) / 1000 for n, s in slices.items()
              if cleared_ms and s.caught_up_ms is not None}
    return {"first_valid_apply_s": quantiles(list(first.values())),
            "caught_up_s": quantiles(list(caught.values())),
            "slices_without_first_apply": sorted(n for n in slices if n not in first),
            "slices_not_caught_up": sorted(n for n in slices if n not in caught),
            "per_slice": {n: {"first_valid_apply_s": first.get(n), "caught_up_s": caught.get(n),
                              "rewinds": s.rewinds[:5]} for n, s in sorted(slices.items())},
            "rewinds_total": sum(len(s.rewinds) for s in slices.values())}


def verdict(report: dict) -> tuple[str, list[str]]:
    if report.get("restart") is None:
        return "NOT_MEASURED", ["no writer restart observed inside the wait window"]
    summary = report["recovery"]
    reasons = []
    if summary["first_valid_apply_s"].get("n") and summary["first_valid_apply_s"]["max"] > GATE["first_valid_apply_le_s"]:
        reasons.append(f"first valid apply max {summary['first_valid_apply_s']['max']} s > 30 s")
    if summary["caught_up_s"].get("n") and summary["caught_up_s"]["max"] > GATE["caught_up_le_s"]:
        reasons.append(f"caught up max {summary['caught_up_s']['max']} s > 60 s")
    if summary["rewinds_total"]:
        reasons.append(f"{summary['rewinds_total']} rewinds")
    if any(sample["ahead"] for sample in report["cursor_samples"]):
        reasons.append("committed cursor ahead of applied state")
    observed = report["observe_seconds"]
    late = summary["slices_without_first_apply"] + summary["slices_not_caught_up"]
    if observed >= GATE["caught_up_le_s"] and summary["slices_not_caught_up"]:
        reasons.append(f"{len(summary['slices_not_caught_up'])} slices not caught up within {observed} s "
                       "(includes quiet TRADE slices; see per_slice)")
    if reasons:
        return "FAIL", reasons
    gaps = []
    if late:
        gaps.append("slices without a measured first apply/caught-up")
    if not report["cursor_samples"] or not any(s["compared"] for s in report["cursor_samples"]):
        gaps.append("cursor-not-ahead not compared")
    return ("NOT_MEASURED", gaps) if gaps else ("PASS", [])


def inspect_writer(name: str) -> dict | None:
    result = subprocess.run(["docker", "inspect", name, "--format",
                             '{"id":{{json .Id}},"started":{{json .State.StartedAt}},"finished":{{json .State.FinishedAt}},'
                             '"running":{{json .State.Running}},"restarts":{{.RestartCount}}}'],
                            capture_output=True, text=True, timeout=20)
    return json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else None


def read_cursors(writer: str) -> dict[str, int] | None:
    result = subprocess.run(["docker", "exec", writer, "python", "-B", "-c", CURSOR_DUMP],
                            capture_output=True, text=True, timeout=30)
    try:
        return {str(k): int(v) for k, v in json.loads(result.stdout).items()} if result.returncode == 0 else None
    except (ValueError, AttributeError):
        return None


def run(args) -> dict:
    binding = json.loads(Path(args.binding).read_text())
    items = demanded_slices(binding)
    host = args.host or container_ip(args.redis_container, args.network)
    redis = ReadOnlyRedis(host, args.port, args.db)
    ids = identities_from_keys(redis.scan("cache:market:v2:*", limit=20000))
    plan, slices, uid_slice, unresolved = [], {}, {}, []
    for item in items:
        identity = ids.get((item["venue"], item["native"]))
        if identity is None:
            unresolved.append(f"{item['venue']}|{item['native']}|{item['feed']}")
            continue
        keys = slice_keys(item, identity)
        plan.append((item, keys))
        slices[keys["name"]] = SliceRecovery(keys["name"], item["feed"], item["interval"], args.poll_ms)
        uid_slice[f"{item['uid']}|{item['feed']}"] = keys["name"]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    evidence = BoundedJsonl(out / "g11-observations.jsonl", args.max_bytes)
    value_keys = [k["value"] for _, k in plan]
    applied_keys = [k["applied"] for _, k in plan if k["applied"]]

    def poll():
        commands = [("MGET", *value_keys)] + [("PTTL", k["ttl"]) for _, k in plan]
        if applied_keys:
            commands.append(("MGET", *applied_keys))
        replies = redis.pipeline(commands)
        values, ttls = replies[0], replies[1:1 + len(plan)]
        applied = dict(zip(applied_keys, replies[-1])) if applied_keys else {}
        now_ms = time.time_ns() // 1_000_000
        return now_ms, {k["name"]: observe(item["feed"], raw, applied.get(k["applied"]), ttl, item["interval"])
                        for (item, k), raw, ttl in zip(plan, values, ttls)}

    base = inspect_writer(args.writer)
    if base is None or not base["running"]:
        raise SystemExit("writer is not running at arm time; the probe measures a restart from a running writer")
    evidence.write({"kind": "armed", "at_ms": time.time_ns() // 1_000_000, "writer": base,
                    "slices": len(plan), "unresolved": unresolved})
    print(json.dumps({"kind": "G11_ARMED", "writer": args.writer, "wait_seconds": args.wait_seconds}), flush=True)
    restart, phase, last_old_seen_ms = None, "PRE", None
    cleared_ms = None
    deadline = time.monotonic() + args.wait_seconds
    observe_until = None
    next_inspect = time.monotonic()
    cursor_samples, read_errors = [], 0
    try:
        while not stopped():
            now = time.monotonic()
            if observe_until is None and now >= deadline:
                break
            if observe_until is not None and now >= observe_until:
                break
            if now >= next_inspect:
                next_inspect = now + 1.0
                state = inspect_writer(args.writer)
                if restart is None:
                    if state and state["started"] == base["started"] and state["id"] == base["id"]:
                        last_old_seen_ms = time.time_ns() // 1_000_000
                    else:
                        phase = "DOWN"
                        restart = {"detected_ms": time.time_ns() // 1_000_000, "last_old_seen_ms": last_old_seen_ms}
                if restart is not None and cleared_ms is None and state and state["running"] and (
                        state["started"] != base["started"] or state["id"] != base["id"]):
                    restart.update(new=state, outage_start_ms=parse_docker_time(state["finished"])
                                   if state["id"] == base["id"] else last_old_seen_ms,
                                   fault_cleared_ms=parse_docker_time(state["started"]))
                    cleared_ms = restart["fault_cleared_ms"]
                    phase = "POST"
                    observe_until = time.monotonic() + args.observe_seconds
                    evidence.write({"kind": "restart", **restart})
            try:
                now_ms, observed = poll()
            except Exception as error:
                read_errors += 1
                evidence.write({"kind": "read_error", "at_ms": time.time_ns() // 1_000_000,
                                "error": type(error).__name__})
                pause(args.poll_ms / 1000)
                continue
            effective = "POST" if phase == "POST" and cleared_ms is not None and now_ms >= cleared_ms else (
                "DOWN" if phase != "PRE" else "PRE")
            for name, obs in observed.items():
                before = slices[name].first_apply_ms, slices[name].caught_up_ms
                slices[name].update(now_ms, obs, phase=effective)
                if effective == "POST" and (slices[name].first_apply_ms, slices[name].caught_up_ms) != before:
                    evidence.write({"kind": "slice_progress", "slice": name, "at_ms": now_ms,
                                    "first_apply_ms": slices[name].first_apply_ms,
                                    "caught_up_ms": slices[name].caught_up_ms, "event_ms": obs["event_ms"]})
            if (effective == "POST" and len(cursor_samples) < args.cursor_samples
                    and now_ms - cleared_ms >= 20_000 * (len(cursor_samples) + 1)):
                cursors = read_cursors(args.writer)  # cursor first, then the applied offsets it must not exceed
                _, after = poll()
                applied = {n: o["offset"] for n, o in after.items() if o["offset"] is not None}
                sample = (cursor_check(cursors, applied, uid_slice) if cursors is not None
                          else {"compared": 0, "unmatched": 0, "ahead": [], "error": "cursor read failed"})
                sample["at_ms"] = now_ms
                cursor_samples.append(sample)
                evidence.write({"kind": "cursor_check", **sample})
            pause(max(0.0, args.poll_ms / 1000 - (time.monotonic() - now)))
    finally:
        redis.close()
        evidence.close()
    report = {"schema": "qdl.p184.g11-writer-restart-probe.v1", "gate": GATE, "writer": args.writer,
              "redis": {"container": args.redis_container, "host": host, "db": args.db}, "poll_ms": args.poll_ms,
              "measuring_point": "host poll of TS cache keys; first apply/caught-up are upper bounds by one poll",
              "observe_seconds": args.observe_seconds, "unresolved_slices": unresolved, "read_errors": read_errors,
              "restart": restart, "recovery": recovery_summary(slices, cleared_ms), "cursor_samples": cursor_samples}
    if restart and restart.get("outage_start_ms") and cleared_ms:
        report["outage_seconds"] = round((cleared_ms - restart["outage_start_ms"]) / 1000, 3)
    report["verdict"], report["reasons"] = verdict(report)
    write_json_exclusive(out / "g11-summary.json", report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--binding", required=True)
    parser.add_argument("--writer", default="market_data_service_p184w")
    parser.add_argument("--redis-container", default="redis_marketdata")
    parser.add_argument("--network", default="executor_network")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=1)
    parser.add_argument("--wait-seconds", type=float, default=900)
    parser.add_argument("--observe-seconds", type=float, default=180)
    parser.add_argument("--poll-ms", type=int, default=500)
    parser.add_argument("--cursor-samples", type=int, default=3)
    parser.add_argument("--max-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    install_stop_signals()
    if not 100 <= args.poll_ms <= 5000 or not 0 < args.wait_seconds <= 3600 or not 60 <= args.observe_seconds <= 900:
        parser.error("poll-ms 100..5000, wait-seconds 0..3600, observe-seconds 60..900")
    report = run(args)
    print(json.dumps({"verdict": report["verdict"], "reasons": report["reasons"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

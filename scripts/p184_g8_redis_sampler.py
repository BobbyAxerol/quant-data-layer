#!/usr/bin/env python3
"""P18.4 gate G8: TS execution Redis command CPU, SLOWLOG count and PING RTT under full mixed traffic (read-only).

Which Redis: the writer projects the execution market cache through TRADING_MARKET_REDIS_URL (TS shared/config.py,
default redis://redis_marketdata:6379/1; MARKET_REDIS_ROLLOUT=EPHEMERAL_ONLY writes only there, TS
services/market_data/redis_plane.py write_targets). TRADING_REDIS_URL (redis_service) carries heartbeats and the
control plane, not the APPLY scripts. The campaign runner records the writer's resolved URL beside this evidence.

Samples (allowlisted read-only commands only, never MONITOR, never SLOWLOG RESET or CONFIG SET):
* PING every --ping-ms: client-side round trip (host -> container IP), ms;
* every --stats-seconds: INFO commandstats (cumulative usec/calls per command) and SLOWLOG GET 128 plus SLOWLOG
  threshold (CONFIG GET slowlog-log-slower-than). Per SLOWLOG row: id, start time, duration, command name, and
  (attribution, report-only, 2026-10-15) the client address and client name Redis >= 4 records in the entry, and for
  EVAL/EVALSHA(_RO) a script identity: the sha argument of EVALSHA, the sha1 of the EVAL script body (computed here,
  the body is never stored; SLOWLOG cuts arguments over 128 bytes, so for a cut body the sha1 is of the visible
  prefix and script_body_truncated is true - it then identifies the script family, not the SCRIPT LOAD sha). Keys
  and every other argument are never kept;
* every 60 s: CLIENT LIST, reduced to the count of clients whose flags contain 'O' (MONITOR).

Gate (frozen table, rev 2): sum of INFO commandstats usec deltas <= 60 % of one core over every 10-min window; SLOWLOG
(10 ms threshold) new-entry count 0 in steady state; PING p99 <= 5 ms under full mixed traffic. summarize() is shared
with the campaign scorer, which passes the steady window; a commandstats counter that goes backwards (Redis restart)
invalidates that window instead of producing a negative delta.
usage: p184_g8_redis_sampler.py --seconds N --out DIR [--container redis_marketdata]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_resp_readonly import (  # noqa: E402
    install_stop_signals, pause, stopped,
    BoundedJsonl, ReadOnlyRedis, container_ip, quantiles, read_jsonl, write_json_exclusive,
)

GATE = {"commandstats_core_fraction_le": 0.60, "window_seconds": 600, "slowlog_steady_count": 0,
        "ping_p99_le_ms": 5.0, "slowlog_threshold_us": 10000}


def parse_commandstats(text: str) -> dict[str, dict[str, int]]:
    out = {}
    for line in text.splitlines():
        if not line.startswith("cmdstat_"):
            continue
        name, _, fields = line.partition(":")
        values = dict(item.split("=", 1) for item in fields.split(",") if "=" in item)
        try:
            out[name[len("cmdstat_"):]] = {"calls": int(values["calls"]), "usec": int(values["usec"])}
        except (KeyError, ValueError):
            continue
    return out


_SLOWLOG_CUT = re.compile(rb"\.\.\. \((\d+) more bytes\)$")
_SHA1_HEX = re.compile(r"[0-9a-f]{40}")


def _text(value, limit: int) -> str | None:
    if value is None:
        return None
    return (value.decode(errors="replace") if isinstance(value, bytes) else str(value))[:limit]


def script_identity(command: str, args) -> dict:
    """Script identity of an EVAL/EVALSHA(_RO) SLOWLOG row; {} for any other command. Only a digest is returned."""
    if len(args) < 2:
        return {}
    body = args[1] if isinstance(args[1], bytes) else str(args[1]).encode()
    if command in ("EVALSHA", "EVALSHA_RO"):
        sha = body.decode(errors="replace").lower()
        return {"script_sha1": sha if _SHA1_HEX.fullmatch(sha) else None, "script_sha1_basis": "EVALSHA_ARGUMENT"}
    if command in ("EVAL", "EVAL_RO"):
        cut = _SLOWLOG_CUT.search(body)
        visible = body[:cut.start()] if cut else body
        return {"script_sha1": hashlib.sha1(visible).hexdigest(),
                "script_sha1_basis": "SHA1_OF_VISIBLE_PREFIX" if cut else "SHA1_OF_BODY",
                "script_body_truncated": bool(cut)}
    return {}


def slowlog_rows(reply) -> list[dict]:
    """SLOWLOG GET rows: [id, start, duration_us, args, client_addr, client_name] (the last two from Redis 4).
    Keeps the command name, client address/name and a script digest; never a key or any other argument."""
    rows = []
    for entry in reply or []:
        try:
            args = entry[3] or []
            command = args[0].decode(errors="replace")[:32].upper() if args else "?"
            row = {"id": int(entry[0]), "start_s": int(entry[1]), "duration_us": int(entry[2]), "command": command,
                   "client_addr": _text(entry[4], 64) if len(entry) > 4 else None,
                   "client_name": _text(entry[5], 64) if len(entry) > 5 else None}
            row.update(script_identity(command, args))
            rows.append(row)
        except (IndexError, TypeError, ValueError, AttributeError):
            continue
    return rows


def slowlog_attribution(rows) -> list[dict]:
    """Report-only: count / max duration per (command, script, client name)."""
    groups = {}
    for row in rows:
        key = (row.get("command"), row.get("script_sha1"), row.get("client_name"))
        g = groups.setdefault(key, {"command": key[0], "script_sha1": key[1], "client_name": key[2], "count": 0,
                                    "max_duration_us": 0, "client_addrs": set()})
        g["count"] += 1
        g["max_duration_us"] = max(g["max_duration_us"], row.get("duration_us") or 0)
        if row.get("client_addr"):
            g["client_addrs"].add(row["client_addr"])
    out = [{**g, "client_addrs": sorted(g["client_addrs"])[:10]} for g in groups.values()]
    return sorted(out, key=lambda g: (-g["count"], str(g["command"])))


def monitor_clients(text: str) -> int:
    count = 0
    for line in text.splitlines():
        for field in line.split():
            if field.startswith("flags=") and "O" in field[len("flags="):]:
                count += 1
    return count


def summarize(samples, *, steady_from_ns=None, steady_to_ns=None, window_seconds=600) -> dict:
    """Window the raw samples. Windows are consecutive window_seconds spans from the first stats sample; a window
    shorter than 90 % of window_seconds (the tail) is reported but not judged."""
    stats = [s for s in samples if s.get("kind") == "stats"]
    pings = [s for s in samples if s.get("kind") == "ping"]
    slow, seen_ids = [], set()
    baseline_ids = None
    for s in stats:
        ids = {row["id"] for row in s.get("slowlog", [])}
        if baseline_ids is None:
            baseline_ids = ids  # entries already in the log at start are history, not this run
            continue
        for row in s.get("slowlog", []):
            if row["id"] not in baseline_ids and row["id"] not in seen_ids:
                seen_ids.add(row["id"])
                slow.append(row)
    windows = []
    if len(stats) >= 2:
        origin = stats[0]["at_ns"]
        span = window_seconds * 1_000_000_000
        index = 0
        while True:
            lo, hi = origin + index * span, origin + (index + 1) * span
            inside = [s for s in stats if lo <= s["at_ns"] <= hi]
            if len(inside) < 2:
                break
            first, last = inside[0], inside[-1]
            seconds = (last["at_ns"] - first["at_ns"]) / 1e9
            deltas, valid = {}, True
            for name, value in last["commandstats"].items():
                before = first["commandstats"].get(name, {"usec": 0, "calls": 0})
                delta = value["usec"] - before["usec"]
                if delta < 0:
                    valid = False
                deltas[name] = delta
            total = sum(deltas.values())
            fraction = round(total / (seconds * 1e6), 4) if seconds > 0 and valid else None
            ping = quantiles([p["rtt_ms"] for p in pings if lo <= p["at_ns"] < hi and p.get("rtt_ms") is not None])
            steady = (steady_from_ns is not None and steady_to_ns is not None
                      and lo >= steady_from_ns and hi <= steady_to_ns)
            windows.append({
                "from_ns": first["at_ns"], "to_ns": last["at_ns"], "seconds": round(seconds, 1),
                "judged": seconds >= 0.9 * window_seconds and valid,
                "valid": valid, "steady": steady, "core_fraction": fraction,
                "top_commands_usec": dict(sorted(deltas.items(), key=lambda kv: -kv[1])[:5]),
                "slowlog_new": sum(1 for r in slow if lo // 10**9 <= r["start_s"] < hi // 10**9),
                "ping_ms": ping,
            })
            index += 1
    in_steady = lambda ns: (steady_from_ns is not None and steady_to_ns is not None  # noqa: E731
                            and steady_from_ns <= ns <= steady_to_ns)
    steady_pings = [p["rtt_ms"] for p in pings if p.get("rtt_ms") is not None and in_steady(p["at_ns"])]
    steady_slow = [r for r in slow if in_steady(r["start_s"] * 10**9)]
    ping_failures = sum(1 for p in pings if p.get("rtt_ms") is None)
    return {
        "windows": windows,
        "slowlog_new_total": len(slow),
        "slowlog_new_steady": len(steady_slow),
        "slowlog_steady_rows": steady_slow[:50],
        "slowlog_steady_attribution": slowlog_attribution(steady_slow),
        "slowlog_new_attribution": slowlog_attribution(slow),
        "ping_all_ms": quantiles([p["rtt_ms"] for p in pings if p.get("rtt_ms") is not None]),
        "ping_steady_ms": quantiles(steady_pings),
        "ping_failures": ping_failures,
        "monitor_clients_max": max((s.get("monitor_clients", 0) for s in stats if "monitor_clients" in s), default=None),
        "slowlog_threshold_us": next((s["slowlog_threshold_us"] for s in stats if "slowlog_threshold_us" in s), None),
    }


def verdict(summary: dict) -> tuple[str, list[str]]:
    reasons = []
    judged = [w for w in summary["windows"] if w["judged"]]
    for w in judged:
        if w["core_fraction"] is not None and w["core_fraction"] > GATE["commandstats_core_fraction_le"]:
            reasons.append(f"commandstats {w['core_fraction']} core > 0.60 in window from {w['from_ns']}")
    if summary["slowlog_new_steady"] > 0:
        reasons.append(f"SLOWLOG {summary['slowlog_new_steady']} new entries in steady state")
    steady_ping = summary["ping_steady_ms"]
    if steady_ping.get("p99") is not None and steady_ping["p99"] > GATE["ping_p99_le_ms"]:
        reasons.append(f"PING p99 {steady_ping['p99']} ms > 5 ms")
    if reasons:
        return "FAIL", reasons
    missing = []
    if not any(w["steady"] and w["judged"] for w in summary["windows"]):
        missing.append("no judged 10-min window inside the steady state")
    if steady_ping.get("p99") is None:
        missing.append("steady PING n < 100")
    if summary["slowlog_threshold_us"] != GATE["slowlog_threshold_us"]:
        missing.append(f"SLOWLOG threshold is {summary['slowlog_threshold_us']} us, gate assumes 10000")
    if summary["monitor_clients_max"]:
        missing.append("MONITOR clients attached during the run (their cost is in commandstats)")
    return ("NOT_MEASURED", missing) if missing else ("PASS", [])


def run(args) -> Path:
    host = args.host or container_ip(args.container, args.network)
    redis = ReadOnlyRedis(host, args.port, 0)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    evidence = BoundedJsonl(out / "g8-samples.jsonl", args.max_bytes)
    evidence.write({"kind": "scope", "container": args.container, "host": host, "ping_ms": args.ping_ms,
                    "stats_seconds": args.stats_seconds, "commands": "PING, INFO commandstats, SLOWLOG GET 128, "
                    "CONFIG GET slowlog-log-slower-than, CLIENT LIST (MONITOR flag count only)"})
    deadline = time.monotonic() + args.seconds
    next_stats = next_clients = time.monotonic()
    try:
        while time.monotonic() < deadline and not stopped():
            tick = time.monotonic()
            try:
                started = time.perf_counter_ns()
                redis.call("PING")
                evidence.write({"kind": "ping", "at_ns": time.time_ns(),
                                "rtt_ms": round((time.perf_counter_ns() - started) / 1e6, 3)})
            except Exception as error:
                evidence.write({"kind": "ping", "at_ns": time.time_ns(), "rtt_ms": None,
                                "error": type(error).__name__})
            if tick >= next_stats:
                next_stats += args.stats_seconds
                try:
                    info, slowlog, threshold = redis.pipeline([("INFO", "commandstats"), ("SLOWLOG", "GET", 128),
                                                               ("CONFIG", "GET", "slowlog-log-slower-than")])
                    sample = {"kind": "stats", "at_ns": time.time_ns(),
                              "commandstats": parse_commandstats(info.decode()),
                              "slowlog": slowlog_rows(slowlog),
                              "slowlog_threshold_us": int(threshold[1])}
                    if tick >= next_clients:
                        next_clients += 60
                        sample["monitor_clients"] = monitor_clients(redis.call("CLIENT", "LIST").decode())
                    evidence.write(sample)
                except Exception as error:
                    evidence.write({"kind": "stats_error", "at_ns": time.time_ns(), "error": type(error).__name__})
            pause(max(0.0, args.ping_ms / 1000.0 - (time.monotonic() - tick)))
    finally:
        redis.close()
        evidence.close()
    summary = summarize(list(read_jsonl(out / "g8-samples.jsonl")))
    state, reasons = verdict(summary)
    write_json_exclusive(out / "g8-summary.json", {"schema": "qdl.p184.g8-redis-sampler.v1", "gate": GATE,
                                                   "redis": {"container": args.container, "host": host},
                                                   "steady_window": "not applied (whole run)", **summary,
                                                   "self_verdict_whole_run": state, "reasons": reasons})
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--container", default="redis_marketdata")
    parser.add_argument("--network", default="executor_network")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--ping-ms", type=int, default=500)
    parser.add_argument("--stats-seconds", type=float, default=10.0)
    parser.add_argument("--max-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    install_stop_signals()
    if not 100 <= args.ping_ms <= 10000 or not 0 < args.seconds <= 4 * 3600:
        parser.error("ping-ms must be 100..10000 and seconds 0..14400")
    out = run(args)
    print(json.dumps({"output": str(out)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""DL load observation: one read-only pass over the KN Data Layer, typed PASS/WARN/FAIL lines.

Purpose
    The owner gates the bundled DL release (v2.4.3 + DL-BL-2026-11-25-B/C + R1 retention) on observing DL under
    the larger multi-alpha Paper load (plan journal "DL release decision 2026-11-25"). Each run prints the
    signals of that gate and compares them with the first run, the baseline, which it records:
    - bar_edge heartbeat counters;
    - new error classes per role;
    - Query refusal rates by code;
    - projector state;
    - market_cache memory and its growth;
    - BAR freshness for OKX/Binance 1m/1h/12h;
    - host memory, swap and OOM kills;
    - load proxies.

Boundary
    Read-only. Uses only `docker inspect`, `docker logs`, `docker exec ... cat` of the bar_edge heartbeat, read-only
    `redis-cli` (INFO, SCAN, HGET) on market_cache, and /proc.
    - Never starts, stops or recreates a container.
    - Never writes Redis, Kafka or Postgres; never reads secret files.
    - Writes only under --out-dir: the run report, `baseline.json` (first run) and `last-sample.json`.
    Logs are read whole into memory and grepped in Python. There is no `grep -q` behind a pipe, which reports "no
    match" exactly when the line exists under `set -o pipefail` (SIGPIPE; v2.4.3 window lesson).

Known limit
    DL exposes no per-consumer Query request counter: no access log, no metrics route, no audit file (checked
    2026-11-25 against qdl/runtime/stable.py). Load growth is shown with proxies instead:
    - Stream `subscriptions_active` / `hub_subscribers`;
    - Query slow-request and refusal volume per route;
    - warmup:batch volume.
    A per-consumer counter is registered as DL-BL-2026-11-25-D.

Usage
    python3 -B dl_load_observation.py [--since 2026-11-25T10:04:14Z] [--out-dir DIR]
    --since is the start of the error-class and rate window (default: 1 h before now).
    Exit 0 = no FAIL; 1 = at least one FAIL.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import re
import subprocess
import sys
import time
from pathlib import Path

P = "qdl_v2_stable_candidate"
ROLES = ("binance_bar_edge", "market_projector_1", "market_projector_2", "query_kn_1", "query_kn_2",
         "stream_kn_1", "stream_kn_2", "ingestor_okx_swap", "ingestor_binance_usdm", "rust_core", "rust_core_2",
         "rust_core_3", "market_cache", "kafka1", "kafka2", "kafka3")
LOG_ROLES = ROLES[:12]  # every role with application logs (not market_cache or kafka)
HB = "/var/lib/qdl-kn/heartbeat/bar-edge.json"
KNOWN_HISTORY_FAILED = 5  # the five Binance 3d OVERLAPPING_PROVIDER_WINDOWS bindings (since 2026-11-19)
STEP = {"1m": 60_000, "1h": 3_600_000, "12h": 43_200_000}
GRACE = {"1m": 90_000, "1h": 300_000, "12h": 300_000}
ERROR_LINE = re.compile(r" ERROR |Traceback|panicked|\"level\":\"(?:ERROR|error)\"|"
                        r"\"event\":\"[a-z_]*(?:fail|error|reject|panic)[a-z_]*\"")
MASK = [(re.compile(r"[0-9a-f]{8}-[0-9a-f-]{27}"), "U"), (re.compile(r"binding=[a-z0-9-]+"), "binding=B"),
        (re.compile(r"\"binding\":\"[^\"]+\""), "\"binding\":B"), (re.compile(r"[0-9]+"), "N")]
LINES: list[str] = []
WORST = {"PASS": 0, "INFO": 0, "WARN": 1, "FAIL": 2}
STATUS = {"worst": 0}


def run(*args: str, timeout: int = 120) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return result.stdout + result.stderr


def say(level: str, area: str, text: str) -> None:
    line = f"{level:4} {area:18} {text}"
    LINES.append(line)
    print(line, flush=True)
    STATUS["worst"] = max(STATUS["worst"], WORST[level])


def logs(role: str, since: str) -> str:
    return run("docker", "logs", "--since", since, f"{P}-{role}-1", timeout=300)


def error_classes(text: str) -> collections.Counter:
    classes: collections.Counter = collections.Counter()
    for line in text.splitlines():
        if not ERROR_LINE.search(line):
            continue
        key = line
        match = re.search(r"\"event\":\"([a-z_]+)\"", line)
        if match:
            code = re.search(r"\"code\":\"([A-Z_]+)\"", line)
            key = match.group(1) + (":" + code.group(1) if code else "")
        else:
            key = re.sub(r"^\S+ \S+ ", "", line)  # python logging timestamp
            for pattern, replacement in MASK:
                key = pattern.sub(replacement, key)
            key = key[:140]
        classes[key] += 1
    return classes


def heartbeat() -> dict:
    raw = run("docker", "exec", f"{P}-binance_bar_edge-1", "cat", HB)
    try:
        document = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    detail: dict = {}
    for token in document.get("detail", "").split():
        name, _, value = token.partition("=")
        detail[name] = value  # last occurrence wins (bindings= appears twice)
    detail["_age_s"] = int((time.time_ns() - int(document.get("updated_at_ns", 0))) / 1e9)
    return detail


def bar_freshness() -> dict:
    script = ('for iv in 1m 1h 12h; do redis-cli --scan --pattern "kn3:paper:ptr:lpk1|paper|*|BAR|$iv" '
              '--count 50000; done | while read -r k; do lpk=${k#kn3:paper:ptr:}; g=$(redis-cli HGET "$k" ready); '
              'echo "$lpk $(redis-cli HGET "kn3:paper:bm:$g:$lpk" last)"; done')
    out = run("docker", "exec", f"{P}-market_cache-1", "sh", "-c", script, timeout=600)
    now = int(time.time() * 1000)
    total: collections.Counter = collections.Counter()
    stale: collections.Counter = collections.Counter()
    for line in out.splitlines():
        parts = line.split()
        if not parts or "|" not in parts[0]:
            continue
        fields = parts[0].split("|")
        venue, interval = fields[2], fields[6]
        step = STEP[interval]
        expected = (now // step) * step - step
        if now - (now // step) * step < GRACE[interval]:
            expected -= step
        last = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        total[(venue, interval)] += 1
        stale[(venue, interval)] += last < expected
    return {f"{venue} {interval}": [total[(venue, interval)], stale[(venue, interval)]]
            for venue, interval in sorted(total)}


def refusal_rates(text: str, hours: float) -> dict:
    counts: collections.Counter = collections.Counter()
    for line in text.splitlines():
        if '"event":"qdl_refusal"' not in line:
            continue
        try:
            document = json.loads(line[line.index("{"):])
        except (ValueError, json.JSONDecodeError):
            continue
        key = f'{document.get("code")}[{",".join(document.get("reasons", []))}]{document.get("feed", "")}'
        counts[key] += 1 + int(document.get("suppressed_before", 0))
    return {key: round(value / hours, 1) for key, value in counts.items()}


def load_proxies(text_by_role: dict, hours: float) -> dict:
    proxies: dict = {}
    for role in ("query_kn_1", "query_kn_2"):
        routes: collections.Counter = collections.Counter()
        for line in text_by_role[role].splitlines():
            match = re.search(r'"kind":"slow_request".*?"path":"([^"]+)"', line)
            if match:
                routes[re.sub(r"[0-9a-f]{8}-[0-9a-f-]{27}", "{uid}", match.group(1))] += 1
        proxies[f"{role} slow_requests/h"] = {key: round(value / hours, 1) for key, value in routes.most_common(6)}
    for role in ("stream_kn_1", "stream_kn_2"):
        last = [line for line in text_by_role[role].splitlines() if '"subscriptions_active"' in line][-1:]
        if last:
            document = json.loads(last[0][last[0].index("{"):])
            proxies[f"{role} subscriptions"] = {key: document.get(key) for key in
                                                ("subscriptions_active", "hub_subscribers", "lagging", "refused")}
    return proxies


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--since", default=None)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args(argv)
    now = dt.datetime.now(dt.timezone.utc)
    since = args.since or (now - dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    hours = max(1 / 60, (now - dt.datetime.fromisoformat(since.replace("Z", "+00:00"))).total_seconds() / 3600)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    baseline_path, last_path = out_dir / "baseline.json", out_dir / "last-sample.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.is_file() else None
    last = json.loads(last_path.read_text()) if last_path.is_file() else None
    sample: dict = {"at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "since": since, "hours": round(hours, 3)}
    say("INFO", "run", f"at {sample['at']} window since {since} ({hours:.2f} h); baseline "
        f"{'from ' + baseline['at'] if baseline else 'is recorded by this run'}")

    # Roles: image, restarts, health, OOMKilled
    roles = {}
    for role in ROLES:
        state = run("docker", "inspect", f"{P}-{role}-1", "--format",
                    "{{.Image}} {{.RestartCount}} {{.State.Status}} {{.State.OOMKilled}} {{.State.StartedAt}} "
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}").split()
        roles[role] = state
        if len(state) < 6:
            say("FAIL", "role", f"{role}: not inspectable")
            continue
        image, restarts, status, oom, started, health = state
        reference = (baseline or {}).get("roles", {}).get(role)
        changed = reference is not None and (reference[0] != image or reference[1] != restarts
                                             or reference[4] != started)
        level = "FAIL" if status != "running" or oom == "true" or health == "unhealthy" else (
            "WARN" if changed else "PASS")
        say(level, "role", f"{role}: {status} health={health} restarts={restarts} oom={oom} image={image[7:19]}"
            + (" CHANGED since baseline" if changed else ""))
    sample["roles"] = roles

    # bar_edge heartbeat
    hb = heartbeat()
    sample["heartbeat"] = hb
    if not hb:
        say("FAIL", "bar_edge", "heartbeat unreadable")
    else:
        checks = [
            ("published", f"{hb.get('published')}/{hb.get('bindings')}", hb.get("published") == hb.get("bindings")),
            ("rebootstrap_waiting", hb.get("rebootstrap_waiting"), hb.get("rebootstrap_waiting") == "0"),
            ("resume_pending", hb.get("resume_pending"), hb.get("resume_pending") == "0"),
            ("venue_holes_refused", hb.get("venue_holes_refused"), hb.get("venue_holes_refused") == "0"),
            ("history_failed", hb.get("history_failed"), hb.get("history_failed") == str(KNOWN_HISTORY_FAILED)),
            ("instrument_unavailable", hb.get("instrument_unavailable"), hb.get("instrument_unavailable") == "0"),
            ("pending/overdue", f"{hb.get('pending')}/{hb.get('overdue')}", hb.get("overdue") == "0"),
            ("heartbeat age", f"{hb['_age_s']}s", hb["_age_s"] < 180),
        ]
        for name, value, ok in checks:
            say("PASS" if ok else "WARN", "bar_edge", f"{name}={value}")
        say("INFO", "bar_edge", f"venue_holes_recorded={hb.get('venue_holes_recorded')} short={hb.get('short')} "
            f"history_suspended={hb.get('history_suspended')} gate_closed={hb.get('gate_closed')}")

    # Error classes per role since --since
    texts = {role: logs(role, since) for role in LOG_ROLES}
    known = (baseline or {}).get("error_classes", {})
    classes_now: dict = {}
    for role in LOG_ROLES:
        found = error_classes(texts[role])
        classes_now[role] = dict(found)
        new = {key: count for key, count in found.items() if baseline and key not in known.get(role, {})}
        if new:
            for key, count in sorted(new.items(), key=lambda item: -item[1])[:8]:
                say("FAIL", "errors", f"{role}: NEW class x{count}: {key}")
        say("PASS" if not new else "WARN", "errors",
            f"{role}: {sum(found.values())} error lines in {len(found)} known class(es)" if found
            else f"{role}: no error lines")
    sample["error_classes"] = classes_now

    # Query refusal rates per hour vs baseline
    rates = {}
    for role in ("query_kn_1", "query_kn_2"):
        rates[role] = refusal_rates(texts[role], hours)
        reference = (baseline or {}).get("refusal_rates", {}).get(role, {})
        for key, value in sorted(rates[role].items(), key=lambda item: -item[1]):
            base = reference.get(key)
            if baseline and base is None:
                level, note = "WARN", "new code"
            elif base and value > 2 * base + 20:
                level, note = "WARN", f"> 2x baseline {base}/h"
            else:
                level, note = "PASS", f"baseline {base}/h" if base is not None else "baseline"
            say(level, "query refusals", f"{role} {key}: {value}/h ({note})")
    sample["refusal_rates"] = rates

    # Projectors
    for role in ("market_projector_1", "market_projector_2"):
        lines = [line for line in texts[role].splitlines() if "projector-status" in line][-1:]
        if not lines:
            say("WARN", "projector", f"{role}: no status line in window")
            continue
        document = json.loads(lines[0][lines[0].index("{"):])
        stage_b = document["stage_b"]
        state = {key: stage_b.get(key) for key in ("conflicts", "builds", "cache_reconnects", "ownership_lost",
                                                    "rebuilding", "last_rebuild_error", "memory_pressure_ms",
                                                    "bars_applied")}
        previous = (last or {}).get("projector", {}).get(role, {})
        grew = previous and state["conflicts"] > previous.get("conflicts", 0)
        bad = document.get("errors") or state["rebuilding"] or state["last_rebuild_error"]
        say("FAIL" if bad else ("WARN" if grew else "PASS"), "projector",
            f"{role}: {json.dumps(state, sort_keys=True)} errors={document.get('errors')}"
            + (f" conflicts +{state['conflicts'] - previous.get('conflicts', 0)} since last run" if grew else ""))
        sample.setdefault("projector", {})[role] = state

    # market_cache memory
    info = run("docker", "exec", f"{P}-market_cache-1", "redis-cli", "INFO", "memory")
    memory = {key: int(value) for key, value in re.findall(r"^(used_memory|maxmemory|used_memory_rss):(\d+)",
                                                           info, flags=re.M)}
    keys = run("docker", "exec", f"{P}-market_cache-1", "redis-cli", "DBSIZE").strip()
    memory["dbsize"] = int(keys) if keys.isdigit() else -1
    sample["market_cache"] = memory
    used, cap = memory.get("used_memory", 0), memory.get("maxmemory", 1)
    growth = ""
    if last and "market_cache" in last:
        elapsed_h = max(1 / 60, (now - dt.datetime.fromisoformat(last["at"].replace("Z", "+00:00")))
                        .total_seconds() / 3600)
        growth = f" growth {(used - last['market_cache'].get('used_memory', used)) / 2**20 / elapsed_h:+.1f} MiB/h"
    ratio = used / cap
    say("FAIL" if ratio > 0.9 else "WARN" if ratio > 0.8 else "PASS", "market_cache",
        f"used {used / 2**30:.2f} GiB of maxmemory {cap / 2**30:.2f} GiB ({ratio:.0%}), rss "
        f"{memory.get('used_memory_rss', 0) / 2**30:.2f} GiB, keys {memory['dbsize']}{growth}")

    # BAR freshness
    fresh = bar_freshness()
    sample["bar_freshness"] = fresh
    for key, (total, stale) in fresh.items():
        say("PASS" if stale == 0 else "FAIL", "bar freshness", f"{key}: {total - stale}/{total} current")

    # Host memory, swap, OOM kills
    meminfo = dict(re.findall(r"^(\w+):\s+(\d+)", Path("/proc/meminfo").read_text(), flags=re.M))
    vmstat = dict(re.findall(r"^(\w+) (\d+)", Path("/proc/vmstat").read_text(), flags=re.M))
    avail = int(meminfo["MemAvailable"]) // 1024
    swap_used = (int(meminfo["SwapTotal"]) - int(meminfo["SwapFree"])) // 1024
    oom = int(vmstat.get("oom_kill", 0))
    sample["host"] = {"mem_available_mib": avail, "swap_used_mib": swap_used, "oom_kill": oom}
    say("FAIL" if avail < 2048 else "WARN" if avail < 4096 else "PASS", "host",
        f"MemAvailable {avail} MiB (owner floor 2048 MiB in live windows)")
    previous_swap = (last or {}).get("host", {}).get("swap_used_mib")
    say("WARN" if previous_swap is not None and swap_used > previous_swap + 512 else "INFO", "host",
        f"swap used {swap_used} MiB" + (f" ({swap_used - previous_swap:+d} MiB since last run)"
                                        if previous_swap is not None else ""))
    previous_oom = (last or {}).get("host", {}).get("oom_kill")
    say("FAIL" if previous_oom is not None and oom > previous_oom else "INFO", "host",
        f"/proc/vmstat oom_kill {oom}" + (f" (+{oom - previous_oom} since last run: a cgroup or global OOM kill; "
                                          "check which container)" if previous_oom is not None and
                                          oom > previous_oom else ""))

    # Load proxies (no per-consumer counter exists: DL-BL-2026-11-25-D)
    proxies = load_proxies(texts, hours)
    sample["load_proxies"] = proxies
    for key, value in proxies.items():
        say("INFO", "load", f"{key}: {json.dumps(value, sort_keys=True)}")
    say("WARN", "load", "per-consumer Query request rate: UNAVAILABLE (no DL counter; DL-BL-2026-11-25-D)")

    verdict = {0: "PASS", 1: "WARN", 2: "FAIL"}[STATUS["worst"]]
    say("INFO", "verdict", verdict)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    (out_dir / f"observation-{stamp}.txt").write_text("\n".join(LINES) + "\n", encoding="utf-8")
    last_path.write_text(json.dumps(sample, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    if baseline is None:
        baseline_path.write_text(json.dumps(sample, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    return 1 if STATUS["worst"] == 2 else 0


if __name__ == "__main__":
    sys.exit(main())

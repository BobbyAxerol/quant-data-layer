#!/usr/bin/env python3
"""Decision B runbook (P18.4 G12, owner 2026-10-16): roll kafka1-3 to ``auto.leader.rebalance.enable=false`` with the
production consumer stall measured per step, then a scoped preferred election, then the post-B parity check.

Purpose: one modular driver around the existing tools, so each step runs on its own and leaves a receipt:
``scripts/kafka_auto_leader_rebalance_roll.py`` (packet, --check, --roll with its postconditions and automatic
RESTORED_VERIFIED rollback), ``scripts/kafka_preferred_leader_election.py`` (--observe / --apply --window-out),
``scripts/kafka_internal_topic_compaction.py --observe`` (partition-43 size, temp key),
``scripts/p184_g9_g10_writer_counters.py`` (writer heartbeat READY counts, read-only) and
``scripts/p184_g12_isolated_fault.py --parity-snapshot``. Nothing here changes Kafka itself; every mutation is one
of those tools' ``--apply`` calls.

Order (owner/coordinator 2026-10-16): compaction ``--apply-temp`` -> wait for the cleaner pass on
``__consumer_offsets-43`` (pass watcher + ``--observe`` ``temp_removal``) -> ``--remove-temp`` -> ``check`` -> ``roll``
x3 -> ``elect --scope data`` -> ``final-check`` -> G12 final (``plan`` prints it).

Subcommands (receipt dir default ``RECEIPT``; every step writes ``<step>-<UTC>/`` inside it):

* ``plan`` (pure): prints the ordered coordinator sequence; runs nothing.
* ``check``: roll ``--check`` + compaction ``--observe`` (read-only; the observe starts a throwaway admin container).
* ``roll --broker kafkaN``: refuses unless the host lock is held by ``OWNER``; re-reads the quorum leader and refuses
  to roll it while another broker still runs without ``false`` (quorum leader last, re-checked at run time); starts
  the writer sampler; runs the roll tool with ``--apply --lock-owner OWNER``; while the active controller can still
  rebalance (another broker not yet ``false``) waits up to ``--settle-s`` for its automatic preferred election (the
  second coordinator move of the old behaviour, measured, not avoided); then collects the evidence window
  [start - ``--baseline-s``, end + ``--tail-s``]: rust_core progress lines (commit_ms / commit_wait_ms max per line),
  every broker's ``Finished loading`` lines (coordinator partition, records, bytes, ms), restart counts of the cores,
  projectors, stream and writer, and the writer READY counts. Exit code = the roll tool's: 0 rolled and verified;
  1 the tool rolled this broker back (``RESTORED_VERIFIED``): STOP; 3 HARD STOP; 4 refused; 5 command error.
* ``elect --scope data|coordinators|all``: the preferred election with ``--window-out``; ``coordinators``/``all``
  (a coordinator move = a state load = a production stall) refuse unless every ``__consumer_offsets-43`` load
  measured in this receipt dir took <= ``--coordinator-load-budget-ms`` (``--force-coordinators`` overrides; the
  election tool has no per-partition exclusion, only these scopes).
* ``evidence --since UTC --until UTC``: the collection alone (e.g. after a manual step).
* ``final-check``: roll ``--check`` (env false on all three) and a parity snapshot that must read
  ``auto.leader.rebalance.enable`` false (the G12 ``--production-parity`` input); the snapshot describes the production
  topics (``--cert-dir``) and refuses unless the effective config equals the committed plans (temp key absent).

Boundary: no secret is read or printed (cert dir mounted ro by the tools; container logs filtered to progress/load
lines; the writer env is never printed). Without the lock held by ``OWNER`` only ``plan``/``check``/``evidence``/
``final-check`` run.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.kafka_preferred_leader_election import GROUP_LOAD_LINE, TXN_LOAD_LINE  # noqa: E402
from scripts.host_measurement_lock import LOCK, lock_state  # noqa: E402

OWNER = "claude-kafka-b"
PROJECT = "qdl_v2_stable_candidate"
BROKERS = ("kafka1", "kafka2", "kafka3")
CORES = tuple(f"{PROJECT}-{name}-1" for name in ("rust_core", "rust_core_2", "rust_core_3"))
WATCHED = (*CORES, f"{PROJECT}-market_projector_1-1", f"{PROJECT}-market_projector_2-1",
           f"{PROJECT}-stream_kn_1-1", f"{PROJECT}-stream_kn_2-1", "market_data_service_p184w")
WRITER = "market_data_service_p184w"
STATE = Path.home() / ".local/state/qdl-v2"
RECEIPT = STATE / "p184-kafka-b-20261016"
PACKET = STATE / "p184-g12-coordinator-load-20261016/b-packet"
CERT = STATE / "655d2106d01f/cert-material-rotate-20260822T144323Z"
WATCH_PARTITION = "__consumer_offsets-43"
STALL_MS = 1000.0
PY = [sys.executable, "-B"]


class Refused(Exception):
    pass


def utc(t: dt.datetime | None = None) -> str:
    return (t or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


def tool(name: str) -> list[str]:
    return [*PY, str(ROOT / "scripts" / name)]


def roll_check_argv(packet: Path, cert: Path) -> list[str]:
    return [*tool("kafka_auto_leader_rebalance_roll.py"), "--check", str(packet), "--cert-dir", str(cert)]


def roll_argv(packet: Path, cert: Path, broker: str) -> list[str]:
    return [*tool("kafka_auto_leader_rebalance_roll.py"), "--roll", str(packet), "--broker", broker, "--apply",
            "--cert-dir", str(cert), "--lock-owner", OWNER]


def rollback_argv(packet: Path, cert: Path, broker: str) -> list[str]:
    return [*tool("kafka_auto_leader_rebalance_roll.py"), "--rollback", str(packet), "--broker", broker, "--apply",
            "--cert-dir", str(cert), "--lock-owner", OWNER]


def observe_leaders_argv(cert: Path) -> list[str]:
    return [*tool("kafka_preferred_leader_election.py"), "--observe", "--cert-dir", str(cert)]


def elect_argv(cert: Path, scope: str, window_out: Path) -> list[str]:
    return [*tool("kafka_preferred_leader_election.py"), "--apply", "--scope", scope, "--cert-dir", str(cert),
            "--window-out", str(window_out), "--lock-owner", OWNER]


def compaction_observe_argv(cert: Path) -> list[str]:
    return [*tool("kafka_internal_topic_compaction.py"), "--observe", "--cert-dir", str(cert)]


def sampler_argv(out: Path, log_dir: str, seconds: int) -> list[str]:
    return [*tool("p184_g9_g10_writer_counters.py"), "--container", "redis_service", "--db", "0",
            "--log-dir", log_dir, "--seconds", str(seconds), "--sample-seconds", "5", "--out", str(out)]


def parity_argv(out: Path, cert: Path) -> list[str]:
    return [*tool("p184_g12_isolated_fault.py"), "--parity-snapshot", str(out), "--cert-dir", str(cert)]


class Host:
    """Side effects, injectable for tests."""

    def run(self, argv: Sequence[str], timeout: float = 3600) -> tuple[int, str, str]:
        result = subprocess.run(list(argv), capture_output=True, text=True, timeout=timeout, cwd=ROOT)
        return result.returncode, result.stdout, result.stderr

    def inspect(self, name: str) -> dict | None:
        result = subprocess.run(["docker", "inspect", name], capture_output=True, text=True, timeout=30)
        return json.loads(result.stdout)[0] if result.returncode == 0 else None

    def logs(self, name: str, since: str, until: str) -> str:
        result = subprocess.run(["docker", "logs", "--timestamps", "--since", since, "--until", until, name],
                                capture_output=True, text=True, timeout=300)
        return result.stdout + result.stderr

    def popen(self, argv: Sequence[str], log: Path):
        handle = log.open("w", encoding="utf-8")
        return subprocess.Popen(list(argv), stdout=handle, stderr=subprocess.STDOUT, cwd=ROOT,
                                start_new_session=True)

    def now(self) -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def require_own_lock(path: Path = LOCK) -> dict:
    state, doc = lock_state(path)
    if state != "VALID" or (doc or {}).get("owner") != OWNER:
        raise Refused(f"host lock must be VALID and owned by {OWNER} (state {state}, owner {(doc or {}).get('owner')}); "
                      f"take it with scripts/host_measurement_lock.py acquire --owner {OWNER}")
    return doc


def step_dir(receipt: Path, step: str, host: Host) -> Path:
    receipt.mkdir(mode=0o700, parents=True, exist_ok=True)
    base = receipt / f"{step}-{host.now().strftime('%Y%m%dT%H%M%SZ')}"
    out, n = base, 1
    while out.exists():  # two steps in the same second never share a directory
        n += 1
        out = base.with_name(f"{base.name}-{n}")
    out.mkdir(mode=0o700)
    return out


def write(path: Path, body: Any) -> None:
    path.write_text(body if isinstance(body, str) else json.dumps(body, indent=1, sort_keys=True) + "\n",
                    encoding="utf-8")


def last_json(text: str) -> dict | None:
    try:
        return json.loads(text)
    except ValueError:
        start = text.find("{")
        try:
            return json.loads(text[start:]) if start >= 0 else None
        except ValueError:
            return None


# ---- evidence -------------------------------------------------------------------------------------------------

def core_progress(text: str) -> list[dict[str, Any]]:
    """``docker logs --timestamps`` of a rust_core -> one row per qdl_realtime_core_progress line."""
    rows = []
    for line in text.splitlines():
        stamp, _, body = line.partition(" ")
        if '"qdl_realtime_core_progress"' not in body:
            continue
        try:
            item = json.loads(body)
        except ValueError:
            continue
        span = {k: (item.get(k) or {}).get("max") for k in ("commit_ms", "commit_wait_ms", "raw_age_websocket_ms")}
        rows.append({"t": stamp[:23] + "Z", "generation": item.get("generation"), "batches": item.get("batches"),
                     "commit_max_ms": span["commit_ms"], "commit_wait_max_ms": span["commit_wait_ms"],
                     "raw_age_ws_max_ms": span["raw_age_websocket_ms"]})
    return rows


def summarize_core(rows: Sequence[Mapping[str, Any]], start: dt.datetime) -> dict[str, Any]:
    def part(selected):
        waits = [r["commit_wait_max_ms"] for r in selected if isinstance(r["commit_wait_max_ms"], (int, float))]
        commits = [r["commit_max_ms"] for r in selected if isinstance(r["commit_max_ms"], (int, float))]
        return {"lines": len(selected), "commit_wait_max_ms": max(waits, default=None),
                "commit_max_ms": max(commits, default=None),
                "stall_lines": [r for r in selected if (r["commit_wait_max_ms"] or 0) >= STALL_MS]}
    before = [r for r in rows if parse_utc(r["t"]) < start]
    during = [r for r in rows if parse_utc(r["t"]) >= start]
    return {"baseline": part(before), "window": part(during)}


def load_lines(text: str, broker: str) -> list[dict[str, Any]]:
    found = []
    for pattern, kind in ((GROUP_LOAD_LINE, "group"), (TXN_LOAD_LINE, "txn")):
        for match in pattern.finditer(text):
            item = {k: (int(v) if k not in ("ts", "tp") and v is not None else v) for k, v in match.groupdict().items()}
            found.append({"broker": broker, "kind": kind, **item})
    return sorted(found, key=lambda r: r["ts"])


def restarts(host: Host) -> dict[str, Any]:
    out = {}
    for name in WATCHED:
        item = host.inspect(name)
        out[name] = None if item is None else {"restarts": item.get("RestartCount"),
                                               "started": item["State"].get("StartedAt"),
                                               "running": item["State"].get("Running")}
    return out


def collect(host: Host, out: Path, start: dt.datetime, end: dt.datetime, baseline_s: float,
            before: Mapping[str, Any] | None = None) -> dict[str, Any]:
    since, until = utc(start - dt.timedelta(seconds=baseline_s)), utc(end)
    cores = {}
    with (out / "core-progress.jsonl").open("w", encoding="utf-8") as handle:
        for name in CORES:
            rows = core_progress(host.logs(name, since, until))
            for row in rows:
                handle.write(json.dumps({"core": name, **row}) + "\n")
            cores[name] = summarize_core(rows, start)
    loads = []
    for broker in BROKERS:
        loads += load_lines(host.logs(f"{PROJECT}-{broker}-1", utc(start), until), broker)
    write(out / "coordinator-loads.json", loads)
    after = restarts(host)
    restarted = sorted(n for n in WATCHED if before and before.get(n) and after.get(n)
                       and (before[n]["restarts"], before[n]["started"]) != (after[n]["restarts"], after[n]["started"]))
    watch = [r for r in loads if r.get("tp") == WATCH_PARTITION]
    summary = {"window_utc": [utc(start), utc(end)], "baseline_s": baseline_s,
               "core": {n: {"baseline_commit_wait_max_ms": c["baseline"]["commit_wait_max_ms"],
                            "window_commit_wait_max_ms": c["window"]["commit_wait_max_ms"],
                            "window_commit_max_ms": c["window"]["commit_max_ms"],
                            "window_stall_lines": len(c["window"]["stall_lines"]),
                            "first_stall": (c["window"]["stall_lines"] or [None])[0]} for n, c in cores.items()},
               "coordinator_loads": len(loads),
               "watch_partition_loads": [{k: r.get(k) for k in ("broker", "ts", "ms", "records", "bytes")}
                                         for r in watch],
               "max_load_ms": max((r["ms"] for r in loads), default=None),
               "restarted_during_window": restarted, "restarts_after": after}
    write(out / "evidence-summary.json", summary)
    return summary


def writer_summary(out: Path) -> dict[str, Any] | None:
    path = out / "writer-g9g10" / "g9-g10-summary.json"
    if not path.exists():
        return None
    g9 = json.loads(path.read_text(encoding="utf-8")).get("g9") or {}
    return {k: g9.get(k) for k in ("readable_samples", "unknown_samples", "demanded_values",
                                    "demanded_ne_session_ready_samples", "execution_ready_min",
                                    "execution_ready_max", "worker_not_live_samples", "eligibility_losses_data_path")}


class Sampler:
    def __init__(self, host: Host, out: Path, seconds: int) -> None:
        item = host.inspect(WRITER) or {}
        log_dir = next((m["Source"] for m in item.get("Mounts", []) if m.get("Destination") == "/app/logs"), None)
        self.proc = None
        if log_dir:
            self.proc = host.popen(sampler_argv(out / "writer-g9g10", log_dir, seconds), out / "writer-sampler.log")

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            self.proc.kill()


# ---- steps ----------------------------------------------------------------------------------------------------

def run_check(host: Host, packet: Path, cert: Path) -> dict[str, Any]:
    code, stdout, stderr = host.run(roll_check_argv(packet, cert), timeout=600)
    body = last_json(stdout)
    if code != 0 or body is None:
        raise RuntimeError(f"roll --check rc={code}: {stderr.strip()[-400:]}")
    return body


def roll_order_ok(check: Mapping[str, Any], broker: str) -> None:
    env = check.get("env") or {}
    if env.get(broker) == "false":
        raise Refused(f"{broker} already runs with auto.leader.rebalance.enable=false")
    leader = check.get("quorum_leader")
    pending = [b for b in BROKERS if env.get(b) != "false" and b != broker]
    if leader is not None and broker == f"kafka{leader}" and pending:
        raise Refused(f"{broker} is the quorum leader; roll {pending} first (quorum leader last)")
    if not check.get("ok"):
        raise Refused(f"cluster preconditions failed: drift/unhealthy/URP {json.dumps(check)[:600]}")


def wait_auto_rebalance(host: Host, cert: Path, out: Path, settle_s: float, period_s: float = 30.0) -> dict:
    """While the active controller still has rebalance true: wait for its automatic preferred election."""
    start, samples = host.now(), []
    while True:
        code, stdout, _ = host.run(observe_leaders_argv(cert), timeout=300)
        body = last_json(stdout) or {}
        summary = body.get("summary") or {}
        samples.append({"t": utc(host.now()), "rc": code, **summary})
        if code == 0 and summary.get("coordinator") == 0 and summary.get("data") == 0:
            result = {"balanced": True, "after_s": (host.now() - start).total_seconds()}
            break
        if (host.now() - start).total_seconds() >= settle_s:
            result = {"balanced": False, "after_s": settle_s, "last": summary}
            break
        host.sleep(period_s)
    write(out / "auto-rebalance-wait.json", {**result, "samples": samples})
    return result


def do_roll(host: Host, args: argparse.Namespace) -> int:
    require_own_lock()
    check = run_check(host, args.packet, args.cert_dir)
    roll_order_ok(check, args.broker)
    out = step_dir(args.receipt, f"roll-{args.broker}", host)
    write(out / "check-before.json", check)
    before = restarts(host)
    sampler = Sampler(host, out, 3600)
    start = host.now()
    try:
        code, stdout, stderr = host.run(roll_argv(args.packet, args.cert_dir, args.broker), timeout=3600)
        write(out / "roll.json", stdout or "{}")
        write(out / "roll.stderr.txt", stderr[-4000:])
        report = last_json(stdout) or {}
        others_pending = [b for b in BROKERS if b != args.broker and (check.get("env") or {}).get(b) != "false"]
        settle = None
        if code == 0 and others_pending:
            settle = wait_auto_rebalance(host, args.cert_dir, out, args.settle_s)
        host.sleep(args.tail_s)
        end = host.now()
    finally:
        sampler.stop()
    evidence = collect(host, out, start, end, args.baseline_s, before)
    summary = {"broker": args.broker, "roll_rc": code, "ok": report.get("ok"),
               "auto_rollback": report.get("auto_rollback"), "healthy_after_s": report.get("healthy_after_s"),
               "isr_restored_after_s": report.get("isr_restored_after_s"), "broker_config": report.get("broker_config"),
               "quorum_leader_before": check.get("quorum_leader"), "auto_rebalance_wait": settle,
               "writer": writer_summary(out), "evidence": evidence}
    if code == 1:
        summary["next"] = ("STOP: the tool restored this broker (RESTORED_VERIFIED). Brokers already rolled keep "
                           "false (the setting acts only on the active controller, so a mixed cluster is a valid "
                           "state); to return all of them run, one at a time: "
                           + " ; ".join(" ".join(rollback_argv(args.packet, args.cert_dir, b))
                                        for b in BROKERS if (check.get("env") or {}).get(b) == "false"))
    elif code == 3:
        summary["next"] = "HARD STOP: rollback after failure not verified; do not roll another broker; escalate"
    write(out / "summary.json", summary)
    print(json.dumps({k: v for k, v in summary.items() if k != "evidence"} | {
        "core": evidence["core"], "watch_partition_loads": evidence["watch_partition_loads"],
        "max_load_ms": evidence["max_load_ms"], "restarted": evidence["restarted_during_window"],
        "receipt": str(out)}, indent=1))
    return code


def measured_watch_loads(receipt: Path) -> list[int]:
    loads = []
    for path in receipt.glob("*/coordinator-loads.json"):
        loads += [r["ms"] for r in json.loads(path.read_text(encoding="utf-8")) if r.get("tp") == WATCH_PARTITION]
    return loads


def do_elect(host: Host, args: argparse.Namespace) -> int:
    require_own_lock()
    if args.scope != "data" and not args.force_coordinators:
        loads = measured_watch_loads(args.receipt)
        if not loads or max(loads) > args.coordinator_load_budget_ms:
            raise Refused(f"scope {args.scope} moves coordinators; measured {WATCH_PARTITION} loads {loads} ms "
                          f"(budget {args.coordinator_load_budget_ms} ms, none measured = refused): use --scope data")
    out = step_dir(args.receipt, f"elect-{args.scope}", host)
    before = restarts(host)
    sampler = Sampler(host, out, int(args.tail_s + 900))
    start = host.now()
    try:
        code, stdout, stderr = host.run(elect_argv(args.cert_dir, args.scope, args.receipt / "election-windows.jsonl"),
                                        timeout=900)
        write(out / "election.json", stdout or "{}")
        write(out / "election.stderr.txt", stderr[-4000:])
        host.sleep(args.tail_s)
        end = host.now()
    finally:
        sampler.stop()
    evidence = collect(host, out, start, end, args.baseline_s, before)
    report = last_json(stdout) or {}
    summary = {"scope": args.scope, "rc": code, "mode": report.get("mode"), "before": report.get("before"),
               "after": report.get("after"), "coordinator_moves": len(report.get("coordinator_moves") or []),
               "writer": writer_summary(out), "evidence": evidence}
    write(out / "summary.json", summary)
    print(json.dumps({k: v for k, v in summary.items() if k != "evidence"} | {
        "core": evidence["core"], "max_load_ms": evidence["max_load_ms"], "receipt": str(out)}, indent=1))
    return code


def do_check(host: Host, args: argparse.Namespace) -> int:
    out = step_dir(args.receipt, "check", host)
    check = run_check(host, args.packet, args.cert_dir)
    write(out / "roll-check.json", check)
    code, stdout, stderr = host.run(compaction_observe_argv(args.cert_dir), timeout=600)
    write(out / "internal-topics-observe.json", stdout or json.dumps({"rc": code, "stderr": stderr[-400:]}))
    observe = last_json(stdout) or {}
    print(json.dumps({"ok": check.get("ok"), "quorum_leader": check.get("quorum_leader"),
                      "recommended_order": check.get("recommended_order"), "env": check.get("env"),
                      "temp_removal": observe.get("temp_removal"),
                      "c_state": {t: v.get("c_state") for t, v in (observe.get("topics") or {}).items()},
                      "receipt": str(out)}, indent=1))
    return 0 if check.get("ok") else 3


def do_evidence(host: Host, args: argparse.Namespace) -> int:
    out = step_dir(args.receipt, "evidence", host)
    print(json.dumps(collect(host, out, parse_utc(args.since), parse_utc(args.until), args.baseline_s), indent=1))
    return 0


def do_final_check(host: Host, args: argparse.Namespace) -> int:
    out = step_dir(args.receipt, "final-check", host)
    check = run_check(host, args.packet, args.cert_dir)
    write(out / "roll-check.json", check)
    code, _, stderr = host.run(parity_argv(out / "production-parity.json", args.cert_dir), timeout=600)
    rebalance = effective = None
    if code == 0:
        doc = json.loads((out / "production-parity.json").read_text())
        rebalance = doc["kafka"]["auto.leader.rebalance.enable"]
        effective = (doc.get("effective") or {}).get("sha256")
    # The snapshot itself refuses (rc != 0) unless the DESCRIBED production config equals the committed plans: C keys,
    # frozen other overrides, temporary cleaner key absent, md.latest A', rebalance env on every broker (Codex K3).
    ok = check.get("ok") and set((check.get("env") or {}).values()) == {"false"} and rebalance is False \
        and effective is not None
    print(json.dumps({"ok": bool(ok), "env": check.get("env"), "parity_rebalance": rebalance,
                      "parity_effective_sha256": effective, "parity_rc": code,
                      "parity_error": stderr.strip()[-300:] if code else None, "receipt": str(out)}, indent=1))
    return 0 if ok else 3


def plan_text(packet: Path, cert: Path, receipt: Path) -> str:
    me = " ".join([*PY, str(ROOT / "scripts/kafka_b_roll_runbook.py")])
    lock = " ".join([*PY, str(ROOT / "scripts/host_measurement_lock.py")])
    compaction = " ".join(tool("kafka_internal_topic_compaction.py"))
    return "\n".join([
        f"# Decision B runbook. Receipt dir {receipt}. Each line is one independent step; stop on any non-zero exit.",
        f"{lock} acquire --owner {OWNER} --purpose 'P18.4 partition-43 cleaner pass + decision B roll' --minutes 170",
        f"{me} check                       # read-only: order, quorum leader, C/temp state, partition-43 size",
        "# 1. temporary dirty ratio (owner 2026-10-16): one cleaner pass removes the markers already past deleteHorizon",
        f"{compaction} --apply-temp --apply --cert-dir {cert} --lock-owner {OWNER}",
        "# 2. wait for the pass: the read-only pass watcher (other agent) + this observe until temp_removal."
        "remove_recommended (status REMOVE_RECOMMENDED: every ASSIGNED replica of 43 observed error-free and <= 32 MiB; "
        "UNKNOWN_HOLD = incomplete evidence, wait) AND pass-watch --compare exit 0 (observation_valid, offsets_preserved, "
        "cleaner_completed_all_replicas); expected ~10-20 min at the 4 MiB/s cleaner throttle",
        f"{compaction} --observe --cert-dir {cert}",
        "# 3. remove the temporary key (back to the C plan only)",
        f"{compaction} --remove-temp --apply --cert-dir {cert} --lock-owner {OWNER}",
        "# 4. B roll, quorum leader last",
        f"{me} roll --broker <first of recommended_order>",
        f"{me} roll --broker <second>      # refuses if it is the quorum leader while another broker is still true",
        f"{me} roll --broker <quorum leader, re-read by the tool>",
        f"{me} elect --scope data          # coordinators stay where the rolls left them (no extra load stall)",
        f"# optional: {me} elect --scope coordinators   # refused unless every measured {WATCH_PARTITION} load "
        "<= budget",
        f"{me} final-check                 # env false x3 + parity snapshot rebalance=false",
        f"{lock} release --owner {OWNER}",
        "# 5. G12 final in its own lock: upgrade/evidence/releases/v2.2.4/g12-g13-final-rerun.md section 1, PHASE=final",
        "# per-broker undo (one at a time, lock held): "
        + " ".join(rollback_argv(packet, cert, "kafkaN")),
    ])


def main(argv: Sequence[str] | None = None, host: Host | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--receipt", type=Path, default=RECEIPT)
    parser.add_argument("--packet", type=Path, default=PACKET)
    parser.add_argument("--cert-dir", type=Path, default=CERT)
    parser.add_argument("--baseline-s", type=float, default=300.0)
    parser.add_argument("--tail-s", type=float, default=60.0)
    sub = parser.add_subparsers(dest="step", required=True)
    sub.add_parser("plan")
    sub.add_parser("check")
    roll = sub.add_parser("roll")
    roll.add_argument("--broker", choices=BROKERS, required=True)
    roll.add_argument("--settle-s", type=float, default=420.0,
                      help="max wait for the not-yet-rolled controller's automatic preferred election")
    elect = sub.add_parser("elect")
    elect.add_argument("--scope", choices=("data", "coordinators", "all"), required=True)
    elect.add_argument("--coordinator-load-budget-ms", type=float, default=1000.0)
    elect.add_argument("--force-coordinators", action="store_true")
    evidence = sub.add_parser("evidence")
    evidence.add_argument("--since", required=True)
    evidence.add_argument("--until", required=True)
    sub.add_parser("final-check")
    args = parser.parse_args(argv)
    host = host or Host()
    os.umask(0o077)
    try:
        if args.step == "plan":
            print(plan_text(args.packet, args.cert_dir, args.receipt))
            return 0
        return {"check": do_check, "roll": do_roll, "elect": do_elect, "evidence": do_evidence,
                "final-check": do_final_check}[args.step](host, args)
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"error": str(error)[:600]}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""W12-A of the v2.2.5 window: TS writer composes for the TLS identity switch, readiness check, passive soak.

Purpose
    The v2.2.5 window (``upgrade/evidence/releases/v2.2.5/MAINTENANCE_WINDOW_PACKET.md`` W12) keeps the
    production TS writer image (2deb90b0) and changes exactly one thing for good: the host source of its mesh TLS
    identity mount ``/run/secrets/qdl-v2`` moves to the new generation. Because the window outage exceeds the
    writer's 120 s execution-surface TTL, the writer first starts once with ``MARKET_DATA_NATIVE_REQUIRE_RESUME=0``
    (BOOTSTRAP) and is then recreated with the deployed ``=1`` (STRICT). Hand-editing a 100-key compose in the
    middle of a full-mesh outage is the risk this tool removes.

    render  (before W1, writer still running) builds the exact effective config of the live writer from
            ``docker inspect`` (``kn_production_packet.service_from_inspect``, the practice of every earlier writer
            roll) and writes four compose files, 0600:
              bootstrap.compose.json           new identity, REQUIRE_RESUME=0
              strict.compose.json              new identity, REQUIRE_RESUME=1 (deployed value)
              rollback-bootstrap.compose.json  old identity, REQUIRE_RESUME=0
              rollback.compose.json            old identity, REQUIRE_RESUME=1 (= the live writer)
            and ``render.json`` with their sha256 and the computed field deltas. It refuses unless every target
            differs from the live config by exactly the intended keys, the live writer is STRICT with manifest
            revision 11, and the new identity directory holds ca.crt / client.crt / client.key.
    ready   polls the writer heartbeat (Postgres ``service_heartbeats`` row ``market_data``, read-only) until
            ``samples`` distinct heartbeats of THIS container (``last_seen_at`` after its ``StartedAt``) show
            READY, V2_PRIMARY, demanded == ready == ``--expect-demanded``, unhealthy 0, V1 fallback 0, and the
            container has RestartCount 0. READY slices and execution-ready slices are reported as two numbers.
    soak    (R4, passive, no load added) samples every ``--interval-s`` for ``--minutes``: the writer heartbeat
            (status, demanded/ready/execution-ready/unhealthy, the native_probe request-latency histograms), the
            ``docker stats`` memory of query_kn_1/2 and the restart count of every KN role and the writer; at the
            end it counts, from the writer log files (byte offsets taken at the start) and ``docker logs --since``,
            auth/protocol failures and, separately, data-policy refusals per code and feed. Output: one JSONL
            sample per interval plus a summary (exit 0 always; the verdict is the reader's, against the packet).

Boundary
    Read-only: ``docker inspect``, ``sudo -n test -f`` on the identity files (the hardened generation keeps
    ``identities/`` root 0750), and one ``psql`` SELECT through ``docker exec live_data_executor``. It never
    starts, stops or recreates a container; the window operator runs the printed ``docker compose`` commands.

Usage
    kn_window_writer.py render --new-identity-dir DIR --out-dir DIR [--container NAME]
    kn_window_writer.py ready --expect-demanded 64 [--samples 3] [--deadline-s 900] [--container NAME]
    kn_window_writer.py soak --out FILE.jsonl [--minutes 60] [--interval-s 60] [--expect-demanded 64]
"""
from __future__ import annotations

import argparse
import calendar
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kn_production_packet import compose_literal, service_from_inspect  # noqa: E402

WRITER = "market_data_service_p184w"
PROJECT = "trading-system-market-data"  # compose project of the deployed writer (container label)
IDENTITY_TARGET = "/run/secrets/qdl-v2"
RESUME_KEY = "MARKET_DATA_NATIVE_REQUIRE_RESUME"
MANIFEST_KEY = "DATA_LAYER_V2_JWT_MANIFEST_REVISION"
EXPECTED_MANIFEST = "11"
IDENTITY_FILES = ("ca.crt", "client.crt", "client.key")
HEARTBEAT_SQL = ("select status, details->>'consumer_mode', details->>'demanded_v2_slices', "
                 "details->>'ready_v2_slices', details->>'execution_ready_v2_slices', "
                 "details->>'unhealthy_v2_slices', details->>'v1_fallback_count', extract(epoch from last_seen_at) "
                 "from service_heartbeats where service_name='market_data'")


class Refused(Exception):
    pass


def docker_inspect(name: str) -> dict:
    return json.loads(subprocess.run(["docker", "inspect", name], check=True, capture_output=True, text=True,
                                     timeout=30).stdout)[0]


def identity_mount(service: dict) -> dict:
    mounts = [m for m in service["volumes"] if m["target"] == IDENTITY_TARGET]
    if len(mounts) != 1 or mounts[0]["type"] != "bind" or not mounts[0]["read_only"]:
        raise Refused(f"expected exactly one read-only bind at {IDENTITY_TARGET}, got {mounts}")
    return mounts[0]


def variant(live: dict, identity_dir: str, resume: str) -> dict:
    service = copy.deepcopy(live)
    identity_mount(service)["source"] = identity_dir
    service["environment"][RESUME_KEY] = resume
    return service


def deltas(before, after, path: str = "") -> list[dict]:
    """Field-level differences of two compose service dicts (volumes compared by target)."""
    if isinstance(before, dict) and isinstance(after, dict):
        out = []
        for key in sorted(set(before) | set(after)):
            out += deltas(before.get(key), after.get(key), f"{path}.{key}" if path else key)
        return out
    if path == "volumes" and isinstance(before, list) and isinstance(after, list):
        b = {m["target"]: m for m in before}
        a = {m["target"]: m for m in after}
        if len(b) != len(before) or len(a) != len(after):
            raise Refused("duplicate volume target")
        return deltas(b, a, "volumes")
    return [] if before == after else [{"field": path, "before": before, "after": after}]


def plan(live: dict, new_identity: str) -> dict[str, dict]:
    env = live["environment"]
    if env.get(RESUME_KEY) != "1":
        raise Refused(f"live writer is not STRICT ({RESUME_KEY}={env.get(RESUME_KEY)!r})")
    if env.get(MANIFEST_KEY) != EXPECTED_MANIFEST:
        raise Refused(f"{MANIFEST_KEY}={env.get(MANIFEST_KEY)!r}, expected {EXPECTED_MANIFEST}")
    old_identity = identity_mount(live)["source"]
    if os.path.realpath(old_identity) == os.path.realpath(new_identity):
        raise Refused("the live writer already mounts the new identity")
    targets = {
        "bootstrap": variant(live, new_identity, "0"),
        "strict": variant(live, new_identity, "1"),
        "rollback-bootstrap": variant(live, old_identity, "0"),
        "rollback": variant(live, old_identity, "1"),
    }
    expected = {
        "bootstrap": {f"volumes.{IDENTITY_TARGET}.source", f"environment.{RESUME_KEY}"},
        "strict": {f"volumes.{IDENTITY_TARGET}.source"},
        "rollback-bootstrap": {f"environment.{RESUME_KEY}"},
        "rollback": set(),
    }
    for name, service in targets.items():
        got = {d["field"] for d in deltas(live, service)}
        if got != expected[name]:
            raise Refused(f"{name}: deltas {sorted(got)} != intended {sorted(expected[name])}")
    return targets


def compose_doc(service: dict) -> dict:
    networks = sorted(service["networks"])
    return {"name": PROJECT, "services": {WRITER: compose_literal(service)},
            "networks": {n: {"external": True, "name": n} for n in networks}}


def write_private(path: Path, data: bytes) -> str:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return hashlib.sha256(data).hexdigest()


def identity_files_present(directory: str) -> list[str]:
    missing = []
    for name in IDENTITY_FILES:
        if subprocess.run(["sudo", "-n", "test", "-f", os.path.join(directory, name)], timeout=15).returncode:
            missing.append(name)
    return missing


def render(args) -> int:
    item = docker_inspect(args.container)
    if item["Name"].lstrip("/") != args.container:
        raise Refused(f"inspect returned {item['Name']}")
    live = service_from_inspect(item)
    live["environment"] = dict(live["environment"])
    new_identity = os.path.abspath(os.path.expanduser(args.new_identity_dir))
    missing = identity_files_present(new_identity)
    if missing:
        raise Refused(f"new identity dir {new_identity} lacks {missing}")
    targets = plan(live, new_identity)
    out = Path(args.out_dir).expanduser()
    out.mkdir(parents=True, exist_ok=True, mode=0o700)
    report = {"schema": "qdl.v225.writer-compose-render.v1", "container": args.container,
              "container_id": item["Id"], "image": item["Image"], "started_at": item["State"]["StartedAt"],
              "running": item["State"]["Running"], "old_identity": identity_mount(live)["source"],
              "new_identity": new_identity, "label_compose": item["Config"]["Labels"].get(
                  "com.docker.compose.project.config_files"), "files": {}}
    label = report["label_compose"]
    try:  # informational: the compose the writer was created from vs its effective config now
        label_service = json.loads(Path(label).read_text())["services"][args.container]
        report["label_compose_deltas"] = [d["field"] for d in deltas(label_service, compose_literal(live))]
    except (OSError, KeyError, TypeError, ValueError) as error:
        report["label_compose_deltas"] = f"not compared: {type(error).__name__}"
    for name, service in targets.items():
        data = (json.dumps(compose_doc(service), indent=2, sort_keys=True) + "\n").encode()
        report["files"][name] = {"path": str(out / f"{name}.compose.json"),
                                 "sha256": write_private(out / f"{name}.compose.json", data),
                                 "deltas": [d["field"] for d in deltas(live, service)]}
    report["commands"] = {
        name: f"docker compose -p {PROJECT} -f {out / f'{name}.compose.json'} up -d --no-deps --force-recreate {WRITER}"
        for name in targets}
    write_private(out / "render.json", (json.dumps(report, indent=2, sort_keys=True) + "\n").encode())
    print(json.dumps({k: report[k] for k in ("container_id", "image", "old_identity", "new_identity",
                                         "label_compose_deltas", "commands")},
                     indent=2))
    return 0


def heartbeat() -> dict | None:
    sql = HEARTBEAT_SQL.replace("'", "'\"'\"'")
    proc = subprocess.run(["docker", "exec", "live_data_executor", "sh", "-c",
                           f"psql -U \"$POSTGRES_USER\" -d live_data_executor -At -c '{sql}'"],
                          capture_output=True, text=True, timeout=30)
    line = proc.stdout.strip().splitlines()[-1] if proc.returncode == 0 and proc.stdout.strip() else ""
    parts = line.split("|")
    if len(parts) != 8:
        return None
    keys = ("status", "consumer_mode", "demanded", "ready", "execution_ready", "unhealthy", "v1_fallback")
    row = dict(zip(keys, parts[:7]))
    row["last_seen_unix"] = float(parts[7])
    return row


def started_unix(item: dict) -> float:
    started = item["State"]["StartedAt"]  # RFC 3339 with nanoseconds
    base, _, frac = started.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S")) + float("0." + (frac or "0"))


def evaluate(row: dict | None, expect: int, since_unix: float) -> list[str]:
    if row is None:
        return ["no heartbeat row"]
    problems = []
    if row["last_seen_unix"] <= since_unix:
        problems.append("heartbeat older than the container start")
    if row["status"] != "READY":
        problems.append(f"status {row['status']}")
    if row["consumer_mode"] != "V2_PRIMARY":
        problems.append(f"mode {row['consumer_mode']}")
    for key in ("demanded", "ready"):
        if row[key] != str(expect):
            problems.append(f"{key} {row[key]} != {expect}")
    for key in ("unhealthy", "v1_fallback"):
        if row[key] not in ("0", ""):
            problems.append(f"{key} {row[key]}")
    return problems


def ready(args) -> int:
    deadline = time.monotonic() + args.deadline_s
    seen, last = [], None
    while time.monotonic() < deadline:
        item = docker_inspect(args.container)
        since = started_unix(item)
        if item["RestartCount"] or not item["State"]["Running"]:
            print(json.dumps({"ready": False, "reason": "container not running or restarted",
                              "restart_count": item["RestartCount"], "status": item["State"]["Status"]}))
            return 1
        row = heartbeat()
        problems = evaluate(row, args.expect_demanded, since)
        last = {"row": row, "problems": problems}
        if not problems and row["last_seen_unix"] not in seen:
            seen.append(row["last_seen_unix"])
            if len(seen) >= args.samples:
                print(json.dumps({"ready": True, "samples": len(seen), "container_started_unix": since,
                                  "heartbeat": row, "ready_slices": row["ready"],
                                  "execution_ready_slices": row["execution_ready"]}, sort_keys=True))
                return 0
        elif problems:
            seen = []  # distinct READY samples must be consecutive
        time.sleep(args.poll_s)
    print(json.dumps({"ready": False, "reason": "deadline", "last": last}, sort_keys=True, default=str))
    return 1


# ---------------------------------------------------------------------------------------------- soak (R4)
KN_PROJECT = "qdl_v2_stable_candidate"
QUERY_ROLES = ("query_kn_1", "query_kn_2")
LOG_ROLES = ("query_kn_1", "query_kn_2", "stream_kn_1", "stream_kn_2")
AUTH_PROTOCOL = ("401", "UNAUTHENTICATED", "CERTIFICATE_VERIFY_FAILED", "SSLError", "STRICT_RESUME_REFUSAL",
                 "HANDOFF_RESUME_REQUIRED")
DATA_POLICY = ("DATA_STALE", "SESSION_STATE", "EVENT_AGE", "SOURCE_UNAVAILABLE", "OPEN_SEQUENCE_GAP")
# market-data-writer health.rs PROBE_LATENCY_BOUNDS_MS (inclusive upper bounds, one overflow bucket after)
PROBE_BOUNDS_MS = (10, 25, 50, 100, 250, 500, 750, 1000, 1250, 1500, 2000)
DISCONNECT = re.compile(r"venue=(\S+) symbol=(\S+) feed=(\S+).* code=(\S+)")
# whole tokens only: not inside numbers, decimals or log timestamps ("05:43:03,401" is a millisecond field)
AUTH_RE = re.compile(r"(?<![0-9A-Za-z_,.:])(401|UNAUTHENTICATED|CERTIFICATE_VERIFY_FAILED|SSLError|STRICT_RESUME_REFUSAL|"
                     r"HANDOFF_RESUME_REQUIRED)(?![0-9A-Za-z_.])")
SOAK_SQL = ("select status, details->>'demanded_v2_slices', details->>'ready_v2_slices', "
            "details->>'execution_ready_v2_slices', details->>'unhealthy_v2_slices', details->>'consumer_mode', "
            "extract(epoch from last_seen_at), details->'native_probe' from service_heartbeats "
            "where service_name='market_data'")


def psql(sql: str) -> str:
    quoted = sql.replace("'", "'\"'\"'")
    proc = subprocess.run(["docker", "exec", "live_data_executor", "sh", "-c",
                           f"psql -U \"$POSTGRES_USER\" -d live_data_executor -At -c '{quoted}'"],
                          capture_output=True, text=True, timeout=30)
    return proc.stdout.strip() if proc.returncode == 0 else ""


def soak_heartbeat() -> dict:
    parts = psql(SOAK_SQL).split("|", 7)
    if len(parts) != 8:
        return {"error": "no heartbeat"}
    keys = ("status", "demanded", "ready", "execution_ready", "unhealthy", "consumer_mode")
    row = dict(zip(keys, parts[:6]))
    row["last_seen_unix"] = float(parts[6] or 0)
    try:
        row["native_probe"] = json.loads(parts[7]) if parts[7] else {}
    except ValueError:
        row["native_probe"] = {}
    return row


def probe_totals(native_probe: dict) -> dict:
    """Per feed, summed over writer workers: request count, latency sum and bucket counts (cumulative)."""
    totals: dict[str, dict] = {}
    for worker in (native_probe or {}).values():
        for feed, item in ((worker or {}).get("feeds") or {}).items():
            hist = item.get("probe_latency_ms") or {}
            slot = totals.setdefault(feed, {"count": 0, "sum": 0.0, "buckets": [0] * (len(PROBE_BOUNDS_MS) + 1),
                                            "max": 0.0, "failed": 0, "unavailable": 0})
            slot["count"] += int(hist.get("count") or 0)
            slot["sum"] += float(hist.get("sum") or 0.0)
            slot["max"] = max(slot["max"], float(hist.get("max") or 0.0))
            for i, value in enumerate((hist.get("buckets") or [])[:len(slot["buckets"])]):
                slot["buckets"][i] += int(value)
            slot["failed"] += int(item.get("probes_failed") or 0)
            slot["unavailable"] += int(item.get("probes_unavailable") or 0)
    return totals


def bucket_quantile(buckets: list[int], q: float):
    """Upper bound (ms) of the bucket holding quantile q; '>2000' for the overflow bucket; None when empty."""
    total = sum(buckets)
    if total <= 0:
        return None
    rank, seen = q * total, 0
    for i, count in enumerate(buckets):
        seen += count
        if seen >= rank:
            return PROBE_BOUNDS_MS[i] if i < len(PROBE_BOUNDS_MS) else f">{PROBE_BOUNDS_MS[-1]}"
    return f">{PROBE_BOUNDS_MS[-1]}"


def probe_delta(first: dict, last: dict) -> dict:
    """Request latency over the soak (writer native_probe, basis QUERY_REQUEST_START_TO_REDIS_APPLY_DECISION):
    differences of the cumulative counters; a counter that went backwards (process restart) is reported, not
    differenced."""
    out = {}
    for feed in sorted(set(first) | set(last)):
        a, b = first.get(feed), last.get(feed)
        if not a or not b or b["count"] < a["count"]:
            out[feed] = {"error": "counter reset or missing"}
            continue
        buckets = [y - x for x, y in zip(a["buckets"], b["buckets"])]
        count = b["count"] - a["count"]
        out[feed] = {"requests": count, "mean_ms": round((b["sum"] - a["sum"]) / count, 2) if count else None,
                     "p50_le_ms": bucket_quantile(buckets, 0.50), "p95_le_ms": bucket_quantile(buckets, 0.95),
                     "p99_le_ms": bucket_quantile(buckets, 0.99), "max_ms_since_start": b["max"],
                     "failed": b["failed"] - a["failed"], "unavailable": b["unavailable"] - a["unavailable"]}
    return out


def memory_mib(text: str):
    """docker stats MemUsage '557.1MiB / 2GiB' -> 557.1."""
    value = text.split("/")[0].strip()
    for unit, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1 / 1024), ("B", 1 / 2**20)):
        if value.endswith(unit):
            return round(float(value[: -len(unit)]) * factor, 1)
    return None


def query_memory() -> dict:
    names = [f"{KN_PROJECT}-{role}-1" for role in QUERY_ROLES]
    proc = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}", *names],
                          capture_output=True, text=True, timeout=60)
    out = {}
    for line in proc.stdout.strip().splitlines():
        name, _, usage = line.partition("|")
        out[name.replace(f"{KN_PROJECT}-", "").removesuffix("-1")] = memory_mib(usage)
    return out


def restart_counts() -> dict:
    ids = subprocess.run(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={KN_PROJECT}"],
                         capture_output=True, text=True, timeout=30).stdout.split()
    proc = subprocess.run(["docker", "inspect", "-f", "{{.Name}}|{{.RestartCount}}|{{.State.Status}}|{{.State.StartedAt}}",
                           *ids, WRITER], capture_output=True, text=True, timeout=60)
    out = {}
    for line in proc.stdout.strip().splitlines():
        name, restarts, status, started = line.split("|")
        out[name.lstrip("/")] = {"restarts": int(restarts), "status": status, "started_at": started}
    return out


def writer_log_files(item: dict) -> list[Path]:
    logs = next((m["Source"] for m in item["Mounts"] if m["Destination"] == "/app/logs"), None)
    return sorted(Path(logs).glob("market_data*.log")) if logs else []


def count_log_lines(lines) -> dict:
    auth, policy = {}, {}
    for line in lines:
        hit = AUTH_RE.search(line)
        if hit:
            auth[hit.group(1)] = auth.get(hit.group(1), 0) + 1
        match = DISCONNECT.search(line)
        if match and match.group(4) in DATA_POLICY:
            key = f"{match.group(4)} {match.group(1)} {match.group(2)} {match.group(3)}"
            policy[key] = policy.get(key, 0) + 1
    return {"auth_protocol": auth, "data_policy_by_code_venue_symbol_feed": policy}


def read_from(path: Path, offset: int):
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        handle.seek(offset if offset <= size else 0)  # a rotated (shorter) file is read from its start
        return handle.read().decode("utf-8", "replace").splitlines()


def soak(args) -> int:
    out = Path(args.out).expanduser()
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    started_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    item = docker_inspect(WRITER)
    offsets = {str(p): p.stat().st_size for p in writer_log_files(item)}
    first = soak_heartbeat()
    restarts_start = restart_counts()
    samples, deadline = [], time.monotonic() + args.minutes * 60
    with os.fdopen(fd, "w") as handle:
        while True:
            beat = soak_heartbeat()
            sample = {"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "unix": round(time.time(), 1),
                      "writer": {k: beat.get(k) for k in ("status", "demanded", "ready", "execution_ready",
                                                          "unhealthy", "consumer_mode", "last_seen_unix")},
                      "query_mem_mib": query_memory()}
            samples.append(sample)
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
            handle.flush()
            if time.monotonic() >= deadline:
                break
            time.sleep(min(args.interval_s, max(0.0, deadline - time.monotonic())))
        last = soak_heartbeat()
        restarts_end = restart_counts()
        lines = []
        for path, offset in offsets.items():
            lines += read_from(Path(path), offset)
        writer_counts = count_log_lines(lines)
        role_auth = {}
        for role in LOG_ROLES:
            proc = subprocess.run(["docker", "logs", "--since", started_iso, f"{KN_PROJECT}-{role}-1"],
                                  capture_output=True, text=True, timeout=120)
            role_auth[role] = count_log_lines((proc.stdout + proc.stderr).splitlines())["auth_protocol"]
        writer_stdout = subprocess.run(["docker", "logs", "--since", started_iso, WRITER], capture_output=True,
                                       text=True, timeout=120)
        expect = str(args.expect_demanded)
        ready_flags = [s["writer"]["status"] == "READY" and s["writer"]["demanded"] == expect
                       and s["writer"]["ready"] == expect and s["writer"]["unhealthy"] == "0" for s in samples]
        mem = {role: [s["query_mem_mib"].get(role) for s in samples if s["query_mem_mib"].get(role) is not None]
               for role in QUERY_ROLES}
        per_10min = max(1, int(600 // max(args.interval_s, 1)))
        summary = {
            "summary": True, "started": started_iso, "samples": len(samples),
            "writer_ready_samples": f"{sum(ready_flags)}/{len(ready_flags)}",
            "execution_ready_min_max": [min((int(s["writer"]["execution_ready"] or 0) for s in samples), default=None),
                                        max((int(s["writer"]["execution_ready"] or 0) for s in samples), default=None)],
            "request_latency_native_probe": probe_delta(probe_totals(first.get("native_probe")),
                                                        probe_totals(last.get("native_probe"))),
            "served_event_age": "NOT_MEASURED (no passive counter in the writer heartbeat)",
            "query_mem_mib": {role: {"first": v[0] if v else None, "last": v[-1] if v else None,
                                     "max": max(v, default=None),
                                     "growth_last_10min": round(v[-1] - v[-1 - per_10min], 1)
                                     if len(v) > per_10min else None} for role, v in mem.items()},
            "restarts_during": {name: restarts_end.get(name, {}).get("restarts", 0) - info["restarts"]
                                for name, info in restarts_start.items()
                                if restarts_end.get(name, {}).get("restarts", 0) != info["restarts"]
                                or restarts_end.get(name, {}).get("started_at") != info["started_at"]},
            "writer_logs": writer_counts,
            "writer_stdout_auth_protocol": count_log_lines(
                (writer_stdout.stdout + writer_stdout.stderr).splitlines())["auth_protocol"],
            "dl_roles_auth_protocol": role_auth}
        handle.write(json.dumps(summary, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    r = sub.add_parser("render")
    r.add_argument("--new-identity-dir", required=True)
    r.add_argument("--out-dir", required=True)
    r.add_argument("--container", default=WRITER)
    q = sub.add_parser("ready")
    q.add_argument("--expect-demanded", type=int, required=True)
    q.add_argument("--samples", type=int, default=3)
    q.add_argument("--deadline-s", type=float, default=900.0)
    q.add_argument("--poll-s", type=float, default=5.0)
    q.add_argument("--container", default=WRITER)
    k = sub.add_parser("soak")
    k.add_argument("--out", required=True)
    k.add_argument("--minutes", type=float, default=60.0)
    k.add_argument("--interval-s", type=float, default=60.0)
    k.add_argument("--expect-demanded", type=int, default=64)
    args = parser.parse_args(argv)
    try:
        return {"render": render, "ready": ready, "soak": soak}[args.mode](args)
    except Refused as error:
        print(json.dumps({"refused": str(error)}))
        return 3


if __name__ == "__main__":
    sys.exit(main())

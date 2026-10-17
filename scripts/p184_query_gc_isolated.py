#!/usr/bin/env python3
"""IDR-114: isolated two-replica Query under the stage-100 load, for GC pause measurement.

Purpose: measure the per-replica Query stall of RCA 507b446 (0.3-1.1 s every ~76 s) where the product runs - two
Query replicas of the production image, the source under test mounted read-only over ``/app/qdl``, served over HTTPS
mTLS to the unchanged stage-100 driver (``scripts/phase3_consumer_load_acceptance.py --mode target --sessions 100``).
Each replica writes the ``qdl.gc.pause`` telemetry line (qdl/runtime/gc_telemetry.py); ``analyse`` lines the pauses
up with the driver's per-replica read outliers (>= 250 ms).

Topology (prefix ``qdl-gcq-``, label ``qdl.p184.gcq=true``, every container ``--rm``):
  qdl-gcq-net          new ``--internal`` network: driver, the two replicas (aliases query_v2_1 / query_v2_2, the
                       production Query certificate's SANs) and two TCP relays (aliases qdl-v2-stream-a/-b).
  qdl-gcq-query-1/-2   production Query container spec (env, read-only mounts, 1.5 CPU, 1536 MiB, uid 10001),
                       except: quota/lease Redis = qdl-gcq-quota (private), state dir = a scratch dir, instance id.
                       Also joins the stack's stable_internal (market_cache, rust_core) and stable_egress (venue REST
                       of pass-through reads) networks with no alias.
  qdl-gcq-quota        private Redis (stack Redis digest, tmpfs, no persistence).
  qdl-gcq-relay-a/-b   byte relays to the production Streams (TLS stays end to end), for the driver's subscriptions
                       and the replicas' hot reads.
Production reads only: market cache Redis, Stream gRPC (hot view, subscriptions), rust_core admission. No production
container, network alias, route, env, image or Redis key changes; the production Query is not touched.

Optional ``--saveall``: a ``sitecustomize`` probe in replica 2 turns on ``gc.DEBUG_SAVEALL`` for a bounded window,
then forces one full collection and writes a type/frame summary of what the collector would have freed
(``saveall.json``). That replica's latency is not scored inside the window.

Usage (host python, under the host measurement lock):
  p184_query_gc_isolated.py run --src DIR --output NEW_DIR [--saveall --saveall-at S --saveall-seconds S]
  p184_query_gc_isolated.py analyse --output DIR
  p184_query_gc_isolated.py cleanup
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "qdl-gcq-"
LABEL = "qdl.p184.gcq=true"
NETWORK = PREFIX + "net"
STACK = "qdl_v2_stable_candidate"
PROD_QUERY = f"{STACK}-query_kn_1-1"
INTERNAL_NETWORKS = (f"{STACK}_stable_internal", f"{STACK}_stable_egress")
STREAM_RELAYS = {"a": f"{STACK}-stream_kn_1-1", "b": f"{STACK}-stream_kn_2-1"}
REDIS_IMAGE = "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8"
QUERY_IMAGE = "sha256:c21e164ac4db14af498b73104e344f003fa7117fdae2e465f50df8668cdca252"
CAMPAIGN = Path.home() / ".local/state/qdl-v2/p184-final-campaign-20261017T135719Z"
BUDGET = ROOT / "config/v2/p184-stage100-budget.json"
LOCK = Path.home() / ".local/state/host-measurement-window.lock"
MOUNTS = ("/runtime", "/session-liveness", "/consumer-realms", "/kn", "/stable-certs")
STALL_MS = 250.0
CLUSTER_GAP_MS = 3000

RELAY = r"""
import asyncio, sys
TARGET, PORT = sys.argv[1], int(sys.argv[2])
async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()
async def handle(client_reader, client_writer):
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(TARGET, PORT)
    except Exception:
        client_writer.close()
        return
    await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))
async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    async with server:
        await server.serve_forever()
asyncio.run(main())
"""

SAVEALL_PROBE = r'''
"""IDR-114 DEBUG_SAVEALL probe (isolated replica only): summary of cyclic garbage, no object payloads."""
import collections, gc, json, os, sys, threading, time

def _rss():
    with open("/proc/self/statm") as handle:
        return int(handle.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576

def _name(obj):
    kind = type(obj)
    return f"{kind.__module__}.{kind.__qualname__}"

def _summary(objects):
    types, sizes, frames, excs, coros = collections.Counter(), collections.Counter(), collections.Counter(), collections.Counter(), collections.Counter()
    for obj in objects:
        name = _name(obj)
        types[name] += 1
        try:
            sizes[name] += sys.getsizeof(obj)
        except Exception:
            pass
        if name == "builtins.frame":
            code = obj.f_code
            frames[f"{code.co_filename.rsplit('/app/', 1)[-1]}:{code.co_qualname}:{obj.f_lineno}"] += 1
        elif isinstance(obj, BaseException):
            excs[name] += 1
        elif name == "builtins.coroutine":
            coros[getattr(obj, "__qualname__", "?")] += 1
    leaf = 0
    for obj in objects:  # untracked payload (bytes/str) one level below the garbage
        for ref in gc.get_referents(obj):
            if isinstance(ref, (bytes, bytearray, str)):
                leaf += sys.getsizeof(ref)
    return {"objects": len(objects), "shallow_bytes": sum(sizes.values()), "leaf_bytes_one_level": leaf,
            "types": types.most_common(40), "type_bytes": sizes.most_common(25),
            "frames": frames.most_common(60), "exceptions": excs.most_common(30), "coroutines": coros.most_common(20)}

def _run():
    at = float(os.environ["QDL_GCQ_SAVEALL_AT"])
    seconds = float(os.environ.get("QDL_GCQ_SAVEALL_SECONDS", "120"))
    limit = float(os.environ.get("QDL_GCQ_SAVEALL_RSS_LIMIT_MIB", "1250"))
    out = os.environ.get("QDL_GCQ_SAVEALL_OUT", "/var/lib/qdl-kn/saveall.json")
    time.sleep(max(0.0, at - time.time()))
    record = {"started_wall_ms": time.time_ns() // 1_000_000, "rss_start_mib": _rss(), "stats_start": gc.get_stats()}
    gc.set_debug(gc.DEBUG_SAVEALL)
    stopped = "window"
    while time.time() < at + seconds:
        time.sleep(0.5)
        if _rss() > limit:
            stopped = "rss_limit"
            break
    gc.set_debug(0)
    window = list(gc.garbage)
    gc.garbage.clear()
    record.update(window_end_wall_ms=time.time_ns() // 1_000_000, stopped=stopped, rss_window_end_mib=_rss(),
                  stats_window_end=gc.get_stats(), window=_summary(window))
    del window
    gc.set_debug(gc.DEBUG_SAVEALL)
    started = time.perf_counter()
    gc.collect(2)
    record["forced_full_ms"] = round((time.perf_counter() - started) * 1000, 1)
    gc.set_debug(0)
    full = list(gc.garbage)
    gc.garbage.clear()
    record.update(full=_summary(full), rss_after_full_mib=_rss(), done_wall_ms=time.time_ns() // 1_000_000)
    del full
    with open(out, "w") as handle:
        json.dump(record, handle, indent=1)

if os.environ.get("QDL_GCQ_SAVEALL_AT") and os.getpid() == 1:  # the Query entrypoint, not a docker exec
    threading.Thread(target=_run, name="gcq-saveall", daemon=True).start()
'''


def sh(*args, check=True, capture=True, timeout=120):
    result = subprocess.run(list(args), capture_output=capture, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"{args[:3]} failed: {result.stderr.strip()[:400]}")
    return result.stdout.strip() if capture else ""


def owned_containers():
    out = sh("docker", "ps", "-a", "--filter", f"label={LABEL}", "--format", "{{.Names}}", check=False)
    return [name for name in out.splitlines() if name.startswith(PREFIX)]


def cleanup():
    removed = []
    for name in owned_containers():
        sh("docker", "rm", "-f", name, check=False)
        removed.append(name)
    nets = sh("docker", "network", "ls", "--filter", f"label={LABEL}", "--format", "{{.Name}}", check=False)
    for net in nets.splitlines():
        if net == NETWORK:
            sh("docker", "network", "rm", net, check=False)
            removed.append(net)
    return removed


def lock_owner():
    try:
        return json.loads(LOCK.read_text()).get("owner")
    except (OSError, ValueError):
        return None


def prod_query_spec():
    raw = json.loads(sh("docker", "inspect", PROD_QUERY))[0]
    if raw["Image"] != QUERY_IMAGE:
        raise RuntimeError(f"production Query image is {raw['Image'][:19]}, expected {QUERY_IMAGE[:19]}")
    mounts = {m["Destination"]: m["Source"] for m in raw["Mounts"] if m["Destination"] in MOUNTS}
    if set(mounts) != set(MOUNTS):
        raise RuntimeError(f"production Query mounts differ: {sorted(mounts)}")
    env = dict(item.split("=", 1) for item in raw["Config"]["Env"])
    return env, mounts, raw["Config"]["Cmd"], raw["HostConfig"]


def write_env(path: Path, env: dict):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        for key, value in sorted(env.items()):
            if "\n" in value:
                raise RuntimeError(f"env {key} spans lines; --env-file cannot carry it")
            handle.write(f"{key}={value}\n")


def start_query(index: int, *, src: Path, output: Path, env: dict, mounts: dict, cmd: list, host: dict,
                probe: Path | None, saveall_env: dict):
    name = f"{PREFIX}query-{index}"
    state = output / f"state-{index}"
    state.mkdir(mode=0o777)
    os.chmod(state, 0o777)
    # As in the production state dir: runtime/session-liveness -> the read-only ingestor liveness mount. Without it
    # every session-checked read is refused DATA_STALE SESSION_STATE (first baseline attempt, 2026-10-17).
    for sub in ("runtime", "shared"):
        (state / sub).mkdir(mode=0o777)
        os.chmod(state / sub, 0o777)
    (state / "runtime/session-liveness").symlink_to("/session-liveness")
    env = dict(env)
    env.update({
        "QDL_STABLE_REDIS_URL": f"redis://{PREFIX}quota:6379/0",
        "QDL_STABLE_INSTANCE_ID": f"gcq-query-{index}",
        "QDL_STABLE_CONSUMER_GROUP": f"gcq-query-{index}",
        "QDL_STABLE_AUDIT_PATH": f"/var/lib/qdl-kn/runtime/gcq-query-{index}-audit.jsonl",
    })
    if probe is not None:
        env["PYTHONPATH"] = "/gcprobe:" + env.get("PYTHONPATH", "/app")
        env.update(saveall_env)
    env_file = output / f".env-{index}"
    write_env(env_file, env)
    try:
        args = [
            "docker", "create", "--rm", "--name", name, "--label", LABEL,
            "--network", NETWORK, "--network-alias", f"query_v2_{index}",
            "--user", "10001:10001", "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=67108864",
            "--cpus", "1.5", "--memory", str(host["Memory"]), "--pids-limit", "256",
            "--security-opt", "no-new-privileges:true", "--cap-drop", "ALL",
            "--log-opt", "max-size=50m", "--log-opt", "max-file=2",
            "--env-file", str(env_file),
            *[a for dst, srcp in sorted(mounts.items()) for a in ("--mount", f"type=bind,src={srcp},dst={dst},readonly")],
            "--mount", f"type=bind,src={state},dst=/var/lib/qdl-kn",
            "--mount", f"type=bind,src={src / 'qdl'},dst=/app/qdl,readonly",
        ]
        if probe is not None:
            args += ["--mount", f"type=bind,src={probe},dst=/gcprobe,readonly"]
        args += [QUERY_IMAGE, *cmd]
        sh(*args)
    finally:
        env_file.unlink(missing_ok=True)
    for network in INTERNAL_NETWORKS:
        sh("docker", "network", "connect", network, name)
    sh("docker", "start", name)
    return name


def wait_ready(names, timeout=240):
    probe = ("import ssl,urllib.request; c=ssl.create_default_context(cafile='/stable-certs/query/ca.crt'); "
             "c.load_cert_chain('/stable-certs/query/server.crt','/stable-certs/query/server.key'); "
             "urllib.request.urlopen('https://localhost:8200/health/ready',context=c,timeout=2)")
    deadline = time.monotonic() + timeout
    pending = set(names)
    while pending and time.monotonic() < deadline:
        for name in sorted(pending):
            if subprocess.run(["docker", "exec", name, "python", "-c", probe], capture_output=True,
                              timeout=20).returncode == 0:
                pending.discard(name)
        time.sleep(3)
    if pending:
        raise RuntimeError(f"not ready: {sorted(pending)}")


def make_profile(output: Path, names) -> Path:
    profile = json.loads((CAMPAIGN / "profile.json").read_text())
    profile.update({
        "network": NETWORK, "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
        "stream_targets": ["qdl-v2-stream-a:8210", "qdl-v2-stream-b:8210"],
        "query_containers": list(names), "scope": "shadow",
        "monitored_containers": [*names, f"{PREFIX}relay-a", f"{PREFIX}relay-b", f"{PREFIX}quota",
                                 "market_data_service_p184w"],
    })
    path = output / "profile.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(profile, handle, indent=1)
    return path


def run(args) -> int:
    if lock_owner() != args.lock_owner:
        raise SystemExit(f"host measurement lock is not held by {args.lock_owner}")
    if owned_containers():
        raise SystemExit("qdl-gcq containers already exist; run cleanup first")
    src = args.src.resolve()
    if not (src / "qdl/runtime/stall_telemetry.py").is_file():
        raise SystemExit("--src must hold the qdl package with stall_telemetry.py")
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    env, mounts, cmd, host = prod_query_spec()
    meta = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "src": str(src),
            "src_tree_sha256": sh("sh", "-c", f"cd {src} && find qdl -name '*.py' -type f | sort | xargs sha256sum | sha256sum"),
            "query_image": QUERY_IMAGE, "saveall": bool(args.saveall)}
    probe = None
    saveall_env = {}
    names = []
    try:
        sh("docker", "network", "create", "--internal", "--label", LABEL, NETWORK)
        sh("docker", "run", "-d", "--rm", "--name", f"{PREFIX}quota", "--label", LABEL, "--network", NETWORK,
           "--cpus", "0.2", "--memory", "128m", "--read-only", "--tmpfs", "/data:size=64m",
           REDIS_IMAGE, "redis-server", "--save", "", "--appendonly", "no", "--maxmemory", "96mb")
        for side, target in STREAM_RELAYS.items():
            relay = f"{PREFIX}relay-{side}"
            sh("docker", "create", "--rm", "--name", relay, "--label", LABEL, "--network", NETWORK,
               "--network-alias", f"qdl-v2-stream-{side}", "--user", "10001:10001", "--read-only",
               "--cpus", "0.5", "--memory", "128m", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
               QUERY_IMAGE, "python", "-c", RELAY, target, "8210")
            sh("docker", "network", "connect", "qdl_v2_native_consumer", relay)
            sh("docker", "start", relay)
        if args.saveall:
            probe = output / "probe"
            probe.mkdir()
            (probe / "sitecustomize.py").write_text(SAVEALL_PROBE)
            os.chmod(probe, 0o755)
            os.chmod(probe / "sitecustomize.py", 0o644)
        driver_start = time.time() + 60  # approximate: replicas ready + driver launch
        for index in (1, 2):
            if args.saveall and index == 2:
                saveall_env = {"QDL_GCQ_SAVEALL_AT": str(int(driver_start + args.saveall_at)),
                               "QDL_GCQ_SAVEALL_SECONDS": str(args.saveall_seconds)}
            names.append(start_query(index, src=src, output=output, env=env, mounts=mounts, cmd=cmd, host=host,
                                     probe=probe if index == 2 else None, saveall_env=saveall_env))
        wait_ready(names)
        meta["ready_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        meta["saveall_at_epoch"] = int(saveall_env.get("QDL_GCQ_SAVEALL_AT", 0)) or None
        profile = make_profile(output, names)
        meta["driver_start_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        driver = subprocess.run(
            [sys.executable, "-B", str(ROOT / "scripts/phase3_consumer_load_acceptance.py"), "--profile", str(profile),
             "--budget", str(BUDGET), "--output", str(output / "stage100"), "--mode", "target", "--sessions", "100",
             "--duration-seconds", str(args.duration)],
            capture_output=True, text=True, timeout=args.duration + 900)
        (output / "driver.out").write_text(driver.stdout[-20000:] + "\n--stderr--\n" + driver.stderr[-20000:])
        meta["driver_returncode"] = driver.returncode
        meta["driver_end_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    finally:
        for name in names:
            logs = subprocess.run(["docker", "logs", "-t", name], capture_output=True, text=True, timeout=120)
            (output / f"{name}.log").write_text(logs.stdout + logs.stderr)
        for index in (1, 2):
            saved = output / f"state-{index}" / "saveall.json"
            if saved.is_file():
                shutil.copy(saved, output / "saveall.json")
        meta["removed"] = cleanup()
        for index in (1, 2):
            shutil.rmtree(output / f"state-{index}", ignore_errors=True)
        if probe is not None:
            shutil.rmtree(probe, ignore_errors=True)
        (output / "profile.json").unlink(missing_ok=True)
        (output / "run.json").write_text(json.dumps(meta, indent=1) + "\n")
    return 0 if meta.get("driver_returncode") == 0 else 1


def stall_records(path: Path):
    """``qdl.stall`` records (kind gc/loop_lag/slow_request); the first telemetry cut wrote ``qdl.gc.pause`` (gc)."""
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        if "qdl.stall " in line:
            rows.append(json.loads(line.split("qdl.stall ", 1)[1]))
        elif "qdl.gc.pause " in line:
            rows.append({"kind": "gc", **json.loads(line.split("qdl.gc.pause ", 1)[1])})
    return rows


def pauses(path: Path):
    return [row for row in stall_records(path) if row["kind"] == "gc"]


def clusters(points):
    out = []
    for at, value in sorted(points):
        if out and at - out[-1]["end_ms"] <= CLUSTER_GAP_MS:
            c = out[-1]
            c["end_ms"] = at
            c["reads"] += 1
            c["peak_ms"] = max(c["peak_ms"], value)
        else:
            out.append({"start_ms": at, "end_ms": at, "reads": 1, "peak_ms": value})
    return out


def analyse(args) -> int:
    output = args.output.resolve()
    receipt = json.loads((output / "stage100/receipt.json").read_text())
    meta = json.loads((output / "run.json").read_text())
    exclude = None
    saveall = None
    if (output / "saveall.json").is_file():
        saveall = json.loads((output / "saveall.json").read_text())
        exclude = (saveall["started_wall_ms"] - 5000, saveall.get("done_wall_ms", saveall["window_end_wall_ms"]) + 5000)
    load_start, load_end = (int(v * 1000) for v in receipt["load_window_unix_s"])
    outliers = [item for item in _iter_outliers(receipt) if load_start <= item[0] <= load_end]
    result = {"run": meta, "replicas": {}, "load_window_ms": [load_start, load_end],
              "outliers_in_load_window": len(outliers), "saveall_excluded_ms": exclude}
    for index in (1, 2):
        replica = f"query_v2_{index}"
        gcs = pauses(output / f"{PREFIX}query-{index}.log")
        points = [(at, value) for at, kind, value, label in outliers
                  if kind == "READ" and value >= STALL_MS and label.rsplit("|", 1)[-1] == replica
                  and not (index == 2 and exclude and exclude[0] <= at <= exclude[1])]
        cl = clusters(points)
        gcs = [g for g in gcs if load_start <= g["wall_ms"] <= load_end]
        big = [g for g in gcs if g["duration_ms"] >= 100.0
               and not (index == 2 and exclude and exclude[0] <= g["wall_ms"] <= exclude[1])]
        aligned = 0
        for c in cl:
            if any(c["start_ms"] - 2500 <= g["wall_ms"] <= c["end_ms"] + 1500 for g in big):
                c["gc_aligned"] = True
                aligned += 1
        per_gen = {}
        for gen in (0, 1, 2):
            rows = [g for g in gcs if g["generation"] == gen]
            if not rows:
                continue
            walls = [g["wall_ms"] for g in rows]
            gaps = [(b - a) / 1000 for a, b in zip(walls, walls[1:])]
            durations = [g["duration_ms"] for g in rows]
            per_gen[str(gen)] = {
                "logged": len(rows), "p50_ms": round(statistics.median(durations), 1), "max_ms": max(durations),
                "median_period_s": round(statistics.median(gaps), 1) if gaps else None,
                "collected_max": max(g["collected"] for g in rows),
            }
        others = [row for row in stall_records(output / f"{PREFIX}query-{index}.log")
                  if row["kind"] != "gc" and load_start <= row["wall_ms"] <= load_end
                  and not (index == 2 and exclude and exclude[0] <= row["wall_ms"] <= exclude[1])]
        lags = [row["lag_ms"] for row in others if row["kind"] == "loop_lag"]
        slow = [row["duration_ms"] for row in others if row["kind"] == "slow_request"]
        result["replicas"][replica] = {
            "loop_lag_records": len(lags), "loop_lag_ge_250ms": sum(v >= 250 for v in lags),
            "loop_lag_max_ms": max(lags, default=None),
            "slow_requests": len(slow), "slow_request_max_ms": max(slow, default=None),
            "stall_clusters": len(cl), "stall_reads": len(points),
            "max_read_ms": max((v for _, v in points), default=None),
            "cluster_peaks_ms": [round(c["peak_ms"]) for c in cl],
            "cluster_median_period_s": round(statistics.median(
                [(b["start_ms"] - a["start_ms"]) / 1000 for a, b in zip(cl, cl[1:])]), 1) if len(cl) > 2 else None,
            "clusters_aligned_with_gc_ge_100ms": aligned,
            "gc_pauses_logged": len(gcs), "gc_ge_100ms": len(big), "gc_ge_250ms": sum(g["duration_ms"] >= 250 for g in big),
            "gc_max_ms": max((g["duration_ms"] for g in gcs), default=None), "gc_by_generation": per_gen,
            "gc_largest": sorted(gcs, key=lambda g: -g["duration_ms"])[:8],
        }
    if saveall is not None:
        result["saveall"] = {k: saveall.get(k) for k in ("stopped", "rss_start_mib", "rss_window_end_mib",
                                                         "forced_full_ms", "rss_after_full_mib")}
    text = json.dumps(result, indent=1)
    (output / "analysis.json").write_text(text + "\n")
    print(text)
    return 0


def _iter_outliers(receipt):
    """The driver's bounded outlier timelines ([wall_ms, kind, value_ms, label]) wherever the receipt nests them."""
    stack = [receipt]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "outliers" and isinstance(value, list):
                    for row in value:
                        if isinstance(row, list) and len(row) == 4:
                            yield tuple(row)
                else:
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(node)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--src", type=Path, required=True)
    r.add_argument("--output", type=Path, required=True)
    r.add_argument("--duration", type=int, default=1800)
    r.add_argument("--lock-owner", default="claude-dl-query-gc-idr114")
    r.add_argument("--saveall", action="store_true")
    r.add_argument("--saveall-at", type=int, default=1200, help="seconds after the approximate driver start")
    r.add_argument("--saveall-seconds", type=int, default=120)
    a = sub.add_parser("analyse")
    a.add_argument("--output", type=Path, required=True)
    sub.add_parser("cleanup")
    args = parser.parse_args()
    if args.command == "run":
        return run(args)
    if args.command == "analyse":
        return analyse(args)
    print(json.dumps({"removed": cleanup()}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

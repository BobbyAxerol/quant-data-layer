#!/usr/bin/env python3
"""Isolated rehearsal of the v2.2.5 maintenance window: atomic mesh mTLS rotation + versioned-compose roll.

Purpose
    Run the window procedure of ``upgrade/evidence/releases/v2.2.5/MAINTENANCE_WINDOW_PACKET.md`` on a
    disposable copy of the KN topology before production sees it, and measure it:

    A  bring the mini-stack up from the committed compose (``deploy/compose/qdl-v2-stable-kn.yml``) on the OLD
       (deployed) TLS generation and the v2.2.4 image set, mirror the production Kafka dynamic listener truststore
       override, start a TS writer clone (deployed image and env, deployed JWT key, OLD identity, BOOTSTRAP) and
       wait until it serves every demanded slice;
    B  ROTATE atomically exactly as the packet says: clean writer stop, stop every mesh role, recreate brokers ->
       core -> ingestors -> projector -> query -> streams on the NEW generation and the target image set, restart
       the writer on the NEW identity in STRICT resume; measure downtime and recovery; verify that the new mesh
       refuses the old identity (the trust anchor really moved), no 401/JWT/manifest refusal, writer ready;
    C  ROLL BACK the same way to the OLD generation and v2.2.4 images; measure and verify again;
    then tear everything down and count what was removed.

    The stack: kafka1-3, rust_core, ingestor_binance_usdm + ingestor_okx_swap (live public venue WebSockets,
    read-only, bindings narrowed to ETH perpetuals so the cluster fits the CPU cap), market_projector_1,
    market_cache, stable_redis, query_kn_1, stream_kn_1, stream_kn_2 (the writer binding names both stream
    aliases), and the TS writer clone (scripts/p184_dr05_isolated.WriterClone from the TS worktree, ETH on both
    venues, disposable Redis/Postgres in a holder namespace on the rehearsal consumer network).

Isolation and limits
    compose project ``qdl-v225-reh`` (own containers, networks, volumes; no published port), rehearsal labels,
    total CPU caps <= 3.0 vCPU (CPU_CAPS), the host measurement lock must be held by ``--lock-owner``;
    production networks/volumes/containers are only inspected (the deployed writer, for its env), never joined or
    changed. Production host directories are mounted READ-ONLY (KN release dir, realm, core config, the deployed
    TLS generation); everything writable is under ``--base``. Kafka in the rehearsal grants super.users to every
    mesh principal by CN (the production per-principal ACL granularity is not mirrored; CN continuity is what a
    rotation can break, and a wrong CN is refused either way).

Honesty: this is ISOLATED rehearsal evidence on the same host (AGENTS 19), not production evidence; timings are
under the rehearsal CPU caps and narrowed data, so they bound the procedure, not production latency.

Usage
    kn_mesh_rotation_rehearsal.py --run BASE --old-gen ENV --new-gen ENV --old-release ENV --new-release ENV
        --secrets ENV --lock-owner OWNER
    kn_mesh_rotation_rehearsal.py --down BASE      (idempotent teardown of everything the run created)
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.host_measurement_lock import lock_state  # noqa: E402
from scripts import p184_g12_isolated_fault as g12  # noqa: E402  (WriterClone loader, clone schema, lifecycle argv)

PROJECT = "qdl-v225-reh"
LABEL = "qdl.rehearsal"
COMPOSE = ROOT / "deploy/compose/qdl-v2-stable-kn.yml"
KAFKA_IMAGE = "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
TS_ROOT = Path("/home/bobby/.worktrees/trading-system-next-upgrade")
WRITER = "market_data_service_p184w"
BROKERS = ("kafka1", "kafka2", "kafka3")
MESH_CLIENTS = ("rust_core", "ingestor_binance_usdm", "ingestor_okx_swap", "market_projector_1", "query_kn_1",
                "stream_kn_1", "stream_kn_2")
CACHES = ("stable_redis", "market_cache")
ROLES = BROKERS + CACHES + MESH_CLIENTS
# vCPU caps: sum with the writer clone (0.35), its Redis (0.05) / Postgres (0.15), the holder (0.02) and the
# warmup-burst driver (0.2, final pre-window run) = 2.99. Query keeps the production memory cap (2 GiB) so that the
# burst RSS check sees the window's limit.
CPU_CAPS = {"kafka1": 0.29, "kafka2": 0.29, "kafka3": 0.29, "stable_redis": 0.05, "market_cache": 0.07, "rust_core": 0.3,
            "ingestor_binance_usdm": 0.12, "ingestor_okx_swap": 0.12, "market_projector_1": 0.2,
            "query_kn_1": 0.25, "stream_kn_1": 0.12, "stream_kn_2": 0.12}
WRITER_CPUS, CLONE_REDIS_CPUS, CLONE_PG_CPUS, HOLDER_CPUS, BURST_CPUS = 0.35, 0.05, 0.15, 0.02, 0.2
MEM = {"kafka1": "1g", "kafka2": "1g", "kafka3": "1g", "market_cache": "512m", "query_kn_1": "2g"}
# Warmup-burst check (packet section 1b, IDR-129): the window's writer starts open 64 concurrent /warmup requests of
# the TS manifest mix (BAR 1m x1000 rows + latest-only feeds). Predeclared gate on the rehearsal Query: no restart /
# OOM kill, usage (cgroup memory.current - inactive_file, the docker-stats figure) after the settle <= baseline +
# BURST_RSS_TOLERANCE_MIB and < BURST_RSS_CEILING_MIB, and no per-burst step between the last two bursts
# (<= BURST_STEP_MIB). Request outcomes, latency and the writer clone's readiness during the bursts are reported,
# not gated (0.25 vCPU Query). Data volume = rehearsal history (ETH, minutes of bars), not production volume: the
# production-volume delta proof is the IDR-129 isolated A/B.
BURST = {"slices": 64, "bursts": 3, "gap_s": 30, "settle_s": 120, "sample_s": 2.0, "rounds": 4}
BURST_RSS_TOLERANCE_MIB, BURST_RSS_CEILING_MIB, BURST_STEP_MIB = 128, 1638, 32
ETH_PERPS = {"ETHUSDT", "ETH-USDT-SWAP"}
TS_MANIFEST = "trading-system.paper.stable-r11.yaml"
SUPER_USERS = ("User:phase8-admin;User:kafka1;User:kafka2;User:kafka3;User:phase8-core;User:phase8-producer;"
               "User:phase8-consumer;User:kn-projector;User:kn-stream;User:stable-authority-dispatcher")
ETH = {"ETHUSDT", "ETH-USDT-SWAP", "ETH-USDT"}  # ETH-USDT: the OKX index-tickers lane of ETH-USDT-SWAP MARK_INDEX
TOPICS = {  # name -> config (6 partitions, RF3; production names from the broker log dirs 2026-10-19)
    "md.raw.realtime.v2": {}, "md.raw.stable.v1": {}, "md.canonical.v2": {"retention.ms": "21600000"},
    "md.quarantine.stable.v1": {}, "md.canary.canonical.v2": {}, "md.projector.public.v2": {},
    "md.projector.legacy.v1": {},
    "md.latest.v2": {"cleanup.policy": "compact", "delete.retention.ms": "21600000", "segment.ms": "3600000",
                     "segment.bytes": "134217728"},
    "md.bars.v2": {"cleanup.policy": "compact", "delete.retention.ms": "604800000",
                   "min.compaction.lag.ms": "3600000", "segment.ms": "3600000", "segment.bytes": "134217728"},
    "qdl.authority.v1": {"cleanup.policy": "compact"}, "qdl.target-checkpoint.v1": {"cleanup.policy": "compact"},
}
DYNAMIC_TRUST = "listener.name.ssl.ssl.truststore.location=/etc/kafka/secrets/truststore-kn-v220.jks"
LIMITS = {"deadline_s": 2 * 3600 + 1800, "brokers_s": 300, "roles_s": 600, "writer_s": 900, "steady_s": 60}


BURST_DRIVER = r"""
import asyncio, json, statistics, sys, time
import httpx
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import (BarRevisionPolicy, DataRequirement, Feed, GapPolicy, Grade, RecoveryPolicy,
                            StalePolicy)
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.transport import RestQueryTransport

cfg = json.load(open("/work/burst.json"))
ENUMS = {"feed": Feed, "consumer_grade": Grade, "stale_policy": StalePolicy, "gap_policy": GapPolicy,
         "recovery": RecoveryPolicy, "bar_revision_policy": BarRevisionPolicy, "event_recency_policy": StalePolicy}

def requirement(item):
    return DataRequirement(**{k: (ENUMS[k](v) if k in ENUMS and v is not None else v) for k, v in item.items()})

async def one(t, req, out):
    # one writer slice: queued warmup (single replica, no redirect header); retry a retryable refusal or a transport
    # error like the bridge reconnect loop
    started = time.perf_counter()
    for round_ in range(cfg["rounds"]):
        t0 = time.perf_counter()
        try:
            payload = await t.warmup(req, consumer_id=cfg["consumer_id"])
        except DataLayerError as caught:
            out["requests"].append([caught.code, caught.reason, round((time.perf_counter() - t0) * 1e3)])
            if not caught.retryable:
                out["refused"].append(caught.code)
                return
            await asyncio.sleep(max(0.25 * (round_ + 1), (caught.retry_after_ms or 0) / 1000))
            continue
        except httpx.HTTPError as caught:
            out["requests"].append(["TRANSPORT", type(caught).__name__, round((time.perf_counter() - t0) * 1e3)])
            await asyncio.sleep(1.0)
            continue
        out["requests"].append(["200", len(payload.get("data", ())), round((time.perf_counter() - t0) * 1e3)])
        out["ok_ms"].append(round((time.perf_counter() - started) * 1e3))
        return
    out["failed"] += 1

async def main():
    j = cfg["jwt"]
    cred = RotatingJwtCredentialProvider(private_key_file="/jwt/private.key", key_id=j["key_id"], algorithm="RS256",
        issuer=j["issuer"], audience=j["audience"], subject=j["subject"], environment=j["environment"],
        roles=("market_data_reader", "historical_reader", "stream_consumer"), venues=("BINANCE", "OKX"),
        consumer_manifest_revision=j["manifest_revision"], lifetime_seconds=600, refresh_before_seconds=120)
    tls = WorkloadTlsConfig("/id/ca.crt", "/id/client.crt", "/id/client.key")
    t = RestQueryTransport(cfg["url"], timeout_seconds=30.0, credential_provider=cred, tls=tls)
    reqs = [requirement(item) for item in cfg["requirements"]]
    out = {"requests": [], "ok_ms": [], "refused": [], "failed": 0}
    t0 = time.perf_counter()
    await asyncio.gather(*(one(t, r, out) for r in reqs))
    statuses = {}
    for status, _, _ in out["requests"]:
        statuses[status] = statuses.get(status, 0) + 1
    ok = sorted(ms for st, _, ms in out["requests"] if st == "200")
    rows = sorted(n for st, n, _ in out["requests"] if st == "200")
    print(json.dumps({"wall_s": round(time.perf_counter() - t0, 1), "slices": len(reqs), "statuses": statuses,
                      "failed_slices": out["failed"], "refused": sorted(set(out["refused"])),
                      "req200_ms_p50": ok[len(ok) // 2] if ok else None,
                      "req200_ms_max": ok[-1] if ok else None, "rows_max": rows[-1] if rows else None,
                      "rows_total": sum(rows)}))
    await t.close()

asyncio.run(main())
"""


def now_ns() -> int:
    return time.time_ns()


def utc(ns=None) -> str:
    return dt.datetime.fromtimestamp((ns or now_ns()) / 1e9, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def sh(argv, *, timeout=120, check=True, env=None, input_text=None):
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=env, input=input_text)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(argv[:4])} -> {proc.returncode}: {proc.stderr.strip()[-600:]}")
    return proc


def read_env(path: Path) -> dict:
    out = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k] = v.strip("'\"")
    return out


class Rehearsal:
    def __init__(self, base: Path, args):
        self.base = base.resolve()
        self.args = args
        self.evidence = self.base / "evidence"
        self.events = self.evidence / "events.jsonl"
        self.started = time.monotonic()
        self.clone = None
        self.holder = f"{PROJECT}-tsholder"
        self.first_healthy = {}
        self.current_gen_env = None
        self.ts_root = Path(getattr(args, "ts_root", None) or TS_ROOT)  # TS checkout for the writer clone loader

    # ------------------------------------------------------------------ bookkeeping
    def note(self, event, **fields):
        row = {"t": utc(), "ns": now_ns(), "event": event, **fields}
        with self.events.open("a") as handle:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        print(json.dumps(row, sort_keys=True, default=str)[:600], flush=True)
        return row

    def remaining(self):
        return LIMITS["deadline_s"] - (time.monotonic() - self.started)

    def wait(self, seconds, until, poll=2.0):
        limit = time.monotonic() + min(seconds, max(0, self.remaining()))
        while time.monotonic() < limit:
            value = until()
            if value:
                return value
            time.sleep(poll)
        return None

    # ------------------------------------------------------------------ preparation
    def prepare(self):
        state, lock = lock_state()
        if state != "VALID" or (lock or {}).get("owner") != self.args.lock_owner:
            raise SystemExit(f"host measurement lock must be held by {self.args.lock_owner}: {state} {lock}")
        if shutil.disk_usage("/").free < 40 * 2**30:
            raise SystemExit("less than 40 GiB free on /")
        if sh(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"]).stdout.strip():
            raise SystemExit(f"project {PROJECT} already has containers; run --down first")
        self.base.mkdir(parents=True, exist_ok=False)
        self.evidence.mkdir()
        for sub in ("projector-1", "query-1", "query-2", "projector-2", "bar-edge"):
            (self.base / "state" / sub).mkdir(parents=True)
        for query in ("query-1", "query-2"):  # production layout: runtime/session-liveness -> /session-liveness
            (self.base / "state" / query / "runtime").mkdir()
            (self.base / "state" / query / "shared").mkdir()
            (self.base / "state" / query / "runtime" / "session-liveness").symlink_to("/session-liveness")
        sh(["sudo", "-n", "chown", "-R", "-h", "10001:10001", str(self.base / "state")])
        sh(["sudo", "-n", "chmod", "-R", "0750", str(self.base / "state")])
        # Ingestor configs: the production files narrowed to ETH perpetuals (test-only, recorded with digests).
        cfg_dir = self.base / "ingestor-config"
        cfg_dir.mkdir()
        release_old = read_env(self.args.old_release)
        narrowed = {}
        for name in ("ingestor_okx_swap.json", "ingestor_binance_usdm.json"):
            src = Path(release_old["QDL_INGESTOR_CONFIG_DIR"]) / name
            doc = json.loads(sh(["sudo", "-n", "cat", str(src)]).stdout)
            before = len(doc["bindings"])
            doc["bindings"] = [b for b in doc["bindings"] if b.get("native_symbol") in ETH]
            (cfg_dir / name).write_text(json.dumps(doc, indent=1, sort_keys=True))
            narrowed[name] = {"bindings_before": before, "bindings_after": len(doc["bindings"]),
                              "feeds": sorted({b["feed"] for b in doc["bindings"]})}
        os.chmod(cfg_dir, 0o755)
        for f in cfg_dir.iterdir():
            os.chmod(f, 0o644)
        self.write_rehearsal_env(topic_id="pending")
        (self.base / "override.yml").write_text(self.override())
        self.note("prepared", narrowed_ingestors=narrowed, cpu_caps=CPU_CAPS,
                  cpu_total=round(sum(CPU_CAPS.values()) + WRITER_CPUS + CLONE_REDIS_CPUS + CLONE_PG_CPUS + HOLDER_CPUS
                                  + BURST_CPUS, 3))

    def write_rehearsal_env(self, topic_id):
        (self.base / "rehearsal.env").write_text(
            "# rehearsal-only overrides (writable state and narrowed ingestor configs under the run base)\n"
            f"QDL_KN_STATE_DIR={self.base / 'state'}\nQDL_INGESTOR_CONFIG_DIR={self.base / 'ingestor-config'}\n"
            f"REH_TOPIC_ID={topic_id}\n")

    def override(self) -> str:
        lines = ["# Rehearsal overlay (scripts/kn_mesh_rotation_rehearsal.py): isolated networks/volumes, no ports,",
                 "# CPU caps, rehearsal Kafka super.users and canonical topic id.", "networks:"]
        for key, internal in (("stable_internal", True), ("stable_egress", False), ("kn_consumer", True),
                              ("executor_network", False)):
            lines.append(f"  {key}: !override")
            lines.append(f"    name: {PROJECT}_{key}")
            lines.append(f"    internal: {'true' if internal else 'false'}")
            lines.append(f"    labels: {{{LABEL}: v225}}")
        lines.append("volumes:")
        for key in ("kafka1_data", "kafka2_data", "kafka3_data", "stable_state"):
            lines.append(f"  {key}: !override")
            lines.append(f"    name: {PROJECT}_{key}")
            lines.append(f"    labels: {{{LABEL}: v225}}")
        lines.append("services:")
        for role in ROLES:
            lines.append(f"  {role}:")
            lines.append(f"    cpus: {CPU_CAPS[role]}")
            if role in MEM:
                lines.append(f"    mem_limit: {MEM[role]}")
                lines.append(f"    memswap_limit: {MEM[role]}")
            lines.append("    ports: !reset []")
            lines.append(f"    labels: {{{LABEL}: v225}}")
            env = {}
            if role in BROKERS:
                env = {"KAFKA_SUPER_USERS": SUPER_USERS, "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"}
            if role.startswith(("market_projector", "stream_kn", "query_kn")):
                env["QDL_KN_TOPIC_ID"] = "${REH_TOPIC_ID:?}"
            if env:
                lines.append("    environment:")
                for k, v in env.items():
                    lines.append(f"      {k}: {json.dumps(v)}")
        return "\n".join(lines) + "\n"

    def compose(self, release: Path, gen: Path, *args, timeout=600, check=True):
        argv = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), "-f", str(self.base / "override.yml"),
                "--env-file", str(release), "--env-file", str(gen), "--env-file", str(self.args.secrets),
                "--env-file", str(self.base / "rehearsal.env"), *args]
        return sh(argv, timeout=timeout, check=check)

    # ------------------------------------------------------------------ kafka admin
    def admin(self, gen_env: dict, tool, *args, timeout=90, check=True):
        return sh(["docker", "run", "--rm", "--network", f"{PROJECT}_stable_internal", "--label", f"{LABEL}=v225",
                   "--cpus", "0.25", "--memory", "512m", "-e", "KAFKA_HEAP_OPTS=-Xms64m -Xmx256m",
                   "-v", f"{gen_env['QDL_TLS_KAFKA_DIR']}:/etc/kafka/secrets:ro", "--entrypoint",
                   f"/opt/kafka/bin/{tool}", KAFKA_IMAGE, "--bootstrap-server", "kafka1:9092,kafka2:9092,kafka3:9092",
                   "--command-config", "/etc/kafka/secrets/admin.properties", *args], timeout=timeout, check=check)

    def brokers_ok(self, gen_env):
        proc = self.admin(gen_env, "kafka-broker-api-versions.sh", timeout=60, check=False)
        ids = {line.split("(id: ")[1].split(" ")[0] for line in proc.stdout.splitlines() if "(id: " in line}
        return ids == {"1", "2", "3"}

    def create_topics(self, gen_env):
        for topic, cfg in TOPICS.items():
            args = ["--create", "--if-not-exists", "--topic", topic, "--partitions", "6", "--replication-factor", "3",
                    "--config", "min.insync.replicas=2"]
            for k, v in cfg.items():
                args += ["--config", f"{k}={v}"]
            self.admin(gen_env, "kafka-topics.sh", *args)
        out = self.admin(gen_env, "kafka-topics.sh", "--describe", "--topic", "md.canonical.v2").stdout
        topic_id = next(part.split(":", 1)[1].strip() for line in out.splitlines() for part in line.split("\t")
                        if part.strip().startswith("TopicId:"))
        for node in (1, 2, 3):
            self.admin(gen_env, "kafka-configs.sh", "--alter", "--entity-type", "brokers", "--entity-name", str(node),
                       "--add-config", DYNAMIC_TRUST)
        dyn = {node: self.admin(gen_env, "kafka-configs.sh", "--describe", "--entity-type", "brokers",
                                "--entity-name", str(node)).stdout.count("truststore-kn-v220.jks") for node in (1, 2, 3)}
        return topic_id, dyn

    def urp(self, gen_env):
        out = self.admin(gen_env, "kafka-topics.sh", "--describe", "--under-replicated-partitions", check=False)
        return len([l for l in out.stdout.splitlines() if l.strip()]) if out.returncode == 0 else None

    def group_offsets(self, gen_env):
        """Committed offsets of every consumer group (group|topic|partition -> offset); continuity evidence."""
        out = self.admin(gen_env, "kafka-consumer-groups.sh", "--describe", "--all-groups", timeout=120,
                         check=False).stdout
        offsets = {}
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 4 and parts[2].isdigit() and parts[3].isdigit():
                offsets[f"{parts[0]}|{parts[1]}|{parts[2]}"] = int(parts[3])
        return offsets

    @staticmethod
    def offset_continuity(before, after):
        regressed = {k: (v, after[k]) for k, v in before.items() if k in after and after[k] < v}
        missing = sorted(k for k in before if k not in after)
        groups = sorted({k.split("|")[0] for k in before})
        return {"groups": groups, "partitions_before": len(before), "partitions_after": len(after),
                "regressed": regressed, "missing_after": missing[:20], "ok": not regressed and not missing}

    def route_timeline(self, seconds, t0):
        """Writer heartbeat every 3 s until every demanded slice is healthy: per-route time of last unhealthy."""
        last_bad, first_seen, samples = {}, None, 0
        limit = time.monotonic() + seconds
        while time.monotonic() < limit:
            beat = self.clone.heartbeat() if self.clone else {}
            samples += 1
            rows = beat.get("unhealthy") or []
            t = round((now_ns() - t0) / 1e9, 1)
            for u in rows:
                last_bad[f"{u.get('provider')}|{u.get('symbol')}|{u.get('feed')}|{u.get('error_code')}"] = t
            fresh = float(beat.get("last_seen_unix") or 0) * 1e9 > t0  # a beat of THIS process, not the stopped one
            if beat.get("heartbeat") and fresh and first_seen is None:
                first_seen = t
            if fresh and self.writer_beat()["ready"]:
                return {"ready": True, "ready_after_s": t, "first_heartbeat_s": first_seen, "samples": samples,
                        "route_last_unhealthy_s": dict(sorted(last_bad.items()))}
            time.sleep(3)
        return {"ready": False, "first_heartbeat_s": first_seen, "samples": samples,
                "route_last_unhealthy_s": dict(sorted(last_bad.items())), "last": self.writer_beat()}

    # ------------------------------------------------------------------ role health
    def health(self, role):
        out = sh(["docker", "inspect", "-f", "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}",
                  f"{PROJECT}-{role}-1"], check=False).stdout.strip()
        return out

    def all_healthy(self, roles):
        states = {r: self.health(r) for r in roles}
        t = now_ns()
        for r, state in states.items():  # first time each role was seen healthy in the current phase
            if state == "running|healthy":
                self.first_healthy.setdefault(r, t)
        return states if all(s == "running|healthy" for s in states.values()) else None

    def probe_query(self, identity_dir, image):
        """Query /health/ready over mTLS with a TS identity (Python ssl, the server cert's SAN qdl-v2-query)."""
        code = ("import json,ssl,sys,urllib.request\n"
                "c=ssl.create_default_context(cafile='/id/ca.crt');c.load_cert_chain('/id/client.crt','/id/client.key')\n"
                "try:\n r=urllib.request.urlopen('https://qdl-v2-query:8200/health/ready',context=c,timeout=5)\n"
                " b=json.loads(r.read() or b'{}');print(json.dumps({'ok':True,'status':r.status,'body_status':b.get('status'),"
                "'keys':sorted(b)[:12]}))\n"
                "except Exception as e:\n print(json.dumps({'ok':False,'error':type(e).__name__,'detail':str(e)[:200]}))\n")
        proc = sh(["docker", "run", "--rm", "--network", f"{PROJECT}_executor_network", "--label", f"{LABEL}=v225",
                   "--cpus", "0.2", "--memory", "256m", "--read-only", "--user", "0:0", "-v", f"{identity_dir}:/id:ro",
                   "--entrypoint", "python", image, "-B", "-c", code], timeout=60, check=False)
        try:
            return json.loads(proc.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError):
            return {"ok": False, "error": "NO_OUTPUT", "detail": proc.stderr[-200:]}

    def probe_stream(self, identity_dir, alias, image):
        """TLS handshake to a Stream gateway with a TS identity (openssl s_client, verify the server chain)."""
        proc = sh(["docker", "run", "--rm", "--network", f"{PROJECT}_executor_network", "--label", f"{LABEL}=v225",
                   "--cpus", "0.2", "--memory", "128m", "--read-only", "--user", "0:0", "-v", f"{identity_dir}:/id:ro",
                   "--entrypoint", "openssl", image, "s_client", "-connect", f"{alias}:8210", "-servername", alias,
                   "-cert", "/id/client.crt", "-key", "/id/client.key", "-CAfile", "/id/ca.crt",
                   "-verify_return_error", "-brief"], timeout=40, check=False, input_text="")
        text = (proc.stdout + proc.stderr)
        return {"ok": proc.returncode == 0 and "Verification: OK" in text, "rc": proc.returncode,
                "tail": " ".join(text.split())[-200:]}

    # ------------------------------------------------------------------ TS writer clone
    def writer_inspect(self, ts_identity_dir):
        inspect = json.loads(sh(["docker", "inspect", WRITER]).stdout)[0]
        clone = copy.deepcopy(inspect)
        for m in clone["Mounts"]:
            if m["Destination"] == "/run/secrets/qdl-v2":
                m["Source"] = ts_identity_dir
        return inspect, clone

    def start_writer(self, ts_identity_dir):
        iso = g12.load_writer_clone(self.ts_root)
        deployed, inspect = self.writer_inspect(ts_identity_dir)
        sh(["docker", "run", "-d", "--rm", "--name", self.holder, "--label", f"{LABEL}=v225", "--network",
            f"{PROJECT}_executor_network", "--cpus", str(HOLDER_CPUS), "--memory", "32m", "--entrypoint", "sleep",
            "alpine:3.20", str(LIMITS["deadline_s"])])
        logs = self.base / "tslogs"
        logs.mkdir(mode=0o700, exist_ok=True)
        tempfile.tempdir = str(logs)

        def run(*args, check=True, capture=True, timeout=120):
            args = list(args)
            if args[:3] == ["docker", "run", "-d"]:
                args[3:3] = ["--label", f"{LABEL}=v225"]
                if "--cpus" in args:  # WriterClone sizes for a 4-worker production clone; the rehearsal caps it
                    i = args.index("--cpus")
                    name = args[args.index("--name") + 1] if "--name" in args else ""
                    args[i + 1] = str(WRITER_CPUS if "writer" in name else CLONE_REDIS_CPUS)
                else:
                    name = args[args.index("--name") + 1] if "--name" in args else ""
                    args[3:3] = ["--cpus", str(CLONE_PG_CPUS if "tspg" in name else CLONE_REDIS_CPUS)]
            elif args[:3] == ["docker", "rm", "-f"]:
                args[3:3] = ["-v"]
            result = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
            if check and result.returncode:
                raise RuntimeError(f"{args[:3]} -> {result.returncode}: {result.stderr[-300:]}")
            return result.stdout.strip() if capture else ""

        clone = iso.WriterClone(inspect["Image"], redis_image="redis:7.2-alpine", postgres_image="postgres:16-alpine",
                                venues=("BINANCE", "OKX"), network="container:" + self.holder, redis_port=6379,
                                pg_port=5432, inspect=inspect, run=run, tag="v225reh", keep_exited=True)
        clone.name, clone.redis_name, clone.pg_name = f"{PROJECT}-tswriter", f"{PROJECT}-tsredis", f"{PROJECT}-tspg"
        clone.deadline = clone.clock() + LIMITS["writer_s"]
        clone.ready_s = 240.0  # Postgres initdb under a 0.15 vCPU cap
        destination = g12.writer_state_destination(deployed)
        clone.state_dir = self.base / "tsstate"
        clone.state_dir.mkdir(mode=0o700)
        os.chmod(clone.state_dir, 0o777)
        base_args = clone.writer_args
        # G12 pattern: WriterClone's own lifecycle stays inactive (no "required"); the durable state bind and the
        # BOOTSTRAP/STRICT resume flag come from g12.lifecycle_writer_args over the deployed argv.
        clone.lifecycle = {"phase": "BOOTSTRAP"}
        clone.writer_args = (lambda c=clone, b=base_args, d=destination: g12.lifecycle_writer_args(
            b(), c.image, str(c.state_dir), d, c.lifecycle["phase"]))
        clone._prepare_lifecycle = lambda: None  # lifecycle managed here (G12 pattern)
        sql, schema = g12.clone_schema_sql(self.ts_root)
        ready = clone._wait_ready
        clone._wait_ready = lambda c=clone, r=ready, s=sql: (r(), g12.apply_clone_schema(c, s))
        clone.__enter__()
        self.clone = clone
        self.note("writer_clone_started", phase="BOOTSTRAP", image=inspect["Image"], identity=ts_identity_dir,
                  schema=schema, cpus=WRITER_CPUS)

    def writer_beat(self):
        beat = self.clone.heartbeat() if self.clone else {"heartbeat": False}
        unhealthy = beat.get("unhealthy_v2_slices")
        n_unhealthy = len(unhealthy) if isinstance(unhealthy, list) else int(unhealthy or 0)
        demanded = beat.get("demanded_v2_slices")
        n_demanded = len(demanded) if isinstance(demanded, list) else int(demanded or 0)
        fresh = bool(beat.get("last_seen_unix")) and time.time() - float(beat["last_seen_unix"]) < 30
        return {"ready": bool(beat.get("heartbeat")) and fresh and n_demanded > 0 and n_unhealthy == 0
                and not beat.get("unhealthy"), "demanded": n_demanded, "unhealthy": n_unhealthy,
                "unhealthy_rows": (beat.get("unhealthy") or [])[:6], "status": beat.get("status"),
                "alive": self.clone.alive() if self.clone else False}

    def writer_ready_since(self, since_unix):
        beat = self.writer_beat()
        return beat if beat["ready"] else None

    def restart_writer(self, ts_identity_dir, phase):
        self.clone.run("docker", "stop", "-t", "30", self.clone.name, check=False, timeout=60)
        self.clone.run("docker", "rm", "-fv", self.clone.name, check=False, timeout=30)
        for m in self.clone.inspect["Mounts"]:
            if m["Destination"] == "/run/secrets/qdl-v2":
                m["Source"] = ts_identity_dir
        self.clone.lifecycle["phase"] = phase
        self.clone.run(*self.clone.writer_args())
        self.note("writer_restarted", phase=phase, identity=ts_identity_dir)

    def refusal_counts(self, since_iso):
        counts = {}
        for role in ("query_kn_1", "stream_kn_1", "stream_kn_2"):
            text = sh(["docker", "logs", "--since", since_iso, f"{PROJECT}-{role}-1"], check=False, timeout=60)
            blob = (text.stdout + text.stderr)
            counts[role] = {k: blob.count(k) for k in (" 401", "UNAUTHENTICATED", "manifest_revision", "certificate",
                                                       "PERMISSION_DENIED")}
        if self.clone:
            tail = self.clone.run("docker", "exec", self.clone.name, "sh", "-c",
                                  "cat /tmp/logs/*.log 2>/dev/null | tail -c 4000000", check=False, timeout=60)
            # auth/protocol failures and legitimate data-policy refusals are counted apart (never merged)
            # whole-word matches (a bare substring count also hit numbers such as "...401..." in the 2026-10-19 final
            # run), with up to 3 matching lines kept for attribution
            auth = {k: re.findall(rf"(?<![0-9A-Za-z_,.:]){k}(?![0-9A-Za-z_.])", tail) for k in (
                "401", "UNAUTHENTICATED", "CERTIFICATE_VERIFY_FAILED", "SSLError", "STRICT_RESUME_REFUSAL",
                "HANDOFF_RESUME_REQUIRED")}
            pattern = re.compile(r"(?<![0-9A-Za-z_,.:])(401|UNAUTHENTICATED|CERTIFICATE_VERIFY_FAILED|SSLError|"
                                 r"STRICT_RESUME_REFUSAL|HANDOFF_RESUME_REQUIRED)(?![0-9A-Za-z_.])")
            counts["ts_writer"] = {
                "auth_protocol": {k: len(v) for k, v in auth.items()},
                "auth_protocol_lines": [line[:300] for line in tail.splitlines() if pattern.search(line)][:3],
                "data_policy": {k: tail.count(k) for k in ("DATA_STALE", "SESSION_STATE", "EVENT_AGE",
                                                            "SOURCE_UNAVAILABLE", "OPEN_SEQUENCE_GAP")}}
        return counts

    # ------------------------------------------------------------------ phases
    def phase_up(self, name, release, gen, *, first):
        gen_env, release_env = read_env(gen), read_env(release)
        t0 = now_ns()
        self.first_healthy = {}
        self.compose(release, gen, "up", "-d", "--no-deps", *BROKERS)
        if not self.wait(LIMITS["brokers_s"], lambda: self.brokers_ok(gen_env), poll=3):
            raise RuntimeError(f"{name}: brokers never answered with generation {gen_env['QDL_TLS_GENERATION']}")
        t_brokers = now_ns()
        extra = {}
        if first:
            topic_id, dyn = self.create_topics(gen_env)
            self.write_rehearsal_env(topic_id)
            extra = {"canonical_topic_id": topic_id, "dynamic_truststore_override": dyn}
            # Production's stable_state volume was initialised by the stable_state_init one-shot (uid 10001); the
            # rehearsal volume is fresh, so the same ownership is set before the ingestors write session liveness.
            sh(["docker", "volume", "create", "--label", f"{LABEL}=v225", f"{PROJECT}_stable_state"])
            sh(["docker", "run", "--rm", "--network", "none", "--label", f"{LABEL}=v225", "-v",
                f"{PROJECT}_stable_state:/v", "alpine:3.20", "sh", "-c",
                "mkdir -p /v/runtime/session-liveness /v/runtime/generations && chown -R 10001:10001 /v"])
            self.compose(release, gen, "up", "-d", "--no-deps", *CACHES)
        self.compose(release, gen, "up", "-d", "--no-deps", "--force-recreate", "rust_core",
                     "ingestor_binance_usdm", "ingestor_okx_swap")
        self.compose(release, gen, "up", "-d", "--no-deps", "--force-recreate", "market_projector_1")
        healthy_ingest = self.wait(LIMITS["roles_s"], lambda: self.all_healthy(
            ("rust_core", "ingestor_binance_usdm", "ingestor_okx_swap", "market_projector_1")), poll=3)
        self.compose(release, gen, "up", "-d", "--no-deps", "--force-recreate", "query_kn_1")
        self.compose(release, gen, "up", "-d", "--no-deps", "--force-recreate", "stream_kn_1", "stream_kn_2")
        healthy = self.wait(LIMITS["roles_s"], lambda: self.all_healthy(ROLES), poll=3)
        t_dl = now_ns()
        query = self.wait(LIMITS["roles_s"], lambda: (lambda p: p if p.get("ok") else None)(
            self.probe_query(gen_env["QDL_TLS_TS_IDENTITY_DIR"], release_env["QDL_IMAGE_QUERY"])), poll=3)
        t_query = now_ns()
        streams = {a: self.probe_stream(gen_env["QDL_TLS_TS_IDENTITY_DIR"], a, release_env["QDL_IMAGE_STREAM"])
                   for a in ("qdl-v2-stream-a", "qdl-v2-stream-b")}
        return self.note(f"{name}_dl_up", generation=gen_env["QDL_TLS_GENERATION"], release=str(release),
                         brokers_s=round((t_brokers - t0) / 1e9, 2), dl_healthy_s=round((t_dl - t0) / 1e9, 2),
                         query_ready_s=round((t_query - t0) / 1e9, 2), ingest_healthy=bool(healthy_ingest),
                         all_healthy=healthy, query=query, streams=streams, urp=self.urp(gen_env),
                         healthy_after_s={r: round((t - t0) / 1e9, 1) for r, t in sorted(self.first_healthy.items())},
                         **extra)

    def atomic_switch(self, name, release, gen, phase_label):
        """The window transaction: writer clean stop, every mesh role stopped, recreate on ``gen``/``release``."""
        gen_env = read_env(gen)
        offsets_before = self.group_offsets(self.current_gen_env)
        cursors_before = self.clone.checkpoint_items()
        t0 = now_ns()
        self.clone.run("docker", "stop", "-t", "30", self.clone.name, check=False, timeout=60)
        exit_state = sh(["docker", "inspect", "-f", "{{.State.ExitCode}}", self.clone.name], check=False).stdout.strip()
        t_writer_stopped = now_ns()
        clients = [f"{PROJECT}-{r}-1" for r in MESH_CLIENTS]
        sh(["docker", "stop", "-t", "45", *clients], timeout=180)
        # stable_redis is recreated inside the window (owner C1: healthcheck cadence) while every reader of it is
        # stopped; it is ephemeral (quota / session state), market_cache (the projection cache) is never touched.
        sh(["docker", "stop", "-t", "10", f"{PROJECT}-stable_redis-1"], timeout=60)
        sh(["docker", "stop", "-t", "60", *[f"{PROJECT}-{b}-1" for b in BROKERS]], timeout=180)
        t_all_stopped = now_ns()
        self.note(f"{name}_stopped", writer_exit_code=exit_state,
                  writer_stop_s=round((t_writer_stopped - t0) / 1e9, 2),
                  mesh_stop_s=round((t_all_stopped - t0) / 1e9, 2))
        self.compose(release, gen, "up", "-d", "--no-deps", "--force-recreate", *BROKERS, "stable_redis")
        up = self.phase_up(name, release, gen, first=False)
        self.clone.run("docker", "rm", "-fv", self.clone.name, check=False, timeout=30)
        for m in self.clone.inspect["Mounts"]:
            if m["Destination"] == "/run/secrets/qdl-v2":
                m["Source"] = gen_env["QDL_TLS_TS_IDENTITY_DIR"]
        # The writer's execution surfaces in its own Redis expire 120 s after the last market row (debug run
        # 2026-10-19: every TRADE/QUOTE slice refused the STRICT resume, HANDOFF_RESUME_REQUIRED, terminal per
        # IDR-101). An outage longer than that recovers through the deployed lifecycle: BOOTSTRAP start
        # (MARKET_DATA_NATIVE_REQUIRE_RESUME=0) until ready, then a clean restart in STRICT (the deployed env).
        self.clone.lifecycle["phase"] = "BOOTSTRAP"
        self.clone.run(*self.clone.writer_args())
        t_writer_started = now_ns()
        timeline = self.route_timeline(LIMITS["writer_s"], t_writer_started)
        t_bootstrap_ready = now_ns()
        strict = None
        if timeline["ready"] and phase_label == "STRICT":
            self.restart_writer(gen_env["QDL_TLS_TS_IDENTITY_DIR"], "STRICT")
            t_strict = now_ns()
            strict = self.route_timeline(LIMITS["writer_s"], t_strict)
        ready = self.writer_beat() if timeline["ready"] and (strict is None or strict["ready"]) else None
        t_ready = now_ns()
        self.current_gen_env = gen_env
        offsets_after = self.group_offsets(gen_env)
        cursors_after = self.clone.checkpoint_items()
        return self.note(f"{name}_measured", generation=gen_env["QDL_TLS_GENERATION"],
                         writer_ready=bool(ready), writer=ready or self.writer_beat(), route_recovery=timeline,
                         strict_restart=strict, bootstrap_ready_after_stop_s=round((t_bootstrap_ready - t0) / 1e9, 2),
                         kafka_offset_continuity=self.offset_continuity(offsets_before, offsets_after),
                         writer_cursor_items={"before": cursors_before, "after": cursors_after},
                         consumer_visible_downtime_s=round((t_ready - t0) / 1e9, 2),
                         dl_query_ready_after_stop_s=round((t_writer_started - t0) / 1e9, 2),
                         writer_recovery_after_restart_s=round((t_ready - t_writer_started) / 1e9, 2),
                         dl=up.get("query"))

    def negative_probes(self, name, release, current_gen, other_gen):
        """The mesh on ``current_gen`` must refuse every identity of ``other_gen`` (trust anchor really moved)."""
        cur, oth, rel = read_env(current_gen), read_env(other_gen), read_env(release)
        return self.note(f"{name}_negative_probes",
                         query_with_other_identity=self.probe_query(oth["QDL_TLS_TS_IDENTITY_DIR"], rel["QDL_IMAGE_QUERY"]),
                         stream_with_other_identity=self.probe_stream(oth["QDL_TLS_TS_IDENTITY_DIR"], "qdl-v2-stream-a",
                                                                      rel["QDL_IMAGE_STREAM"]),
                         kafka_admin_with_other_generation=self.brokers_ok(oth),
                         kafka_admin_with_current_generation=self.brokers_ok(cur))

    def steady(self, name, gen):
        """Writer ready for LIMITS['steady_s'] continuously, plus the refusal counters since phase start."""
        start = now_ns()
        samples, ok = [], True
        while (now_ns() - start) / 1e9 < LIMITS["steady_s"]:
            beat = self.writer_beat()
            samples.append(beat["ready"])
            ok = ok and beat["ready"]
            time.sleep(5)
        return self.note(f"{name}_steady", ready_all_samples=ok, samples=len(samples), last=self.writer_beat(),
                         query=self.probe_query(read_env(gen)["QDL_TLS_TS_IDENTITY_DIR"],
                                                read_env(self.current_release)["QDL_IMAGE_QUERY"]))

    # ------------------------------------------------------------------ warmup-burst RSS check (IDR-129, packet 1b)
    def query_memory(self):
        """Rehearsal Query cgroup: usage = memory.current - inactive_file (the docker-stats figure), anon, OOM kills."""
        cid, restarts, started = sh(["docker", "inspect", "-f", "{{.Id}}|{{.RestartCount}}|{{.State.StartedAt}}",
                                     f"{PROJECT}-query_kn_1-1"]).stdout.strip().split("|")
        base = Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope")
        stat = dict(line.split() for line in (base / "memory.stat").read_text().splitlines())
        events = dict(line.split() for line in (base / "memory.events").read_text().splitlines())
        current = int((base / "memory.current").read_text())
        return {"usage_mib": round((current - int(stat["inactive_file"])) / 2**20, 1),
                "anon_mib": round(int(stat["anon"]) / 2**20, 1), "restart_count": int(restarts),
                "started_at": started, "oom_kill": int(events.get("oom_kill", 0))}

    def burst_requirements(self, release_env, deployed):
        """The TS manifest (r11, the realm the rehearsal Query serves) requirements of the ETH perpetual products of
        the deployed writer binding (= the clone's 12 slices), cycled to BURST['slices'] in a fixed order."""
        import yaml  # host dependency of the rehearsal tool only here
        binding_src = next(m["Source"] for m in deployed["Mounts"]
                           if m["Destination"].startswith("/run/secrets/qdl-v2-binding/"))
        binding = json.loads(Path(binding_src).read_text())
        eth = {(p["instrument_uid"], p["feed"], p.get("interval")) for p in binding["products"]
               if p.get("native_symbol") in ETH_PERPS}
        manifest = yaml.safe_load((Path(release_env["QDL_CONSUMER_REALM_DIR"]) / TS_MANIFEST).read_text())
        chosen = sorted((r for r in manifest["spec"]["requirements"]
                         if (r["instrument_uid"], r["feed"], r.get("interval")) in eth),
                        key=lambda r: (r["feed"], r["instrument_uid"], r.get("interval") or ""))
        if len(chosen) != len(eth):
            raise RuntimeError(f"burst: manifest matches {len(chosen)} of {len(eth)} ETH binding products")
        return [chosen[i % len(chosen)] for i in range(BURST["slices"])], len(chosen)

    def burst_check(self, release, gen):
        release_env, gen_env = read_env(release), read_env(gen)
        deployed = json.loads(sh(["docker", "inspect", WRITER]).stdout)[0]
        env = dict(item.split("=", 1) for item in deployed["Config"]["Env"])
        jwt_src = next(m["Source"] for m in deployed["Mounts"] if m["Destination"] == "/run/secrets/qdl-v2-jwt/private.key")
        requirements, distinct = self.burst_requirements(release_env, deployed)
        work = self.base / "burst"
        work.mkdir(mode=0o755)
        (work / "driver.py").write_text(BURST_DRIVER)
        (work / "burst.json").write_text(json.dumps({
            "url": "https://qdl-v2-query:8200", "consumer_id": env["MARKET_DATA_DATA_LAYER_V2_CONSUMER_ID"],
            "rounds": BURST["rounds"], "requirements": requirements,
            "jwt": {"key_id": env["DATA_LAYER_V2_JWT_KEY_ID"], "issuer": env["DATA_LAYER_V2_JWT_ISSUER"],
                    "audience": env["DATA_LAYER_V2_JWT_AUDIENCE"], "subject": env["DATA_LAYER_V2_JWT_SUBJECT"],
                    "environment": env["DATA_LAYER_V2_JWT_ENVIRONMENT"],
                    "manifest_revision": int(env["DATA_LAYER_V2_JWT_MANIFEST_REVISION"])}}))
        baseline = self.query_memory()
        self.note("burst_baseline", query=baseline, slices=len(requirements), distinct_requirements=distinct,
                  feeds=sorted({r["feed"] for r in requirements}), image=release_env["QDL_IMAGE_QUERY"])
        bursts = []
        for number in range(1, BURST["bursts"] + 1):
            samples, writer_ready = [], []
            proc = subprocess.Popen(
                ["docker", "run", "--rm", "--name", f"{PROJECT}-burst", "--label", f"{LABEL}=v225", "--network",
                 f"{PROJECT}_executor_network", "--cpus", str(BURST_CPUS), "--memory", "512m", "--read-only",
                 "--user", "0:0", "-v", f"{gen_env['QDL_TLS_TS_IDENTITY_DIR']}:/id:ro",
                 "-v", f"{jwt_src}:/jwt/private.key:ro", "-v", f"{work}:/work:ro", "--entrypoint", "python",
                 release_env["QDL_IMAGE_QUERY"], "-B", "/work/driver.py"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 600
            while proc.poll() is None and time.monotonic() < deadline:
                samples.append(self.query_memory()["usage_mib"])
                writer_ready.append(self.writer_beat()["ready"])
                time.sleep(BURST["sample_s"])
            if proc.poll() is None:
                proc.kill()
            stdout, stderr = proc.communicate(timeout=60)
            try:
                result = json.loads(stdout.strip().splitlines()[-1])
            except (IndexError, ValueError):
                result = {"error": "NO_OUTPUT", "stderr": stderr[-400:]}
            time.sleep(BURST["settle_s"] if number == BURST["bursts"] else BURST["gap_s"])
            after = self.query_memory()
            bursts.append({"burst": number, "driver": result, "peak_usage_mib": max(samples, default=None),
                           "samples": len(samples), "writer_ready_samples": f"{sum(writer_ready)}/{len(writer_ready)}",
                           "after": after})
            self.note("burst", **bursts[-1])
        last = bursts[-1]["after"]
        step = (round(bursts[-1]["after"]["usage_mib"] - bursts[-2]["after"]["usage_mib"], 1)
                if len(bursts) > 1 else None)
        checks = {
            "no_restart": last["restart_count"] == baseline["restart_count"] and last["started_at"] == baseline["started_at"],
            "no_oom_kill": last["oom_kill"] == baseline["oom_kill"],
            "settled_near_baseline": last["usage_mib"] <= baseline["usage_mib"] + BURST_RSS_TOLERANCE_MIB,
            "below_ceiling": last["usage_mib"] < BURST_RSS_CEILING_MIB,
            "no_per_burst_step": step is None or step <= BURST_STEP_MIB}
        return self.note("burst_check", verdict="PASS" if all(checks.values()) else "FAIL", checks=checks,
                         baseline_usage_mib=baseline["usage_mib"], settled_usage_mib=last["usage_mib"],
                         last_step_mib=step, thresholds={"tolerance_mib": BURST_RSS_TOLERANCE_MIB,
                                                         "ceiling_mib": BURST_RSS_CEILING_MIB,
                                                         "step_mib": BURST_STEP_MIB, **BURST},
                         scope="window-config check at rehearsal data volume; production-volume proof = IDR-129 A/B")

    def run(self):
        a = self.args
        self.prepare()
        self.current_release = a.old_release
        # A: deployed generation + v2.2.4 images
        self.note("phase_A_start")
        phase_a_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.phase_up("A_old", a.old_release, a.old_gen, first=True)
        self.current_gen_env = read_env(a.old_gen)
        self.start_writer(read_env(a.old_gen)["QDL_TLS_TS_IDENTITY_DIR"])
        if not self.wait(LIMITS["writer_s"], lambda: self.writer_ready_since(0), poll=5):
            self.note("A_writer_not_ready", writer=self.writer_beat())
            raise RuntimeError("STOP phase A: the writer clone never became ready on the deployed generation")
        self.note("A_writer_ready", writer=self.writer_beat())
        # The production writer runs STRICT; a clean BOOTSTRAP -> STRICT restart on the same identity first.
        self.restart_writer(read_env(a.old_gen)["QDL_TLS_TS_IDENTITY_DIR"], "STRICT")
        strict_a = self.route_timeline(LIMITS["writer_s"], now_ns())
        self.note("A_strict_restart", **strict_a)
        if not strict_a["ready"]:
            raise RuntimeError("STOP phase A: STRICT restart never became ready")
        self.steady("A", a.old_gen)
        self.note("A_refusals", counts=self.refusal_counts(phase_a_iso))
        # B: atomic rotation to the new generation + target images
        self.current_release = a.new_release
        b_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.note("phase_B_start")
        self.atomic_switch("B_rotate", a.new_release, a.new_gen, "STRICT")
        self.steady("B", a.new_gen)
        self.negative_probes("B", a.new_release, a.new_gen, a.old_gen)
        self.note("B_refusals", counts=self.refusal_counts(b_iso))
        # C: rollback to the deployed generation + v2.2.4 images
        self.current_release = a.old_release
        c_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.note("phase_C_start")
        self.atomic_switch("C_rollback", a.old_release, a.old_gen, "STRICT")
        self.steady("C", a.old_gen)
        self.negative_probes("C", a.old_release, a.old_gen, a.new_gen)
        self.note("C_refusals", counts=self.refusal_counts(c_iso))
        # D: roll forward again after the rollback (old -> new -> old -> new): the window can be retried
        self.current_release = a.new_release
        d_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.note("phase_D_start")
        self.atomic_switch("D_reroll", a.new_release, a.new_gen, "STRICT")
        self.steady("D", a.new_gen)
        self.negative_probes("D", a.new_release, a.new_gen, a.old_gen)
        self.note("D_refusals", counts=self.refusal_counts(d_iso))
        if a.burst:
            burst = self.burst_check(a.new_release, a.new_gen)
            self.note("D_refusals_after_burst", counts=self.refusal_counts(d_iso))
            if burst["verdict"] != "PASS":
                raise RuntimeError(f"warmup-burst check FAIL: {burst['checks']}")
        self.note("rehearsal_complete")

    # ------------------------------------------------------------------ teardown
    def down(self):
        removed = {"containers": 0, "networks": 0, "volumes": 0, "dirs": 0}
        if self.clone is not None:
            try:
                self.clone.close()
            except Exception as error:  # noqa: BLE001 - teardown continues
                print(f"clone close: {type(error).__name__}", flush=True)
        ids = sh(["docker", "ps", "-aq", "--filter", f"label={LABEL}=v225"], check=False).stdout.split()
        ids += sh(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}"],
                  check=False).stdout.split()
        ids += sh(["docker", "ps", "-aq", "--filter", f"name=^{PROJECT}-"], check=False).stdout.split()
        ids = sorted(set(ids))
        if ids:
            sh(["docker", "rm", "-fv", *ids], check=False, timeout=180)
        removed["containers"] = len(ids)
        nets = sh(["docker", "network", "ls", "-q", "--filter", f"name={PROJECT}_"], check=False).stdout.split()
        for net in nets:
            sh(["docker", "network", "rm", net], check=False)
        removed["networks"] = len(nets)
        vols = sh(["docker", "volume", "ls", "-q", "--filter", f"name={PROJECT}_"], check=False).stdout.split()
        vols += sh(["docker", "volume", "ls", "-q", "--filter", f"label={LABEL}=v225"], check=False).stdout.split()
        vols = sorted(set(vols))
        for vol in vols:
            sh(["docker", "volume", "rm", vol], check=False)
        removed["volumes"] = len(vols)
        for sub in ("state", "tsstate", "tslogs", "ingestor-config", "burst"):
            path = self.base / sub
            if path.exists():
                sh(["sudo", "-n", "rm", "-rf", "--one-file-system", str(path)], check=False)
                removed["dirs"] += 1
        left = sh(["docker", "ps", "-aq", "--filter", f"name=^{PROJECT}-"], check=False).stdout.split()
        removed["left_containers"] = len(left)
        if self.evidence.exists():
            self.note("teardown", **removed)
        print(json.dumps(removed), flush=True)
        return removed


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--run", type=Path)
    p.add_argument("--down", type=Path)
    p.add_argument("--old-gen", type=Path)
    p.add_argument("--new-gen", type=Path)
    p.add_argument("--old-release", type=Path)
    p.add_argument("--new-release", type=Path)
    p.add_argument("--secrets", type=Path)
    p.add_argument("--lock-owner")
    p.add_argument("--keep", action="store_true", help="do not tear down after the run (diagnosis only)")
    p.add_argument("--burst", action="store_true", help="after phase D: warmup-burst RSS check on the rehearsal Query")
    p.add_argument("--expect-query-image", help="refuse unless the new release env pins this Query image")
    p.add_argument("--ts-root", help=f"TS checkout providing the writer clone loader and schema (default {TS_ROOT})")
    args = p.parse_args(argv)
    if args.down:
        Rehearsal(args.down, args).down()
        return 0
    if not all([args.run, args.old_gen, args.new_gen, args.old_release, args.new_release, args.secrets,
                args.lock_owner]):
        p.error("--run needs --old-gen --new-gen --old-release --new-release --secrets --lock-owner")
    if args.expect_query_image and read_env(args.new_release).get("QDL_IMAGE_QUERY") != args.expect_query_image:
        p.error(f"{args.new_release} does not pin QDL_IMAGE_QUERY={args.expect_query_image}")
    reh = Rehearsal(args.run, args)
    status = 0
    try:
        reh.run()
    except Exception as error:  # noqa: BLE001 - recorded, then teardown
        reh.note("rehearsal_failed", error=type(error).__name__, detail=str(error)[:1500])
        status = 2
    finally:
        if not args.keep:
            reh.down()
    return status


if __name__ == "__main__":
    sys.exit(main())

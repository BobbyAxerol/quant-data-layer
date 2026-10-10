#!/usr/bin/env python3
"""P18.4 G12 / D4-R3: isolated Kafka broker / projector / Stream fault acceptance.

Purpose: run the G12 gate (TS upgrade/p184-phase-d-gates.md) once, in a disposable
namespace: three KRaft brokers (RF3, min ISR 2, mTLS), the production core,
projector, Stream and Query images, a test-provenance BOOK publisher, two TS
writer clones (the TS-owned ``WriterClone``, production entrypoint, 4 workers)
and a sampler that reads each clone's BOOK view through the TS at-use validator.
Faults: stop/start one broker, restart the projector, restart Stream. Per fault
it records the G11 four timestamps (injected, cleared, first valid apply,
caught up) and then G1 transit inside a fixed window.

Reuse, not a new engine: material/frames/raw envelopes come from the DR05 tools
(``p184_dr05_dl_fault``, ``p184_dr05_raw_publisher``), the Query read view is the
DR05/DR08 ``serve_query``, cluster/ISR/committed-offset reads are the D4-R2
``kn_recovery_observe.KafkaAdminSource``, the TS clone is
``scripts/p184_dr05_isolated.WriterClone`` loaded read-only from the TS worktree.

Boundary: every container is ``--rm``, named ``<ns>-<role>`` and labelled
``p184.g12=<ns>``; all join one holder's ``--network none`` namespace (no Docker
network, no published port). The shared stack, shared Redis/Kafka/Postgres and
any container not created here are never touched; the deployed TS writer is only
``docker inspect``-ed (WriterClone's own contract). Production containers'
restart counts are watched as a stop condition. No secret is printed. Synthetic
data carries TEST_ONLY provenance; results are ISOLATED_FAULT rehearsal evidence,
not production or HA evidence.

Host modes (stdlib only): ``--run BASE`` (orchestrate, absolute deadline, teardown
in ``finally``), ``--down BASE`` (exact-name, label-checked teardown),
``--analyze BASE`` (recompute the verdict from the JSONL evidence).

Image set: ``IMAGES`` below are DEFAULTS only. ``--run`` accepts ``--image-set FILE``
(a JSON object role -> sha256:<64hex>, any subset of the roles) and ``--image
ROLE=sha256:<64hex>`` (repeatable, applied last). The resolved set is checked against
the local image IDs, sealed in ``work/handoff.json`` ("g12_images", plus "images" in
the DR05 role names so the in-container ``--material`` step writes the core digest
from it), passed to every in-namespace helper as ``P184_G12_IMAGES`` (checked against
the seal by ``_packet``), and every container this run starts (including restarts and
the WriterClone containers) is ``docker inspect``-ed: its image ID must equal the
passed value for its role (``evidence/provenance.jsonl``). ``--query-code image``
runs Query from the image's own /app (no worktree mount); the default ``worktree``
keeps the earlier read-only worktree mount.
In-container modes (python image, inside the namespace): ``--topics``,
``--publish``, ``--monitor``; each bounded by ``--seconds`` / its own deadline.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LABEL = "p184.g12"
NS_PATTERN = r"p184-g12-dl-[0-9a-f]{16}"
TS_ROOT = Path("/home/bobby/.worktrees/trading-system-next-upgrade")
TS_WRITER = "market_data_service_p184w"
IMAGES = {
    "kafka": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
    "core": "sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023",
    "projector": "sha256:a696cfba6ab383173607f96413c814f74c7abcc98a4c41f57c9c6e78a41bb70b",
    "stream": "sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9",
    "python": "sha256:672503b7fe87265d12af3d45fc4d344fdef0a76e94e06efe52e8dc59cab24d0a",
    "redis": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
    "ts": "sha256:15524c0bea8c51949d7ced79291d4c72722292fea2f0a5d4f8e9c7fd61dbb5ca",
    "postgres": "sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685",
    "holder": "sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc",
}
BROKERS = (1, 2, 3)
CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"
BOOTSTRAP = ",".join(f"localhost:{9092 + 100 * (n - 1)}" for n in BROKERS)
VENUES = ("BINANCE", "OKX")
CLONE_PORTS = {"BINANCE": (6391, 5441), "OKX": (6392, 5442)}
PROJECTOR_GROUPS = ("kn-projector-v3-a", "kn-projector-v3-b")
# Frozen before the run (packet sections 5-6).
GATE = {"caught_up_s": 600, "age_ms": 1000, "hold_s": 30, "window_s": 300,
        "p95_ms": 500, "p99_ms": 1000, "max_ms": 2000, "p99_min_n": 100}
LIMITS = {"disk_free_bytes": 20 * 10**9, "mem_available_bytes": 3 * 2**30,
          "deadline_s": 3600, "holder_s": 5400, "broker_outage_s": 60,
          "role_return_s": 120, "clone_ready_s": 300, "baseline_s": 300}
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
QUERY_CODE = ("worktree", "image")
# G12 role -> DR05/DR08 image role (handoff "images"; dr05 image_set() requires exactly these keys).
DR05_IMAGE_ROLES = {"query": "python", "core": "core", "stream": "stream", "python": "python",
                    "kafka": "kafka", "redis": "redis"}
PRIVATE_FILES = ("client.key", "server.p12", "binance.key", "okx.key", "cursor-keys.json",
                 "read-view.secret", "broker.properties", "broker-1.properties",
                 "broker-2.properties", "broker-3.properties")


def resolve_image_set(image_set=None, overrides=()):
    """IMAGES (defaults) <- JSON file/object <- ROLE=ID overrides; every role an immutable image ID."""
    images = dict(IMAGES)
    layers = []
    if image_set is not None:
        loaded = image_set if isinstance(image_set, dict) else json.loads(Path(image_set).read_text())
        if not isinstance(loaded, dict):
            raise ValueError("image set file must be a JSON object role -> sha256:<64hex>")
        layers.append(loaded)
    parsed = {}
    for value in overrides or ():
        role, sep, image = str(value).partition("=")
        if not sep or role in parsed:
            raise ValueError(f"--image ROLE=sha256:<64hex>, once per role: {value!r}")
        parsed[role] = image
    layers.append(parsed)
    for layer in layers:
        for role, image in layer.items():
            if role not in IMAGES:
                raise ValueError(f"unknown image role {role!r}; roles: {sorted(IMAGES)}")
            images[role] = image
    for role, image in images.items():
        if not isinstance(image, str) or not IMAGE_ID.fullmatch(image):
            raise ValueError(f"immutable {role} image ID required (sha256:<64hex>)")
    return images


def dr05_images(images):
    """The sealed set in DR05's role names (handoff.json "images")."""
    return {role: images[g12_role] for role, g12_role in DR05_IMAGE_ROLES.items()}


def role_image_key(role):
    """Which image-set role a container role runs."""
    base = re.sub(r"-(binance|okx)$", "", role)
    return {"net": "holder", "quota": "redis", "query": "python", "helper": "python", "monitor": "python",
            "publisher": "python", "sampler": "ts", "tswriter": "ts", "tsredis": "redis",
            "tspg": "postgres"}.get(base, "kafka" if base.startswith("kafka-") else base)


def namespace(value):
    if not isinstance(value, str) or not re.fullmatch(NS_PATTERN, value):
        raise ValueError("exact disposable G12 namespace required")
    return value


def broker_port(node):
    if node not in BROKERS:
        raise ValueError("unknown broker")
    return 9092 + 100 * (node - 1)


def topics(ns):
    """name -> (partitions, config). RF3 and min ISR 2 everywhere (production shape)."""
    namespace(ns)
    data = {"min.insync.replicas": "2"}
    state = {**data, "cleanup.policy": "compact", "delete.retention.ms": "604800000"}
    return {ns + ".raw": (1, data), ns + ".canonical": (1, data), ns + ".quarantine": (1, data),
            ns + ".latest": (3, state), ns + ".bars": (3, {**state, "min.compaction.lag.ms": "3600000"})}


def broker_password(text):
    """The keystore password DR05 material wrote; never logged."""
    found = re.findall(r"^ssl\.keystore\.password=(\S+)$", text, flags=re.M)
    if len(found) != 1:
        raise ValueError("exactly one keystore password required")
    return found[0]


def broker_properties(password, node):
    port = broker_port(node)
    voters = ",".join(f"{n}@localhost:{broker_port(n) + 1}" for n in BROKERS)
    return f"""process.roles=broker,controller
node.id={node}
controller.quorum.voters={voters}
listeners=SSL://:{port},CONTROLLER://:{port + 1}
advertised.listeners=SSL://localhost:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/server.p12
ssl.keystore.password={password}
ssl.key.password={password}
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca.crt
ssl.client.auth=required
log.dirs=/data
log.segment.bytes=16777216
metadata.log.segment.bytes=16777216
log.index.size.max.bytes=1048576
num.partitions=1
default.replication.factor=3
min.insync.replicas=2
offsets.topic.num.partitions=1
offsets.topic.replication.factor=3
transaction.state.log.num.partitions=1
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
"""


def _base(ns, role, *, network, memory, cpus, readonly=True, uid=None):
    namespace(ns)
    argv = ["docker", "run", "-d", "--rm", "--pull=never", "--name", f"{ns}-{role}",
            "--label", f"{LABEL}={ns}", "--network", network, "--memory", memory, "--cpus", cpus,
            "--pids-limit", "256", "--log-opt", "max-size=4m", "--log-opt", "max-file=1"]
    if readonly:
        argv.append("--read-only")
    if uid is not None:
        argv += ["--user", uid]
    return argv


def _env(argv, env):
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    return argv


def topology(ns, work, topic_id="", *, uid=None, images=None, query_code="worktree"):
    """Docker argv per role (adapted from p184_dr05_dl_fault.topology: 3 brokers,
    a projector, one holder-owned loopback namespace). Executes nothing."""
    namespace(ns)
    images = images or IMAGES
    if query_code not in QUERY_CODE:
        raise ValueError("query_code must be one of " + ",".join(QUERY_CODE))
    work = Path(work).resolve()
    if work == ROOT or ROOT in work.parents or any(c in str(work) for c in (":", ",")):
        raise ValueError("external unambiguous work directory required")
    uid = uid or f"{os.getuid()}:{os.getgid()}"
    net = "container:" + ns + "-net"
    tls = {"QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
           "QDL_KN_KAFKA_KEY": "/work/client.key"}
    common = {"QDL_ENVIRONMENT": "paper", "QDL_KN_CURSOR_KEYS_FILE": "/work/cursor-keys.json",
              "QDL_KN_CURSOR_ACTIVE_KEY_ID": "dr08", "QDL_KN_TOPIC_ID": topic_id,
              "QDL_KN_ROUTE_GENERATION": ns, "QDL_KN_READ_VIEW_SECRET_FILE": "/work/read-view.secret"}
    mount = ["-v", f"{work}:/work"]
    # The holder's hostname is the namespace's: Kafka/log4j resolve it (DR05 kafka: --hostname localhost).
    roles = {"net": _base(ns, "net", network="none", memory="16m", cpus="0.05")
             + ["--hostname", "localhost", "--entrypoint", "sleep", images["holder"], str(LIMITS["holder_s"])]}
    for node in BROKERS:
        argv = _base(ns, f"kafka-{node}", network=net, memory="1024m", cpus="0.75", uid=uid)
        argv += ["--tmpfs", "/tmp:rw,exec,size=64m,mode=1777", "-v", f"{work}/kafka-{node}:/data",
                 "-v", f"{work}:/work:ro"]
        for target in ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data"):
            argv += ["--tmpfs", target + ":rw,size=1m,mode=1777"]  # no anonymous volumes
        _env(argv, {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": "/tmp/kafka-logs"})
        props = f"/work/broker-{node}.properties"
        roles[f"kafka-{node}"] = argv + ["--entrypoint", "/bin/sh", images["kafka"], "-ec",
            f"/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t {CLUSTER_ID} -c {props}"
            f" && exec /opt/kafka/bin/kafka-server-start.sh {props}"]
    roles["quota"] = _base(ns, "quota", network=net, memory="192m", cpus="0.1", uid=uid) + [
        "--tmpfs", "/data:rw,size=1m,mode=1777", "--entrypoint", "redis-server", images["redis"],
        "--save", "", "--appendonly", "no", "--maxmemory", "128mb", "--maxmemory-policy", "noeviction"]
    roles["core"] = _env(_base(ns, "core", network=net, memory="256m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
        {"QDL_KAFKA_CERT_ROOT": "/work", "QDL_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
         "QDL_KAFKA_CLIENT_ID": ns, "QDL_KAFKA_GROUP_ID": ns, "QDL_PROVIDER_ADMISSION_ENABLED": "false"}) + [
        "--entrypoint", "/usr/local/bin/qdl-realtime-core", images["core"], "/work/core.json"]
    roles["projector"] = _env(_base(ns, "projector", network=net, memory="256m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
        {**tls, "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP, "QDL_KN_BUNDLE_PATH": "/work/bundle.json",
         "QDL_KN_TOPIC_ID": topic_id, "QDL_KN_REPLICA": "a", "QDL_KN_MATERIALIZER_EPOCH": "1",
         "QDL_KN_MARKET_CACHE_URL": "redis://localhost:6379/1",
         "QDL_KN_STATUS_PATH": "/work/g12-projector-status.json", "QDL_KN_STATUS_INTERVAL_S": "5",
         "QDL_KN_CANONICAL_TOPIC": ns + ".canonical", "QDL_KN_LATEST_TOPIC": ns + ".latest",
         "QDL_KN_BARS_TOPIC": ns + ".bars"}) + [
        "--entrypoint", "/usr/local/bin/qdl-projector", images["projector"], "run"]
    roles["stream"] = _env(_base(ns, "stream", network=net, memory="512m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
        {**common, **tls, "QDL_KN_BUNDLE_FILE": "/work/bundle.json",
         "QDL_KN_JWT_CONFIG_FILE": "/work/jwt-config.json",
         "QDL_KN_QUOTA_REDIS_URL": "redis://localhost:6379/0", "QDL_KN_QUOTA_PREFIX": "qdl:stable:v2:paper:" + ns,
         "QDL_KN_TLS_CERT_FILE": "/work/client.crt", "QDL_KN_TLS_KEY_FILE": "/work/client.key",
         "QDL_KN_TLS_CLIENT_CA_FILE": "/work/ca.crt", "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP,
         "QDL_KN_KAFKA_TOPIC": ns + ".canonical", "QDL_KN_KAFKA_GROUP": "kn-" + ns,
         "QDL_KN_KAFKA_CLIENT_ID": ns + "-stream", "QDL_KN_READ_VIEW_URLS": "https://localhost:8200",
         "QDL_KN_READ_VIEW_CA_FILE": "/work/ca.crt", "QDL_KN_HOT_READ_ENABLED": "true",
         "QDL_KN_METRICS_SECONDS": "10", "QDL_KN_MAX_REPLAY_EVENTS": "10000",
         "QDL_KN_QUEUE_BYTES_TOTAL": "33554432"}) + [
        "--entrypoint", "/usr/local/bin/qdl-stream-gateway", images["stream"], "serve"]
    # "image": the Query code is the image's own /app (the release under test); "worktree": read-only mount.
    code = ["-w", "/app"] if query_code == "image" else ["-v", f"{ROOT}:/app:ro", "-w", "/app"]
    roles["query"] = _env(_base(ns, "query", network=net, memory="512m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount + code,
        {**common, "PYTHONPATH": "/app:/app/generated/python", "PYTHONDONTWRITEBYTECODE": "1"}) + [
        "--entrypoint", "python", images["python"], "-B", "scripts/p184_dr05_observer.py", "--query", "/work"]
    return roles


SERVICE_ORDER = ("quota", "core", "projector", "stream", "query")
TEARDOWN_ORDER = ("sampler", "publisher", "monitor", "query", "stream", "projector", "core", "quota",
                  "kafka-3", "kafka-2", "kafka-1", "helper", "net")


def helper_argv(ns, work, args, *, role="helper", detach=False, evidence=None, network=None,
                cpus="0.5", memory="512m", script="scripts/p184_g12_isolated_fault.py", extra=(), images=None):
    namespace(ns)
    images = images or IMAGES
    argv = _base(ns, role, network=network or "container:" + ns + "-net", memory=memory, cpus=cpus,
                 uid=f"{os.getuid()}:{os.getgid()}")
    if not detach:
        argv.remove("-d")
    argv += ["--tmpfs", "/tmp:rw,size=64m,mode=1777", "-v", f"{Path(work).resolve()}:/work",
             "-v", f"{ROOT}:/app:ro", "-w", "/app", "-e", "PYTHONPATH=/app:/app/generated/python",
             "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "P184_G12_NAMESPACE=" + ns,
             "-e", "P184_G12_IMAGES=" + json.dumps(images, sort_keys=True, separators=(",", ":")), *extra]
    if evidence is not None:
        argv += ["-v", f"{Path(evidence).resolve()}:/evidence"]
    return argv + ["--entrypoint", "python", images["python"], "-B", script, *args]


SAMPLER = r'''
import asyncio, json, signal, sys, time
from redis.asyncio import Redis
from services.market_data.cache_reader import MarketCacheReader
from services.risk_engine.execution_market_context import _mapping, _missing_reason, _validate_book
from domain.instrument_identity import CanonicalInstrumentId
IDS = {"BINANCE": ("BINANCE", "USD_M", "ETHUSDT", "ETHUSDT"), "OKX": ("OKX", "SWAP", "ETHUSDT", "ETH-USDT-SWAP")}
STOP = []
signal.signal(signal.SIGTERM, lambda *_: STOP.append(1))
async def view(r, venue):
    now_ms = time.time_ns() // 1_000_000
    c = await MarketCacheReader(r).execution_market_context(CanonicalInstrumentId(*IDS[venue]),
                                                           required_feeds=frozenset({"BOOK_SNAPSHOT"}))
    book = (c or {}).get("book") or {}
    seq = book.get("sequence") or book.get("native_sequence_end")
    try:
        _validate_book(_mapping((c or {}).get("book"), reason=_missing_reason(c or {}, "book",
                       "EXECUTION_CONTEXT_BOOK_MISSING")), now_ms=now_ms, max_age_ms=2000, side="BUY",
                       require_levels=True)
        verdict = "APPROVED"
    except Exception as e:
        verdict = (type(e).__name__ + ":" + str(e))[:160]
    try:
        seq = None if seq is None else int(seq)
    except (TypeError, ValueError):
        seq = None
    return seq, verdict
async def beat(r):
    keys = [k async for k in r.scan_iter(match="service:heartbeat:market_data:*", count=100)]
    if not keys:
        return {"heartbeat": False}
    b = json.loads(await r.get(keys[0]) or "{}"); d = b.get("details") or {}
    return {"heartbeat": True, "status": b.get("status"), "demanded": d.get("demanded_v2_slices"),
            "session_ready": d.get("session_ready_v2_slices")}
async def main(out, seconds, interval_ms, targets):
    views = {v: Redis.from_url(u, socket_timeout=1, socket_connect_timeout=1) for v, u in targets.items()}
    beats = {v: Redis.from_url(u.rsplit("/", 1)[0] + "/2", socket_timeout=1, socket_connect_timeout=1,
                               decode_responses=True) for v, u in targets.items()}
    end = time.monotonic() + seconds
    last, last_t, last_beat = {}, {}, 0.0
    with open(out, "a", buffering=1) as f:
        while time.monotonic() < end and not STOP:
            for venue, r in views.items():
                t = time.time_ns()
                try:
                    seq, verdict = await view(r, venue)
                    row = {"k": "view", "t": t, "venue": venue, "seq": seq, "v": verdict}
                except Exception as e:
                    row = {"k": "view", "t": t, "venue": venue, "seq": None,
                           "v": "READ_ERROR:" + type(e).__name__}
                key = (row["seq"], row["v"])
                if last.get(venue) != key or t - last_t.get(venue, 0) >= 500_000_000:
                    f.write(json.dumps(row) + "\n"); last[venue] = key; last_t[venue] = t
            if time.monotonic() - last_beat >= 2:
                last_beat = time.monotonic()
                for venue, r in beats.items():
                    try:
                        row = await beat(r)
                    except Exception as e:
                        row = {"heartbeat": None, "error": type(e).__name__}
                    f.write(json.dumps({"k": "hb", "t": time.time_ns(), "venue": venue, **row}) + "\n")
            await asyncio.sleep(interval_ms / 1000)
    for r in (*views.values(), *beats.values()):
        await r.aclose()
asyncio.run(main(sys.argv[1], float(sys.argv[2]), float(sys.argv[3]),
                 dict(item.split("=", 1) for item in sys.argv[4].split(","))))
'''


def sampler_argv(ns, evidence, seconds, interval_ms=50, images=None):
    images = images or IMAGES
    targets = ",".join(f"{v}=redis://localhost:{CLONE_PORTS[v][0]}/1" for v in VENUES)
    argv = _base(ns, "sampler", network="container:" + ns + "-net", memory="384m", cpus="0.25",
                 uid=f"{os.getuid()}:{os.getgid()}")
    return argv + ["--tmpfs", "/tmp:rw,size=16m,mode=1777", "-v", f"{Path(evidence).resolve()}:/evidence",
                   "-e", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", "python", images["ts"], "-B", "-c",
                   SAMPLER, "/evidence/sampler.jsonl", str(int(seconds)), str(interval_ms), targets]


# ---------------------------------------------------------------- analysis (pure)

def load_jsonl(path):
    rows = []
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # a torn last line while the writer is live
    except FileNotFoundError:
        pass
    return rows


def publish_index(rows):
    """(venue, sequence) -> publish ns (the frame's own event time)."""
    return {(r["venue"], r["seq"]): r["t"] for r in rows if r.get("k") == "pub"}


def view_series(rows):
    series = {}
    for r in rows:
        if r.get("k") == "view":
            series.setdefault(r["venue"], []).append((r["t"], r.get("seq"), r.get("v") == "APPROVED"))
    for values in series.values():
        values.sort(key=lambda item: item[0])
    return series


def caught_up(series, index, venue, cleared_ns, *, age_ms=GATE["age_ms"], hold_s=GATE["hold_s"]):
    """First valid apply and stable caught-up for one venue after ``cleared_ns``.

    A row counts when the view is APPROVED, shows a frame published at/after
    cleared_ns, and its age (row time - publish time) is <= age_ms; caught-up
    needs every row of the following hold_s to count as well, and rows that
    reach past the hold (otherwise: not yet decidable -> None).
    """
    rows = [r for r in series.get(venue, ()) if r[0] >= cleared_ns]
    first_valid = None
    age_ns, hold_ns = age_ms * 1_000_000, int(hold_s * 1e9)

    def fresh(row):
        published = index.get((venue, row[1]))
        return (row[2] and published is not None and published >= cleared_ns
                and row[0] - published <= age_ns)

    for t, seq, ok in rows:
        published = index.get((venue, seq))
        if ok and published is not None and published >= cleared_ns:
            first_valid = t
            break
    i = 0
    while i < len(rows):
        if not fresh(rows[i]):
            i += 1
            continue
        start, j, broken = rows[i][0], i, None
        while j < len(rows) and rows[j][0] <= start + hold_ns:
            if not fresh(rows[j]):
                broken = j
                break
            j += 1
        if broken is not None:
            i = broken + 1
            continue
        if j >= len(rows):
            return {"first_valid_apply_ns": first_valid, "caught_up_ns": None}
        return {"first_valid_apply_ns": first_valid, "caught_up_ns": start}
    return {"first_valid_apply_ns": first_valid, "caught_up_ns": None}


def first_seen(series, venue):
    seen = {}
    for t, seq, ok in series.get(venue, ()):
        if seq is not None and ok and seq not in seen:
            seen[seq] = t
    return seen


def transit(series, index, venue, start_ns, end_ns):
    """ms from publish to the first APPROVED sample showing each frame, first seen in the window."""
    out = []
    for seq, t in first_seen(series, venue).items():
        published = index.get((venue, seq))
        if published is not None and start_ns <= t < end_ns:
            out.append((t - published) / 1e6)
    return out


def quantile(sorted_values, q):
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (position - low)


def g1_stats(values):
    values = sorted(values)
    n = len(values)
    stats = {"n": n, "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
             "p99_ms": quantile(values, .99) if n >= GATE["p99_min_n"] else None,
             "max_ms": values[-1] if values else None}
    stats["pass"] = bool(n and stats["p95_ms"] <= GATE["p95_ms"] and stats["max_ms"] < GATE["max_ms"]
                         and (stats["p99_ms"] is None or stats["p99_ms"] <= GATE["p99_ms"]))
    if n and stats["p99_ms"] is None:
        stats["note"] = "p99 not reported (n < 100)"
    return {k: (round(v, 3) if isinstance(v, float) else v) for k, v in stats.items()}


def cleared_at(kind, monitor_rows, restart_ns, node=None):
    """Fault-cleared time from the monitor (packet section 5)."""
    for row in monitor_rows:
        if row.get("t", 0) < restart_ns:
            continue
        if kind == "broker" and node in ((row.get("cluster") or {}).get("broker_ids") or ()):
            return row["t"]
        if kind == "projector" and ((row.get("groups") or {}).get("kn-projector-v3-a") or {}).get("members", 0) >= 1:
            return row["t"]
        if kind == "stream" and (row.get("ports") or {}).get("8210") is True:
            return row["t"]
    return None


def first_monitor(monitor_rows, start_ns, predicate):
    for row in monitor_rows:
        if row.get("t", 0) >= start_ns and predicate(row):
            return row["t"]
    return None


def evaluate_fault(fault, publish_rows, sampler_rows, monitor_rows, *, window_s=GATE["window_s"]):
    index, series = publish_index(publish_rows), view_series(sampler_rows)
    cleared = cleared_at(fault["kind"], monitor_rows, fault["restart_ns"], fault.get("node"))
    result = {"fault": fault["name"], "kind": fault["kind"], "node": fault.get("node"),
              "fault_injected_ns": fault["injected_ns"], "restart_issued_ns": fault["restart_ns"],
              "fault_cleared_ns": cleared, "venues": {}, "pass": False}
    if cleared is None:
        result["reason"] = "FAULT_NOT_CLEARED"
        return result
    ends = []
    for venue in VENUES:
        state = caught_up(series, index, venue, cleared)
        state["caught_up_s"] = (None if state["caught_up_ns"] is None
                                else round((state["caught_up_ns"] - cleared) / 1e9, 3))
        rows = [r for r in series.get(venue, ()) if fault["injected_ns"] <= r[0] <= (state["caught_up_ns"] or r[0])]
        ages = [(t - index[(venue, s)]) / 1e6 for t, s, ok in rows if (venue, s) in index]
        state["max_view_age_ms_injected_to_caught_up"] = round(max(ages), 3) if ages else None
        state["not_approved_samples_injected_to_caught_up"] = sum(1 for _, _, ok in rows if not ok)
        result["venues"][venue] = state
        ends.append(state["caught_up_ns"])
    if any(end is None for end in ends):
        result["reason"] = "NOT_CAUGHT_UP"
        return result
    caught = max(ends)
    result["consumers_caught_up_ns"] = caught
    result["caught_up_s"] = round((caught - cleared) / 1e9, 3)
    window = (caught, caught + int(window_s * 1e9))
    result["g1_window_ns"] = list(window)
    result["g1"] = {venue: g1_stats(transit(series, index, venue, *window)) for venue in VENUES}
    last = max((r["t"] for r in sampler_rows if r.get("k") == "view"), default=0)
    complete = last >= window[1]
    result["g1_window_complete"] = complete
    # Committed offset vs LSO keeps 1 for the transaction's commit marker in steady state
    # (run 2026-10-10: lag 1 in 921 of 1,257 samples, 0 only twice), so "steady" is <= 1.
    result["projector_lag_steady_ns"] = first_monitor(
        monitor_rows, cleared, lambda r: r.get("projector_lag") is not None and r["projector_lag"] <= 1)
    result["isr_restored_ns"] = first_monitor(monitor_rows, cleared, lambda r: r.get("under_replicated") == 0)
    result["g12_caught_up_pass"] = result["caught_up_s"] <= GATE["caught_up_s"]
    result["pass"] = (result["g12_caught_up_pass"] and complete
                      and all(result["g1"][v]["pass"] for v in VENUES))
    return result


def evaluate(evidence):
    evidence = Path(evidence)
    publish_rows = load_jsonl(evidence / "publish.jsonl")
    sampler_rows = load_jsonl(evidence / "sampler.jsonl")
    monitor_rows = sorted(load_jsonl(evidence / "monitor.jsonl"), key=lambda r: r.get("t", 0))
    faults = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "fault"]
    index, series = publish_index(publish_rows), view_series(sampler_rows)
    out = {"schema": "p184.g12.verdict.v1", "provenance": "TEST_ONLY", "level": "ISOLATED_FAULT",
           "gate": GATE, "faults": [], "g2": "NOT_MEASURED (BOOK-only isolated source)",
           "g3": "NOT_MEASURED (BOOK-only isolated source)"}
    base = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "baseline"]
    if base:
        window = (base[0]["start_ns"], base[0]["end_ns"])
        out["baseline_g1"] = {v: g1_stats(transit(series, index, v, *window)) for v in VENUES}
    for fault in faults:
        out["faults"].append(evaluate_fault(fault, publish_rows, sampler_rows, monitor_rows))
    acks = [r for r in publish_rows if r.get("k") == "ack"]
    out["publisher"] = {"frames": sum(1 for r in publish_rows if r.get("k") == "pub"),
                        "acked": sum(1 for r in acks if "error" not in r),
                        "delivery_errors": sum(1 for r in acks if "error" in r),
                        "resyncs": sum(1 for r in publish_rows if r.get("k") == "resync")}
    out["pass"] = bool(out["faults"]) and len(out["faults"]) == 3 and all(f["pass"] for f in out["faults"])
    return out


# ---------------------------------------------------------------- in-container modes

def _packet(work):
    work = Path(work)
    packet = json.loads((work / "packet.json").read_text())
    ns = namespace(packet["namespace"])
    if os.environ.get("P184_G12_NAMESPACE") != ns or packet.get("bootstrap") != BOOTSTRAP:
        raise ValueError("G12 namespace attestation and isolated bootstrap required")
    check_sealed_images(work, os.environ.get("P184_G12_IMAGES"))
    return packet


def check_sealed_images(work, attested):
    """In-container: the parent's passed image set must equal the run's sealed set."""
    handoff = Path(work) / "handoff.json"
    sealed = json.loads(handoff.read_text()).get("g12_images") if handoff.is_file() else None
    if sealed is None and attested is None:
        return None  # a run sealed before the image set was a parameter
    if sealed is None or attested is None or resolve_image_set(json.loads(attested)) != resolve_image_set(sealed):
        raise ValueError("G12 image set attestation does not match the sealed run image set")
    return sealed


def _security(packet):
    from scripts.p184_dr08_isolated import kafka_security
    return kafka_security(packet)


def create_topics(work):
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import TopicCollection
    packet = _packet(work)
    admin = AdminClient(_security(packet))
    deadline = time.monotonic() + 120
    while True:
        try:
            if len(admin.describe_cluster(request_timeout=3).result(timeout=4).nodes) == len(BROKERS):
                break
        except Exception:  # noqa: BLE001 - not ready yet; the deadline decides
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("three-broker cluster readiness")
        time.sleep(1)
    wanted = topics(packet["namespace"])
    futures = admin.create_topics([NewTopic(name, parts, len(BROKERS), config=dict(config))
                                   for name, (parts, config) in wanted.items()], request_timeout=30)
    for future in futures.values():
        future.result(40)
    canonical = packet["namespace"] + ".canonical"
    described = describe_until_known(lambda: admin.describe_topics(TopicCollection([canonical]))[canonical]
                                     .result(20))
    packet["slice"]["topic_id"] = str(described.topic_id)
    (Path(work) / "packet.json").write_text(json.dumps(packet))
    return {"topics": sorted(wanted), "replication_factor": len(BROKERS), "min_isr": 2}


def describe_until_known(describe, seconds=30.0, pause=1.0):
    """A topic just created may not be in every broker's metadata yet (final-image run
    2026-10-10, attempt 1: UNKNOWN_TOPIC_OR_PART right after create); retry only that, bounded."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return describe()
        except Exception as error:  # noqa: BLE001 - only the propagation race is retried
            if "UNKNOWN_TOPIC_OR_PART" not in str(error) or time.monotonic() >= deadline:
                raise
        time.sleep(pause)


class _Stop:
    def __init__(self):
        self.requested = False
        signal.signal(signal.SIGTERM, self._set)

    def _set(self, *_):
        self.requested = True


def publish(work, out, seconds, hz):
    """Steady provider-shaped BOOK frames per venue; resync (new generation +
    snapshot) only after a failed delivery, as a venue adapter would."""
    from confluent_kafka import Producer
    from scripts.p184_dr05_dl_fault import frame
    from scripts.p184_dr05_raw_publisher import raw_envelope
    packet = _packet(work)
    ns = packet["namespace"]
    config = json.loads((Path(work) / "core.json").read_text())
    if config.get("raw_topics") != [ns + ".raw"] or config["core"].get("allow_test_provenance") is not True:
        raise ValueError("isolated test-only raw topic required")
    bindings = {b["venue"]: b for b in config["core"]["bindings"]}
    if set(bindings) != set(VENUES) or not 0 < hz <= 20 or not 0 < seconds <= LIMITS["deadline_s"]:
        raise ValueError("two-venue roster and bounded rate/duration required")
    producer = Producer({**_security(packet), "enable.idempotence": True, "acks": "all",
                         "linger.ms": 0, "message.timeout.ms": 30000, "queue.buffering.max.messages": 10000})
    state = {v: {"gen": 7, "seq": 1000, "snapshot": True} for v in VENUES}
    stop, end, period = _Stop(), time.monotonic() + seconds, 1.0 / hz
    with open(out, "a", buffering=1) as log:
        def ack(venue, seq, gen, sent_ns):
            def callback(error, message):
                row = {"k": "ack", "venue": venue, "seq": seq, "gen": gen,
                       "ack_ms": round((time.time_ns() - sent_ns) / 1e6, 3)}
                if error is not None:
                    row["error"] = str(error.code())
                    if state[venue]["gen"] == gen:
                        state[venue]["gen"] += 1
                        state[venue]["snapshot"] = True
                        log.write(json.dumps({"k": "resync", "t": time.time_ns(), "venue": venue,
                                              "after_seq": seq, "new_gen": gen + 1}) + "\n")
                else:
                    row["offset"] = message.offset()
                log.write(json.dumps(row) + "\n")
            return callback

        next_at = time.monotonic()
        while time.monotonic() < end and not stop.requested:
            for venue in VENUES:
                s = state[venue]
                snapshot = s["snapshot"]
                seq = s["seq"] + 1
                at_ns = time.time_ns()
                raw = raw_envelope(bindings[venue], frame(venue, snapshot, seq, 0 if snapshot else s["seq"],
                                                          s["gen"], str(seq % 1000 + 1), at_ns), ns)
                try:
                    producer.produce(config["raw_topics"][0], partition=0, key=raw.subscription_id.encode(),
                                     value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                                     on_delivery=ack(venue, seq, s["gen"], at_ns))
                except BufferError:
                    log.write(json.dumps({"k": "ack", "venue": venue, "seq": seq, "gen": s["gen"],
                                          "error": "LOCAL_QUEUE_FULL"}) + "\n")
                    s["gen"] += 1
                    s["snapshot"] = True
                    continue
                s["seq"], s["snapshot"] = seq, False
                log.write(json.dumps({"k": "pub", "t": at_ns, "venue": venue, "seq": seq, "gen": s["gen"],
                                      "snapshot": snapshot}) + "\n")
            next_at += period
            while True:
                producer.poll(0)
                left = next_at - time.monotonic()
                if left <= 0:
                    break
                time.sleep(min(left, .02))
        remaining = producer.flush(10)
        log.write(json.dumps({"k": "end", "t": time.time_ns(), "unflushed": remaining}) + "\n")
    return {"stopped": True}


def monitor(work, out, seconds, period=1.0):
    """Read-only 1 s observation: cluster/ISR, projector committed vs LSO,
    group membership, Query/Stream ports, disk. No group join or commit."""
    import socket
    from confluent_kafka.admin import AdminClient
    from scripts.kn_recovery_observe import Deadline, KafkaAdminSource
    packet = _packet(work)
    ns = packet["namespace"]
    admin = AdminClient(_security(packet))
    source = KafkaAdminSource(admin)
    roster = topics(ns)
    rosters = {"kn-projector-v3-a": {ns + ".canonical": roster[ns + ".canonical"][0]},
               "kn-projector-v3-b": {ns + ".latest": roster[ns + ".latest"][0],
                                     ns + ".bars": roster[ns + ".bars"][0]}}
    groups = (*PROJECTOR_GROUPS, "kn-" + ns, ns)
    stop, end = _Stop(), time.monotonic() + seconds
    status_path = Path(work) / "g12-projector-status.json"
    with open(out, "a", buffering=1) as log:
        while time.monotonic() < end and not stop.requested:
            started = time.monotonic()
            row = {"t": time.time_ns()}
            deadline = Deadline(3.0)
            try:
                row["cluster"] = source.describe_cluster(deadline)
            except Exception as error:  # noqa: BLE001 - recorded, never inferred
                row["cluster_error"] = type(error).__name__
            try:
                described = source.describe_topics(sorted(roster), Deadline(3.0))
                parts = [p for values in described.values() for p in values]
                row["under_replicated"] = sum(1 for p in parts if len(p["isr"]) < len(p["replicas"]))
                row["offline"] = sum(1 for p in parts if p["leader"] is None)
                row["leaders"] = {f"{t}/{p['partition']}": p["leader"] for t, ps in described.items() for p in ps}
                row["min_isr_size"] = min(len(p["isr"]) for p in parts)
            except Exception as error:  # noqa: BLE001
                row["topics_error"] = type(error).__name__
            try:
                lag = {}
                for group, group_roster in rosters.items():
                    committed = source.committed_offsets(group, group_roster, Deadline(3.0))
                    ends = source.log_end_offsets(sorted(committed), Deadline(3.0))
                    lag[group] = sum(max(0, ends[tp] - max(committed[tp], 0)) for tp in committed)
                row["lag"] = lag
                row["projector_lag"] = lag["kn-projector-v3-a"]
            except Exception as error:  # noqa: BLE001
                row["lag_error"] = type(error).__name__
            try:
                described = admin.describe_consumer_groups(list(groups), request_timeout=3)
                row["groups"] = {}
                for group, future in described.items():
                    value = future.result(timeout=4)
                    row["groups"][group] = {"state": str(getattr(value, "state", "")).split(".")[-1],
                                            "members": len(value.members or ())}
            except Exception as error:  # noqa: BLE001
                row["groups_error"] = type(error).__name__
            row["ports"] = {}
            for port in (8200, 8210):
                try:
                    with socket.create_connection(("localhost", port), timeout=.5):
                        row["ports"][str(port)] = True
                except OSError:
                    row["ports"][str(port)] = False
            usage = shutil.disk_usage("/work")
            row["disk_free_bytes"] = usage.free
            try:
                stat = status_path.stat()
                row["projector_status_mtime"] = stat.st_mtime
            except FileNotFoundError:
                row["projector_status_mtime"] = None
            log.write(json.dumps(row, default=list) + "\n")
            time.sleep(max(0.0, period - (time.monotonic() - started)))
    return {"stopped": True}


# ---------------------------------------------------------------- host orchestration

def sh(argv, timeout=60, check=True):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{argv[:4]} -> {result.returncode}: {result.stderr.strip()[-400:]}")
    return result.stdout


def owned_names(ns):
    """Exact names of running/stopped containers carrying this namespace's label."""
    namespace(ns)
    out = sh(["docker", "ps", "-a", "--filter", f"label={LABEL}={ns}", "--format", "{{.Names}}"])
    names = [n for n in out.split() if n]
    if any(not n.startswith(ns + "-") for n in names):
        raise RuntimeError("label/name ownership mismatch")
    return names


def running_names(ns):
    out = sh(["docker", "ps", "--filter", f"label={LABEL}={ns}", "--format", "{{.Names}}"])
    return set(out.split())


def stop_role(ns, role, timeout=30):
    """Stop one owned --rm container and wait until it is gone (identity checked)."""
    name = f"{ns}-{role}"
    rows = json.loads(sh(["docker", "inspect", name], check=False) or "[]")
    if not rows:
        return False
    row = rows[0]
    if row["Config"]["Labels"].get(LABEL) != ns or row["Name"] != "/" + name:
        raise RuntimeError("refuse to stop a container this run does not own: " + name)
    if row["HostConfig"].get("AutoRemove") is not True:
        raise RuntimeError("refuse non-disposable container: " + name)
    sh(["docker", "stop", "--time", str(timeout), row["Id"]], timeout=timeout + 30, check=False)
    limit = time.monotonic() + 30
    while sh(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + row["Id"]]).strip():
        if time.monotonic() >= limit:
            raise RuntimeError("--rm container still present: " + name)
        time.sleep(.5)
    return True


def production_guard():
    """Read-only: name -> [restart count, started at, status] of the running shared stack and TS writer."""
    names = sh(["docker", "ps", "--filter", "name=qdl_v2_stable_candidate-",
                "--format", "{{.Names}}"]).split() + [TS_WRITER]
    out = sh(["docker", "inspect", "--format", "{{.Name}}|{{.RestartCount}}|{{.State.StartedAt}}|{{.State.Status}}",
              *names])
    rows = {}
    for line in out.splitlines():
        if line:
            name, count, started, status = line.split("|")
            rows[name.lstrip("/")] = [int(count), started, status]
    return rows


def guard_breaches(before, now):
    """Stop on a crash signal (restart count up, or a running container gone/not running).

    A changed start time with the restart count unchanged is a recreate by
    another operator (the shared stack is not ours); it is recorded, not a stop.
    """
    breaches, recreated = {}, {}
    for name, (count, started, status) in before.items():
        current = now.get(name)
        if current is None or current[2] != "running" or current[0] > count:
            breaches[name] = [[count, started, status], current]
        elif current[1] != started:
            recreated[name] = [started, current[1]]
    return breaches, recreated


def host_resources():
    free = shutil.disk_usage("/").free
    available = None
    with open("/proc/meminfo") as handle:
        for line in handle:
            if line.startswith("MemAvailable:"):
                available = int(line.split()[1]) * 1024
    return {"disk_free_bytes": free, "mem_available_bytes": available}


def container_image(name):
    """The image ID a container actually runs (read-only docker inspect)."""
    return sh(["docker", "inspect", "--format", "{{.Image}}", name], timeout=30).strip()


def image_record(image):
    """Local image ID, tags and OCI revision/version labels (provenance, read-only)."""
    rows = json.loads(sh(["docker", "image", "inspect", image], timeout=30) or "[]")
    if len(rows) != 1:
        raise RuntimeError("local image missing " + image)
    labels = (rows[0].get("Config") or {}).get("Labels") or {}
    return {"id": rows[0]["Id"], "tags": rows[0].get("RepoTags") or [],
            "revision": labels.get("org.opencontainers.image.revision"),
            "version": labels.get("org.opencontainers.image.version")}


def load_writer_clone():
    import importlib.util
    path = TS_ROOT / "scripts/p184_dr05_isolated.py"
    spec = importlib.util.spec_from_file_location("ts_p184_dr05_isolated", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def labelled_run(ns):
    """WriterClone's runner with this namespace's label on every container it starts."""
    def run(*args, check=True, capture=True, timeout=120):
        args = list(args)
        if args[:3] == ["docker", "run", "-d"]:
            args[3:3] = ["--label", f"{LABEL}={ns}"]
        elif args[:3] == ["docker", "rm", "-f"]:
            # WriterClone removes with rm -f; the redis image's VOLUME /data would
            # otherwise stay behind as an anonymous volume (G12 run 2026-10-10: 2 left).
            args[3:3] = ["-v"]
        result = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{args[:3]} -> {result.returncode}: {result.stderr[-300:]}")
        return result.stdout.strip() if capture else ""
    return run


class Run:
    def __init__(self, base, export, ns, images=None, query_code="worktree"):
        self.images = resolve_image_set(images)
        if query_code not in QUERY_CODE:
            raise ValueError("query_code must be one of " + ",".join(QUERY_CODE))
        self.query_code = query_code
        self.base = Path(base).resolve()
        self.work, self.evidence = self.base / "work", self.base / "evidence"
        self.export = Path(export).resolve()
        self.ns = namespace(ns)
        self.clones = {}
        self.expected = set()
        self.started = time.monotonic()
        self.guard0 = None
        self.last_guard = 0.0
        self.last_stats = 0.0

    # -- evidence
    def note(self, step, **fields):
        row = {"t": time.time_ns(), "step": step, **fields}
        with (self.evidence / "orchestrator.jsonl").open("a") as handle:
            handle.write(json.dumps(row, default=str) + "\n")
        print(step, json.dumps(fields, default=str)[:400], flush=True)

    def verify_image(self, role):
        """Provenance: the started container runs exactly the passed image for its role."""
        name, expected = f"{self.ns}-{role}", self.images[role_image_key(role)]
        actual = container_image(name)
        row = {"t": time.time_ns(), "role": role, "image_role": role_image_key(role),
               "expected": expected, "actual": actual, "match": actual == expected}
        with (self.evidence / "provenance.jsonl").open("a") as handle:
            handle.write(json.dumps(row) + "\n")
        if actual != expected:
            raise RuntimeError(f"STOP provenance: {name} runs {actual}, expected {expected}")

    def topology(self, topic_id=""):
        return topology(self.ns, self.work, topic_id, images=self.images, query_code=self.query_code)

    def helper(self, *args, **kwargs):
        return helper_argv(*args, images=self.images, **kwargs)

    def fault_row(self, row):
        with (self.evidence / "faults.jsonl").open("a") as handle:
            handle.write(json.dumps(row) + "\n")

    # -- checks
    def check(self, faulted=()):
        resources = host_resources()
        if resources["disk_free_bytes"] < LIMITS["disk_free_bytes"]:
            raise RuntimeError(f"STOP disk free {resources['disk_free_bytes']} < 20 GB")
        if resources["mem_available_bytes"] < LIMITS["mem_available_bytes"]:
            raise RuntimeError(f"STOP memory available {resources['mem_available_bytes']} < 3 GiB")
        missing = {f"{self.ns}-{r}" for r in self.expected - set(faulted)} - running_names(self.ns)
        if missing:
            raise RuntimeError("STOP role exited: " + ",".join(sorted(missing)))
        now = time.monotonic()
        if now - self.last_guard >= 60:
            self.last_guard = now
            breaches, recreated = guard_breaches(self.guard0, production_guard())
            if recreated and recreated != getattr(self, "recreated", None):
                self.recreated = recreated
                self.note("production_external_recreate_observed", containers=recreated)
            if breaches:
                raise RuntimeError("STOP production guard breach: " + json.dumps(breaches))
        if now - self.last_stats >= 30:
            self.last_stats = now
            names = sorted(running_names(self.ns))
            if names:
                out = sh(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names], timeout=60)
                with (self.evidence / "stats.jsonl").open("a") as handle:
                    for line in out.splitlines():
                        item = json.loads(line)
                        handle.write(json.dumps({"t": time.time_ns(), "name": item.get("Name"),
                                                 "cpu": item.get("CPUPerc"), "mem": item.get("MemUsage")}) + "\n")
        return resources

    def wait(self, seconds, faulted=(), until=None, step=5.0):
        limit = time.monotonic() + seconds
        while time.monotonic() < limit:
            self.check(faulted)
            if until is not None:
                value = until()
                if value:
                    return value
            time.sleep(min(step, max(0.0, limit - time.monotonic())))
        return until() if until is not None else None

    def start(self, role, topic_id):
        argv = self.topology(topic_id)[role]
        sh(argv, timeout=60)
        self.expected.add(role)
        self.verify_image(role)

    # -- phases
    def prepare(self):
        self.base.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.work.mkdir(mode=0o700)
        self.evidence.mkdir(mode=0o755)
        resources = host_resources()
        self.note("preflight", **resources)
        if resources["disk_free_bytes"] < LIMITS["disk_free_bytes"]:
            raise RuntimeError("disk below 20 GB before start")
        records = {}
        for role, image in self.images.items():
            records[role] = image_record(image)
            if records[role]["id"] != image:
                raise RuntimeError("local image mismatch " + image)
        (self.evidence / "images.json").write_text(json.dumps(
            {"images": self.images, "local": records, "query_code": self.query_code,
             "defaults_overridden": sorted(r for r in self.images if self.images[r] != IMAGES[r])},
            indent=2) + "\n")
        self.note("image_set", images=self.images, query_code=self.query_code)
        if owned_names(self.ns) or sh(["docker", "ps", "-aq", "--filter", "name=" + self.ns]).strip():
            raise RuntimeError("namespace already in use")
        self.guard0 = production_guard()
        self.note("production_guard_before", containers=len(self.guard0), guard=self.guard0)
        self.volumes0 = len(sh(["docker", "volume", "ls", "-q"]).split())
        self.networks0 = len(sh(["docker", "network", "ls", "-q"]).split())
        (self.work / "handoff.json").write_text(json.dumps({
            "namespace": self.ns, "status": "PREPARED_HANDOFF_NOT_RUN", "provenance": "TEST_ONLY",
            "level": "ISOLATED_FAULT", "acceptance": False, "material_ready": False,
            "images": dr05_images(self.images), "g12_images": self.images,
            "query_code": self.query_code}, indent=2) + "\n")
        sh(self.helper(self.ns, self.work, ["--material", "/work", "--core-config", "/export.json"],
                       network="none", script="scripts/p184_dr05_dl_fault.py",
                       extra=["-v", f"{self.export}:/export.json:ro"]), timeout=180)
        packet = json.loads((self.work / "packet.json").read_text())
        packet["bootstrap"] = BOOTSTRAP
        (self.work / "packet.json").write_text(json.dumps(packet))
        password = broker_password((self.work / "broker.properties").read_text())
        for node in BROKERS:
            path = self.work / f"broker-{node}.properties"
            with path.open("x") as handle:
                os.chmod(path, 0o600)
                handle.write(broker_properties(password, node))
            (self.work / f"kafka-{node}").mkdir(mode=0o700)
        self.note("material_ready", namespace=self.ns, volumes_before=self.volumes0,
                  networks_before=self.networks0)

    def up(self):
        sh(self.topology()["net"])
        self.expected.add("net")
        self.verify_image("net")
        for node in BROKERS:
            self.start(f"kafka-{node}", "")
        result = sh(self.helper(self.ns, self.work, ["--topics", "/work"]), timeout=180)
        self.note("topics", result=result.strip()[-400:])
        topic_id = json.loads((self.work / "packet.json").read_text())["slice"]["topic_id"]
        self.topic_id = topic_id
        for role in SERVICE_ORDER:
            self.start(role, topic_id)
        limit = time.monotonic() + LIMITS["role_return_s"]
        while True:
            self.check()
            probe = subprocess.run(self.helper(self.ns, self.work, ["--probe", "/work"],
                                               script="scripts/p184_dr05_dl_fault.py"),
                                   capture_output=True, text=True, timeout=60)
            if probe.returncode == 0:
                self.note("services_reachable", out=probe.stdout.strip()[-300:])
                break
            if time.monotonic() >= limit:
                raise RuntimeError("services not reachable: " + probe.stderr.strip()[-400:])
            time.sleep(2)
        remaining = LIMITS["deadline_s"] - (time.monotonic() - self.started) - 60
        sh(self.helper(self.ns, self.work, ["--monitor", "/work", "--out", "/evidence/monitor.jsonl",
                                            "--seconds", str(int(remaining))], role="monitor", detach=True,
                       evidence=self.evidence, cpus="0.25", memory="256m"))
        self.expected.add("monitor")
        self.verify_image("monitor")
        sh(self.helper(self.ns, self.work, ["--publish", "/work", "--out", "/evidence/publish.jsonl",
                                            "--seconds", str(int(remaining)), "--hz", "4"], role="publisher",
                       detach=True, evidence=self.evidence, cpus="0.25", memory="256m"))
        self.expected.add("publisher")
        self.verify_image("publisher")
        self.note("publisher_monitor_started", seconds=int(remaining))
        iso = load_writer_clone()
        import tempfile
        logs = self.work / "tslogs"
        logs.mkdir(mode=0o700)
        tempfile.tempdir = str(logs)  # WriterClone's stderr dir stays under the run, not /tmp
        run = labelled_run(self.ns)
        for venue in VENUES:
            redis_port, pg_port = CLONE_PORTS[venue]
            spec = json.loads((self.work / f"ts-consumer.{venue.lower()}.json").read_text())
            clone = iso.WriterClone(self.images["ts"], redis_image=self.images["redis"],
                                    postgres_image=self.images["postgres"],
                                    venues=(venue,), network="container:" + self.ns + "-net",
                                    redis_port=redis_port, pg_port=pg_port, consumer=spec,
                                    dl_work=str(self.work), run=run, tag=venue.lower())
            label = venue.lower()
            clone.name = f"{self.ns}-tswriter-{label}"
            clone.redis_name = f"{self.ns}-tsredis-{label}"
            clone.pg_name = f"{self.ns}-tspg-{label}"
            clone.deadline = clone.clock() + LIMITS["clone_ready_s"]
            self.clones[venue] = clone
            clone.__enter__()
            self.expected |= {f"tswriter-{label}", f"tsredis-{label}", f"tspg-{label}"}
            for kind in ("tswriter", "tsredis", "tspg"):
                self.verify_image(f"{kind}-{label}")
        self.note("ts_clones_started", names=[c.name for c in self.clones.values()])
        sh(sampler_argv(self.ns, self.evidence, remaining - 30, images=self.images))
        self.expected.add("sampler")
        self.verify_image("sampler")
        started = time.time_ns()

        def ready():
            index = publish_index(load_jsonl(self.evidence / "publish.jsonl"))
            series = view_series(load_jsonl(self.evidence / "sampler.jsonl"))
            return all(caught_up(series, index, v, started, hold_s=10)["caught_up_ns"] for v in VENUES)

        if not self.wait(LIMITS["clone_ready_s"], until=ready):
            raise RuntimeError("STOP TS clones never showed a fresh APPROVED book view")
        self.note("ts_views_fresh")

    def baseline(self):
        start = time.time_ns()
        self.wait(LIMITS["baseline_s"])
        self.fault_row({"k": "baseline", "start_ns": start, "end_ns": time.time_ns()})
        self.note("baseline_done")

    def canonical_leader(self):
        rows = load_jsonl(self.evidence / "monitor.jsonl")
        for row in reversed(rows):
            leader = (row.get("leaders") or {}).get(f"{self.ns}.canonical/0")
            if leader in BROKERS:
                return leader
        raise RuntimeError("canonical leader unknown")

    def fault(self, name, kind):
        self.check()
        node = self.canonical_leader() if kind == "broker" else None
        role = f"kafka-{node}" if kind == "broker" else kind
        injected = time.time_ns()
        stop_role(self.ns, role)
        self.note("fault_injected", fault=name, role=role)
        if kind == "broker":
            self.wait(LIMITS["broker_outage_s"], faulted=(role,))
        restart = time.time_ns()
        sh(self.topology(self.topic_id)[role], timeout=60)
        self.note("restart_issued", fault=name, role=role)
        self.verify_image(role)
        row = {"k": "fault", "name": name, "kind": kind, "node": node, "role": role,
               "injected_ns": injected, "restart_ns": restart}
        self.fault_row(row)
        monitor_rows = lambda: sorted(load_jsonl(self.evidence / "monitor.jsonl"), key=lambda r: r.get("t", 0))
        cleared = self.wait(LIMITS["role_return_s"],
                            until=lambda: cleared_at(kind, monitor_rows(), restart, node))
        if not cleared:
            raise RuntimeError(f"STOP {name}: fault not cleared within {LIMITS['role_return_s']} s")
        self.note("fault_cleared", fault=name, cleared_ns=cleared)

        def caught():
            result = evaluate_fault(row, load_jsonl(self.evidence / "publish.jsonl"),
                                    load_jsonl(self.evidence / "sampler.jsonl"), monitor_rows(), window_s=0)
            return result if result.get("consumers_caught_up_ns") else None

        result = self.wait(GATE["caught_up_s"] + GATE["hold_s"] + 10, until=caught)
        if not result or result["caught_up_s"] > GATE["caught_up_s"]:
            raise RuntimeError(f"STOP {name}: consumers not caught up within {GATE['caught_up_s']} s")
        self.note("caught_up", fault=name, caught_up_s=result["caught_up_s"])
        end = result["consumers_caught_up_ns"] + int(GATE["window_s"] * 1e9)
        self.wait(max(0.0, (end - time.time_ns()) / 1e9) + 2)
        verdict = evaluate_fault(row, load_jsonl(self.evidence / "publish.jsonl"),
                                 load_jsonl(self.evidence / "sampler.jsonl"), monitor_rows())
        self.note("fault_verdict", fault=name, verdict=verdict)
        if not verdict["pass"]:
            raise RuntimeError(f"STOP {name}: G12/G1 verdict FAIL")

    def keep_logs(self):
        """Bounded log tails of every owned container (--rm loses them at stop)."""
        logs = self.evidence / "logs"
        logs.mkdir(exist_ok=True)
        for name in owned_names(self.ns):
            result = subprocess.run(["docker", "logs", "--tail", "120", name], capture_output=True,
                                    text=True, timeout=30)
            (logs / (name[len(self.ns) + 1:] + ".log")).write_text(
                (result.stdout + result.stderr)[-60000:])

    def down(self):
        removed, errors = [], []
        try:
            self.keep_logs()
        except Exception as error:  # noqa: BLE001 - evidence is best effort, teardown is not
            errors.append("logs:" + type(error).__name__)
        for clone in self.clones.values():
            try:
                for item in clone.close():
                    removed.append(item)
            except Exception as error:  # noqa: BLE001 - cleanup continues
                errors.append(type(error).__name__)
        report = teardown(self.ns)
        report["writer_clone_removed"] = removed
        report["errors"] = errors + report.get("errors", [])
        report["volumes_before"] = getattr(self, "volumes0", None)
        report["networks_before"] = getattr(self, "networks0", None)
        report["volumes_after"] = len(sh(["docker", "volume", "ls", "-q"]).split())
        report["networks_after"] = len(sh(["docker", "network", "ls", "-q"]).split())
        erase_private(self.work)
        if self.guard0 is not None:
            breaches, recreated = guard_breaches(self.guard0, production_guard())
            report["production_guard_breaches"] = breaches
            report["production_external_recreates"] = recreated
        with (self.evidence / "teardown.json").open("w") as handle:
            json.dump(report, handle, indent=2)
        self.note("teardown", **report)
        return report


def teardown(ns):
    """Exact-name, label-checked stop of every owned container, then verify zero."""
    removed, errors = [], []
    for name in owned_names(ns):
        role = name[len(ns) + 1:]
        if role not in TEARDOWN_ORDER and not re.fullmatch(r"ts(writer|redis|pg)-(binance|okx)", role):
            errors.append("unknown role " + role)
    order = list(TEARDOWN_ORDER[:3]) + [f"ts{k}-{v}" for v in ("binance", "okx") for k in ("writer", "redis", "pg")] \
        + list(TEARDOWN_ORDER[3:])
    for role in order:
        try:
            if stop_role(ns, role, timeout=10):
                removed.append(role)
        except Exception as error:  # noqa: BLE001 - continue, then report
            errors.append(f"{role}:{type(error).__name__}:{error}")
    left = owned_names(ns)
    by_name = sh(["docker", "ps", "-aq", "--filter", "name=" + ns]).split()
    networks = sh(["docker", "network", "ls", "-q", "--filter", "name=" + ns]).split()
    volumes = sh(["docker", "volume", "ls", "-q", "--filter", "label=" + LABEL]).split()
    return {"removed_containers": removed, "removed_count": len(removed), "left_by_label": len(left),
            "left_by_name": len(by_name), "networks_by_name": len(networks), "volumes_by_label": len(volumes),
            "errors": errors}


def erase_private(work):
    work = Path(work)
    for name in PRIVATE_FILES:
        (work / name).unlink(missing_ok=True)
    for node in BROKERS:
        shutil.rmtree(work / f"kafka-{node}", ignore_errors=True)
    shutil.rmtree(work / "tslogs", ignore_errors=True)


def orchestrate(base, export, ns, images=None, query_code="worktree"):
    run = Run(base, export, ns, images, query_code)

    def expire(_signum, _frame):
        raise TimeoutError("G12 orchestrator absolute deadline")
    def terminated(signum, _frame):
        # A session end / kill (SIGTERM, SIGHUP, SIGINT) still runs evidence + teardown in finally
        # (final-image attempt 2, 2026-10-10: the orchestrator died with the session, 18 roles orphaned).
        raise SystemExit(f"G12 orchestrator received signal {signum}")
    previous = signal.signal(signal.SIGALRM, expire)
    previous_term = {sig: signal.signal(sig, terminated) for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
    signal.alarm(LIMITS["deadline_s"])
    status, error = "FAIL", None
    try:
        run.prepare()
        run.up()
        run.baseline()
        for name, kind in (("broker_stop_start", "broker"), ("projector_restart", "projector"),
                           ("stream_restart", "stream")):
            run.fault(name, kind)
        status = "COMPLETED"
    except BaseException as exc:  # noqa: BLE001 - evidence first, then teardown
        error = f"{type(exc).__name__}: {exc}"
        print("STOPPED", error, flush=True)
        if run.evidence.exists():
            run.note("stopped", error=error)
    finally:
        signal.alarm(0)
        try:
            if run.evidence.exists():
                verdict = evaluate(run.evidence)
                verdict.update(status=status, error=error, namespace=ns, images=run.images,
                               query_code=run.query_code)
                (run.evidence / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
        finally:
            run.down() if run.evidence.exists() else teardown(ns)
            signal.signal(signal.SIGALRM, previous)
            for sig, handler in previous_term.items():
                signal.signal(sig, handler)
    return 0 if status == "COMPLETED" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("run", "down", "analyze", "topics", "publish", "monitor"):
        mode.add_argument("--" + flag, type=Path)
    parser.add_argument("--export", type=Path)
    parser.add_argument("--namespace")
    parser.add_argument("--out")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--hz", type=float, default=4.0)
    parser.add_argument("--image-set", type=Path, help="JSON object role -> sha256:<64hex> (defaults: IMAGES)")
    parser.add_argument("--image", action="append", default=[], metavar="ROLE=sha256:<64hex>")
    parser.add_argument("--query-code", choices=QUERY_CODE, default="worktree")
    args = parser.parse_args(argv)
    if args.run:
        if args.export is None or args.namespace is None:
            parser.error("--run needs --export and --namespace")
        images = resolve_image_set(args.image_set, args.image)
        return orchestrate(args.run, args.export, args.namespace, images, args.query_code)
    if args.down:
        ns = namespace(json.loads((args.down / "work" / "handoff.json").read_text())["namespace"])
        report = teardown(ns)
        erase_private(args.down / "work")
        print(json.dumps(report))
        return 0 if not report["errors"] and report["left_by_label"] == 0 else 1
    if args.analyze:
        print(json.dumps(evaluate(args.analyze / "evidence" if (args.analyze / "evidence").exists()
                                  else args.analyze), indent=2))
        return 0
    if args.seconds is None and not args.topics:
        parser.error("--seconds required")
    if args.topics:
        print(json.dumps(create_topics(args.topics)))
    elif args.publish:
        print(json.dumps(publish(args.publish, args.out, args.seconds, args.hz)))
    else:
        print(json.dumps(monitor(args.monitor, args.out, args.seconds)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

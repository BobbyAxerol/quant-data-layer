#!/usr/bin/env python3
"""P18.4 G12 / D4-R3: isolated Kafka broker / projector / Stream fault acceptance.

Purpose: run the G12 gate (TS upgrade/p184-phase-d-gates.md) once, in a disposable
namespace: three KRaft brokers (RF3, min ISR 2, mTLS), the production core,
projector, Stream and Query images, a test-provenance publisher (BOOK, and
TRADE / QUOTE / closed BAR 1m / MARK-INDEX on their own provider lanes), two TS
writer clones (the TS-owned ``WriterClone``, production entrypoint, 4 workers) and a
sampler that reads each clone's BOOK view through the TS at-use validator and receives
every TRADE / QUOTE / MARK / BAR the clone applies (its own ``events.market.v2.*``
notifications).
Faults: stop/start one broker, restart the projector, restart Stream. Per fault
it records the G11 four timestamps (injected, cleared, first valid apply,
caught up) and then, inside one fixed window, G1 (BOOK), G2 (TRADE, QUOTE and
MARK_INDEX_PRICE new-event transit) and G3 (BAR 1m from close). A fault passes only
when caught up in time and G1, every G2 feed and G3 pass; a part with too few samples
is NOT_MEASURED, never a pass.

Feeds: the DR05 material is BOOK-only; the in-container ``--feeds`` step (network
none) seals TRADE, QUOTE, BAR 1m and MARK_INDEX_PRICE for the same two instruments
into the same material: core bindings from the production acquisition mapping
(``ProductionCatalogBuilder._acquisition``, checked equal to the production plan;
MARK/INDEX as the deployment's physical components), the source catalog's own
bindings, the TS paper manifest's requirement shape (event recency / session
liveness null like the DR05 BOOK requirements), the route binding and ts-consumer
via the DR05 ``consumer_packet`` and a recompiled bundle. The writer reads MARK
through Query reference reads woken by Stream: the Query role (``--query``) is the
DR05 Query of the code under test with the production KN execution MARK/INDEX
reader wired in (``qdl.runtime.stable`` kn3). Exits of every owned container
(``--rm`` removes it, so its own inspect is gone) are recorded from the daemon's
die/oom/kill events (``evidence/container-events.jsonl``, ``exit-states.json``).

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
``--publish``, ``--monitor``; each bounded by ``--seconds`` / its own deadline;
``--feeds`` (network none, material only); ``--query`` (the Query role).
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import inspect
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
          "role_return_s": 120, "clone_ready_s": 300, "baseline_s": 300, "md_ready_s": 180,
          "monitor_sample_s": 5}
# A fault whose clearance / catch-up interval has no valid monitor sample for longer than this is NOT_MEASURED:
# its fault-cleared time (the monitor's) would be uncertain by more than the gap (steady cadence: 1.0 s).
MONITOR_MAX_GAP_S = 5
# G2 / G3 (frozen TS gate rows; owner decision 2026-10-09). min_n: below it the part is NOT_MEASURED.
# G2 is new-event transit (publish -> first applied notification); G3 is BAR 1m from close.
GATE_G2_G3 = {"TRADE": {"p95_ms": 500, "max_ms": 1500, "min_n": 100},
              "QUOTE": {"p95_ms": 500, "max_ms": 1000, "min_n": 100},
              "MARK": {"p95_ms": 500, "max_ms": 1000, "min_n": 100},
              "BAR": {"p95_ms": 5000, "max_ms": 60000, "min_n": 3}}
G2_FEEDS = ("TRADE", "QUOTE", "MARK")  # MARK = MARK_INDEX_PRICE
# An event not applied this long after its reference is lost (TRADE/BAR: a FAIL). QUOTE (ON_CHANGE) and MARK (a
# latest-state reference read per Stream wake) may legitimately skip a superseded value: only counted as unseen.
# Before that an event is pending and not sampled.
LOST_AFTER_S = {"TRADE": 10, "QUOTE": 10, "MARK": 10, "BAR": 60}
LATEST_STATE_FEEDS = ("QUOTE", "MARK")
MD_FEEDS = ("TRADE", "QUOTE", "BAR", "MARK")
CATALOG_FEED = {"TRADE": "TRADE", "QUOTE": "QUOTE", "BAR": "BAR", "MARK": "MARK_INDEX_PRICE"}
# The production acquisition plan: read-only, only for the OKX index symbol of a MARK_INDEX binding (never inferred).
PRODUCTION_ACQUISITION = "config/v2/stable-acquisition-bindings.yaml"
BAR_INTERVAL, BAR_MS = "1m", 60_000
# Synthetic venue cadence per instrument: steady trades plus a short burst, on-change quotes, one final bar per
# minute shortly after its close (Binance's native lane measured 0.04-1.1 s), a few closed bars seeded at start
# (the bar edge's REST bootstrap role) so the writer's BAR warmup has a retained view, one MARK/INDEX observation
# per second (Binance @markPrice@1s; OKX mark-price + index-tickers pushed as one changed pair per second).
MD_RATE = {"trade_per_s": 3, "burst_every_s": 15, "burst_trades": 8, "quote_per_s": 2,
           "bar_delay_ms": 200, "seed_bars": 5, "mark_per_s": 1}
TEST_ADAPTER = "p184-synthetic-test-only"
# Requirement shape of the deployed TS paper manifest (consumers/stable/trading-system-paper.yaml r10) for these feeds;
# event recency and session liveness stay null like the DR05 BOOK requirements (no isolated provider-session signal).
MD_REQUIREMENTS = {"TRADE": {"max_freshness_ms": 3000, "warmup_limit": 0, "require_final_bars": True},
                   "QUOTE": {"max_freshness_ms": 2000, "warmup_limit": 0, "require_final_bars": True},
                   "BAR": {"max_freshness_ms": 180000, "warmup_limit": 1000, "interval": BAR_INTERVAL,
                           "require_final_bars": True},
                   "MARK": {"max_freshness_ms": 2000, "warmup_limit": 0, "require_final_bars": False,
                            "recovery": "FRESH_SNAPSHOT"}}
# provider kind suffix -> lane. MARK lanes: Binance one BOTH component; OKX a MARK and an INDEX component.
MD_KIND_FEED = {"_trade": "TRADE", "_bbo": "QUOTE", "_bar": "BAR", "_mark_index": "MARK", "_mark_price": "MARK",
                "_index_price": "INDEX"}
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
    # Either way the entry is this harness's --query (serve_query): the DR05/DR08 Query of that code plus the
    # execution MARK/INDEX reader; with "image" only this one harness file is mounted, never the worktree.
    if query_code == "image":
        code = ["-v", f"{ROOT / 'scripts/p184_g12_isolated_fault.py'}:/harness/p184_g12_isolated_fault.py:ro",
                "-w", "/app"]
        entry = "/harness/p184_g12_isolated_fault.py"
    else:
        code, entry = ["-v", f"{ROOT}:/app:ro", "-w", "/app"], "scripts/p184_g12_isolated_fault.py"
    roles["query"] = _env(_base(ns, "query", network=net, memory="512m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount + code,
        {**common, "PYTHONPATH": "/app:/app/generated/python", "PYTHONDONTWRITEBYTECODE": "1"}) + [
        "--entrypoint", "python", images["python"], "-B", entry, "--query", "/work"]
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


def md_event_row(channel, data):
    """One applied TS notification -> evidence fields, or None (not a G2/G3 sample).

    Runs inside the sampler (its source is embedded in SAMPLER). TRADE/QUOTE: the
    writer's per-event ``events.market.v2.execution.<feed>.*`` publish (every applied
    event; observed_at_ms = canonical source_event_time); id = native trade id, or the
    quote's event ms. MARK: the ordered MARK_INDEX_PRICE state publish
    (``execution.mark_index_price``, only when the CAS applies a newer view); id = the
    pair's observed ms. BAR: ``events.market.v2.bar.*`` closed 1m rows; id = open ms.
    """
    parts = str(channel).split(".")
    try:
        payload = json.loads(data)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or parts[:3] != ["events", "market", "v2"] or len(parts) < 5:
        return None
    if parts[3] == "execution" and parts[4] in ("trade", "quote", "mark_index_price"):
        feed = {"trade": "TRADE", "quote": "QUOTE", "mark_index_price": "MARK"}[parts[4]]
        event_ms = payload.get("observed_at_ms")
        ident = ((payload.get("payload") or {}).get("native_trade_id") if feed == "TRADE" else event_ms)
        return {"feed": feed, "id": None if ident is None else str(ident), "ev_ms": event_ms}
    if parts[3] == "bar" and payload.get("is_closed") is True and payload.get("interval") == "1m":
        opened = payload.get("timestamp_ms")
        return {"feed": "BAR", "id": None if opened is None else str(opened), "ev_ms": opened}
    return None


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
MD_PATTERNS = ("events.market.v2.execution.trade.*", "events.market.v2.execution.quote.*",
               "events.market.v2.execution.mark_index_price.*", "events.market.v2.bar.*")
async def listen(url, venue, f, end):
    """PSUBSCRIBE only (read-only): receipt time of every applied TRADE/QUOTE/MARK/closed BAR of one clone."""
    while time.monotonic() < end and not STOP:
        r = Redis.from_url(url.rsplit("/", 1)[0] + "/0", socket_connect_timeout=1, decode_responses=True)
        p = r.pubsub(ignore_subscribe_messages=True)
        try:
            await p.psubscribe(*MD_PATTERNS)
            while time.monotonic() < end and not STOP:
                m = await p.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if not m or m.get("type") != "pmessage":
                    continue
                t = time.time_ns()
                row = md_event_row(m["channel"], m["data"])
                if row is not None:
                    f.write(json.dumps({"k": "ev", "t": t, "venue": venue, **row}) + "\n")
        except Exception as e:
            f.write(json.dumps({"k": "ev_error", "t": time.time_ns(), "venue": venue, "error": type(e).__name__}) + "\n")
            await asyncio.sleep(1)
        finally:
            for close in (p.aclose, r.aclose):
                try:
                    await close()
                except Exception:
                    pass
async def main(out, seconds, interval_ms, targets):
    views = {v: Redis.from_url(u, socket_timeout=1, socket_connect_timeout=1) for v, u in targets.items()}
    beats = {v: Redis.from_url(u.rsplit("/", 1)[0] + "/2", socket_timeout=1, socket_connect_timeout=1,
                               decode_responses=True) for v, u in targets.items()}
    end = time.monotonic() + seconds
    last, last_t, last_beat = {}, {}, 0.0
    with open(out, "a", buffering=1) as f:
        listeners = [asyncio.create_task(listen(u, v, f, end)) for v, u in targets.items()]
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
        await asyncio.wait(listeners, timeout=3)
        for task in listeners:
            task.cancel()
    for r in (*views.values(), *beats.values()):
        await r.aclose()
asyncio.run(main(sys.argv[1], float(sys.argv[2]), float(sys.argv[3]),
                 dict(item.split("=", 1) for item in sys.argv[4].split(","))))
'''
# The sampler runs the same parser the unit tests exercise.
SAMPLER = SAMPLER.replace("STOP = []\n", "STOP = []\n" + inspect.getsource(md_event_row), 1)


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


def md_publish_index(rows):
    """(feed, venue, id) -> the publisher's row of one TRADE/QUOTE/BAR source event (k == pub_md)."""
    return {(r["feed"], r["venue"], str(r["id"])): r for r in rows if r.get("k") == "pub_md"}


def md_first_seen(rows):
    """(feed, venue, id) -> first receipt ns of the clone's applied notification (k == ev)."""
    seen = {}
    for r in rows:
        if r.get("k") == "ev" and r.get("id") is not None:
            key = (r["feed"], r["venue"], str(r["id"]))
            if key not in seen or r["t"] < seen[key]:
                seen[key] = r["t"]
    return seen


def md_reference_ns(row):
    """Transit origin: the bar close for BAR (open + 1 m), else the publish stamp (= the frame's event time)."""
    return (row["close_ms"] * 1_000_000) if row["feed"] == "BAR" else row["t"]


def md_sample(index, seen, feed, venue, start_ns, end_ns, covered_ns):
    """New-event transit (ms) of every non-seed event of one feed/venue whose reference lies in [start, end).

    An event never applied is lost once the sampler covered reference + LOST_AFTER_S (QUOTE / MARK: unseen,
    a latest-state feed may skip a superseded value), else pending (excluded: not yet decidable).
    """
    values, counts = [], {"lost": 0, "unseen": 0, "pending": 0}
    for key, row in index.items():
        if key[0] != feed or key[1] != venue or row.get("seed"):
            continue
        reference = md_reference_ns(row)
        if not start_ns <= reference < end_ns:
            continue
        applied = seen.get(key)
        if applied is not None:
            values.append((applied - reference) / 1e6)
        elif covered_ns >= reference + LOST_AFTER_S[feed] * 1_000_000_000:
            counts["unseen" if feed in LATEST_STATE_FEEDS else "lost"] += 1
        else:
            counts["pending"] += 1
    return values, counts


def md_stats(feed, values, counts=None):
    """G2/G3 verdict of one feed/venue: PASS, FAIL, or NOT_MEASURED below the minimum sample count."""
    gate = GATE_G2_G3[feed]
    values = sorted(values)
    n = len(values)
    stats = {"n": n, "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
             "p99_ms": quantile(values, .99) if n >= 100 else None, "max_ms": values[-1] if values else None,
             **(counts or {"lost": 0, "unseen": 0, "pending": 0})}
    if stats["lost"]:
        stats["status"] = "FAIL"
        stats["note"] = "events never applied"
    elif n < gate["min_n"]:
        stats["status"] = "NOT_MEASURED"
        stats["note"] = f"n < {gate['min_n']}"
    else:
        stats["status"] = ("PASS" if stats["p95_ms"] <= gate["p95_ms"] and stats["max_ms"] < gate["max_ms"]
                           else "FAIL")
    stats["pass"] = stats["status"] == "PASS"
    return {k: (round(v, 3) if isinstance(v, float) else v) for k, v in stats.items()}


def part_status(stats):
    """One gate part over its cells: any FAIL -> FAIL, else any NOT_MEASURED -> NOT_MEASURED, else PASS."""
    statuses = [s["status"] for s in stats]
    if not statuses:
        return "NOT_MEASURED"
    return "FAIL" if "FAIL" in statuses else "NOT_MEASURED" if "NOT_MEASURED" in statuses else "PASS"


def md_window(publish_rows, sampler_rows, start_ns, end_ns):
    """G2 (TRADE, QUOTE, MARK_INDEX_PRICE) and G3 (BAR 1m) over one window, plus each part's status."""
    index, seen = md_publish_index(publish_rows), md_first_seen(sampler_rows)
    covered = max((r["t"] for r in sampler_rows if r.get("k") in ("view", "ev")), default=0)
    cell = lambda feed, venue: md_stats(feed, *md_sample(index, seen, feed, venue, start_ns, end_ns, covered))
    g2 = {feed: {venue: cell(feed, venue) for venue in VENUES} for feed in G2_FEEDS}
    g3 = {"BAR_" + BAR_INTERVAL: {venue: cell("BAR", venue) for venue in VENUES}}
    return {"g2": g2, "g3": g3,
            "g2_feed_status": {feed: part_status(list(cells.values())) for feed, cells in g2.items()},
            "g2_status": part_status([s for cells in g2.values() for s in cells.values()]),
            "g3_status": part_status([s for cells in g3.values() for s in cells.values()])}


def md_missing(sampler_rows):
    """FEED@VENUE never applied by a clone yet (start-up gate before the baseline)."""
    seen = {(key[0], key[1]) for key in md_first_seen(sampler_rows)}
    return [f"{feed}@{venue}" for venue in VENUES for feed in MD_FEEDS if (feed, venue) not in seen]


def publisher_md_summary(publish_rows):
    """Per feed: published source events (seed bars apart), broker ACKs and delivery errors."""
    out = {feed: {"published": 0, "seed": 0, "acked": 0, "delivery_errors": 0} for feed in MD_FEEDS}
    for r in publish_rows:
        feed = out.get(r.get("feed"))
        if feed is None:
            continue
        if r.get("k") == "pub_md":
            feed["seed" if r.get("seed") else "published"] += 1
        elif r.get("k") == "ack_md":
            feed["delivery_errors" if "error" in r else "acked"] += 1
    return out


def exit_states(event_rows):
    """role -> exit record of every owned container life, from the daemon's container events.

    ``--rm`` removes a container as it exits, so ``docker inspect`` has nothing left to read;
    the die event carries exitCode, an oom event marks the OOM kill of that life, a kill event
    the signal that was sent (docker stop / teardown). Attributes are the daemon's; never inferred.
    """
    states = {}
    pending = {}
    for row in sorted(event_rows, key=lambda r: r.get("timeNano") or 0):
        attrs = (row.get("Actor") or {}).get("Attributes") or {}
        match = re.fullmatch(NS_PATTERN + r"-(.+)", str(attrs.get("name", "")))
        if not match:
            continue
        role, action = match.group(1), row.get("Action") or row.get("status")
        life = pending.setdefault(role, {"oom_killed": False, "kill_signals": []})
        if action == "oom":
            life["oom_killed"] = True
        elif action == "kill":
            life["kill_signals"].append(attrs.get("signal"))
        elif action == "die":
            code = attrs.get("exitCode")
            life.update(exit_code=int(code) if str(code).lstrip("-").isdigit() else None,
                        died_ns=row.get("timeNano"),
                        oom_killed=life["oom_killed"] or str(attrs.get("oomKilled", "")).lower() == "true")
            states.setdefault(role, {"lives": []})["lives"].append(pending.pop(role))
    for role, value in states.items():
        value.update({k: value["lives"][-1][k] for k in ("exit_code", "oom_killed", "died_ns")})
    return states


def monitor_valid(monitor_rows):
    """Monitor samples that observed: a ``sample_error`` (crashed / timed-out sample) or the end marker is
    never evidence of anything."""
    return [row for row in monitor_rows if not row.get("k")]


def monitor_max_gap_s(monitor_rows, start_ns, end_ns):
    """Longest interval (s) in [start, end] without a valid monitor sample, the edges included."""
    points = [start_ns, *sorted(r["t"] for r in monitor_valid(monitor_rows) if start_ns < r["t"] < end_ns), end_ns]
    return round(max(b - a for a, b in zip(points, points[1:])) / 1e9, 3) if end_ns > start_ns else 0.0


def cleared_at(kind, monitor_rows, restart_ns, node=None):
    """Fault-cleared time from the monitor (packet section 5)."""
    for row in monitor_rows:
        if row.get("t", 0) < restart_ns or row.get("k"):
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
        if row.get("t", 0) >= start_ns and not row.get("k") and predicate(row):
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
    # The fault-cleared time is the monitor's: with no valid sample for longer than MONITOR_MAX_GAP_S between the
    # restart and catch-up it is uncertain by that much, so the fault is NOT_MEASURED (never a PASS).
    result["monitor_max_gap_s"] = monitor_max_gap_s(monitor_rows, fault["restart_ns"], caught)
    result["monitor_sample_errors"] = [
        {k: r.get(k) for k in ("t", "error", "phase")} for r in monitor_rows
        if r.get("k") == "sample_error" and fault["injected_ns"] <= r["t"] <= caught]
    monitored = "PASS" if result["monitor_max_gap_s"] <= MONITOR_MAX_GAP_S else "NOT_MEASURED"
    if monitored != "PASS":
        result["reason"] = "MONITOR_GAP"
    # G2/G3 over the same window as G1 (new events whose publish time / bar close lies in it).
    result.update(md_window(publish_rows, sampler_rows, *window))
    result["parts"] = {"caught_up": result["g12_caught_up_pass"], "window_complete": complete,
                       "g1": "PASS" if all(result["g1"][v]["pass"] for v in VENUES) else "FAIL",
                       "g2": result["g2_status"], "g2_feeds": result["g2_feed_status"],
                       "g3": result["g3_status"], "monitor": monitored}
    result["pass"] = (monitored == "PASS" and result["g12_caught_up_pass"] and complete
                      and all(result["g1"][v]["pass"] for v in VENUES)
                      and result["g2_status"] == "PASS" and result["g3_status"] == "PASS")
    return result


def evaluate(evidence):
    evidence = Path(evidence)
    publish_rows = load_jsonl(evidence / "publish.jsonl")
    sampler_rows = load_jsonl(evidence / "sampler.jsonl")
    monitor_rows = sorted(load_jsonl(evidence / "monitor.jsonl"), key=lambda r: r.get("t", 0))
    faults = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "fault"]
    index, series = publish_index(publish_rows), view_series(sampler_rows)
    out = {"schema": "p184.g12.verdict.v2", "provenance": "TEST_ONLY", "level": "ISOLATED_FAULT",
           "gate": GATE, "gate_g2_g3": GATE_G2_G3, "faults": [],
           "g2": "per fault: TRADE, QUOTE and MARK (MARK_INDEX_PRICE) new-event transit "
                 "(publish -> first applied notification)",
           "g3": "per fault: BAR 1m from close (open + 60 s -> first applied closed-bar notification)"}
    base = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "baseline"]
    if base:
        window = (base[0]["start_ns"], base[0]["end_ns"])
        out["baseline_g1"] = {v: g1_stats(transit(series, index, v, *window)) for v in VENUES}
        out["baseline_g2_g3"] = md_window(publish_rows, sampler_rows, *window)
    for fault in faults:
        out["faults"].append(evaluate_fault(fault, publish_rows, sampler_rows, monitor_rows))
    acks = [r for r in publish_rows if r.get("k") == "ack"]
    out["publisher"] = {"frames": sum(1 for r in publish_rows if r.get("k") == "pub"),
                        "acked": sum(1 for r in acks if "error" not in r),
                        "delivery_errors": sum(1 for r in acks if "error" in r),
                        "resyncs": sum(1 for r in publish_rows if r.get("k") == "resync"),
                        "market_data": publisher_md_summary(publish_rows)}
    out["exit_states"] = exit_states(load_jsonl(evidence / "container-events.jsonl"))
    errors = [r for r in monitor_rows if r.get("k") == "sample_error"]
    out["monitor"] = {"samples": len(monitor_valid(monitor_rows)), "sample_errors": len(errors),
                      "errors": sorted({(r.get("error"), r.get("phase")) for r in errors}, key=str),
                      "max_gap_rule_s": MONITOR_MAX_GAP_S}
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


SYMBOLS = {"BINANCE": "ETHUSDT", "OKX": "ETH-USDT-SWAP"}  # the DR05 linear scopes' native symbols


def _md_frame(venue, feed, payload, at_ns, lane=None):
    return {"venue": venue, "native_symbol": SYMBOLS[venue], "feed": feed, "lane": lane or feed.lower(),
            "transport": "WEBSOCKET", "received_at_ns": at_ns, "test_provenance": True, "provenance": "TEST_ONLY",
            "adapter_version": TEST_ADAPTER, "payload": payload}


def _check_md(venue, at_ns):
    if venue not in SYMBOLS or type(at_ns) is not int or at_ns <= 0:
        raise ValueError("venue and positive integer timestamp required")
    return at_ns // 1_000_000


def trade_frame(venue, trade_id, at_ns):
    """One provider-shaped trade (Binance ``@trade`` / OKX ``trades``), event time = the stamp; deterministic."""
    ms = _check_md(venue, at_ns)
    if type(trade_id) is not int or trade_id <= 0:
        raise ValueError("positive integer trade id required")
    price = f"{60000 + (trade_id % 20) / 10:.1f}"
    if venue == "BINANCE":
        payload = {"e": "trade", "E": ms, "T": ms, "s": SYMBOLS[venue], "t": trade_id, "p": price,
                   "q": "0.010", "X": "MARKET", "m": trade_id % 2 == 0}
    else:
        payload = {"arg": {"channel": "trades", "instId": SYMBOLS[venue]}, "data": [{
            "instId": SYMBOLS[venue], "tradeId": str(trade_id), "px": price, "sz": "1",
            "side": "sell" if trade_id % 2 == 0 else "buy", "ts": str(ms)}]}
    return _md_frame(venue, "TRADE", payload, at_ns)


def quote_frame(venue, sequence, at_ns):
    """One top-of-book quote (Binance ``@bookTicker`` / OKX ``bbo-tbt``); every quote differs from the previous
    one (the bid size moves), so ON_CHANGE delivery has a change to deliver."""
    ms = _check_md(venue, at_ns)
    if type(sequence) is not int or sequence <= 0:
        raise ValueError("positive integer quote sequence required")
    size = f"{1 + (sequence % 97) / 100:.2f}"
    if venue == "BINANCE":
        payload = {"e": "bookTicker", "u": sequence, "s": SYMBOLS[venue], "b": "60000.0", "B": size,
                   "a": "60001.0", "A": "1.00", "T": ms, "E": ms}
    else:
        payload = {"arg": {"channel": "bbo-tbt", "instId": SYMBOLS[venue]}, "data": [{
            "asks": [["60001.0", "1", "0", "1"]], "bids": [["60000.0", size, "0", "1"]],
            "ts": str(ms), "seqId": sequence}]}
    return _md_frame(venue, "QUOTE", payload, at_ns)


def bar_frame(venue, open_ms, at_ns, trades=0):
    """One FINAL 1m bar (Binance closed kline x=true / OKX candle1m confirm=1), sent after its close."""
    ms = _check_md(venue, at_ns)
    if type(open_ms) is not int or open_ms <= 0 or open_ms % BAR_MS or ms < open_ms + BAR_MS:
        raise ValueError("a closed minute-aligned bar is required")
    if venue == "BINANCE":
        payload = {"e": "kline", "E": ms, "s": SYMBOLS[venue], "k": {
            "t": open_ms, "T": open_ms + BAR_MS - 1, "s": SYMBOLS[venue], "i": BAR_INTERVAL, "f": -1, "L": -1,
            "o": "60000.0", "c": "60001.0", "h": "60001.9", "l": "60000.0", "v": "1.000", "n": int(trades),
            "x": True, "q": "60000.50", "V": "0.500", "Q": "30000.25"}}
    else:
        payload = {"arg": {"channel": "candle" + BAR_INTERVAL, "instId": SYMBOLS[venue]}, "data": [[
            str(open_ms), "60000.0", "60001.9", "60000.0", "60001.0", "10", "0.1", "6000.05", "1"]]}
    return _md_frame(venue, "BAR", payload, at_ns)


INDEX_SYMBOLS = {"OKX": "ETH-USDT"}  # default for frames only; the sealed binding's physical symbol is used at run


def mark_frames(venue, sequence, at_ns, index_symbol=None):
    """One MARK/INDEX observation: Binance one ``@markPrice@1s`` frame (mark and index together, component BOTH);
    OKX a ``mark-price`` frame and an ``index-tickers`` frame of the index symbol, same ms (the core pairs them on
    one generation). Each frame's ``feed`` is its component lane (MARK / INDEX)."""
    ms = _check_md(venue, at_ns)
    if type(sequence) is not int or sequence <= 0:
        raise ValueError("positive integer mark sequence required")
    mark, index = f"{60000.5 + (sequence % 10) / 100:.2f}", f"{60000.4 + (sequence % 10) / 100:.2f}"
    if venue == "BINANCE":
        funding = (ms // 28_800_000 + 1) * 28_800_000
        return [_md_frame(venue, "MARK", {"e": "markPriceUpdate", "E": ms, "s": SYMBOLS[venue], "p": mark,
                                          "P": mark, "i": index, "r": "0.00010000", "T": funding}, at_ns, "mark")]
    index_symbol = index_symbol or INDEX_SYMBOLS[venue]
    mark_frame = _md_frame(venue, "MARK", {"arg": {"channel": "mark-price", "instId": SYMBOLS[venue]}, "data": [{
        "instType": "SWAP", "instId": SYMBOLS[venue], "markPx": mark, "ts": str(ms)}]}, at_ns, "mark")
    index_frame = _md_frame(venue, "INDEX", {"arg": {"channel": "index-tickers", "instId": index_symbol}, "data": [{
        "instId": index_symbol, "idxPx": index, "high24h": "60100.0", "low24h": "59900.0", "open24h": "60000.0",
        "sodUtc0": "60000.0", "sodUtc8": "60000.0", "ts": str(ms)}]}, at_ns, "mark")
    index_frame["native_symbol"] = index_symbol
    return [mark_frame, index_frame]


def market_plan(tick, hz, rate=MD_RATE):
    """Deterministic (trades, quotes, marks) to send at publisher tick ``tick`` of a ``hz`` loop: the per-second
    rates spread over the ticks, plus a burst of trades every ``burst_every_s``."""
    def spread(per_s):
        return math.floor((tick + 1) * per_s / hz) - math.floor(tick * per_s / hz)
    every = max(1, round(rate["burst_every_s"] * hz))
    trades = spread(rate["trade_per_s"]) + (rate["burst_trades"] if tick and tick % every == 0 else 0)
    return trades, spread(rate["quote_per_s"]), spread(rate["mark_per_s"])


def seed_opens(now_ms, count=MD_RATE["seed_bars"]):
    """Open times of the ``count`` closed bars just before the current minute (oldest first)."""
    current = now_ms // BAR_MS * BAR_MS
    return [current - (count - i) * BAR_MS for i in range(count)]


def bars_due(now_ms, last_open_ms, delay_ms=MD_RATE["bar_delay_ms"]):
    """Opens of every bar after ``last_open_ms`` whose close + delay has passed, in order (no gap after a stall)."""
    due, nxt = [], last_open_ms + BAR_MS
    while nxt + BAR_MS + delay_ms <= now_ms:
        due.append(nxt)
        nxt += BAR_MS
    return due


class MarketDataSource:
    """The synthetic venue's TRADE / QUOTE / BAR / MARK events, per tick of the publisher loop (no I/O).

    Each event is {venue, feed, id, frame | frames, extra}: id is the identity the TS notification carries back
    (trade id, quote event ms, bar open ms, mark observation ms); a frame is stamped by ``clock`` when it is
    built (an OKX MARK event is its mark + index pair). Trade ids and quote / mark sequences only increase; bars
    are contiguous from the seed on, each sent once after its close.
    """

    def __init__(self, venues=VENUES, rate=MD_RATE, clock=time.time_ns, index_symbols=None):
        self.venues, self.rate, self.clock = tuple(venues), rate, clock
        self.index_symbols = dict(INDEX_SYMBOLS if index_symbols is None else index_symbols)
        self.mark_seq = {v: 0 for v in self.venues}
        self.mark_ms = {v: 0 for v in self.venues}
        self.trade_id = {v: 0 for v in self.venues}
        self.quote_seq = {v: 0 for v in self.venues}
        self.quote_ms = {v: 0 for v in self.venues}
        self.last_open = {v: None for v in self.venues}
        self.minute_trades = {v: {} for v in self.venues}

    def _bar(self, venue, open_ms, at_ns, seed):
        frame = bar_frame(venue, open_ms, at_ns, self.minute_trades[venue].pop(open_ms, 0))
        self.last_open[venue] = open_ms
        return {"venue": venue, "feed": "BAR", "id": str(open_ms), "frame": frame,
                "extra": {"close_ms": open_ms + BAR_MS, "seed": seed}}

    def seed(self, now_ns):
        """The closed bars just before the current minute (a retained view for the writer's BAR warmup)."""
        return [self._bar(venue, open_ms, now_ns, True) for venue in self.venues
                for open_ms in seed_opens(now_ns // 1_000_000, self.rate["seed_bars"])]

    def tick(self, tick, hz):
        events = []
        trades, quotes, marks = market_plan(tick, hz, self.rate)
        for venue in self.venues:
            for _ in range(trades):
                self.trade_id[venue] += 1
                at_ns = self.clock()
                minute = at_ns // 1_000_000 // BAR_MS * BAR_MS
                self.minute_trades[venue][minute] = self.minute_trades[venue].get(minute, 0) + 1
                events.append({"venue": venue, "feed": "TRADE", "id": str(self.trade_id[venue]),
                               "frame": trade_frame(venue, self.trade_id[venue], at_ns), "extra": {}})
            for _ in range(quotes):
                at_ns = self.clock()
                if at_ns // 1_000_000 <= self.quote_ms[venue]:
                    continue  # the quote identity is its event ms: never two in one ms
                self.quote_seq[venue] += 1
                self.quote_ms[venue] = at_ns // 1_000_000
                events.append({"venue": venue, "feed": "QUOTE", "id": str(self.quote_ms[venue]),
                               "frame": quote_frame(venue, self.quote_seq[venue], at_ns), "extra": {}})
            for _ in range(marks):
                at_ns = self.clock()
                if at_ns // 1_000_000 <= self.mark_ms[venue]:
                    continue  # the mark identity is its observation ms
                self.mark_seq[venue] += 1
                self.mark_ms[venue] = at_ns // 1_000_000
                events.append({"venue": venue, "feed": "MARK", "id": str(self.mark_ms[venue]), "extra": {},
                               "frames": mark_frames(venue, self.mark_seq[venue], at_ns,
                                                     self.index_symbols.get(venue))})
            if self.last_open[venue] is not None:
                for open_ms in bars_due(self.clock() // 1_000_000, self.last_open[venue], self.rate["bar_delay_ms"]):
                    events.append(self._bar(venue, open_ms, self.clock(), False))
        return events


def lane_feed(provider_kind):
    """The lane of a sealed core binding by provider kind (TRADE/QUOTE/BAR/MARK/INDEX), None otherwise."""
    return next((f for suffix, f in MD_KIND_FEED.items() if provider_kind.endswith(suffix)), None)


def md_bindings(config):
    """(venue, lane) -> the sealed core binding of each TRADE/QUOTE/BAR/MARK lane (and the OKX INDEX
    component); exactly one each."""
    out = {}
    for binding in config["core"]["bindings"]:
        if binding.get("l2"):
            continue
        feed = lane_feed(binding["provider_kind"])
        if feed is None or (binding["venue"], feed) in out:
            raise ValueError("unexpected or duplicate market-data binding " + binding["source_id"])
        out[(binding["venue"], feed)] = binding
    expected = {(venue, feed) for venue in VENUES for feed in MD_FEEDS} | {(v, "INDEX") for v in INDEX_SYMBOLS}
    if set(out) != expected:
        raise ValueError("sealed TRADE/QUOTE/BAR/MARK lane per venue (and the OKX INDEX component) required")
    return out


def md_envelope(binding, frame, ns):
    """Raw envelope of a TRADE/QUOTE/BAR/MARK test frame on its own provider lane (session
    ``<ns>.<venue>.<lane>``, generation 1, never changed): the BOOK lane's resync generations never fence these
    lanes. A MARK/INDEX component carries its physical symbol/channel (the core's binding key)."""
    from qdl.raw.envelope import build_raw_envelope
    import uuid
    namespace(ns)
    if frame.get("test_provenance") is not True or frame.get("provenance") != "TEST_ONLY" \
            or frame.get("adapter_version") != TEST_ADAPTER or frame.get("transport") != "WEBSOCKET":
        raise ValueError("explicit test provenance and websocket transport required")
    symbol = binding.get("physical_native_symbol") or binding["native_symbol"]
    channel = binding.get("physical_native_channel") or binding["native_channel"]
    if (binding["venue"], symbol) != (frame["venue"], frame["native_symbol"]) or \
            lane_feed(binding["provider_kind"]) != frame["feed"]:
        raise ValueError("frame/binding identity mismatch")
    payload = json.dumps(frame["payload"], separators=(",", ":"), allow_nan=False).encode()
    return build_raw_envelope(
        capture_id=uuid.uuid4().bytes, provider=binding["provider"], venue=binding["venue"],
        market=binding["market"], product_type=binding["product_type"],
        native_symbol=symbol, native_channel=channel,
        subscription_id=binding["source_id"], source_session_id=f"{ns}.{binding['venue']}.{frame['lane']}",
        connection_generation=1, lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
        received_at_ns=frame["received_at_ns"], transport_protocol=1, transport_compression=1,
        capture_boundary=2, raw_frame_bytes=payload, adapter_version=TEST_ADAPTER, config_revision=1,
        instrument_catalog_revision=binding["instrument_catalog_revision"],
        correlation_id=str(uuid.uuid4()), test_provenance=True)


def md_core_binding(book, source, acquisition):
    """Core binding of one TRADE/QUOTE/BAR lane: the BOOK binding's exported instrument identity, the source
    catalog's source, the production acquisition mapping's provider kind / channel / sequence policy."""
    if acquisition.get("mode") != "RUST_NATIVE":
        raise ValueError("native acquisition lane required: " + source["binding_id"])
    if source["instrument_uid"] != book["instrument_uid"] or source["source"]["provider"] != book["provider"]:
        raise ValueError("source/instrument identity mismatch: " + source["binding_id"])
    binding = {key: book[key] for key in ("provider", "venue", "market", "product_type", "native_symbol",
                                          "instrument_uid", "instrument_id", "instrument_revision",
                                          "instrument_catalog_revision")}
    binding.update(native_channel=acquisition["native_channel"], provider_kind=acquisition["provider_kind"],
                   source_id=source["source"]["source_id"], source_role=source["source"]["source_role"],
                   normalizer_version=source["source"]["normalizer_version"],
                   require_final_bar=bool(source["quality"]["require_final_bar"]),
                   sequence_policy=acquisition["sequence_policy"])
    return binding


def md_core_bindings(book, source, acquisition):
    """All core bindings of one lane: one, or the MARK_INDEX physical components exactly as the production
    deployment emits them (StableMarkIndexAcquisition.component_entries + the core mapping): Binance one BOTH
    component, OKX a MARK component and an INDEX component on the explicit index symbol, each with its signed
    quiet cadence."""
    binding = md_core_binding(book, source, acquisition)
    mark_index = acquisition.get("mark_index")
    if source["feed"] != "MARK_INDEX_PRICE":
        if mark_index is not None:
            raise ValueError("mark/index acquisition on a non-MARK lane")
        return [binding]
    cadence = mark_index["component_quiet_after_ms"]
    if mark_index["provider_protocol"] == "BINANCE_MARK_PRICE":
        entries = [(book["native_symbol"], acquisition["native_channel"], "binance_usdm_mark_index", "BOTH")]
    elif mark_index["provider_protocol"] == "OKX_MARK_INDEX":
        entries = [(book["native_symbol"], "mark-price", "okx_mark_price", "MARK"),
                   (str(mark_index["index_native_symbol"]), "index-tickers", "okx_index_price", "INDEX")]
    else:
        raise ValueError("MARK_INDEX provider protocol is not certified")
    out = []
    for symbol, channel, kind, component in entries:
        item = dict(binding, provider_kind=kind, physical_native_symbol=symbol, physical_native_channel=channel,
                    mark_index={"component": component})
        if cadence.get(component) is not None:
            item["mark_index"]["quiet_after_ms"] = cadence[component]
        out.append(item)
    return out


def _rewrite(path, value):
    """Replace one non-private material file in place (same 0644 mode the DR05 material used)."""
    path = Path(path)
    temporary = path.with_name(path.name + ".g12-tmp")
    with temporary.open("x") as handle:
        os.chmod(temporary, 0o644)
        handle.write(value if isinstance(value, str) else json.dumps(value))
    os.replace(temporary, path)


def seal_market_feeds(work, ns):
    """Add TRADE / QUOTE / BAR 1m / MARK_INDEX_PRICE of the two sealed instruments to the DR05 BOOK material
    (offline, no I/O outside ``work`` and the read-only source catalog / production acquisition plan): core.json,
    catalog.yaml, both manifests, route bindings, ts-consumers and bundle.json. Each generated acquisition must
    equal the production plan's entry for that binding. Once per run (handoff "g12_feeds")."""
    import yaml
    from qdl.query import ConsumerGrade, FeedType
    from qdl.runtime.production_catalog import ProductionCatalogBuilder, ProductionDemand
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from scripts.kn_gateway_bundle import compile_bundle
    from scripts.kn_native_slice_probe import ISSUER, AUDIENCE
    from scripts.p184_dr05_case import BOOK_FEEDS, consumer_packet
    from scripts.p184_dr05_dl_fault import source_catalog_path

    work = Path(work)
    handoff = json.loads((work / "handoff.json").read_text())
    if namespace(handoff["namespace"]) != namespace(ns) or handoff.get("g12_feeds") is not None:
        raise ValueError("unsealed G12 material of this namespace required")
    config = json.loads((work / "core.json").read_text())
    books = config["core"]["bindings"]
    if any(not b.get("l2") for b in books) or sorted(b["venue"] for b in books) != sorted(VENUES):
        raise ValueError("the DR05 two-venue BOOK material is required")
    source = yaml.safe_load(source_catalog_path(work).read_text())
    catalog = yaml.safe_load((work / "catalog.yaml").read_text())
    production = {item["binding_id"]: item for item in
                  yaml.safe_load((ROOT / PRODUCTION_ACQUISITION).read_text())["bindings"]}
    lanes = []
    for book in books:
        for feed in MD_FEEDS:
            interval = BAR_INTERVAL if feed == "BAR" else None
            matches = [b for b in source["bindings"] if b["instrument_uid"] == book["instrument_uid"]
                       and b["feed"] == CATALOG_FEED[feed] and b.get("interval") == interval
                       and b["source"]["source_role"] == "PRIMARY"]
            if len(matches) != 1:
                raise ValueError(f"exactly one primary {feed} source required for {book['instrument_id']}")
            planned = production.get(matches[0]["binding_id"]) or {}
            mark = ({"max_freshness_ms": MD_REQUIREMENTS["MARK"]["max_freshness_ms"], "require_live": True,
                     "index_native_symbol": (planned.get("mark_index") or {}).get("index_native_symbol")}
                    if feed == "MARK" else {})
            demand = ProductionDemand(
                consumer_id=ns + "." + book["venue"].lower(), consumer_grade=ConsumerGrade.EXECUTION,
                venue=book["venue"], market=book["market"], product_type=book["product_type"],
                native_symbol=book["native_symbol"], feed=FeedType(CATALOG_FEED[feed]), interval=interval,
                source_policy_id=matches[0]["source"]["source_policy_id"], **mark)
            acquisition = ProductionCatalogBuilder._acquisition(matches[0]["binding_id"], demand)
            fields = ("mode", "provider_kind", "native_channel", "sequence_policy", "mark_index")
            if {k: acquisition.get(k) for k in fields} != {k: planned.get(k) for k in fields}:
                raise ValueError("generated acquisition differs from the production plan: "
                                 + matches[0]["binding_id"])
            entry = copy.deepcopy(matches[0])
            entry["source"]["adapter_version"] = TEST_ADAPTER
            lanes.append((book, feed, entry, md_core_bindings(book, matches[0], acquisition)))
    config["core"]["bindings"] = books + [item for lane in lanes for item in lane[3]]
    digest = hashlib.sha256(json.dumps(config["core"]["bindings"], sort_keys=True).encode()).hexdigest()
    config["authority"].update(capability_manifest_digest=digest, contract_digest=digest,
                               partition_plan_digest=digest)
    catalog["bindings"] = catalog["bindings"] + [lane[2] for lane in lanes]
    _rewrite(work / "core.json", config)
    _rewrite(work / "catalog.yaml", yaml.safe_dump(catalog))
    loaded = StableSourceCatalog.load(work / "catalog.yaml")
    manifests = []
    for book in books:
        label = book["venue"].lower()  # the linear scopes: scope id == venue
        path = work / (label + ".yaml")
        mapping = yaml.safe_load(path.read_text())
        template = next(r for r in mapping["spec"]["requirements"] if r["feed"] == "BOOK_SNAPSHOT")
        for _, feed, entry, _ in (lane for lane in lanes if lane[0] is book):
            requirement = {k: v for k, v in template.items() if k not in ("interval", "event_recency_policy",
                                                                          "max_session_liveness_ms")}
            requirement.update(feed=CATALOG_FEED[feed], source_policy_id=entry["source"]["source_policy_id"],
                               bar_revision_policy="LATEST", **MD_REQUIREMENTS[feed])
            mapping["spec"]["requirements"].append(requirement)
        mapping["spec"]["quotas"]["max_streams"] = 50  # the TS paper manifest's; 6 slices per clone now
        _rewrite(path, yaml.safe_dump(mapping))
        consumer, route = consumer_packet(path, loaded, book["venue"], ISSUER, AUDIENCE,
                                          feeds=BOOK_FEEDS + tuple(CATALOG_FEED[f] for f in MD_FEEDS))
        _rewrite(work / (label + ".binding.json"), route)
        _rewrite(work / ("ts-consumer." + label + ".json"), consumer)
        manifests.append(path)
    _rewrite(work / "bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml",
                                                  manifest_paths=manifests))
    handoff["g12_feeds"] = {"feeds": list(BOOK_FEEDS) + [CATALOG_FEED[f] for f in MD_FEEDS],
                            "bar_interval": BAR_INTERVAL, "source_ids": [lane[2]["source"]["source_id"] for lane in lanes],
                            "core_bindings_sha256": digest}
    (work / "handoff.json").write_text(json.dumps(handoff, indent=2) + "\n")
    return handoff["g12_feeds"]


def with_execution_mark_index(build):
    """Wrap ``build_stable_query_stack`` with the production KN execution MARK/INDEX wiring of
    ``qdl.runtime.stable`` (query backend kn3): reference data enabled, execution reads from the market cache
    (``build_cache_execution_mark_index_reader``), alpha reads likewise. No acquisition quiet policy and no
    session-liveness reader: the sealed requirement is event-age BLOCK, which needs neither."""
    from qdl.reference.local_mark_index import (
        build_cache_alpha_mark_index_reader, build_cache_execution_mark_index_reader)

    def stack(*, catalog, backend, **kwargs):
        kwargs.update(
            reference_data_enabled=True,
            execution_mark_index_reader=build_cache_execution_mark_index_reader(
                catalog=catalog, backend=backend, acquisition=None, session_liveness_reader=None),
            alpha_mark_index_reader_factory=lambda: build_cache_alpha_mark_index_reader(
                catalog=catalog, backend=backend))
        return build(catalog=catalog, backend=backend, **kwargs)
    return stack


def isolated_state_topics(ns):
    """The market-cache reader's state-topic settings of this namespace (the projector's own topics; it takes
    their partition counts from the topic metadata, ``topics()`` creates them)."""
    from qdl.runtime.kn_market_cache import (
        BARS_PARTITIONS_ENV, BARS_TOPIC_ENV, LATEST_PARTITIONS_ENV, LATEST_TOPIC_ENV)
    wanted = topics(ns)
    return {LATEST_TOPIC_ENV: ns + ".latest", LATEST_PARTITIONS_ENV: str(wanted[ns + ".latest"][0]),
            BARS_TOPIC_ENV: ns + ".bars", BARS_PARTITIONS_ENV: str(wanted[ns + ".bars"][0])}


def with_isolated_state_topics(build_reader, ns):
    """Wrap ``reader_from_environment`` so the Query reads this namespace's state topics.

    The DR08 ``serve_query`` passes a literal two-key environment, so the reader fell back to the production
    defaults ``md.latest.v2`` / ``md.bars.v2`` with 6 partitions. A BAR read takes its source boundary from the
    bars checkpoint ``ckpt:<bars topic>:<state partition>``; under the wrong topic that hash does not exist, every
    BAR history is ``SOURCE_BOUNDARY_UNKNOWN`` (typed DATA_NOT_READY) and the writer's BAR slice never starts
    (G12 run 2026-10-11 p184-g12-rc10-full: 22 warmup refusals, BAR never applied; latest feeds were unaffected
    because a latest read also proves its boundary from the entry's own offset)."""
    extra = isolated_state_topics(ns)

    def reader(environ):
        return build_reader({**dict(environ), **extra})
    return reader


def serve_query(work):
    """In-namespace Query (--query): the unchanged DR05 diagnostic Query (DR08 serve_query, public HTTP boundary,
    KN market cache + hot view) of the code under test, with execution MARK/INDEX reads enabled and the cache
    reader on this namespace's state topics."""
    import qdl.runtime.kn_market_cache as kn_market_cache
    import qdl.runtime.stable_source as stable_source
    from scripts.p184_dr05_observer import diagnostic_query
    ns = namespace(json.loads((Path(work) / "packet.json").read_text())["namespace"])
    kn_market_cache.reader_from_environment = with_isolated_state_topics(kn_market_cache.reader_from_environment, ns)
    stable_source.build_stable_query_stack = with_execution_mark_index(stable_source.build_stable_query_stack)
    diagnostic_query(Path(work))


def extend_material(work):
    """In-container --feeds (network none): attested namespace / image set, then seal_market_feeds."""
    packet = _packet(work)
    return seal_market_feeds(work, packet["namespace"])


def publish(work, out, seconds, hz):
    """Steady provider-shaped BOOK frames per venue; resync (new generation +
    snapshot) only after a failed delivery, as a venue adapter would. TRADE /
    QUOTE / closed BAR 1m / MARK-INDEX frames on their own lanes at the MD_RATE
    cadence; their delivery errors are recorded, never a BOOK resync."""
    from confluent_kafka import Producer
    from scripts.p184_dr05_dl_fault import frame
    from scripts.p184_dr05_raw_publisher import raw_envelope
    packet = _packet(work)
    ns = packet["namespace"]
    config = json.loads((Path(work) / "core.json").read_text())
    if config.get("raw_topics") != [ns + ".raw"] or config["core"].get("allow_test_provenance") is not True:
        raise ValueError("isolated test-only raw topic required")
    bindings = {b["venue"]: b for b in config["core"]["bindings"] if b.get("l2")}
    lanes = md_bindings(config)
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

        def md_ack(event, sent_ns):
            def callback(error, message):
                row = {"k": "ack_md", "venue": event["venue"], "feed": event["feed"], "id": event["id"],
                       "ack_ms": round((time.time_ns() - sent_ns) / 1e6, 3)}
                if error is not None:
                    row["error"] = str(error.code())
                else:
                    row["offset"] = message.offset()
                log.write(json.dumps(row) + "\n")
            return callback

        def send_md(event):
            frames = event.get("frames") or [event["frame"]]  # an OKX MARK event is its mark + index pair
            at_ns = frames[0]["received_at_ns"]
            for item in frames:
                raw = md_envelope(lanes[(event["venue"], item["feed"])], item, ns)
                try:
                    producer.produce(config["raw_topics"][0], partition=0, key=raw.subscription_id.encode(),
                                     value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                                     on_delivery=md_ack(event, at_ns))
                except BufferError:
                    log.write(json.dumps({"k": "ack_md", "venue": event["venue"], "feed": event["feed"],
                                          "id": event["id"], "error": "LOCAL_QUEUE_FULL"}) + "\n")
                    return
            log.write(json.dumps({"k": "pub_md", "t": at_ns, "venue": event["venue"], "feed": event["feed"],
                                  "id": event["id"], **event["extra"]}) + "\n")

        source = MarketDataSource(index_symbols={venue: lane["physical_native_symbol"]
                                                 for (venue, feed), lane in lanes.items() if feed == "INDEX"})
        for event in source.seed(time.time_ns()):
            send_md(event)
        tick = 0
        next_at = time.monotonic()
        while time.monotonic() < end and not stop.requested:
            for event in source.tick(tick, hz):
                send_md(event)
            tick += 1
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


def kafka_sample(source, admin, roster, rosters, groups, phase=lambda name: None):
    """The Kafka part of one monitor sample (cluster/ISR, projector committed vs LSO, group membership).
    ``phase(name)`` is announced before each admin call, so a native crash names the call it died in."""
    from scripts.kn_recovery_observe import Deadline
    row = {}
    phase("describe_cluster")
    try:
        row["cluster"] = source.describe_cluster(Deadline(3.0))
    except Exception as error:  # noqa: BLE001 - recorded, never inferred
        row["cluster_error"] = type(error).__name__
    phase("describe_topics")
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
            phase("list_consumer_group_offsets:" + group)
            committed = source.committed_offsets(group, group_roster, Deadline(3.0))
            phase("list_offsets:" + group)
            ends = source.log_end_offsets(sorted(committed), Deadline(3.0))
            lag[group] = sum(max(0, ends[tp] - max(committed[tp], 0)) for tp in committed)
        row["lag"] = lag
        row["projector_lag"] = lag["kn-projector-v3-a"]
    except Exception as error:  # noqa: BLE001
        row["lag_error"] = type(error).__name__
    phase("describe_consumer_groups")
    try:
        described = admin.describe_consumer_groups(list(groups), request_timeout=3)
        row["groups"] = {}
        for group, future in described.items():
            value = future.result(timeout=4)
            row["groups"][group] = {"state": str(getattr(value, "state", "")).split(".")[-1],
                                    "members": len(value.members or ())}
    except Exception as error:  # noqa: BLE001
        row["groups_error"] = type(error).__name__
    phase("done")
    return row


def local_sample(work):
    """The non-Kafka part of one monitor sample: Query/Stream ports, disk, projector status file."""
    import socket
    row = {"ports": {}}
    for port in (8200, 8210):
        try:
            with socket.create_connection(("localhost", port), timeout=.5):
                row["ports"][str(port)] = True
        except OSError:
            row["ports"][str(port)] = False
    row["disk_free_bytes"] = shutil.disk_usage(str(work)).free
    try:
        row["projector_status_mtime"] = (Path(work) / "g12-projector-status.json").stat().st_mtime
    except FileNotFoundError:
        row["projector_status_mtime"] = None
    return row


def exit_reason(code):
    """A child's exit as a typed reason: SIGSEGV / SIGKILL / ... for a signal, EXIT_<n> otherwise."""
    if code is not None and code < 0:
        try:
            return signal.Signals(-code).name
        except ValueError:
            return f"SIGNAL_{-code}"
    return f"EXIT_{code}"


def _sample_child(conn, make_sampler):
    """Forked sampler: builds its own client on first use, answers one sample per request."""
    sampler = None
    while True:
        try:
            request = conn.recv()
        except (EOFError, OSError):
            return
        if request is None:
            return
        try:
            if sampler is None:
                sampler = make_sampler()
            fields = sampler(lambda name: conn.send(("phase", name)))
        except Exception as error:  # noqa: BLE001 - typed back to the parent, then this child ends
            conn.send(("error", type(error).__name__ + ":" + str(error)[:200]))
            return
        conn.send(("row", fields))


class SampleSupervisor:
    """Runs each sample in a long-lived forked child with a hard per-sample timeout.

    A native crash (librdkafka SIGSEGV, G12 run 2026-10-11 full2: the monitor exited 139 one second into a
    broker stop) or a hang loses only the sample in flight: it comes back as a typed error ({error, phase,
    exitcode}) and the next sample starts a fresh child. The parent never loads the Kafka client (fork-safe).
    """

    def __init__(self, make_sampler, timeout_s, context=None):
        import multiprocessing
        self.make_sampler, self.timeout_s = make_sampler, timeout_s
        self.context = context or multiprocessing.get_context("fork")
        self.proc = self.conn = None
        self.restarts, self.started = 0, False

    def _start(self):
        parent, child = self.context.Pipe()
        self.proc = self.context.Process(target=_sample_child, args=(child, self.make_sampler), daemon=True)
        self.proc.start()
        child.close()
        self.conn = parent

    def _stop(self, kill):
        proc, conn, self.proc, self.conn = self.proc, self.conn, None, None
        if proc is None:
            return None
        if kill and proc.is_alive():
            proc.kill()
        proc.join(5)
        if conn is not None:
            conn.close()
        return proc.exitcode

    def sample(self):
        """(fields, None) or (None, error)."""
        if self.proc is None or not self.proc.is_alive():
            if self.proc is not None:
                self._stop(kill=True)
            if self.started:
                self.restarts += 1
            self._start()
            self.started = True
        phase, limit = None, time.monotonic() + self.timeout_s
        try:
            self.conn.send("sample")
        except (BrokenPipeError, OSError):
            code = self._stop(kill=True)
            return None, {"error": exit_reason(code), "phase": None, "exitcode": code}
        while True:
            left = limit - time.monotonic()
            if left <= 0:
                code = self._stop(kill=True)
                return None, {"error": "TIMEOUT", "phase": phase, "exitcode": code}
            try:
                if self.conn.poll(min(left, .05)):
                    kind, value = self.conn.recv()
                    if kind == "phase":
                        phase = value
                        continue
                    if kind == "row":
                        return value, None
                    code = self._stop(kill=True)
                    return None, {"error": "EXCEPTION", "detail": value, "phase": phase, "exitcode": code}
            except (EOFError, OSError):
                pass
            if not self.proc.is_alive():
                code = self._stop(kill=False)
                return None, {"error": exit_reason(code), "phase": phase, "exitcode": code}

    def close(self):
        if self.conn is not None:
            try:
                self.conn.send(None)
            except (BrokenPipeError, OSError):
                pass
        self._stop(kill=True)


def monitor(work, out, seconds, period=1.0):
    """Read-only 1 s observation: cluster/ISR, projector committed vs LSO,
    group membership, Query/Stream ports, disk. No group join or commit.

    The Kafka admin calls run in a supervised child (SampleSupervisor): a crash or
    hang of one sample is written as ``{"k": "sample_error", "error": "SIGSEGV" |
    "TIMEOUT" | ..., "phase": <the admin call>}`` and never stops the monitor; the
    G12 analysis excludes those rows and refuses a fault whose clearance or
    catch-up falls in a monitor gap (MONITOR_MAX_GAP_S)."""
    packet = _packet(work)
    ns = packet["namespace"]
    roster = topics(ns)
    rosters = {"kn-projector-v3-a": {ns + ".canonical": roster[ns + ".canonical"][0]},
               "kn-projector-v3-b": {ns + ".latest": roster[ns + ".latest"][0],
                                     ns + ".bars": roster[ns + ".bars"][0]}}
    groups = (*PROJECTOR_GROUPS, "kn-" + ns, ns)

    def make_sampler():
        from confluent_kafka.admin import AdminClient
        from scripts.kn_recovery_observe import KafkaAdminSource
        admin = AdminClient(_security(packet))
        source = KafkaAdminSource(admin)
        return lambda phase: kafka_sample(source, admin, roster, rosters, groups, phase)

    supervisor = SampleSupervisor(make_sampler, LIMITS["monitor_sample_s"])
    stop, end = _Stop(), time.monotonic() + seconds
    try:
        with open(out, "a", buffering=1) as log:
            while time.monotonic() < end and not stop.requested:
                started = time.monotonic()
                t = time.time_ns()
                fields, error = supervisor.sample()
                if error is not None:
                    row = {"t": t, "k": "sample_error", **error, "restarts": supervisor.restarts}
                else:
                    row = {"t": t, **fields, **local_sample(work)}
                log.write(json.dumps(row, default=list) + "\n")
                time.sleep(max(0.0, period - (time.monotonic() - started)))
            log.write(json.dumps({"k": "end", "t": time.time_ns(),
                                  "reason": "SIGTERM" if stop.requested else "seconds"}) + "\n")
    finally:
        supervisor.close()
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


def events_argv(ns, since_s, until_s):
    """Bounded read-only ``docker events`` of this namespace's containers: die (exitCode), oom, kill (signal).
    ``--until`` makes the follower exit by itself even if the orchestrator is killed."""
    namespace(ns)
    if not 0 < since_s < until_s:
        raise ValueError("bounded event window required")
    return ["docker", "events", "--since", str(int(since_s)), "--until", str(int(until_s)),
            "--filter", f"label={LABEL}={ns}", "--filter", "type=container", "--filter", "event=die",
            "--filter", "event=oom", "--filter", "event=kill", "--format", "{{json .}}"]


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
        self.events_proc = None

    # -- container exit states
    def start_events(self):
        """Follow this namespace's die/oom/kill events into the evidence for the whole run (bounded)."""
        now = time.time()
        out = (self.evidence / "container-events.jsonl").open("a")
        err = (self.evidence / "container-events.stderr").open("a")
        try:
            self.events_proc = subprocess.Popen(
                events_argv(self.ns, now - 1, now + LIMITS["deadline_s"] + 900), stdout=out, stderr=err,
                stdin=subprocess.DEVNULL)
        finally:
            out.close()
            err.close()

    def exit_states(self, roles=(), wait_s=3.0):
        """Exit states recorded so far; waits (bounded) until each named role has a die event."""
        limit = time.monotonic() + wait_s
        while True:
            states = exit_states(load_jsonl(self.evidence / "container-events.jsonl"))
            if all(role in states for role in roles) or time.monotonic() >= limit:
                return states
            time.sleep(.2)

    def stop_events(self):
        proc, self.events_proc = self.events_proc, None
        if proc is None:
            return None
        time.sleep(1.0)  # the last teardown die events reach the follower
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        return proc.returncode

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
            roles = sorted(name[len(self.ns) + 1:] for name in missing)
            states = self.exit_states(roles)
            detail = {role: ({k: states[role][k] for k in ("exit_code", "oom_killed")} if role in states
                             else "NO_DIE_EVENT") for role in roles}
            self.note("role_exited", roles=roles, exit_states={r: states.get(r) for r in roles})
            raise RuntimeError("STOP role exited: " + ",".join(sorted(missing)) + " " + json.dumps(detail))
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
        self.start_events()
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
        feeds = sh(self.helper(self.ns, self.work, ["--feeds", "/work"], network="none"), timeout=180)
        self.note("feeds_sealed", result=feeds.strip()[-400:])
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
        # Fail fast instead of an hour of NOT_MEASURED: every clone applies TRADE, QUOTE and a closed BAR.
        if not self.wait(LIMITS["md_ready_s"], until=lambda: not md_missing(
                load_jsonl(self.evidence / "sampler.jsonl"))):
            raise RuntimeError("STOP TS clones never applied: " + ",".join(
                md_missing(load_jsonl(self.evidence / "sampler.jsonl"))))
        self.note("ts_market_data_applied")

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
            raise RuntimeError(f"STOP {name}: G12 verdict not PASS: " + json.dumps(verdict.get("parts")))

    def keep_logs(self):
        """Bounded log tails of every owned container (--rm loses them at stop), and each TS writer clone's own
        stderr file (the writer logs there, not to docker logs; the file is erased with work/tslogs)."""
        logs = self.evidence / "logs"
        logs.mkdir(exist_ok=True)
        for name in owned_names(self.ns):
            result = subprocess.run(["docker", "logs", "--tail", "120", name], capture_output=True,
                                    text=True, timeout=30)
            (logs / (name[len(self.ns) + 1:] + ".log")).write_text(
                (result.stdout + result.stderr)[-60000:])
        for venue, clone in self.clones.items():
            (logs / f"tswriter-{venue.lower()}.stderr.log").write_text(clone.stderr_tail(60000))

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
        try:
            report["events_follower_exit"] = self.stop_events()
            states = exit_states(load_jsonl(self.evidence / "container-events.jsonl"))
            (self.evidence / "exit-states.json").write_text(json.dumps(states, indent=2) + "\n")
            report["exit_states"] = {role: {k: s[k] for k in ("exit_code", "oom_killed")}
                                     for role, s in sorted(states.items())}
        except Exception as error:  # noqa: BLE001 - evidence is best effort, teardown is not
            report["errors"].append("exit_states:" + type(error).__name__)
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


def keep_diagnostics(out):
    """Detached --rm roles lose their stderr when they exit: write it (and a native
    crash's faulthandler dump) next to the evidence file (final-image attempt 3,
    2026-10-10: the monitor exited during the broker fault with no log left)."""
    if not out:
        return None
    import faulthandler
    stream = open(str(out) + ".stderr", "a", buffering=1)
    sys.stderr = stream
    faulthandler.enable(file=stream, all_threads=True)
    stream.write(json.dumps({"k": "start", "t": time.time_ns(), "pid": os.getpid()}) + "\n")
    return stream


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("run", "down", "analyze", "topics", "publish", "monitor", "feeds", "query"):
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
    if args.feeds:
        print(json.dumps(extend_material(args.feeds)))
        return 0
    if args.query:
        serve_query(args.query)
        return 0
    if args.seconds is None and not args.topics:
        parser.error("--seconds required")
    if args.topics:
        print(json.dumps(create_topics(args.topics)))
        return 0
    keep_diagnostics(args.out)
    if args.publish:
        print(json.dumps(publish(args.publish, args.out, args.seconds, args.hz)))
    else:
        print(json.dumps(monitor(args.monitor, args.out, args.seconds)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

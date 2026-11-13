#!/usr/bin/env python3
"""DL v2.3.0 R-cert 2: live shadow of DERIVED_FINAL bars in one isolated namespace.

Purpose: the owner-approved packet
``upgrade/evidence/releases/v2.3.0/drafts/RCERT2_DERIVED_BARS_SHADOW_PACKET.md``.
For 5 OKX swaps, live 1m candles feed a disposable Data Layer slice (1 KRaft
broker, Redis cache, ingestor, core, projector, bar edge). The bar edge
derives the 13 other OKX intervals, and a comparator checks every derived bar
against the native OKX candle from public REST. That proves the gates G2.1-G2.6.
The shadow fault cases S1-S6 run on demand through ``--fault``.

Reuse, not a new engine:
- single-broker TLS material and properties follow ``p184_dr05_dl_fault.material``;
- role argv come from ``p184_g12_isolated_fault`` (``_base``, ``_env``, ``describe_until_known``);
- configs are rendered by the code under test (``StableAcquisitionPlan.core_config`` /
  ``native_ingestor_configs``, ``kn_gateway_bundle.compile_bundle``, ``stable_authority_record``);
- parity uses the rules of ``scripts/v230_derived_bar_parity.py``.

Boundary:
- every container is ``--rm``, ``--cpu-shares 256``, capped, named ``<ns>-<role>`` and
  labelled ``qdl.v230.rcert2=<ns>``;
- all roles share the holder's network namespace on the new bridge ``<ns>-net``
  (egress for public OKX WS/REST, no published port);
- the production compose project, its Kafka, Redis and Postgres, the mesh certs and every
  container not created here are never touched; production is only ``docker ps``-ed for
  the G2.6 isolation hash.

Hard caps total 1.15 CPU and about 2.3 GiB (``CAPS``; enforced by tests).

Usage:
- ``--render WORK --ns NS --rust-image ID --python-image ID``
- ``--plan WORK``
- ``--up WORK --approved TEXT --until 2026-11-16T12:00:00Z``
- ``--fault WORK S1..S6``
- ``--status WORK``
- ``--teardown WORK``
- in-container: ``--create-topics WORK``, ``--compare WORK``

No subcommand starts anything without ``--up``. ``--up`` refuses to start
without an approval text, the images present by ID, and a deadline.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LABEL = "qdl.v230.rcert2"
NS_PATTERN = r"qdl-v230-rcert2-[0-9]{8}t[0-9]{4}z"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP")
# target -> base: the chain the declaration tool renders (largest finer divisor
# with a compatible anchor). 1m stays the native realtime lane.
CHAIN = {"3m": "1m", "5m": "1m", "15m": "5m", "30m": "15m", "1h": "30m", "2h": "1h", "4h": "2h",
         "6h": "2h", "12h": "6h", "1d": "12h", "2d": "1d", "3d": "1d", "1w": "1d"}
INTERVALS = ("1m", *CHAIN)
WARMUP_ROWS = 300
BOOTSTRAP = "localhost:9092"
CLUSTER_ID = "VjIzMFJDZXJ0MlNoYWRvdw"
CACHE_URL = "redis://localhost:6379/0"
BARS_PARTITIONS = 3
# role -> (cpus, memory MiB). Packet section 3; total 1.15 CPU / 2,336 MiB.
CAPS = {"net": (0.05, 32), "kafka": (0.35, 768), "cache": (0.10, 256), "ingestor": (0.10, 128),
        "core": (0.15, 128), "projector": (0.15, 256), "baredge": (0.20, 512), "compare": (0.05, 256)}
CAP_TOTAL_CPUS = 1.15
CAP_TOTAL_MIB = 2_336
SERVICE_ORDER = ("cache", "core", "projector", "baredge", "ingestor", "compare")
TEARDOWN_ORDER = ("compare", "ingestor", "baredge", "projector", "core", "cache", "kafka", "net")
EXISTING_IMAGES = {"kafka": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
                   "redis": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
                   "holder": "alpine:3.20",
                   # S5 only: the v2.2.4 projector (production image of market_projector_1/2).
                   "old_projector": "qdl-v2-rust:2.2.4-c7ce7d3"}
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
OKX_REST = "https://www.okx.com/api/v5/market/history-candles"
COMPARE_PAUSE_S = 0.5  # <= 2 req/s on the shared public per-IP budget
COMPARE_LOOP_S = 20
GAP_AFTER_S = 300  # a target still absent this long after its close is classified
PRODUCTION_PROJECT = "qdl_v2_stable_candidate"


def namespace(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(NS_PATTERN, value):
        raise ValueError("exact disposable R-cert 2 namespace required (qdl-v230-rcert2-YYYYMMDDtHHMMz)")
    return value


def topic_names(ns: str) -> dict[str, tuple[int, dict]]:
    """name -> (partitions, config). RF1, 72 h retention; bars/latest compacted."""
    namespace(ns)
    keep = {"retention.ms": str(72 * 3_600_000)}
    state = {"cleanup.policy": "compact", "delete.retention.ms": "604800000"}
    return {ns + ".raw": (1, keep), ns + ".canonical": (1, keep), ns + ".quarantine": (1, keep),
            ns + ".latest": (BARS_PARTITIONS, state),
            ns + ".bars": (BARS_PARTITIONS, {**state, "min.compaction.lag.ms": "3600000"})}


# ------------------------------------------------------------------ render (offline)

def _select_bindings(catalog) -> dict[tuple[str, str], object]:
    """(native symbol, interval) -> the one OKX SWAP final BAR binding of the catalog."""
    selected: dict[tuple[str, str], object] = {}
    for item in catalog.bindings:
        identity = item.instrument.identity
        if (identity.venue, identity.market, item.feed.value) != ("OKX", "SWAP", "BAR"):
            continue
        key = (item.instrument.native_symbol, item.interval)
        if key[0] in SYMBOLS and key[1] in INTERVALS and item.source_role == "PRIMARY":
            if key in selected:
                raise ValueError(f"two PRIMARY BAR bindings for {key}")
            selected[key] = item
    missing = [(s, i) for s in SYMBOLS for i in INTERVALS if (s, i) not in selected]
    if missing:
        raise ValueError(f"catalog lacks BAR bindings: {missing}")
    return selected


def derived_acquisition_rows(plan_rows: list[dict], selected: dict, ns: str) -> list[dict]:
    """The acquisition rows of the shadow: native 1m, DERIVED_FINAL for the chain."""
    from qdl.adapters.intervals import okx_candle_channel

    by_id = {row["binding_id"]: row for row in plan_rows}
    rows = []
    for (symbol, interval), source in sorted(selected.items()):
        row = dict(by_id[source.binding_id])
        row["enabled"] = True
        if interval == "1m":
            if row["mode"] != "RUST_NATIVE":
                raise ValueError(f"{source.binding_id}: the 1m base must be the native lane")
        else:
            for key in ("l2", "mark_index"):
                row.pop(key, None)
            row.update(mode="DERIVED_FINAL", provider_kind="okx_bar",
                       native_channel=okx_candle_channel(interval), sequence_policy="NONE",
                       websocket_url=None, business_websocket_url=None, market_websocket_url=None,
                       derived_from={"interval": CHAIN[interval]})
        rows.append(row)
    return rows


def _tls(work: Path, ns: str, hours: int) -> str:
    """Throwaway CA, one localhost leaf (server+client), PKCS12 keystore. Returns the keystore password."""
    import secrets
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.now(timezone.utc)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ns)])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
          .not_valid_after(now + timedelta(hours=hours))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), True).sign(key, hashes.SHA256()))
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
            .issuer_name(name).public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=hours))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                  ExtendedKeyUsageOID.CLIENT_AUTH]), False)
            .sign(key, hashes.SHA256()))
    pem = serialization.Encoding.PEM
    _save(work / "ca.crt", ca.public_bytes(pem).decode())
    _save(work / "client.crt", leaf.public_bytes(pem).decode())
    _save(work / "client.key", leaf_key.private_bytes(pem, serialization.PrivateFormat.PKCS8,
                                                      serialization.NoEncryption()).decode(), private=True)
    password = secrets.token_hex(16)
    _save(work / "server.p12", pkcs12.serialize_key_and_certificates(
        b"rcert2", leaf_key, leaf, [ca], serialization.BestAvailableEncryption(password.encode())), private=True)
    return password


def broker_properties(password: str) -> str:
    """Single KRaft node, mTLS client auth (``p184_dr05_dl_fault.material``), RF1."""
    return f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=SSL://:9092,CONTROLLER://:9093
advertised.listeners=SSL://localhost:9092
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
default.replication.factor=1
min.insync.replicas=1
offsets.topic.num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
"""


def _save(path: Path, value, *, private: bool = False) -> None:
    data = value if isinstance(value, (bytes, bytearray)) else (
        value if isinstance(value, str) else json.dumps(value, indent=1, sort_keys=True)).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        os.chmod(path, 0o600 if private else 0o644)
        handle.write(data)


def render(work: Path, ns: str, *, rust_image: str, python_image: str, tls_hours: int = 120) -> dict:
    """Write every config of the shadow into ``work`` (must not exist). Starts nothing."""
    import yaml
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record
    from scripts.kn_gateway_bundle import compile_bundle
    from tests.phase7_support import manifest_mapping

    namespace(ns)
    for image in (rust_image, python_image):
        if not IMAGE_ID.fullmatch(image):
            raise ValueError("rc images must be immutable IDs sha256:<64hex>")
    work = Path(work).resolve()
    if work == ROOT or ROOT in work.parents or work.exists():
        raise ValueError("a new work directory outside the repository is required")
    work.mkdir(parents=True)
    topics = topic_names(ns)
    raw_topic, canonical_topic, quarantine_topic = ns + ".raw", ns + ".canonical", ns + ".quarantine"

    repo_catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    selected = _select_bindings(repo_catalog)
    ids = {item.binding_id for item in selected.values()}
    catalog_doc = json.loads((ROOT / "config/v2/stable-source-bindings.yaml").read_text(encoding="utf-8"))
    catalog_doc["canonical_stream"] = canonical_topic
    catalog_doc["bindings"] = [row for row in catalog_doc["bindings"] if row["binding_id"] in ids]
    _save(work / "catalog.yaml", catalog_doc)
    catalog = StableSourceCatalog.load(work / "catalog.yaml")

    plan_doc = yaml.safe_load((ROOT / "config/v2/stable-acquisition-bindings.yaml").read_text(encoding="utf-8"))
    plan_doc["topics"] = {"raw": raw_topic, "canonical": canonical_topic, "quarantine": quarantine_topic}
    plan_doc["bindings"] = derived_acquisition_rows(plan_doc["bindings"], selected, ns)
    _save(work / "acquisition.yaml", yaml.safe_dump(plan_doc, sort_keys=False))
    plan = StableAcquisitionPlan.load(work / "acquisition.yaml", catalog=catalog)
    parents = plan.derived_parents({item.binding_id: item for item in catalog.bindings})
    if len(parents) != len(SYMBOLS) * len(CHAIN):
        raise ValueError("every chain interval of every symbol must be DERIVED_FINAL")

    authority = stable_authority_record(
        rust_image_digest=rust_image.removeprefix("sha256:"),
        capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
        contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
        partition_plan=(work / "acquisition.yaml").read_bytes(),
        effective_at_ns=time.time_ns(),
    )
    _save(work / "runtime/authority.json", authority)
    core = plan.core_config(catalog=catalog, authority=authority)
    _save(work / "core.json", core)
    ingestors = plan.native_ingestor_configs(catalog=catalog, authority=authority)
    if set(ingestors) != {"okx-swap"}:
        raise ValueError(f"exactly the OKX SWAP ingestor expected, got {sorted(ingestors)}")
    ingestor = dict(ingestors["okx-swap"])
    ingestor["generation_state_path"] = "/work/state/ingestor-generations"
    ingestor["session_liveness_dir"] = "/work/state/session-liveness"
    channels = sorted({item["native_channel"] for item in ingestor["bindings"]})
    if channels != ["candle1m"] or len(ingestor["bindings"]) != len(SYMBOLS):
        raise ValueError(f"the shadow ingestor subscribes candle1m x 5 only, got {channels}")
    _save(work / "ingestor.json", ingestor)

    requirements = []
    for source in sorted(selected.values(), key=lambda value: value.binding_id):
        mapping = manifest_mapping(consumer_id="rcert2.shadow", subject="spiffe://qdl/paper/rcert2-shadow",
                                   instrument_uid=source.instrument.instrument_uid, feed="BAR",
                                   interval=source.interval, source_policy_id=source.source_policy_id)
        requirement = mapping["spec"]["requirements"][0]
        requirement["warmup_limit"] = WARMUP_ROWS
        requirements.append(requirement)
    mapping["spec"]["requirements"] = requirements
    _save(work / "manifest.yaml", yaml.safe_dump(mapping))
    _save(work / "bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml",
                                               manifest_paths=[work / "manifest.yaml"]))
    password = _tls(work, ns, tls_hours)
    _save(work / "broker.properties", broker_properties(password), private=True)
    for directory in ("kafka", "state", "evidence"):
        (work / directory).mkdir()
    packet = {"namespace": ns, "bootstrap": BOOTSTRAP, "topics": sorted(topics),
              "client_tls": {"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"},
              "images": {"rust": rust_image, "python": python_image, **EXISTING_IMAGES},
              "symbols": list(SYMBOLS), "chain": CHAIN, "derived_parents": parents,
              "rendered_at": _now(), "source_commit": _git_head(), "topic_id": ""}
    _save(work / "packet.json", packet)
    return packet


def _git_head() -> str:
    try:
        return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], check=True,
                              capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------ topology (offline)

def _base(ns: str, role: str, *, network: str, uid: str, readonly: bool = True) -> list[str]:
    cpus, mib = CAPS[role]
    argv = ["docker", "run", "-d", "--rm", "--pull=never", "--name", f"{ns}-{role}",
            "--label", f"{LABEL}={ns}", "--network", network, "--cpus", f"{cpus:.2f}",
            "--cpu-shares", "256", "--memory", f"{mib}m", "--memory-swap", f"{mib}m",
            "--pids-limit", "256", "--log-opt", "max-size=8m", "--log-opt", "max-file=2",
            "--security-opt", "no-new-privileges:true", "--cap-drop", "ALL", "--user", uid]
    if readonly:
        argv.append("--read-only")
    return argv


def _env(argv: list[str], env: dict) -> list[str]:
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    return argv


def topology(work: Path, *, uid: str | None = None, deadline_s: int = 0) -> dict[str, list[str]]:
    """docker argv per role. Executes nothing."""
    work = Path(work).resolve()
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    ns = namespace(packet["namespace"])
    images = packet["images"]
    uid = uid or f"{os.getuid()}:{os.getgid()}"
    net = "container:" + ns + "-net"
    mount = ["-v", f"{work}:/work"]
    tmp = ["--tmpfs", "/tmp:rw,size=32m,mode=1777"]
    no_proxy = {"HTTP_PROXY": "", "HTTPS_PROXY": "", "http_proxy": "", "https_proxy": "", "NO_PROXY": "*"}
    roles = {"net": _base(ns, "net", network=ns + "-net", uid=uid)
             + ["--hostname", "localhost", "--entrypoint", "sleep", images["holder"], str(max(deadline_s, 60))]}
    kafka = _base(ns, "kafka", network=net, uid=uid) + tmp + [
        "-v", f"{work}/kafka:/data", "-v", f"{work}:/work:ro"]
    for target in ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data"):
        kafka += ["--tmpfs", target + ":rw,size=1m,mode=1777"]
    roles["kafka"] = _env(kafka, {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx384m", "LOG_DIR": "/tmp/kafka-logs"}) + [
        "--entrypoint", "/bin/sh", images["kafka"], "-ec",
        f"/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t {CLUSTER_ID} -c /work/broker.properties"
        " && exec /opt/kafka/bin/kafka-server-start.sh /work/broker.properties"]
    roles["cache"] = _base(ns, "cache", network=net, uid=uid) + [
        "--tmpfs", "/data:rw,size=1m,mode=1777", "--entrypoint", "redis-server", images["redis"],
        "--save", "", "--appendonly", "no", "--maxmemory", "192mb", "--maxmemory-policy", "noeviction",
        "--hash-max-listpack-entries", "128", "--hash-max-listpack-value", "2048"]
    kafka_client = {"QDL_KAFKA_CERT_ROOT": "/work", "QDL_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
                    "QDL_KAFKA_REQUEST_TIMEOUT_SECONDS": "30"}
    roles["core"] = _env(_base(ns, "core", network=net, uid=uid) + tmp + mount,
        {**kafka_client, "QDL_KAFKA_CLIENT_ID": ns + "-core", "QDL_KAFKA_GROUP_ID": ns + "-core",
         "QDL_PROVIDER_ADMISSION_ENABLED": "false"}) + [
        "--entrypoint", "/usr/local/bin/qdl-realtime-core", images["rust"], "/work/core.json"]
    roles["projector"] = _env(_base(ns, "projector", network=net, uid=uid) + tmp + mount,
        {"QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
         "QDL_KN_KAFKA_KEY": "/work/client.key", "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP,
         "QDL_KN_BUNDLE_PATH": "/work/bundle.json", "QDL_KN_TOPIC_ID": packet["topic_id"],
         "QDL_KN_REPLICA": "a", "QDL_KN_MATERIALIZER_EPOCH": "1", "QDL_KN_MARKET_CACHE_URL": CACHE_URL,
         "QDL_KN_STATUS_PATH": "/work/state/projector-status.json", "QDL_KN_STATUS_INTERVAL_S": "5",
         "QDL_KN_CANONICAL_TOPIC": ns + ".canonical", "QDL_KN_LATEST_TOPIC": ns + ".latest",
         "QDL_KN_BARS_TOPIC": ns + ".bars"}) + [
        "--entrypoint", "/usr/local/bin/qdl-projector", images["rust"], "run"]
    roles["baredge"] = _env(_base(ns, "baredge", network=net, uid=uid) + tmp + mount,
        {**kafka_client, **no_proxy, "QDL_KAFKA_CLIENT_ID": ns + "-baredge", "QDL_ENVIRONMENT": "paper",
         "QDL_STABLE_SOURCE_BINDINGS": "/work/catalog.yaml",
         "QDL_STABLE_ACQUISITION_BINDINGS": "/work/acquisition.yaml",
         "QDL_STABLE_RUNTIME_DIR": "/work/runtime", "QDL_STABLE_BAR_READBACK": "kn3",
         "QDL_KN3_MARKET_CACHE_URL": CACHE_URL, "QDL_KN3_ENVIRONMENT": "paper",
         "QDL_KN_BARS_TOPIC": ns + ".bars", "QDL_KN_BARS_PARTITIONS": str(BARS_PARTITIONS),
         "QDL_STABLE_BAR_DEMAND_BUNDLE": "/work/bundle.json", "QDL_STABLE_BAR_WARMUP_ROWS": str(WARMUP_ROWS),
         "QDL_STABLE_BAR_MAX_CONCURRENT_REQUESTS": "2",
         "QDL_STABLE_BAR_STATE_PATH": "/work/state/bar-edge-checkpoint.json",
         "QDL_STABLE_HEARTBEAT_PATH": "/work/state/bar-edge-heartbeat.json"}) + [
        "--entrypoint", "python", images["python"], "-m", "app.entrypoints.binance_bar_v2_stable"]
    roles["ingestor"] = _env(_base(ns, "ingestor", network=net, uid=uid) + tmp + mount,
        {**kafka_client, "QDL_KAFKA_CLIENT_ID": ns + "-ingestor", "QDL_KAFKA_GROUP_ID": ns + "-ingestor"}) + [
        "--entrypoint", "/usr/local/bin/qdl-native-raw-ingestor", images["rust"], "/work/ingestor.json"]
    roles["compare"] = _env(_base(ns, "compare", network=net, uid=uid) + tmp + mount,
        {**no_proxy, "PYTHONDONTWRITEBYTECODE": "1"}) + [
        "--entrypoint", "python", images["python"], "-B", "/app/scripts/v230_rcert2_shadow.py",
        "--compare", "/work"]
    return roles


def caps_total() -> tuple[float, int]:
    return round(sum(cpus for cpus, _ in CAPS.values()), 2), sum(mib for _, mib in CAPS.values())


def helper_argv(work: Path, args: list[str], *, uid: str | None = None) -> list[str]:
    """A one-shot in-namespace helper (topic creation), run while no service role runs (cap reuse)."""
    work = Path(work).resolve()
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    ns = namespace(packet["namespace"])
    uid = uid or f"{os.getuid()}:{os.getgid()}"
    cpus, mib = CAPS["compare"]
    return ["docker", "run", "--rm", "--pull=never", "--name", f"{ns}-helper", "--label", f"{LABEL}={ns}",
            "--network", "container:" + ns + "-net", "--cpus", f"{cpus:.2f}", "--cpu-shares", "256",
            "--memory", f"{mib}m", "--pids-limit", "128", "--read-only", "--tmpfs", "/tmp:rw,size=32m",
            "--user", uid, "-v", f"{work}:/work", "-e", "PYTHONDONTWRITEBYTECODE=1",
            "--entrypoint", "python", packet["images"]["python"], "-B", "/app/scripts/v230_rcert2_shadow.py",
            *args]


# ------------------------------------------------------------------ in-container steps

def _kafka_security(packet: dict) -> dict:
    tls = packet["client_tls"]
    return {"bootstrap.servers": packet["bootstrap"], "security.protocol": "SSL",
            "ssl.ca.location": tls["ca"], "ssl.certificate.location": tls["cert"],
            "ssl.key.location": tls["key"]}


def create_topics(work: Path) -> dict:
    from confluent_kafka import TopicCollection
    from confluent_kafka.admin import AdminClient, NewTopic

    work = Path(work)
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    admin = AdminClient(_kafka_security(packet))
    deadline = time.monotonic() + 180
    while True:
        try:
            admin.list_topics(timeout=3)
            break
        except Exception:  # noqa: BLE001 - not ready; the deadline decides
            if time.monotonic() >= deadline:
                raise TimeoutError("shadow broker readiness")
            time.sleep(1)
    wanted = topic_names(packet["namespace"])
    for future in admin.create_topics([NewTopic(name, parts, 1, config=dict(config))
                                       for name, (parts, config) in wanted.items()],
                                      request_timeout=30).values():
        future.result(40)
    canonical = packet["namespace"] + ".canonical"
    for _ in range(30):
        try:
            described = admin.describe_topics(TopicCollection([canonical]))[canonical].result(20)
            break
        except Exception as error:  # noqa: BLE001 - only the metadata propagation race
            if "UNKNOWN_TOPIC_OR_PART" not in str(error):
                raise
            time.sleep(1)
    else:
        raise TimeoutError("canonical topic metadata")
    packet["topic_id"] = str(described.topic_id)
    (work / "packet.json").write_text(json.dumps(packet, indent=1, sort_keys=True))
    return {"topics": sorted(wanted), "topic_id": packet["topic_id"]}


def native_matches(derived_payload: dict, native_row: list[str]) -> list[str]:
    """Fields where a derived BAR differs from the native OKX row (contract-unit SWAP mapping)."""
    pairs = (("open", 1), ("high", 2), ("low", 3), ("close", 4), ("volume", 5),
             ("base_volume", 6), ("quote_volume", 7))
    return [name for name, index in pairs
            if derived_payload.get(name) is None or Decimal(str(derived_payload[name])) != Decimal(native_row[index])]


def classify(*, derived, native_row, interval: str, base_interval: str) -> dict:
    """One target's verdict from the shadow cache row (``DurableBarRow`` or None) and the native row."""
    from qdl.adapters.intervals import canonical_interval_ms
    from qdl.common.v1 import common_pb2
    from qdl.runtime.stable_source import bar_item_fields

    if derived is None:
        return {"verdict": "MISSING"}
    bar = derived.envelope.bar
    if bar.origin != common_pb2.BAR_ORIGIN_AGGREGATED:
        return {"verdict": "BOOTSTRAP_NATIVE"}  # venue history of the binding, not a derivation
    expected = canonical_interval_ms(interval) // canonical_interval_ms(base_interval)
    if not bar.HasField("aggregation") or bar.aggregation.constituent_count != expected \
            or bar.aggregation.base_interval != base_interval:
        return {"verdict": "LINEAGE_MISMATCH"}
    if native_row is None or native_row[8] != "1":
        return {"verdict": "NATIVE_UNAVAILABLE"}
    differs = native_matches(bar_item_fields(derived.envelope)["payload"], native_row)
    # The core stamps the edge's publish receipt; minus the close is the derivation latency.
    latency_ms = (int(derived.envelope.published_at_ns) - int(bar.close_time_ns)) // 1_000_000
    return {"verdict": "MISMATCH" if differs else "MATCH", "fields": differs, "derive_latency_ms": latency_ms}


def summarize(path: Path) -> dict:
    """Gate view of every compared target so far (recomputed from the evidence file, restart-safe)."""
    summary = {"by_verdict": {}, "by_interval": {}, "mismatches": 0, "missing": 0, "latency_ms": {},
               "updated_at": _now()}
    latencies: dict[str, list[int]] = {}
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()] if Path(path).exists() else []
    for row in rows:
        verdict, interval = row["verdict"], row["interval"]
        summary["by_verdict"][verdict] = summary["by_verdict"].get(verdict, 0) + 1
        bucket = summary["by_interval"].setdefault(interval, {})
        bucket[verdict] = bucket.get(verdict, 0) + 1
        summary["mismatches"] += verdict in {"MISMATCH", "LINEAGE_MISMATCH"}
        summary["missing"] += verdict == "MISSING"
        if "derive_latency_ms" in row:
            latencies.setdefault(interval, []).append(int(row["derive_latency_ms"]))
    for interval, values in latencies.items():
        values.sort()
        summary["latency_ms"][interval] = {"n": len(values), "p50": values[len(values) // 2],
                                           "p99": values[min(len(values) - 1, int(len(values) * 0.99))]}
    return summary


def compare(work: Path, *, once: bool = False, http=None, readback=None, started_ms: int | None = None) -> dict:
    """Comparator loop (in-container): derived rows of the shadow cache vs native OKX REST."""
    from qdl.adapters.intervals import canonical_interval_ms, latest_closed_boundary_ms, okx_bar_size
    from qdl.runtime.stable_catalog import StableSourceCatalog

    work = Path(work)
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    catalog = StableSourceCatalog.load(work / "catalog.yaml")
    sources = {item.binding_id: item for item in catalog.bindings}
    if readback is None:
        import redis
        from qdl.runtime.kn_bar_readback import KnBarReadback

        readback = KnBarReadback(redis.Redis.from_url(CACHE_URL, decode_responses=False), "paper",
                                 bars_topic=packet["namespace"] + ".bars", bars_partitions=BARS_PARTITIONS)
    if http is None:
        import httpx

        client = httpx.Client(timeout=15.0, trust_env=False)

        def http(params):  # noqa: E306 - one bounded public GET
            time.sleep(COMPARE_PAUSE_S)
            return client.get(OKX_REST, params=params).json()
    started_ms = int(time.time() * 1000) if started_ms is None else started_ms
    done_path = work / "evidence/compared.jsonl"
    seen = set()
    if done_path.exists():
        seen = {(row["binding_id"], row["open_ms"]) for row in map(json.loads, done_path.read_text().splitlines())}
    while True:
        now_ms = int(time.time() * 1000)
        for child, parent in sorted(packet["derived_parents"].items()):
            source = sources[child]
            step = canonical_interval_ms(source.interval)
            boundary = latest_closed_boundary_ms(source.interval, now_ms - GAP_AFTER_S * 1000, provider="OKX")
            opens = [boundary - k * step for k in range(1, 4) if boundary - k * step + step > started_ms]
            opens = [value for value in opens if (child, value) not in seen]
            if not opens:
                continue
            rows = readback.final_bar_rows(source, frozenset(opens))
            for open_ms in sorted(opens):
                reply = http({"instId": source.instrument.native_symbol, "bar": okx_bar_size(source.interval),
                              "after": str(open_ms + 1), "limit": "1"})
                data = [row for row in (reply.get("data") or []) if int(row[0]) == open_ms]
                result = classify(derived=rows.get(open_ms), native_row=data[0] if data else None,
                                  interval=source.interval, base_interval=sources[parent].interval)
                record = {"binding_id": child, "interval": source.interval,
                          "symbol": source.instrument.native_symbol, "open_ms": open_ms,
                          "checked_at": _now(), **result}
                verdict = result["verdict"]
                if verdict == "NATIVE_UNAVAILABLE":
                    continue  # retried while the target stays in the 3-target window
                with done_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, sort_keys=True) + "\n")
                seen.add((child, open_ms))
        summary = summarize(done_path)
        (work / "evidence/compare-summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True))
        if once:
            return summary
        time.sleep(COMPARE_LOOP_S)


# ------------------------------------------------------------------ host orchestration

def sh(argv: list[str], *, timeout: int = 120, check: bool = True) -> str:
    return subprocess.run(argv, check=check, capture_output=True, text=True, timeout=timeout).stdout


def production_snapshot() -> dict:
    """G2.6: read-only identity of the production compose project (ids, images, names)."""
    out = sh(["docker", "ps", "--all", "--no-trunc", "--filter", f"label=com.docker.compose.project={PRODUCTION_PROJECT}",
              "--format", "{{.ID}} {{.Image}} {{.Names}} {{.CreatedAt}}"])
    rows = sorted(line for line in out.splitlines() if line.strip())
    return {"at": _now(), "containers": len(rows), "sha256": hashlib.sha256("\n".join(rows).encode()).hexdigest(),
            "rows": rows}


def _log(work: Path, event: str, **fields) -> None:
    with (Path(work) / "evidence/driver.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"at": _now(), "event": event, **fields}, sort_keys=True) + "\n")


def up(work: Path, *, approved: str, until: str) -> None:
    work = Path(work).resolve()
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    ns = namespace(packet["namespace"])
    if len(approved.strip()) < 10:
        raise SystemExit("--approved must quote the owner approval")
    deadline = datetime.strptime(until, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    seconds = int((deadline - datetime.now(timezone.utc)).total_seconds())
    if not 3_600 <= seconds <= 5 * 86_400:
        raise SystemExit("--until must be 1 h .. 5 days ahead")
    for key in ("rust", "python"):
        sh(["docker", "image", "inspect", packet["images"][key], "--format", "{{.Id}}"])
    if sh(["docker", "ps", "-aq", "--filter", f"label={LABEL}={ns}"]).strip():
        raise SystemExit("namespace already has containers")
    before = production_snapshot()
    _save(work / "evidence/production-before.json", before)
    _log(work, "up", approved=approved, until=until, caps=caps_total())
    sh(["docker", "network", "create", "--label", f"{LABEL}={ns}", ns + "-net"])
    roles = topology(work, deadline_s=seconds + 1800)
    sh(roles["net"])
    sh(roles["kafka"])
    sh(helper_argv(work, ["--create-topics", "/work"]), timeout=300)
    roles = topology(work, deadline_s=seconds + 1800)  # projector env now carries the topic id
    for role in SERVICE_ORDER:
        sh(roles[role])
        _log(work, "started", role=role)
        time.sleep(5)
    try:
        while datetime.now(timezone.utc) < deadline:
            running = set(sh(["docker", "ps", "--filter", f"label={LABEL}={ns}", "--format", "{{.Names}}"]).split())
            gone = [role for role in ("net", "kafka", *SERVICE_ORDER) if f"{ns}-{role}" not in running]
            _log(work, "status", running=len(running), gone=gone)
            if gone and not (work / "state/fault-in-progress").exists():
                _log(work, "role_exited_stop", gone=gone)
                break
            time.sleep(60)
    finally:
        _save(work / "evidence/production-after.json", production_snapshot())
        teardown(work)


def fault(work: Path, case: str, *, outage_s: int = 180) -> dict:
    """S1-S6 (packet section 2), one at a time, on demand."""
    work = Path(work).resolve()
    packet = json.loads((work / "packet.json").read_text(encoding="utf-8"))
    ns = namespace(packet["namespace"])
    roles = topology(work)
    marker = work / "state/fault-in-progress"
    marker.write_text(case)
    target = {"S1": "baredge", "S2": "ingestor", "S3": "projector", "S4": "projector", "S6": "core"}
    try:
        _log(work, "fault_start", case=case)
        if case in target:
            role = target[case]
            sh(["docker", "stop", "-t", "20", f"{ns}-{role}"], timeout=60)
            if case == "S4":
                sh(["docker", "exec", f"{ns}-cache", "redis-cli", "FLUSHALL"])
            time.sleep(outage_s if case == "S2" else 5)
            sh(roles[role])
        elif case == "S5":
            old = _env(_base(ns, "projector", network="container:" + ns + "-net", uid=f"{os.getuid()}:{os.getgid()}")
                       + ["--tmpfs", "/tmp:rw,size=32m,mode=1777", "-v", f"{work}:/work"],
                       {"QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
                        "QDL_KN_KAFKA_KEY": "/work/client.key", "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP,
                        "QDL_KN_BUNDLE_PATH": "/work/bundle.json", "QDL_KN_TOPIC_ID": packet["topic_id"],
                        "QDL_KN_REPLICA": "s5-old", "QDL_KN_MATERIALIZER_EPOCH": "1",
                        "QDL_KN_MARKET_CACHE_URL": "redis://localhost:6379/1",
                        "QDL_KN_STATUS_PATH": "/work/state/s5-status.json",
                        "QDL_KN_CANONICAL_TOPIC": ns + ".canonical", "QDL_KN_LATEST_TOPIC": ns + ".s5latest",
                        "QDL_KN_BARS_TOPIC": ns + ".s5bars"})
            old[old.index("--name") + 1] = f"{ns}-s5old"
            # The v2.2.4 projector replaces the shadow projector for 120 s (one role at a time keeps the caps).
            sh(["docker", "stop", "-t", "20", f"{ns}-projector"], timeout=60)
            try:
                sh(old + ["--entrypoint", "/usr/local/bin/qdl-projector", packet["images"]["old_projector"], "run"])
                time.sleep(120)
                logs = sh(["docker", "logs", "--tail", "400", f"{ns}-s5old"], check=False)
                refused = "ENVELOPE_NOT_CANONICAL" in logs
                _log(work, "s5_result", refused=refused)
            finally:
                sh(["docker", "rm", "-f", f"{ns}-s5old"], check=False)
                sh(roles["projector"])
        else:
            raise SystemExit("fault must be S1..S6")
        _log(work, "fault_done", case=case)
        return {"case": case}
    finally:
        marker.unlink(missing_ok=True)


def teardown(work: Path) -> dict:
    work = Path(work).resolve()
    ns = namespace(json.loads((work / "packet.json").read_text(encoding="utf-8"))["namespace"])
    removed = []
    for role in TEARDOWN_ORDER + ("helper", "s5old"):
        result = subprocess.run(["docker", "rm", "-f", f"{ns}-{role}"], capture_output=True, text=True)
        if result.returncode == 0:
            removed.append(role)
    leftovers = sh(["docker", "ps", "-aq", "--filter", f"label={LABEL}={ns}"]).split()
    for ident in leftovers:
        subprocess.run(["docker", "rm", "-f", ident], capture_output=True)
    network = subprocess.run(["docker", "network", "rm", ns + "-net"], capture_output=True, text=True).returncode == 0
    import shutil

    shutil.rmtree(work / "kafka", ignore_errors=True)  # broker data (written by our uid)
    for name in ("server.p12", "client.key", "broker.properties"):
        (work / name).unlink(missing_ok=True)
    result = {"removed": removed, "leftovers_forced": len(leftovers), "network_removed": network}
    _log(work, "teardown", **result)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    action = parser.add_mutually_exclusive_group(required=True)
    for flag in ("--render", "--plan", "--up", "--status", "--teardown", "--create-topics", "--compare"):
        action.add_argument(flag, metavar="WORK", type=Path)
    action.add_argument("--fault", nargs=2, metavar=("WORK", "CASE"))
    parser.add_argument("--ns")
    parser.add_argument("--rust-image")
    parser.add_argument("--python-image")
    parser.add_argument("--approved", default="")
    parser.add_argument("--until", default="")
    args = parser.parse_args(argv)
    if args.render:
        print(json.dumps(render(args.render, args.ns, rust_image=args.rust_image, python_image=args.python_image),
                         indent=1))
    elif args.plan:
        for role, argv_ in topology(args.plan).items():
            print(role, " ".join(argv_))
        print("caps", caps_total())
    elif args.up:
        up(args.up, approved=args.approved, until=args.until)
    elif args.status:
        print((Path(args.status) / "evidence/compare-summary.json").read_text())
    elif args.teardown:
        print(json.dumps(teardown(args.teardown)))
    elif args.create_topics:
        print(json.dumps(create_topics(args.create_topics)))
    elif args.compare:
        compare(args.compare)
    else:
        print(json.dumps(fault(Path(args.fault[0]), args.fault[1])))
    return 0


if __name__ == "__main__":
    sys.exit(main())

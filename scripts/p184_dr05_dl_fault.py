#!/usr/bin/env python3
"""Offline DR05 preparation. No runtime acceptance or shared-runtime discovery.

--prepare writes a NEW external directory with topology, fault schedules and
nonqualifying verifier rows. --readiness/--cleanup are owner stage3 utilities.
The publisher/TS adapter handoff is deliberately explicit, not a status mock.

Image set: --init/--prepare accept --image ROLE=sha256:<id> (repeatable, roles of
scripts/p184_dr08_isolated.py IMAGES, which stay the defaults). The resolved set is sealed
in handoff.json "images"; every later step (--up/--ready/--down, reset, channel, the
in-container --material) reads it from there via image_set(work), never from the constants.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.p184_dr08_isolated import IMAGES  # immutable image inventory only: the DEFAULT set
from scripts.p184_dr08_isolated import parse_image_overrides, resolve_images

VENUES = ("BINANCE", "OKX")
FAULTS = ("GAP_REFUSAL", "GENERATION_CHANGE_FENCED")
LABEL = "p184.dr05.dl"
ROLES = ("kafka", "quota", "core", "stream", "query")
CELLS = tuple(f"DR05.{fault}@{venue}" for venue in VENUES for fault in FAULTS)


def namespace(value):
    # G12 (scripts/p184_g12_isolated_fault.py) reuses this material/frame code
    # for its own disposable namespace; both shapes stay exact and disposable.
    if not re.fullmatch(r"p184-(?:dr05|g12)-dl-[0-9a-f]{16}", value):
        raise ValueError("exact disposable DR05/G12 namespace required")
    return value


def pending_rows():
    # No provenance header: this file must not be submitted as runtime evidence.
    return [{"cell": cell, "level": "ISOLATED_FAULT", "ok": False,
             "outcome": "PREPARED_NOT_RUN", "provenance": "TEST_ONLY",
             "detail": {"gate": "stage3 real DL path AND TS adapter observations required"}}
            for cell in CELLS]


def frame(venue, snapshot, sequence, previous, generation, quantity, at_ns):
    """Provider-shaped test bytes, following realtime-core delayed_book_frame.

    Stamp at publish time, not preparation time. Same lane across generations;
    a lane rename tests a different fence and is not this case.
    """
    if venue not in VENUES or type(at_ns) is not int or at_ns <= 0:
        raise ValueError("venue and positive integer timestamp required")
    symbol = "ETHUSDT" if venue == "BINANCE" else "ETH-USDT-SWAP"
    ms = at_ns // 1_000_000
    if venue == "OKX":
        payload = {"arg": {"channel": "books", "instId": symbol},
                   "action": "snapshot" if snapshot else "update", "data": [{
                       "seqId": str(sequence), "prevSeqId": "-1" if snapshot else str(previous),
                       "ts": str(ms), "bids": [["60000", quantity, "0", "1"]],
                       "asks": [["60001", "1", "0", "1"]]}]}
    elif snapshot:
        payload = {"lastUpdateId": sequence, "E": ms,
                   "bids": [["60000", quantity]], "asks": [["60001", "1"]]}
    else:
        payload = {"s": symbol, "U": previous, "u": sequence, "pu": previous,
                   "E": ms, "b": [["60000", quantity]], "a": [["60001", "1"]]}
    return {"venue": venue, "native_symbol": symbol, "connection_generation": generation,
            "sequence": sequence, "previous": previous, "snapshot": snapshot,
            "transport": "HTTP" if snapshot and venue == "BINANCE" else "WEBSOCKET",
            "received_at_ns": at_ns, "test_provenance": True, "provenance": "TEST_ONLY",
            "adapter_version": "p184-synthetic-test-only", "payload": payload}


def schedule(venue, fault):
    if venue not in VENUES or fault not in FAULTS:
        raise ValueError("unknown DR05 cell")
    # tuples: phase, snapshot, seq, previous, generation, bid quantity
    steps = [("positive_anchor", True, 100, 0, 7, "1"),
             ("positive_control", False, 101, 100, 7, "2")]
    if fault == "GAP_REFUSAL":
        steps += [("gap", False, 110, 109, 7, "3"),
                  ("still_unanchored", False, 111, 110, 7, "4")]
    else:
        steps += [("new_generation_unanchored", False, 201, 200, 8, "3")]
    # Binance replays the buffered 201 delta when snapshot200 arrives.
    # OKX rejects the unanchored delta and starts from snapshot200 instead.
    return_previous = 201 if venue == "BINANCE" and fault == "GENERATION_CHANGE_FENCED" else 200
    steps += [("return_anchor", True, 200, 0, 8, "5"),
              ("return_control", False, 202, return_previous, 8, "6"),
              ("old_generation", True, 300, 0, 7, "99"),
              ("post_fence_control", False, 203, 202, 8, "7")]
    return [{"phase": phase, "venue": venue, "snapshot": snap, "sequence": seq,
             "previous": prev, "generation": gen, "quantity": qty}
            for phase, snap, seq, prev, gen, qty in steps]


def drive(adapter, venue, fault):
    """Owner adapter contract; NOT a verifier or an implemented runtime adapter.

    fresh_case must start/reset only a disposable case (generation starts at 7).
    observe must wait for this raw Kafka offset through real core + Query/Stream
    and actual TS validation; it must never manufacture a status from phase.
    All calls must have bounded deadlines. Returned observations remain untrusted
    until owner review. Even an adapter returning all-success cannot emit ok=True.
    """
    adapter.fresh_case(venue, fault, timeout=30)
    observations = []
    for step in schedule(venue, fault):
        values = {k: v for k, v in step.items() if k != "phase"}
        raw = frame(**values, at_ns=time.time_ns())
        offset = adapter.publish(raw, timeout=5)
        observations.append({"phase": step["phase"], "raw_offset": offset,
                             "observation": adapter.observe(offset, timeout=10)})
    return {"cell": f"DR05.{fault}@{venue}", "level": "ISOLATED_FAULT",
            "ok": False, "outcome": "OWNER_REVIEW_REQUIRED", "provenance": "TEST_ONLY",
            "observations": observations}


def core_override(value):
    """Pin an explicitly supplied test binary; never discover or build one."""
    if value is None:
        return None
    supplied = value if isinstance(value, dict) else {"path": str(value)}
    path = Path(supplied["path"]).resolve(strict=True)
    if not path.is_file() or not os.access(path, os.X_OK) or any(c in str(path) for c in (":", ",")):
        raise ValueError("explicit executable core binary path required")
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    if "sha256" in supplied and supplied["sha256"] != digest:
        raise ValueError("candidate core binary digest changed")
    return {"path": str(path), "sha256": digest}


def image_set(work=None):
    """This run's sealed image set (handoff.json "images"), else the pinned defaults."""
    handoff = Path(work) / "handoff.json" if work is not None else None
    if handoff is not None and handoff.is_file():
        images = json.loads(handoff.read_text()).get("images")
        if images is not None:
            if not isinstance(images, dict) or set(images) != set(IMAGES):
                raise ValueError("handoff image set must name exactly the harness roles")
            return resolve_images(images)
    return dict(IMAGES)


def topology(ns, work, topic_id="", *, core_binary=None, images=None):
    """Generate executable Docker argv, but do not execute or discover config.

    Material preparation is a stage3 gate; topic UUID comes from the new broker.
    Query reuses DR08's generic real hot-read service, NOT its TRADE preparer.
    """
    namespace(ns)
    core_binary = core_override(core_binary)
    images = images or image_set(work)
    work = Path(work).resolve()
    if work == ROOT or ROOT in work.parents or ":" in str(work) or "," in str(work):
        raise ValueError("external unambiguous work directory required")
    common = {"QDL_ENVIRONMENT": "paper", "QDL_KN_CURSOR_KEYS_FILE": "/work/cursor-keys.json",
              "QDL_KN_CURSOR_ACTIVE_KEY_ID": "dr08", "QDL_KN_TOPIC_ID": topic_id,
              "QDL_KN_ROUTE_GENERATION": ns, "QDL_KN_READ_VIEW_SECRET_FILE": "/work/read-view.secret"}
    spec = {
        "kafka": ("kafka", "1536m", "1", {}, ["/bin/sh", "-ec",
            "/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /work/broker.properties && exec /opt/kafka/bin/kafka-server-start.sh /work/broker.properties"]),
        "quota": ("redis", "64m", "0.1", {}, ["redis-server", "--save", "", "--appendonly", "no",
            "--maxmemory", "48mb", "--maxmemory-policy", "noeviction"]),
        "core": ("core", "256m", "0.5", {"QDL_KAFKA_CERT_ROOT": "/work",
            "QDL_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", "QDL_KAFKA_CLIENT_ID": ns,
            "QDL_KAFKA_GROUP_ID": ns, "QDL_PROVIDER_ADMISSION_ENABLED": "false"},
            ["/usr/local/bin/qdl-realtime-core", "/work/core.json"]),
        "stream": ("stream", "512m", "0.5", {**common,
            "QDL_KN_BUNDLE_FILE": "/work/bundle.json", "QDL_KN_JWT_CONFIG_FILE": "/work/jwt-config.json",
            "QDL_KN_QUOTA_REDIS_URL": "redis://localhost:6379/0", "QDL_KN_QUOTA_PREFIX": "qdl:stable:v2:paper:" + ns,
            "QDL_KN_TLS_CERT_FILE": "/work/client.crt", "QDL_KN_TLS_KEY_FILE": "/work/client.key",
            "QDL_KN_TLS_CLIENT_CA_FILE": "/work/ca.crt", "QDL_KN_KAFKA_BOOTSTRAP": "localhost:9092",
            "QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
            "QDL_KN_KAFKA_KEY": "/work/client.key", "QDL_KN_KAFKA_TOPIC": ns + ".canonical",
            "QDL_KN_KAFKA_GROUP": "kn-" + ns, "QDL_KN_KAFKA_CLIENT_ID": ns + "-stream",
            "QDL_KN_READ_VIEW_URLS": "https://localhost:8200", "QDL_KN_READ_VIEW_CA_FILE": "/work/ca.crt",
            "QDL_KN_HOT_READ_ENABLED": "true", "QDL_KN_METRICS_SECONDS": "1",
            "QDL_KN_MAX_REPLAY_EVENTS": "10000", "QDL_KN_QUEUE_BYTES_TOTAL": "33554432"},
            ["/usr/local/bin/qdl-stream-gateway", "serve"]),
        "query": ("query", "512m", "0.5", {**common, "PYTHONPATH": "/app:/app/generated/python",
            "PYTHONDONTWRITEBYTECODE": "1"},
            ["python", "-B", "scripts/p184_dr05_observer.py", "--query", "/work"]),
    }
    result = {}
    for role, (image, memory, cpus, env, command) in spec.items():
        network = "none" if role == "kafka" else "container:" + ns + "-kafka"
        argv = ["docker", "run", "-d", "--rm", "--pull=never", "--name", ns + "-" + role,
                "--label", LABEL + "=" + ns, "--network", network, "--read-only",
                "--user", f"{os.getuid()}:{os.getgid()}", "--memory", memory, "--cpus", cpus,
                "--pids-limit", "128", "--log-opt", "max-size=4m", "--log-opt", "max-file=1",
                "--tmpfs", "/tmp:rw," + ("exec," if role == "kafka" else "") + "size=64m,mode=1777",
                "-v", str(work) + ":/work", "-v", str(ROOT) + ":/app:ro", "-w", "/app"]
        if role == "kafka":
            argv += ["--hostname", "localhost", "--tmpfs", "/data:rw,size=512m,mode=1777"]
            # Override image-declared VOLUMEs: reset must never retain broker state.
            for target in ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data"):
                argv += ["--tmpfs", target + ":rw,size=1m,mode=1777"]
            env = {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": "/tmp/kafka-logs"}
        elif role == "quota":
            argv += ["--tmpfs", "/data:rw,size=1m,mode=1777"]
        if role == "core" and core_binary:
            argv += ["-v", core_binary["path"] + ":/usr/local/bin/qdl-realtime-core:ro"]
        for key, value in env.items():
            argv += ["-e", key + "=" + value]
        result[role] = argv + ["--entrypoint", command[0], images[image], *command[1:]]
    return result


def sh(argv):
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=15).stdout


def owned(ns, role, call=sh, *, allow_disappearance=True):
    name = namespace(ns) + "-" + role
    if role not in (*ROLES, "helper", "tsvalidator"):
        raise ValueError("unknown owned role")
    ids = call(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"]).splitlines()
    if not ids:
        return None
    if len(ids) != 1:
        raise ValueError("ambiguous container")
    try:
        row = json.loads(call(["docker", "inspect", ids[0]]))[0]
    except subprocess.CalledProcessError:
        if not allow_disappearance:
            raise
        # --rm can finish between the exact-name listing and inspection.
        if not call(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"]).strip():
            return None
        raise
    if row["Name"] != "/" + name or row["Config"].get("Labels", {}).get(LABEL) != ns:
        raise ValueError("cleanup/readiness ownership mismatch")
    return row


def readiness(ns, call=sh, images=None):
    images = images or IMAGES
    rows = {role: owned(ns, role, call) for role in ROLES}
    broker = rows["kafka"]
    checks = {}
    for role, row in rows.items():
        expected_image = images[{"quota": "redis"}.get(role, role)]
        network = "none" if role == "kafka" else "container:" + (broker or {}).get("Id", "MISSING")
        checks[role] = bool(row and row["State"]["Running"] and row["Image"] == expected_image
                            and row["HostConfig"]["NetworkMode"] in
                            (network, "container:" + ns + "-kafka" if role != "kafka" else network)
                            and not row["HostConfig"].get("PortBindings")
                            and row["HostConfig"].get("AutoRemove") is True)
    return {"process_isolation_ready": all(checks.values()), "roles": checks,
            "functional_ready": False, "acceptance": False,
            "remaining": "mTLS broker metadata, real Query/Stream positive controls and TS adapter"}


def cleanup(ns, call=sh):
    removed = []
    # Preflight ALL ownership before stopping anything. Never remove networks,
    # images, shared volumes, files or a container selected only by a label.
    rows = [(role, owned(ns, role, call, allow_disappearance=False))
            for role in reversed((*ROLES, "helper", "tsvalidator"))]
    if any(row and row["HostConfig"].get("AutoRemove") is not True for _, row in rows):
        raise ValueError("refuse non-disposable container")
    failures = []
    for role, row in rows:
        if row:
            try:
                stop = ["docker", "stop", "--time", "5", row["Id"]]
                try:
                    call(stop)
                except subprocess.CalledProcessError as error:
                    # Only this pinned disposable's exact absence can excuse a failed stop.
                    missing = "Error response from daemon: No such container: " + row["Id"]
                    if error.returncode != 1 or error.cmd != stop or (error.stderr or "").strip() != missing:
                        raise
                    if call(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + row["Id"]]).strip():
                        raise
                    if call(["docker", "ps", "-aq", "--no-trunc", "--filter",
                             "name=^/" + ns + "-" + role + "$"]).strip():
                        raise RuntimeError("container identity replaced during cleanup") from error
                    removed.append(role)
                    continue
                deadline = time.monotonic() + 5
                while (remaining := owned(ns, role, call, allow_disappearance=False)) is not None:
                    if remaining["Id"] != row["Id"]:
                        raise RuntimeError("container identity replaced during cleanup")
                    if remaining.get("State", {}).get("Running") or time.monotonic() >= deadline:
                        raise RuntimeError("--rm container still present")
                    time.sleep(.1)
                if call(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + row["Id"]]).strip():
                    raise RuntimeError("inspected container still present during cleanup")
                removed.append(role)
            except Exception as error:
                failures.append(role + ":" + type(error).__name__)
    if failures:
        raise RuntimeError("cleanup incomplete: " + ",".join(failures))
    return {"removed": removed, "scope": ns}


def prepare(work, images=None):
    ns = "p184-dr05-dl-" + uuid.uuid4().hex[:16]
    images = resolve_images(images)
    topology(ns, work, images=images)  # Validate path before writing.
    work = Path(work).resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    packet = {"namespace": ns, "status": "PREPARED_HANDOFF_NOT_RUN", "provenance": "TEST_ONLY",
              "level": "ISOLATED_FAULT", "images": images,
              "deadline_seconds": 600, "acceptance": False,
              "material_ready": False, "adapter_ready": False,
              "schedules": {f"DR05.{f}@{v}": schedule(v, f) for v in VENUES for f in FAULTS}}
    (work / "handoff.json").write_text(json.dumps(packet, indent=2) + "\n")
    (work / "pending-cells.jsonl").write_text("".join(json.dumps(r) + "\n" for r in pending_rows()))
    return packet


def isolated_config(exported, ns, images=None):
    """Take only public binding fields; never copy transport, credentials or authority."""
    namespace(ns)
    images = images or IMAGES
    required = {"provider", "venue", "market", "product_type", "native_symbol", "native_channel",
                "provider_kind", "instrument_uid", "instrument_id", "instrument_revision",
                "instrument_catalog_revision", "source_id", "source_role", "normalizer_version",
                "sequence_policy", "l2"}
    optional = {"physical_native_symbol", "physical_native_channel", "require_final_bar"}
    selected = []
    for venue, market, symbol in (("BINANCE", "USDM", "ETHUSDT"), ("OKX", "SWAP", "ETH-USDT-SWAP")):
        matches = [b for b in exported["core"]["bindings"] if b.get("l2") and
                   (b.get("venue"), b.get("market"), b.get("native_symbol")) == (venue, market, symbol)]
        if len(matches) != 1 or not required <= matches[0].keys():
            raise ValueError("exactly one complete exported ETH BOOK binding required: " + venue)
        binding = {k: copy.deepcopy(v) for k, v in matches[0].items() if k in required | optional}
        if binding["product_type"] != "PERPETUAL" or binding["source_role"] != "PRIMARY":
            raise ValueError("ETH primary perpetual BOOK required")
        allowed_l2 = {"provider_protocol", "depth_per_side", "snapshot_refresh_seconds",
                      "materialized_snapshot_interval_ms"}
        if set(binding["l2"]) - allowed_l2:
            raise ValueError("unexpected L2 configuration")
        selected.append(binding)
    digest = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
    return {"core": {"bindings": selected, "canonical_stream": ns + ".canonical",
                     "quarantine_stream": ns + ".quarantine", "allow_test_provenance": True,
                     "dedup_capacity": 10000},
            "raw_topics": [ns + ".raw"], "shard_id": ns, "transactional_id": ns,
            "batch_size": 25, "batch_wait_ms": 10, "max_events": 0, "metrics_every_batches": 1,
            "strict_subscription_scope": True, "book_resync_quarantine": True,
            "authority": {"schema": "qdl.authority-record.v1", "slice_id": ns, "revision": 1,
                "mode": "RUST_SHADOW", "candidate_image_digest": images["core"],
                "capability_manifest_digest": digest, "contract_digest": digest,
                "partition_plan_digest": digest, "public_write_allowed": False,
                "legacy_write_allowed": False, "approved_by": "TEST_ONLY ISOLATED_FAULT",
                "effective_at_ns": time.time_ns()}}


def material(work, exported):
    """Offline, fresh credentials. No shared discovery; source catalog is read-only."""
    import secrets
    import yaml
    from datetime import datetime, timedelta, timezone
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from tests.phase7_support import manifest_mapping
    from scripts.kn_gateway_bundle import compile_bundle
    from scripts.kn_native_slice_probe import ISSUER, AUDIENCE

    work = Path(work)
    packet = json.loads((work / "handoff.json").read_text())
    namespace = packet["namespace"]
    cfg = isolated_config(exported, namespace, image_set(work))

    def save(name, value, private=False):
        with (work / name).open("x") as handle:
            os.chmod(work / name, 0o600 if private else 0o644)
            handle.write(value if isinstance(value, str) else json.dumps(value))

    catalog = yaml.safe_load((ROOT / "config/v2/stable-source-bindings.yaml").read_text())
    catalog["canonical_stream"] = cfg["core"]["canonical_stream"]
    ids = {b["source_id"] for b in cfg["core"]["bindings"]}
    catalog["bindings"] = [b for b in catalog["bindings"] if b["source"]["source_id"] in ids
                           and b["feed"] in ("BOOK_SNAPSHOT", "BOOK_DELTA")]
    if len(catalog["bindings"]) != 4:
        raise ValueError("both ETH BOOK products must match source catalog")
    for binding in catalog["bindings"]:
        binding["source"]["adapter_version"] = "p184-synthetic-test-only"
    save("catalog.yaml", yaml.safe_dump(catalog))
    loaded = StableSourceCatalog.load(work / "catalog.yaml")
    for b in cfg["core"]["bindings"]:
        matches = [x for x in loaded.bindings if x.source_id == b["source_id"]]
        if len(matches) != 2 or any(x.instrument.instrument_uid != b["instrument_uid"] for x in matches):
            raise ValueError("export/catalog identity mismatch")
    save("core.json", cfg)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, namespace)])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
          .public_key(key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
          .sign(key, hashes.SHA256()))
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf = (x509.CertificateBuilder().subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])).issuer_name(name)
            .public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=2))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                  ExtendedKeyUsageOID.CLIENT_AUTH]), False)
            .sign(key, hashes.SHA256()))
    pem = serialization.Encoding.PEM
    save("ca.crt", ca.public_bytes(pem).decode())
    save("client.crt", leaf.public_bytes(pem).decode())
    save("client.key", leaf_key.private_bytes(pem, serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption()).decode(), True)
    password = secrets.token_hex(16)
    with (work / "server.p12").open("xb") as handle:
        (work / "server.p12").chmod(0o600)
        handle.write(pkcs12.serialize_key_and_certificates(b"dr08", leaf_key, leaf, [ca],
                     serialization.BestAvailableEncryption(password.encode())))
    props = """process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=SSL://:9092,CONTROLLER://:9093
advertised.listeners=SSL://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/server.p12
ssl.keystore.password=PASSWORD
ssl.key.password=PASSWORD
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca.crt
ssl.client.auth=required
log.dirs=/data
log.segment.bytes=16777216
metadata.log.segment.bytes=16777216
log.index.size.max.bytes=1048576
offsets.topic.num.partitions=1
transaction.state.log.num.partitions=1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"""
    save("broker.properties", props.replace("PASSWORD", password), True)

    manifests, identities, keys, subjects = [], [], {}, {}
    for b in cfg["core"]["bindings"]:
        label = b["venue"].lower()
        cid = namespace + "." + label
        subject = "spiffe://qdl/paper/" + cid
        requirements = []
        for feed in ("BOOK_SNAPSHOT", "BOOK_DELTA"):
            binding = next(x for x in loaded.bindings if x.source_id == b["source_id"] and x.feed.value == feed)
            mapping = manifest_mapping(consumer_id=cid, subject=subject, instrument_uid=b["instrument_uid"],
                                       feed=feed, interval=None, source_policy_id=binding.source_policy_id,
                                       grade="EXECUTION", purposes=("INTERNAL_EXECUTION",))
            requirement = mapping["spec"]["requirements"][0]
            requirement.update(warmup_limit=1, require_full_coverage=True, require_final_bars=False,
                               max_freshness_ms=2000)
            requirements.append(requirement)
        mapping["spec"]["requirements"] = requirements
        mapping["spec"]["quotas"]["max_buffer_events"] = 1000
        save(label + ".yaml", yaml.safe_dump(mapping))
        from scripts.p184_dr05_case import consumer_packet
        consumer, route = consumer_packet(work / (label + ".yaml"), loaded, b["venue"], ISSUER, AUDIENCE)
        save(label + ".binding.json", route)
        save("ts-consumer." + label + ".json", consumer)
        manifests.append(work / (label + ".yaml"))
        jwt_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        save(label + ".key", jwt_key.private_bytes(pem, serialization.PrivateFormat.PKCS8,
             serialization.NoEncryption()).decode(), True)
        keys[label] = jwt_key.public_key().public_bytes(pem, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        subjects[label] = subject
        identities.append({"id": cid, "tls": {"ca_file": "/work/ca.crt", "cert_file": "/work/client.crt",
            "key_file": "/work/client.key"}, "jwt": {"private_key_file": "/work/" + label + ".key", "key_id": label}})
    save("jwt-config.json", {"issuer": ISSUER, "audience": AUDIENCE, "algorithms": "RS256",
         "keys": keys, "subjects": subjects, "environments": {k: "paper" for k in keys}, "max_lifetime_seconds": 900})
    save("profile.json", {"identities": identities})
    save("bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
    save("cursor-keys.json", {"dr08": secrets.token_hex(32)}, True)
    save("read-view.secret", secrets.token_hex(32), True)
    save("probes.json", [])
    runtime_packet = {"namespace": namespace, "bootstrap": "localhost:9092", "provenance": "TEST_ONLY",
        "client_tls": {"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"},
        "isolated_manifest_paths": ["/work/binance.yaml", "/work/okx.yaml"],
        "slice": {"bundle": "/work/bundle.json", "cursor_keys": "/work/cursor-keys.json",
                  "profile": "/work/profile.json", "probes": "/work/probes.json",
                  "route_generation": namespace, "target": "localhost:8210"}}
    save("packet.json", runtime_packet)
    packet.update(material_ready=True, status="MATERIAL_READY_NOT_STARTED")
    (work / "handoff.json").write_text(json.dumps(packet, indent=2) + "\n")
    return packet


def helper_command(ns, work, args, *, network="none", exported=None):
    namespace(ns)
    if network not in ("none", "container:" + ns + "-kafka"):
        raise ValueError("helper must stay in the disposable namespace")
    topology(ns, work)
    if exported and any(c in str(Path(exported).resolve()) for c in (":", ",")):
        raise ValueError("unambiguous export path required")
    argv = ["docker", "run", "--rm", "--pull=never", "--name", ns + "-helper", "--label", LABEL + "=" + ns,
            "--network", network, "--read-only", "--user", f"{os.getuid()}:{os.getgid()}",
            "--memory", "512m", "--cpus", "0.5", "--pids-limit", "128",
            "--log-opt", "max-size=4m", "--log-opt", "max-file=1",
            "--tmpfs", "/tmp:rw,size=64m,mode=1777", "-v", str(Path(work).resolve()) + ":/work",
            "-v", str(ROOT) + ":/app:ro", "-w", "/app", "-e", "PYTHONPATH=/app:/app/generated/python",
            "-e", "PYTHONDONTWRITEBYTECODE=1"]
    if exported:
        argv += ["-v", str(Path(exported).resolve()) + ":/export.json:ro"]
    return argv + ["--entrypoint", "python", image_set(work)["python"], "-B", "scripts/p184_dr05_dl_fault.py", *args]


def bounded(argv, timeout=120):
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=timeout).stdout


PRIVATE_FILES = ("client.key", "server.p12", "binance.key", "okx.key", "cursor-keys.json",
                 "read-view.secret", "broker.properties")


def erase_private(work):
    work = Path(work).resolve()
    namespace(json.loads((work / "handoff.json").read_text())["namespace"])
    for name in PRIVATE_FILES:
        (work / name).unlink(missing_ok=True)
    packet = json.loads((work / "handoff.json").read_text())
    packet.update(material_ready=False, status="STOPPED_PRIVATE_MATERIAL_REMOVED")
    (work / "handoff.json").write_text(json.dumps(packet, indent=2) + "\n")


def init(work, export_path, images=None):
    # Runtime-free initialization in the already-local dependency image.
    packet = prepare(work, images)
    ns = packet["namespace"]
    try:
        bounded(helper_command(ns, work, ["--material", "/work", "--core-config", "/export.json"], exported=export_path))
    except BaseException:
        try:
            cleanup(ns)
        finally:
            erase_private(work)
        raise
    return {"namespace": ns, "material_ready": True, "acceptance": False}


def up(work, timeout=120, *, core_binary=None):
    deadline = time.monotonic() + timeout

    def run(argv, limit=120):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("isolated services startup deadline")
        return bounded(argv, min(limit, remaining))

    work = Path(work).resolve()
    packet = json.loads((work / "handoff.json").read_text())
    ns = namespace(packet["namespace"])
    if not packet.get("material_ready"):
        raise ValueError("run --init with sanitized export first")
    if any(owned(ns, role, run) is not None for role in (*ROLES, "helper")):
        raise ValueError("namespace already in use; no implicit restart")
    selected_core = core_override(core_binary if core_binary is not None else packet.get("core_binary"))
    if selected_core:
        packet["core_binary"] = selected_core
        (work / "handoff.json").write_text(json.dumps(packet, indent=2) + "\n")
    images = image_set(work)
    for digest in set(images.values()):
        if run(["docker", "image", "inspect", digest, "--format", "{{.Id}} "]).strip() != digest:
            raise ValueError("local immutable image mismatch")
    try:
        run(topology(ns, work, core_binary=selected_core, images=images)["kafka"])
        run(helper_command(ns, work, ["--topics", "/work"], network="container:" + ns + "-kafka"))
        topic = json.loads((work / "packet.json").read_text())["slice"]["topic_id"]
        for role, argv in topology(ns, work, topic, core_binary=selected_core, images=images).items():
            if role != "kafka":
                run(argv)
        deadline = min(deadline, time.monotonic() + 45)
        while True:
            result = readiness(ns, run, images)
            if result["process_isolation_ready"]:
                try:
                    run(helper_command(ns, work, ["--probe", "/work"], network="container:" + ns + "-kafka"), 25)
                    return {**result, "services_reachable": True, "acceptance": False, "core_binary": selected_core}
                except subprocess.CalledProcessError:
                    pass
            if time.monotonic() >= deadline:
                raise TimeoutError("isolated services startup deadline")
            time.sleep(.5)
    except BaseException:
        try:
            cleanup(ns)
        finally:
            erase_private(work)
        raise


def probe(work):
    """Authenticated service connectivity only, never a market-data acceptance."""
    import socket
    import ssl
    import asyncio
    import grpc
    from confluent_kafka.admin import AdminClient
    from scripts.p184_dr08_isolated import kafka_security
    packet = json.loads((work / "packet.json").read_text())
    AdminClient(kafka_security(packet)).list_topics(timeout=3)
    context = ssl.create_default_context(cafile=str(work / "ca.crt"))
    context.load_cert_chain(str(work / "client.crt"), str(work / "client.key"))
    with socket.create_connection(("localhost", 8200), timeout=3) as sock:
        with context.wrap_socket(sock, server_hostname="localhost"):
            pass
    async def connect():
        credentials = grpc.ssl_channel_credentials((work / "ca.crt").read_bytes(),
                        (work / "client.key").read_bytes(), (work / "client.crt").read_bytes())
        async with grpc.aio.secure_channel("localhost:8210", credentials,
                options=(("grpc.enable_http_proxy", 0),)) as channel:
            await asyncio.wait_for(channel.channel_ready(), 5)
    asyncio.run(connect())
    return {"services_reachable": True, "functional_ready": False, "acceptance": False}


def reset_case(work, venue, timeout=30):
    """Host-only reset; the owned JSONL helper may remain, TS clones must stop."""
    from scripts.p184_dr05_case import reset_case as reset
    return reset(work, venue, timeout)


def canonical_observe(work, raw_receipt, timeout=10):
    """Run inside the attested isolated broker namespace."""
    from scripts.p184_dr05_case import canonical_observe as observe
    return observe(work, raw_receipt, timeout)


def dispatch(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("prepare", "init", "material", "up", "down", "topics", "probe", "ready"):
        mode.add_argument("--" + flag, type=Path)
    mode.add_argument("--readiness")
    mode.add_argument("--cleanup")
    parser.add_argument("--core-config", type=Path)
    parser.add_argument("--image", action="append", default=[], metavar="ROLE=sha256:ID",
                        help="--init/--prepare image override (repeatable); sealed in handoff.json")
    args = parser.parse_args(argv)
    if args.image and not (args.init or args.prepare):
        parser.error("--image only with --init/--prepare; later steps read handoff.json")
    images = resolve_images(parse_image_overrides(args.image))
    if args.init or args.material:
        if args.core_config is None:
            parser.error("--core-config sanitized exported JSON required")
        if args.init:
            result = init(args.init, args.core_config, images)
        else:
            result = material(args.material, json.loads(args.core_config.read_text()))
            result = {k: result[k] for k in ("namespace", "status", "material_ready")}
    elif args.up:
        result = up(args.up)
    elif args.down or args.ready:
        work = args.down or args.ready
        ns = namespace(json.loads((work / "handoff.json").read_text())["namespace"])
        if args.down:
            try:
                result = cleanup(ns)
            finally:
                erase_private(work)
        else:
            result = readiness(ns, images=image_set(work))
            if result["process_isolation_ready"]:
                result.update(json.loads(bounded(helper_command(ns, work, ["--probe", "/work"],
                                          network="container:" + ns + "-kafka"))))
    elif args.topics:
        from scripts.p184_dr08_isolated import prepare_topics
        prepare_topics(args.topics)
        result = {"topics_ready": True}
    elif args.probe:
        result = probe(args.probe)
    elif args.prepare:
        packet = prepare(args.prepare, images)
        result = {k: packet[k] for k in ("namespace", "status", "acceptance")}
    else:
        result = readiness(args.readiness) if args.readiness else cleanup(args.cleanup)
    print(json.dumps(result))
    return 1 if result.get("process_isolation_ready") is False else 0


def main(argv=None):
    def deadline(_signum, _frame):
        raise TimeoutError("DR05 command ten-minute deadline")
    previous = signal.signal(signal.SIGALRM, deadline)
    signal.alarm(600)
    try:
        return dispatch(argv)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == "__main__":
    raise SystemExit(main())

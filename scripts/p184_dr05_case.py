"""DR05 isolated consumer packet and bounded case APIs; never acceptance evidence.

reset_case runs on the Docker host before TS clones start, preserving the owned
broker and long-lived JSONL channel. The channel requests reset through bounded
file IPC; no Docker socket enters a container. canonical_observe runs inside the
attested broker namespace. Callers must serialize all case operations.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

from scripts import p184_dr05_dl_fault as dr


def consumer_packet(manifest_path, catalog, venue, issuer, audience):
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.consumer.realtime_route import requirement_key
    from qdl.consumer.universal_release import (
        ConsumerRequestPolicy, ConsumerRouteBinding, UniversalConsumerClass,
        UniversalReleaseProduct, UniversalReleasePolicy,
    )

    manifest = ConsumerManifestLoader.load(manifest_path)
    label = venue.lower()
    cid = manifest.consumer_id
    dr.namespace(cid.removesuffix("." + label))
    if venue not in dr.VENUES or not cid.endswith("." + label):
        raise ValueError("isolated venue consumer required")
    policy = UniversalReleasePolicy.load(dr.ROOT / "config/v2/universal-release-policy.yaml", manifest_root=dr.ROOT)
    # A test-only routing generation, not a production release certificate.
    digest = hashlib.sha256(Path(manifest_path).read_bytes()).hexdigest()
    products, policies = [], []
    for req in manifest.requirements:
        instrument = catalog.instrument_for(req.instrument_uid)
        identity = instrument.identity
        if identity.venue != venue or req.feed.value not in ("BOOK_SNAPSHOT", "BOOK_DELTA"):
            raise ValueError("isolated venue BOOK requirements only")
        rid = hashlib.sha256(f"{cid}:{requirement_key(req)}".encode()).hexdigest()
        products.append(UniversalReleaseProduct(
            consumer_id=cid, consumer_class=UniversalConsumerClass.TRADING_SYSTEM,
            requirement_id=rid, instrument_uid=identity.instrument_uid,
            instrument_id=identity.instrument_id, venue=venue, market=identity.market,
            product_type=identity.product_type.value, native_symbol=instrument.native_symbol,
            feed=req.feed.value, interval=req.interval, source_policy_id=req.source_policy_id,
            provider_plane="L2", max_freshness_ms=req.max_freshness_ms,
            require_final_bars=req.require_final_bars, require_live=True,
            execution_grade=req.consumer_grade.value == "EXECUTION", route="V2_PRIMARY",
            fallback="BLOCKED", fallback_rule_id=None, blocked_reason="TEST_ONLY_NO_V1"))
        recency = req.event_recency_policy
        policies.append(ConsumerRequestPolicy(rid, None if recency is None else recency.value,
                                               req.max_session_liveness_ms))
    binding = ConsumerRouteBinding(
        consumer_id=cid, consumer_class=UniversalConsumerClass.TRADING_SYSTEM,
        release_revision=manifest.manifest_revision, universal_manifest_sha256=digest,
        policy_sha256=policy.policy_sha256, capability_matrix_sha256=policy.capability_matrix_sha256,
        capability_matrix_revision=policy.capability_matrix_revision, inventory_sha256=digest,
        v1_rollback=policy.v1_rollback, independent_v1_venues=("DNSE",),
        products=tuple(sorted(products, key=lambda p: p.requirement_id)),
        consumer_manifest_revision=manifest.manifest_revision,
        request_policies=tuple(sorted(policies, key=lambda p: p.requirement_id)))
    sources = {p.source_policy_id for p in products}
    if len(products) != 2 or len(sources) != 1:
        raise ValueError("exact BOOK snapshot/delta source policy required")
    return {"schema": "p184.dr05.ts-consumer.v1", "venue": venue,
        "consumer_id": cid, "subject": "spiffe://qdl/paper/" + cid,
        "jwt_private_key_file": "/work/" + label + ".key", "jwt_key_id": label,
        "issuer": issuer, "audience": audience, "environment": "paper",
        "manifest_revision": manifest.manifest_revision,
        "query_url": "https://localhost:8200", "grpc_target": "localhost:8210",
        "tls_ca_file": "/work/ca.crt", "tls_cert_file": "/work/client.crt",
        "tls_key_file": "/work/client.key", "binding_file": "/work/" + label + ".binding.json",
        "release_manifest_sha256": digest, "execution_book_source_policy_id": sources.pop()}, binding.canonical_mapping()


def _deadline(timeout):
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise ValueError("deadline must be in (0, 120] seconds")
    return time.monotonic() + timeout


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("DR05 case deadline")
    return remaining


def _packet(work):
    work = Path(work).resolve()
    handoff = json.loads((work / "handoff.json").read_text())
    packet = json.loads((work / "packet.json").read_text())
    config = json.loads((work / "core.json").read_text())
    ns = dr.namespace(handoff["namespace"])
    if (handoff.get("provenance") != "TEST_ONLY" or handoff.get("material_ready") is not True
            or packet.get("namespace") != ns or packet.get("provenance") != "TEST_ONLY"
            or packet.get("bootstrap") != "localhost:9092"
            or packet.get("client_tls") != {"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"}
            or config.get("raw_topics") != [ns + ".raw"]
            or config.get("core", {}).get("canonical_stream") != ns + ".canonical"
            or config["core"].get("quarantine_stream") != ns + ".quarantine"
            or config["core"].get("allow_test_provenance") is not True):
        raise ValueError("isolated packet/config identity mismatch")
    return work, ns, packet, config


def _reset_preflight(work, ns, call, core_binary=None, *, allow_channel=False):
    core_binary = dr.core_override(core_binary)
    rows = {role: dr.owned(ns, role, call) for role in (*dr.ROLES, "helper", "tsvalidator")}
    if rows["tsvalidator"] or (rows["helper"] and not allow_channel):
        raise ValueError("stop helpers and TS clones before reset")
    if allow_channel:
        channel = rows["helper"]
        if (not channel or not channel.get("State", {}).get("Running")
                or "P184_DR05_ISOLATED_NAMESPACE=" + ns not in channel["Config"].get("Env", [])
                or "scripts/p184_dr05_channel.py" not in channel["Config"].get("Cmd", [])
                or "--channel" not in channel["Config"].get("Cmd", [])):
            raise ValueError("exact owned attested JSONL channel required")
    broker = rows["kafka"]
    if any(rows.values()) and broker is None:
        raise ValueError("partial stack without owned broker; cleanup explicitly")
    allowed = {row["Id"] for row in rows.values() if row}
    for role, row in rows.items():
        if not row:
            continue
        host = row["HostConfig"]
        expected = "none" if role == "kafka" else "container:" + broker["Id"]
        mounts = row.get("Mounts", [])
        binds = {m["Destination"]: m["Source"] for m in mounts if m["Type"] == "bind"}
        expected_binds = {"/work": str(work), "/app": str(dr.ROOT)}
        if role == "core" and core_binary:
            expected_binds["/usr/local/bin/qdl-realtime-core"] = core_binary["path"]
            if any(m.get("RW") is not False for m in mounts if m["Destination"] == "/usr/local/bin/qdl-realtime-core"):
                raise ValueError("candidate core binary mount must be read-only")
        if (row["Image"] != dr.IMAGES[{"quota": "redis", "helper": "python"}.get(role, role)]
                or host.get("AutoRemove") is not True or host.get("PortBindings")
                or host.get("NetworkMode") not in (expected, "container:" + ns + "-kafka" if role != "kafka" else expected)
                or host.get("ReadonlyRootfs") is not True
                or binds != expected_binds
                or any(m["Type"] not in ("bind", "tmpfs") for m in mounts)
                or (role == "kafka" and "/data" not in host.get("Tmpfs", {}))):
            raise ValueError("reset requires exact disposable images, mounts and tmpfs broker")
    # Read only IDs/network modes, never shared container environments or secrets.
    ids = call(["docker", "ps", "-aq"]).split()
    if ids:
        modes = call(["docker", "inspect", "--format", "{{.Id}} {{.HostConfig.NetworkMode}}", *ids])
        targets = {"container:" + ns + "-kafka"}
        if broker:
            targets.add("container:" + broker["Id"])
        for line in modes.splitlines():
            ident, mode = line.split(maxsplit=1)
            if mode in targets and ident not in allowed:
                raise ValueError("unowned container or TS clone still attached; reset refused")
    return broker


def reset_case(work, venue, timeout=30):
    """Host-only bounded reset preserving the broker and JSONL network namespace."""
    from scripts.p184_dr05_reset import reset_preserving_broker
    return reset_preserving_broker(work, venue, timeout)


def canonical_observe(work, raw_receipt, timeout=10):
    """Verify actual raw bytes then delegate committed canonical/quarantine barrier."""
    deadline = _deadline(timeout)
    _, ns, packet, config = _packet(work)
    if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != ns:
        raise ValueError("disposable namespace attestation required")
    r = raw_receipt
    if (not isinstance(r, dict) or r.get("topic") != ns + ".raw"
            or type(r.get("partition")) is not int or r["partition"] != 0
            or type(r.get("offset")) is not int or r["offset"] < 0
            or r.get("provenance") != "TEST_ONLY"
            or not re.fullmatch("[0-9a-f]{32}", str(r.get("capture_id", "")))
            or not re.fullmatch("[0-9a-f]{64}", str(r.get("raw_sha256", "")))):
        raise ValueError("isolated raw receipt coordinates/hash required")
    from confluent_kafka import Consumer, TopicPartition
    from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
    from scripts.p184_dr08_isolated import kafka_security
    from scripts.p184_dr05_observer import CanonicalObserver

    reader = Consumer({**kafka_security(packet), "group.id": ns + ".receipt",
                       "enable.auto.commit": False, "isolation.level": "read_committed",
                       "fetch.wait.max.ms": 10})
    try:
        reader.assign([TopicPartition(r["topic"], 0, r["offset"])])
        message = reader.poll(_remaining(deadline))
        if message is None:
            raise TimeoutError("raw receipt readback deadline")
        if message.error():
            raise RuntimeError("raw receipt readback failed")
        wire = message.value()
        raw = RawProviderEnvelope.FromString(wire)
        if ((message.topic(), message.partition(), message.offset()) != (r["topic"], 0, r["offset"])
                or hashlib.sha256(wire).hexdigest() != r["raw_sha256"]
                or raw.capture_id.hex() != r["capture_id"] or not raw.test_provenance):
            raise ValueError("raw receipt does not match committed test bytes")
    finally:
        reader.close()
    observer = CanonicalObserver(packet, config)

    class BoundedConsumer:
        def __init__(self, consumer):
            self.consumer = consumer

        def __getattr__(self, key):
            return getattr(self.consumer, key)

        def poll(self, timeout):
            return self.consumer.poll(min(timeout, _remaining(deadline)))

        def committed(self, partitions, timeout):
            return self.consumer.committed(partitions, timeout=min(timeout, _remaining(deadline)))

    observer.consumer = BoundedConsumer(observer.consumer)
    observer.offset_reader = BoundedConsumer(observer.offset_reader)
    try:
        result = observer.observe(r, timeout=_remaining(deadline))
    finally:
        observer.close()
    if (result["raw_committed_next"] <= r["offset"]
            or set(result["read_committed_eof"]) != set(observer.topics)):
        raise RuntimeError("committed canonical barrier missing")
    matched = result["matched"]
    records = [dict(row) for row in matched]
    if any(row.get("partition") != 0 for row in records):
        raise RuntimeError("canonical observer partition missing or unexpected")
    return {"observed": True, "raw": {k: r[k] for k in ("topic", "partition", "offset", "capture_id")},
            "raw_committed_next": result["raw_committed_next"],
            "read_committed_eof": result["read_committed_eof"], "records": records,
            "outcome": "COMMITTED_OUTPUT" if records else "FILTERED_AFTER_COMMIT",
            "provenance": "TEST_ONLY", "acceptance": False}

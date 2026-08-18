#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import pathlib
import tempfile
import time
from collections import Counter

import yaml

os.environ.setdefault("QDL_PHASE8_PROJECT", "qdl_phase91_certification")
from phase80_broker_certification import (  # noqa: E402
    BOOTSTRAP,
    PROJECT,
    ROOT,
    add_acls,
    cleanup,
    compose,
    consume,
    create_topic,
    kafka,
    run,
    total_end_offset,
    v1_health,
    v1_topology,
    wait_for_cluster,
    wait_for_replicas,
)


CAPTURE = ROOT / "upgrade/evidence/captures/phase8-real-provider-frames.json.gz"
CAPTURE_EVIDENCE = ROOT / "upgrade/evidence/phase8-real-provider-shadow.json"
CANDIDATE = ROOT / "config/phase9/candidate-slice.yaml"
PREREQUISITE_DECISION = ROOT / "upgrade/evidence/phase90c-production-prerequisites.json"
OUTPUT = ROOT / "upgrade/evidence/phase91-rust-canary-certification.json"
REPORT = ROOT / "upgrade/evidence/PHASE91_RUST_CANARY_REPORT.md"
CHECKSUM = ROOT / "upgrade/evidence/phase91-evidence.sha256"
BUNDLE_PATH = ROOT / "target/phase91-authentic-replay.json"


def candidate_digest(payload: dict) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def consume_compacted_records(env: dict[str, str], topic: str) -> list[str]:
    offsets = kafka(
        env,
        "kafka-get-offsets.sh",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        "/etc/kafka/secrets/admin.properties",
        "--topic",
        topic,
        "--time",
        "-1",
    )
    records: list[str] = []
    for line in offsets.stdout.splitlines():
        fields = line.rsplit(":", 2)
        if len(fields) != 3 or not fields[1].isdigit() or not fields[2].isdigit():
            continue
        partition, end_offset = int(fields[1]), int(fields[2])
        if end_offset <= 0:
            continue
        result = kafka(
            env,
            "kafka-console-consumer.sh",
            "--bootstrap-server",
            BOOTSTRAP,
            "--consumer.config",
            "/etc/kafka/secrets/consumer.properties",
            "--topic",
            topic,
            "--partition",
            str(partition),
            "--offset",
            "earliest",
            "--timeout-ms",
            "5000",
            check=False,
            timeout=20.0,
        )
        records.extend(
            item.strip()
            for item in result.stdout.splitlines()
            if item.strip().startswith("{") and item.strip().endswith("}")
        )
    if not records:
        raise RuntimeError(f"compacted topic has offsets but no readable records: {topic}")
    return records


def authentic_fixtures(*, repeat: int) -> tuple[dict, dict]:
    compressed = CAPTURE.read_bytes()
    capture_evidence = json.loads(CAPTURE_EVIDENCE.read_text())
    actual_digest = hashlib.sha256(compressed).hexdigest()
    if actual_digest != capture_evidence["capture_bundle_sha256"]:
        raise RuntimeError("frozen authentic capture checksum mismatch")
    payload = json.loads(gzip.decompress(compressed))
    if (
        payload.get("schema") != "qdl.phase8.authentic-capture-bundle.v1"
        or payload.get("provenance") != "REAL_PROVIDER_READ_ONLY"
        or payload.get("production_writes") != 0
    ):
        raise RuntimeError("authentic capture provenance is invalid")
    fixtures = []
    for sequence, item in enumerate(
        (
            record
            for record in payload["captures"]
            if record.get("provider") == "BINANCE_DIRECT"
            and record.get("venue") == "BINANCE"
            and record.get("market") == "USDM"
            and record.get("native_symbol") == "BTCUSDT"
            and record.get("test_provenance") is False
        ),
        start=1,
    ):
        raw_frame = base64.b64decode(item["raw_frame_base64"], validate=True)
        if len(raw_frame) != item["raw_frame_bytes"]:
            raise RuntimeError("authentic raw frame length mismatch")
        if hashlib.sha256(raw_frame).hexdigest() != item["raw_frame_sha256"]:
            raise RuntimeError("authentic raw frame checksum mismatch")
        wrapper = json.loads(raw_frame)
        if wrapper.get("stream") != item["native_channel"] or not isinstance(wrapper.get("data"), dict):
            raise RuntimeError("authentic Binance combined frame is malformed")
        received = int(item["received_at_ns"])
        fixtures.append({
            "provider_kind": "binance_usdm_trade",
            "context": {
                "instrument_uid": "85ad7cb6-7ebf-5c81-9d82-12c4c10ca85c",
                "instrument_id": "BINANCE.USDM.PERPETUAL.BTCUSDT",
                "instrument_revision": 1,
                "venue": "BINANCE",
                "market": "USDM",
                "product_type": "PERPETUAL",
                "native_symbol": "BTCUSDT",
                "provider": "BINANCE_DIRECT",
                "source_id": "binance-usdm-phase91-canary",
                "lease_epoch": 2,
                "received_at_ns": received,
                "normalized_at_ns": received + 1,
                "published_at_ns": received + 2,
                "partition_sequence": sequence,
                "normalizer_version": "qdl-normalizer/2.0.0-phase91",
                "adapter_version": "binance-usdm/2.0.0-shadow",
                "config_revision": 1,
                "correlation_id": "phase91-authentic-replay",
                "source_session_id": item["source_session_id"],
                "connection_generation": int(item["connection_generation"]),
                "authority_revision": 2,
                "partition_plan_epoch": 1,
                "raw_capture_id": list(bytes.fromhex(item["capture_id"])),
                "raw_frame_sha256": list(bytes.fromhex(item["raw_frame_sha256"])),
            },
            "raw": wrapper["data"],
        })
    if len(fixtures) < 32:
        raise RuntimeError(f"too few authentic Binance frames: {len(fixtures)}")
    BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BUNDLE_PATH.write_text(json.dumps(
        {"fixtures": fixtures, "repeat": repeat},
        sort_keys=True,
        separators=(",", ":"),
    ))
    return {
        "path": str(BUNDLE_PATH),
        "fixtures": len(fixtures),
        "repeat": repeat,
        "events": len(fixtures) * repeat,
        "capture_sha256": actual_digest,
        "capture_provenance": payload["provenance"],
        "raw_checks_passed": len(fixtures),
    }, {"fixtures": fixtures, "repeat": repeat}


def parity(image: str, repeat: int) -> dict:
    capture, _ = authentic_fixtures(repeat=repeat)
    mount = f"type=bind,source={ROOT},target=/app,readonly"
    python_result = run([
        "docker", "run", "--rm", "--read-only", "--security-opt", "no-new-privileges:true",
        "--mount", mount,
        "--entrypoint", "python",
        "data-layer:v0.1.0",
        "/app/scripts/phase91_python_parity.py",
        "/app/target/phase91-authentic-replay.json",
    ], timeout=240)
    python_metrics = json.loads(python_result.stdout.strip().splitlines()[-1])
    rust_runs = []
    for _ in range(3):
        result = run([
            "docker", "run", "--rm", "--read-only", "--security-opt", "no-new-privileges:true",
            "--mount", mount,
            "--entrypoint", "/usr/local/bin/qdl-parity-replay",
            image,
            "/app/target/phase91-authentic-replay.json",
        ], timeout=240)
        rust_runs.append(json.loads(result.stdout.strip().splitlines()[-1]))
    expected = python_metrics["aggregate_sha256"]
    mismatch_runs = sum(
        item.get("aggregate_sha256") != expected
        or item.get("record_sha256") != python_metrics["record_sha256"]
        for item in rust_runs
    )
    if mismatch_runs:
        raise RuntimeError(f"authentic Python/Rust parity diverged in {mismatch_runs} runs")
    return {
        "status": "PASS",
        "capture": capture,
        "python": python_metrics,
        "rust_clean_process_runs": len(rust_runs),
        "rust_events_per_second_min": min(item["events_per_second"] for item in rust_runs),
        "aggregate_sha256": expected,
        "semantic_mismatches": 0,
        "process_restart_mismatches": 0,
    }


def rust_command(
    *, image: str, cert_dir: str, entrypoint: str, env_values: dict[str, str]
) -> list[str]:
    command = [
        "docker", "run", "--rm",
        "--network", f"{PROJECT}_phase8_shadow",
        "--read-only", "--security-opt", "no-new-privileges:true",
        "--mount", f"type=bind,source={cert_dir},target=/certs,readonly",
        "--entrypoint", entrypoint,
    ]
    for key, value in env_values.items():
        command.extend(("--env", f"{key}={value}"))
    command.append(image)
    return command


def broker_rehearsal(image: str, image_digest: str, candidate: dict, decision: dict) -> dict:
    topics = {
        "authority": "qdl.phase8.phase91.control.authority.v2",
        "audit": "qdl.phase8.phase91.audit.authority.v2",
        "shadow_raw": "qdl.phase8.phase91.shadow.raw.v2",
        "shadow": "qdl.phase8.phase91.shadow.canonical.v2",
        "canary": "qdl.phase8.phase91.canary.canonical.v2",
        "public": "qdl.phase8.phase91.public.must-remain-empty",
        "legacy": "qdl.phase8.phase91.legacy.must-remain-empty",
        "transport": "qdl.phase8.phase91.transport",
    }
    v1_before = v1_topology()
    health_before = v1_health()
    cleanup_result: dict[str, object] = {}
    result: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="qdl-phase91-certs-") as cert_dir:
        env = os.environ.copy()
        env["QDL_PHASE8_CERT_DIR"] = cert_dir
        run([str(ROOT / "scripts/phase80_generate_tls.sh"), cert_dir], env=env, timeout=120)
        try:
            compose(env, "down", "--volumes", "--remove-orphans", check=False, timeout=120)
            compose(env, "up", "-d", timeout=180)
            wait_for_cluster(env)
            # KRaft may answer one metadata request before every controller/broker
            # has remained stable. Require a short stable window before topic I/O.
            time.sleep(10.0)
            wait_for_cluster(env)
            running = set(
                compose(env, "ps", "--status", "running", "--services").stdout.splitlines()
            )
            if not {"kafka1", "kafka2", "kafka3"}.issubset(running):
                raise RuntimeError(f"Kafka brokers did not survive stability window: {sorted(running)}")
            for name, topic in topics.items():
                create_topic(
                    env,
                    topic,
                    partitions=1 if name in {"authority", "audit"} else 3,
                    cleanup_policy="compact" if name == "authority" else "delete",
                )
            add_acls(env)
            wait_for_replicas(env)
            before = {name: total_end_offset(env, topic) for name, topic in topics.items()}
            nonce = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:16]
            runtime_result = run(rust_command(
                image=image,
                cert_dir=cert_dir,
                entrypoint="/usr/local/bin/qdl-phase91-canary-rehearsal",
                env_values={
                    "QDL_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
                    "QDL_KAFKA_CERT_ROOT": "/certs",
                    "QDL_AUTHORITY_TOPIC": topics["authority"],
                    "QDL_AUDIT_TOPIC": topics["audit"],
                    "QDL_SHADOW_RAW_TOPIC": topics["shadow_raw"],
                    "QDL_SHADOW_CANONICAL_TOPIC": topics["shadow"],
                    "QDL_CANARY_CANONICAL_TOPIC": topics["canary"],
                    "QDL_PUBLIC_TOPIC": topics["public"],
                    "QDL_LEGACY_TOPIC": topics["legacy"],
                    "QDL_AUTHORITY_NONCE": nonce,
                    "QDL_CANDIDATE_DIGEST": candidate_digest(candidate),
                    "QDL_PREREQUISITE_BUNDLE_ID": decision["bundle_id"],
                    "QDL_SLICE_ID": candidate["slice_id"],
                    "QDL_SHADOW_OWNER_ID": candidate["owner_id"],
                    "QDL_CANARY_OWNER_ID": "rust-canary-binance-usdm-shard-0",
                },
            ), timeout=180)
            runtime = json.loads(runtime_result.stdout.strip().splitlines()[-1])
            after = {name: total_end_offset(env, topic) for name, topic in topics.items()}
            deltas = {name: after[name] - before[name] for name in topics}
            expected_deltas = {
                "authority": 4,
                "audit": 4,
                "shadow_raw": 0,
                "shadow": 2,
                "canary": 64,
                "public": 0,
                "legacy": 0,
                "transport": 0,
            }
            if runtime.get("status") != "PASS" or deltas != expected_deltas:
                raise RuntimeError(f"Phase 9.1 runtime/offset mismatch runtime={runtime} deltas={deltas}")

            compose(env, "stop", "kafka3")
            one_loss_before = total_end_offset(env, topics["transport"])
            one_loss = run(rust_command(
                image=image,
                cert_dir=cert_dir,
                entrypoint="/usr/local/bin/qdl-kafka-smoke",
                env_values={
                    "QDL_KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092,kafka2:9092",
                    "QDL_KAFKA_CERT_ROOT": "/certs",
                    "QDL_KAFKA_SMOKE_TOPIC": topics["transport"],
                    "QDL_KAFKA_SMOKE_NONCE": f"{nonce}-one-loss",
                },
            ), timeout=90)
            one_loss_after = total_end_offset(env, topics["transport"])
            one_replica_loss_acked = one_loss.returncode == 0 and one_loss_after == one_loss_before + 1
            min_isr_before = one_loss_after
            compose(env, "stop", "kafka2")
            min_isr = run(rust_command(
                image=image,
                cert_dir=cert_dir,
                entrypoint="/usr/local/bin/qdl-kafka-smoke",
                env_values={
                    "QDL_KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092",
                    "QDL_KAFKA_CERT_ROOT": "/certs",
                    "QDL_KAFKA_SMOKE_TOPIC": topics["transport"],
                    "QDL_KAFKA_SMOKE_NONCE": f"{nonce}-min-isr",
                },
            ), check=False, timeout=35)
            min_isr_client_failed = min_isr.returncode != 0
            compose(env, "start", "kafka2", "kafka3", timeout=120)
            wait_for_cluster(env)
            wait_for_replicas(env)
            min_isr_after = total_end_offset(env, topics["transport"])
            min_isr_failed_closed = (
                min_isr_client_failed and min_isr_after == min_isr_before
            )
            if not one_replica_loss_acked or not min_isr_failed_closed:
                raise RuntimeError(
                    f"broker durability gate failed one_loss={one_replica_loss_acked} "
                    f"min_isr={min_isr_failed_closed} "
                    f"min_isr_returncode={min_isr.returncode} "
                    f"offset_before={min_isr_before} offset_after={min_isr_after} "
                    f"stderr={min_isr.stderr[-800:]}"
                )
            compose(env, "restart", "kafka1", "kafka2", "kafka3", timeout=120)
            wait_for_cluster(env)
            wait_for_replicas(env)

            audit = [json.loads(item) for item in consume(
                env, topics["audit"], 4, f"phase8-phase91-audit-{nonce}"
            )]
            authority = [
                json.loads(item)
                for item in consume_compacted_records(env, topics["authority"])
            ]
            slow_started = time.monotonic()
            time.sleep(1.0)
            canary = [json.loads(item) for item in consume(
                env, topics["canary"], 64, f"phase8-phase91-slow-{nonce}", timeout=60
            )]
            catchup_seconds = time.monotonic() - slow_started
            audit_states = [item["state"] for item in audit]
            latest = max(authority, key=lambda item: item["authority_revision"])
            canary_watermarks = [item["source_watermark"] for item in canary]
            if audit_states != ["RUST_SHADOW", "RUST_CANARY", "BLOCKED", "RUST_SHADOW"]:
                raise RuntimeError(f"authority audit order diverged: {audit_states}")
            if latest["state"] != "RUST_SHADOW" or latest["authority_revision"] != 4:
                raise RuntimeError(f"latest compacted authority diverged: {latest}")
            if canary_watermarks != list(range(102, 166)):
                raise RuntimeError("slow consumer catch-up changed canary order or coverage")
            result = {
                "status": "PASS",
                "schema": "qdl.phase91.broker-rehearsal.v1",
                "mode": "ISOLATED_REHEARSAL",
                "production_authorized": False,
                "image_digest": image_digest,
                "runtime": runtime,
                "offset_deltas": deltas,
                "one_replica_loss_acked": one_replica_loss_acked,
                "min_isr_failed_closed": min_isr_failed_closed,
                "authority_audit_states_after_restart": audit_states,
                "latest_authority_after_restart": {
                    "state": latest["state"],
                    "revision": latest["authority_revision"],
                    "owner_id": latest["owner_id"],
                    "lease_epoch": latest["lease_epoch"],
                },
                "slow_consumer": {
                    "delayed_seconds": 1.0,
                    "catchup_seconds": catchup_seconds,
                    "records": len(canary),
                    "ordered_gap_free": True,
                },
                "public_writes": 0,
                "legacy_writes": 0,
                "final_authority": "RUST_SHADOW",
            }
        except Exception as error:
            status = compose(env, "ps", "--all", check=False, timeout=30)
            logs = compose(env, "logs", "--no-color", "--tail", "240", check=False, timeout=60)
            diagnostic = (status.stdout + status.stderr + logs.stdout + logs.stderr)[-12000:]
            raise RuntimeError(
                f"Phase 9.1 broker rehearsal failed before cleanup: {error}\n"
                f"isolated broker diagnostics:\n{diagnostic}"
            ) from error
        finally:
            cleanup_result = cleanup(env)
    v1_after = v1_topology()
    health_after = v1_health()
    cleanup_result.update({
        "v1_health_before": health_before,
        "v1_health_after": health_after,
        "v1_topology_unchanged": v1_before == v1_after,
    })
    result["cleanup"] = cleanup_result
    if (
        health_before != 200
        or health_after != 200
        or not cleanup_result["v1_topology_unchanged"]
        or any(cleanup_result[key] for key in ("containers_after", "networks_after", "volumes_after"))
    ):
        raise RuntimeError(f"Phase 9.1 cleanup/V1 invariant failed: {cleanup_result}")
    return result


def render_report(evidence: dict) -> str:
    parity = evidence["parity"]
    broker = evidence["broker"]
    capture = parity["capture"]
    cleanup_result = broker["cleanup"]
    return f"""# Phase 9.1 Rust Canary Certification Report

## Decision

- Status: `{evidence['status']}`
- Production authorized: `{str(evidence['production_authorized']).lower()}`
- Production mutations: `{evidence['production_mutations']}`
- Prerequisite decision: `{evidence['prerequisite_decision']}`
- Slice: `{evidence['slice_id']}`
- Candidate digest: `{evidence['candidate_digest']}`

## Authentic Parity

- Provenance: `{capture['capture_provenance']}`
- Frozen fixtures: `{capture['fixtures']}`
- Repetition: `{capture['repeat']}`
- Canonical events: `{capture['events']}`
- Semantic mismatches: `{parity['semantic_mismatches']}`
- Clean Rust process runs: `{parity['rust_clean_process_runs']}`
- Aggregate SHA-256: `{parity['aggregate_sha256']}`
- Python throughput: `{parity['python']['events_per_second']:.3f}` events/s
- Minimum Rust throughput: `{parity['rust_events_per_second_min']:.3f}` events/s

## Authority And Broker Recovery

- Transition audit: `{', '.join(broker['authority_audit_states_after_restart'])}`
- Final authority: `{broker['final_authority']}`
- One-replica-loss ACK: `{str(broker['one_replica_loss_acked']).lower()}`
- Below-min-ISR fail closed: `{str(broker['min_isr_failed_closed']).lower()}`
- Slow-consumer records: `{broker['slow_consumer']['records']}`
- Slow-consumer ordered and gap-free: `{str(broker['slow_consumer']['ordered_gap_free']).lower()}`
- Public writes: `{broker['public_writes']}`
- Legacy writes: `{broker['legacy_writes']}`

## Isolation And Cleanup

- V1 health before/after: `{cleanup_result['v1_health_before']}/{cleanup_result['v1_health_after']}`
- V1 topology unchanged: `{str(cleanup_result['v1_topology_unchanged']).lower()}`
- Containers/networks/volumes remaining: `{cleanup_result['containers_after']}/{cleanup_result['networks_after']}/{cleanup_result['volumes_after']}`

## Remaining External Gates

- Production Phase 9.0-C infrastructure and operator gates remain `NO_GO_EXTERNAL`.
- Same-host replicated broker rehearsal is not an independent production failure domain.
- Python V1 remains the sole authoritative public and legacy writer. This report does not authorize a production `RUST_CANARY` transition.
"""


def write_evidence(evidence: dict) -> None:
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    REPORT.write_text(render_report(evidence))
    lines = []
    for path in (OUTPUT, REPORT):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(ROOT)}")
    CHECKSUM.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-image", required=True)
    parser.add_argument("--repeat", type=int, default=200)
    args = parser.parse_args()
    if args.repeat < 10:
        raise ValueError("Phase 9.1 parity repeat must be at least 10")
    candidate = yaml.safe_load(CANDIDATE.read_text())
    decision = json.loads(PREREQUISITE_DECISION.read_text())
    digest = candidate_digest(candidate)
    if decision.get("candidate_digest") != digest:
        raise RuntimeError("Phase 9.0-C decision is not bound to the selected candidate")
    if decision.get("decision") != "NO_GO_EXTERNAL":
        raise RuntimeError("this isolated harness expects current production NO_GO_EXTERNAL")
    inspect = run([
        "docker", "image", "inspect", args.rust_image, "--format", "{{.Id}}"
    ])
    image_digest = inspect.stdout.strip()
    parity_result: dict = {}
    broker_result: dict = {}
    try:
        parity_result = parity(args.rust_image, args.repeat)
        broker_result = broker_rehearsal(
            args.rust_image, image_digest, candidate, decision
        )
    finally:
        BUNDLE_PATH.unlink(missing_ok=True)
    evidence = {
        "schema": "qdl.phase91.rust-canary-certification.v1",
        "status": "COMPLETE_IMPLEMENTATION / CANARY_NOT_AUTHORIZED",
        "issued_at_ns": time.time_ns(),
        "slice_id": candidate["slice_id"],
        "candidate_digest": digest,
        "prerequisite_bundle_id": decision["bundle_id"],
        "prerequisite_decision": decision["decision"],
        "production_authorized": False,
        "production_mutations": 0,
        "python_v1_public_authority_unchanged": True,
        "parity": parity_result,
        "broker": broker_result,
        "technical_debt": [
            "production Phase 9.0-C infrastructure/operator gates remain NO_GO_EXTERNAL",
            "same-host replicated broker rehearsal is not an independent production failure domain",
        ],
    }
    write_evidence(evidence)
    print(json.dumps({
        "status": evidence["status"],
        "authentic_events": parity_result["capture"]["events"],
        "semantic_mismatches": parity_result["semantic_mismatches"],
        "public_writes": broker_result["public_writes"],
        "legacy_writes": broker_result["legacy_writes"],
        "cleanup": broker_result["cleanup"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

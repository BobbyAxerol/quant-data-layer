#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import tempfile
import time

import yaml

os.environ.setdefault("QDL_PHASE8_PROJECT", "qdl_phase92_certification")

from phase80_broker_certification import (  # noqa: E402
    BOOTSTRAP,
    PROJECT,
    ROOT,
    add_acls,
    cleanup,
    compose,
    consume,
    create_topic,
    run,
    total_end_offset,
    v1_health,
    v1_topology,
    wait_for_cluster,
    wait_for_replicas,
)
from phase91_canary_certification import (  # noqa: E402
    BUNDLE_PATH,
    candidate_digest,
    consume_compacted_records,
    parity,
    rust_command,
)


CANDIDATE = ROOT / "config/phase9/candidate-slice.yaml"
PREREQUISITE_DECISION = ROOT / "upgrade/evidence/phase90c-production-prerequisites.json"
MIGRATION_EVIDENCE = ROOT / "upgrade/evidence/phase92-authority-migration.json"
OUTPUT = ROOT / "upgrade/evidence/phase92-bounded-primary-certification.json"
REPORT = ROOT / "upgrade/evidence/PHASE92_BOUNDED_PRIMARY_REPORT.md"
CHECKSUM = ROOT / "upgrade/evidence/phase92-evidence.sha256"


def broker_rehearsal(
    image: str, image_digest: str, candidate: dict, decision: dict
) -> dict:
    topics = {
        "authority": "qdl.phase8.phase92.control.authority.v3",
        "audit": "qdl.phase8.phase92.audit.authority.v3",
        "checkpoint": "qdl.phase8.phase92.control.checkpoint.v1",
        "handoff": "qdl.phase8.phase92.control.handoff.v1",
        "primary": "qdl.phase8.phase92.primary.canonical.v1",
        "public": "qdl.phase8.phase92.isolated.public.v2",
        "legacy": "qdl.phase8.phase92.isolated.legacy.v1",
        "production_public": "qdl.phase8.phase92.production-public.must-remain-empty",
        "production_legacy": "qdl.phase8.phase92.production-legacy.must-remain-empty",
        "transport": "qdl.phase8.phase92.transport",
    }
    v1_before = v1_topology()
    health_before = v1_health()
    cleanup_result: dict[str, object] = {}
    result: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="qdl-phase92-certs-") as cert_dir:
        env = os.environ.copy()
        env["QDL_PHASE8_CERT_DIR"] = cert_dir
        run([str(ROOT / "scripts/phase80_generate_tls.sh"), cert_dir], env=env, timeout=120)
        try:
            compose(env, "down", "--volumes", "--remove-orphans", check=False, timeout=120)
            compose(env, "up", "-d", timeout=180)
            wait_for_cluster(env)
            time.sleep(10.0)
            wait_for_cluster(env)
            running = set(
                compose(env, "ps", "--status", "running", "--services").stdout.splitlines()
            )
            if not {"kafka1", "kafka2", "kafka3"}.issubset(running):
                raise RuntimeError(
                    f"Kafka brokers did not survive stability window: {sorted(running)}"
                )
            for name, topic in topics.items():
                create_topic(
                    env,
                    topic,
                    partitions=1 if name != "transport" else 3,
                    cleanup_policy="compact" if name == "authority" else "delete",
                )
            add_acls(env)
            wait_for_replicas(env)
            before = {name: total_end_offset(env, topic) for name, topic in topics.items()}
            nonce = hashlib.sha256(str(time.time_ns()).encode()).hexdigest()[:16]
            runtime_env = {
                "QDL_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
                "QDL_KAFKA_CERT_ROOT": "/certs",
                "QDL_AUTHORITY_TOPIC": topics["authority"],
                "QDL_AUDIT_TOPIC": topics["audit"],
                "QDL_CHECKPOINT_TOPIC": topics["checkpoint"],
                "QDL_HANDOFF_TOPIC": topics["handoff"],
                "QDL_PRIMARY_CANONICAL_TOPIC": topics["primary"],
                "QDL_ISOLATED_PUBLIC_TOPIC": topics["public"],
                "QDL_ISOLATED_LEGACY_TOPIC": topics["legacy"],
                "QDL_PRODUCTION_PUBLIC_TOPIC": topics["production_public"],
                "QDL_PRODUCTION_LEGACY_TOPIC": topics["production_legacy"],
                "QDL_AUTHORITY_NONCE": nonce,
                "QDL_CANDIDATE_DIGEST": candidate_digest(candidate),
                "QDL_PREREQUISITE_BUNDLE_ID": decision["bundle_id"],
                "QDL_SLICE_ID": candidate["slice_id"],
                "QDL_PYTHON_OWNER_ID": "python-primary-isolated",
                "QDL_RUST_OWNER_ID": "rust-primary-isolated",
                "QDL_ROLLBACK_OWNER_ID": "python-rollback-isolated",
            }
            runtime_result = run(
                rust_command(
                    image=image,
                    cert_dir=cert_dir,
                    entrypoint="/usr/local/bin/qdl-phase92-primary-rehearsal",
                    env_values=runtime_env,
                ),
                timeout=180,
            )
            runtime = json.loads(runtime_result.stdout.strip().splitlines()[-1])
            if runtime.get("status") != "PASS" or not all(
                runtime.get("checks", {}).values()
            ):
                raise RuntimeError(f"Phase 9.2 runtime checks failed: {runtime}")

            recovery_result = run(
                rust_command(
                    image=image,
                    cert_dir=cert_dir,
                    entrypoint="/usr/local/bin/qdl-phase92-primary-rehearsal",
                    env_values={
                        **runtime_env,
                        "QDL_REHEARSAL_MODE": "RECOVERY_VERIFY",
                        "QDL_RECOVERY_AUTHORITY_REVISION": "11",
                        "QDL_RECOVERY_FIRST_WATERMARK": "101",
                        "QDL_RECOVERY_LAST_WATERMARK": "180",
                    },
                ),
                timeout=180,
            )
            recovery = json.loads(recovery_result.stdout.strip().splitlines()[-1])
            if recovery.get("status") != "PASS" or not all(
                recovery.get("checks", {}).values()
            ):
                raise RuntimeError(
                    f"Phase 9.2 process restart recovery failed: {recovery}"
                )

            after = {name: total_end_offset(env, topic) for name, topic in topics.items()}
            deltas = {name: after[name] - before[name] for name in topics}
            expected = {
                "authority": 5,
                "audit": 5,
                "checkpoint": 2,
                "handoff": 2,
                "primary": 81,
                "public": 81,
                "legacy": 81,
                "production_public": 0,
                "production_legacy": 0,
                "transport": 0,
            }
            if deltas != expected:
                raise RuntimeError(
                    f"Phase 9.2 isolated topic deltas diverged: {deltas} != {expected}"
                )

            compose(env, "stop", "kafka3")
            one_loss_before = total_end_offset(env, topics["transport"])
            one_loss = run(
                rust_command(
                    image=image,
                    cert_dir=cert_dir,
                    entrypoint="/usr/local/bin/qdl-kafka-smoke",
                    env_values={
                        "QDL_KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092,kafka2:9092",
                        "QDL_KAFKA_CERT_ROOT": "/certs",
                        "QDL_KAFKA_SMOKE_TOPIC": topics["transport"],
                        "QDL_KAFKA_SMOKE_NONCE": f"{nonce}-one-loss",
                    },
                ),
                check=False,
                timeout=35,
            )
            one_loss_after = total_end_offset(env, topics["transport"])
            one_replica_loss_acked = (
                one_loss.returncode == 0 and one_loss_after == one_loss_before + 1
            )
            compose(env, "stop", "kafka2")
            min_isr_before = one_loss_after
            min_isr = run(
                rust_command(
                    image=image,
                    cert_dir=cert_dir,
                    entrypoint="/usr/local/bin/qdl-kafka-smoke",
                    env_values={
                        "QDL_KAFKA_BOOTSTRAP_SERVERS": "kafka1:9092",
                        "QDL_KAFKA_CERT_ROOT": "/certs",
                        "QDL_KAFKA_SMOKE_TOPIC": topics["transport"],
                        "QDL_KAFKA_SMOKE_NONCE": f"{nonce}-min-isr",
                    },
                ),
                check=False,
                timeout=35,
            )
            compose(env, "start", "kafka2", "kafka3", timeout=120)
            wait_for_cluster(env)
            wait_for_replicas(env)
            min_isr_after = total_end_offset(env, topics["transport"])
            min_isr_failed_closed = (
                min_isr.returncode != 0 and min_isr_after == min_isr_before
            )
            if not one_replica_loss_acked or not min_isr_failed_closed:
                raise RuntimeError(
                    "Phase 9.2 broker durability gate failed "
                    f"one_loss={one_replica_loss_acked} min_isr={min_isr_failed_closed}"
                )

            compose(env, "restart", "kafka1", "kafka2", "kafka3", timeout=120)
            wait_for_cluster(env)
            wait_for_replicas(env)

            audit = [
                json.loads(item)
                for item in consume(
                    env,
                    topics["audit"],
                    5,
                    f"phase8-phase92-audit-{nonce}",
                )
            ]
            checkpoints = [
                json.loads(item)
                for item in consume(
                    env,
                    topics["checkpoint"],
                    2,
                    f"phase8-phase92-checkpoint-{nonce}",
                )
            ]
            handoffs = [
                json.loads(item)
                for item in consume(
                    env,
                    topics["handoff"],
                    2,
                    f"phase8-phase92-handoff-{nonce}",
                )
            ]
            time.sleep(1.0)
            slow_started = time.monotonic()
            projections = {}
            for name in ("primary", "public", "legacy"):
                projections[name] = [
                    json.loads(item)
                    for item in consume(
                        env,
                        topics[name],
                        81,
                        f"phase8-phase92-slow-{name}-{nonce}",
                        timeout=60,
                    )
                ]
            catchup_seconds = time.monotonic() - slow_started
            authority = [
                json.loads(item)
                for item in consume_compacted_records(env, topics["authority"])
            ]
            latest = max(authority, key=lambda item: item["authority_revision"])
            states = [item["state"] for item in audit]
            expected_states = [
                "RUST_CANARY",
                "RUST_PRIMARY",
                "BLOCKED",
                "ROLLBACK_PENDING",
                "PYTHON_PRIMARY",
            ]
            watermarks = [
                item["source_watermark"] for item in projections["primary"]
            ]
            projection_parity = (
                projections["primary"] == projections["public"]
                == projections["legacy"]
            )
            boundary_gap_free = watermarks == list(range(101, 182))
            owner_boundary = (
                all(
                    item["owner_id"] == "rust-primary-isolated"
                    for item in projections["primary"][:64]
                )
                and all(
                    item["owner_id"] == "python-rollback-isolated"
                    for item in projections["primary"][64:]
                )
            )
            if (
                states != expected_states
                or latest["state"] != "PYTHON_PRIMARY"
                or latest["authority_revision"] != 11
                or len(checkpoints) != 2
                or len(handoffs) != 2
                or not projection_parity
                or not boundary_gap_free
                or not owner_boundary
            ):
                raise RuntimeError("Phase 9.2 recovery/projection evidence diverged")
            result = {
                "schema": "qdl.phase92.broker-rehearsal.v1",
                "status": "PASS",
                "mode": "ISOLATED_REHEARSAL",
                "production_authorized": False,
                "image_digest": image_digest,
                "runtime": runtime,
                "process_restart_recovery": recovery,
                "offset_deltas": deltas,
                "authority_audit_states_after_restart": states,
                "latest_authority_after_restart": {
                    "state": latest["state"],
                    "revision": latest["authority_revision"],
                    "owner_id": latest["owner_id"],
                    "lease_epoch": latest["lease_epoch"],
                },
                "terminal_checkpoints": len(checkpoints),
                "accepted_handoffs": len(handoffs),
                "projection_parity": projection_parity,
                "boundary_gap_free": boundary_gap_free,
                "owner_boundary_correct": owner_boundary,
                "one_replica_loss_acked": one_replica_loss_acked,
                "min_isr_failed_closed": min_isr_failed_closed,
                "slow_consumer": {
                    "delayed_seconds": 1.0,
                    "catchup_seconds": catchup_seconds,
                    "records_per_projection": 81,
                    "ordered_gap_free": boundary_gap_free,
                },
                "cutover_ms": runtime["cutover_ms"],
                "rollback_ms": runtime["rollback_ms"],
                "production_public_writes": 0,
                "production_legacy_writes": 0,
                "final_authority": "PYTHON_PRIMARY",
            }
        except Exception as error:
            status = compose(env, "ps", "--all", check=False, timeout=30)
            logs = compose(
                env, "logs", "--no-color", "--tail", "240", check=False, timeout=60
            )
            diagnostic = (
                status.stdout + status.stderr + logs.stdout + logs.stderr
            )[-12000:]
            raise RuntimeError(
                f"Phase 9.2 broker rehearsal failed before cleanup: {error}\n"
                f"isolated broker diagnostics:\n{diagnostic}"
            ) from error
        finally:
            cleanup_result = cleanup(env)
    v1_after = v1_topology()
    health_after = v1_health()
    cleanup_result.update(
        {
            "v1_health_before": health_before,
            "v1_health_after": health_after,
            "v1_topology_unchanged": v1_before == v1_after,
        }
    )
    result["cleanup"] = cleanup_result
    if (
        health_before != 200
        or health_after != 200
        or not cleanup_result["v1_topology_unchanged"]
        or any(
            cleanup_result[key]
            for key in ("containers_after", "networks_after", "volumes_after")
        )
    ):
        raise RuntimeError(f"Phase 9.2 cleanup/V1 invariant failed: {cleanup_result}")
    return result


def render_report(evidence: dict) -> str:
    parity_result = evidence["parity"]
    broker = evidence["broker"]
    cleanup_result = broker["cleanup"]
    return f"""# Phase 9.2 Bounded Rust Primary Certification Report

## Decision

- Status: `{evidence['status']}`
- Production authorized: `{str(evidence['production_authorized']).lower()}`
- Production mutations: `{evidence['production_mutations']}`
- Prerequisite decision: `{evidence['prerequisite_decision']}`
- Slice: `{evidence['slice_id']}`
- Candidate digest: `{evidence['candidate_digest']}`

## Authentic Parity

- Provenance: `{parity_result['capture']['capture_provenance']}`
- Canonical events: `{parity_result['capture']['events']}`
- Semantic mismatches: `{parity_result['semantic_mismatches']}`
- Clean Rust process runs: `{parity_result['rust_clean_process_runs']}`

## Terminal Handoff And Recovery

- Authority states: `{', '.join(broker['authority_audit_states_after_restart'])}`
- Terminal checkpoints / accepted handoffs: `{broker['terminal_checkpoints']} / {broker['accepted_handoffs']}`
- Projection parity: `{str(broker['projection_parity']).lower()}`
- Boundary gap-free: `{str(broker['boundary_gap_free']).lower()}`
- Owner boundary correct: `{str(broker['owner_boundary_correct']).lower()}`
- Restart recovery: `{broker['process_restart_recovery']['status']}`
- Recovered target watermarks: `{broker['process_restart_recovery']['restored_target_watermarks']}`
- First post-restart watermark: `{broker['process_restart_recovery']['resumed_watermark']}`
- Cutover / rollback measurement: `{broker['cutover_ms']:.3f} ms / {broker['rollback_ms']:.3f} ms`
- One-replica-loss ACK: `{str(broker['one_replica_loss_acked']).lower()}`
- Below-min-ISR fail closed: `{str(broker['min_isr_failed_closed']).lower()}`
- Final authority: `{broker['final_authority']}`
- Production public / legacy writes: `{broker['production_public_writes']} / {broker['production_legacy_writes']}`

## Isolation And Cleanup

- V1 health before/after: `{cleanup_result['v1_health_before']} / {cleanup_result['v1_health_after']}`
- V1 topology unchanged: `{str(cleanup_result['v1_topology_unchanged']).lower()}`
- Containers/networks/volumes remaining: `{cleanup_result['containers_after']} / {cleanup_result['networks_after']} / {cleanup_result['volumes_after']}`

## Remaining External Gates

- Phase 9.0-C remains `NO_GO_EXTERNAL`; a production primary transition is not authorized.
- Same-host replicated broker rehearsal is not an independent production failure domain.
- A real production canary hold and explicit exact-slice approval remain required.
"""


def write_evidence(evidence: dict) -> None:
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    REPORT.write_text(render_report(evidence))
    lines = []
    for path in (OUTPUT, REPORT, MIGRATION_EVIDENCE):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        lines.append(f"{digest}  {path.relative_to(ROOT)}")
    CHECKSUM.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-image", required=True)
    parser.add_argument("--repeat", type=int, default=200)
    args = parser.parse_args()
    if args.repeat < 10:
        raise ValueError("Phase 9.2 parity repeat must be at least 10")
    candidate = yaml.safe_load(CANDIDATE.read_text())
    decision = json.loads(PREREQUISITE_DECISION.read_text())
    digest = candidate_digest(candidate)
    if decision.get("candidate_digest") != digest:
        raise RuntimeError("Phase 9.0-C decision does not bind the candidate")
    if decision.get("decision") != "NO_GO_EXTERNAL":
        raise RuntimeError("isolated Phase 9.2 harness expects NO_GO_EXTERNAL")
    migration = json.loads(MIGRATION_EVIDENCE.read_text())
    if migration.get("status") != "PASS" or migration.get("production_mutations") != 0:
        raise RuntimeError("Phase 9.2 migration evidence is absent or invalid")
    inspect = run(
        ["docker", "image", "inspect", args.rust_image, "--format", "{{.Id}}"]
    )
    image_digest = inspect.stdout.strip()
    try:
        parity_result = parity(args.rust_image, args.repeat)
        broker_result = broker_rehearsal(
            args.rust_image, image_digest, candidate, decision
        )
    finally:
        BUNDLE_PATH.unlink(missing_ok=True)
    evidence = {
        "schema": "qdl.phase92.bounded-primary-certification.v1",
        "status": "COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED",
        "issued_at_ns": time.time_ns(),
        "slice_id": candidate["slice_id"],
        "candidate_digest": digest,
        "prerequisite_bundle_id": decision["bundle_id"],
        "prerequisite_decision": decision["decision"],
        "production_authorized": False,
        "production_mutations": 0,
        "python_v1_public_authority_unchanged": True,
        "migration": migration,
        "parity": parity_result,
        "broker": broker_result,
        "technical_debt": [
            "production Phase 9.0-C infrastructure/operator gates remain NO_GO_EXTERNAL",
            "real production canary hold and exact-slice approval remain unavailable",
            "same-host replicated broker is not an independent failure domain",
        ],
    }
    write_evidence(evidence)
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "authentic_events": parity_result["capture"]["events"],
                "semantic_mismatches": parity_result["semantic_mismatches"],
                "projection_parity": broker_result["projection_parity"],
                "boundary_gap_free": broker_result["boundary_gap_free"],
                "production_public_writes": broker_result["production_public_writes"],
                "production_legacy_writes": broker_result["production_legacy_writes"],
                "cleanup": broker_result["cleanup"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

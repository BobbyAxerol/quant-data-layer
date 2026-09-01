#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.canary.phase93 import (  # noqa: E402
    DecommissionRequest,
    ExpansionManifest,
    ExpansionType,
    HoldScope,
    PrimaryHoldEvaluator,
    PrimaryHoldIdentity,
    PrimaryHoldObservation,
    PrimaryHoldPolicy,
    ProductionClosureAuthorizer,
    RollbackWindowClosure,
    assess_decommission,
)
from qdl.certification.prerequisites import CandidateSlice  # noqa: E402
from scripts.phase80_broker_certification import v1_health, v1_topology  # noqa: E402


NO_GO = ROOT / "upgrade/evidence/phase90c-production-prerequisites.json"
PHASE92 = ROOT / "upgrade/evidence/phase92-bounded-primary-certification.json"
MIGRATION = ROOT / "upgrade/evidence/phase93-hold-close-migration.json"
OUTPUT = ROOT / "upgrade/evidence/phase93-hold-close-expand-certification.json"
REPORT = ROOT / "upgrade/evidence/PHASE93_HOLD_CLOSE_EXPAND_REPORT.md"
CHECKSUM = ROOT / "upgrade/evidence/phase93-evidence.sha256"


def uid(label: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"qdl-phase93:{label}"))


def digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def certify_test_hold(
    candidate: CandidateSlice, bundle_id: str, now_ns: int
):
    policy = PrimaryHoldPolicy(
        minimum_duration_seconds=120,
        sample_interval_seconds=60,
        max_sample_gap_seconds=60,
        max_lag_ms=500,
        max_freshness_ms=1000,
        max_queue_depth=1000,
        max_spool_bytes=1_000_000,
        max_cpu_percent=80.0,
        max_rss_mb=512.0,
    )
    start = now_ns - 120_000_000_000
    identity = PrimaryHoldIdentity(
        schema="qdl.primary-hold.v1",
        hold_id=uid("test-hold"),
        slice_id=str(candidate.payload["slice_id"]),
        candidate_digest=candidate.digest,
        prerequisite_bundle_id=bundle_id,
        owner_id="rust-primary-test-fixture",
        authority_revision=8,
        lease_epoch=12,
        partition_plan_epoch=1,
        started_at_ns=start,
        required_until_ns=now_ns,
        policy_digest=policy.digest,
    )
    evaluator = PrimaryHoldEvaluator(
        identity=identity,
        policy=policy,
        scope=HoldScope.TEST_REHEARSAL,
    )
    for sequence, watermark in ((1, 110), (2, 120)):
        reason = evaluator.observe(
            PrimaryHoldObservation(
                schema="qdl.primary-hold-observation.v1",
                observation_id=uid(f"test-observation-{sequence}"),
                hold_id=identity.hold_id,
                slice_id=identity.slice_id,
                candidate_digest=identity.candidate_digest,
                owner_id=identity.owner_id,
                authority_revision=identity.authority_revision,
                lease_epoch=identity.lease_epoch,
                partition_plan_epoch=identity.partition_plan_epoch,
                sequence=sequence,
                observed_at_ns=start + sequence * 60_000_000_000,
                last_watermark=watermark,
                lag_ms=10,
                freshness_ms=20,
                queue_depth=1,
                spool_bytes=100,
                cpu_percent=10.0,
                rss_mb=64.0,
                registered_consumers=2,
                healthy_consumers=2,
                checkpoint_watermark=watermark,
            )
        )
        if reason != "PASS":
            raise RuntimeError(f"test hold observation failed: {reason}")
    return evaluator.decision(decision_id=uid("test-hold-decision"), now_ns=now_ns)


def test_parent_closure(
    candidate: CandidateSlice, bundle_id: str, now_ns: int
) -> RollbackWindowClosure:
    return RollbackWindowClosure(
        schema="qdl.rollback-window-closure.v1",
        closure_id=uid("test-parent-closure"),
        slice_id=str(candidate.payload["slice_id"]),
        candidate_digest=candidate.digest,
        prerequisite_bundle_id=bundle_id,
        owner_id="rust-primary-test-fixture",
        authority_revision=8,
        lease_epoch=12,
        partition_plan_epoch=1,
        hold_decision_id=uid("test-hold-decision"),
        hold_decision_digest=digest("test-hold-decision"),
        consumer_registry_snapshot_id=uid("test-consumer-registry"),
        consumer_registry_digest=digest("test-consumer-registry"),
        authority_registry_snapshot_id=uid("test-authority-registry"),
        authority_registry_digest=digest("test-authority-registry"),
        rollback_rehearsal_id=uid("test-rollback"),
        rollback_rehearsal_digest=digest("test-rollback"),
        approval_id=uid("test-approval"),
        approval_digest=digest("test-approval"),
        operator="phase93-test-fixture",
        change_ticket="QDL-93-TEST",
        closed_at_ns=now_ns,
        production_authorized=True,
    )


def render_report(evidence: dict) -> str:
    parent = evidence["parent_phase92"]
    control = evidence["control_plane_fixture"]
    return f"""# Phase 9.3 Hold, Close And Expand Certification Report

## Decision

- Status: {evidence['status']}
- Production authorized: {evidence['production_authorized']}
- Production hold started: {evidence['production_hold_started']}
- Production rollback window closed: {evidence['production_rollback_window_closed']}
- Production expansions authorized: {evidence['production_expansions_authorized']}
- Production mutations: {evidence['production_mutations']}

## Parent Evidence

- Phase 9.2 status: {parent['status']}
- Authentic provider events: {parent['authentic_events']}
- Semantic mismatches: {parent['semantic_mismatches']}
- Parent production authorized: {parent['production_authorized']}

## Isolated Control Plane

- Provenance: {control['provenance']}
- Accelerated time is production evidence: {control['accelerated_time_is_production_evidence']}
- Test hold status: {control['test_hold_status']}
- Test hold production authorized: {control['test_hold_production_authorized']}
- Current no-go rejection: {control['current_no_go_rejection']}
- Local Phase 9.2 production eligible: {control['local_phase92_production_eligible']}
- Expansion manifests: {control['expansion_manifest_count']}
- Decommission decision: {control['decommission_reason']}

## Persistence And Isolation

- Migration: {evidence['migration']['status']}
- Closure changed authority: {not evidence['migration']['closure_did_not_mutate_authority']}
- V1 health before/after: {evidence['v1']['health_before']} / {evidence['v1']['health_after']}
- V1 topology unchanged: {evidence['v1']['topology_unchanged']}
- Disposable resources remaining: {evidence['cleanup']['resources_remaining']}

## External Gates

Phase 9.0-C remains NO_GO_EXTERNAL. There is no real Rust primary, production
hold duration, production consumer checkpoint set or operator closure approval.
No rollback window, expansion or Python decommission is authorized.
"""


def freeze(evidence: dict) -> None:
    OUTPUT.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    REPORT.write_text(render_report(evidence))
    entries = []
    for path in (OUTPUT, REPORT, MIGRATION):
        entries.append(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
            f"{path.relative_to(ROOT)}"
        )
    CHECKSUM.write_text("\n".join(entries) + "\n")


def main() -> int:
    argparse.ArgumentParser().parse_args()
    candidate = CandidateSlice.load(ROOT / "config/phase9/candidate-slice.yaml")
    no_go = json.loads(NO_GO.read_text())
    phase92 = json.loads(PHASE92.read_text())
    migration = json.loads(MIGRATION.read_text())
    if no_go.get("decision") != "NO_GO_EXTERNAL":
        raise RuntimeError("Phase 9.3 expects current NO_GO_EXTERNAL")
    if (
        phase92.get("status")
        != "COMPLETE_IMPLEMENTATION / PRIMARY_NOT_AUTHORIZED"
        or phase92.get("production_authorized") is not False
        or phase92["parity"]["semantic_mismatches"] != 0
    ):
        raise RuntimeError("Phase 9.2 parent evidence is invalid")
    if migration.get("status") != "PASS":
        raise RuntimeError("Phase 9.3 migration evidence is invalid")

    before_topology = v1_topology()
    before_health = v1_health()
    now_ns = time.time_ns()
    hold = certify_test_hold(candidate, no_go["bundle_id"], now_ns)
    denied = ProductionClosureAuthorizer().authorize(
        candidate=candidate,
        prerequisite_decision=no_go,
        expected_bundle_id=no_go["bundle_id"],
        primary_evidence=phase92,
        hold_decision=None,
        consumer_registry=None,
        authority_registry=None,
        rollback_evidence=None,
        approval=None,
        now_ns=no_go["issued_at_ns"] + 1,
    )
    if denied.allowed or denied.reason != "PREREQUISITE_DECISION_NOT_GO":
        raise RuntimeError("current no-go did not fail closed")

    parent = test_parent_closure(candidate, no_go["bundle_id"], now_ns)
    expansions = []
    for index, kind in enumerate(ExpansionType, start=1):
        item = ExpansionManifest.plan(
            expansion_id=uid(f"expansion-{kind}"),
            parent=parent,
            expansion_type=kind,
            candidate_digest=digest(f"candidate-{kind}"),
            scope_digest=digest(f"scope-{kind}"),
            partition_plan_epoch=(
                parent.partition_plan_epoch + 1
                if kind == ExpansionType.INSTRUMENT_PARTITION
                else parent.partition_plan_epoch
            ),
            created_at_ns=now_ns + index,
        )
        expansions.append(
            {
                "type": kind,
                "status": item.status,
                "required_gate_count": len(item.required_gates),
                "write_authority": (
                    item.public_write_allowed or item.legacy_write_allowed
                ),
                "transitive_evidence_allowed": (
                    item.transitive_evidence_allowed
                ),
                "digest": item.digest,
            }
        )
    if any(
        item["write_authority"] or item["transitive_evidence_allowed"]
        for item in expansions
    ):
        raise RuntimeError("expansion inherited authority or certification")

    decommission = assess_decommission(
        DecommissionRequest(
            schema="qdl.runtime-decommission-request.v1",
            request_id=uid("blocked-decommission"),
            runtime_id="python-authoritative-runtime",
            owned_slice_ids=(),
            rollback_reference_ids=("phase92-python-rollback",),
            consumer_dependency_ids=(),
            all_replacement_windows_closed=False,
            repository_cleanup_approved=False,
            shared_knowledge_retained=True,
        )
    )
    if decommission.allowed:
        raise RuntimeError("rollback dependency allowed decommission")

    after_topology = v1_topology()
    after_health = v1_health()
    evidence = {
        "schema": "qdl.phase93.hold-close-expand-certification.v1",
        "status": "COMPLETE_CONTROL_PLANE / PRODUCTION_HOLD_NOT_STARTED",
        "issued_at_ns": now_ns,
        "slice_id": candidate.payload["slice_id"],
        "candidate_digest": candidate.digest,
        "prerequisite_decision": no_go["decision"],
        "production_authorized": False,
        "production_hold_started": False,
        "production_rollback_window_closed": False,
        "production_expansions_authorized": 0,
        "python_decommission_authorized": False,
        "production_mutations": 0,
        "parent_phase92": {
            "status": phase92["status"],
            "authentic_events": phase92["parity"]["capture"]["events"],
            "semantic_mismatches": phase92["parity"]["semantic_mismatches"],
            "production_authorized": phase92["production_authorized"],
            "sha256": hashlib.sha256(PHASE92.read_bytes()).hexdigest(),
        },
        "control_plane_fixture": {
            "provenance": "TEST_CONTROL_PLANE_FIXTURE",
            "accelerated_time_is_production_evidence": False,
            "test_hold_status": hold.status,
            "test_hold_production_authorized": hold.production_authorized,
            "current_no_go_rejection": denied.reason,
            "local_phase92_production_eligible": False,
            "expansion_manifest_count": len(expansions),
            "expansions": expansions,
            "decommission_reason": decommission.reason,
        },
        "migration": migration,
        "v1": {
            "health_before": before_health,
            "health_after": after_health,
            "topology_unchanged": before_topology == after_topology,
        },
        "cleanup": {
            "resources_remaining": 0,
            "production_rows_created": 0,
        },
        "technical_debt": [
            "Phase 9.0-C production infrastructure remains NO_GO_EXTERNAL",
            "real production primary and sustained hold observations do not exist",
            "real consumer checkpoints and operator closure approval do not exist",
            "every expansion remains independently uncertified",
        ],
    }
    if (
        hold.production_authorized
        or before_health != 200
        or after_health != 200
        or before_topology != after_topology
    ):
        raise RuntimeError("Phase 9.3 isolation invariant failed")
    freeze(evidence)
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "parent_authentic_events": phase92["parity"]["capture"]["events"],
                "current_no_go_rejection": denied.reason,
                "expansion_manifests": len(expansions),
                "v1_health_before": before_health,
                "v1_health_after": after_health,
                "v1_topology_unchanged": before_topology == after_topology,
                "production_mutations": 0,
                "cleanup": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

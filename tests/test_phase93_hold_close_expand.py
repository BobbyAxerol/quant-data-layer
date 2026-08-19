from __future__ import annotations

import copy
import json
import unittest
import uuid
from dataclasses import replace
from pathlib import Path

from qdl.canary.phase93 import (
    AuthorityRegistrySnapshot,
    ClosureApproval,
    ConsumerCheckpoint,
    ConsumerRegistrySnapshot,
    DecommissionRequest,
    ExpansionManifest,
    ExpansionType,
    HoldScope,
    HoldStatus,
    PrimaryHoldEvaluator,
    PrimaryHoldIdentity,
    PrimaryHoldObservation,
    PrimaryHoldPolicy,
    ProductionClosureAuthorizer,
    RollbackRehearsalEvidence,
    assess_decommission,
)
from qdl.certification.prerequisites import CandidateSlice


ROOT = Path(__file__).resolve().parents[1]


class Phase93Fixtures:
    def setUp(self) -> None:
        self.candidate = CandidateSlice.load(
            ROOT / "config/phase9/candidate-slice.yaml"
        )
        self.no_go = json.loads(
            (
                ROOT
                / "upgrade/evidence/phase90c-production-prerequisites.json"
            ).read_text()
        )
        self.bundle_id = self.no_go["bundle_id"]
        self.start = 1_000_000_000
        self.policy = PrimaryHoldPolicy(
            minimum_duration_seconds=120,
            sample_interval_seconds=60,
            max_sample_gap_seconds=60,
            max_lag_ms=500,
            max_freshness_ms=1_000,
            max_queue_depth=1_000,
            max_spool_bytes=1_000_000,
            max_cpu_percent=80.0,
            max_rss_mb=512.0,
        )
        self.identity = PrimaryHoldIdentity(
            schema="qdl.primary-hold.v1",
            hold_id=str(uuid.uuid4()),
            slice_id=self.candidate.payload["slice_id"],
            candidate_digest=self.candidate.digest,
            prerequisite_bundle_id=self.bundle_id,
            owner_id="rust-primary",
            authority_revision=8,
            lease_epoch=12,
            partition_plan_epoch=1,
            started_at_ns=self.start,
            required_until_ns=self.start + 120_000_000_000,
            policy_digest=self.policy.digest,
        )

    def observation(
        self,
        sequence: int,
        *,
        watermark: int | None = None,
        observed_at_ns: int | None = None,
        **changes: object,
    ) -> PrimaryHoldObservation:
        payload = {
            "schema": "qdl.primary-hold-observation.v1",
            "observation_id": str(uuid.uuid4()),
            "hold_id": self.identity.hold_id,
            "slice_id": self.identity.slice_id,
            "candidate_digest": self.identity.candidate_digest,
            "owner_id": self.identity.owner_id,
            "authority_revision": self.identity.authority_revision,
            "lease_epoch": self.identity.lease_epoch,
            "partition_plan_epoch": self.identity.partition_plan_epoch,
            "sequence": sequence,
            "observed_at_ns": (
                observed_at_ns
                if observed_at_ns is not None
                else self.start + sequence * 60_000_000_000
            ),
            "last_watermark": watermark if watermark is not None else 100 + sequence,
            "lag_ms": 10,
            "freshness_ms": 20,
            "queue_depth": 1,
            "spool_bytes": 100,
            "cpu_percent": 10.0,
            "rss_mb": 64.0,
            "registered_consumers": 2,
            "healthy_consumers": 2,
            "checkpoint_watermark": watermark if watermark is not None else 100 + sequence,
        }
        payload.update(changes)
        return PrimaryHoldObservation(**payload)

    def passing_hold(self, *, scope: HoldScope):
        evaluator = PrimaryHoldEvaluator(
            identity=self.identity, policy=self.policy, scope=scope
        )
        self.assertEqual(evaluator.observe(self.observation(1, watermark=110)), "PASS")
        self.assertEqual(evaluator.observe(self.observation(2, watermark=120)), "PASS")
        return evaluator.decision(
            decision_id=str(uuid.uuid4()),
            now_ns=self.identity.required_until_ns,
        )

    def go(self, now_ns: int) -> dict:
        payload = copy.deepcopy(self.no_go)
        payload.update(
            {
                "decision": "GO",
                "passed": len(payload["gates"]),
                "blocked": 0,
                "issued_at_ns": now_ns - 1_000_000,
                "authority_state": "RUST_SHADOW",
                "v1_unchanged": True,
                "production_mutations": 0,
            }
        )
        for gate in payload["gates"]:
            gate.update({"passed": True, "reason": "PASS"})
        return payload

    def closure_inputs(self, now_ns: int) -> dict:
        hold = self.passing_hold(scope=HoldScope.PRODUCTION)
        authority = AuthorityRegistrySnapshot(
            schema="qdl.authority-registry-snapshot.v1",
            snapshot_id=str(uuid.uuid4()),
            slice_id=hold.slice_id,
            state="RUST_PRIMARY",
            owner_id=hold.owner_id,
            authority_revision=hold.authority_revision,
            lease_epoch=hold.lease_epoch,
            partition_plan_epoch=hold.partition_plan_epoch,
            candidate_digest=hold.candidate_digest,
            prerequisite_bundle_id=hold.prerequisite_bundle_id,
            current_watermark=130,
            public_write_allowed=True,
            legacy_write_allowed=True,
            observed_at_ns=now_ns - 1,
        )
        consumers = ConsumerRegistrySnapshot(
            schema="qdl.consumer-registry-snapshot.v1",
            snapshot_id=str(uuid.uuid4()),
            slice_id=hold.slice_id,
            authority_revision=hold.authority_revision,
            checkpoints=tuple(
                ConsumerCheckpoint(
                    consumer_id=name,
                    requirement_digest=digest * 64,
                    contract_major=2,
                    applied_watermark=130,
                    checkpointed_watermark=130,
                    status="READY",
                    migration_status="COMPLETE",
                    rollback_ready=True,
                )
                for name, digest in (("alpha-a", "a"), ("execution-b", "b"))
            ),
            observed_at_ns=now_ns - 1,
        )
        rollback = RollbackRehearsalEvidence(
            schema="qdl.rollback-rehearsal.v1",
            rehearsal_id=str(uuid.uuid4()),
            slice_id=hold.slice_id,
            candidate_digest=hold.candidate_digest,
            owner_id=hold.owner_id,
            authority_revision=hold.authority_revision,
            lease_epoch=hold.lease_epoch,
            partition_plan_epoch=hold.partition_plan_epoch,
            rollback_manifest_digest=self.candidate.payload[
                "rollback_manifest_digest"
            ],
            reconciled_through_watermark=130,
            rto_ms=500.0,
            status="PASS",
            production_scope=True,
            observed_at_ns=now_ns - 1,
            expires_at_ns=now_ns + 60_000_000_000,
        )
        approval = ClosureApproval(
            schema="qdl.rollback-window-closure-approval.v1",
            closure_id=str(uuid.uuid4()),
            decision="APPROVE",
            slice_id=hold.slice_id,
            candidate_digest=hold.candidate_digest,
            prerequisite_bundle_id=hold.prerequisite_bundle_id,
            hold_id=hold.hold_id,
            hold_policy_digest=hold.policy_digest,
            operator="phase93-test-operator",
            change_ticket="QDL-93",
            allow_close_rollback_window=True,
            repository_cleanup_approved=False,
            approved_at_ns=now_ns - 1,
            expires_at_ns=now_ns + 60_000_000_000,
        )
        primary = {
            "schema": "qdl.phase92.production-primary.v1",
            "status": "PRODUCTION_PRIMARY_ACTIVE",
            "production_authorized": True,
            "slice_id": hold.slice_id,
            "candidate_digest": hold.candidate_digest,
            "prerequisite_bundle_id": hold.prerequisite_bundle_id,
            "authority": {
                "state": "RUST_PRIMARY",
                "owner_id": hold.owner_id,
                "authority_revision": hold.authority_revision,
                "lease_epoch": hold.lease_epoch,
                "partition_plan_epoch": hold.partition_plan_epoch,
                "current_watermark": 130,
            },
        }
        return {
            "candidate": self.candidate,
            "prerequisite_decision": self.go(now_ns),
            "expected_bundle_id": self.bundle_id,
            "primary_evidence": primary,
            "hold_decision": hold,
            "consumer_registry": consumers,
            "authority_registry": authority,
            "rollback_evidence": rollback,
            "approval": approval,
            "now_ns": now_ns,
        }


class PrimaryHoldEvaluatorTest(Phase93Fixtures, unittest.TestCase):
    def test_clean_dense_hold_passes_without_production_authority_in_test_scope(self):
        decision = self.passing_hold(scope=HoldScope.TEST_REHEARSAL)
        self.assertEqual(decision.status, HoldStatus.PASSED)
        self.assertEqual(decision.reason, "PASS")
        self.assertFalse(decision.production_authorized)
        self.assertEqual(decision.observation_count, 2)
        self.assertEqual(decision.terminal_watermark, 120)
        self.assertEqual(len(decision.digest), 64)

    def test_incomplete_sparse_out_of_order_and_watermark_regression_fail(self):
        evaluator = PrimaryHoldEvaluator(
            identity=self.identity,
            policy=self.policy,
            scope=HoldScope.TEST_REHEARSAL,
        )
        empty = evaluator.decision(
            decision_id=str(uuid.uuid4()), now_ns=self.identity.required_until_ns
        )
        self.assertEqual(empty.reason, "HOLD_OBSERVATION_MISSING")

        cases = (
            (
                self.observation(2),
                "HOLD_SEQUENCE_NOT_CONTIGUOUS",
            ),
            (
                self.observation(
                    1,
                    observed_at_ns=self.start + 61_000_000_000,
                ),
                "HOLD_OBSERVATION_GAP_EXCEEDED",
            ),
        )
        for item, expected in cases:
            with self.subTest(expected=expected):
                candidate = PrimaryHoldEvaluator(
                    identity=self.identity,
                    policy=self.policy,
                    scope=HoldScope.TEST_REHEARSAL,
                )
                self.assertEqual(candidate.observe(item), expected)
                self.assertEqual(
                    candidate.decision(
                        decision_id=str(uuid.uuid4()),
                        now_ns=self.identity.required_until_ns,
                    ).status,
                    HoldStatus.BLOCKED,
                )

        regression = PrimaryHoldEvaluator(
            identity=self.identity,
            policy=self.policy,
            scope=HoldScope.TEST_REHEARSAL,
        )
        regression.observe(self.observation(1, watermark=120))
        self.assertEqual(
            regression.observe(self.observation(2, watermark=119)),
            "HOLD_WATERMARK_REGRESSED",
        )

    def test_every_correctness_and_resource_breach_is_sticky(self):
        breaches = {
            "semantic_mismatches": "SEMANTIC_MISMATCH",
            "open_gaps": "OPEN_GAP",
            "duplicate_external_writes": "DUPLICATE_EXTERNAL_WRITE",
            "accepted_stale_writer_writes": "ACCEPTED_STALE_WRITER_WRITE",
            "authority_ambiguities": "AUTHORITY_AMBIGUITY",
            "durable_ack_failures": "DURABLE_ACK_FAILURE",
            "projection_mismatches": "PROJECTION_MISMATCH",
            "consumer_checkpoint_regressions": "CONSUMER_CHECKPOINT_REGRESSION",
            "unexplained_quality_failures": "UNEXPLAINED_QUALITY_FAILURE",
            "lag_ms": "LAG_THRESHOLD_EXCEEDED",
            "freshness_ms": "FRESHNESS_THRESHOLD_EXCEEDED",
            "queue_depth": "QUEUE_THRESHOLD_EXCEEDED",
            "spool_bytes": "SPOOL_THRESHOLD_EXCEEDED",
            "cpu_percent": "CPU_THRESHOLD_EXCEEDED",
            "rss_mb": "RSS_THRESHOLD_EXCEEDED",
        }
        values = {
            "lag_ms": self.policy.max_lag_ms + 1,
            "freshness_ms": self.policy.max_freshness_ms + 1,
            "queue_depth": self.policy.max_queue_depth + 1,
            "spool_bytes": self.policy.max_spool_bytes + 1,
            "cpu_percent": self.policy.max_cpu_percent + 1,
            "rss_mb": self.policy.max_rss_mb + 1,
        }
        for field, expected in breaches.items():
            with self.subTest(field=field):
                evaluator = PrimaryHoldEvaluator(
                    identity=self.identity,
                    policy=self.policy,
                    scope=HoldScope.TEST_REHEARSAL,
                )
                item = self.observation(1, **{field: values.get(field, 1)})
                self.assertEqual(evaluator.observe(item), expected)
                self.assertEqual(
                    evaluator.observe(self.observation(2)),
                    "HOLD_ALREADY_BLOCKED",
                )
                self.assertEqual(
                    evaluator.decision(
                        decision_id=str(uuid.uuid4()),
                        now_ns=self.identity.required_until_ns,
                    ).reason,
                    expected,
                )

    def test_identity_consumer_and_numeric_type_guards(self):
        evaluator = PrimaryHoldEvaluator(
            identity=self.identity,
            policy=self.policy,
            scope=HoldScope.TEST_REHEARSAL,
        )
        changed = self.observation(1, owner_id="other-owner")
        self.assertEqual(
            evaluator.observe(changed), "HOLD_AUTHORITY_IDENTITY_CHANGED"
        )
        for change in (
            {"registered_consumers": 0, "healthy_consumers": 0},
            {"healthy_consumers": 1},
            {"checkpoint_watermark": 99, "last_watermark": 100},
        ):
            with self.subTest(change=change):
                current = PrimaryHoldEvaluator(
                    identity=self.identity,
                    policy=self.policy,
                    scope=HoldScope.TEST_REHEARSAL,
                )
                reason = current.observe(self.observation(1, **change))
                self.assertNotEqual(reason, "PASS")
        with self.assertRaises(ValueError):
            self.observation(1, open_gaps=True)


class ProductionClosureTest(Phase93Fixtures, unittest.TestCase):
    def test_current_no_go_and_local_primary_evidence_cannot_close(self):
        now_ns = self.no_go["issued_at_ns"] + 1
        values = self.closure_inputs(now_ns)
        values["prerequisite_decision"] = self.no_go
        denied = ProductionClosureAuthorizer().authorize(**values)
        self.assertFalse(denied.allowed)
        self.assertEqual(denied.reason, "PREREQUISITE_DECISION_NOT_GO")

        values = self.closure_inputs(now_ns)
        values["primary_evidence"] = json.loads(
            (
                ROOT
                / "upgrade/evidence/phase92-bounded-primary-certification.json"
            ).read_text()
        )
        denied = ProductionClosureAuthorizer().authorize(**values)
        self.assertFalse(denied.allowed)
        self.assertEqual(denied.reason, "PRIMARY_EVIDENCE_SCHEMA_INVALID")

    def test_complete_production_shaped_fixture_authorizes_without_mutation(self):
        now_ns = 2_000_000_000_000
        values = self.closure_inputs(now_ns)
        result = ProductionClosureAuthorizer().authorize(**values)
        self.assertTrue(result.allowed)
        self.assertTrue(result.production_authorized)
        self.assertIsNotNone(result.closure)
        closure = result.closure
        assert closure is not None
        self.assertEqual(closure.owner_id, "rust-primary")
        self.assertEqual(closure.authority_revision, 8)
        self.assertEqual(len(closure.digest), 64)

    def test_registry_rollback_and_approval_mismatches_fail_closed(self):
        now_ns = 2_000_000_000_000
        cases = []

        values = self.closure_inputs(now_ns)
        values["consumer_registry"] = replace(
            values["consumer_registry"],
            observed_at_ns=now_ns - 301_000_000_000,
        )
        cases.append((values, "REGISTRY_SNAPSHOT_STALE"))

        values = self.closure_inputs(now_ns)
        values["primary_evidence"] = copy.deepcopy(values["primary_evidence"])
        values["primary_evidence"]["authority"]["lease_epoch"] = 99
        cases.append((values, "PRIMARY_AUTHORITY_REGISTRY_MISMATCH"))

        values = self.closure_inputs(now_ns)
        checkpoint = values["consumer_registry"].checkpoints[0]
        values["consumer_registry"] = replace(
            values["consumer_registry"],
            checkpoints=(
                replace(
                    checkpoint,
                    applied_watermark=129,
                    checkpointed_watermark=129,
                ),
                values["consumer_registry"].checkpoints[1],
            ),
        )
        cases.append((values, "CONSUMER_CHECKPOINT_BEHIND"))

        values = self.closure_inputs(now_ns)
        values["rollback_evidence"] = replace(
            values["rollback_evidence"], production_scope=False
        )
        cases.append((values, "ROLLBACK_REHEARSAL_NOT_PRODUCTION"))

        values = self.closure_inputs(now_ns)
        values["rollback_evidence"] = replace(
            values["rollback_evidence"], rollback_manifest_digest="f" * 64
        )
        cases.append((values, "ROLLBACK_MANIFEST_MISMATCH"))

        values = self.closure_inputs(now_ns)
        values["approval"] = replace(
            values["approval"], hold_policy_digest="e" * 64
        )
        cases.append((values, "CLOSURE_APPROVAL_IDENTITY_MISMATCH"))

        for values, expected in cases:
            with self.subTest(expected=expected):
                result = ProductionClosureAuthorizer().authorize(**values)
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, expected)

    def test_test_scope_hold_cannot_close_production_window(self):
        now_ns = 2_000_000_000_000
        values = self.closure_inputs(now_ns)
        values["hold_decision"] = self.passing_hold(
            scope=HoldScope.TEST_REHEARSAL
        )
        result = ProductionClosureAuthorizer().authorize(**values)
        self.assertFalse(result.allowed)
        self.assertEqual(result.reason, "PRIMARY_HOLD_NOT_PRODUCTION")


class ExpansionAndDecommissionTest(Phase93Fixtures, unittest.TestCase):
    def parent_closure(self):
        values = self.closure_inputs(2_000_000_000_000)
        result = ProductionClosureAuthorizer().authorize(**values)
        assert result.closure is not None
        return result.closure

    def test_each_expansion_requires_distinct_independent_certification(self):
        parent = self.parent_closure()
        manifests = []
        for index, expansion_type in enumerate(ExpansionType, start=1):
            manifest = ExpansionManifest.plan(
                expansion_id=str(uuid.uuid4()),
                parent=parent,
                expansion_type=expansion_type,
                candidate_digest=f"{index}" * 64,
                scope_digest=format(index + 5, "x") * 64,
                partition_plan_epoch=(
                    parent.partition_plan_epoch + 1
                    if expansion_type == ExpansionType.INSTRUMENT_PARTITION
                    else parent.partition_plan_epoch
                ),
                created_at_ns=2_100_000_000_000 + index,
            )
            manifests.append(manifest)
            self.assertEqual(
                manifest.status, "INDEPENDENT_CERTIFICATION_REQUIRED"
            )
            self.assertFalse(manifest.transitive_evidence_allowed)
            self.assertFalse(manifest.public_write_allowed)
            self.assertFalse(manifest.legacy_write_allowed)
            self.assertIn("rollback", manifest.required_gates)
            self.assertIn("exact_frame_parity", manifest.required_gates)
        self.assertEqual(
            len({item.candidate_digest for item in manifests}),
            len(ExpansionType),
        )

    def test_expansion_cannot_reuse_parent_or_weaken_gates(self):
        parent = self.parent_closure()
        with self.assertRaises(ValueError):
            ExpansionManifest.plan(
                expansion_id=str(uuid.uuid4()),
                parent=parent,
                expansion_type=ExpansionType.BBO,
                candidate_digest=parent.candidate_digest,
                scope_digest="d" * 64,
                partition_plan_epoch=1,
                created_at_ns=2_100_000_000_000,
            )
        with self.assertRaises(ValueError):
            ExpansionManifest.plan(
                expansion_id=str(uuid.uuid4()),
                parent=parent,
                expansion_type=ExpansionType.INSTRUMENT_PARTITION,
                candidate_digest="c" * 64,
                scope_digest="d" * 64,
                partition_plan_epoch=parent.partition_plan_epoch,
                created_at_ns=2_100_000_000_000,
            )
        valid = ExpansionManifest.plan(
            expansion_id=str(uuid.uuid4()),
            parent=parent,
            expansion_type=ExpansionType.BBO,
            candidate_digest="c" * 64,
            scope_digest="d" * 64,
            partition_plan_epoch=1,
            created_at_ns=2_100_000_000_000,
        )
        with self.assertRaises(ValueError):
            replace(valid, required_gates=("rollback",))
        with self.assertRaises(ValueError):
            replace(valid, transitive_evidence_allowed=True)

    def test_decommission_requires_zero_dependency_and_explicit_cleanup(self):
        base = DecommissionRequest(
            schema="qdl.runtime-decommission-request.v1",
            request_id=str(uuid.uuid4()),
            runtime_id="python-binance-usdm-trade",
            owned_slice_ids=(),
            rollback_reference_ids=(),
            consumer_dependency_ids=(),
            all_replacement_windows_closed=True,
            repository_cleanup_approved=True,
            shared_knowledge_retained=True,
        )
        self.assertTrue(assess_decommission(base).allowed)
        cases = (
            (
                replace(base, owned_slice_ids=("slice-a",)),
                "RUNTIME_STILL_OWNS_SLICES",
            ),
            (
                replace(base, rollback_reference_ids=("rollback-a",)),
                "RUNTIME_STILL_REQUIRED_FOR_ROLLBACK",
            ),
            (
                replace(base, consumer_dependency_ids=("consumer-a",)),
                "RUNTIME_HAS_CONSUMER_DEPENDENCIES",
            ),
            (
                replace(base, all_replacement_windows_closed=False),
                "REPLACEMENT_WINDOWS_NOT_CLOSED",
            ),
            (
                replace(base, repository_cleanup_approved=False),
                "REPOSITORY_CLEANUP_NOT_APPROVED",
            ),
            (
                replace(base, shared_knowledge_retained=False),
                "SHARED_KNOWLEDGE_REMOVAL_FORBIDDEN",
            ),
        )
        for request, expected in cases:
            with self.subTest(expected=expected):
                decision = assess_decommission(request)
                self.assertFalse(decision.allowed)
                self.assertEqual(decision.reason, expected)


if __name__ == "__main__":
    unittest.main()

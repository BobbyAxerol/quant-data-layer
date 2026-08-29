from __future__ import annotations

from dataclasses import replace
import unittest
from pathlib import Path

from qdl.consumer import (
    ConsumerManifestLoader,
    ConsumerMigrationRegistry,
    ConsumerRoute,
    MigrationState,
    UsageTelemetry,
)
from qdl.query import ConsumerGrade, FeedType, StalePolicy
from tests.phase7_support import manifest_mapping


ROOT = Path(__file__).resolve().parents[1]


class ConsumerManifestTests(unittest.TestCase):
    def test_reference_and_execution_manifests_are_typed_and_auditable(self):
        alpha = ConsumerManifestLoader.load(
            ROOT / "consumers/shadow/alpha-okx-reference.yaml"
        )
        execution = ConsumerManifestLoader.load(
            ROOT / "consumers/shadow/trading-system-binance-execution.yaml"
        )
        self.assertEqual(alpha.requirements[0].feed, FeedType.BAR)
        self.assertEqual(alpha.requirements[0].consumer_grade, ConsumerGrade.ALPHA)
        self.assertEqual(execution.requirements[0].consumer_grade, ConsumerGrade.EXECUTION)
        self.assertEqual(len(alpha.manifest_sha256), 64)
        self.assertEqual(alpha.rollback_contract, "V1")

    def test_unknown_fields_and_weak_execution_policy_fail_closed(self):
        base = manifest_mapping(
            consumer_id="consumer",
            subject="spiffe://qdl/paper/consumer",
            instrument_uid="uid",
            feed="TRADE",
            interval=None,
            grade="EXECUTION",
            source_policy_id="execution",
            purposes=("INTERNAL_EXECUTION",),
        )
        base["spec"]["requirements"][0]["gap_policy"] = "OBSERVE"
        with self.assertRaisesRegex(ValueError, "gap policy must BLOCK"):
            ConsumerManifestLoader.from_mapping(base)
        base["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "unknown top-level"):
            ConsumerManifestLoader.from_mapping(base)

    def test_manifest_binds_quiet_trade_policy_and_session_sla(self):
        manifest = ConsumerManifestLoader.load(
            ROOT / "consumers/stable/trading-system-paper.yaml"
        )
        requirement = next(
            item for item in manifest.requirements if item.feed is FeedType.TRADE
        )
        self.assertIs(requirement.event_recency_policy, StalePolicy.OBSERVE)
        self.assertEqual(requirement.max_session_liveness_ms, 45_000)
        self.assertTrue(manifest.requirement_allowed(requirement))
        self.assertFalse(manifest.requirement_allowed(replace(
            requirement,
            event_recency_policy=None,
        )))
        self.assertFalse(manifest.requirement_allowed(replace(
            requirement,
            max_session_liveness_ms=45_001,
        )))


class ConsumerMigrationTests(unittest.TestCase):
    def setUp(self):
        self.manifest = ConsumerManifestLoader.load(
            ROOT / "consumers/shadow/alpha-okx-reference.yaml"
        )
        self.registry = ConsumerMigrationRegistry(clock_ns=lambda: 100)

    def test_governed_shadow_activation_and_rollback(self):
        registered = self.registry.register(self.manifest, reason="Phase 5 registration")
        self.assertEqual(registered.state, MigrationState.REGISTERED)
        shadow = self.registry.transition(
            self.manifest.consumer_id,
            MigrationState.SHADOW,
            owner=self.manifest.owner,
            reason="Parity observation",
        )
        self.assertEqual(shadow.state, MigrationState.SHADOW)
        self.assertEqual(
            self.registry.route(self.manifest.consumer_id),
            ConsumerRoute.V1_WITH_V2_SHADOW,
        )
        with self.assertRaisesRegex(ValueError, "invalid migration transition"):
            self.registry.transition(
                self.manifest.consumer_id,
                MigrationState.ACTIVE,
                owner=self.manifest.owner,
                reason="Skip acceptance",
            )
        rolled_back = self.registry.transition(
            self.manifest.consumer_id,
            MigrationState.ROLLED_BACK,
            owner=self.manifest.owner,
            reason="Exercise V1 rollback",
        )
        self.assertEqual(rolled_back.state, MigrationState.ROLLED_BACK)
        self.assertEqual(self.registry.route(self.manifest.consumer_id), ConsumerRoute.V1)

    def test_owner_and_manifest_mutation_are_rejected(self):
        self.registry.register(self.manifest, reason="register")
        with self.assertRaises(PermissionError):
            self.registry.transition(
                self.manifest.consumer_id,
                MigrationState.SHADOW,
                owner="other-owner",
                reason="unauthorized",
            )

    def test_usage_telemetry_is_aggregate_and_marks_v1_deprecated(self):
        telemetry = UsageTelemetry(max_consumers=2)
        telemetry.record(
            consumer_id="alpha-a", sdk_major=1, contract="/v1/preload", cursor_offset=0
        )
        telemetry.record(
            consumer_id="alpha-a", sdk_major=1, contract="/v1/preload", cursor_offset=0
        )
        telemetry.record(
            consumer_id="alpha-b", sdk_major=2, contract="/v2/warmup", cursor_offset=7
        )
        snapshot = telemetry.snapshot()
        self.assertEqual(snapshot[0]["requests"], 2)
        self.assertTrue(snapshot[0]["deprecated"])
        self.assertFalse(snapshot[1]["deprecated"])
        self.assertNotIn("payload", snapshot[0])
        notices = telemetry.deprecation_notices({"alpha-a": "alpha-owner"})
        self.assertEqual(notices[0]["owner"], "alpha-owner")
        self.assertIn("V2_MANIFEST", notices[0]["action"])
        with self.assertRaisesRegex(ValueError, "notification owner"):
            telemetry.deprecation_notices({})
        with self.assertRaisesRegex(RuntimeError, "capacity"):
            telemetry.record(
                consumer_id="alpha-c", sdk_major=2, contract="/v2/warmup", cursor_offset=0
            )


if __name__ == "__main__":
    unittest.main()

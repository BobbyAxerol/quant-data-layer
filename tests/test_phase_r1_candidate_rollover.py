from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from qdl.control.authority_rollover import CandidateRolloverPacket, prepare_rollover_packet
from qdl.control.cutover_packet import AuthorityCutoverPacket
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_production_core_bundle,
)
from scripts.phasec40_authority_bootstrap import prepare_packet
from scripts.phase_r1_prepare_transition import prepare_transition_packet
from scripts.phase_r2_prepare_primary import prepare_primary_packets


ROOT = Path(__file__).resolve().parents[1]
NOW = 1_800_000_000_000_000_000


class CandidateRolloverPacketTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="qdl-r1-rollover-")
        self.root = Path(self.temp.name)
        for name, body in (
            ("sbom.json", '{"bomFormat":"CycloneDX"}\n'),
            ("rollback.json", '{"route":"V1"}\n'),
            ("acceptance.json", json.dumps({
                "status": "PASS",
                "provider_provenance": "REAL",
                "semantic_mismatches": 0,
                "open_gaps": 0,
                "duplicate_external_effects": 0,
                "consumer_errors": 0,
                "execution_state_changed": False,
            }) + "\n"),
        ):
            (self.root / name).write_text(body, encoding="utf-8")
        self.catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        self.acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
        self.scope_path = ROOT / "config/v2/stable-authority-promotion-scope.yaml"
        self.contract_path = ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto"
        catalog = StableSourceCatalog.load(self.catalog_path)
        acquisition = StableAcquisitionPlan.load(self.acquisition_path, catalog=catalog)
        scope = AuthorityPromotionScope.load(self.scope_path, catalog=catalog)
        authority = stable_authority_record(
            rust_image_digest="sha256:" + "a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=self.contract_path,
            partition_plan=self.acquisition_path.read_bytes(),
            effective_at_ns=NOW,
        )
        write_production_core_bundle(
            self.root,
            catalog=catalog,
            acquisition=acquisition,
            promotion_scope=scope,
            raw_authority=authority,
        )
        self.bootstrap = prepare_packet(
            catalog_path=self.catalog_path,
            acquisition_path=self.acquisition_path,
            promotion_scope_path=self.scope_path,
            production_core_manifest_path=self.root / "production-core-manifest.json",
            contract_path=self.contract_path,
            sbom_path=self.root / "sbom.json",
            rollback_manifest_path=self.root / "rollback.json",
            acceptance_path=self.root / "acceptance.json",
            rust_image_digest="sha256:" + "a" * 64,
            actor="operator@example",
            issued_at_ns=NOW,
        )
        self.current_rows = []
        for item in self.bootstrap["slices"]:
            self.current_rows.append({
                **{field: item[field] for field in (
                    "slice_id", "environment", "venue", "market", "product_type",
                    "feed", "partition_plan_epoch", "partition_id", "schema_major",
                    "signature_identity", "contract_digest", "normalizer_version",
                    "adapter_version", "config_revision", "instrument_catalog_revision",
                    "source_policy_revision", "partition_plan_digest",
                    "rollback_manifest_digest",
                )},
                "state": "BLOCKED",
                "authority_revision": 4,
                "owner_id": "qdl-v2-rust-canary",
                "lease_epoch": 2,
                "candidate_digest": "c" * 64,
                "artifact_image_digest": "sha256:" + "d" * 64,
            })

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_prepares_exact_blocked_only_packet(self) -> None:
        packet = prepare_rollover_packet(
            self.bootstrap,
            self.current_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            issued_at_ns=NOW,
        )
        parsed = CandidateRolloverPacket.parse(packet, now_ns=NOW)
        self.assertEqual(len(parsed.rollovers), 12)
        self.assertEqual(parsed.plan()["kafka_offset_mutations"], 0)
        self.assertTrue(all(item.expected_revision == 4 for item in parsed.rollovers))
        self.assertTrue(all(item.new_lease_epoch == 3 for item in parsed.rollovers))
        self.assertTrue(all(
            item.new_provenance["candidate_digest"] == parsed.candidate_digest
            for item in parsed.rollovers
        ))

    def test_tampered_provenance_and_semantic_drift_fail_closed(self) -> None:
        packet = prepare_rollover_packet(
            self.bootstrap,
            self.current_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            issued_at_ns=NOW,
        )
        tampered = copy.deepcopy(packet)
        tampered["rollovers"][0]["new_provenance"]["candidate_digest"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "candidate differs"):
            CandidateRolloverPacket.parse(tampered, now_ns=NOW)
        drifted = copy.deepcopy(self.current_rows)
        drifted[0]["contract_digest"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "source semantics"):
            prepare_rollover_packet(
                self.bootstrap,
                drifted,
                actor="operator@example",
                change_ticket="QDL-R1-001",
                issued_at_ns=NOW,
            )

    def test_transition_packets_terminalize_revalidate_and_canary_exact_scope(self) -> None:
        rollover_raw = prepare_rollover_packet(
            self.bootstrap,
            self.current_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            issued_at_ns=NOW,
        )
        rollover = CandidateRolloverPacket.parse(rollover_raw, now_ns=NOW)
        terminal_rows = [
            {**row, "state": "RUST_CANARY", "authority_revision": 3, "lease_epoch": 2}
            for row in self.current_rows
        ]
        block = prepare_transition_packet(
            stage="BLOCK_CANARY",
            current_rows=terminal_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            issued_at_ns=NOW,
        )
        self.assertEqual(
            AuthorityCutoverPacket.parse(block, now_ns=NOW).stage,
            "BLOCK_CANARY",
        )
        blocked_rows = []
        by_rollover = {item.slice_id: item for item in rollover.rollovers}
        for row in self.current_rows:
            item = by_rollover[row["slice_id"]]
            blocked_rows.append({
                **row,
                "state": "BLOCKED",
                "authority_revision": item.expected_revision + 1,
                "lease_epoch": item.new_lease_epoch,
                "candidate_digest": item.new_provenance["candidate_digest"],
                "artifact_image_digest": item.new_provenance["artifact_image_digest"],
                "contract_digest": item.new_provenance["contract_digest"],
                "partition_plan_digest": item.new_provenance["partition_plan_digest"],
            })
        revalidate = prepare_transition_packet(
            stage="REVALIDATE",
            current_rows=blocked_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            rollover=rollover,
            issued_at_ns=NOW,
        )
        self.assertEqual(
            AuthorityCutoverPacket.parse(revalidate, now_ns=NOW).stage,
            "REVALIDATE",
        )
        validating_rows = [
            {**row, "state": "VALIDATING", "authority_revision": row["authority_revision"] + 1}
            for row in blocked_rows
        ]
        canary = prepare_transition_packet(
            stage="CANARY",
            current_rows=validating_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            rollover=rollover,
            issued_at_ns=NOW,
        )
        parsed = AuthorityCutoverPacket.parse(canary, now_ns=NOW)
        self.assertEqual(parsed.stage, "CANARY")
        self.assertEqual(len(parsed.slices), 12)
        self.assertTrue(all(item.terminal_watermark == 0 for item in parsed.slices))

    def test_r2_primary_packet_binds_live_checkpoint_to_exact_binding(self) -> None:
        rollover_raw = prepare_rollover_packet(
            self.bootstrap,
            self.current_rows,
            actor="operator@example",
            change_ticket="QDL-R1-001",
            issued_at_ns=NOW,
        )
        rollover = CandidateRolloverPacket.parse(rollover_raw, now_ns=NOW)
        by_rollover = {item.slice_id: item for item in rollover.rollovers}
        canary_rows = []
        live_slices = []
        for index, row in enumerate(self.current_rows, start=1):
            item = by_rollover[row["slice_id"]]
            revision = item.expected_revision + 3
            canary_rows.append({
                **row,
                "state": "RUST_CANARY",
                "authority_revision": revision,
                "lease_epoch": item.new_lease_epoch,
                "candidate_digest": item.new_provenance["candidate_digest"],
                "artifact_image_digest": item.new_provenance["artifact_image_digest"],
                "contract_digest": item.new_provenance["contract_digest"],
                "partition_plan_digest": item.new_provenance["partition_plan_digest"],
            })
            watermark = 1_000 + index
            live_slices.append({
                "binding_id": item.expected_partition_id,
                "slice_id": item.slice_id,
                "checkpoint": {
                    "owner_id": row["owner_id"],
                    "authority_revision": revision,
                    "lease_epoch": item.new_lease_epoch,
                    "shard_id": item.expected_partition_id,
                    "source_watermark": watermark,
                    "source_event_id": f"event-{index}",
                    "output_payload_sha256": "4" * 64,
                    "committed_at_ns": NOW,
                },
                "source_session_id": f"session-{index}",
                "connection_generation": 1,
                "overlap_start_watermark": watermark - 31,
                "overlap_end_watermark": watermark,
                "old_event_count": 32,
                "new_event_count": 32,
                "semantic_mismatches": 0,
                "open_gaps": 0,
            })
        live = {
            "schema": "qdl.c40.live-handoff-evidence.v1",
            "provider_provenance": "REAL",
            "production_mutations": 0,
            "scope_digest": rollover.candidate["promotion_scope_digest"],
            "slices": live_slices,
        }
        terminal, primary = prepare_primary_packets(
            rollover=rollover,
            live=live,
            current_rows=canary_rows,
            actor="operator@example",
            change_ticket="QDL-R2-001",
            issued_at_ns=NOW,
        )
        self.assertEqual(len(terminal["checkpoints"]), 12)
        self.assertEqual(len(terminal["handoffs"]), 12)
        self.assertTrue(all(
            row["first_new_watermark"] == row["terminal_watermark"] + 1
            for row in terminal["handoffs"]
        ))
        self.assertEqual(AuthorityCutoverPacket.parse(primary, now_ns=NOW).stage, "PRIMARY")
        bad = copy.deepcopy(live)
        bad["slices"][0]["checkpoint"]["shard_id"] = "wrong-binding"
        with self.assertRaisesRegex(ValueError, "authority/parity differs"):
            prepare_primary_packets(
                rollover=rollover,
                live=bad,
                current_rows=canary_rows,
                actor="operator@example",
                change_ticket="QDL-R2-001",
                issued_at_ns=NOW,
            )

    def test_migration_is_additive_and_direct_provenance_rewrite_is_guarded(self) -> None:
        sql = (ROOT / "migrations/postgres/0011_authority_candidate_rollover.sql").read_text()
        for name in (
            "qdl_authority_candidate_rollovers",
            "qdl_guard_authority_candidate_provenance",
            "qdl_rollover_authority_candidate",
            "qdl_authority_candidate_provenance_guard",
        ):
            self.assertIn(name, sql)
        self.assertIn("state <> 'BLOCKED'", sql)
        self.assertIn("compare-and-swap precondition failed", sql)
        self.assertIn("append-only", sql)
        self.assertNotIn("DROP TABLE", sql.upper())
        self.assertNotIn("TRUNCATE", sql.upper())

    def test_operator_entrypoint_supports_help(self) -> None:
        for name in (
            "phase_r1_candidate_rollover.py",
            "phase_r1_prepare_transition.py",
            "phase_r2_prepare_primary.py",
        ):
            completed = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / name), "--help"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("usage:", completed.stdout)


if __name__ == "__main__":
    unittest.main()

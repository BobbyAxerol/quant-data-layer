from __future__ import annotations

import copy
import json
from pathlib import Path
import tempfile
import unittest

from qdl.control.authority_outbox import build_authority_control_event
from qdl.control.cutover_packet import AuthorityCutoverPacket
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_production_core_bundle,
)
from scripts.phasec40_authority_bootstrap import prepare_packet
from scripts.phasec40_prepare_cutover import (
    CANARY_OWNER,
    prepare_primary,
    prepare_stage,
    validate_terminal,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = 1_800_000_000_000_000_000


class C40CutoverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="qdl-c40-cutover-")
        root = Path(self.temp.name)
        catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
        scope_path = ROOT / "config/v2/stable-authority-promotion-scope.yaml"
        contract_path = ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto"
        for name, body in (
            ("sbom.json", "{}\n"),
            ("rollback.json", '{"route":"V1"}\n'),
            (
                "acceptance.json",
                json.dumps({
                    "status": "PASS",
                    "provider_provenance": "REAL",
                    "semantic_mismatches": 0,
                    "open_gaps": 0,
                    "duplicate_external_effects": 0,
                    "consumer_errors": 0,
                    "execution_state_changed": False,
                }),
            ),
        ):
            (root / name).write_text(body, encoding="utf-8")
        catalog = StableSourceCatalog.load(catalog_path)
        acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
        scope = AuthorityPromotionScope.load(scope_path, catalog=catalog)
        authority = stable_authority_record(
            rust_image_digest="sha256:" + "a" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=contract_path,
            partition_plan=acquisition_path.read_bytes(),
            effective_at_ns=NOW,
        )
        write_production_core_bundle(
            root,
            catalog=catalog,
            acquisition=acquisition,
            promotion_scope=scope,
            raw_authority=authority,
        )
        self.bootstrap = prepare_packet(
            catalog_path=catalog_path,
            acquisition_path=acquisition_path,
            promotion_scope_path=scope_path,
            production_core_manifest_path=root / "production-core-manifest.json",
            contract_path=contract_path,
            sbom_path=root / "sbom.json",
            rollback_manifest_path=root / "rollback.json",
            acceptance_path=root / "acceptance.json",
            rust_image_digest="sha256:" + "a" * 64,
            actor="operator@example",
            issued_at_ns=NOW,
        )
        self.live = {
            "schema": "qdl.c40.live-handoff-evidence.v1",
            "captured_at_ns": NOW,
            "provider_provenance": "REAL",
            "production_mutations": 0,
            "scope_revision": 2,
            "scope_digest": self.bootstrap["candidate"]["promotion_scope_digest"],
            "topics": {},
            "scans": {},
            "slices": [],
        }
        for index, item in enumerate(self.bootstrap["slices"], start=1):
            watermark = 1000 + index
            self.live["slices"].append({
                "binding_id": item["binding_id"],
                "slice_id": item["slice_id"],
                "checkpoint": {
                    "schema": "qdl.target-watermark-checkpoint.v1",
                    "slice_id": item["slice_id"],
                    "owner_id": CANARY_OWNER,
                    "authority_revision": 3,
                    "lease_epoch": 2,
                    "partition_plan_epoch": 1,
                    "shard_id": item["binding_id"],
                    "target": "CANARY_CANONICAL",
                    "source_watermark": watermark,
                    "source_event_id": f"{index:032x}",
                    "decision": "CANONICAL",
                    "output_payload_sha256": f"{index:064x}",
                    "candidate_digest": self.bootstrap["candidate_digest"],
                    "committed_at_ns": NOW + index * 1000,
                },
                "source_session_id": f"session-{index}",
                "connection_generation": 1,
                "terminal_raw_payload_sha256": "f" * 64,
                "overlap_start_watermark": watermark - 31,
                "overlap_end_watermark": watermark,
                "old_event_count": 32,
                "new_event_count": 32,
                "semantic_mismatches": 0,
                "open_gaps": 0,
                "parity_set_sha256": "e" * 64,
            })

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_shadow_validate_and_canary_packets_are_exact_and_parse(self):
        shadow = prepare_stage(
            self.bootstrap, stage="SHADOW_VALIDATE",
            actor="operator@example", issued_at_ns=NOW,
        )
        canary = prepare_stage(
            self.bootstrap, stage="CANARY",
            actor="operator@example", issued_at_ns=NOW,
        )
        self.assertEqual(len(shadow["slices"]), 12)
        self.assertEqual({item["new_state"] for item in shadow["slices"]}, {"VALIDATING"})
        self.assertEqual({item["new_state"] for item in canary["slices"]}, {"RUST_CANARY"})
        self.assertEqual({item["new_owner_id"] for item in canary["slices"]}, {CANARY_OWNER})
        AuthorityCutoverPacket.parse(shadow, now_ns=NOW)
        AuthorityCutoverPacket.parse(canary, now_ns=NOW)

    def test_primary_artifacts_bind_outbox_serializer_and_w_plus_one(self):
        terminal, primary = prepare_primary(
            self.bootstrap, self.live,
            actor="operator@example", issued_at_ns=NOW,
        )
        validate_terminal(terminal, now_ns=NOW)
        self.assertEqual(len(primary["slices"]), 12)
        self.assertTrue(all(
            item["terminal_watermark"] + 1
            == next(
                handoff["first_new_watermark"]
                for handoff in terminal["handoffs"]
                if handoff["handoff_id"] == item["handoff_id"]
            )
            for item in primary["slices"]
        ))
        parsed = AuthorityCutoverPacket.parse(primary, now_ns=NOW)
        first_slice = primary["slices"][0]
        checkpoint = next(
            value for value in terminal["checkpoints"]
            if value["slice_id"] == first_slice["slice_id"]
        )
        handoff = next(
            value for value in terminal["handoffs"]
            if value["handoff_id"] == first_slice["handoff_id"]
        )
        transition = {
            "transition_id": first_slice["transition_id"],
            "slice_id": first_slice["slice_id"],
            "new_revision": first_slice["expected_revision"] + 1,
        }
        authority = {
            **next(
                value for value in self.bootstrap["slices"]
                if value["slice_id"] == first_slice["slice_id"]
            ),
            "state": "RUST_PRIMARY",
            "authority_revision": 4,
            "owner_id": "qdl-v2-rust-primary",
            "lease_epoch": 3,
            "terminal_watermark": first_slice["terminal_watermark"],
            "prerequisite_bundle_id": first_slice["prerequisite_bundle_id"],
            "approved_by": "operator@example",
            "approved_at": handoff["approved_at"],
            "hold_until": handoff["expires_at"],
        }
        outbox_payload = {
            "schema": "qdl.authority-outbox-event.v1",
            "event_id": first_slice["transition_id"],
            "transition": transition,
            "authority": authority,
            "checkpoint": checkpoint,
            "handoff": handoff,
        }
        event = build_authority_control_event(outbox_payload)
        self.assertEqual(event["authority"]["start_watermark"], first_slice["terminal_watermark"])
        self.assertEqual(event["authority"]["authority_revision"], parsed.slices[0].expected_revision + 1)

    def test_dirty_or_incomplete_live_evidence_fails_closed(self):
        dirty = copy.deepcopy(self.live)
        dirty["slices"][0]["semantic_mismatches"] = 1
        with self.assertRaisesRegex(ValueError, "live slice evidence"):
            prepare_primary(
                self.bootstrap, dirty,
                actor="operator@example", issued_at_ns=NOW,
            )
        missing = copy.deepcopy(self.live)
        missing["slices"].pop()
        with self.assertRaisesRegex(ValueError, "differs from bootstrap"):
            prepare_primary(
                self.bootstrap, missing,
                actor="operator@example", issued_at_ns=NOW,
            )


if __name__ == "__main__":
    unittest.main()

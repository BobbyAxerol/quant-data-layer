from __future__ import annotations

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
    stable_authority_record,
    write_production_core_bundle,
)
from scripts.phasec40_authority_bootstrap import prepare_packet, validate_packet


ROOT = Path(__file__).resolve().parents[1]
NOW = 1_800_000_000_000_000_000


class C40AuthorityBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog_path = ROOT / "config/v2/stable-source-bindings.yaml"
        self.acquisition_path = ROOT / "config/v2/stable-acquisition-bindings.yaml"
        self.scope_path = ROOT / "config/v2/stable-authority-promotion-scope.yaml"
        self.contract_path = ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto"
        self.temp = tempfile.TemporaryDirectory(prefix="qdl-c40-bootstrap-")
        self.root = Path(self.temp.name)
        for name, body in (
            ("sbom.json", '{"bomFormat":"CycloneDX"}\n'),
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
                }) + "\n",
            ),
        ):
            (self.root / name).write_text(body, encoding="utf-8")
        catalog = StableSourceCatalog.load(self.catalog_path)
        acquisition = StableAcquisitionPlan.load(
            self.acquisition_path, catalog=catalog
        )
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

    def tearDown(self) -> None:
        self.temp.cleanup()

    def packet(self) -> dict:
        return prepare_packet(
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

    def test_exact_crypto_scope_is_deterministic_and_dnse_spot_free(self):
        first = self.packet()
        second = self.packet()
        self.assertEqual(first, second)
        self.assertEqual(len(first["slices"]), 12)
        self.assertEqual(
            {(item["venue"], item["market"]) for item in first["slices"]},
            {("BINANCE", "USDM"), ("OKX", "SWAP")},
        )
        self.assertEqual(
            {item["feed"] for item in first["slices"]},
            {"TRADE", "QUOTE", "BAR"},
        )
        self.assertTrue(all(item["state"] == "RUST_SHADOW" for item in first["slices"]))
        validate_packet(first, now_ns=NOW)

    def test_real_c39_acceptance_schema_is_strictly_bound(self):
        acceptance = self.root / "acceptance.json"
        acceptance.write_text(json.dumps({
            "schema": "qdl.c39.final-acceptance.v1",
            "rust_image_unchanged": "sha256:" + "a" * 64,
            "market_data_health": {
                "top": "READY",
                "service": "READY",
                "details": {
                    "demanded_v2_slices": 8,
                    "ready_v2_slices": 8,
                    "unhealthy_v2_slices": 0,
                    "reported_unhealthy_slices": [],
                    "unreported_unhealthy_slices": 0,
                },
            },
            "alpha_smoke": {
                "coverage": "FULL",
                "acknowledged": True,
                "execution_enabled": False,
                "container_removed": True,
            },
            "provider_cache": {
                "providers": ["BINANCE_DIRECT", "OKX_DIRECT"],
                "present": 8,
                "demanded_slices": 8,
                "bars_closed": True,
                "trades_subsecond_at_sample": True,
            },
            "cache": {"sqlite_quarantine": 0},
            "post_soak": {"disk_growth_bytes": 0, "new_market_data_log_lines": 0},
            "execution_baseline_unchanged": {
                "order.inbound": 0,
                "commands.execution.paper": 0,
            },
        }), encoding="utf-8")
        self.assertEqual(len(self.packet()["slices"]), 12)
        dirty = json.loads(acceptance.read_text(encoding="utf-8"))
        dirty["market_data_health"]["details"]["ready_v2_slices"] = 7
        acceptance.write_text(json.dumps(dirty), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "C39 final acceptance"):
            self.packet()

    def test_r1_precanary_admission_is_strict_and_cannot_claim_live_candidate_parity(self):
        acceptance = self.root / "acceptance.json"
        target = "sha256:" + "a" * 64
        admission = {
            "schema": "qdl.r1.pre-canary-admission.v1",
            "status": "PASS",
            "issued_at_ns": NOW - 1,
            "expires_at_ns": NOW + 1_000_000_000,
            "provider_provenance": "REAL",
            "production_mutations": 0,
            "execution_state_changed": False,
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "duplicate_external_effects": 0,
            "consumer_errors": 0,
            "candidate_runtime_parity_status": "PENDING_R1_CANARY",
            "candidate_source_commit": "abcdef123456",
            "candidate_image_digest": target,
            "candidate_image_inspect_sha256": "1" * 64,
            "rollback_rust_image_digest": "sha256:" + "b" * 64,
            "promotion_scope_digest": "2" * 64,
            "contract_sha256": "3" * 64,
            "partition_plan_sha256": "4" * 64,
            "release_artifact_sha256": "5" * 64,
            "sbom_sha256": "6" * 64,
            "rollback_manifest_sha256": "7" * 64,
            "reference_runtime_image_digest": "sha256:" + "c" * 64,
            "reference_source_commit": "c7f3c34f",
            "reference_parity_sha256": "8" * 64,
            "reference_captured_at_ns": NOW - 2,
            "sample_count": 96,
        }
        acceptance.write_text(json.dumps(admission), encoding="utf-8")
        self.assertEqual(len(self.packet()["slices"]), 12)
        admission["reference_runtime_image_digest"] = target
        acceptance.write_text(json.dumps(admission), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "candidate-bound"):
            self.packet()

    def test_stale_manifest_and_dirty_acceptance_fail_closed(self):
        manifest_path = self.root / "production-core-manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["promotion_scope_revision"] = 1
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "manifest differs"):
            self.packet()

        self.setUp_manifest_again()
        acceptance = self.root / "acceptance.json"
        payload = json.loads(acceptance.read_text(encoding="utf-8"))
        payload["semantic_mismatches"] = 1
        acceptance.write_text(json.dumps(payload), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "clean real-provider"):
            self.packet()

    def setUp_manifest_again(self) -> None:
        catalog = StableSourceCatalog.load(self.catalog_path)
        acquisition = StableAcquisitionPlan.load(
            self.acquisition_path, catalog=catalog
        )
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

    def test_operator_entrypoints_support_direct_help(self):
        for script in (
            "phasec40_authority_bootstrap.py",
            "phasec40_prepare_cutover.py",
            "phasec40_collect_live_handoff.py",
        ):
            completed = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / script), "--help"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("usage:", completed.stdout)

    def test_packet_tamper_and_expiry_fail_closed(self):
        packet = self.packet()
        tampered = copy.deepcopy(packet)
        tampered["slices"][0]["market"] = "SPOT"
        with self.assertRaisesRegex(ValueError, "non-crypto"):
            validate_packet(tampered, now_ns=NOW)
        with self.assertRaisesRegex(ValueError, "approval window"):
            validate_packet(packet, now_ns=packet["expires_at_ns"])


if __name__ == "__main__":
    unittest.main()

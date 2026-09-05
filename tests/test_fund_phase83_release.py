from __future__ import annotations

import json
import pathlib
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
import unittest

import yaml

from qdl.certification.release import build_spdx, verify_release_bundle


ROOT = pathlib.Path(__file__).resolve().parents[1]


class Phase83CandidateContractTests(unittest.TestCase):
    def test_frozen_candidate_evidence_passes_and_signatures_verify(self):
        authority = json.loads(
            (ROOT / "upgrade/evidence/phase8-authority-rehearsal.json").read_text()
        )
        capacity = json.loads(
            (ROOT / "upgrade/evidence/phase8-release-capacity.json").read_text()
        )
        release_dir = ROOT / "upgrade/evidence/phase8-release"
        self.assertEqual(authority["status"], "PASS")
        self.assertEqual(authority["final_authority"], "RUST_SHADOW")
        self.assertEqual(authority["authority_audit_revisions_after_restart"], [1, 2, 3])
        self.assertTrue(authority["authority_state_persisted_after_full_broker_restart"])
        self.assertEqual(authority["public_writes"], 0)
        self.assertEqual(authority["legacy_writes"], 0)
        self.assertEqual(capacity["status"], "PASS")
        self.assertTrue(capacity["thresholds_pass"])
        self.assertEqual(capacity["replay"]["record_mismatches"], 0)
        self.assertFalse((release_dir / "private.pem").exists())
        manifest = verify_release_bundle(
            ROOT,
            release_dir,
            verification_key=release_dir / "attestation-public.pem",
            verify_repository_artifacts=False,
        )
        self.assertEqual(manifest["git_sha"], "053ec76")
        self.assertEqual(manifest["authority"], "SHADOW")

    def test_release_capacity_uses_authentic_multi_venue_inputs(self):
        source = (ROOT / "scripts/phase83_release_capacity.py").read_text()
        self.assertIn("phase82._collect_live", source)
        self.assertIn("phase82._collect_dnse", source)
        self.assertIn('"production_writes": 0', source)
        self.assertIn('"fixture_only_venues": ["DERIBIT"]', source)

    def test_freeze_can_use_offline_inspect_evidence_without_docker_socket(self):
        source = (ROOT / "scripts/phase83_freeze_candidate.py").read_text()
        self.assertIn('"--image-inspect-json"', source)
        self.assertIn('"--runtime-inspect-json"', source)
        self.assertIn('"--v1-health-status"', source)

    def test_candidate_partition_plan_is_shadow_only_and_single_owner(self):
        plan = json.loads(
            (ROOT / "config/phase8/candidate-partition-plan.json").read_text()
        )
        self.assertEqual(plan["schema"], "qdl.partition-plan.v1")
        self.assertEqual(plan["authority"], "RUST_SHADOW")
        self.assertFalse(plan["public_write_allowed"])
        self.assertFalse(plan["legacy_write_allowed"])
        self.assertEqual(len(plan["assignments"]), 1)
        self.assertEqual(plan["assignments"][0]["native_symbol"], "BTCUSDT")

    def test_authority_topic_is_compacted_and_candidate_image_is_non_root(self):
        topology = yaml.safe_load(
            (ROOT / "config/phase8/broker-topology.yaml").read_text()
        )
        authority = next(
            item for item in topology["topics"]
            if item["name"] == "qdl.phase8.control.authority.v1"
        )
        self.assertEqual(authority["cleanup_policy"], "compact")
        self.assertEqual(authority["partition_key"], "authority_slice")
        audit = next(
            item for item in topology["topics"]
            if item["name"] == "qdl.phase8.audit.v1"
        )
        self.assertNotEqual(audit.get("cleanup_policy"), "compact")
        rehearsal = (
            ROOT / "rust/qdl-kafka/src/bin/qdl-authority-rehearsal.rs"
        ).read_text()
        self.assertIn('required("QDL_AUDIT_TOPIC")', rehearsal)
        certification = (
            ROOT / "scripts/phase83_authority_certification.py"
        ).read_text()
        self.assertNotIn('f"phase83-', certification)
        self.assertIn('f"phase8-phase83-audit-restart-', certification)
        dockerfile = (ROOT / "Dockerfile.phase8-rust").read_text()
        runtime_from = next(
            line for line in dockerfile.splitlines()
            if line.startswith("FROM debian:bookworm-slim")
        )
        self.assertRegex(runtime_from, r"@sha256:[0-9a-f]{64}$")
        self.assertIn("USER 10001:10001", dockerfile)
        for binary in (
            "qdl-kafka-smoke", "qdl-authority-rehearsal",
            "qdl-binance-shadow", "qdl-parity-replay", "qdl-venue-core-certify",
        ):
            self.assertIn(f"/usr/local/bin/{binary}", dockerfile)
        self.assertIn('io.qdl.authority.default="RUST_SHADOW"', dockerfile)
        self.assertIn("COPY deny.toml ./deny.toml", dockerfile)
        self.assertIn(
            "COPY contracts/golden ./contracts/golden",
            dockerfile,
        )
        self.assertIn(
            "COPY tests/fixtures/phase2 ./tests/fixtures/phase2",
            dockerfile,
        )

    def test_rust_supply_chain_policy_is_explicit_and_fail_closed(self):
        policy = tomllib.loads((ROOT / "deny.toml").read_text())
        self.assertEqual(policy["advisories"]["ignore"], [])
        self.assertEqual(policy["bans"]["wildcards"], "deny")
        self.assertEqual(policy["sources"]["unknown-registry"], "deny")
        self.assertEqual(policy["sources"]["unknown-git"], "deny")
        self.assertEqual(policy["sources"]["allow-git"], [])
        self.assertIn("MIT", policy["licenses"]["allow"])
        self.assertIn("Apache-2.0", policy["licenses"]["allow"])
        self.assertEqual(policy["licenses"]["exceptions"], [])

    def test_sbom_includes_authority_transport_dependencies(self):
        packages = build_spdx(ROOT, release="phase8-test")["packages"]
        purls = {item["externalRefs"][0]["referenceLocator"] for item in packages}
        self.assertTrue(any(item.startswith("pkg:cargo/rdkafka@") for item in purls))
        self.assertTrue(any(item.startswith("pkg:cargo/rustls@") for item in purls))


if __name__ == "__main__":
    unittest.main()

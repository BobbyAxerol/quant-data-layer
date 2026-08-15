from __future__ import annotations

import json
import pathlib
import unittest

import yaml

from qdl.certification.release import build_spdx


ROOT = pathlib.Path(__file__).resolve().parents[1]


class Phase83CandidateContractTests(unittest.TestCase):
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

    def test_sbom_includes_authority_transport_dependencies(self):
        packages = build_spdx(ROOT, release="phase8-test")["packages"]
        purls = {item["externalRefs"][0]["referenceLocator"] for item in packages}
        self.assertTrue(any(item.startswith("pkg:cargo/rdkafka@") for item in purls))
        self.assertTrue(any(item.startswith("pkg:cargo/rustls@") for item in purls))


if __name__ == "__main__":
    unittest.main()

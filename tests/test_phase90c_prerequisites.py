from __future__ import annotations

import copy
import hashlib
import subprocess
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path

import yaml

from qdl.certification.prerequisites import (
    CandidateSlice,
    EvidenceRecord,
    EvidenceScope,
    PrerequisitePolicy,
    evaluate_prerequisites,
    load_inventory,
)


ROOT = Path(__file__).resolve().parents[1]


class Phase90CPrerequisiteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = PrerequisitePolicy.load(
            ROOT / "config/phase9/production-prerequisite-policy.yaml"
        )
        cls.candidate = CandidateSlice.load(ROOT / "config/phase9/candidate-slice.yaml")

    def test_local_inventory_is_honest_no_go(self):
        inventory = load_inventory(ROOT / "config/phase9/evidence-inventory.local.yaml")
        decision = evaluate_prerequisites(
            self.policy, self.candidate, inventory,
            repository_root=ROOT, now_ns=1_787_063_420_299_706_631,
        )
        self.assertEqual(decision.decision, "NO_GO_EXTERNAL")
        results = {item.gate_id: item for item in decision.results}
        self.assertEqual(results["replicated_durable_transport"].reason, "INSUFFICIENT_SCOPE")
        self.assertEqual(results["signed_artifact_admission"].reason, "EVIDENCE_BLOCKED")
        self.assertEqual(results["independent_failure_domain_dr"].reason, "MISSING_EVIDENCE")
        self.assertFalse(any(item.passed for item in decision.results))

    def _passing_inventory(self, root: Path, now_ns: int):
        artifact = root / "evidence.json"
        artifact.write_text("{}\n")
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        records = []
        for gate in self.policy.gates:
            details = {key: f"proof-{key}" for key in gate.required_details}
            details.update(gate.assertions)
            details.update(gate.minimums)
            details.update(gate.maximums)
            details.update({
                detail_key: self.candidate.payload[candidate_key]
                for detail_key, candidate_key in gate.candidate_field_matches.items()
            })
            if gate.candidate_bound:
                details["candidate_digest"] = self.candidate.digest
            if gate.gate_id == "exact_slice_approval":
                details["hold_until_ns"] = now_ns + 30_000_000_000
            records.append(EvidenceRecord.from_mapping({
                "evidence_id": f"evidence-{gate.gate_id}",
                "gate_id": gate.gate_id,
                "environment": "production",
                "scope": gate.minimum_scope.value,
                "status": "PASS",
                "issuer": "production-control-plane",
                "observed_at_ns": now_ns - 1_000_000,
                "expires_at_ns": now_ns + 60_000_000_000,
                "artifact_path": "evidence.json",
                "artifact_sha256": digest,
                "details": details,
            }))
        return tuple(records), artifact

    def test_all_exact_production_evidence_is_go(self):
        now_ns = time.time_ns()
        with tempfile.TemporaryDirectory() as directory:
            records, _ = self._passing_inventory(Path(directory), now_ns)
            decision = evaluate_prerequisites(
                self.policy, self.candidate, records,
                repository_root=directory, now_ns=now_ns,
            )
        self.assertEqual(decision.decision, "GO")
        self.assertTrue(all(item.passed for item in decision.results))

    def test_tampered_expired_and_candidate_mismatch_fail_closed(self):
        now_ns = time.time_ns()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records, artifact = self._passing_inventory(root, now_ns)
            artifact.write_text("tampered\n")
            decision = evaluate_prerequisites(
                self.policy, self.candidate, records,
                repository_root=root, now_ns=now_ns,
            )
            self.assertTrue(all(item.reason == "ARTIFACT_DIGEST_MISMATCH" for item in decision.results))

            records, _ = self._passing_inventory(root, now_ns)
            first = records[0]
            expired = EvidenceRecord(**{**first.__dict__, "expires_at_ns": now_ns - 1})
            decision = evaluate_prerequisites(
                self.policy, self.candidate, (expired, *records[1:]),
                repository_root=root, now_ns=now_ns,
            )
            self.assertEqual(decision.results[0].reason, "EVIDENCE_EXPIRED")

            bound_index = next(i for i, gate in enumerate(self.policy.gates) if gate.candidate_bound)
            bound = records[bound_index]
            wrong = EvidenceRecord(**{
                **bound.__dict__,
                "details": {**bound.details, "candidate_digest": "0" * 64},
            })
            changed = list(records); changed[bound_index] = wrong
            decision = evaluate_prerequisites(
                self.policy, self.candidate, tuple(changed),
                repository_root=root, now_ns=now_ns,
            )
            self.assertEqual(decision.results[bound_index].reason, "CANDIDATE_DIGEST_MISMATCH")

    def test_unknown_sensitive_duplicate_and_unsafe_candidate_are_rejected(self):
        raw = yaml.safe_load((ROOT / "config/phase9/evidence-inventory.local.yaml").read_text())
        item = copy.deepcopy(raw["evidence"][0])
        item["details"]["private_key"] = "must-not-enter-evidence"
        with self.assertRaisesRegex(ValueError, "sensitive"):
            EvidenceRecord.from_mapping(item)
        item["details"].pop("private_key")
        item["details"]["nested"] = {"access_token": "must-not-enter-evidence"}
        with self.assertRaisesRegex(ValueError, "sensitive"):
            EvidenceRecord.from_mapping(item)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.yaml"
            raw["evidence"].append(copy.deepcopy(raw["evidence"][0]))
            path.write_text(yaml.safe_dump(raw))
            with self.assertRaisesRegex(ValueError, "unique"):
                load_inventory(path)

        candidate = yaml.safe_load((ROOT / "config/phase9/candidate-slice.yaml").read_text())
        candidate["public_write_allowed"] = True
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as handle:
            yaml.safe_dump(candidate, handle); handle.flush()
            with self.assertRaisesRegex(ValueError, "cannot write"):
                CandidateSlice.load(handle.name)

        candidate = yaml.safe_load((ROOT / "config/phase9/candidate-slice.yaml").read_text())
        del candidate["normalizer_version"]
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as handle:
            yaml.safe_dump(candidate, handle); handle.flush()
            with self.assertRaisesRegex(ValueError, "incomplete"):
                CandidateSlice.load(handle.name)

        candidate = yaml.safe_load((ROOT / "config/phase9/candidate-slice.yaml").read_text())
        candidate["instrument_uids"] = ["not-a-uuid"]
        with tempfile.NamedTemporaryFile("w", suffix=".yaml") as handle:
            yaml.safe_dump(candidate, handle); handle.flush()
            with self.assertRaisesRegex(ValueError, "UID is invalid"):
                CandidateSlice.load(handle.name)

    def test_semantic_assertion_threshold_candidate_and_hold_fail_closed(self):
        now_ns = time.time_ns()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records, _ = self._passing_inventory(root, now_ns)
            indexes = {item.gate_id: index for index, item in enumerate(records)}

            cases = (
                ("replicated_durable_transport", {"acks_all": False}, "ASSERTION_FAILED"),
                ("replicated_durable_transport", {"broker_count": 2}, "MINIMUM_NOT_MET"),
                ("replicated_durable_transport", {"broker_count": True}, "MINIMUM_NOT_MET"),
                ("postgres_pitr", {"rpo_seconds": 301}, "MAXIMUM_EXCEEDED"),
                ("signed_artifact_admission", {"image_digest": "sha256:" + "0" * 64}, "CANDIDATE_FIELD_MISMATCH"),
                ("exact_slice_approval", {"hold_until_ns": now_ns}, "APPROVAL_HOLD_WINDOW_INVALID"),
            )
            for gate_id, detail_update, expected in cases:
                changed = list(records)
                index = indexes[gate_id]
                changed[index] = replace(
                    changed[index], details={**changed[index].details, **detail_update}
                )
                decision = evaluate_prerequisites(
                    self.policy, self.candidate, tuple(changed),
                    repository_root=root, now_ns=now_ns,
                )
                self.assertEqual(decision.results[index].reason, expected, gate_id)

    def test_unknown_evidence_gate_is_rejected(self):
        now_ns = time.time_ns()
        with tempfile.TemporaryDirectory() as directory:
            records, _ = self._passing_inventory(Path(directory), now_ns)
            unknown = replace(records[0], evidence_id="unknown", gate_id="unknown_gate")
            with self.assertRaisesRegex(ValueError, "unknown prerequisite"):
                evaluate_prerequisites(
                    self.policy, self.candidate, (*records, unknown),
                    repository_root=directory, now_ns=now_ns,
                )

    def test_policy_is_complete_and_provider_neutral(self):
        ids = {gate.gate_id for gate in self.policy.gates}
        self.assertEqual(len(ids), 12)
        self.assertTrue({
            "replicated_durable_transport", "production_observability",
            "workload_identity_rbac_network", "external_secret_rotation",
            "signed_artifact_admission", "postgres_pitr", "object_store_restore",
            "independent_failure_domain_dr", "redis_projector_rebuild",
            "consumer_registration_rollback", "persistent_authority_sink_fencing",
            "exact_slice_approval",
        }.issubset(ids))
        policy_text = (ROOT / "config/phase9/production-prerequisite-policy.yaml").read_text()
        self.assertNotIn("BINANCE", policy_text)
        self.assertEqual(
            next(g.minimum_scope for g in self.policy.gates if g.gate_id == "independent_failure_domain_dr"),
            EvidenceScope.INDEPENDENT_FAILURE_DOMAIN,
        )

    def test_operator_entrypoint_imports_from_repository_root(self):
        completed = subprocess.run(
            ["python3", "scripts/phase90c_prerequisite_certification.py", "--help"],
            cwd=ROOT, text=True, capture_output=True, check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()

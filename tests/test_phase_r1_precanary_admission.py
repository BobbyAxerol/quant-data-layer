from __future__ import annotations

import copy
import unittest

from scripts.phase_r1_prepare_precanary_admission import prepare_admission


NOW = 1_800_000_000_000_000_000
CANDIDATE = "sha256:" + "a" * 64
REFERENCE = "sha256:" + "b" * 64
ROLLBACK = "sha256:" + "c" * 64
SCOPE = "d" * 64
CONTRACT = "e" * 64
PLAN = "f" * 64
OTHER = "1" * 64


def artifact() -> dict[str, object]:
    return {
        "schema": "qdl.r1.release-artifact-manifest.v1",
        "status": "PRE_CANARY",
        "source_commit": "abcdef123456",
        "rust_image_digest": CANDIDATE,
        "rollback_rust_image_digest": ROLLBACK,
        "promotion_scope_digest": SCOPE,
        "contract_sha256": CONTRACT,
        "partition_plan_sha256": PLAN,
        "sbom_sha256": OTHER,
        "rollback_manifest_sha256": "2" * 64,
        "public_write_allowed": False,
        "legacy_write_allowed": False,
    }


def reference_parity() -> dict[str, object]:
    return {
        "schema": "qdl.c40.live-core-parity.v1",
        "status": "PASS",
        "captured_at_ns": NOW - 1,
        "provider_provenance": "REAL",
        "production_mutations": 0,
        "source_commit": "c7f3c34f",
        "candidate_image_digest": REFERENCE,
        "scope_digest": SCOPE,
        "slice_count": 12,
        "sample_count": 96,
        "semantic_mismatches": 0,
        "invalid_provenance": 0,
        "slices": [
            {"samples": 8, "semantic_mismatches": 0, "invalid_provenance": 0}
            for _ in range(12)
        ],
    }


def image() -> dict[str, object]:
    return {
        "Id": CANDIDATE,
        "Config": {
            "User": "10001:10001",
            "Labels": {
                "org.opencontainers.image.revision": "abcdef123456",
                "io.qdl.authority.default": "RUST_SHADOW",
            },
        },
    }


class R1PrecanaryAdmissionTests(unittest.TestCase):
    def test_admission_is_reference_only_and_candidate_bound(self) -> None:
        value = prepare_admission(
            release_artifact=artifact(),
            reference_parity=reference_parity(),
            image_inspect=image(),
            now_ns=NOW,
        )
        self.assertEqual(value["status"], "PASS")
        self.assertEqual(value["candidate_runtime_parity_status"], "PENDING_R1_CANARY")
        self.assertEqual(value["candidate_image_digest"], CANDIDATE)
        self.assertEqual(value["reference_runtime_image_digest"], REFERENCE)
        self.assertEqual(value["production_mutations"], 0)

    def test_dirty_or_mislabeled_inputs_fail_closed(self) -> None:
        dirty = reference_parity()
        dirty["semantic_mismatches"] = 1
        with self.assertRaisesRegex(ValueError, "semantic/provenance"):
            prepare_admission(
                release_artifact=artifact(), reference_parity=dirty,
                image_inspect=image(), now_ns=NOW,
            )
        mislabeled = image()
        mislabeled["Config"]["Labels"]["org.opencontainers.image.revision"] = "deadbeef"
        with self.assertRaisesRegex(ValueError, "revision differs"):
            prepare_admission(
                release_artifact=artifact(), reference_parity=reference_parity(),
                image_inspect=mislabeled, now_ns=NOW,
            )
        false_parity = reference_parity()
        false_parity["candidate_image_digest"] = CANDIDATE
        with self.assertRaisesRegex(ValueError, "must not claim candidate runtime parity"):
            prepare_admission(
                release_artifact=artifact(), reference_parity=false_parity,
                image_inspect=image(), now_ns=NOW,
            )

    def test_expiry_window_is_bounded(self) -> None:
        with self.assertRaisesRegex(ValueError, "TTL"):
            prepare_admission(
                release_artifact=artifact(), reference_parity=reference_parity(),
                image_inspect=image(), now_ns=NOW, ttl_seconds=7_201,
            )


if __name__ == "__main__":
    unittest.main()

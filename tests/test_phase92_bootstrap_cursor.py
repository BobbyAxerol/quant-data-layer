from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.control.phase92_bootstrap import (
    BootstrapPartition,
    BootstrapScope,
    build_signed_cursor,
    verify_signed_cursor,
)
from scripts.phase92_issue_bootstrap_cursor import atomic_write, prepare


KEY_ID = "phase92-k1"
KEY = "1" * 64
SCOPE = BootstrapScope(
    consumer_group_id="qdl-production-core-r1-canary-0001-phase92-raw",
    raw_topics=("md.raw.stable.v1",),
    promotion_scope_digest="a" * 64,
    candidate_digest="b" * 64,
    partition_plan_epoch=1,
)


class Phase92BootstrapCursorTests(unittest.TestCase):
    def test_signed_cursor_is_generation_and_scope_bound(self):
        envelope = build_signed_cursor(
            key_id=KEY_ID,
            keyring={KEY_ID: KEY},
            scope=SCOPE,
            partitions=[BootstrapPartition("md.raw.stable.v1", 0, 99)],
            generation=7,
            issued_at_ns=100,
            ttl_seconds=60,
            cursor_id="11111111-1111-4111-8111-111111111111",
        )
        payload = verify_signed_cursor(envelope, keyring={KEY_ID: KEY}, scope=SCOPE)
        self.assertEqual(payload["generation"], 7)
        self.assertEqual(payload["partitions"][0]["offset"], 99)
        tampered = dict(envelope)
        tampered["signature_hex"] = "00" + envelope["signature_hex"][2:]
        with self.assertRaisesRegex(ValueError, "signature"):
            verify_signed_cursor(tampered, keyring={KEY_ID: KEY}, scope=SCOPE)
        wrong = BootstrapScope(
            consumer_group_id=SCOPE.consumer_group_id,
            raw_topics=SCOPE.raw_topics,
            promotion_scope_digest=SCOPE.promotion_scope_digest,
            candidate_digest="c" * 64,
            partition_plan_epoch=1,
        )
        with self.assertRaisesRegex(ValueError, "scope"):
            verify_signed_cursor(envelope, keyring={KEY_ID: KEY}, scope=wrong)

    def test_issuer_dry_run_never_writes_or_resets_and_requires_empty_group(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = root / "runtime"
            runtime.mkdir()
            (runtime / "production-core-001.json").write_text(json.dumps({
                "topics": {"raw_inputs": ["md.raw.stable.v1"]},
                "promotion_scope_digest": "a" * 64,
                "partition_plan_epoch": 1,
            }), encoding="utf-8")
            env = root / "stable.env"
            env.write_text("\n".join((
                f"QDL_STABLE_RUNTIME_DIR={runtime}",
                "QDL_PHASE92_BOOTSTRAP_GROUP_ID=qdl-production-core-r1-canary-0001",
                f"QDL_PHASE92_BOOTSTRAP_CURSOR_KEYS_JSON='{{\"{KEY_ID}\":\"{KEY}\"}}'",
                f"QDL_PHASE92_BOOTSTRAP_CURSOR_ACTIVE_KEY_ID={KEY_ID}",
                "QDL_STABLE_CORE_CERT_DIR=/private/certs",
            )) + "\n", encoding="utf-8")
            calls = []
            def collector(**kwargs):
                calls.append(kwargs)
                return [
                    BootstrapPartition("md.raw.stable.v1", 0, 100),
                    BootstrapPartition("md.raw.stable.v1", 1, 101),
                ]
            report, envelope, target = prepare(
                env_file=env,
                candidate_digest="b" * 64,
                generation=9,
                ttl_seconds=60,
                collector=collector,
                now_ns=lambda: 500,
            )
            self.assertEqual(report["kafka_offset_mutations"], 0)
            self.assertEqual(report["partition_count"], 2)
            self.assertFalse(target.exists())
            self.assertEqual(calls[0]["group_id"], SCOPE.consumer_group_id)
            self.assertTrue(envelope["signature_hex"])

    def test_issued_cursor_is_non_secret_runtime_input(self):
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "production-bootstrap.json"
            atomic_write(target, {"schema": "qdl.phase92.signed-bootstrap.v1"})
            self.assertEqual(target.stat().st_mode & 0o777, 0o644)
            self.assertEqual(
                json.loads(target.read_text(encoding="utf-8"))["schema"],
                "qdl.phase92.signed-bootstrap.v1",
            )


if __name__ == "__main__":
    unittest.main()

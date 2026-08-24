from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts.phaseb_bootstrap_stable_broker import (
    CORE_GROUP_PREFIXES,
    READ_ONLY_AUDIT_EXTRA_TOPICS,
    READ_ONLY_AUDIT_GROUP_PREFIXES,
    TOPIC_POLICIES,
    bootstrap,
)
from scripts.phasec40_collect_live_core_parity import _require_r1_reference_group
from scripts.run_authority_outbox_dispatcher import write_health


ROOT = Path(__file__).resolve().parents[1]


class AuthorityRuntimeContractTests(unittest.TestCase):
    def test_authority_and_checkpoint_topics_are_compacted(self):
        self.assertEqual(TOPIC_POLICIES["qdl.authority.v1"], "compact")
        self.assertEqual(TOPIC_POLICIES["qdl.target-checkpoint.v1"], "compact")
        self.assertEqual(TOPIC_POLICIES["md.raw.realtime.v2"], "delete")
        self.assertEqual(TOPIC_POLICIES["md.raw.stable.v1"], "delete")
        self.assertEqual(TOPIC_POLICIES["md.canonical.v2"], "delete")
        source = (
            ROOT / "scripts/phaseb_bootstrap_stable_broker.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"stable-authority-dispatcher"', source)
        self.assertIn('"qdl-v2-production-core-"', source)
        self.assertEqual(
            CORE_GROUP_PREFIXES,
            ("qdl-v2-production-core-v1-", "qdl-v2-production-core-r1-"),
        )
        self.assertEqual(
            READ_ONLY_AUDIT_GROUP_PREFIXES,
            ("qdl-r1-reference-parity-", "qdl-c40-handoff-"),
        )
        self.assertEqual(
            READ_ONLY_AUDIT_EXTRA_TOPICS,
            ("md.canary.canonical.v2", "qdl.target-checkpoint.v1"),
        )
        self.assertNotIn('"--group", "*"', source)

    def test_r1_reference_collector_refuses_shared_or_broad_group(self):
        self.assertEqual(
            _require_r1_reference_group("qdl-r1-reference-parity-20260824"),
            "qdl-r1-reference-parity-20260824",
        )
        for invalid in (
            "qdl-r1-reference-parity-",
            "qdl-v2-stable-core-v1",
            "qdl-c40-handoff-audit",
        ):
            with self.assertRaisesRegex(ValueError, "isolated"):
                _require_r1_reference_group(invalid)

    def test_bootstrap_grants_only_exact_r1_control_plane_acl_namespaces(self):
        from unittest.mock import patch

        calls = []

        def fake_kafka(_env, executable, *arguments):
            calls.append((executable, arguments))
            if executable == "kafka-topics.sh" and "--describe" in arguments:
                return (
                    "Topic: md.raw.realtime.v2 PartitionCount: 6 "
                    "ReplicationFactor: 3 Configs: min.insync.replicas=2"
                )
            return ""

        with patch("scripts.phaseb_bootstrap_stable_broker.kafka", fake_kafka):
            report = bootstrap(Path("/tmp/stable.env"))

        acl_calls = [arguments for executable, arguments in calls if executable == "kafka-acls.sh"]
        self.assertEqual(report["status"], "PASS")
        self.assertIn(
            (
                "--add", "--allow-principal", "User:phase8-core", "--operation", "READ",
                "--group", "qdl-v2-production-core-r1-",
                "--resource-pattern-type", "prefixed",
            ),
            acl_calls,
        )
        for prefix in READ_ONLY_AUDIT_GROUP_PREFIXES:
            self.assertIn(
                (
                    "--add", "--allow-principal", "User:phase8-consumer",
                    "--operation", "READ", "--group", prefix,
                    "--resource-pattern-type", "prefixed",
                ),
                acl_calls,
            )
        for topic in READ_ONLY_AUDIT_EXTRA_TOPICS:
            self.assertIn(
                (
                    "--add", "--allow-principal", "User:phase8-consumer",
                    "--operation", "READ", "--operation", "DESCRIBE",
                    "--topic", topic,
                ),
                acl_calls,
            )
        self.assertIn("md.raw.realtime.v2", report["topics"])
        self.assertIn("md.raw.stable.v1", report["topics"])
        self.assertIn(
            (
                "--add", "--allow-principal", "User:phase8-producer",
                "--operation", "WRITE", "--operation", "DESCRIBE",
                "--topic", "md.raw.realtime.v2",
            ),
            acl_calls,
        )
        self.assertNotIn(
            (
                "--add", "--allow-principal", "User:phase8-producer",
                "--operation", "WRITE", "--operation", "DESCRIBE",
                "--topic", "md.raw.stable.v1",
            ),
            acl_calls,
        )
        self.assertFalse(any("*" in arguments for arguments in acl_calls))

    def test_dispatcher_role_is_function_scoped_and_not_table_writer(self):
        migration = (
            ROOT / "migrations/postgres/0010_authority_dispatcher_security.sql"
        ).read_text(encoding="utf-8")
        init = (
            ROOT / "migrations/postgres/9999_init_authority_dispatcher_role.sh"
        ).read_text(encoding="utf-8")
        self.assertEqual(migration.count("SECURITY DEFINER"), 3)
        self.assertEqual(migration.count("SET search_path = pg_catalog, public"), 3)
        self.assertEqual(migration.count("FROM PUBLIC"), 3)
        self.assertIn("NOSUPERUSER NOCREATEDB NOCREATEROLE", init)
        self.assertEqual(init.count("GRANT EXECUTE ON FUNCTION"), 3)
        self.assertNotIn("GRANT INSERT", init)
        self.assertNotIn("GRANT UPDATE", init)
        self.assertNotIn("GRANT DELETE", init)

    def test_dispatcher_health_is_atomic_bounded_and_fail_closed(self):
        with tempfile.TemporaryDirectory(prefix="qdl-authority-health-") as directory:
            path = Path(directory) / "runtime/health.json"
            write_health(path, status="READY", published=3)
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "qdl.authority-dispatcher-health.v1")
            self.assertEqual(payload["status"], "READY")
            self.assertEqual(payload["published_last_cycle"], 3)
            self.assertGreater(payload["heartbeat_ns"], 0)
            self.assertFalse(path.with_suffix(".json.tmp").exists())

            write_health(
                path, status="DEGRADED", published=0, error="x" * 2000
            )
            degraded = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(degraded["status"], "DEGRADED")
            self.assertEqual(len(degraded["error"]), 1000)
            with self.assertRaises(ValueError):
                write_health(path, status="UNKNOWN", published=0)
            with self.assertRaises(ValueError):
                write_health(path, status="READY", published=-1)


if __name__ == "__main__":
    unittest.main()

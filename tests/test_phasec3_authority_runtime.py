from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts.phaseb_bootstrap_stable_broker import TOPIC_POLICIES
from scripts.run_authority_outbox_dispatcher import write_health


ROOT = Path(__file__).resolve().parents[1]


class AuthorityRuntimeContractTests(unittest.TestCase):
    def test_authority_and_checkpoint_topics_are_compacted(self):
        self.assertEqual(TOPIC_POLICIES["qdl.authority.v1"], "compact")
        self.assertEqual(TOPIC_POLICIES["qdl.target-checkpoint.v1"], "compact")
        self.assertEqual(TOPIC_POLICIES["md.canonical.v2"], "delete")
        source = (
            ROOT / "scripts/phaseb_bootstrap_stable_broker.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"stable-authority-dispatcher"', source)
        self.assertIn('"qdl-v2-production-core-"', source)
        self.assertIn('"qdl-v2-production-core-v1-"', source)

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

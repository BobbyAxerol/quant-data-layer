from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "postgres" / "0001_phase1_control_plane.sql"


class Phase1MigrationContractTests(unittest.TestCase):
    def test_contains_required_control_metadata_and_no_tick_tables(self):
        sql = MIGRATION.read_text(encoding="utf-8").lower()
        required = {
            "qdl_config_revisions",
            "qdl_session_calendars",
            "qdl_instruments",
            "qdl_instrument_revisions",
            "qdl_instrument_aliases",
            "qdl_source_profiles",
            "qdl_source_policies",
            "qdl_subscription_specs",
            "qdl_ingestion_leases",
            "qdl_job_states",
            "qdl_control_audit",
        }
        created = set(re.findall(r"create table if not exists\s+([a-z0-9_]+)", sql))
        self.assertTrue(required.issubset(created))
        self.assertFalse(any("tick" in table or "trade_event" in table for table in created))

    def test_migrations_are_transactional_and_idempotent_by_ddl(self):
        for path in sorted(MIGRATION.parent.glob("*.sql")):
            sql = path.read_text(encoding="utf-8").strip().lower()
            self.assertTrue(sql.startswith("begin;"), path.name)
            self.assertTrue(sql.endswith("commit;"), path.name)
            self.assertNotIn("drop table", sql)


if __name__ == "__main__":
    unittest.main()

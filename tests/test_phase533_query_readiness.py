from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qdl.runtime.readiness import ComponentState
from qdl.runtime.stable import build_stable_spool, stable_readiness
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig


def _event(index: int, payload: bytes) -> DurableEvent:
    return DurableEvent(
        stream="md.canonical.v2.trade",
        partition_key="BINANCE.USDM.PERPETUAL.BTC-USDT/trade",
        event_id=index.to_bytes(16, "big"),
        payload=payload,
        accepted_at_ns=index,
        content_type="application/json",
    )


class _Quota:
    def ping(self) -> bool:
        return True


class _Registry:
    count = 1
    revisions = (("alpha.binance.paper.stable", 8),)


class Phase533QueryReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="qdl-phase533-readiness-")
        self.spool_config = SpoolConfig(
            path=Path(self.temp.name) / "cache.sqlite3",
            min_free_disk_bytes=0,
            max_partition_records=1,
        )
        self.spool = SQLiteDurableSpool(self.spool_config)
        self.runtime_config = SimpleNamespace(
            role="query_v2",
            authority_mode="RUST_PRIMARY",
            authority_revision=10,
            config_revision="phase533-test",
        )

    def tearDown(self) -> None:
        self.spool.close()
        self.temp.cleanup()

    def test_readiness_summary_tracks_transactional_usage_after_trim(self):
        self.spool.append(_event(1, b"first"))
        self.spool.append(_event(2, b"second"))

        summary = self.spool.readiness_summary()

        self.assertEqual(summary.records, 1)
        self.assertEqual(summary.payload_bytes, len(b"second"))

    def test_existing_usage_state_reopens_without_event_aggregate(self):
        self.spool.append(_event(1, b"first"))
        self.spool.close()

        with patch.object(
            SQLiteDurableSpool,
            "_aggregate_event_usage_locked",
            side_effect=AssertionError("live cache must not be rescanned"),
        ):
            reopened = SQLiteDurableSpool(self.spool_config)
        try:
            self.assertEqual(reopened.readiness_summary().records, 1)
        finally:
            reopened.close()

    def test_missing_usage_state_is_reconstructed_once(self):
        self.spool.append(_event(1, b"first"))
        self.spool.append(_event(2, b"second"))
        self.spool.close()
        with sqlite3.connect(self.spool_config.path) as connection:
            connection.execute("DELETE FROM spool_state")

        reopened = SQLiteDurableSpool(self.spool_config)
        try:
            summary = reopened.readiness_summary()
            self.assertEqual(summary.records, 1)
            self.assertEqual(summary.payload_bytes, len(b"second"))
        finally:
            reopened.close()

    def test_default_spool_open_retains_integrity_check(self):
        path = Path(self.temp.name) / "default-integrity.sqlite3"
        with patch.object(
            SQLiteDurableSpool, "integrity_check", return_value=True
        ) as check:
            spool = SQLiteDurableSpool(SpoolConfig(path=path, min_free_disk_bytes=0))
        try:
            check.assert_called_once_with()
        finally:
            spool.close()

    def test_rebuildable_stable_spool_skips_open_time_integrity_scan(self):
        stable_config = SimpleNamespace(
            durable_state_dir=Path(self.temp.name) / "stable",
            cursor_ttl_seconds=60,
        )
        with patch.object(
            SQLiteDurableSpool,
            "_validate_integrity",
            side_effect=AssertionError("stable startup must not full-scan cache"),
        ):
            spool = build_stable_spool(stable_config)
        try:
            self.assertFalse(spool.config.verify_integrity_on_open)
            self.assertTrue(spool.integrity_check())
        finally:
            spool.close()

    def test_open_does_not_rebuild_retention_index(self):
        path = Path(self.temp.name) / "no-reindex.sqlite3"
        initial = SQLiteDurableSpool(SpoolConfig(path=path, min_free_disk_bytes=0))
        initial.close()

        statements: list[str] = []
        original_connect = sqlite3.connect

        class TracedConnection(sqlite3.Connection):
            def executescript(self, script):
                statements.append(script)
                return super().executescript(script)

        with patch(
            "qdl.transport.sqlite_spool.sqlite3.connect",
            side_effect=lambda *args, **kwargs: original_connect(
                *args, factory=TracedConnection, **kwargs
            ),
        ):
            reopened = SQLiteDurableSpool(
                SpoolConfig(path=path, min_free_disk_bytes=0)
            )
        try:
            self.assertTrue(
                any("CREATE INDEX IF NOT EXISTS idx_qdl_spool_events_retention" in sql for sql in statements)
            )
            self.assertFalse(any("DROP INDEX" in sql for sql in statements))
        finally:
            reopened.close()

    def test_stable_readiness_uses_bounded_summary_not_full_stats(self):
        self.spool.append(_event(1, b"payload"))
        readiness = stable_readiness(
            self.runtime_config, _Registry(), self.spool, quota=_Quota()
        )

        with patch.object(self.spool, "stats", side_effect=AssertionError("full scan")):
            snapshot = asyncio.run(readiness.snapshot())

        component = next(item for item in snapshot.components if item.name == "query_cache")
        self.assertTrue(snapshot.ready)
        self.assertEqual(component.state, ComponentState.READY)
        self.assertIn("records=1", component.detail)

    def test_stable_readiness_fails_closed_when_summary_read_fails(self):
        class BrokenSpool:
            def readiness_summary(self):
                raise sqlite3.DatabaseError("cache unavailable")

        readiness = stable_readiness(
            self.runtime_config, _Registry(), BrokenSpool(), quota=_Quota()
        )
        snapshot = asyncio.run(readiness.snapshot())

        component = next(item for item in snapshot.components if item.name == "query_cache")
        self.assertFalse(snapshot.ready)
        self.assertEqual(component.state, ComponentState.NOT_READY)
        self.assertIn("DatabaseError", component.detail)

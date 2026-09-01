from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from qdl.runtime.readiness import ComponentState
from qdl.runtime.stable import stable_readiness
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
        self.spool = SQLiteDurableSpool(
            SpoolConfig(
                path=Path(self.temp.name) / "cache.sqlite3",
                min_free_disk_bytes=0,
                max_partition_records=1,
            )
        )
        self.config = SimpleNamespace(
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

    def test_stable_readiness_uses_bounded_summary_not_full_stats(self):
        self.spool.append(_event(1, b"payload"))
        readiness = stable_readiness(
            self.config, _Registry(), self.spool, quota=_Quota()
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
            self.config, _Registry(), BrokenSpool(), quota=_Quota()
        )
        snapshot = asyncio.run(readiness.snapshot())

        component = next(item for item in snapshot.components if item.name == "query_cache")
        self.assertFalse(snapshot.ready)
        self.assertEqual(component.state, ComponentState.NOT_READY)
        self.assertIn("DatabaseError", component.detail)

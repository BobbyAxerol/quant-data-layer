"""R1.28: carrying the bar-edge checkpoint across an acquisition revision.

`StableBinanceBarEdge._restore_state` refuses a checkpoint whose identity
differs from the runtime authority, field by field. That is correct and it is
also why bumping the acquisition revision stops the edge from starting - for
every BAR binding it owns, at every interval, on both venues, not only the ones
that changed.

R1.28 bumps the acquisition from 16 to 17 by changing five acquisition *modes*
and nothing else, which leaves every watermark valid. These tests pin what the
migration may carry across and, more importantly, what it must refuse: a moved
binding set or a moved catalog revision are a bootstrap or a repair, and
carrying watermarks across either would claim history the edge does not have.

The last test is the one that matters: it feeds the migrated payload to the
real `_restore_state` and asserts the edge accepts it and keeps its watermarks.
A migration that only satisfies its own author's idea of the format would pass
the first four and fail the fifth.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/migrate_stable_bar_edge_checkpoint.py"
SPEC = importlib.util.spec_from_file_location("migrate_stable_bar_edge_checkpoint", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
migrate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migrate
SPEC.loader.exec_module(migrate)

from qdl.runtime.stable_bar_edge import StableBinanceBarEdge  # noqa: E402

BINDINGS = ["binance-usdm-btcusdt-bar-1m", "okx-swap-btcusdt-bar-1m"]


def _checkpoint(**overrides) -> dict:
    payload = {
        "schema": "qdl.stable-bar-edge-state.v4",
        "slice_id": "qdl-v2-shared-realtime-primary",
        "authority_revision": 1,
        "catalog_revision": 8,
        "acquisition_revision": 16,
        "warmup_rows": 10_000,
        "binding_ids": list(BINDINGS),
        "canonical_cache_id": "16e01c69dd3b4ff183d723132e2e4657",
        "connection_generation": 1_789_663_161_610_942_215,
        "last_open_ms": {name: 1_789_732_800_000 for name in BINDINGS},
    }
    payload.update(overrides)
    return payload


def _planned(**overrides) -> dict:
    payload = {
        "catalog_revision": 8,
        "acquisition_revision": 17,
        "binding_ids": sorted(BINDINGS),
    }
    payload.update(overrides)
    return payload


class MigrationDecisionTests(unittest.TestCase):
    def test_a_mode_only_revision_bump_carries_every_watermark(self):
        migrated, problems, noop = migrate.migration_plan(_checkpoint(), _planned())
        self.assertEqual(problems, [])
        self.assertFalse(noop)
        assert migrated is not None
        self.assertEqual(migrated["acquisition_revision"], 17)
        self.assertEqual(migrated["last_open_ms"], _checkpoint()["last_open_ms"])
        self.assertEqual(
            migrated["connection_generation"], _checkpoint()["connection_generation"]
        )
        self.assertEqual(
            migrated["canonical_cache_id"], _checkpoint()["canonical_cache_id"]
        )

    def test_only_the_acquisition_revision_changes(self):
        current = _checkpoint()
        migrated, _problems, _noop = migrate.migration_plan(current, _planned())
        assert migrated is not None
        differing = {k for k in set(current) | set(migrated) if current.get(k) != migrated.get(k)}
        self.assertEqual(differing, {"acquisition_revision"})

    def test_a_moved_binding_set_is_refused(self):
        """A new or dropped BAR binding means the edge has no watermark for it."""
        planned = _planned(binding_ids=sorted(BINDINGS + ["binance-usdm-xrpusdt-bar-1m"]))
        migrated, problems, _noop = migrate.migration_plan(_checkpoint(), planned)
        self.assertIsNone(migrated)
        self.assertTrue(any("bootstrap and not a migration" in item for item in problems))

    def test_a_moved_catalog_revision_is_refused(self):
        """The instruments moved, so the watermarks may describe different bars."""
        migrated, problems, _noop = migrate.migration_plan(
            _checkpoint(), _planned(catalog_revision=9)
        )
        self.assertIsNone(migrated)
        self.assertTrue(any("catalog_revision differs" in item for item in problems))

    def test_an_already_migrated_checkpoint_is_a_no_op(self):
        migrated, _problems, noop = migrate.migration_plan(
            _checkpoint(acquisition_revision=17), _planned()
        )
        self.assertTrue(noop)
        self.assertIsNone(migrated)


class RestoreAcceptsTheMigratedCheckpointTests(unittest.TestCase):
    """The falsifiable one: the real `_restore_state` must accept the result."""

    def _edge(self, *, acquisition_revision: int) -> StableBinanceBarEdge:
        edge = object.__new__(StableBinanceBarEdge)
        edge.repair_only = False
        edge.authority = {"slice_id": "qdl-v2-shared-realtime-primary", "revision": 1}
        edge.catalog = SimpleNamespace(catalog_revision=8)
        edge.acquisition = SimpleNamespace(revision=acquisition_revision)
        edge.warmup_rows = 10_000
        edge.canonical_cache_id = "16e01c69dd3b4ff183d723132e2e4657"
        edge.connection_generation = 0
        edge._last_open_ms = {}
        edge._history_bootstrapped = False
        # One interval, so `_valid_source_bar_open_ms` has a cadence to check the
        # watermark against; the pairs are shaped like the real ones.
        sources = tuple(
            SimpleNamespace(
                binding_id=name,
                interval="1m",
                feed=SimpleNamespace(value="BAR"),
                instrument=SimpleNamespace(
                    identity=SimpleNamespace(
                        venue="BINANCE" if name.startswith("binance") else "OKX"
                    )
                ),
            )
            for name in BINDINGS
        )
        edge.history_bindings = tuple(
            (item, SimpleNamespace(runtime="BINANCE", mode="RUST_NATIVE"))
            for item in sources
            if item.binding_id.startswith("binance")
        )
        edge.history_okx_bindings = tuple(
            (item, SimpleNamespace(runtime="OKX", mode="RUST_NATIVE"))
            for item in sources
            if item.binding_id.startswith("okx")
        )
        edge._checkpoint_history_gaps = lambda restored: {}
        return edge

    def _restore(self, payload: dict, *, acquisition_revision: int) -> StableBinanceBarEdge:
        edge = self._edge(acquisition_revision=acquisition_revision)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            edge.state_path = path
            edge._restore_state()
        return edge

    def test_the_unmigrated_checkpoint_is_refused_at_the_new_revision(self):
        """The failure this migration exists to prevent, reproduced first."""
        with self.assertRaisesRegex(RuntimeError, "acquisition_revision"):
            self._restore(_checkpoint(), acquisition_revision=17)

    def test_the_migrated_checkpoint_restores_with_its_watermarks(self):
        migrated, problems, _noop = migrate.migration_plan(_checkpoint(), _planned())
        self.assertEqual(problems, [])
        assert migrated is not None
        edge = self._restore(migrated, acquisition_revision=17)
        self.assertEqual(set(edge._last_open_ms), set(BINDINGS))
        self.assertEqual(
            edge._last_open_ms["binance-usdm-btcusdt-bar-1m"], 1_789_732_800_000
        )
        self.assertTrue(edge._history_bootstrapped, "no re-bootstrap after a migration")
        self.assertEqual(
            edge.connection_generation, _checkpoint()["connection_generation"]
        )


if __name__ == "__main__":
    unittest.main()

"""P18.4 capacity step 1: the core batch-size migration changes one value only."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "migrate_core_batch_size", ROOT / "scripts/migrate_core_batch_size.py"
)
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


def _core(batch_size: int = 256) -> bytes:
    # Shape and key order of a rendered core config; text kept as rendered.
    return (
        '{\n "authority": {"revision": 1},\n "batch_size": %d,\n "batch_wait_ms": 25,\n'
        ' "core": {"bindings": [{"source_id": "s", "batch_size_hint": 256}]},\n'
        ' "max_events": 0,\n "metrics_every_batches": 100,\n'
        ' "transactional_id": "qdl-v2-realtime-core-001"\n}\n' % batch_size
    ).encode()


class CoreBatchSizeMigrationTests(unittest.TestCase):
    def test_only_the_batch_size_bytes_change(self):
        source = _core()
        derived = migration.derive(source)
        self.assertEqual(json.loads(derived)["batch_size"], 2048)
        self.assertEqual(derived.replace(b'"batch_size": 2048', b'"batch_size": 256'), source)
        self.assertEqual(len(derived) - len(source), 1)
        # A binding field that merely contains the name is not touched.
        self.assertIn(b'"batch_size_hint": 256', derived)

    def test_already_migrated_is_idempotent_and_others_are_refused(self):
        self.assertEqual(migration.derive(_core(2048)), _core(2048))
        with self.assertRaisesRegex(ValueError, "unexpected current batch_size 128"):
            migration.derive(_core(128))
        with self.assertRaisesRegex(ValueError, "exactly one"):
            migration.derive(b'{"core": {}, "transactional_id": "x"}')
        with self.assertRaisesRegex(ValueError, "exactly one"):
            migration.derive(_core().replace(b'"max_events"', b'"batch_size": 256, "x"'))
        with self.assertRaisesRegex(ValueError, "not a realtime-core"):
            migration.derive(b'{"batch_size": 256}')

    def test_dry_run_writes_nothing_and_apply_never_overwrites(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            source = directory / "rust_core.json"
            source.write_bytes(_core())
            output = directory / "out"
            receipt = migration.run([source], output, apply=False)
            self.assertFalse(output.exists())
            self.assertFalse(receipt["applied"])
            receipt = migration.run([source], output, apply=True)
            written = (output / "rust_core.json").read_bytes()
            self.assertEqual(written, migration.derive(_core()))
            self.assertEqual(source.read_bytes(), _core())
            self.assertEqual(
                receipt["files"][0]["output_sha256"],
                migration._sha256(written),
            )
            self.assertEqual((output / "rust_core.json").stat().st_mode & 0o777, 0o640)
            with self.assertRaises(FileExistsError):
                migration.run([source], output, apply=True)


if __name__ == "__main__":
    unittest.main()

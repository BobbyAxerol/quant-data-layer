"""P18.4 capacity step 1: the core batch-size migration changes one value only."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
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
        derived = migration.derive(source, target=2048)
        self.assertEqual(json.loads(derived)["batch_size"], 2048)
        self.assertEqual(derived.replace(b'"batch_size": 2048', b'"batch_size": 256'), source)
        self.assertEqual(len(derived) - len(source), 1)
        # A binding field that merely contains the name is not touched.
        self.assertIn(b'"batch_size_hint": 256', derived)

    def test_already_migrated_is_idempotent_and_others_are_refused(self):
        self.assertEqual(migration.derive(_core(2048), target=2048), _core(2048))
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
            # The tool creates files with os.open(..., 0o640), which the process umask can narrow (CI: 0077);
            # fix the umask so the asserted mode is the tool's requested mode, not the environment's.
            previous_umask = os.umask(0o027)
            try:
                receipt = migration.run([source], output, apply=True)
            finally:
                os.umask(previous_umask)
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

    def test_target_1024_is_derived_and_recorded_in_the_receipt(self):
        source = _core()
        derived = migration.derive(source, target=1024)
        self.assertEqual(json.loads(derived)["batch_size"], 1024)
        self.assertEqual(derived.replace(b'"batch_size": 1024', b'"batch_size": 256'), source)
        self.assertIn(b'"batch_size_hint": 256', derived)
        # Already at the target: idempotent; a 2048 source is not silently lowered.
        self.assertEqual(migration.derive(_core(1024), target=1024), _core(1024))
        with self.assertRaisesRegex(ValueError, "unexpected current batch_size 2048"):
            migration.derive(_core(2048), target=1024)
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            source_path = directory / "rust_core.json"
            source_path.write_bytes(source)
            output = directory / "out"
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(
                    migration.main(
                        [
                            str(source_path),
                            "--output-dir",
                            str(output),
                            "--target-batch-size",
                            "1024",
                            "--apply",
                        ]
                    ),
                    0,
                )
            printed = json.loads(stdout.getvalue())
            self.assertEqual(printed["batch_size"], {"from": 256, "to": 1024})
            stored = json.loads((output / "core-batch-size-receipt.json").read_text())
            self.assertEqual(stored, printed)
            written = (output / "rust_core.json").read_bytes()
            self.assertEqual(written, derived)
            self.assertEqual(stored["files"][0]["output_sha256"], migration._sha256(written))
            self.assertEqual(stored["files"][0]["source_sha256"], migration._sha256(source))
            self.assertEqual(source_path.read_bytes(), source)

    def test_default_target_is_the_stable_constant_1024(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            source_path = directory / "rust_core.json"
            source_path.write_bytes(_core())
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                migration.main([str(source_path), "--output-dir", str(directory / "out")])
            receipt = json.loads(stdout.getvalue())
            self.assertEqual(receipt["batch_size"], {"from": 256, "to": 1024})
            self.assertFalse(receipt["applied"])
            self.assertFalse((directory / "out").exists())

    def test_out_of_range_and_non_integer_targets_are_refused(self):
        self.assertEqual(migration.CORE_BATCH_SIZE_MAX, 10_000)
        self.assertEqual(
            json.loads(migration.derive(_core(), target=10_000))["batch_size"], 10_000
        )
        self.assertEqual(json.loads(migration.derive(_core(), target=1))["batch_size"], 1)
        for bad in (0, -1, 10_001):
            with self.assertRaisesRegex(ValueError, "outside 1..=10000"):
                migration.derive(_core(), target=bad)
        for bad in ("1024", 1024.0, True, None):
            with self.assertRaisesRegex(ValueError, "must be an integer"):
                migration.derive(_core(), target=bad)
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            source_path = directory / "rust_core.json"
            source_path.write_bytes(_core())
            output = directory / "out"
            with self.assertRaisesRegex(ValueError, "outside"):
                migration.run([source_path], output, apply=True, target_batch_size=0)
            self.assertFalse(output.exists())
            for bad in ("0", "10001", "-5", "abc", "1024.0", "1e3", ""):
                with self.subTest(arg=bad), contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        migration.main(
                            [
                                str(source_path),
                                "--output-dir",
                                str(output),
                                "--target-batch-size",
                                bad,
                                "--apply",
                            ]
                        )
                    self.assertEqual(raised.exception.code, 2)
            self.assertFalse(output.exists())
            self.assertEqual(source_path.read_bytes(), _core())


if __name__ == "__main__":
    unittest.main()

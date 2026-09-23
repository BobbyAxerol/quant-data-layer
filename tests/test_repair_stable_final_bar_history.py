from __future__ import annotations

import contextlib
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/repair_stable_final_bar_history.py"


def _module():
    spec = importlib.util.spec_from_file_location("repair_stable_final_bar_history", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RepairStableFinalBarHistoryCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _module()
        self.binding = "okx-swap-btcusdt-bar-1m"
        self.plan = SimpleNamespace(
            source=SimpleNamespace(binding_id=self.binding),
            acquisition=SimpleNamespace(runtime="OKX"),
            envelopes=(object(), object(), object()),
            expected_opens=frozenset({120_000, 180_000, 240_000}),
            missing_envelopes=(object(),),
        )

    def _arguments(self, *extra: str) -> list[str]:
        return [
            "--binding", self.binding,
            "--rows", "3",
            "--expected-missing", f"{self.binding}=1",
            *extra,
        ]

    def test_explicit_dry_run_uses_publisher_disabled_builder(self) -> None:
        edge = SimpleNamespace(
            prepare_history_repair=lambda *_args, **_kwargs: self.plan,
            stop=lambda: None,
        )
        output = io.StringIO()
        with patch.object(
            self.module,
            "build_readonly_repair_probe_from_environment",
            return_value=edge,
        ) as readonly, patch.object(
            self.module,
            "build_from_environment",
        ) as writable, contextlib.redirect_stdout(output):
            self.assertEqual(self.module.main(self._arguments("--dry-run")), 0)
        readonly.assert_called_once_with()
        writable.assert_not_called()
        self.assertIn('"status": "DRY_RUN"', output.getvalue())
        self.assertIn('"production_mutations": 0', output.getvalue())

    def test_pinned_window_reaches_provider_probe_unchanged(self) -> None:
        edge = Mock()
        edge.prepare_history_repair.return_value = self.plan
        edge._open_time_ms.return_value = 180_000
        with patch.object(self.module, 'build_readonly_repair_probe_from_environment', return_value=edge), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.module.main(self._arguments(
                '--dry-run', '--observed-ms', '300001', '--expected-open', f'{self.binding}=180000'
            )), 0)
        edge.prepare_history_repair.assert_called_once_with(self.binding, rows=3, observed_ms=300001)
        edge.apply_history_repair.assert_not_called()

    def test_same_count_wrong_historical_open_cannot_publish(self) -> None:
        edge = Mock()
        edge.prepare_history_repair.return_value = self.plan
        edge._open_time_ms.return_value = 240_000
        with patch.object(self.module, 'build_from_environment', return_value=edge):
            with self.assertRaisesRegex(RuntimeError, 'missing opens differ'):
                self.module.main(self._arguments(
                    '--apply', '--confirm', self.module.CONFIRM,
                    '--observed-ms', '300001', '--expected-open', f'{self.binding}=180000'
                ))
        edge.apply_history_repair.assert_not_called()
        edge.stop.assert_called_once()

    def test_exact_pinned_repair_converges_with_one_publish(self) -> None:
        edge = Mock()
        edge.prepare_history_repair.return_value = self.plan
        edge._open_time_ms.return_value = 180_000
        edge.apply_history_repair.return_value = 1
        edge.history_repair_remaining_rows.return_value = 0
        output = io.StringIO()
        with patch.object(self.module, 'build_from_environment', return_value=edge), contextlib.redirect_stdout(output):
            self.assertEqual(self.module.main(self._arguments(
                '--apply', '--confirm', self.module.CONFIRM,
                '--observed-ms', '300001', '--expected-open', f'{self.binding}=180000'
            )), 0)
        edge.apply_history_repair.assert_called_once_with(self.plan, expected_missing_rows=1)
        self.assertIn('"status": "CONVERGED"', output.getvalue())

    def test_dry_run_and_apply_cannot_be_combined(self) -> None:
        with self.assertRaisesRegex(SystemExit, "mutually exclusive"):
            self.module.main(self._arguments("--dry-run", "--apply"))

    def test_apply_requires_confirmation_before_building_a_writer(self) -> None:
        with patch.object(self.module, "build_from_environment") as builder:
            with self.assertRaisesRegex(SystemExit, "requires --confirm"):
                self.module.main(self._arguments("--apply"))
        builder.assert_not_called()


if __name__ == "__main__":
    unittest.main()

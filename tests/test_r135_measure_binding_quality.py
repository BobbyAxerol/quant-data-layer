"""R1.35-B: the read-only quality probe honors declared feed semantics."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


def _probe_module():
    script_dir = Path(__file__).resolve().parents[1] / "scripts"
    spec = importlib.util.spec_from_file_location(
        "r135_measure_binding_quality", script_dir / "measure_binding_quality.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(script_dir))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(script_dir))
    return module


class MeasureBindingQualitySemanticsTests(unittest.TestCase):
    def test_strict_and_quiet_requirements_use_their_declared_policy(self):
        probe = _probe_module()
        quality = SimpleNamespace(
            state="LIVE",
            complete=True,
            execution_eligible=True,
            gap_open=False,
            freshness_ms=2_001,
        )
        strict = SimpleNamespace(
            effective_event_recency_policy=SimpleNamespace(value="BLOCK"),
            max_freshness_ms=2_000,
        )
        quiet = SimpleNamespace(
            effective_event_recency_policy=SimpleNamespace(value="OBSERVE"),
            max_freshness_ms=2_000,
        )

        self.assertFalse(probe._quality_is_usable(strict, quality))
        self.assertTrue(probe._quality_is_usable(quiet, quality))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

"""Every entry module must import first in a fresh interpreter.

A cycle between packages is invisible when something else has already imported
half of it. `qdl.consumer.stable` gained a top-level import of
`qdl.runtime.provider_history`, which reaches `qdl.security` and comes back to
`qdl.consumer` — a cycle the whole existing suite passed straight through,
because by the time any test ran, the modules were already in `sys.modules` in
a benign order. It failed the moment a container started with a different entry
point.

Each import therefore runs in its own interpreter, which is the only place the
order is actually fresh.
"""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Entry points a role or a tool actually imports first.
MODULES = (
    "qdl.runtime.provider_history",
    "qdl.runtime.stable",
    "qdl.runtime.stable_catalog",
    "qdl.runtime.stable_source",
    "qdl.runtime.routed_query",
    "qdl.consumer",
    "qdl.consumer.stable",
    "qdl.security",
    "qdl.stream",
    "qdl.api_v2.router",
)


class ImportOrderTests(unittest.TestCase):
    def test_each_entry_module_imports_on_its_own(self) -> None:
        for module in MODULES:
            with self.subTest(module=module):
                result = subprocess.run(
                    [sys.executable, "-B", "-c", f"import {module}"],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    f"{module} cannot be imported first:\n{result.stderr}",
                )


if __name__ == "__main__":
    unittest.main()

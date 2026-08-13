from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ProductionProvenanceTests(unittest.TestCase):
    def test_ingestion_and_live_smoke_do_not_import_fixture_or_simulator_modules(self):
        production_files = [
            *sorted((ROOT / "qdl/adapters").rglob("*.py")),
            *sorted((ROOT / "qdl/ingestion").rglob("*.py")),
            ROOT / "scripts/phase3_real_provider_smoke.py",
        ]
        violations = []
        for path in production_files:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                for name in names:
                    if "fixture" in name or "simulator" in name:
                        violations.append(f"{path.relative_to(ROOT)}:{name}")
        self.assertEqual(violations, [])

    def test_synthetic_sources_are_confined_to_tests(self):
        for path in sorted((ROOT / "qdl").rglob("*.py")):
            if "simulator" in path.parts:
                continue
            text = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("generate_fake_market", text, str(path))
            self.assertNotIn("seed_market_data", text, str(path))


if __name__ == "__main__":
    unittest.main()

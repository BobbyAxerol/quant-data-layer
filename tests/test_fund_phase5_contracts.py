from __future__ import annotations

import json
import unittest
from pathlib import Path

from scripts.generate_phase5_openapi import build_openapi


ROOT = Path(__file__).resolve().parents[1]


class Phase5OpenApiContractTests(unittest.TestCase):
    def test_v2_openapi_matches_frozen_snapshot_and_has_typed_public_responses(self):
        expected = json.loads(
            (ROOT / "contracts/v2/openapi.snapshot.json").read_text(encoding="utf-8")
        )
        current = build_openapi()
        self.assertEqual(current, expected)
        paths = current["paths"]
        self.assertEqual(len(paths), 10)
        for path, operations in paths.items():
            for method, operation in operations.items():
                if method not in {"get", "post"}:
                    continue
                success = operation["responses"]["200"]["content"]["application/json"]
                self.assertIn("schema", success, f"untyped success response: {method} {path}")


if __name__ == "__main__":
    unittest.main()

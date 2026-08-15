from __future__ import annotations

import json
import unittest
from pathlib import Path

from qdl.ingestion.contracts import FeedType
from qdl.ingestion.extension import AdapterDeclaration


ROOT = Path(__file__).resolve().parents[1]


class AdapterExtensionTests(unittest.TestCase):
    def test_deribit_style_fixture_proves_boundary_without_activation_claim(self):
        fixture = json.loads(
            (ROOT / "tests/fixtures/phase3/deribit_option_book.json").read_text()
        )
        self.assertEqual(fixture["provenance"], "TEST_SYNTHETIC_EXTENSION_FIXTURE")
        declaration = AdapterDeclaration(
            provider=fixture["provider"], venue=fixture["venue"],
            markets=frozenset({fixture["market"]}), feeds=frozenset({FeedType.BOOK}),
            production_certified=False,
        )
        declaration.require(market="OPTION", feed=FeedType.BOOK, production=False)
        with self.assertRaisesRegex(RuntimeError, "not production-certified"):
            declaration.require(market="OPTION", feed=FeedType.BOOK, production=True)

    def test_capability_boundary_rejects_undeclared_feed(self):
        declaration = AdapterDeclaration(
            provider="OKX_DIRECT", venue="OKX", markets=frozenset({"SWAP"}),
            feeds=frozenset({FeedType.TRADE, FeedType.BOOK}), production_certified=False,
        )
        with self.assertRaisesRegex(RuntimeError, "does not declare feed"):
            declaration.require(market="SWAP", feed=FeedType.BAR, production=False)


if __name__ == "__main__":
    unittest.main()

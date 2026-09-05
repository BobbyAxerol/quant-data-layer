from __future__ import annotations

from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest

import yaml

from qdl.consumer import ConsumerManifestLoader, requirement_key
from qdl.query import ConsumerGrade, FeedType


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase533_materialize_alpha_runtime_entitlements.py"


def _module():
    spec = importlib.util.spec_from_file_location("phase533_alpha_entitlements", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class Phase533AlphaRuntimeEntitlementTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tool = _module()
        cls.catalog = _load(ROOT / "config/v2/stable-source-bindings.yaml")
        cls.demand = _load(ROOT / "config/v2/stable-crypto-demand.yaml")
        cls.reference = _load(ROOT / "consumers/stable/reference-l2-stable.yaml")
        cls.manifests = {
            consumer_id: _load(ROOT / "consumers/stable" / target["filename"])
            for consumer_id, target in cls.tool._TARGETS.items()
        }
        cls.release = _load(ROOT / "config/v2/stable-v2-release-routing.yaml")
        cls.primary = _load(ROOT / "config/v2/stable-primary-consumer-routing.yaml")
        (
            cls.rendered,
            cls.rendered_release,
            cls.rendered_primary,
            cls.summary,
        ) = cls.tool.build_documents(
            catalog=cls.catalog,
            demand=cls.demand,
            reference_manifest=cls.reference,
            alpha_manifests=cls.manifests,
            release_route=cls.release,
            primary_route=cls.primary,
        )

    def test_five_liquid_manifests_are_complete_bounded_and_non_execution(self) -> None:
        expected = {
            "alpha.binance.paper.stable": {
                FeedType.BAR: 70,
                FeedType.TRADE: 5,
                FeedType.QUOTE: 5,
                FeedType.BOOK_SNAPSHOT: 5,
                FeedType.BOOK_DELTA: 5,
                "reference": 35,
                "total": 125,
            },
            "alpha.okx.paper.stable": {
                FeedType.BAR: 70,
                FeedType.TRADE: 5,
                FeedType.QUOTE: 5,
                FeedType.BOOK_SNAPSHOT: 5,
                FeedType.BOOK_DELTA: 5,
                "reference": 20,
                "total": 110,
            },
        }
        reference_feeds = {
            FeedType.BASIS,
            FeedType.CONTRACT_METADATA,
            FeedType.FUNDING_RATE,
            FeedType.LONG_SHORT_RATIO,
            FeedType.MARK_INDEX_PRICE,
            FeedType.OPEN_INTEREST,
            FeedType.TAKER_FLOW,
        }
        for consumer_id, payload in self.rendered.items():
            manifest = ConsumerManifestLoader.from_mapping(payload)
            wanted = expected[consumer_id]
            self.assertEqual(len(manifest.requirements), wanted["total"])
            self.assertEqual(manifest.execution_dependency, "FORBIDDEN")
            self.assertEqual(manifest.quotas.max_warmup_rows, 10_000)
            self.assertEqual({item.consumer_grade for item in manifest.requirements}, {ConsumerGrade.ALPHA})
            for feed in (
                FeedType.BAR,
                FeedType.TRADE,
                FeedType.QUOTE,
                FeedType.BOOK_SNAPSHOT,
                FeedType.BOOK_DELTA,
            ):
                self.assertEqual(sum(item.feed is feed for item in manifest.requirements), wanted[feed])
            references = [item for item in manifest.requirements if item.feed in reference_feeds]
            self.assertEqual(len(references), wanted["reference"])
            self.assertTrue(all(item.source_policy_id == "crypto_liquid_v2" for item in references))
            self.assertTrue(all(item.recovery.value == "SNAPSHOT_AND_REPLAY" for item in manifest.requirements if item.feed is FeedType.BAR))
            self.assertTrue(all(item.require_final_bars for item in manifest.requirements if item.feed is FeedType.BAR))
            self.assertTrue(all(item.max_session_liveness_ms == 45_000 for item in manifest.requirements if item.feed in {FeedType.TRADE, FeedType.QUOTE, FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}))

    def test_native_identity_interval_and_venue_never_cross_mix(self) -> None:
        catalog_by_uid = {
            item["instrument_uid"]: item for item in self.catalog["instruments"]
        }
        expected_venue = {
            "alpha.binance.paper.stable": ("BINANCE", "USDM"),
            "alpha.okx.paper.stable": ("OKX", "SWAP"),
        }
        for consumer_id, payload in self.rendered.items():
            manifest = ConsumerManifestLoader.from_mapping(payload)
            venue, market = expected_venue[consumer_id]
            bars_by_uid: dict[str, set[str]] = {}
            for requirement in manifest.requirements:
                instrument = catalog_by_uid[requirement.instrument_uid]
                self.assertEqual((instrument["venue"], instrument["market"]), (venue, market))
                if requirement.feed is FeedType.BAR:
                    bars_by_uid.setdefault(requirement.instrument_uid, set()).add(requirement.interval)
            self.assertEqual(len(bars_by_uid), 5)
            self.assertTrue(all(len(intervals) == 14 for intervals in bars_by_uid.values()))

    def test_release_routes_are_complete_and_only_trade_can_fallback(self) -> None:
        routes = {
            item["consumer_id"]: item
            for item in self.rendered_release["consumers"]
            if item["consumer_id"] in self.rendered
        }
        for consumer_id, payload in self.rendered.items():
            manifest = ConsumerManifestLoader.from_mapping(payload)
            products = {
                item["requirement_key"]: item for item in routes[consumer_id]["products"]
            }
            self.assertEqual(set(products), {requirement_key(item) for item in manifest.requirements})
            for requirement in manifest.requirements:
                product = products[requirement_key(requirement)]
                self.assertEqual(product["route"], "V2_PRIMARY")
                if requirement.feed is FeedType.TRADE and consumer_id.startswith("alpha.binance"):
                    self.assertEqual(product["fallback"], "V1")
                else:
                    self.assertEqual(product["fallback"], "BLOCKED")

    def test_render_is_idempotent_and_capacity_remains_bounded(self) -> None:
        rerun = self.tool.build_documents(
            catalog=self.catalog,
            demand=self.demand,
            reference_manifest=self.reference,
            alpha_manifests=self.rendered,
            release_route=self.rendered_release,
            primary_route=self.rendered_primary,
        )
        self.assertEqual(rerun[0], self.rendered)
        self.assertEqual(rerun[1], self.rendered_release)
        self.assertEqual(rerun[2], self.rendered_primary)
        self.assertFalse(rerun[3]["manifest_changed"])
        self.assertFalse(rerun[3]["release_route_changed"])

        oversized = deepcopy(self.rendered["alpha.binance.paper.stable"])
        oversized["spec"]["requirements"] = (
            oversized["spec"]["requirements"] * 3
        )[:257]
        with self.assertRaisesRegex(ValueError, "1..256"):
            ConsumerManifestLoader.from_mapping(oversized)


if __name__ == "__main__":
    unittest.main()

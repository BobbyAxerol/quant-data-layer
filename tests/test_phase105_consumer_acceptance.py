from __future__ import annotations

import unittest
from pathlib import Path

from qdl.certification.phase105_consumer_acceptance import (
    PHASE105_PAPER_CONSUMER_IDS,
    build_release_consumer_acceptance_scope,
)
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]


class Phase105ConsumerAcceptanceScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.release = StableReleaseRoutePlan.load(
            ROOT / "config/v2/stable-v2-release-routing.yaml",
            manifest_root=ROOT,
        )
        cls.catalog = StableSourceCatalog.load(
            ROOT / "config/v2/stable-source-bindings.yaml"
        )
        cls.acquisition = StableAcquisitionPlan.load(
            ROOT / "config/v2/stable-acquisition-bindings.yaml",
            catalog=cls.catalog,
        )

    def test_scope_is_exactly_release_v2_primary_for_all_paper_classes(self):
        scope = build_release_consumer_acceptance_scope(
            self.release,
            catalog=self.catalog,
            acquisition=self.acquisition,
        )
        self.assertEqual(scope.schema, "qdl.phase105.consumer-acceptance-scope.v1")
        self.assertEqual(
            {item.consumer_id for item in scope.products},
            PHASE105_PAPER_CONSUMER_IDS,
        )
        expected = {
            (consumer.consumer_id, product.requirement_key)
            for consumer in self.release.consumers
            if consumer.consumer_id in PHASE105_PAPER_CONSUMER_IDS
            for product in consumer.products
            if product.route == "V2_PRIMARY"
        }
        actual = {
            (item.consumer_id, requirement_key(item.requirement))
            for item in scope.products
        }
        self.assertEqual(actual, expected)
        self.assertTrue(scope.products)

    def test_v1_primary_vn_requirements_are_explicitly_excluded(self):
        scope = build_release_consumer_acceptance_scope(
            self.release,
            catalog=self.catalog,
            acquisition=self.acquisition,
        )
        self.assertEqual(
            {(item.consumer_id, item.reason) for item in scope.excluded},
            {
                ("monitoring.multivenue.stable", "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE"),
                ("trading-system.paper.stable", "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE"),
            },
        )


if __name__ == "__main__":
    unittest.main()

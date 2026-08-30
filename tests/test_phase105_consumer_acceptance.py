from __future__ import annotations

import unittest
from pathlib import Path

import yaml

from qdl.certification.phase105_consumer_acceptance import (
    PHASE105_PAPER_CONSUMER_IDS,
    build_release_consumer_acceptance_scope,
)
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.query import FeedType, StalePolicy
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
FIVE_LIQUID_CONSUMER_IDS = frozenset({
    "trading-system.paper.stable",
    "alpha.binance.paper.stable",
    "alpha.okx.paper.stable",
})


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
        cls.five_liquid_demand = yaml.safe_load(
            (ROOT / "config/v2/phase115c-paper-consumer-demand.yaml").read_text(
                encoding="utf-8"
            )
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
            if (
                product.route == "V2_PRIMARY"
                and next(
                    requirement
                    for requirement in consumer.manifest.requirements
                    if requirement_key(requirement) == product.requirement_key
                ).feed in {FeedType.TRADE, FeedType.QUOTE, FeedType.BAR}
            )
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

    def test_five_liquid_selection_is_exactly_the_sixty_trade_quote_bar_routes(self):
        scope = build_release_consumer_acceptance_scope(
            self.release,
            catalog=self.catalog,
            acquisition=self.acquisition,
            consumer_ids=FIVE_LIQUID_CONSUMER_IDS,
        )
        expected = {
            (
                consumer["consumer_id"],
                requirement["venue"],
                requirement["market"],
                requirement["product_type"],
                requirement["native_symbol"],
                requirement["feed"],
                requirement["interval"],
                requirement["source_policy_id"],
            )
            for consumer in self.five_liquid_demand["consumers"]
            for requirement in consumer["requirements"]
            if requirement["feed"] in {"TRADE", "QUOTE", "BAR"}
        }
        actual = {
            (
                product.consumer_id,
                product.venue,
                product.market,
                self.catalog.instrument_for(product.instrument_uid).identity.product_type,
                product.native_symbol,
                product.feed.value,
                product.interval,
                product.source_policy_id,
            )
            for product in scope.products
        }
        self.assertEqual(len(scope.products), 60)
        self.assertEqual(actual, expected)
        self.assertEqual(
            {
                consumer_id: sum(
                    product.consumer_id == consumer_id for product in scope.products
                )
                for consumer_id in FIVE_LIQUID_CONSUMER_IDS
            },
            {
                "trading-system.paper.stable": 30,
                "alpha.binance.paper.stable": 15,
                "alpha.okx.paper.stable": 15,
            },
        )
        self.assertEqual(
            {(item.consumer_id, item.reason) for item in scope.excluded},
            {("trading-system.paper.stable", "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE")},
        )

    def test_paper_trade_routes_declare_observed_event_recency_and_session_sla(self):
        scope = build_release_consumer_acceptance_scope(
            self.release,
            catalog=self.catalog,
            acquisition=self.acquisition,
            consumer_ids=FIVE_LIQUID_CONSUMER_IDS,
        )
        trades = [item for item in scope.products if item.feed is FeedType.TRADE]
        self.assertEqual(len(trades), 20)
        self.assertTrue(all(
            item.requirement.event_recency_policy is StalePolicy.OBSERVE
            and item.requirement.max_session_liveness_ms == 45_000
            and item.requirement.stale_policy is StalePolicy.BLOCK
            for item in trades
        ))

    def test_paper_quote_routes_keep_strict_freshness_and_session_sla(self):
        scope = build_release_consumer_acceptance_scope(
            self.release,
            catalog=self.catalog,
            acquisition=self.acquisition,
            consumer_ids=FIVE_LIQUID_CONSUMER_IDS,
        )
        quotes = [item for item in scope.products if item.feed is FeedType.QUOTE]
        self.assertEqual(len(quotes), 10)
        self.assertTrue(all(
            item.requirement.event_recency_policy is None
            and item.requirement.max_freshness_ms == 2_000
            and item.requirement.max_session_liveness_ms == 45_000
            and item.requirement.stale_policy is StalePolicy.BLOCK
            for item in quotes
        ))

    def test_execution_mark_and_l2_routes_are_exact_and_fail_closed(self):
        consumer = next(
            item
            for item in self.release.consumers
            if item.consumer_id == "trading-system.paper.stable"
        )
        requirements = {
            requirement_key(item): item for item in consumer.manifest.requirements
        }
        typed = [
            (route, requirements[route.requirement_key])
            for route in consumer.products
            if requirements[route.requirement_key].feed in {
                FeedType.MARK_INDEX_PRICE,
                FeedType.BOOK_SNAPSHOT,
                FeedType.BOOK_DELTA,
            }
        ]
        self.assertEqual(len(typed), 12)
        self.assertTrue(all(route.route == "V2_PRIMARY" for route, _ in typed))
        self.assertTrue(all(route.fallback == "BLOCKED" for route, _ in typed))
        self.assertEqual(
            {requirement.source_policy_id for _, requirement in typed},
            {"crypto_liquid_v2"},
        )
        self.assertTrue(all(
            requirement.consumer_grade.value == "EXECUTION"
            and requirement.interval is None
            and requirement.require_final_bars is False
            and requirement.require_full_coverage is True
            for _, requirement in typed
        ))


if __name__ == "__main__":
    unittest.main()

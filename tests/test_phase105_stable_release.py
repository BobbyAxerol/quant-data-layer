from __future__ import annotations

from dataclasses import replace
import copy
import hashlib
from pathlib import Path
import shutil
import tempfile
import unittest

import yaml

from qdl.consumer import (
    RealtimeRoute,
    ReleaseRouteObservation,
    StableReleaseRoutePlan,
    evaluate_release_readiness,
)
from qdl.runtime.production_catalog import ProductionDemandManifest
from qdl.runtime.stable_catalog import StableSourceCatalog


ROOT = Path(__file__).resolve().parents[1]
ROUTE_PATH = ROOT / "config/v2/stable-v2-release-routing.yaml"


class StableReleaseRoutePlanTests(unittest.TestCase):
    def load(self) -> StableReleaseRoutePlan:
        return StableReleaseRoutePlan.load(ROUTE_PATH, manifest_root=ROOT)

    def test_release_scope_is_per_requirement_and_fails_closed(self):
        plan = self.load()
        self.assertEqual(plan.contract_version, "2.0.0")
        self.assertEqual(plan.v1_fallback.release_tag, "v1.2.4")
        self.assertEqual(
            plan.v1_fallback.source_commit,
            "2b0dcf74454c9f87c352d3c47389955aeb955804",
        )
        self.assertEqual(len(plan.consumers), 5)
        self.assertEqual(len(plan.products()), 98)
        self.assertEqual(
            {
                consumer.consumer_id: len(consumer.products)
                for consumer in plan.consumers
            },
            {
                "monitoring.multivenue.stable": 5,
                "trading-system.paper.stable": 61,
                "alpha.binance.paper.stable": 15,
                "alpha.okx.paper.stable": 15,
                "alpha.vn.paper.stable": 2,
            },
        )
        for consumer in plan.consumers:
            with self.subTest(consumer_id=consumer.consumer_id):
                self.assertEqual(
                    {item.requirement_key for item in consumer.products},
                    {
                        ":".join((
                            item.instrument_uid,
                            item.feed.value,
                            item.interval or "",
                            item.source_policy_id,
                        ))
                        for item in consumer.manifest.requirements
                    },
                )
                self.assertEqual(consumer.demand_revision, plan.crypto_demand.revision)

        products = {
            (consumer_id, item.requirement_key): item
            for consumer_id, item in plan.products()
        }
        self.assertEqual(
            products[(
                "trading-system.paper.stable",
                "a953e16e-7138-5562-b5e8-c337a44d0b65:QUOTE::crypto_primary_v2",
            )].fallback,
            "BLOCKED",
        )
        vn_products = [
            item for _consumer_id, item in plan.products()
            if "vn_primary_v2" in item.requirement_key
        ]
        self.assertTrue(vn_products)
        self.assertTrue(all(item.route == "V1_PRIMARY" for item in vn_products))
        self.assertTrue(all(item.fallback == "NONE" for item in vn_products))

    def test_artifact_manifest_and_route_mutations_are_rejected(self):
        payload = yaml.safe_load(ROUTE_PATH.read_text(encoding="utf-8"))

        unknown = copy.deepcopy(payload)
        unknown["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
            StableReleaseRoutePlan.from_mapping(unknown, manifest_root=ROOT)

        changed_catalog = copy.deepcopy(payload)
        changed_catalog["source_catalog"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "checksum differs"):
            StableReleaseRoutePlan.from_mapping(changed_catalog, manifest_root=ROOT)

        missing_product = copy.deepcopy(payload)
        missing_product["consumers"][0]["products"].pop()
        with self.assertRaisesRegex(ValueError, "differ from consumer manifest"):
            StableReleaseRoutePlan.from_mapping(missing_product, manifest_root=ROOT)

        incompatible_v1 = copy.deepcopy(payload)
        product = next(
            item
            for consumer in incompatible_v1["consumers"]
            if consumer["consumer_id"] == "alpha.binance.paper.stable"
            for item in consumer["products"]
            if ":BAR:1m:" in item["requirement_key"]
        )
        product["fallback"] = "V1"
        product["reason"] = None
        with self.assertRaisesRegex(ValueError, "lacks proven binding compatibility"):
            StableReleaseRoutePlan.from_mapping(incompatible_v1, manifest_root=ROOT)

        vn_primary = copy.deepcopy(payload)
        product = vn_primary["consumers"][3]["products"][0]
        product["route"] = "V2_PRIMARY"
        product["fallback"] = "BLOCKED"
        product["reason"] = "TEST_ONLY"
        with self.assertRaisesRegex(ValueError, "certified Binance/OKX"):
            StableReleaseRoutePlan.from_mapping(vn_primary, manifest_root=ROOT)

    def test_materialized_v2_product_must_remain_in_declared_demand(self):
        payload = yaml.safe_load(ROUTE_PATH.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            shutil.copytree(ROOT / "config", temporary_root / "config")
            shutil.copytree(ROOT / "consumers", temporary_root / "consumers")
            demand_path = temporary_root / "config/v2/stable-crypto-demand.yaml"
            demand = yaml.safe_load(demand_path.read_text(encoding="utf-8"))
            requirements = demand["consumers"][0]["requirements"]
            removed = next(
                item for item in requirements
                if item["venue"] == "BINANCE"
                and item["market"] == "USDM"
                and item["native_symbol"] == "BTCUSDT"
                and item["feed"] == "BAR"
                and item["interval"] == "1m"
            )
            requirements.remove(removed)
            demand_path.write_text(
                yaml.safe_dump(demand, sort_keys=False), encoding="utf-8"
            )
            payload["crypto_demand"]["sha256"] = hashlib.sha256(
                demand_path.read_bytes()
            ).hexdigest()
            with self.assertRaisesRegex(
                ValueError,
                "materialized V2 release product is absent from crypto demand",
            ):
                StableReleaseRoutePlan.from_mapping(
                    payload,
                    manifest_root=temporary_root,
                )

    def test_digest_is_deterministic_and_binds_every_referenced_revision(self):
        first = self.load()
        second = self.load()
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(len(first.digest), 64)
        self.assertEqual(
            first.source_catalog.revision,
            StableSourceCatalog.load(
                ROOT / "config/v2/stable-source-bindings.yaml"
            ).catalog_revision,
        )
        self.assertEqual(
            first.crypto_demand.revision,
            ProductionDemandManifest.load_many([
                ROOT / "config/v2/stable-crypto-demand.yaml"
            ]).revision,
        )
        self.assertEqual(first.capability_matrix.revision, 5)
        self.assertEqual(first.resource_budget.max_consumer_lag, 10_000)
        self.assertEqual(first.resource_budget.max_cpu_millicores, 750)
        self.assertEqual(first.resource_budget.max_rss_bytes, 805_306_368)


class StableReleaseReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = StableReleaseRoutePlan.load(ROUTE_PATH, manifest_root=ROOT)

    def observations(self) -> tuple[ReleaseRouteObservation, ...]:
        result = []
        for consumer_id, product in self.plan.products():
            route = (
                RealtimeRoute.V1_PRIMARY.value
                if product.route == "V1_PRIMARY"
                else RealtimeRoute.V2_PRIMARY.value
            )
            result.append(ReleaseRouteObservation(
                consumer_id=consumer_id,
                requirement_key=product.requirement_key,
                route=route,
                reason=product.reason if route == "V1_PRIMARY" else "V2_READY",
                v2_source_age_ms=None if route == "V1_PRIMARY" else 10,
                v2_receive_age_ms=None if route == "V1_PRIMARY" else 12,
                v2_gap_open=False,
                v1_source_age_ms=None,
                v1_receive_age_ms=None,
                consumer_lag=3,
                cpu_millicores=250,
                rss_bytes=64 * 1024 * 1024,
            ))
        return tuple(result)

    def test_ready_degraded_and_blocked_states_are_explicit(self):
        observations = self.observations()
        ready = evaluate_release_readiness(self.plan, observations)
        self.assertEqual(ready.status, "READY")
        self.assertTrue(ready.ready)
        self.assertEqual(ready.blocked_count, 0)
        self.assertEqual(ready.fallback_count, 0)

        fallback_index = next(
            index
            for index, (consumer_id, product) in enumerate(self.plan.products())
            if product.route == "V2_PRIMARY" and product.fallback == "V1"
            and observations[index].consumer_id == consumer_id
        )
        fallback_observations = list(observations)
        fallback_observations[fallback_index] = replace(
            fallback_observations[fallback_index],
            route=RealtimeRoute.V1_FALLBACK.value,
            reason="V2_DATA_STALE",
            v2_source_age_ms=999_999,
            v1_source_age_ms=10,
        )
        degraded = evaluate_release_readiness(self.plan, fallback_observations)
        self.assertEqual(degraded.status, "DEGRADED")
        self.assertFalse(degraded.ready)
        self.assertEqual(degraded.fallback_count, 1)
        self.assertGreater(degraded.fallback_rate, 0.0)

        blocked_index = next(
            index
            for index, (_consumer_id, product) in enumerate(self.plan.products())
            if product.route == "V2_PRIMARY" and product.fallback == "BLOCKED"
        )
        blocked_observations = list(observations)
        blocked_observations[blocked_index] = replace(
            blocked_observations[blocked_index],
            route=RealtimeRoute.BLOCKED.value,
            reason="V2_OPEN_SEQUENCE_GAP",
            v2_gap_open=True,
        )
        blocked = evaluate_release_readiness(self.plan, blocked_observations)
        self.assertEqual(blocked.status, "NOT_READY")
        self.assertFalse(blocked.ready)
        self.assertEqual(blocked.blocked_count, 1)

        over_budget_observations = list(observations)
        over_budget_observations[0] = replace(
            over_budget_observations[0], consumer_lag=10_001
        )
        over_budget = evaluate_release_readiness(self.plan, over_budget_observations)
        self.assertEqual(over_budget.status, "NOT_READY")
        self.assertIn(
            "CONSUMER_LAG:monitoring.multivenue.stable",
            over_budget.budget_violations,
        )

    def test_v1_exclusion_cannot_claim_freshness_or_change_reason(self):
        observations = list(self.observations())
        excluded_index = next(
            index
            for index, (_consumer_id, product) in enumerate(self.plan.products())
            if product.route == "V1_PRIMARY"
        )
        with self.assertRaisesRegex(ValueError, "explicit V2 exclusion"):
            evaluate_release_readiness(
                self.plan,
                tuple(replace(
                    observations[excluded_index], v1_source_age_ms=20
                ) if index == excluded_index else observation
                    for index, observation in enumerate(observations)),
            )
        with self.assertRaisesRegex(ValueError, "explicit V2 exclusion"):
            evaluate_release_readiness(
                self.plan,
                tuple(replace(
                    observations[excluded_index], reason="V1_EXCLUDED"
                ) if index == excluded_index else observation
                    for index, observation in enumerate(observations)),
            )

    def test_observation_set_cannot_hide_a_missing_or_duplicate_product(self):
        observations = self.observations()
        with self.assertRaisesRegex(ValueError, "differ from frozen manifest"):
            evaluate_release_readiness(self.plan, observations[:-1])
        with self.assertRaisesRegex(ValueError, "differ from frozen manifest"):
            evaluate_release_readiness(self.plan, observations + (observations[0],))


if __name__ == "__main__":
    unittest.main()

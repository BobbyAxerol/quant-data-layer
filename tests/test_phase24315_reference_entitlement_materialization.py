from __future__ import annotations

from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest

import yaml

from qdl.consumer import StablePrimaryConsumerRoutePlan, StableReleaseRoutePlan, requirement_key
from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.query import ConsumerGrade, FeedType, RecoveryPolicy
from qdl.runtime.stable_catalog import StableSourceCatalog


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/phase24315_materialize_alpha_reference_entitlements.py"


def _module():
    spec = importlib.util.spec_from_file_location("phase24315_reference", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Phase24315ReferenceEntitlementMaterializationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tool = _module()
        cls.reference = yaml.safe_load(
            (ROOT / "consumers/stable/reference-l2-stable.yaml").read_text(encoding="utf-8")
        )
        cls.manifests = {
            consumer_id: yaml.safe_load((ROOT / "consumers/stable" / filename).read_text(encoding="utf-8"))
            for consumer_id, filename in cls.tool._TARGETS.items()
        }
        cls.route = yaml.safe_load(
            (ROOT / "config/v2/stable-v2-release-routing.yaml").read_text(encoding="utf-8")
        )
        cls.primary = yaml.safe_load(
            (ROOT / "config/v2/stable-primary-consumer-routing.yaml").read_text(encoding="utf-8")
        )

    def _before_materialization(self):
        manifests = deepcopy(self.manifests)
        for payload in manifests.values():
            payload["spec"]["requirements"] = [
                item for item in payload["spec"]["requirements"]
                if not self.tool._is_managed_reference(item)
            ]
            payload["metadata"]["revision"] -= 1
        route = deepcopy(self.route)
        route["revision"] -= 1
        for consumer in route["consumers"]:
            consumer_id = consumer["consumer_id"]
            if consumer_id not in manifests:
                continue
            manifest = ConsumerManifestLoader.from_mapping(manifests[consumer_id])
            old_keys = {requirement_key(item) for item in manifest.requirements}
            consumer["manifest_revision"] = manifest.manifest_revision
            consumer["manifest_sha256"] = manifest.manifest_sha256
            consumer["products"] = [
                item for item in consumer["products"]
                if item["requirement_key"] in old_keys
            ]
        primary = deepcopy(self.primary)
        primary["revision"] -= 1
        return manifests, route, primary

    def test_exact_provider_supported_reference_products_are_materialized_and_idempotent(self) -> None:
        before_manifests, before_route, before_primary = self._before_materialization()
        manifests, route, primary, summary = self.tool.build_documents(
            reference_manifest=self.reference,
            alpha_manifests=before_manifests,
            release_route=before_route,
            primary_route=before_primary,
        )
        self.assertEqual(manifests, self.manifests)
        self.assertEqual(route, self.route)
        self.assertEqual(primary, self.primary)
        self.assertTrue(summary["manifest_changed"])
        self.assertTrue(summary["release_route_changed"])
        self.assertEqual(summary["alpha_reference_counts"], {
            "alpha.binance.paper.stable": 35,
            "alpha.okx.paper.stable": 20,
        })

        rerun = self.tool.build_documents(
            reference_manifest=self.reference,
            alpha_manifests=manifests,
            release_route=route,
            primary_route=primary,
        )
        self.assertEqual(rerun[0], manifests)
        self.assertEqual(rerun[1], route)
        self.assertEqual(rerun[2], primary)
        self.assertFalse(rerun[3]["manifest_changed"])
        self.assertFalse(rerun[3]["release_route_changed"])

    def test_alpha_reference_contract_is_typed_bounded_and_not_execution_authority(self) -> None:
        expected = {
            "alpha.binance.paper.stable": {
                FeedType.BASIS, FeedType.CONTRACT_METADATA, FeedType.FUNDING_RATE,
                FeedType.LONG_SHORT_RATIO, FeedType.MARK_INDEX_PRICE,
                FeedType.OPEN_INTEREST, FeedType.TAKER_FLOW,
            },
            "alpha.okx.paper.stable": {
                FeedType.CONTRACT_METADATA, FeedType.FUNDING_RATE,
                FeedType.MARK_INDEX_PRICE, FeedType.OPEN_INTEREST,
            },
        }
        for consumer_id, payload in self.manifests.items():
            manifest = ConsumerManifestLoader.from_mapping(payload)
            reference = [
                item for item in manifest.requirements
                if item.feed in expected[consumer_id]
            ]
            self.assertEqual({item.feed for item in reference}, expected[consumer_id])
            self.assertEqual({item.consumer_grade for item in reference}, {ConsumerGrade.ALPHA})
            self.assertEqual({item.recovery for item in reference}, {RecoveryPolicy.FRESH_SNAPSHOT})
            self.assertTrue(all(not item.require_final_bars for item in reference))
            self.assertTrue(all(item.stale_policy.value == "BLOCK" for item in reference))
            self.assertEqual(manifest.execution_dependency, "FORBIDDEN")

        catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
        for payload in self.manifests.values():
            manifest = ConsumerManifestLoader.from_mapping(payload)
            for requirement in manifest.requirements:
                if requirement.feed is not FeedType.FUNDING_RATE:
                    continue
                venue = catalog.instrument_for(requirement.instrument_uid).identity.venue
                self.assertEqual(
                    requirement.max_freshness_ms,
                    32_400_000 if venue == "BINANCE" else 28_800_000,
                )

        expected_reference_keys = {
            (consumer_id, requirement_key(requirement))
            for consumer_id, payload in self.manifests.items()
            for requirement in ConsumerManifestLoader.from_mapping(payload).requirements
            if requirement.feed in expected[consumer_id]
        }
        release = StableReleaseRoutePlan.load(
            ROOT / "config/v2/stable-v2-release-routing.yaml", manifest_root=ROOT
        )
        routes = {
            (consumer_id, product.requirement_key): product
            for consumer_id, product in release.products()
            if (consumer_id, product.requirement_key) in expected_reference_keys
        }
        self.assertEqual(set(routes), expected_reference_keys)
        self.assertTrue(all(item.route == "V2_PRIMARY" for item in routes.values()))
        self.assertTrue(all(item.fallback == "BLOCKED" for item in routes.values()))
        self.assertTrue(all(item.reason == "V1_REFERENCE_EQUIVALENCE_UNPROVEN" for item in routes.values()))
        StablePrimaryConsumerRoutePlan.load(
            ROOT / "config/v2/stable-primary-consumer-routing.yaml",
            manifest_root=ROOT,
            catalog=StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml"),
        )


if __name__ == "__main__":
    unittest.main()

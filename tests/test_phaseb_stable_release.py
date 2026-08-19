from __future__ import annotations

import copy
import json
import tempfile
import tomllib
import unittest
from pathlib import Path

import yaml

import qdl_sdk
from qdl.consumer.stable import StableConsumerMigrationPlan
from qdl.runtime.stable_catalog import StableSourceCatalog
from scripts.generate_phase5_openapi import build_openapi


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
MIGRATION_PATH = ROOT / "config/v2/stable-consumer-migration.yaml"


class StableConsumerMigrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)

    def load(self) -> StableConsumerMigrationPlan:
        return StableConsumerMigrationPlan.load(
            MIGRATION_PATH,
            manifest_root=ROOT,
            catalog=self.catalog,
        )

    def test_five_real_consumer_manifests_are_catalog_bound_and_fail_closed(self):
        plan = self.load()
        self.assertEqual(plan.contract_version, "2.0.0")
        self.assertEqual(plan.authority, "V1")
        self.assertEqual(plan.target_route, "V1_WITH_V2_SHADOW")
        self.assertEqual(len(plan.consumers), 5)
        self.assertEqual(
            {item.consumer_id for item in plan.consumers},
            {
                "monitoring.multivenue.stable",
                "alpha.binance.paper.stable",
                "alpha.okx.paper.stable",
                "alpha.vn.paper.stable",
                "trading-system.paper.stable",
            },
        )
        for item in plan.consumers:
            with self.subTest(consumer_id=item.consumer_id):
                self.assertEqual(item.state, "SHADOW")
                self.assertEqual(item.rollback_route, "V1")
                self.assertFalse(item.cutover_authorized)
                self.assertEqual(item.manifest.sdk_major, 2)
                self.assertEqual(item.manifest.rollback_contract, "V1")
                self.assertEqual(item.manifest.environment, "paper")
                for requirement in item.manifest.requirements:
                    self.assertIsNotNone(self.catalog.binding_for(requirement))

    def test_unknown_fields_active_route_and_unknown_binding_fail_closed(self):
        payload = yaml.safe_load(MIGRATION_PATH.read_text(encoding="utf-8"))
        unknown = copy.deepcopy(payload)
        unknown["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "incomplete or unknown"):
            StableConsumerMigrationPlan.from_mapping(
                unknown, manifest_root=ROOT, catalog=self.catalog
            )

        active = copy.deepcopy(payload)
        active["consumers"][0]["state"] = "ACTIVE"
        active["consumers"][0]["cutover_authorized"] = True
        with self.assertRaisesRegex(ValueError, "not fail-closed"):
            StableConsumerMigrationPlan.from_mapping(
                active, manifest_root=ROOT, catalog=self.catalog
            )

        manifest_path = ROOT / "consumers/stable/alpha-binance-paper.yaml"
        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        manifest["spec"]["requirements"][0]["instrument_uid"] = "unknown"
        with tempfile.TemporaryDirectory(prefix="qdl-phaseb-manifest-") as directory:
            root = Path(directory)
            temporary = root / "consumers/stable/invalid.yaml"
            temporary.parent.mkdir(parents=True)
            temporary.write_text(
                yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8"
            )
            invalid = copy.deepcopy(payload)
            invalid["consumers"] = [invalid["consumers"][1]]
            invalid["consumers"][0]["manifest"] = (
                "/app/consumers/stable/invalid.yaml"
            )
            with self.assertRaisesRegex(KeyError, "no stable source binding"):
                StableConsumerMigrationPlan.from_mapping(
                    invalid, manifest_root=root, catalog=self.catalog
                )

    def test_trading_system_is_the_only_paper_execution_dependency(self):
        plan = self.load()
        policies = {
            item.consumer_id: item.manifest.execution_dependency
            for item in plan.consumers
        }
        self.assertEqual(policies["trading-system.paper.stable"], "PAPER_ONLY")
        self.assertEqual(
            {value for key, value in policies.items() if key != "trading-system.paper.stable"},
            {"FORBIDDEN"},
        )


class StableReleaseVersionContractTests(unittest.TestCase):
    def test_package_sdk_and_openapi_are_exactly_2_0_0(self):
        package = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        snapshot = json.loads(
            (ROOT / "contracts/v2/openapi.snapshot.json").read_text(encoding="utf-8")
        )
        generated = build_openapi()
        self.assertEqual(package["project"]["version"], "2.0.0")
        self.assertEqual(qdl_sdk.__version__, "2.0.0")
        self.assertEqual(generated["info"]["version"], "2.0.0")
        self.assertEqual(snapshot, generated)
        self.assertEqual(len(generated["paths"]), 10)
        self.assertEqual(len(generated["components"]["schemas"]), 42)


if __name__ == "__main__":
    unittest.main()

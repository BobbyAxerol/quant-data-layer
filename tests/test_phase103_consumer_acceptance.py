from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

import yaml

from qdl.certification.phase103_consumer_acceptance import (
    DeliveryClass,
    PHASE103_CONSUMER_IDS,
    build_consumer_acceptance_scope,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
TRADING_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"
ALPHA_MANIFEST = ROOT / "consumers/stable/alpha-binance-paper.yaml"


class Phase103ConsumerAcceptanceScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = StableSourceCatalog.load(CATALOG_PATH)
        cls.acquisition = StableAcquisitionPlan.load(
            ACQUISITION_PATH,
            catalog=cls.catalog,
        )

    def scope(self, *paths: Path, acquisition: StableAcquisitionPlan | None = None):
        return build_consumer_acceptance_scope(
            paths or (TRADING_MANIFEST, ALPHA_MANIFEST),
            catalog=self.catalog,
            acquisition=acquisition or self.acquisition,
        )

    def test_governed_manifests_cover_exact_crypto_products_and_vn_is_explicitly_deferred(self):
        scope = self.scope()
        self.assertEqual(
            {item.consumer_id for item in scope.products},
            PHASE103_CONSUMER_IDS,
        )
        self.assertEqual(len(scope.products), 18)
        self.assertEqual(
            sum(item.delivery is DeliveryClass.DURABLE for item in scope.products),
            16,
        )
        pass_through = [
            item
            for item in scope.products
            if item.delivery is DeliveryClass.PROVIDER_PASS_THROUGH
        ]
        self.assertEqual(len(pass_through), 2)
        self.assertTrue(
            all(
                item.consumer_id == "alpha.binance.paper.stable"
                and item.feed.value == "BAR"
                and item.interval == "15m"
                and item.binding_id is None
                for item in pass_through
            )
        )
        self.assertEqual(len(scope.excluded), 1)
        excluded = scope.excluded[0]
        self.assertEqual(excluded.consumer_id, "trading-system.paper.stable")
        self.assertEqual(excluded.reason, "VENUE_NOT_IN_PHASE103_CRYPTO_SCOPE")
        self.assertEqual(excluded.feed.value, "TRADE")

    def test_scope_digest_is_deterministic_and_evidence_contains_no_market_payload(self):
        first = self.scope()
        second = self.scope(ALPHA_MANIFEST, TRADING_MANIFEST)
        self.assertEqual(first.sha256, second.sha256)
        evidence = first.evidence()
        self.assertEqual(evidence["scope_sha256"], first.sha256)
        self.assertNotIn("payload", str(evidence).lower())
        self.assertNotIn("secret", str(evidence).lower())
        self.assertNotIn("raw_frame", str(evidence).lower())

    def test_missing_or_foreign_manifest_set_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "requires exactly"):
            self.scope(TRADING_MANIFEST)
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-acceptance-") as directory:
            foreign = Path(directory) / "foreign.yaml"
            payload = yaml.safe_load(ALPHA_MANIFEST.read_text(encoding="utf-8"))
            payload["metadata"]["id"] = "alpha.unapproved.paper"
            foreign.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "requires exactly"):
                self.scope(TRADING_MANIFEST, foreign)

    def test_disabled_durable_binding_fails_closed(self):
        disabled_id = "binance-usdm-btcusdt-trade"
        acquisition = StableAcquisitionPlan(
            schema=self.acquisition.schema,
            revision=self.acquisition.revision,
            raw_topic=self.acquisition.raw_topic,
            canonical_topic=self.acquisition.canonical_topic,
            quarantine_topic=self.acquisition.quarantine_topic,
            bindings=tuple(
                replace(item, enabled=False) if item.binding_id == disabled_id else item
                for item in self.acquisition.bindings
            ),
        )
        with self.assertRaisesRegex(ValueError, "disabled acquisition"):
            self.scope(acquisition=acquisition)

    def test_missing_durable_binding_and_wrong_policy_fail_closed(self):
        missing_binding = "binance-usdm-btcusdt-trade"
        catalog = StableSourceCatalog(
            canonical_stream=self.catalog.canonical_stream,
            bindings=tuple(
                item for item in self.catalog.bindings if item.binding_id != missing_binding
            ),
            catalog_revision=self.catalog.catalog_revision,
            source_policy_revision=self.catalog.source_policy_revision,
            authority_revision=self.catalog.authority_revision,
            instruments=self.catalog.instruments,
        )
        with self.assertRaisesRegex(ValueError, "neither a durable binding"):
            build_consumer_acceptance_scope(
                (TRADING_MANIFEST, ALPHA_MANIFEST),
                catalog=catalog,
                acquisition=self.acquisition,
            )

        with tempfile.TemporaryDirectory(prefix="qdl-phase103-acceptance-") as directory:
            changed = Path(directory) / "alpha.yaml"
            payload = yaml.safe_load(ALPHA_MANIFEST.read_text(encoding="utf-8"))
            payload["spec"]["requirements"][0]["source_policy_id"] = "wrong_policy"
            changed.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unapproved source policy"):
                self.scope(TRADING_MANIFEST, changed)


if __name__ == "__main__":
    unittest.main()

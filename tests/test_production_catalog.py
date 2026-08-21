from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionPlan,
)


BINANCE = {
    "serverTime": 1000,
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "contractType": "PERPETUAL",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "deliveryDate": 0,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ],
        },
        {
            "symbol": "ETHUSDT",
            "status": "TRADING",
            "contractType": "PERPETUAL",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "deliveryDate": 0,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ],
        },
        {
            "symbol": "BTCUSDT_260925",
            "status": "TRADING",
            "contractType": "CURRENT_QUARTER",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "deliveryDate": 1789948800000,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            ],
        },
    ],
}
OKX = [
    {
        "instType": "SWAP",
        "instId": "BTC-USDT-SWAP",
        "instFamily": "BTC-USDT",
        "baseCcy": "BTC",
        "quoteCcy": "USDT",
        "settleCcy": "USDT",
        "tickSz": "0.1",
        "lotSz": "1",
        "ctVal": "0.01",
        "ctMult": "1",
        "state": "live",
    },
    {
        "instType": "SWAP",
        "instId": "ETH-USDT-SWAP",
        "instFamily": "ETH-USDT",
        "baseCcy": "",
        "quoteCcy": "",
        "ctValCcy": "ETH",
        "settleCcy": "USDT",
        "tickSz": "0.01",
        "lotSz": "1",
        "ctVal": "0.1",
        "ctMult": "1",
        "state": "live",
    },
    {
        "instType": "SWAP",
        "instId": "PREOPEN-USDT-SWAP",
        "instFamily": "",
        "state": "preopen",
    },
]


class ProductionCatalogTests(unittest.TestCase):
    def _manifest(self, root: Path, *, conflicting=False) -> ProductionDemandManifest:
        policy = "other_policy" if conflicting else "crypto_primary_v2"
        payload = {
            "schema": "qdl.v2.production-demand.v1",
            "revision": 5,
            "consumers": [
                {
                    "consumer_id": "trading-system.execution.v2",
                    "consumer_grade": "EXECUTION",
                    "requirements": [
                        {
                            "venue": "BINANCE", "market": "USDM",
                            "product_type": "PERPETUAL", "native_symbol": "BTCUSDT",
                            "feed": "TRADE", "interval": None,
                            "source_policy_id": "crypto_primary_v2",
                        },
                        {
                            "venue": "OKX", "market": "SWAP",
                            "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP",
                            "feed": "BAR", "interval": "1m",
                            "source_policy_id": "crypto_primary_v2",
                        },
                        {
                            "venue": "BINANCE", "market": "USDM",
                            "product_type": "PERPETUAL", "native_symbol": "ETHUSDT",
                            "feed": "TRADE", "interval": None,
                            "source_policy_id": "crypto_primary_v2",
                        },
                        {
                            "venue": "OKX", "market": "SWAP",
                            "product_type": "PERPETUAL", "native_symbol": "ETH-USDT-SWAP",
                            "feed": "BAR", "interval": "1m",
                            "source_policy_id": "crypto_primary_v2",
                        },
                    ],
                },
                {
                    "consumer_id": "alpha.shared.v2",
                    "consumer_grade": "ALPHA",
                    "requirements": [
                        {
                            "venue": "BINANCE", "market": "USDM",
                            "product_type": "PERPETUAL", "native_symbol": "ETHUSDT",
                            "feed": "TRADE", "interval": None,
                            "source_policy_id": policy,
                        }
                    ],
                },
            ],
        }
        path = root / ("conflict.yaml" if conflicting else "demand.yaml")
        import yaml
        path.write_text(yaml.safe_dump(payload, sort_keys=False))
        return ProductionDemandManifest.load_many([path])

    def test_binance_identity_uses_canonical_pair_and_dated_contract_code(self):
        discovery = parse_exchange_info(BINANCE, valid_from_ns=0)
        by_native = {item.native_symbol: item for item in discovery.records}
        self.assertEqual(
            by_native["ETHUSDT"].instrument_id,
            "BINANCE.USDM.PERPETUAL.ETH-USDT",
        )
        self.assertEqual(
            by_native["BTCUSDT_260925"].instrument_id,
            "BINANCE.USDM.FUTURE.BTC-USDT-260925",
        )

    def test_generator_is_deterministic_validated_and_metadata_authentic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            demand = self._manifest(root)
            builder = ProductionCatalogBuilder(
                catalog_revision=8,
                source_policy_revision=3,
                authority_revision=11,
            )
            first = builder.build(
                demand=demand,
                binance_usdm=parse_exchange_info(BINANCE, valid_from_ns=99),
                okx_rows=OKX,
                metadata_provenance={"capture": "a" * 64},
            )
            second = builder.build(
                demand=demand,
                binance_usdm=parse_exchange_info(BINANCE, valid_from_ns=123),
                okx_rows=list(reversed(OKX)),
                metadata_provenance={"capture": "a" * 64},
            )
            self.assertEqual(first.source_catalog, second.source_catalog)
            self.assertEqual(first.acquisition_plan, second.acquisition_plan)
            self.assertEqual(first.provenance["fabricated_metadata"], False)
            self.assertEqual(first.provenance["instrument_count"], 4)
            paths = first.write(root / "out")
            catalog = StableSourceCatalog.load(paths["source_catalog"])
            self.assertEqual(len(catalog.bindings), 4)
            self.assertEqual(
                {item.instrument.instrument_id for item in catalog.bindings},
                {
                    "BINANCE.USDM.PERPETUAL.BTC-USDT",
                    "BINANCE.USDM.PERPETUAL.ETH-USDT",
                    "OKX.SWAP.PERPETUAL.BTC-USDT",
                    "OKX.SWAP.PERPETUAL.ETH-USDT",
                },
            )
            eth_okx = next(
                item.instrument
                for item in catalog.bindings
                if item.instrument.instrument_id == "OKX.SWAP.PERPETUAL.ETH-USDT"
            )
            self.assertEqual(
                (eth_okx.base_asset, eth_okx.quote_asset, eth_okx.settlement_asset),
                ("ETH", "USDT", "USDT"),
            )
            self.assertEqual(
                json.loads(Path(paths["provenance"]).read_text())["binding_count"], 4
            )
            acquisition = StableAcquisitionPlan.load(
                paths["acquisition_plan"], catalog=catalog
            )
            raw_authority = {
                "schema": "qdl.authority-record.v1",
                "slice_id": "qdl-v2-production-acquisition",
                "revision": 11,
                "mode": "RUST_SHADOW",
                "candidate_image_digest": "sha256:" + "1" * 64,
                "capability_manifest_digest": "2" * 64,
                "contract_digest": "3" * 64,
                "partition_plan_digest": "4" * 64,
                "public_write_allowed": False,
                "legacy_write_allowed": False,
                "approved_by": "production-catalog-test",
                "effective_at_ns": 1,
            }
            promotion_scope = AuthorityPromotionScope(
                schema="qdl.v2.authority-promotion-scope.v1",
                revision=1,
                binding_ids=tuple(item.binding_id for item in catalog.bindings),
            )
            runtime = acquisition.production_core_config(
                catalog=catalog,
                raw_authority=raw_authority,
                promotion_scope=promotion_scope,
                worker_index=1,
            )
            self.assertEqual(len(runtime["slices"]), 4)
            self.assertEqual(
                {item["subscription_id"] for item in runtime["slices"]},
                {item.source_id for item in catalog.bindings},
            )
            self.assertEqual(runtime["topics"]["primary_canonical"], "md.canonical.v2")
            self.assertEqual(runtime["topics"]["authority_control"], "qdl.authority.v1")
            self.assertEqual(runtime["batch_size"], 128)

    def test_conflicts_missing_metadata_and_uncertified_interval_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "conflicting source policies"):
                self._manifest(root, conflicting=True)
            demand = self._manifest(root)
            with self.assertRaisesRegex(ValueError, "metadata missing"):
                ProductionCatalogBuilder(
                    catalog_revision=1,
                    source_policy_revision=1,
                    authority_revision=1,
                ).build(demand=demand, binance_usdm=None, okx_rows=[])
            payload = {
                "schema": "qdl.v2.production-demand.v1",
                "revision": 1,
                "consumers": [{
                    "consumer_id": "alpha", "consumer_grade": "ALPHA",
                    "requirements": [{
                        "venue": "OKX", "market": "SWAP", "product_type": "PERPETUAL",
                        "native_symbol": "ETH-USDT-SWAP", "feed": "BAR",
                        "interval": "15m", "source_policy_id": "crypto_primary_v2",
                    }],
                }],
            }
            import yaml
            path = root / "bad-interval.yaml"
            path.write_text(yaml.safe_dump(payload))
            with self.assertRaisesRegex(ValueError, "currently certified for 1m"):
                ProductionDemandManifest.load_many([path])


if __name__ == "__main__":
    unittest.main()

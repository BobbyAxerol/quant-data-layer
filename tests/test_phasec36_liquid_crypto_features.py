"""C3.6-B source-only tests for liquid reference and L2 declarations."""

from __future__ import annotations

import unittest
from pathlib import Path

from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
from qdl.demand import (
    CapabilityRegistry,
    DataRequirement,
    DemandFeed,
    DemandPurpose,
    LiquidCryptoFeaturePolicy,
    UniverseSelector,
    UniverseSelectorKind,
    build_l2_feature_requirements,
    reference_feeds_for,
    select_liquid_crypto_feature_set,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentStatus,
    ProductType,
)
from qdl.reference.batch import ReferenceBatch
from qdl.reference.contracts import ReferenceProduct, ReferenceRequest, ReferenceStatus
from qdl.reference.runtime import build_default_reference_runtime
from qdl.runtime.l2_demand import L2ProviderBinding


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config/v2/c36-liquid-crypto-feature-policy.yaml"


def _record(
    *,
    venue: str,
    market: str,
    product_type: ProductType,
    native_symbol: str,
    base: str,
    settlement: str = "USDT",
    canonical_suffix: str = "",
    attributes: dict[str, str] | None = None,
) -> InstrumentRecord:
    canonical = f"{base}-{settlement}{canonical_suffix}"
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue=venue,
            market=market,
            product_type=product_type,
            canonical_symbol=canonical,
        ),
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=native_symbol,
        base_asset=base,
        quote_asset=settlement,
        settlement_asset=settlement,
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
        status=InstrumentStatus.ACTIVE,
        expiry_time_ns=(1_900_000_000_000_000_000 if product_type is ProductType.FUTURE else None),
        attributes=attributes or {},
    )


def _records() -> tuple[InstrumentRecord, ...]:
    values: list[InstrumentRecord] = []
    for base in ("BTC", "ETH", "SOL", "DOGE", "BNB"):
        values.append(_record(
            venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
            native_symbol=f"{base}USDT", base=base,
            attributes={"contractType": "PERPETUAL"},
        ))
        values.append(_record(
            venue="OKX", market="SWAP", product_type=ProductType.PERPETUAL,
            native_symbol=f"{base}-USDT-SWAP", base=base,
            attributes={"instFamily": f"{base}-USDT"},
        ))
    for base in ("BTC", "ETH"):
        for contract_type, suffix in (("CURRENT_QUARTER", "-260925"), ("NEXT_QUARTER", "-261225")):
            values.append(_record(
                venue="BINANCE", market="USDM", product_type=ProductType.FUTURE,
                native_symbol=f"{base}USDT_{suffix.removeprefix('-')}", base=base,
                canonical_suffix=suffix,
                attributes={"contractType": contract_type},
            ))
    # Two BTC provider families demonstrate that dated discovery preserves all
    # exact metadata identities instead of pretending one settlement is universal.
    for base, family, settlement in (
        ("BTC", "BTC-USD", "USD"),
        ("BTC", "BTC-USD_UM", "USDT"),
        ("ETH", "ETH-USD", "USD"),
    ):
        for alias, suffix in (("quarter", "260925"), ("next_quarter", "261225")):
            values.append(_record(
                venue="OKX", market="FUTURES", product_type=ProductType.FUTURE,
                native_symbol=f"{family}-{suffix}", base=base, settlement=settlement,
                canonical_suffix=f"-{suffix}",
                attributes={"alias": alias, "instFamily": family},
            ))
    return tuple(values)


class _OkxFutureMetadataClient:
    async def get(self, path, *, params, bucket, attempts=3):
        del bucket, attempts
        if path != "/api/v5/public/instruments":
            raise AssertionError(f"unexpected endpoint: {path}")
        assert params == {"instType": "FUTURES", "instId": "BTC-USD-260925"}
        return [{
            "instId": "BTC-USD-260925",
            "instType": "FUTURES",
            "instFamily": "BTC-USD",
            "tickSz": "0.1",
            "lotSz": "1",
            "minSz": "1",
            "ctVal": "100",
            "ctMult": "1",
            "expTime": "1790000000000",
            "state": "live",
        }]


class PhaseC36LiquidCryptoFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = LiquidCryptoFeaturePolicy.load(POLICY_PATH)
        self.records = _records()

    def test_policy_and_five_perpetuals_are_exact_and_venue_separated(self):
        usdc_duplicate = _record(
            venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
            native_symbol="BTCUSDC", base="BTC", settlement="USDC",
            attributes={"contractType": "PERPETUAL"},
        )
        feature_set = select_liquid_crypto_feature_set(
            (*self.records, usdc_duplicate), policy=self.policy
        )
        self.assertEqual(self.policy.perpetual_base_assets, ("BTC", "ETH", "SOL", "DOGE", "BNB"))
        self.assertEqual(self.policy.perpetual_settlement_asset, "USDT")
        self.assertEqual(len(feature_set.perpetuals), 10)
        by_venue = {
            (record.identity.venue, record.identity.market): {
                item.base_asset
                for item in feature_set.perpetuals
                if (item.identity.venue, item.identity.market)
                == (record.identity.venue, record.identity.market)
            }
            for record in feature_set.perpetuals
        }
        self.assertEqual(by_venue[("BINANCE", "USDM")], set(self.policy.perpetual_base_assets))
        self.assertEqual(by_venue[("OKX", "SWAP")], set(self.policy.perpetual_base_assets))

    def test_missing_liquid_perpetual_fails_closed(self):
        without_doge = tuple(
            item for item in self.records if item.native_symbol not in {"DOGEUSDT", "DOGE-USDT-SWAP"}
        )
        with self.assertRaisesRegex(ValueError, "DOGE"):
            select_liquid_crypto_feature_set(without_doge, policy=self.policy)

    def test_l2_discovery_keeps_provider_discovered_perpetual_and_quarterly_identities(self):
        feature_set = select_liquid_crypto_feature_set(self.records, policy=self.policy)
        self.assertEqual(len(feature_set.l2_books), 14)
        self.assertEqual(
            {(item.identity.venue, item.identity.market) for item in feature_set.l2_books},
            {("BINANCE", "USDM"), ("OKX", "SWAP"), ("OKX", "FUTURES")},
        )
        self.assertEqual(
            {item.native_symbol for item in feature_set.l2_books if item.identity.market == "FUTURES"},
            {
                "BTC-USD-260925", "BTC-USD-261225",
                "BTC-USD_UM-260925", "BTC-USD_UM-261225",
                "ETH-USD-260925", "ETH-USD-261225",
            },
        )
        requirements = build_l2_feature_requirements(
            feature_set, policy=self.policy, consumer_id="c36.source-only"
        )
        self.assertEqual(len(requirements), 28)
        self.assertEqual({item.feed for item in requirements}, {DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA})
        self.assertEqual({item.depth_levels for item in requirements}, {100})
        self.assertTrue(all(item.require_live for item in requirements))

    def test_okx_parser_preserves_provider_quarter_alias_without_date_inference(self):
        record, _alias = parse_public_instrument({
            "instType": "FUTURES", "instId": "BTC-USD-260925",
            "instFamily": "BTC-USD", "alias": "quarter", "baseCcy": "BTC",
            "quoteCcy": "USD", "settleCcy": "USD", "tickSz": "0.1",
            "lotSz": "1", "ctVal": "100", "ctMult": "1",
            "expTime": "1790000000000", "state": "live",
        }, metadata_revision=3, valid_from_ns=1)
        self.assertEqual(record.attributes["alias"], "quarter")
        self.assertEqual(record.native_symbol, "BTC-USD-260925")

    def test_reference_capabilities_preserve_unavailable_without_zero_substitution(self):
        registry = CapabilityRegistry.defaults()
        self.assertTrue(registry.resolve(
            venue="BINANCE", market="USDM", product_type="PERPETUAL", feed=DemandFeed.CONTRACT_METADATA
        ).enabled)
        self.assertTrue(registry.resolve(
            venue="OKX", market="FUTURES", product_type="FUTURE", feed=DemandFeed.BOOK_DELTA
        ).enabled)
        self.assertTrue(registry.resolve(
            venue="OKX", market="FUTURES", product_type="FUTURE", feed=DemandFeed.CONTRACT_METADATA
        ).enabled)
        self.assertFalse(registry.resolve(
            venue="OKX", market="SWAP", product_type="PERPETUAL", feed=DemandFeed.LONG_SHORT_RATIO
        ).enabled)
        future = next(item for item in self.records if item.native_symbol == "BTC-USD-260925")
        self.assertIn(DemandFeed.CONTRACT_METADATA, reference_feeds_for(future))
        self.assertNotIn(DemandFeed.FUNDING_RATE, reference_feeds_for(future))

    def test_contract_metadata_demand_proto_roundtrip_is_additive(self):
        record = next(item for item in self.records if item.native_symbol == "BTCUSDT")
        requirement = DataRequirement(
            consumer_id="c36.contract",
            purpose=DemandPurpose.ALPHA,
            universe=UniverseSelector(
                selector_id="c36:metadata:btcusdt",
                kind=UniverseSelectorKind.EXPLICIT,
                venue=record.identity.venue,
                market=record.identity.market,
                product_type=record.identity.product_type.value,
                native_symbols=(record.native_symbol,),
            ),
            feed=DemandFeed.CONTRACT_METADATA,
            source_policy_id=self.policy.source_policy_id,
            max_freshness_ms=86_400_000,
            require_live=False,
            configuration_revision=self.policy.revision,
        )
        encoded = requirement.to_proto().SerializeToString(deterministic=True)
        decoded = requirement.to_proto().__class__()
        decoded.ParseFromString(encoded)
        self.assertEqual(DataRequirement.from_proto(decoded).canonical_mapping(), requirement.canonical_mapping())


class PhaseC36OkxFuturesReferenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_future_metadata_uses_exact_futures_provider_key(self):
        record = next(item for item in _records() if item.native_symbol == "BTC-USD-260925")
        adapter = OkxSwapReferenceAdapter(_OkxFutureMetadataClient())
        result = await ReferenceBatch({("OKX", "FUTURES"): adapter}).fetch_one(
            ReferenceRequest(record, ReferenceProduct.CONTRACT_METADATA)
        )
        self.assertEqual(result.status, ReferenceStatus.OK)
        self.assertEqual(result.request.instrument.identity.market, "FUTURES")
        self.assertIn("expiry_time_ms", {field.name for field in result.observations[0].fields})

    async def test_default_runtime_has_a_separate_okx_futures_source_identity(self):
        record = next(item for item in _records() if item.native_symbol == "BTC-USD-260925")
        runtime = build_default_reference_runtime()
        self.assertEqual(runtime.source_id_for(record), "qdl-reference-okx-futures-v1")


class PhaseC36L2BindingTests(unittest.TestCase):
    def test_okx_futures_uses_shared_public_books_binding_not_a_worker(self):
        binding = L2ProviderBinding(
            binding_id="l2-okx-futures-btc-usd-260925",
            venue="OKX",
            market="FUTURES",
            product_type="FUTURE",
            native_symbol="BTC-USD-260925",
            instrument_uid="future-uid",
            instrument_id="OKX.FUTURES.FUTURE.BTC-USD-260925",
            source_policy_id="crypto_liquid_v2",
            consumer_ids=("basis-arb",),
            requirement_ids=("a" * 64, "b" * 64),
            feeds=(DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA),
            requested_depth_per_side=100,
            stale_after_ms=60_000,
            ttl_seconds=180,
            require_live=True,
            execution_grade=False,
            provider_protocol="OKX_PUBLIC_BOOKS",
            native_channel="books",
            websocket_url="wss://ws.okx.com:8443/ws/v5/public",
            rest_snapshot_url=None,
        )
        self.assertEqual(binding.subscription.market, "FUTURES")
        self.assertFalse(binding.report_payload().get("runtime_applied", False))


if __name__ == "__main__":
    unittest.main()

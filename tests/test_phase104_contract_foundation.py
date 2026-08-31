from __future__ import annotations

import unittest

from qdl.api_v2.router import _market_item
from qdl.domain.capabilities import (
    CapabilityAvailability,
    binance_usdm_capabilities,
    dnse_capabilities,
    okx_global_capabilities,
)
from qdl.query import (
    ContractMetadata,
    ConsumerGrade,
    DataRequirement as QueryRequirement,
    EXECUTION_PRICE_VALIDATION_FEEDS as QUERY_EXECUTION_PRICE_VALIDATION_FEEDS,
    FeedType,
    MarketDataItem,
    QualityMetadata,
    SourceMetadata,
)
from qdl_sdk import (
    DataRequirement as SdkRequirement,
    EXECUTION_PRICE_VALIDATION_FEEDS,
    Feed,
    Grade,
)
from qdl_sdk.models import (
    BasisKind,
    BasisPayload,
    ContractMetadataPayload,
    DecimalValue,
    LongShortRatioPayload,
    LongShortRatioPopulation,
    MetricUnit,
)


def decimal(
    coefficient: str = "1", scale: int = 0, source_text: str = "1"
) -> dict[str, object]:
    return {
        "coefficient": coefficient,
        "scale": scale,
        "source_text": source_text,
    }


def query_contract() -> ContractMetadata:
    return ContractMetadata(
        schema_digest="a" * 64,
        contract_version="2.0.0",
        normalizer_version="phase104",
        adapter_version="fixture",
        instrument_catalog_revision=1,
        source_policy_revision=1,
        authority_revision=1,
        config_revision=1,
        correlation_id="phase104-contract-foundation",
    )


def reference_item(
    feed: FeedType, interval: str | None, payload: dict[str, object]
) -> MarketDataItem:
    return MarketDataItem(
        instrument_uid="BINANCE.USDM.PERPETUAL.BTC-USDT",
        instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
        instrument_revision=1,
        feed=feed,
        interval=interval,
        observed_at_ns=1,
        payload=payload,
        source=SourceMetadata(
            "BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True
        ),
        quality=QualityMetadata(
            "LIVE", 1, False, True, True, "crypto_primary_v2"
        ),
        contract=query_contract(),
    )


class Phase104ContractFoundationTests(unittest.TestCase):
    def test_decimal_source_text_must_match_exact_canonical_value(self):
        value = DecimalValue.model_validate(decimal("12345", 2, "123.45"))
        self.assertEqual(value.source_text, "123.45")

        with self.assertRaisesRegex(ValueError, "does not match"):
            DecimalValue.model_validate(decimal("12345", 2, "12345"))
        with self.assertRaisesRegex(ValueError, "finite"):
            DecimalValue.model_validate(decimal("1", 0, "NaN"))

    def test_metric_contracts_preserve_missing_and_derived_lineage(self):
        with self.assertRaisesRegex(ValueError, "sampling_interval"):
            LongShortRatioPayload.model_validate(
                {
                    "feed": "LONG_SHORT_RATIO",
                    "population": LongShortRatioPopulation.GLOBAL_ACCOUNT,
                    "sampling_interval": " ",
                    "long_value": decimal(),
                    "short_value": decimal(),
                    "long_short_ratio": decimal(),
                    "value_unit": MetricUnit.RATIO,
                }
            )

        with self.assertRaisesRegex(ValueError, "input instruments"):
            BasisPayload.model_validate(
                {
                    "feed": "BASIS",
                    "kind": BasisKind.DERIVED,
                    "sampling_interval": "1h",
                    "basis": decimal(),
                    "basis_unit": MetricUnit.PERCENT,
                    "formula_id": "basis-spread-v1",
                    "input_instrument_uids": ["perp", "perp"],
                }
            )

        payload = BasisPayload.model_validate(
            {
                "feed": "BASIS",
                "kind": BasisKind.DERIVED,
                "sampling_interval": "1h",
                "basis": decimal(),
                "basis_unit": MetricUnit.PERCENT,
                "formula_id": "basis-spread-v1",
                "input_instrument_uids": ["perp", "quarterly"],
            }
        )
        self.assertEqual(payload.input_instrument_uids, ["perp", "quarterly"])

    def test_contract_metadata_requires_positive_tradable_rules(self):
        with self.assertRaisesRegex(ValueError, "must be positive"):
            ContractMetadataPayload.model_validate(
                {
                    "feed": "CONTRACT_METADATA",
                    "contract_kind": "PERPETUAL",
                    "settlement_asset": "USDT",
                    "contract_multiplier": decimal(),
                    "price_tick": decimal("0", 0, "0"),
                    "quantity_step": decimal(),
                }
            )

    def test_execution_grade_is_limited_to_price_validation_feeds(self):
        self.assertEqual(
            {item.value for item in EXECUTION_PRICE_VALIDATION_FEEDS},
            {item.value for item in QUERY_EXECUTION_PRICE_VALIDATION_FEEDS},
        )
        with self.assertRaisesRegex(ValueError, "execution-price validation"):
            SdkRequirement(
                instrument_uid="BINANCE.USDM.PERPETUAL.BTC-USDT",
                feed=Feed.OPEN_INTEREST,
                consumer_grade=Grade.EXECUTION,
                source_policy_id="crypto_primary_v2",
            )
        with self.assertRaisesRegex(ValueError, "execution-price validation"):
            QueryRequirement(
                instrument_uid="BINANCE.USDM.PERPETUAL.BTC-USDT",
                feed=FeedType.BASIS,
                interval="1h",
                consumer_grade=ConsumerGrade.EXECUTION,
                source_policy_id="crypto_primary_v2",
            )
        QueryRequirement(
            instrument_uid="BINANCE.USDM.PERPETUAL.BTC-USDT",
            feed=FeedType.MARK_INDEX_PRICE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_primary_v2",
        )
        QueryRequirement(
            instrument_uid="BINANCE.USDM.PERPETUAL.BTC-USDT",
            feed=FeedType.BAR,
            interval="1m",
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_primary_v2",
        )

    def test_capabilities_are_truthful_per_venue(self):
        binance = binance_usdm_capabilities()
        self.assertIs(
            binance.capability("long_short_ratio").availability,
            CapabilityAvailability.AVAILABLE,
        )
        self.assertIs(
            binance.capability("taker_flow").availability,
            CapabilityAvailability.AVAILABLE,
        )

        okx = okx_global_capabilities("SWAP")
        self.assertIs(
            okx.capability("long_short_ratio").availability,
            CapabilityAvailability.UNAVAILABLE,
        )
        self.assertIs(
            okx.capability("taker_flow").availability,
            CapabilityAvailability.UNAVAILABLE,
        )
        self.assertIn("derived-only", okx.capability("basis").constraint or "")

        dnse = dnse_capabilities()
        self.assertIs(
            dnse.capability("funding_rate").availability,
            CapabilityAvailability.UNAVAILABLE,
        )
        self.assertIs(
            dnse.capability("contract_metadata").availability,
            CapabilityAvailability.AVAILABLE,
        )

    def test_api_projects_reference_payloads_without_execution_authority(self):
        cases = (
            (
                FeedType.LONG_SHORT_RATIO,
                "1h",
                {
                    "population": "GLOBAL_ACCOUNT",
                    "sampling_interval": "1h",
                    "long_value": "1.2",
                    "short_value": "0.8",
                    "long_short_ratio": "1.5",
                    "value_unit": "RATIO",
                },
            ),
            (
                FeedType.TAKER_FLOW,
                "1h",
                {
                    "sampling_interval": "1h",
                    "buy_volume": "12.3",
                    "sell_volume": "10.1",
                    "buy_sell_ratio": "1.2",
                    "quantity_unit": "BASE_ASSET",
                },
            ),
            (
                FeedType.BASIS,
                "1h",
                {
                    "kind": "DERIVED",
                    "sampling_interval": "1h",
                    "basis": "0.42",
                    "basis_unit": "PERCENT",
                    "annualized_basis": None,
                    "formula_id": "basis-spread-v1",
                    "input_instrument_uids": ["perp", "quarterly"],
                },
            ),
            (
                FeedType.CONTRACT_METADATA,
                None,
                {
                    "contract_kind": "PERPETUAL",
                    "settlement_asset": "USDT",
                    "contract_multiplier": "1",
                    "price_tick": "0.1",
                    "quantity_step": "0.001",
                },
            ),
        )
        for feed, interval, payload in cases:
            with self.subTest(feed=feed):
                result = _market_item(reference_item(feed, interval, payload))
                self.assertEqual(result.feed.value, feed.value)
                self.assertFalse(result.quality.execution_eligible)
        basis = _market_item(reference_item(*cases[2]))
        self.assertIsNone(basis.payload.annualized_basis)
        self.assertEqual(basis.payload.basis.source_text, "0.42")


if __name__ == "__main__":
    unittest.main()

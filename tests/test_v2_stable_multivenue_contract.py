from __future__ import annotations

import json
import unittest
from pathlib import Path

from pydantic import ValidationError

from qdl.api_v2.models import BarPayload, DecimalValue, TradePayload
from qdl.canonical.market import (
    canonicalize_binance_usdm_bar,
    canonicalize_binance_usdm_bbo,
    canonicalize_dnse_bar,
    canonicalize_okx_bar,
    canonicalize_okx_bbo,
)
from qdl.canonical.trade import (
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_dnse_trade,
    canonicalize_okx_trade,
)
from qdl.common.v1 import common_pb2
from qdl.domain.quantity import QuantityUnit, resolve_quantity_unit
from qdl.marketdata.v2 import market_data_pb2


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/phase2"
GOLDEN = ROOT / "contracts/golden/phase2"
MANIFEST = GOLDEN / "manifest-v2-stable-multivenue.json"


def canonicalizer(name: str):
    if "trade" in name:
        if name.startswith("binance"):
            return canonicalize_binance_usdm_trade
        if name.startswith("okx"):
            return canonicalize_okx_trade
        return canonicalize_dnse_trade
    if "bbo" in name:
        return canonicalize_binance_usdm_bbo if name.startswith("binance") else canonicalize_okx_bbo
    if "rest_bar" in name:
        from qdl.canonical.market import canonicalize_binance_usdm_rest_bar
        return canonicalize_binance_usdm_rest_bar
    return canonicalize_dnse_bar if name.startswith(("dnse", "vnstock")) else (
        canonicalize_binance_usdm_bar if name.startswith("binance") else canonicalize_okx_bar
    )


def load_event(fixture_name: str):
    fixture = json.loads((FIXTURES / fixture_name).read_text())
    return canonicalizer(fixture_name)(
        fixture["raw"], TradeContext(**fixture["context"])
    )


def expected_unit(fixture_name: str) -> int:
    if "spot" in fixture_name or fixture_name.startswith("binance"):
        return common_pb2.QUANTITY_UNIT_BASE_ASSET
    if "equity" in fixture_name:
        return common_pb2.QUANTITY_UNIT_SHARE
    return common_pb2.QUANTITY_UNIT_CONTRACT


class StableMultivenueCanonicalContractTests(unittest.TestCase):
    def test_all_nineteen_python_events_match_frozen_bytes(self):
        manifest = json.loads(MANIFEST.read_text())
        self.assertEqual(manifest["schema"], "qdl.v2.stable-multivenue-golden.v1")
        self.assertEqual(len(manifest["cases"]), 19)
        for case in manifest["cases"]:
            with self.subTest(fixture=case["fixture"]):
                event = load_event(case["fixture"])
                self.assertEqual(
                    event.SerializeToString(deterministic=True),
                    (GOLDEN / case["golden"]).read_bytes(),
                )

    def test_trade_quote_and_bar_units_are_never_unspecified(self):
        manifest = json.loads(MANIFEST.read_text())
        for case in manifest["cases"]:
            event = load_event(case["fixture"])
            expected = expected_unit(case["fixture"])
            payload = event.WhichOneof("payload")
            with self.subTest(fixture=case["fixture"], payload=payload):
                if payload == "trade":
                    self.assertEqual(event.trade.quantity_unit, expected)
                elif payload == "quote":
                    self.assertEqual(event.quote.quantity_unit, expected)
                elif payload == "bar":
                    self.assertEqual(event.bar.volume_unit, expected)
                else:
                    self.fail(f"unexpected Phase A payload: {payload}")

    def test_spot_bbo_missing_provider_time_is_explicit(self):
        event = load_event("binance_spot_bbo.json")
        self.assertIn(
            common_pb2.QUALITY_FLAG_SOURCE_TIME_MISSING,
            event.quality_flags,
        )
        self.assertEqual(
            event.source_event_time_ns,
            event.received_at_ns // 1_000_000 * 1_000_000,
        )

    def test_volume_components_preserve_venue_semantics(self):
        swap = load_event("okx_bar.json").bar
        self.assertEqual(swap.volume_unit, common_pb2.QUANTITY_UNIT_CONTRACT)
        self.assertTrue(swap.HasField("contract_volume"))
        self.assertTrue(swap.HasField("base_volume"))
        self.assertTrue(swap.HasField("quote_volume"))
        self.assertEqual(swap.volume.source_text, "12.500")
        self.assertEqual(swap.base_volume.source_text, "12.500")
        self.assertEqual(swap.quote_volume.source_text, "765200.00")

        spot = load_event("okx_spot_bar.json").bar
        self.assertEqual(spot.volume_unit, common_pb2.QUANTITY_UNIT_BASE_ASSET)
        self.assertFalse(spot.HasField("contract_volume"))
        self.assertEqual(spot.base_volume.source_text, spot.volume.source_text)

        derivative = load_event("dnse_derivative_bar.json").bar
        equity = load_event("dnse_equity_bar.json").bar
        self.assertTrue(derivative.HasField("contract_volume"))
        self.assertEqual(equity.volume_unit, common_pb2.QUANTITY_UNIT_SHARE)
        self.assertFalse(equity.HasField("base_volume"))

    def test_dnse_missing_native_identity_is_explicit_not_fabricated(self):
        event = load_event("dnse_derivative_trade.json")
        self.assertEqual(
            event.trade.identity_kind,
            market_data_pb2.TRADE_IDENTITY_KIND_DERIVED_RAW_CAPTURE,
        )
        self.assertTrue(event.trade.native_trade_id.startswith("derived:"))
        self.assertEqual(event.trade.aggressor_side, common_pb2.AGGRESSOR_SIDE_UNSPECIFIED)
        self.assertIn(common_pb2.QUALITY_FLAG_SOURCE_TIME_MISSING, event.quality_flags)
        self.assertIn(common_pb2.QUALITY_FLAG_FIELD_MISSING, event.quality_flags)

    def test_provider_role_and_unknown_identity_fail_closed(self):
        fallback = load_event("vnstock_equity_bar.json")
        self.assertEqual(fallback.source_role, common_pb2.SOURCE_ROLE_SECONDARY)
        with self.assertRaisesRegex(ValueError, "quantity unit is undefined"):
            resolve_quantity_unit(venue="UNKNOWN", market="X", product_type="Y")

    def test_non_positive_trade_price_and_quantity_fail_closed_for_all_venues(self):
        cases = (
            ("binance_usdm_trade.json", canonicalize_binance_usdm_trade, "p", "q"),
            ("okx_trade.json", canonicalize_okx_trade, "px", "sz"),
            ("dnse_derivative_trade.json", canonicalize_dnse_trade, "price", "quantity"),
        )
        for fixture_name, build, price_field, quantity_field in cases:
            fixture = json.loads((FIXTURES / fixture_name).read_text())
            context = TradeContext(**fixture["context"])
            for field, value, message in (
                (price_field, "0", "trade price must be positive"),
                (price_field, "-0.01", "trade price must be positive"),
                (quantity_field, "0", "trade quantity must be positive"),
                (quantity_field, "-0.01", "trade quantity must be positive"),
            ):
                raw = dict(fixture["raw"])
                raw[field] = value
                with self.subTest(fixture=fixture_name, field=field, value=value):
                    with self.assertRaisesRegex(ValueError, message):
                        build(raw, context)


class StablePublicPayloadUnitTests(unittest.TestCase):
    decimal = DecimalValue(coefficient="1", scale=0, source_text="1")

    def test_public_trade_and_bar_require_units_and_identity(self):
        with self.assertRaises(ValidationError):
            TradePayload(
                native_trade_id="1",
                price=self.decimal,
                quantity=self.decimal,
                aggressor_side="BUY",
                identity_kind="NATIVE",
            )
        with self.assertRaises(ValidationError):
            BarPayload(
                interval="1m",
                open_time_ns=1,
                close_time_ns=2,
                open=self.decimal,
                high=self.decimal,
                low=self.decimal,
                close=self.decimal,
                volume=self.decimal,
                lifecycle="FINAL",
                revision=0,
                origin="VENUE_NATIVE",
            )
        trade = TradePayload(
            native_trade_id="1",
            price=self.decimal,
            quantity=self.decimal,
            quantity_unit=QuantityUnit.BASE_ASSET,
            aggressor_side="BUY",
            identity_kind="NATIVE",
        )
        self.assertEqual(trade.quantity_unit, QuantityUnit.BASE_ASSET)


if __name__ == "__main__":
    unittest.main()

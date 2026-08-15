from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.control.universe_importer import import_legacy_universe
from qdl.domain.capabilities import (
    CapabilityAvailability,
    binance_usdm_capabilities,
    dnse_capabilities,
    okx_global_capabilities,
)
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)


def make_record(
    venue: str,
    market: str,
    product_type: ProductType,
    symbol: str,
    native_symbol: str,
    *,
    expiry_time_ns: int | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue=venue,
            market=market,
            product_type=product_type,
            canonical_symbol=symbol,
        ),
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE if product_type is not ProductType.SPOT else AssetClass.CRYPTO,
        native_symbol=native_symbol,
        base_asset="BTC",
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
        expiry_time_ns=expiry_time_ns,
    )


class InstrumentIdentityTests(unittest.TestCase):
    def test_spot_perpetual_dated_and_vn_derivative_do_not_collide(self):
        identities = {
            InstrumentIdentity.create(venue="BINANCE", market="SPOT", product_type="SPOT", canonical_symbol="BTC-USDT"),
            InstrumentIdentity.create(venue="BINANCE", market="USDM", product_type="PERPETUAL", canonical_symbol="BTC-USDT"),
            InstrumentIdentity.create(venue="BINANCE", market="DELIVERY", product_type="FUTURE", canonical_symbol="BTC-USDT-260925"),
            InstrumentIdentity.create(venue="HNX", market="DERIVATIVES", product_type="FUTURE", canonical_symbol="VN30F1M"),
            InstrumentIdentity.create(venue="OKX", market="OPTION", product_type="OPTION", canonical_symbol="BTC-USD-260925-50000-C"),
        }
        self.assertEqual(len({item.instrument_uid for item in identities}), 5)
        self.assertEqual(len({item.instrument_id for item in identities}), 5)

    def test_temporal_alias_resolution_and_snapshot_export(self):
        registry = InstrumentRegistry()
        old = make_record("BINANCE", "DELIVERY", ProductType.FUTURE, "BTC-USDT-260925", "BTCUSDT_260925", expiry_time_ns=10_000)
        new = make_record("BINANCE", "DELIVERY", ProductType.FUTURE, "BTC-USDT-251226", "BTCUSDT_251226", expiry_time_ns=20_000)
        registry.register(old, [InstrumentAlias("BINANCE_DIRECT", "DELIVERY", "BTC-CURRENT", old.instrument_uid, 1, 0, 10_000)])
        registry.register(new, [InstrumentAlias("BINANCE_DIRECT", "DELIVERY", "BTC-CURRENT", new.instrument_uid, 1, 10_000)])

        self.assertEqual(registry.resolve(provider="binance_direct", market="delivery", native_symbol="btc-current", event_time_ns=9_999).instrument_uid, old.instrument_uid)
        self.assertEqual(registry.resolve(provider="binance_direct", market="delivery", native_symbol="btc-current", event_time_ns=10_000).instrument_uid, new.instrument_uid)
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "registry.json"
            registry.export(target)
            self.assertIn("qdl.instrument-registry.snapshot.v1", target.read_text())

    def test_overlapping_alias_ownership_is_rejected(self):
        registry = InstrumentRegistry()
        first = make_record("BINANCE", "USDM", ProductType.PERPETUAL, "BTC-USDT", "BTCUSDT")
        second = make_record("OKX", "SWAP", ProductType.PERPETUAL, "BTC-USDT", "BTC-USDT-SWAP")
        registry.register(first, [InstrumentAlias("REFERENCE", "PERP", "BTC", first.instrument_uid, 1, 0)])
        with self.assertRaisesRegex(ValueError, "overlapping alias"):
            registry.register(second, [InstrumentAlias("REFERENCE", "PERP", "BTC", second.instrument_uid, 1, 5)])
        with self.assertRaisesRegex(KeyError, "unknown instrument_uid"):
            registry.get(second.instrument_uid)


class OkxInstrumentTests(unittest.TestCase):
    def test_swap_uses_registry_inst_family_and_preserves_exact_inst_id(self):
        record, alias = parse_public_instrument(
            {
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "instFamily": "BTC-USDT",
                "baseCcy": "BTC",
                "quoteCcy": "USDT",
                "settleCcy": "USDT",
                "tickSz": "0.1",
                "lotSz": "0.01",
                "ctVal": "0.01",
                "ctMult": "10",
                "state": "live",
            },
            metadata_revision=3,
            valid_from_ns=100,
        )
        self.assertEqual(record.instrument_id, "OKX.SWAP.PERPETUAL.BTC-USDT")
        self.assertEqual(record.native_symbol, "BTC-USDT-SWAP")
        self.assertEqual(alias.native_symbol, "BTC-USDT-SWAP")
        self.assertEqual(record.contract_multiplier.as_decimal(), CanonicalDecimal.from_text("0.1").as_decimal())
        self.assertEqual(record.attributes["ctVal"], "0.01")
        self.assertEqual(record.attributes["ctMult"], "10")

    def test_option_preserves_registry_identity_and_required_fields(self):
        record, _ = parse_public_instrument(
            {
                "instType": "OPTION",
                "instId": "BTC-USD-260925-50000-C",
                "instFamily": "BTC-USD",
                "ctValCcy": "BTC",
                "settleCcy": "BTC",
                "tickSz": "0.1",
                "lotSz": "0.1",
                "ctVal": "0.01",
                "expTime": "1789948800000",
                "stk": "50000",
                "optType": "C",
                "state": "live",
            },
            metadata_revision=1,
            valid_from_ns=100,
        )
        self.assertEqual(record.instrument_id, "OKX.OPTION.OPTION.BTC-USD-260925-50000-C")
        self.assertEqual(record.strike_price.as_decimal(), CanonicalDecimal.from_text("50000").as_decimal())
        self.assertIsNotNone(record.expiry_time_ns)

    def test_missing_registry_family_is_not_guessed(self):
        with self.assertRaisesRegex(ValueError, "instFamily"):
            parse_public_instrument(
                {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "tickSz": "0.1", "lotSz": "1"},
                metadata_revision=1,
                valid_from_ns=0,
            )

    def test_spot_dated_future_and_event_use_exact_registry_ids(self):
        fixtures = [
            (
                {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "tickSz": "0.1",
                    "lotSz": "0.00001",
                    "state": "live",
                },
                "OKX.SPOT.SPOT.BTC-USDT",
            ),
            (
                {
                    "instType": "FUTURES",
                    "instId": "BTC-USDT-260925",
                    "instFamily": "BTC-USDT",
                    "ctValCcy": "BTC",
                    "settleCcy": "USDT",
                    "tickSz": "0.1",
                    "lotSz": "1",
                    "ctVal": "0.01",
                    "expTime": "1789948800000",
                    "state": "live",
                },
                "OKX.FUTURES.FUTURE.BTC-USDT-260925",
            ),
            (
                {
                    "instType": "EVENTS",
                    "instId": "BTC-ABOVE-DAILY-001",
                    "seriesId": "BTC-ABOVE-DAILY",
                    "tickSz": "0.01",
                    "lotSz": "1",
                    "state": "preopen",
                },
                "OKX.EVENTS.EVENT_CONTRACT.BTC-ABOVE-DAILY-001",
            ),
        ]
        for payload, expected_id in fixtures:
            with self.subTest(inst_type=payload["instType"]):
                record, alias = parse_public_instrument(payload, metadata_revision=1, valid_from_ns=0)
                self.assertEqual(record.instrument_id, expected_id)
                self.assertEqual(record.native_symbol, payload["instId"])
                self.assertEqual(alias.native_symbol, payload["instId"])


class CapabilityTests(unittest.TestCase):
    def test_deep_book_is_explicitly_tier_gated(self):
        profile = okx_global_capabilities("OPTION")
        self.assertEqual(profile.capability("l2_deep").availability, CapabilityAvailability.TIER_GATED)
        with self.assertRaisesRegex(RuntimeError, "TIER_GATED"):
            profile.require("l2_deep")
        self.assertTrue(profile.require("l2").resnapshot_on_gap)

    def test_capabilities_are_explicit_across_current_venues(self):
        self.assertTrue(binance_usdm_capabilities().require("trade").live)
        self.assertTrue(dnse_capabilities().require("bbo").live)
        self.assertEqual(
            dnse_capabilities().capability("l2").availability,
            CapabilityAvailability.UNAVAILABLE,
        )


class LegacyUniverseImporterTests(unittest.TestCase):
    def test_imports_requirements_without_fabricating_instrument_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crypto.json").write_text('{"symbols":["BTCUSDT","BTCUSDT","ETHUSDT"]}')
            (root / "vn.yaml").write_text("symbols:\n  - VN30F1M\n  - FPT\n")
            requirements = import_legacy_universe(
                binance_path=root / "crypto.json",
                vn_path=root / "vn.yaml",
            )
        self.assertEqual(len(requirements), 4)
        self.assertTrue(all(item.resolution_state == "PENDING_AUTHORITATIVE_DISCOVERY" for item in requirements))
        self.assertFalse(any(hasattr(item, "price_tick") for item in requirements))


if __name__ == "__main__":
    unittest.main()

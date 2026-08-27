from __future__ import annotations

from types import SimpleNamespace
import unittest

from qdl.demand.liquid_crypto import LiquidCryptoFeaturePolicy, select_liquid_crypto_feature_set
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import AssetClass, InstrumentIdentity, InstrumentRecord, ProductType
from qdl.query import ConsumerGrade, FeedType, RecoveryPolicy
from qdl.reference import ReferenceStatus
from scripts.phasec36_real_provider_certification import (
    CertificationError,
    _bar_requirements,
    _chunks,
    _is_truthful_partial_history,
    _l2_admission_document,
    _reference_certification_batches,
    _reference_work,
    _selected_provider_rows,
    _select_universe_records,
)
from qdl.universe import TopVolumeUniverse, TopVolumeUniversePolicy
from qdl.universe.top_volume import TopVolumeMember


def _record(
    *,
    venue: str,
    market: str,
    product: ProductType,
    native: str,
    base: str,
    attributes: dict[str, str] | None = None,
) -> InstrumentRecord:
    canonical = f"{base}-USDT"
    if product is ProductType.FUTURE:
        canonical = f"{canonical}-{native.rsplit('-', 1)[-1].replace('_', '')}"
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue=venue,
            market=market,
            product_type=product,
            canonical_symbol=canonical,
        ),
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=native,
        base_asset=base,
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
        expiry_time_ns=(1_900_000_000_000_000_000 if product is ProductType.FUTURE else None),
        attributes=attributes or {},
    )


def _policy() -> LiquidCryptoFeaturePolicy:
    return LiquidCryptoFeaturePolicy(
        revision=1,
        perpetual_base_assets=("BTC", "ETH", "SOL", "DOGE", "BNB"),
        perpetual_settlement_asset="USDT",
        perpetual_venue_markets=(("BINANCE", "USDM"), ("OKX", "SWAP")),
        l2_base_assets=("BTC", "ETH"),
        l2_dated_venue_markets=(("BINANCE", "USDM"), ("OKX", "FUTURES")),
        l2_depth_per_side=100,
        l2_freshness_ms=60_000,
        l2_ttl_seconds=180,
        source_policy_id="crypto_liquid_v2",
    )


def _records() -> tuple[InstrumentRecord, ...]:
    values = []
    for venue, market, suffix in (("BINANCE", "USDM", "USDT"), ("OKX", "SWAP", "USDT-SWAP")):
        for base in ("BTC", "ETH", "SOL", "DOGE", "BNB"):
            native = f"{base}{suffix}" if venue == "BINANCE" else f"{base}-{suffix}"
            values.append(_record(
                venue=venue,
                market=market,
                product=ProductType.PERPETUAL,
                native=native,
                base=base,
            ))
    for base in ("BTC", "ETH"):
        for kind, date in (("CURRENT_QUARTER", "260926"), ("NEXT_QUARTER", "261225")):
            values.append(_record(
                venue="BINANCE",
                market="USDM",
                product=ProductType.FUTURE,
                native=f"{base}USDT_{date}",
                base=base,
                attributes={"contractType": kind},
            ))
        for alias, date in (("quarter", "260926"), ("next_quarter", "261225")):
            values.append(_record(
                venue="OKX",
                market="FUTURES",
                product=ProductType.FUTURE,
                native=f"{base}-USDT-{date}",
                base=base,
                attributes={"alias": alias, "instFamily": f"{base}-USDT"},
            ))
    return tuple(values)


class PhaseC36CertificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = _policy()
        self.records = _records()
        self.feature_set = select_liquid_crypto_feature_set(self.records, policy=self.policy)

    def test_reference_matrix_covers_five_assets_and_keeps_okx_absence_typed(self):
        work = _reference_work(
            self.feature_set,
            policy=self.policy,
            now_ms=1_800_000_000_000,
            deadline_ms=60_000,
        )
        self.assertEqual(len(work), 72)
        self.assertEqual(sum(item.expected == "UNAVAILABLE" for item in work), 15)
        self.assertEqual(sum(item.expected == "OK_OR_PARTIAL" for item in work), 15)
        self.assertEqual(
            [len(chunk) for chunk in _chunks(work, 12)],
            [12, 12, 12, 12, 12, 12],
        )
        batches = _reference_certification_batches(work)
        self.assertEqual(sum(len(chunk) for chunk in batches), len(work))
        self.assertTrue(all(len(chunk) <= 12 for chunk in batches))
        self.assertEqual(
            sum(
                len(chunk) == 1
                and chunk[0].requirement.product.value == "BASIS"
                and chunk[0].requirement.basis_series.value == "NATIVE"
                for chunk in batches
            ),
            5,
        )
        self.assertEqual(
            sum(
                item.requirement.product.value == "BASIS"
                and item.requirement.basis_series.value == "CONTINUOUS"
                and item.expected == "OK"
                for item in work
            ),
            2,
        )
        self.assertEqual(
            {item.requirement.instrument_uid for item in work},
            {item.instrument_uid for item in self.feature_set.perpetuals},
        )

    def test_history_partial_requires_real_observations_and_non_full_coverage(self):
        def data(*, observations=True, left=True, right=True, truncated=False):
            return SimpleNamespace(
                status=ReferenceStatus.OK,
                observations=(object(),) if observations else (),
                request=SimpleNamespace(is_history=True),
                coverage=SimpleNamespace(
                    complete_left=left,
                    complete_right=right,
                    truncated=truncated,
                ),
            )

        self.assertTrue(_is_truthful_partial_history(data(truncated=True)))
        self.assertTrue(_is_truthful_partial_history(data(left=False)))
        self.assertFalse(_is_truthful_partial_history(data()))
        self.assertFalse(_is_truthful_partial_history(data(observations=False, truncated=True)))

    def test_l2_admission_is_discovered_identity_only_with_snapshot_and_delta(self):
        document = _l2_admission_document(
            self.feature_set,
            policy=self.policy,
            metadata_sha256={"BINANCE:USDM": "a" * 64, "OKX:SWAP": "b" * 64, "OKX:FUTURES": "c" * 64},
            inventory_sha256="d" * 64,
        )
        rows = document["rows"]
        self.assertEqual(len(rows), len(self.feature_set.l2_books) * 2)
        self.assertEqual({row["feed"] for row in rows}, {"BOOK_SNAPSHOT", "BOOK_DELTA"})
        self.assertTrue(all(row["native_symbol"] in {item.native_symbol for item in self.feature_set.l2_books} for row in rows))
        self.assertTrue(any(row["market"] == "FUTURES" for row in rows))

    def test_final_bar_requirements_are_fresh_snapshot_only_and_bounded(self):
        requirements = _bar_requirements(
            self.feature_set.perpetuals,
            policy=self.policy,
            deadline_ms=60_000,
        )
        self.assertEqual(len(requirements), 10)
        self.assertTrue(all(item.feed is FeedType.BAR for item in requirements))
        self.assertTrue(all(item.consumer_grade is ConsumerGrade.ALPHA for item in requirements))
        self.assertTrue(all(item.recovery is RecoveryPolicy.FRESH_SNAPSHOT for item in requirements))
        self.assertTrue(all(item.warmup_specification and item.warmup_specification.rows == 1 for item in requirements))

    def test_top_volume_selection_rejects_missing_metadata_instead_of_substitution(self):
        policy = TopVolumeUniversePolicy("BINANCE", "USDM", size=1)
        universe = TopVolumeUniverse(
            policy=policy,
            generated_at_ns=1,
            provider_metadata_sha256="a" * 64,
            provider_ticker_sha256="b" * 64,
            eligible_symbols=("BTCUSDT",),
            members=(TopVolumeMember("BTCUSDT", "1", 1),),
        )
        with self.assertRaises(CertificationError):
            _select_universe_records(
                (),
                universe=universe,
                venue="BINANCE",
                market="USDM",
            )

    def test_selected_provider_rows_skip_irrelevant_catalog_rows_but_not_selected_ones(self):
        rows = _selected_provider_rows(
            (
                {"instId": "BTC-USDT-SWAP", "instFamily": "BTC-USDT"},
                {"instId": "IRRELEVANT-SWAP"},
            ),
            symbols=("BTC-USDT-SWAP",),
            symbol_key="instId",
            label="OKX Swap",
        )
        self.assertEqual(rows[0]["instId"], "BTC-USDT-SWAP")
        with self.assertRaises(CertificationError):
            _selected_provider_rows(
                rows,
                symbols=("ETH-USDT-SWAP",),
                symbol_key="instId",
                label="OKX Swap",
            )


if __name__ == "__main__":
    unittest.main()

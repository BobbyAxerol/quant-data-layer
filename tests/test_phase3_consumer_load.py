from __future__ import annotations

from dataclasses import dataclass, replace
import unittest

from qdl.certification.phase3_consumer_load import (
    assert_required_instrument_coverage,
    build_consumer_load_plan,
)


@dataclass(frozen=True)
class _Feed:
    value: str


@dataclass(frozen=True)
class _Delivery:
    value: str


@dataclass(frozen=True)
class _Product:
    consumer_id: str
    venue: str
    native_symbol: str
    feed: _Feed
    interval: str | None = None
    source_policy_id: str = "crypto_primary_v2"
    delivery: _Delivery = _Delivery("DURABLE")

    @property
    def identity(self):
        return (
            self.consumer_id,
            self.native_symbol,
            self.feed.value,
            self.interval or "",
            self.source_policy_id,
        )


@dataclass(frozen=True)
class _Quotas:
    requests_per_minute: int
    max_streams: int


@dataclass(frozen=True)
class _Manifest:
    quotas: _Quotas


def _products(consumer_id: str, venue: str, symbols: tuple[str, ...]) -> tuple[_Product, ...]:
    result = []
    for symbol in symbols:
        result.extend(
            _Product(consumer_id, venue, symbol, _Feed(feed), interval)
            for feed, interval in (
                ("BAR", "1m"),
                ("TRADE", None),
                ("QUOTE", None),
                ("BOOK_SNAPSHOT", None),
                ("BOOK_DELTA", None),
                ("MARK_INDEX_PRICE", None),
            )
        )
    return tuple(result)


class ConsumerLoadPlanTests(unittest.TestCase):
    def setUp(self):
        binance = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT")
        okx = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP")
        self.products = {
            "alpha.binance.paper.stable": _products("alpha.binance.paper.stable", "BINANCE", binance),
            "alpha.okx.paper.stable": _products("alpha.okx.paper.stable", "OKX", okx),
            "monitoring.multivenue.stable": _products("monitoring.multivenue.stable", "BINANCE", binance),
            "trading-system.paper.stable": _products("trading-system.paper.stable", "OKX", okx),
        }
        self.manifests = {
            consumer_id: _Manifest(_Quotas(180 if consumer_id.startswith("alpha") else 1500, 20 if consumer_id.startswith("alpha") else 50))
            for consumer_id in self.products
        }

    def test_final_plan_is_deterministic_bounded_and_covers_all_five_symbols_per_venue(self):
        first = build_consumer_load_plan(
            manifests=self.manifests,
            products_by_consumer=self.products,
            logical_session_count=50,
        )
        second = build_consumer_load_plan(
            manifests=self.manifests,
            products_by_consumer=self.products,
            logical_session_count=50,
        )
        self.assertEqual(first, second)
        self.assertEqual(len(first.logical_sessions), 50)
        self.assertEqual(first.identity_count, 4)
        self.assertEqual(first.stream_count, 54)
        self.assertTrue(all(2 <= len(item.products) <= 5 for item in first.logical_sessions))
        assert_required_instrument_coverage(
            first,
            tuple(("BINANCE", item) for item in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"))
            + tuple(("OKX", item) for item in ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP")),
        )
        for budget in first.identity_budgets:
            self.assertLessEqual(budget.test_requests_per_minute, budget.requests_per_minute // 10 or 1)
            self.assertLessEqual(budget.planned_streams, budget.max_streams)

    def test_small_stage_can_require_all_declared_venue_symbol_pairs(self):
        required = (
            tuple(("BINANCE", item) for item in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"))
            + tuple(("OKX", item) for item in ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"))
        )
        plan = build_consumer_load_plan(
            manifests=self.manifests,
            products_by_consumer=self.products,
            logical_session_count=5,
            required_instruments=required,
        )
        assert_required_instrument_coverage(plan, required)
        self.assertTrue(all(2 <= len(session.products) <= 5 for session in plan.logical_sessions))

    def test_planner_refuses_unavailable_identity_capacity_before_any_traffic(self):
        manifests = {"one": _Manifest(_Quotas(60, 1))}
        products = {"one": _products("one", "BINANCE", ("BTCUSDT", "ETHUSDT"))}
        with self.assertRaisesRegex(ValueError, "max_streams"):
            build_consumer_load_plan(
                manifests=manifests,
                products_by_consumer=products,
                logical_session_count=2,
            )

    def test_planner_rejects_more_than_ten_percent_test_quota(self):
        with self.assertRaisesRegex(ValueError, "ten percent"):
            build_consumer_load_plan(
                manifests=self.manifests,
                products_by_consumer=self.products,
                logical_session_count=5,
                test_quota_fraction=0.101,
            )

    def test_required_coverage_cannot_be_silently_dropped(self):
        plan = build_consumer_load_plan(
            manifests=self.manifests,
            products_by_consumer=self.products,
            logical_session_count=5,
        )
        with self.assertRaisesRegex(ValueError, "misses required instruments"):
            assert_required_instrument_coverage(plan, (("DERIBIT", "BTC-PERPETUAL"),))

    def test_on_demand_product_does_not_count_as_a_live_stream(self):
        products = self.products.copy()
        products["alpha.binance.paper.stable"] = tuple(
            replace(item, delivery=_Delivery("ON_DEMAND"))
            if item.feed.value in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
            else item
            for item in products["alpha.binance.paper.stable"]
        )
        plan = build_consumer_load_plan(
            manifests=self.manifests,
            products_by_consumer=products,
            logical_session_count=5,
        )
        alpha_session = next(
            item for item in plan.logical_sessions
            if item.consumer_id == "alpha.binance.paper.stable"
        )
        self.assertTrue(
            any(
                item.feed.value in {"TRADE", "QUOTE"}
                and item.delivery.value == "DURABLE"
                for item in alpha_session.products
            )
        )


if __name__ == "__main__":
    unittest.main()

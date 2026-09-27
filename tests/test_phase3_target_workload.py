"""v2.1.1 frozen four-class target workload planner.

The planner is the contract between the owner's target and the load driver, so
these tests pin the owner's own numbers rather than whatever the code produces:
stage mixes 2/1/1/1, 8/6/4/2, 14/10/7/4, 20/15/10/5; 9/36/63/90 streams;
5/20/35/50 hot requests per second. They also pin the two ways the plan must
refuse to exist before traffic: a class product missing from a manifest, and a
sealed quota that cannot carry the demand.
"""

from __future__ import annotations

from dataclasses import dataclass
import unittest

from qdl.certification.phase3_consumer_load import (
    TARGET_STAGE_MIX,
    build_target_workload_plan,
)


@dataclass(frozen=True)
class _Feed:
    value: str


@dataclass(frozen=True)
class _Product:
    consumer_id: str
    venue: str
    native_symbol: str
    feed: _Feed
    interval: str | None = None

    @property
    def identity(self):
        return (self.consumer_id, self.venue, self.native_symbol, self.feed.value, self.interval or "")


@dataclass(frozen=True)
class _Quotas:
    requests_per_minute: int
    max_streams: int


@dataclass(frozen=True)
class _Manifest:
    quotas: _Quotas


VENUES = {"BINANCE": "alpha.binance.paper.stable", "OKX": "alpha.okx.paper.stable"}
SYMBOLS = {
    "BINANCE": ("BNBUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"),
    "OKX": ("BNB-USDT-SWAP", "BTC-USDT-SWAP", "DOGE-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"),
}
FEEDS = (("BAR", "1m"), ("QUOTE", None), ("TRADE", None), ("BOOK_DELTA", None),
         ("BOOK_SNAPSHOT", None), ("MARK_INDEX_PRICE", None), ("FUNDING_RATE", None))


def _products(skip: tuple[str, str, str] | None = None):
    result = {}
    for venue, consumer in VENUES.items():
        result[consumer] = tuple(
            _Product(consumer, venue, symbol, _Feed(feed), interval)
            for symbol in SYMBOLS[venue] for feed, interval in FEEDS
            if (venue, symbol, feed) != skip
        )
    return result


def _manifests(rpm: int, streams: int):
    return {consumer: _Manifest(_Quotas(rpm, streams)) for consumer in VENUES.values()}


def _plan(stage: int, *, rpm: int = 2400, streams: int = 60, skip=None, enforce=True):
    return build_target_workload_plan(
        stage=stage, manifests=_manifests(rpm, streams), products_by_consumer=_products(skip),
        venue_identity=VENUES, instruments={k: list(v) for k, v in SYMBOLS.items()},
        enforce_quota=enforce)


class TargetArithmeticTests(unittest.TestCase):
    def test_every_stage_matches_the_owners_frozen_numbers(self):
        expected = {5: ((2, 1, 1, 1), 9, 5), 20: ((8, 6, 4, 2), 36, 20),
                    35: ((14, 10, 7, 4), 63, 35), 50: ((20, 15, 10, 5), 90, 50)}
        self.assertEqual(set(expected), set(TARGET_STAGE_MIX))
        for stage, (mix, stream_count, hot) in expected.items():
            plan = _plan(stage)
            self.assertEqual(plan.class_counts, mix, stage)
            self.assertEqual(plan.stream_count, stream_count, stage)
            self.assertEqual(round(plan.hot_requests_per_second), hot, stage)

    def test_stages_of_ten_or_more_cover_all_ten_pairs_and_small_stages_both_venues(self):
        for stage in (20, 35, 50):
            self.assertEqual(len(_plan(stage).covered_instruments), 10, stage)
        venues = {venue for venue, _ in _plan(5).covered_instruments}
        self.assertEqual(venues, {"BINANCE", "OKX"})

    def test_full_target_splits_evenly_across_the_two_identities(self):
        plan = _plan(50)
        self.assertEqual([d.sessions for d in plan.demands], [25, 25])
        self.assertEqual([d.required_streams for d in plan.demands], [45, 45])

    def test_each_session_keeps_two_to_five_declared_products(self):
        for session in _plan(50).sessions:
            declared = {p.identity for p in session.streams}
            declared |= {p.identity for poll in session.polls for p in poll.products}
            self.assertTrue(2 <= len(declared) <= 5, session)

    def test_multi_symbol_reads_two_instruments_of_one_venue_in_one_batch(self):
        multi = [s for s in _plan(50).sessions if s.alpha_class == "MULTI"]
        self.assertEqual(len(multi), 5)
        for session in multi:
            batch = session.polls[0]
            self.assertEqual(batch.operation, "REFERENCE_BATCH")
            self.assertEqual(len(batch.products), 2)
            self.assertEqual(len({p.venue for p in batch.products}), 1)
            self.assertEqual(len({p.native_symbol for p in batch.products}), 2)
            self.assertEqual(session.polls[1].period_seconds, 60.0)

    def test_grid_takes_a_book_snapshot_on_startup_and_streams_its_delta(self):
        for session in (s for s in _plan(50).sessions if s.alpha_class == "GRID"):
            self.assertEqual([p.feed.value for p in session.startup_snapshots], ["BOOK_SNAPSHOT"])
            self.assertIn("BOOK_DELTA", [p.feed.value for p in session.streams])


class RefusalTests(unittest.TestCase):
    def test_a_missing_class_product_refuses_the_plan_and_names_it(self):
        with self.assertRaisesRegex(ValueError, "BOOK_DELTA for BINANCE BTCUSDT"):
            _plan(50, skip=("BINANCE", "BTCUSDT", "BOOK_DELTA"))

    def test_todays_sealed_quota_refuses_stage_twenty_before_any_traffic(self):
        with self.assertRaisesRegex(ValueError, "does not fit sealed quota"):
            _plan(20, rpm=180, streams=20)

    def test_the_option_a_quota_carries_the_full_target_with_headroom(self):
        plan = _plan(50, rpm=2400, streams=60)
        self.assertTrue(all(d.fits for d in plan.demands))
        self.assertTrue(all(d.quota_needed <= 2400 and d.streams_needed <= 60 for d in plan.demands))

    def test_the_plan_never_lowers_the_offered_rate_to_fit(self):
        tight = _plan(50, rpm=100, streams=10, enforce=False)
        self.assertEqual(round(tight.hot_requests_per_second), 50)

    def test_an_unknown_stage_is_refused(self):
        with self.assertRaises(ValueError):
            _plan(10)


if __name__ == "__main__":
    unittest.main()

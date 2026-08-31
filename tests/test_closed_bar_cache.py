from __future__ import annotations

import json
import unittest
from pathlib import Path

from qdl.runtime.closed_bar_cache import ClosedBarWindowCache
from qdl.query.contracts import (
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.runtime.provider_history import ProviderBarHistorySource
from qdl.runtime.stable_catalog import StableSourceCatalog

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
BINANCE_ETH = "ee93fabf-68df-5b50-8924-51bf25a5a757"
MINUTE_MS = 60_000
BASE_MS = 1_787_385_300_000


class ClosedBarWindowCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = ClosedBarWindowCache(max_entries=3)

    def test_a_window_is_reusable_within_the_same_boundary(self):
        self.cache.put("uid", "1m", 100, ("a", "b", "c"))
        self.assertEqual(self.cache.get("uid", "1m", 100, 3), ("a", "b", "c"))
        self.assertEqual(self.cache.get("uid", "1m", 100, 2), ("b", "c"))

    def test_a_later_boundary_can_never_reuse_an_earlier_window(self):
        self.cache.put("uid", "1m", 100, ("a", "b", "c"))
        self.assertIsNone(self.cache.get("uid", "1m", 160, 3))

    def test_a_different_interval_or_instrument_is_a_different_window(self):
        self.cache.put("uid", "1m", 100, ("a", "b"))
        self.assertIsNone(self.cache.get("uid", "5m", 100, 2))
        self.assertIsNone(self.cache.get("other", "1m", 100, 2))

    def test_a_shorter_cached_window_is_a_miss_not_a_short_answer(self):
        self.cache.put("uid", "1m", 100, ("a", "b"))
        self.assertIsNone(self.cache.get("uid", "1m", 100, 3))

    def test_the_longest_window_for_a_boundary_is_kept(self):
        self.cache.put("uid", "1m", 100, ("a", "b", "c"))
        self.cache.put("uid", "1m", 100, ("b", "c"))
        self.assertEqual(self.cache.get("uid", "1m", 100, 3), ("a", "b", "c"))

    def test_entries_are_bounded_and_evict_least_recently_used(self):
        for index in range(4):
            self.cache.put(f"uid{index}", "1m", 100, ("a",))
        self.assertEqual(self.cache.stats()["entries"], 3)
        self.assertIsNone(self.cache.get("uid0", "1m", 100, 1))
        self.assertIsNotNone(self.cache.get("uid3", "1m", 100, 1))

    def test_it_counts_hits_and_misses(self):
        self.cache.put("uid", "1m", 100, ("a",))
        self.cache.get("uid", "1m", 100, 1)
        self.cache.get("uid", "1m", 999, 1)
        stats = self.cache.stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)


class _RawEnvelope:
    def __init__(self, frame: dict):
        self.raw_frame_bytes = json.dumps(frame, sort_keys=True).encode()
        self.provider = "BINANCE_DIRECT"
        self.capture_id = b"\x01" * 16
        self.raw_frame_sha256 = b"\x02" * 32


class PassThroughCacheIntegrationTests(unittest.TestCase):
    """A wide universe only fits inside a rate limit if windows are shared."""

    def setUp(self) -> None:
        self.catalog = StableSourceCatalog.load(CATALOG_PATH)
        self.fetches = 0
        self.now_ms = BASE_MS + 3 * MINUTE_MS

        def fetcher(binding, *, limit: int, **rest):
            self.fetches += 1
            return tuple(
                _RawEnvelope({
                    "symbol": binding.native_symbol,
                    "interval": binding.interval,
                    "bar_origin": "BACKFILLED",
                    "row": [
                        BASE_MS + index * MINUTE_MS, "100", "101", "99", "100.5",
                        "10", BASE_MS + index * MINUTE_MS + 59_999, "1005",
                        7, "5", "500", "0",
                    ],
                })
                for index in range(limit)
            )

        self.source = ProviderBarHistorySource(
            self.catalog,
            binance_fetcher=fetcher,
            clock_ns=lambda: self.now_ms * 1_000_000,
        )

    def _requirement(self, limit: int = 3) -> DataRequirement:
        return DataRequirement(
            instrument_uid=BINANCE_ETH,
            feed=FeedType.BAR,
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id="crypto_primary_v2",
            interval="1m",
            warmup_limit=limit,
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
        )

    def test_repeat_requests_in_one_period_hit_the_venue_once(self):
        first = self.source.history(self._requirement())
        second = self.source.history(self._requirement())
        self.assertEqual(self.fetches, 1)
        self.assertEqual(first, second)

    def test_a_smaller_request_reuses_the_cached_window(self):
        self.source.history(self._requirement(limit=3))
        smaller = self.source.history(self._requirement(limit=2))
        self.assertEqual(self.fetches, 1)
        self.assertEqual(len(smaller), 2)

    def test_a_larger_request_refetches_rather_than_answering_short(self):
        self.source.history(self._requirement(limit=2))
        larger = self.source.history(self._requirement(limit=3))
        self.assertEqual(self.fetches, 2)
        self.assertEqual(len(larger), 3)

    def test_the_next_bar_period_refetches(self):
        self.source.history(self._requirement())
        self.now_ms += MINUTE_MS
        self.source.history(self._requirement())
        self.assertEqual(self.fetches, 2)


if __name__ == "__main__":
    unittest.main()

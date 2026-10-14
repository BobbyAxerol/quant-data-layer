"""P18.4 G4 observer: filter and offline summary (no Kafka)."""
import json
import unittest

from scripts.p184_g4_observer import ALL_FEEDS, OBSERVER_VERSION, Sampler, accepted, sampling_key, summarize


def obs(feed, venue, source, received, seen, interval=None):
    return json.dumps({"kind": "canonical_observation", "feed": feed, "identity": {"venue": venue},
                       "source_event_ns": source, "received_at_ns": received, "observer_receipt_wall_ns": seen,
                       "interval": interval})


class ObserverTests(unittest.TestCase):
    def test_default_filter_takes_every_symbol_and_feed_of_both_venues(self):
        feeds = set(ALL_FEEDS.split(","))
        self.assertTrue(accepted("OKX", "DOGE-USDT-SWAP", "book_delta", None, feeds))
        self.assertTrue(accepted("BINANCE", "BNBUSDT", "bar", None, feeds))
        self.assertFalse(accepted("KRAKEN", "XBTUSD", "trade", None, feeds))
        self.assertFalse(accepted("OKX", "BTC-USDT-SWAP", "trade", {"ETH-USDT-SWAP"}, feeds))

    def test_summary_groups_by_feed_venue_interval_and_counts_missing_and_negative(self):
        lines = [obs("trade", "OKX", 1_000_000_000, 1_030_000_000, 1_150_000_000),
                 obs("trade", "OKX", 2_000_000_000, 2_020_000_000, 1_990_000_000),
                 obs("trade", "OKX", None, 1, 1),
                 obs("bar", "BINANCE", 3_000_000_000, 3_010_000_000, 3_200_000_000, interval="1m"),
                 json.dumps({"kind": "lag"})]
        out = summarize(lines)
        trade = out["trade|OKX|"]
        self.assertEqual(trade["visibility_ms"]["n"], 2)
        self.assertEqual(trade["visibility_ms"]["max"], 150.0)
        self.assertEqual(trade["visibility_ms"]["negative"], 1)  # skewed sample kept and counted, not clamped
        self.assertEqual(trade["missing_clock"], 1)
        self.assertEqual(out["bar|BINANCE|1m"]["venue_to_dl_ms"]["p50"], 10.0)
        self.assertIsNone(trade["visibility_ms"]["p99"])  # p99 only with n >= 100


S = 1_000_000_000


class SamplingKeyTests(unittest.TestCase):
    """v2 sampling: BAR keyed per interval, every other feed unchanged."""

    def test_simultaneous_closes_of_different_intervals_are_all_kept(self):
        sampler = Sampler(1000)
        closes = ["1m", "3m", "5m", "15m", "30m"]
        kept = [sampler.admit("OKX", "BTC-USDT-SWAP", "bar", interval, 10 * S + i) for i, interval in enumerate(closes)]
        self.assertEqual(kept, [True] * 5)
        # the same interval inside the sample period is still rate limited
        self.assertFalse(sampler.admit("OKX", "BTC-USDT-SWAP", "bar", "3m", 10 * S + 500_000_000))
        self.assertTrue(sampler.admit("OKX", "BTC-USDT-SWAP", "bar", "3m", 11 * S + 1))

    def test_v1_key_collapsed_the_same_closes_to_one(self):
        keys = {sampling_key("BINANCE", "BTCUSDT", "bar", i) for i in ("3m", "5m", "15m", "30m")}
        self.assertEqual(len(keys), 4)
        self.assertEqual(len({k[:3] for k in keys}), 1)  # the v1 key (venue, symbol, feed) had one slot

    def test_non_bar_feeds_keep_the_v1_key_and_rate(self):
        for feed in ("trade", "quote", "mark_index_price", "book_delta", "book_snapshot"):
            self.assertEqual(sampling_key("OKX", "ETH-USDT-SWAP", feed, "1m"), ("OKX", "ETH-USDT-SWAP", feed))
        sampler = Sampler(1000)
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 5 * S))
        self.assertFalse(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 5 * S + 999_999_999))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "trade", None, 6 * S))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "book_snapshot", None, 6 * S))
        self.assertTrue(sampler.admit("OKX", "ETH-USDT-SWAP", "book_snapshot", None, 6 * S))  # unsampled, as before
        self.assertTrue(OBSERVER_VERSION.endswith(".v2"))


if __name__ == "__main__":
    unittest.main()

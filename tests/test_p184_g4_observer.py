"""P18.4 G4 observer: filter and offline summary (no Kafka)."""
import json
import unittest

from scripts.p184_g4_observer import ALL_FEEDS, accepted, summarize


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


if __name__ == "__main__":
    unittest.main()

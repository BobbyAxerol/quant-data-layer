from pathlib import Path
import unittest

from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.consumer.release import StableReleaseRoutePlan
from qdl.consumer.realtime_route import requirement_key
from qdl.consumer.universal_release import ConsumerRouteBinding
from scripts.phase115_render_consumer_route_binding import binding_from_stable_release


ROOT = Path(__file__).resolve().parents[1]
ROUTING = ROOT / "config/v2/stable-v2-release-routing.yaml"
CONSUMER = "trading-system.paper.stable"


class TradingConsumerScopeTests(unittest.TestCase):
    def setUp(self):
        self.binding = binding_from_stable_release(ROUTING, ROOT, CONSUMER)

    def test_complete_native_five_symbol_matrix(self):
        expected = {
            (venue, symbol + suffix, feed, "1m" if feed == "BAR" else None)
            for venue, suffix in (("BINANCE", "USDT"), ("OKX", "-USDT-SWAP"))
            for symbol in ("BTC", "ETH", "SOL", "DOGE", "BNB")
            for feed in ("TRADE", "QUOTE", "BAR", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA")
        }
        self.assertEqual(len(self.binding.products), 60)
        self.assertEqual(expected, {(p.venue, p.native_symbol, p.feed, p.interval) for p in self.binding.products})
        self.assertEqual(len({p.requirement_id for p in self.binding.products}), 60)

    def test_sealed_roundtrip_and_manifest_revision(self):
        parsed = ConsumerRouteBinding.from_canonical_mapping(self.binding.canonical_mapping())
        self.assertEqual(parsed, self.binding)
        self.assertEqual(parsed.consumer_manifest_revision, 9)
        value = self.binding.canonical_mapping()
        value["products"][0]["native_symbol"] = "WRONG"
        with self.assertRaises(ValueError):
            ConsumerRouteBinding.from_canonical_mapping(value)

    def test_exact_certified_route_and_domain_policy(self):
        plan = StableReleaseRoutePlan.load(ROUTING, manifest_root=ROOT)
        consumer = next(c for c in plan.consumers if c.consumer_id == CONSUMER)
        requirements = {requirement_key(r): r for r in consumer.manifest.requirements}
        routes = {r.requirement_key: r for r in consumer.products}
        for product in self.binding.products:
            key = f"{product.instrument_uid}:{product.feed}:{product.interval or ''}:{product.source_policy_id}"
            self.assertEqual(product.route, routes[key].route)
            self.assertEqual(product.fallback, routes[key].fallback)
            self.assertEqual(product.max_freshness_ms, requirements[key].max_freshness_ms)
            self.assertEqual(product.require_final_bars, requirements[key].require_final_bars)
            self.assertTrue(product.execution_grade)
        self.assertEqual(sum(p.fallback == "V1" for p in self.binding.products), 0)
        self.assertEqual(self.binding.independent_v1_venues, ("DNSE",))

    def test_quota_covers_persistent_streams_and_periodic_reads(self):
        manifest = ConsumerManifestLoader.load(ROOT / "consumers/stable/trading-system-paper.yaml")
        streams = sum(p.feed in {"TRADE", "QUOTE", "BAR", "BOOK_DELTA"} for p in self.binding.products)
        polls_per_minute = sum(60 if p.feed == "MARK_INDEX_PRICE" else 2 if p.feed == "BOOK_SNAPSHOT" else 0 for p in self.binding.products)
        self.assertEqual(streams, 40)
        self.assertGreaterEqual(manifest.quotas.max_streams, streams + 10)
        self.assertGreaterEqual(manifest.quotas.requests_per_minute, 2 * polls_per_minute)

    def test_unknown_consumer_is_not_granted_access(self):
        with self.assertRaises(ValueError):
            binding_from_stable_release(ROUTING, ROOT, "unknown")


if __name__ == "__main__":
    unittest.main()

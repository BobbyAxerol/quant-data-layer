"""Offline TEST_ONLY fixtures; no sockets, manifests or runtime writes."""
from dataclasses import dataclass, replace
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts.p184_capacity100 import TSReservation, build_capacity100_candidate
from tests.test_phase3_target_workload import (
    VENUES, SYMBOLS, _Product, _Feed, _products,
)
from qdl.certification.phase3_consumer_load import TARGET_STAGE_MIX


@dataclass(frozen=True)
class Product(_Product):
    delivery: _Feed = _Feed("DURABLE")


def inputs():
    products = {key: tuple(Product(**vars(p)) for p in values)
                for key, values in _products().items()}
    manifests = {key: SimpleNamespace(quotas=SimpleNamespace(
        requests_per_minute=2400, max_streams=60, max_buffer_events=2000,
        max_batch_items=50, max_warmup_rows=10000))
        for key in VENUES.values()}
    manifests["ts"] = SimpleNamespace(quotas=SimpleNamespace(
        requests_per_minute=1500, max_streams=50, max_buffer_events=2000,
        max_batch_items=50, max_warmup_rows=10000))
    return dict(manifests=manifests, products_by_consumer=products,
                venue_identity=VENUES, instruments=SYMBOLS,
                ts=TSReservation("ts", 60, 50, 1000), gateway_subscription_limit=384)


class Capacity100Tests(unittest.TestCase):
    def setUp(self):
        self.args = inputs()
        self.network = patch("socket.socket", side_effect=AssertionError("offline only"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def plan(self, **changes):
        return build_capacity100_candidate(**(self.args | changes))

    def test_realistic_frozen_mix_and_products(self):
        plan, report = self.plan()
        self.assertEqual(plan.class_counts, (40, 30, 20, 10))
        self.assertEqual(plan.stream_count, 180)
        self.assertEqual(plan.hot_requests_per_second, 100)
        self.assertEqual(report["products_per_alpha"], {2: 40, 3: 30, 5: 30})
        self.assertEqual([s.ordinal for s in plan.sessions], list(range(1, 101)))
        self.assertEqual(len(plan.covered_instruments), 10)
        self.assertEqual(report["alpha_identity_count"], 2)
        self.assertEqual(report["total_identity_count"], 3)
        self.assertEqual(report["total_streams"], 230)
        self.assertEqual(report["reserved_streams"], 276)
        self.assertEqual(report["total_steady_rpm"], 7010)

    def test_current_quotas_report_shortfalls_without_shrinking_load(self):
        plan, report = self.plan()
        self.assertFalse(report["sealed_identity_quotas_fit"])
        alpha = [r for r in report["identities"] if r["role"] == "ALPHA"]
        self.assertEqual([r["logical_alphas"] for r in alpha], [50, 50])
        self.assertEqual([r["streams"] for r in alpha], [90, 90])
        self.assertEqual([r["candidate_rpm"] for r in alpha], [4506, 4509])
        self.assertEqual([r["candidate_streams"] for r in alpha], [108, 108])
        self.assertEqual([r["rpm_shortfall"] for r in alpha], [2106, 2109])
        self.assertEqual([r["stream_shortfall"] for r in alpha], [48, 48])
        self.assertEqual(plan.hot_requests_per_second, 100)

    def test_stress500_is_active_and_keeps_exactly_five_products(self):
        plan, report = self.plan(profile="stress5")
        self.assertEqual(plan.stream_count, 500)
        self.assertEqual(report["products_per_alpha"], {5: 100})
        self.assertEqual(report["workload_groups"], {"STRESS5": 100})
        for session in plan.sessions:
            self.assertEqual(len({p.identity for p in session.streams}), 5)
            self.assertEqual({p.feed.value for p in session.streams},
                             {"BAR", "TRADE", "QUOTE", "BOOK_DELTA", "BOOK_SNAPSHOT"})
            self.assertEqual(session.polls[0].period_seconds, 1)
            self.assertEqual(session.startup_snapshots[0].feed.value, "BOOK_SNAPSHOT")
        self.assertEqual(report["total_streams"], 550)
        self.assertEqual(report["reserved_streams"], 660)
        self.assertEqual(report["gateway_stream_shortfall"], 276)
        self.assertFalse(report["gateway_slots_fit"])
        self.assertEqual([d.streams_needed for d in plan.demands], [300, 300])
        self.assertEqual([d.quota_needed for d in plan.demands], [4500, 4500])

    def test_ten_mark_streams_need_seventy_two_ts_slots(self):
        for profile, total, reserved in (("realistic", 240, 288), ("stress5", 560, 672)):
            _, report = self.plan(profile=profile, ts=TSReservation("ts", 60, 60, 1000),
                                  gateway_subscription_limit=1024)
            self.assertEqual(report["total_streams"], total)
            self.assertEqual(report["reserved_streams"], reserved)
            ts = next(row for row in report["identities"] if row["role"] == "TS")
            self.assertEqual(ts["candidate_streams"], 72)
            self.assertEqual(ts["stream_shortfall"], 22)
            self.assertEqual(ts["candidate_rpm"], 1500)
            self.assertTrue(report["gateway_slots_fit"])
            self.assertFalse(report["sealed_identity_quotas_fit"])

    def test_sdk_snapshot_plus_subscribe_handoff_is_reserved(self):
        _, report = self.plan(profile="stress5")
        alpha = [r for r in report["identities"] if r["role"] == "ALPHA"]
        # 50 sessions x (five two-request handoffs + BAR warmup + book anchor).
        self.assertEqual([r["startup_requests"] for r in alpha], [600, 600])
        # Same minute: 1.25 x 3000 steady + startup and full reconnect envelopes.
        self.assertEqual([r["candidate_rpm"] for r in alpha], [4950, 4950])
        self.assertEqual([r["rpm_headroom"] for r in alpha], [1950, 1950])

    def test_existing1024_envelope_not_a_capacity_certificate(self):
        _, report = self.plan(profile="stress5", gateway_subscription_limit=1024)
        self.assertTrue(report["gateway_slots_fit"])
        self.assertFalse(report["sealed_identity_quotas_fit"])
        self.assertEqual(report["status"], "CANDIDATE_NOT_CERTIFIED")
        old, candidate = report["resource_envelopes"]
        self.assertEqual(old["estimated_required_bytes"], 247 * 2**20)
        self.assertEqual(candidate["estimated_required_bytes"], 307 * 2**20)
        self.assertEqual(old["active_stream_shortfall"], 166)
        self.assertEqual(candidate["reserved_stream_shortfall"], 0)
        self.assertEqual(candidate["live_queue_mib"], 32)
        self.assertEqual(report["resource_observation"]["per_client_buffers"], "UNCHANGED")
        self.assertTrue(report["pending_gates"])

    def test_exact_quota_candidate_can_fit_without_certification(self):
        _, first = self.plan(profile="stress5", gateway_subscription_limit=1024)
        for row in first["identities"]:
            quotas = self.args["manifests"][row["consumer_id"]].quotas
            quotas.requests_per_minute = row["candidate_rpm"]
            quotas.max_streams = row["candidate_streams"]
        _, report = self.plan(profile="stress5", gateway_subscription_limit=1024)
        self.assertTrue(report["sealed_identity_quotas_fit"])
        self.assertEqual(report["status"], "CANDIDATE_NOT_CERTIFIED")
        self.args["manifests"]["ts"].quotas.requests_per_minute -= 1
        self.assertFalse(self.plan(profile="stress5")[1]["sealed_identity_quotas_fit"])

    def test_ts_routes_are_not_assumed_to_be_streams(self):
        _, report = self.plan(ts=TSReservation("ts", 60, 70, 2000))
        ts = next(r for r in report["identities"] if r["role"] == "TS")
        self.assertEqual(report["ts_demanded_routes"], 60)
        self.assertEqual(ts["streams"], 70)
        self.assertEqual(ts["candidate_streams"], 84)
        self.assertEqual(ts["candidate_rpm"], 3000)

    def test_burst_startup_reconnect_can_exceed_floor_for_ts(self):
        _, report = self.plan(ts=TSReservation("ts", 60, 50, 1))
        ts = next(r for r in report["identities"] if r["role"] == "TS")
        self.assertEqual(ts["candidate_rpm"], 102)

    def test_no_global_stage_or_input_mutation_and_deterministic(self):
        before = dict(TARGET_STAGE_MIX)
        first = self.plan()
        self.assertEqual(first, self.plan())
        self.assertEqual(before, TARGET_STAGE_MIX)
        self.assertNotIn(100, TARGET_STAGE_MIX)
        self.assertEqual(self.args["manifests"]["ts"].quotas.max_streams, 50)
        json.dumps(first[1], allow_nan=False)

    def test_missing_stress_feed_refused(self):
        key = VENUES["BINANCE"]
        self.args["products_by_consumer"][key] = tuple(
            p for p in self.args["products_by_consumer"][key] if p.feed.value != "TRADE")
        with self.assertRaisesRegex(ValueError, "TRADE"):
            self.plan(profile="stress5")

    def test_ambiguous_stress_feed_refused(self):
        key = VENUES["BINANCE"]
        values = self.args["products_by_consumer"][key]
        self.args["products_by_consumer"][key] += tuple(p for p in values if p.feed.value == "TRADE")
        with self.assertRaisesRegex(ValueError, "exactly one TRADE"):
            self.plan(profile="stress5")

    def test_on_demand_cannot_be_counted_as_live(self):
        key = VENUES["BINANCE"]
        self.args["products_by_consumer"][key] = tuple(
            replace(p, delivery=_Feed("ON_DEMAND")) if p.feed.value == "BOOK_DELTA" else p
            for p in self.args["products_by_consumer"][key])
        for profile in ("realistic", "stress5"):
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, "DURABLE"):
                self.plan(profile=profile)

    def test_missing_ts_manifest_refused(self):
        del self.args["manifests"]["ts"]
        with self.assertRaisesRegex(ValueError, "missing"):
            self.plan()

    def test_shared_ts_alpha_identity_refused(self):
        with self.assertRaisesRegex(ValueError, "separate identity"):
            self.plan(ts=TSReservation(VENUES["BINANCE"], 60, 50, 1000))

    def test_cross_identity_product_refused(self):
        key = VENUES["BINANCE"]
        products = self.args["products_by_consumer"][key]
        self.args["products_by_consumer"][key] = (replace(products[0], consumer_id="other"), *products[1:])
        with self.assertRaisesRegex(ValueError, "mix authenticated"):
            self.plan()

    def test_invalid_reservations_and_limits(self):
        for value in (0, -1, 1.5, float("nan"), True):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    TSReservation("ts", 60, value, 1)
                with self.assertRaises(ValueError):
                    TSReservation("ts", 60, 1, value)
                with self.assertRaises(ValueError):
                    self.plan(gateway_subscription_limit=value)
        with self.assertRaises(ValueError):
            TSReservation("ts", 59, 50, 1000)
        with self.assertRaises(ValueError):
            self.plan(profile="idle100")

    def test_service_buffer_boundary(self):
        self.args["manifests"]["ts"].quotas.max_buffer_events = 10001
        with self.assertRaisesRegex(ValueError, "10000-event"):
            self.plan()

    def test_nonpositive_sealed_quota_refused(self):
        self.args["manifests"]["ts"].quotas.requests_per_minute = 0
        with self.assertRaisesRegex(ValueError, "invalid sealed"):
            self.plan()

    def test_batch_and_warmup_shortfalls_do_not_shrink_workload(self):
        for key in VENUES.values():
            self.args["manifests"][key].quotas.max_batch_items = 1
            self.args["manifests"][key].quotas.max_warmup_rows = 4999
        plan, report = self.plan()
        self.assertEqual(plan.stream_count, 180)
        alpha = [r for r in report["identities"] if r["role"] == "ALPHA"]
        self.assertEqual([r["batch_shortfall"] for r in alpha], [1, 1])
        self.assertEqual([r["warmup_shortfall"] for r in alpha], [1, 1])
        self.assertFalse(report["sealed_identity_quotas_fit"])

    def test_query_service_boundaries(self):
        for field, value in (("max_batch_items", 101), ("max_warmup_rows", 10001)):
            with self.subTest(field=field):
                args = inputs()
                setattr(args["manifests"]["ts"].quotas, field, value)
                with self.assertRaisesRegex(ValueError, "batch/warmup"):
                    build_capacity100_candidate(**args)

    def test_nonexplicit_or_duplicate_scope_refused(self):
        for values in ((), ("BTCUSDT",), ("BTCUSDT", "BTCUSDT"), ("*", "BTCUSDT")):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.plan(instruments=SYMBOLS | {"BINANCE": values})


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest

from qdl.certification.phase105_release_observations import (
    MAX_OBSERVATION_AGE_MS,
    build_release_observation_bundle,
    compact_view_quality,
    expected_release_delivery,
    parse_release_observation_bundle,
)
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog


ROOT = Path(__file__).resolve().parents[1]
ROUTE_PATH = ROOT / "config/v2/stable-v2-release-routing.yaml"


class Phase105ReleaseObservationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = StableReleaseRoutePlan.load(ROUTE_PATH, manifest_root=ROOT)
        self.captured_at_ms = 1_000_000

    def acceptance(self) -> dict[str, object]:
        products: list[dict[str, object]] = []
        catalog = StableSourceCatalog.load(self.plan.source_catalog.path)
        requirements = {
            (consumer.consumer_id, requirement_key(requirement)): requirement
            for consumer in self.plan.consumers
            for requirement in consumer.manifest.requirements
        }
        for index, (consumer_id, product) in enumerate(self.plan.products()):
            if product.route != "V2_PRIMARY":
                continue
            instrument_uid, feed, interval, policy = product.requirement_key.split(":", 3)
            delivery = expected_release_delivery(
                catalog, requirements[(consumer_id, product.requirement_key)]
            )
            products.append({
                "consumer_id": consumer_id,
                "instrument_uid": instrument_uid,
                "feed": feed,
                "interval": interval or None,
                "source_policy_id": policy,
                "delivery": delivery,
                "acknowledged_offset": None if delivery != "DURABLE" else 100 + index,
                "resumed_offset": None if delivery != "DURABLE" else 101 + index,
                "release_quality": {
                    "primary": {
                        "source_age_ms": 10,
                        "receive_age_ms": 12,
                        "gap_open": False,
                    },
                    "secondary": {
                        "source_age_ms": 11,
                        "receive_age_ms": 13,
                        "gap_open": False,
                    },
                },
            })
        return {
            "schema": "qdl.phase105.v2-identity-acceptance.v1",
            "status": "PASS_V2_DATA_PLANE_ONLY",
            "release_route_plan_sha256": self.plan.digest,
            "release_capture": {
                "captured_at_ms": self.captured_at_ms,
                "cpu_millicores": 125,
                "rss_bytes": 64 * 1024 * 1024,
            },
            "products": products,
        }

    def expected_deliveries(self) -> dict[tuple[str, str], str]:
        catalog = StableSourceCatalog.load(self.plan.source_catalog.path)
        requirements = {
            (consumer.consumer_id, requirement_key(requirement)): requirement
            for consumer in self.plan.consumers
            for requirement in consumer.manifest.requirements
        }
        return {
            (consumer_id, product.requirement_key): expected_release_delivery(
                catalog, requirements[(consumer_id, product.requirement_key)]
            )
            for consumer_id, product in self.plan.products()
            if product.route == "V2_PRIMARY"
        }

    def test_full_manifest_bundle_marks_v1_only_routes_as_excluded(self) -> None:
        bundle = build_release_observation_bundle(self.plan, self.acceptance())
        values = parse_release_observation_bundle(
            self.plan, bundle, now_ms=self.captured_at_ms
        )
        self.assertEqual(len(values), 303)
        v1_values = [item for item in values if item.route == "V1_PRIMARY"]
        self.assertEqual(len(v1_values), 4)
        self.assertTrue(all(item.reason == "VN_REAL_PROVIDER_GATE_UNEXERCISED" for item in v1_values))
        self.assertTrue(all(item.v1_source_age_ms is None for item in v1_values))
        v2_values = [item for item in values if item.route == "V2_PRIMARY"]
        self.assertEqual(len(v2_values), 299)
        self.assertTrue(all(item.v2_source_age_ms == 11 for item in v2_values))
        deliveries = self.expected_deliveries()
        self.assertEqual(
            sum(
                value == "ON_DEMAND"
                for (consumer_id, _key), value in deliveries.items()
                if consumer_id == "alpha.binance.paper.stable"
            ),
            35,
        )
        self.assertEqual(
            sum(
                value == "ON_DEMAND"
                for (consumer_id, _key), value in deliveries.items()
                if consumer_id == "alpha.okx.paper.stable"
            ),
            20,
        )
        for (consumer_id, product), value in zip(
            (
                item for item in self.plan.products()
                if item[1].route == "V2_PRIMARY"
            ),
            v2_values,
            strict=True,
        ):
            expected = deliveries[(consumer_id, product.requirement_key)]
            self.assertEqual(value.consumer_lag, 1 if expected == "DURABLE" else 0)

    def test_bundle_rejects_missing_route_bad_resume_and_stale_capture(self) -> None:
        acceptance = self.acceptance()
        products = acceptance["products"]
        self.assertIsInstance(products, list)
        missing = dict(acceptance)
        missing["products"] = products[:-1]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            build_release_observation_bundle(self.plan, missing)

        invalid_resume = self.acceptance()
        invalid_products = invalid_resume["products"]
        self.assertIsInstance(invalid_products, list)
        durable_index = next(
            index for index, product in enumerate(invalid_products)
            if product["delivery"] == "DURABLE"
        )
        invalid_products[durable_index] = dict(
            invalid_products[durable_index], resumed_offset=100 + durable_index
        )
        with self.assertRaisesRegex(ValueError, "did not advance"):
            build_release_observation_bundle(self.plan, invalid_resume)

        invalid_delivery = self.acceptance()
        invalid_delivery_products = invalid_delivery["products"]
        self.assertIsInstance(invalid_delivery_products, list)
        reference_index = next(
            index for index, product in enumerate(invalid_delivery_products)
            if product["delivery"] == "ON_DEMAND"
        )
        invalid_delivery_products[reference_index] = dict(
            invalid_delivery_products[reference_index],
            delivery="DURABLE",
            acknowledged_offset=100 + reference_index,
            resumed_offset=101 + reference_index,
        )
        with self.assertRaisesRegex(ValueError, "delivery differs"):
            build_release_observation_bundle(self.plan, invalid_delivery)

        bundle = build_release_observation_bundle(self.plan, self.acceptance())
        with self.assertRaisesRegex(ValueError, "stale"):
            parse_release_observation_bundle(
                self.plan,
                bundle,
                now_ms=self.captured_at_ms + MAX_OBSERVATION_AGE_MS + 1,
            )

    def test_quiet_bar_observation_requires_two_final_bar_sessions(self) -> None:
        from qdl.certification.phase105_release_observations import _durable_no_cursor_lag
        value = {"feed": "BAR", "stream_handoff": "CURRENT_FINAL_BAR_OBSERVED_NO_CURSOR",
                 "stream_no_event_sessions": ["CURRENT_FINAL_BAR", "CURRENT_FINAL_BAR"]}
        self.assertEqual(_durable_no_cursor_lag(value, index=0), 0)
        for fields in ({"feed": "TRADE"}, {"stream_no_event_sessions": []},
                       {"stream_no_event_sessions": ["CURRENT_FINAL_BAR", "UNKNOWN"]}):
            with self.subTest(fields=fields), self.assertRaisesRegex(ValueError, "both current sessions"):
                _durable_no_cursor_lag(dict(value, **fields), index=0)

    def test_bundle_accepts_proven_live_durable_handoff_without_cursor(self) -> None:
        acceptance = self.acceptance()
        products = acceptance["products"]
        self.assertIsInstance(products, list)
        trade_index = next(
            index for index, product in enumerate(products)
            if product["delivery"] == "DURABLE" and product["feed"] == "TRADE"
        )
        product = products[trade_index]
        products[trade_index] = dict(
            product,
            acknowledged_offset=None,
            resumed_offset=None,
            stream_handoff="LIVE_EVENT_AFTER_REOPEN_NO_CURSOR",
            stream_no_event_sessions=["QUIET_NON_EXECUTABLE", "EVENT_AFTER_REOPEN"],
        )

        bundle = build_release_observation_bundle(self.plan, acceptance)
        values = parse_release_observation_bundle(
            self.plan, bundle, now_ms=self.captured_at_ms
        )
        instrument_uid = product["instrument_uid"]
        requirement = ":".join((
            instrument_uid,
            product["feed"],
            product["interval"] or "",
            product["source_policy_id"],
        ))
        observation = next(
            item for item in values
            if item.consumer_id == product["consumer_id"]
            and item.requirement_key == requirement
        )
        self.assertEqual(observation.consumer_lag, 0)

        invalid = self.acceptance()
        invalid_products = invalid["products"]
        self.assertIsInstance(invalid_products, list)
        invalid_product = invalid_products[trade_index]
        invalid_products[trade_index] = dict(
            invalid_product,
            acknowledged_offset=None,
            resumed_offset=None,
            stream_handoff="UNPROVEN_NO_CURSOR",
            stream_no_event_sessions=["QUIET_NON_EXECUTABLE", "EVENT_AFTER_REOPEN"],
        )
        with self.assertRaisesRegex(ValueError, "no-cursor handoff"):
            build_release_observation_bundle(self.plan, invalid)

        missing_sessions = self.acceptance()
        missing_session_products = missing_sessions["products"]
        self.assertIsInstance(missing_session_products, list)
        missing_product = missing_session_products[trade_index]
        missing_session_products[trade_index] = dict(
            missing_product,
            acknowledged_offset=None,
            resumed_offset=None,
            stream_handoff="LIVE_OBSERVED_NO_NEW_CURSOR",
        )
        with self.assertRaisesRegex(ValueError, "no-cursor sessions"):
            build_release_observation_bundle(self.plan, missing_sessions)

    def test_compact_view_quality_rejects_missing_or_future_receive_time(self) -> None:
        view = SimpleNamespace(
            received_at_ns=999_000_000,
            quality=SimpleNamespace(freshness_ms=7, gap_open=False),
        )
        self.assertEqual(
            compact_view_quality(view, observed_at_ns=1_000_000_000),
            {"source_age_ms": 7, "receive_age_ms": 1, "gap_open": False},
        )
        with self.assertRaisesRegex(ValueError, "future"):
            compact_view_quality(view, observed_at_ns=998_000_000)
        with self.assertRaisesRegex(ValueError, "positive"):
            compact_view_quality(
                SimpleNamespace(
                    received_at_ns=None,
                    quality=SimpleNamespace(freshness_ms=7, gap_open=False),
                ),
                observed_at_ns=1_000_000_000,
            )


if __name__ == "__main__":
    unittest.main()

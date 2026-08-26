from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest

from qdl.certification.phase105_release_observations import (
    MAX_OBSERVATION_AGE_MS,
    build_release_observation_bundle,
    compact_view_quality,
    parse_release_observation_bundle,
)
from qdl.consumer import StableReleaseRoutePlan


ROOT = Path(__file__).resolve().parents[1]
ROUTE_PATH = ROOT / "config/v2/stable-v2-release-routing.yaml"


class Phase105ReleaseObservationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = StableReleaseRoutePlan.load(ROUTE_PATH, manifest_root=ROOT)
        self.captured_at_ms = 1_000_000

    def acceptance(self) -> dict[str, object]:
        products: list[dict[str, object]] = []
        for index, (consumer_id, product) in enumerate(self.plan.products()):
            if product.route != "V2_PRIMARY":
                continue
            instrument_uid, feed, interval, policy = product.requirement_key.split(":", 3)
            products.append({
                "consumer_id": consumer_id,
                "instrument_uid": instrument_uid,
                "feed": feed,
                "interval": interval or None,
                "source_policy_id": policy,
                "delivery": "DURABLE",
                "acknowledged_offset": 100 + index,
                "resumed_offset": 101 + index,
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

    def test_full_manifest_bundle_marks_v1_only_routes_as_excluded(self) -> None:
        bundle = build_release_observation_bundle(self.plan, self.acceptance())
        values = parse_release_observation_bundle(
            self.plan, bundle, now_ms=self.captured_at_ms
        )
        self.assertEqual(len(values), 32)
        v1_values = [item for item in values if item.route == "V1_PRIMARY"]
        self.assertEqual(len(v1_values), 4)
        self.assertTrue(all(item.reason == "VN_REAL_PROVIDER_GATE_UNEXERCISED" for item in v1_values))
        self.assertTrue(all(item.v1_source_age_ms is None for item in v1_values))
        v2_values = [item for item in values if item.route == "V2_PRIMARY"]
        self.assertEqual(len(v2_values), 28)
        self.assertTrue(all(item.v2_source_age_ms == 11 for item in v2_values))
        self.assertTrue(all(item.consumer_lag == 1 for item in v2_values))

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
        invalid_products[0] = dict(invalid_products[0], resumed_offset=100)
        with self.assertRaisesRegex(ValueError, "did not advance"):
            build_release_observation_bundle(self.plan, invalid_resume)

        bundle = build_release_observation_bundle(self.plan, self.acceptance())
        with self.assertRaisesRegex(ValueError, "stale"):
            parse_release_observation_bundle(
                self.plan,
                bundle,
                now_ms=self.captured_at_ms + MAX_OBSERVATION_AGE_MS + 1,
            )

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

from __future__ import annotations

import unittest

from qdl.consumer import (
    ConsumerRoute,
    RealtimeRoute,
    RealtimeSliceObservation,
    select_realtime_route,
)
from qdl.query import CoverageStatus
from tests.phase7_support import make_manifest


class RealtimeRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = make_manifest(
            consumer_id="phase10.alpha",
            subject="spiffe://qdl/paper/phase10-alpha",
            instrument_uid="binance-usdm-eth",
            feed="BAR",
            interval="1m",
        )
        self.requirement = self.manifest.requirements[0]

    @staticmethod
    def _observation(
        *,
        available: bool = True,
        source_age_ms: int | None = 100,
        gap_open: bool = False,
        final_bar_available: bool = True,
        market_closed: bool = False,
    ) -> RealtimeSliceObservation:
        return RealtimeSliceObservation(
            available=available,
            authoritative=True,
            coverage=CoverageStatus.FULL,
            source_age_ms=source_age_ms,
            receive_age_ms=source_age_ms,
            gap_open=gap_open,
            final_bar_available=final_bar_available,
            market_closed=market_closed,
        )

    def _select(self, **kwargs):
        values = {
            "v2_observation": self._observation(),
            "v1_observation": self._observation(),
            "v1_compatible": True,
        }
        values.update(kwargs)
        return select_realtime_route(
            manifest=self.manifest,
            migration_route=ConsumerRoute.V2,
            requirement=self.requirement,
            **values,
        )

    def test_ready_v2_is_primary_and_audit_is_bounded(self):
        decision = self._select()
        self.assertEqual(decision.route, RealtimeRoute.V2_PRIMARY)
        self.assertEqual(decision.reason, "V2_READY")
        audit = decision.audit_record()
        self.assertFalse(audit["fallback_used"])
        self.assertEqual(audit["v2_source_age_ms"], 100)
        self.assertNotIn("payload", audit)

    def test_stale_v2_uses_only_a_fresh_compatible_v1_fallback(self):
        decision = self._select(
            v2_observation=self._observation(source_age_ms=10_001),
            v1_observation=self._observation(source_age_ms=50),
        )
        self.assertEqual(decision.route, RealtimeRoute.V1_FALLBACK)
        self.assertEqual(decision.reason, "V2_DATA_STALE")
        self.assertTrue(decision.fallback_used)

    def test_gap_or_missing_final_bar_is_not_hidden(self):
        for observation in (
            self._observation(gap_open=True),
            self._observation(final_bar_available=False),
        ):
            with self.subTest(observation=observation):
                decision = self._select(
                    v2_observation=observation,
                    v1_observation=self._observation(source_age_ms=20),
                )
                self.assertEqual(decision.route, RealtimeRoute.V1_FALLBACK)
                self.assertIn(decision.reason, {"V2_OPEN_SEQUENCE_GAP", "V2_DATA_NOT_READY"})

    def test_stale_or_incompatible_v1_keeps_execution_blocked(self):
        stale_v2 = self._observation(source_age_ms=10_001)
        stale_v1 = self._observation(source_age_ms=10_001)
        decision = self._select(v2_observation=stale_v2, v1_observation=stale_v1)
        self.assertEqual(decision.route, RealtimeRoute.BLOCKED)
        self.assertEqual(decision.reason, "V1_FALLBACK_NOT_READY")

        incompatible = self._select(v2_observation=stale_v2, v1_compatible=False)
        self.assertEqual(incompatible.route, RealtimeRoute.BLOCKED)
        self.assertEqual(incompatible.reason, "V1_COMPATIBILITY_NOT_APPROVED")

    def test_market_closed_never_routes_to_v1_fallback(self):
        decision = self._select(
            v2_observation=self._observation(
                available=False,
                source_age_ms=None,
                market_closed=True,
            ),
            v1_observation=self._observation(source_age_ms=20),
        )
        self.assertEqual(decision.route, RealtimeRoute.BLOCKED)
        self.assertEqual(decision.reason, "MARKET_CLOSED")

    def test_shadow_and_rollback_manifest_routes_remain_v1_primary(self):
        decision = select_realtime_route(
            manifest=self.manifest,
            migration_route=ConsumerRoute.V1_WITH_V2_SHADOW,
            requirement=self.requirement,
            v2_observation=self._observation(source_age_ms=10_001),
            v1_observation=self._observation(source_age_ms=20),
            v1_compatible=True,
        )
        self.assertEqual(decision.route, RealtimeRoute.V1_PRIMARY)
        self.assertEqual(decision.reason, "MANIFEST_ROUTE_V1")


if __name__ == "__main__":
    unittest.main()

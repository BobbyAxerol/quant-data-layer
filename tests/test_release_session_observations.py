from dataclasses import replace
import unittest

from qdl.consumer.release import ReleaseRouteObservation, v2_observation_is_current
from qdl.certification.phase105_release import parse_release_observations
from qdl.certification.phase105_release_observations import compact_view_quality
from qdl.query.contracts import ConsumerGrade, DataRequirement, FeedType, StalePolicy
from types import SimpleNamespace


class ReleaseSessionObservationTests(unittest.TestCase):
    def requirement(self, feed=FeedType.TRADE, policy=StalePolicy.OBSERVE):
        return DataRequirement(instrument_uid="test-only", feed=feed,
            consumer_grade=ConsumerGrade.EXECUTION, source_policy_id="test-policy",
            max_freshness_ms=3000, max_session_liveness_ms=45000,
            event_recency_policy=policy)

    def observation(self):
        return ReleaseRouteObservation(consumer_id="test-only", requirement_key="test-only",
            route="V2_PRIMARY", reason="V2_READY", v2_source_age_ms=12000,
            v2_receive_age_ms=12050, v2_gap_open=False, v1_source_age_ms=None,
            v1_receive_age_ms=None, consumer_lag=0, cpu_millicores=1, rss_bytes=1,
            v2_quality_state="LIVE", v2_session_state="LIVE", v2_session_liveness_ms=1000,
            v2_complete=True, v2_execution_eligible=False)

    def test_quiet_session_matrix_preserves_actual_age_and_price_safety(self):
        for venue in ("BINANCE", "OKX"):
            for symbol in ("BTC", "ETH", "SOL", "DOGE", "BNB"):
                for feed in (FeedType.TRADE, FeedType.BOOK_DELTA):
                    with self.subTest(venue=venue, symbol=symbol, feed=feed):
                        observation = replace(self.observation(), requirement_key=f"{venue}:{symbol}:{feed.value}")
                        self.assertTrue(v2_observation_is_current(self.requirement(feed), observation))
                        self.assertEqual(observation.v2_source_age_ms, 12000)
                        self.assertFalse(observation.v2_execution_eligible)
                        self.assertEqual(parse_release_observations([observation.public_record()]), (observation,))
                        for policy in (StalePolicy.BLOCK, StalePolicy.PAUSE):
                            self.assertFalse(v2_observation_is_current(self.requirement(feed, policy), observation))
        for feed in (FeedType.QUOTE, FeedType.BOOK_SNAPSHOT, FeedType.MARK_INDEX_PRICE):
            self.assertFalse(v2_observation_is_current(self.requirement(feed), self.observation()))

    def test_incomplete_gap_expired_disconnected_or_unproven_session_stays_blocked(self):
        for fields in (
            {"v2_gap_open": True}, {"v2_complete": False}, {"v2_quality_state": "STALE"},
            {"v2_session_state": "DISCONNECTED"}, {"v2_session_state": "UNKNOWN"},
            {"v2_session_state": "STALE"}, {"v2_session_state": "NOT_APPLICABLE"},
            {"v2_session_liveness_ms": None}, {"v2_session_liveness_ms": 45001},
            {"v2_execution_eligible": True},
            {"v2_quality_state": None, "v2_session_state": None,
             "v2_session_liveness_ms": None, "v2_complete": None, "v2_execution_eligible": None},
        ):
            with self.subTest(fields=fields):
                self.assertFalse(v2_observation_is_current(self.requirement(), replace(self.observation(), **fields)))

    def test_fresh_price_still_requires_typed_live_complete_session(self):
        observation = replace(self.observation(), v2_source_age_ms=100,
                              v2_receive_age_ms=200, v2_execution_eligible=True)
        requirement = self.requirement(FeedType.QUOTE, StalePolicy.BLOCK)
        self.assertTrue(v2_observation_is_current(requirement, observation))
        self.assertFalse(v2_observation_is_current(requirement,
            replace(observation, v2_session_state="DISCONNECTED")))

    def test_compact_quality_does_not_discard_session_or_make_quiet_data_executable(self):
        quality = SimpleNamespace(freshness_ms=12000, gap_open=False, state="LIVE",
            provider_session_state="LIVE", provider_session_liveness_ms=900,
            complete=True, execution_eligible=False)
        evidence = compact_view_quality(SimpleNamespace(quality=quality, received_at_ns=1000000),
                                        observed_at_ns=13000000)
        self.assertEqual(evidence["source_age_ms"], 12000)
        self.assertEqual(evidence["provider_session_liveness_ms"], 900)
        self.assertFalse(evidence["execution_eligible"])

    def test_typed_evidence_rejects_bad_types_and_extra_fields(self):
        for fields in ({"v2_session_liveness_ms": True}, {"v2_complete": 1},
                       {"v2_quality_state": "UNVALIDATED"}, {"v2_session_liveness_ms": -1}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                replace(self.observation(), **fields)
        with self.assertRaises(ValueError):
            parse_release_observations([{**self.observation().public_record(), "extra": True}])

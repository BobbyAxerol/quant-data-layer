"""DL-V2 R1.3: a DATA_STALE rejection must say which predicate failed.

Three independent facts were folded into one boolean and one sentence, so a
stale rejection could not distinguish an event that is simply old from a
provider session the venue reported as bad from a session whose liveness
exceeded the consumer's own bound. Those are different faults with different
fixes, and every investigation had to re-derive which one it was from the
runtime.

The admission rule is unchanged. These tests hold both halves of that claim:
each cause reports its own reason, and the same inputs still admit or reject
exactly as before.
"""

from __future__ import annotations

import unittest

from qdl.query import (
    STALE_REASON_EVENT_AGE,
    STALE_REASON_SESSION_LIVENESS,
    STALE_REASON_SESSION_STATE,
    CanonicalErrorCode,
    CoverageStatus,
    DataRequirement,
    evaluate_requirement,
)
from qdl.query.contracts import ConsumerGrade, FeedType, StalePolicy
from qdl.query.results import QualityMetadata as _Q
from qdl.query.service import _freshness_verdict


def requirement(**overrides) -> DataRequirement:
    base = dict(
        instrument_uid="BINANCE:USD_M:BTCUSDT",
        feed=FeedType.QUOTE,
        consumer_grade=ConsumerGrade.EXECUTION,
        source_policy_id="crypto_primary_v2",
        max_freshness_ms=2_000,
    )
    base.update(overrides)
    return DataRequirement(**base)


def quality(**overrides) -> _Q:
    base = dict(
        state="LIVE",
        freshness_ms=100,
        gap_open=False,
        complete=True,
        execution_eligible=True,
        policy_id="crypto_primary_v2",
        provider_session_state="LIVE",
        provider_session_liveness_ms=100,
    )
    base.update(overrides)
    return _Q(**base)


class FreshnessVerdictTests(unittest.TestCase):
    def test_a_live_fresh_slice_has_no_reason(self) -> None:
        self.assertEqual(_freshness_verdict(requirement(), quality()), (True, None))

    def test_an_old_event_reports_event_age(self) -> None:
        verdict = _freshness_verdict(requirement(), quality(freshness_ms=9_000))
        self.assertEqual(verdict, (False, STALE_REASON_EVENT_AGE))

    def test_a_stale_quality_state_reports_event_age(self) -> None:
        for state in ("STALE", "OFFLINE", "UNAVAILABLE"):
            with self.subTest(state=state):
                verdict = _freshness_verdict(requirement(), quality(state=state))
                self.assertEqual(verdict, (False, STALE_REASON_EVENT_AGE))

    def test_a_bad_provider_session_reports_session_state(self) -> None:
        for state in ("STALE", "DISCONNECTED", "UNKNOWN"):
            with self.subTest(state=state):
                verdict = _freshness_verdict(
                    requirement(), quality(provider_session_state=state)
                )
                self.assertEqual(verdict, (False, STALE_REASON_SESSION_STATE))

    def test_liveness_above_the_consumer_bound_reports_session_liveness(self) -> None:
        verdict = _freshness_verdict(
            requirement(max_session_liveness_ms=45_000),
            quality(provider_session_liveness_ms=90_000),
        )
        self.assertEqual(verdict, (False, STALE_REASON_SESSION_LIVENESS))

    def test_a_missing_liveness_measurement_is_not_read_as_live(self) -> None:
        verdict = _freshness_verdict(
            requirement(max_session_liveness_ms=45_000),
            quality(provider_session_liveness_ms=None),
        )
        self.assertEqual(verdict, (False, STALE_REASON_SESSION_LIVENESS))

    def test_a_closed_market_is_fresh_and_unblamed(self) -> None:
        verdict = _freshness_verdict(
            requirement(), quality(state="MARKET_CLOSED", freshness_ms=10_000_000)
        )
        self.assertEqual(verdict, (True, None))

    def test_an_observed_recency_policy_does_not_make_age_stale(self) -> None:
        """OBSERVE is why a six minute old trade never showed as stale.

        The contract refuses OBSERVE without an explicit provider session SLA
        (`contracts.py:229`), so the feed is not unguarded: age stops blocking,
        but a dead or slow provider session still does. That is the boundary
        worth pinning, because it is the difference between a quiet market and
        a broken feed.
        """

        observed = requirement(
            feed=FeedType.TRADE,
            event_recency_policy=StalePolicy.OBSERVE,
            max_freshness_ms=3_000,
            max_session_liveness_ms=45_000,
        )
        self.assertEqual(
            _freshness_verdict(observed, quality(freshness_ms=360_000)), (True, None)
        )
        self.assertEqual(
            _freshness_verdict(
                observed,
                quality(freshness_ms=360_000, provider_session_liveness_ms=90_000),
            ),
            (False, STALE_REASON_SESSION_LIVENESS),
        )

    def test_session_state_is_checked_before_event_age(self) -> None:
        """A disconnected provider is the more actionable of two true facts."""

        verdict = _freshness_verdict(
            requirement(), quality(freshness_ms=9_000, provider_session_state="DISCONNECTED")
        )
        self.assertEqual(verdict, (False, STALE_REASON_SESSION_STATE))


class ProblemDetailTests(unittest.TestCase):
    def _problem(self, **kwargs):
        return evaluate_requirement(
            requirement(),
            coverage=CoverageStatus.FULL,
            entitled=True,
            available=True,
            authoritative=True,
            gap_open=False,
            **kwargs,
        )

    def test_the_reason_reaches_the_problem_detail(self) -> None:
        problem = self._problem(fresh=False, stale_reason=STALE_REASON_SESSION_LIVENESS)
        self.assertEqual(problem.code, CanonicalErrorCode.DATA_STALE)
        self.assertIn("SESSION_LIVENESS", problem.detail)
        self.assertTrue(problem.detail.startswith("required data exceeds its freshness policy"))
        self.assertTrue(problem.retryable)

    def test_omitting_the_reason_keeps_the_previous_message_exactly(self) -> None:
        """Additive: a caller that supplies nothing sees no change."""

        problem = self._problem(fresh=False)
        self.assertEqual(problem.detail, "required data exceeds its freshness policy")

    def test_an_undeclared_reason_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self._problem(fresh=False, stale_reason="BECAUSE_I_SAID_SO")

    def test_a_reason_never_changes_admission(self) -> None:
        self.assertIsNone(self._problem(fresh=True))
        self.assertIsNone(self._problem(fresh=True, stale_reason=None))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

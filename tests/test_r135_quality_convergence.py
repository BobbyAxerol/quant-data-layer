"""R1.35-A: one quality policy across core, query, SDK and audit inputs.

The fixture is deterministic test provenance.  It deliberately carries no
provider payload, price, credential or runtime state; the real-provider matrix
belongs to R1.35-B/C after this source-only contract is sealed.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from qdl.data_quality.binding_decision import (
    BindingQualityInput,
    ComponentEvidence,
    evaluate_binding_quality,
    freshness_verdict,
)
from qdl.query import DataRequirement
from qdl.query.contracts import ConsumerGrade, FeedType, StalePolicy
from qdl.query.results import QualityMetadata
from qdl.query.service import _freshness_verdict


FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "contracts/golden/quality/binding-quality-decision-v1.json"
)


class BindingQualityGoldenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_python_matches_shared_golden_corpus(self) -> None:
        self.assertEqual(self.fixture["schema"], "qdl.binding-quality-decision.v1")
        self.assertGreaterEqual(len(self.fixture["cases"]), 14)
        for case in self.fixture["cases"]:
            with self.subTest(case=case["name"]):
                raw = dict(case["input"])
                raw["components"] = tuple(
                    ComponentEvidence(**component)
                    for component in raw["components"]
                )
                raw["flags"] = tuple(raw["flags"])
                decision = evaluate_binding_quality(BindingQualityInput(**raw))
                expected = case["expected"]
                self.assertEqual(decision.semantics.value, expected["semantics"])
                self.assertEqual(decision.availability.value, expected["availability"])
                self.assertEqual(decision.state, expected["state"])
                self.assertEqual(
                    decision.event_recency_state, expected["event_recency_state"]
                )
                self.assertEqual(decision.complete, expected["complete"])
                self.assertEqual(
                    decision.execution_eligible, expected["execution_eligible"]
                )
                self.assertEqual(list(decision.reason_codes), expected["reason_codes"])

    def test_quiet_semantics_cannot_relabel_quote_as_executable(self) -> None:
        quiet = next(
            case for case in self.fixture["cases"]
            if case["name"] == "quiet_mark_index_components_live"
        )
        raw = dict(quiet["input"])
        raw.update({
            "feed": "QUOTE",
            "event_recency_policy": "OBSERVE",
            "allow_quiet_execution": True,
            "components": (),
        })
        decision = evaluate_binding_quality(BindingQualityInput(**raw))
        self.assertEqual(decision.semantics.value, "STRICT_EVENT")
        self.assertEqual(decision.state, "STALE")
        self.assertFalse(decision.execution_eligible)

    def test_query_admission_matches_quality_metadata_without_erasing_gap(self) -> None:
        requirement = DataRequirement(
            instrument_uid="BINANCE:USD_M:BTCUSDT",
            feed=FeedType.QUOTE,
            consumer_grade=ConsumerGrade.EXECUTION,
            source_policy_id="crypto_primary_v2",
            max_freshness_ms=2_000,
            event_recency_policy=StalePolicy.BLOCK,
            max_session_liveness_ms=45_000,
        )
        metadata = QualityMetadata(
            state="GAPPED",
            freshness_ms=100,
            gap_open=True,
            complete=False,
            execution_eligible=False,
            policy_id=requirement.source_policy_id,
            event_recency_state="LIVE",
            provider_session_state="LIVE",
            provider_session_liveness_ms=10,
        )
        self.assertEqual(_freshness_verdict(requirement, metadata), (True, None))
        self.assertEqual(
            freshness_verdict(
                state=metadata.state,
                freshness_ms=metadata.freshness_ms,
                event_recency_policy=requirement.effective_event_recency_policy.value,
                max_freshness_ms=requirement.max_freshness_ms,
                provider_session_state=metadata.provider_session_state,
                provider_session_liveness_ms=metadata.provider_session_liveness_ms,
                max_session_liveness_ms=requirement.max_session_liveness_ms,
            ),
            (True, None),
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

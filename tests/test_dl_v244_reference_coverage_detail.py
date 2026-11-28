"""DL-DEF-2026-11-26-G: a reference history that misses part of the window is refused with typed tokens and range.

Incident (2026-11-26 VN, hosted dynamic_grid). The request was FUNDING_RATE ETH-USDT-SWAP, ALPHA, 2025-10-05 →
2026-11-25 (416 days), require_full_coverage, request a38214cf. OKX funding-rate-history ran out of records
(``PROVIDER_EXHAUSTED``; the venue keeps about 3 months) before the requested start. Query refused with
PARTIAL_RESULT, which is correct, but the detail was the fixed text "reference provider history did not cover the
requested complete window". The SDK keeps only UPPER_SNAKE tokens of a detail, so the consumer saw
``reason_tokens=[]``. Only Query's own log line carried PROVIDER_EXHAUSTED.
"""
from __future__ import annotations

import types
import unittest

from qdl.query.service import V2QueryService, _reference_coverage_detail
from qdl.reference.contracts import ReferenceCoverage, ReferenceProduct, ReferenceStatus
from qdl_sdk.client import _batch_item_problems

DAY_MS = 86_400_000
START_MS, END_MS = 1_759_629_600_000, 1_795_629_600_000  # the hosted request window
OBSERVED_MIN_MS = END_MS - 92 * DAY_MS


def coverage(**overrides) -> ReferenceCoverage:
    values = dict(requested_start_ms=START_MS, requested_end_ms=END_MS, observed_min_ms=OBSERVED_MIN_MS,
                  observed_max_ms=END_MS - 8 * 3_600_000, complete_left=False, complete_right=True,
                  truncated=False, terminal_reason="PROVIDER_EXHAUSTED")
    values.update(overrides)
    return ReferenceCoverage(**values)


def problem_for(value: ReferenceCoverage):
    request = types.SimpleNamespace(instrument=types.SimpleNamespace(instrument_uid="e49b54ae"),
                                    product=ReferenceProduct.FUNDING_RATE, is_history=True)
    requirement = types.SimpleNamespace(instrument_uid="e49b54ae", product=ReferenceProduct.FUNDING_RATE,
                                        require_full_coverage=True, max_freshness_ms=None)
    result = types.SimpleNamespace(request=request, status=ReferenceStatus.OK, coverage=value, observations=())
    return V2QueryService._reference_problem(None, requirement, request, result)


class ReferenceCoverageDetailTests(unittest.TestCase):
    def test_the_incident_is_refused_with_its_reason_edge_and_supported_range(self):
        problem = problem_for(coverage())
        self.assertIsNotNone(problem)
        self.assertEqual((problem.code.value, problem.retryable), ("PARTIAL_RESULT", False))
        self.assertIn("PROVIDER_EXHAUSTED LEFT_EDGE_UNCOVERED", problem.detail)
        self.assertIn(f"observed_min_ms={OBSERVED_MIN_MS}", problem.detail)
        self.assertIn(f"requested_start_ms={START_MS}", problem.detail)

    def test_the_sdk_surfaces_the_tokens_to_the_consumer(self):
        detail = _reference_coverage_detail(coverage())
        item = types.SimpleNamespace(instrument_uid="e49b54ae", problem=types.SimpleNamespace(
            code="PARTIAL_RESULT", detail=detail, retryable=False, retry_after_ms=None, request_id="a38214cf"))
        rows = _batch_item_problems("sdk_reference_batch", [item], lambda _index, _item: "FUNDING_RATE")
        self.assertEqual(rows["item_problems"][0]["reason_tokens"], ["PROVIDER_EXHAUSTED", "LEFT_EDGE_UNCOVERED"])

    def test_right_edge_and_truncation_are_typed_too(self):
        detail = _reference_coverage_detail(coverage(complete_left=True, complete_right=False, truncated=True,
                                                     terminal_reason="MAX_PAGES"))
        self.assertIn("MAX_PAGES RIGHT_EDGE_UNCOVERED HISTORY_TRUNCATED", detail)


if __name__ == "__main__":
    unittest.main()

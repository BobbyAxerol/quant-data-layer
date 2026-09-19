"""R1.31: `warmup_limit` caps how far back a caller may look; it is not a quota.

The alpha asks for 10,000 bars at every interval it declares. At 1w that is 192
years and at 4h it is 4.6; neither exists, at the venue or here. The data layer
answered seven of fourteen intervals with
`BAR result row count differs from the warmup horizon` - a refusal - instead of
the bars it holds.

The owner's rule, stated plainly: a consumer may request up to 10,000 bars of
lookback at any interval, and if history is shorter the answer is simply
truncated. These tests pin that, and pin the two things it must not soften: a
result longer than the cap is still a defect, and a hole inside the returned
window is still a gap.
"""

from __future__ import annotations

import unittest

from qdl.query.contracts import CoverageStatus


class WarmupLookbackCapTests(unittest.TestCase):
    """The coverage decision, read from the source it is written in.

    `full` is computed in `StableSpoolQueryBackend.history`; constructing the
    whole backend needs a spool, a catalog and a live handoff, so the predicate
    is reproduced here exactly as written and driven directly. If the source
    changes, `test_the_source_still_computes_full_this_way` fails and this file
    has to be read again rather than quietly passing.
    """

    @staticmethod
    def _full(*, rows_returned: int, rows_requested: int,
              gap_open: bool, exact_boundary: bool) -> bool:
        # Mirrors `full = not gap_open and exact_boundary`.
        del rows_returned, rows_requested
        return not gap_open and exact_boundary

    def test_a_short_contiguous_window_is_full_coverage(self) -> None:
        """156 weekly bars against a 10,000 ask is all the history there is."""
        self.assertTrue(self._full(
            rows_returned=156, rows_requested=10_000,
            gap_open=False, exact_boundary=True))

    def test_a_complete_window_is_still_full_coverage(self) -> None:
        self.assertTrue(self._full(
            rows_returned=10_000, rows_requested=10_000,
            gap_open=False, exact_boundary=True))

    def test_a_hole_inside_the_window_is_not_full_coverage(self) -> None:
        """Short because history ends is fine; short because of a hole is not."""
        self.assertFalse(self._full(
            rows_returned=9_876, rows_requested=10_000,
            gap_open=True, exact_boundary=True))

    def test_a_time_range_request_still_holds_its_endpoints(self) -> None:
        self.assertFalse(self._full(
            rows_returned=500, rows_requested=500,
            gap_open=False, exact_boundary=False))

    def test_the_source_still_computes_full_this_way(self) -> None:
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1]
                  / "qdl/runtime/stable_source.py").read_text(encoding="utf-8")
        self.assertIn("full = not gap_open and exact_boundary", source)
        self.assertNotIn("full = len(items) == requested", source)

    def test_partial_is_still_reachable_so_require_full_coverage_still_bites(self) -> None:
        """`require_full_coverage` must keep meaning something."""
        self.assertIsNot(CoverageStatus.PARTIAL, CoverageStatus.FULL)
        self.assertFalse(self._full(
            rows_returned=10, rows_requested=10, gap_open=True, exact_boundary=True))


class WarmupHorizonCheckTests(unittest.TestCase):
    """The second refusal, in `V2QueryService`."""

    @staticmethod
    def _rejects(*, rows_returned: int, rows_requested: int) -> bool:
        # Mirrors `if len(items) > specification.rows`.
        return rows_returned > rows_requested

    def test_fewer_rows_than_asked_is_accepted(self) -> None:
        self.assertFalse(self._rejects(rows_returned=156, rows_requested=10_000))

    def test_exactly_the_asked_rows_is_accepted(self) -> None:
        self.assertFalse(self._rejects(rows_returned=10_000, rows_requested=10_000))

    def test_more_rows_than_asked_is_still_refused(self) -> None:
        """The cap is a cap. Overrunning it is a real defect and stays one."""
        self.assertTrue(self._rejects(rows_returned=10_001, rows_requested=10_000))

    def test_the_source_still_checks_this_way(self) -> None:
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1]
                  / "qdl/query/service.py").read_text(encoding="utf-8")
        self.assertIn("if len(items) > specification.rows:", source)
        self.assertNotIn("if len(items) != specification.rows:", source)


class PassThroughHorizonTests(unittest.TestCase):
    """The same rule on the pass-through path, which serves unbound intervals.

    `provider_history` answers BAR `FRESH_SNAPSHOT` for an interval with no
    binding. It had the identical exact-count refusal, so an alpha asking 10,000
    rows through pass-through hit the same wall as through the spool.
    """

    @staticmethod
    def _rejects(*, rows_returned: int, expected: int, rows_request: bool) -> bool:
        # Mirrors provider_history.py.
        return rows_returned > expected or (
            not rows_request and rows_returned != expected
        )

    def test_a_rows_request_may_come_back_short(self) -> None:
        self.assertFalse(self._rejects(
            rows_returned=156, expected=10_000, rows_request=True))

    def test_a_rows_request_may_not_come_back_long(self) -> None:
        self.assertTrue(self._rejects(
            rows_returned=10_001, expected=10_000, rows_request=True))

    def test_a_time_range_request_still_demands_the_exact_count(self) -> None:
        """A named window is a contract; it is not a lookback cap."""
        self.assertTrue(self._rejects(
            rows_returned=99, expected=100, rows_request=False))
        self.assertFalse(self._rejects(
            rows_returned=100, expected=100, rows_request=False))

    def test_the_source_still_checks_this_way(self) -> None:
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1]
                  / "qdl/runtime/provider_history.py").read_text(encoding="utf-8")
        self.assertIn("rows_request = expected_start_ns is None", source)
        self.assertIn("not rows_request and len(items) != expected_count", source)

    def test_an_empty_pass_through_result_is_still_refused(self) -> None:
        """Truncating to nothing is not an answer."""
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1]
                  / "qdl/runtime/provider_history.py").read_text(encoding="utf-8")
        self.assertIn("pass-through target history returned no rows", source)


if __name__ == "__main__":
    unittest.main()

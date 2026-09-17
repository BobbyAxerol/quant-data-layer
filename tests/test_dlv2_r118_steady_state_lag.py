"""R1.18: projector health measured in seconds of work, not in records.

The record-count bound in the rebuild runbook is a convergence gate. It proves a
replay drained and the boot-recovery rehearsal depends on it, so it is left
exactly as it is. What it cannot answer is whether a running projector is
healthy: measured 2026-09-17 at 373 records a second, its 500-record bound is
1.3 seconds of work, so a queue that is keeping up perfectly crosses it whenever
it breathes. Five of thirty samples reported a failure while the consumer stayed
ready and never fell back to V1.

These tests pin the replacement measure and, just as importantly, pin the cases
where it must refuse to answer instead of inventing a number.
"""

from __future__ import annotations

import unittest

from scripts.rebuild_v2_stable_projection_cache import (
    CANONICAL_TOPIC,
    EXPECTED_CANONICAL_PARTITIONS,
    MAX_ACCEPTED_LAG,
    lag_sample_acceptable,
    parse_canonical_lag,
    parse_canonical_progress,
    steady_state_lag_seconds,
)

HEADER = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID"


def describe(rows: list[tuple[int, int, int]]) -> str:
    """Render a kafka-consumer-groups --describe table for the given rows."""

    lines = [HEADER]
    for partition, current, lag in rows:
        lines.append(
            f"stable-projector-v1 {CANONICAL_TOPIC} {partition} {current} "
            f"{current + lag} {lag} consumer-1 /10.0.0.1 client-1"
        )
    return "\n".join(lines)


SIX_PARTITIONS = [(p, 1_000_000 + p * 10, 20 + p) for p in range(6)]


class ParseProgressTests(unittest.TestCase):
    def test_returns_consumed_lag_and_partition_count(self) -> None:
        consumed, lag, partitions = parse_canonical_progress(describe(SIX_PARTITIONS))
        self.assertEqual(partitions, EXPECTED_CANONICAL_PARTITIONS)
        self.assertEqual(lag, sum(20 + p for p in range(6)))
        self.assertEqual(consumed, sum(1_000_000 + p * 10 for p in range(6)))

    def test_agrees_with_the_convergence_parser_on_lag_and_partitions(self) -> None:
        # The two readers must never disagree about the same table; the new one
        # only adds a field.
        text = describe(SIX_PARTITIONS)
        total_lag, partitions, _ = parse_canonical_lag(text)
        consumed_lag = parse_canonical_progress(text)
        self.assertEqual(consumed_lag[1], total_lag)
        self.assertEqual(consumed_lag[2], partitions)

    def test_negative_lag_is_floored_like_the_convergence_parser(self) -> None:
        _, lag, _ = parse_canonical_progress(describe([(0, 10, -5), (1, 10, 7)]))
        self.assertEqual(lag, 7)

    def test_a_repeated_partition_is_rejected(self) -> None:
        with self.assertRaises(RuntimeError):
            parse_canonical_progress(describe([(0, 10, 1), (0, 20, 2)]))

    def test_an_empty_table_is_rejected(self) -> None:
        with self.assertRaises(RuntimeError):
            parse_canonical_progress(HEADER)

    def test_a_row_with_a_dash_offset_is_skipped(self) -> None:
        # A consumer that has never committed shows "-" for CURRENT-OFFSET.
        text = "\n".join([
            HEADER,
            f"g {CANONICAL_TOPIC} 0 - - - consumer-1 /10.0.0.1 c",
            f"g {CANONICAL_TOPIC} 1 500 520 20 consumer-1 /10.0.0.1 c",
        ])
        consumed, lag, partitions = parse_canonical_progress(text)
        self.assertEqual((consumed, lag, partitions), (500, 20, 1))

    def test_other_topics_are_ignored(self) -> None:
        text = "\n".join([
            HEADER,
            f"g md.raw.realtime.v2 0 900 999 99 consumer-1 /10.0.0.1 c",
            f"g {CANONICAL_TOPIC} 0 500 520 20 consumer-1 /10.0.0.1 c",
        ])
        self.assertEqual(parse_canonical_progress(text), (500, 20, 1))


class SteadyStateTests(unittest.TestCase):
    def test_the_measured_operating_point(self) -> None:
        # 2026-09-17: 132,894 records consumed in 356 s is 373 a second, with a
        # standing queue of 380 records. That is 1.02 seconds of work.
        first = (1_000_000, 0, 6)
        second = (1_000_000 + 132_894, 380, 6)
        seconds = steady_state_lag_seconds(first, second, 356.0)
        assert seconds is not None
        self.assertAlmostEqual(seconds, 1.018, places=2)

    def test_the_record_gate_and_the_time_gate_disagree_on_a_healthy_queue(self) -> None:
        # The exact situation that produced five false failures: 501 records of
        # lag trips the record gate, while at 373 records a second it is only
        # 1.34 seconds of work.
        self.assertFalse(
            lag_sample_acceptable(MAX_ACCEPTED_LAG + 1, EXPECTED_CANONICAL_PARTITIONS, 100)
        )
        seconds = steady_state_lag_seconds((0, 0, 6), (373 * 60, 501, 6), 60.0)
        assert seconds is not None
        self.assertAlmostEqual(seconds, 1.343, places=2)

    def test_a_faster_projector_at_the_same_lag_is_less_behind(self) -> None:
        slow = steady_state_lag_seconds((0, 0, 6), (6_000, 600, 6), 60.0)
        fast = steady_state_lag_seconds((0, 0, 6), (60_000, 600, 6), 60.0)
        assert slow is not None and fast is not None
        self.assertAlmostEqual(slow, 6.0, places=6)
        self.assertAlmostEqual(fast, 0.6, places=6)
        self.assertLess(fast, slow)

    def test_zero_lag_is_zero_seconds(self) -> None:
        self.assertEqual(steady_state_lag_seconds((0, 0, 6), (10_000, 0, 6), 30.0), 0.0)

    def test_a_stalled_projector_is_unknown_not_infinite(self) -> None:
        # Nothing consumed. The honest answer is "this measure cannot say", and
        # a stalled projector is a different alarm.
        self.assertIsNone(steady_state_lag_seconds((5_000, 100, 6), (5_000, 900, 6), 60.0))

    def test_a_group_reset_reports_unknown(self) -> None:
        # An offset reset to latest moves the consumed offset backwards.
        self.assertIsNone(steady_state_lag_seconds((9_000_000, 100, 6), (12_000, 5, 6), 60.0))

    def test_a_partition_count_change_reports_unknown(self) -> None:
        # A rebalance mid-interval means the two readings are not comparable.
        self.assertIsNone(steady_state_lag_seconds((0, 0, 6), (10_000, 50, 3), 60.0))

    def test_a_non_positive_interval_reports_unknown(self) -> None:
        self.assertIsNone(steady_state_lag_seconds((0, 0, 6), (10_000, 50, 6), 0.0))
        self.assertIsNone(steady_state_lag_seconds((0, 0, 6), (10_000, 50, 6), -5.0))


class ConvergenceGateUnchangedTests(unittest.TestCase):
    """The runbook's own gate must behave exactly as it did before R1.18."""

    def test_a_drained_replay_still_passes(self) -> None:
        self.assertTrue(lag_sample_acceptable(120, EXPECTED_CANONICAL_PARTITIONS, 40))

    def test_the_total_bound_still_binds(self) -> None:
        self.assertFalse(
            lag_sample_acceptable(MAX_ACCEPTED_LAG + 1, EXPECTED_CANONICAL_PARTITIONS, 10)
        )

    def test_a_missing_partition_still_fails(self) -> None:
        self.assertFalse(lag_sample_acceptable(10, EXPECTED_CANONICAL_PARTITIONS - 1, 5))

    def test_the_per_partition_bound_still_binds(self) -> None:
        self.assertFalse(lag_sample_acceptable(300, EXPECTED_CANONICAL_PARTITIONS, 251))


if __name__ == "__main__":
    unittest.main()

"""R1.31: a history repair must have somewhere to land.

Two repairs failed on 2026-09-19 and the second one taught the reason. The spool
keeps the newest `max_partition_records` rows by `logical_offset` and deletes the
rest. Append order is deliberately not market order - after a cache rebuild the
projector replays a recent realtime window before the bar edge backfills older
history - so a partition sitting at its cap has no free rows, and a repair writes
N rows while the trim deletes the N oldest-by-append from inside the same window.
Measured: 124 written, 125 deleted, missing count unchanged, hole moved.

These tests pin the slack that makes a repair land, and the relationship it has
to hold against the largest hole this catalog can produce.
"""

from __future__ import annotations

import unittest

from qdl.runtime.stable_capacity import (
    STABLE_SPOOL_LATE_BACKFILL_HEADROOM,
    STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
    STABLE_SPOOL_PUBLIC_PARTITION_WINDOW,
)

# The largest hole measured across all twenty affected partitions, both venues.
LARGEST_OBSERVED_HOLE_ROWS = 124
# What a caller may request, which is what the public window has to satisfy.
MAX_CONSUMER_LOOKBACK_ROWS = 10_000


class RepairHeadroomTests(unittest.TestCase):
    def test_the_public_window_still_serves_the_full_consumer_lookback(self) -> None:
        self.assertEqual(STABLE_SPOOL_PUBLIC_PARTITION_WINDOW, MAX_CONSUMER_LOOKBACK_ROWS)

    def test_the_physical_window_is_the_public_window_plus_headroom(self) -> None:
        self.assertEqual(
            STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW,
            STABLE_SPOOL_PUBLIC_PARTITION_WINDOW + STABLE_SPOOL_LATE_BACKFILL_HEADROOM,
        )

    def test_headroom_exceeds_the_largest_hole_this_catalog_produced(self) -> None:
        """64 did not, which is why the repair moved the hole instead of closing it."""
        self.assertGreater(STABLE_SPOOL_LATE_BACKFILL_HEADROOM, LARGEST_OBSERVED_HOLE_ROWS)

    def test_headroom_covers_every_hole_at_once(self) -> None:
        """Twenty bindings are repaired in one pass; one partition's worth is 124."""
        self.assertGreaterEqual(
            STABLE_SPOOL_LATE_BACKFILL_HEADROOM, LARGEST_OBSERVED_HOLE_ROWS * 2)

    def test_the_repair_tool_bound_still_fits_the_physical_window(self) -> None:
        """`--rows` is capped at 10,000; a plan that big must still land."""
        self.assertGreaterEqual(
            STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW, MAX_CONSUMER_LOOKBACK_ROWS)

    def test_the_slack_is_not_so_large_that_it_hides_a_retention_bug(self) -> None:
        """Headroom is a reprieve, not the cure; it must stay a minority of the window."""
        self.assertLess(
            STABLE_SPOOL_LATE_BACKFILL_HEADROOM,
            STABLE_SPOOL_PUBLIC_PARTITION_WINDOW // 2)


if __name__ == "__main__":
    unittest.main()

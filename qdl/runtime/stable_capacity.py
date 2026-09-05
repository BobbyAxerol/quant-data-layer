"""Shared bounded capacities for stable V2 durable BAR retention."""

from __future__ import annotations


# Public callers may request at most this many historical BARs. The durable
# spool retains a small additional tail so late authentic backfills do not
# evict an otherwise required public history window by append order.
STABLE_SPOOL_PUBLIC_PARTITION_WINDOW = 10_000
STABLE_SPOOL_LATE_BACKFILL_HEADROOM = 64
STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW = (
    STABLE_SPOOL_PUBLIC_PARTITION_WINDOW + STABLE_SPOOL_LATE_BACKFILL_HEADROOM
)

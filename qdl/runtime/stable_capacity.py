"""Shared bounded capacities for stable V2 durable BAR retention."""

from __future__ import annotations


# Public callers may request at most this many historical BARs. The durable
# spool retains a small additional tail so late authentic backfills do not
# evict an otherwise required public history window by append order.
STABLE_SPOOL_PUBLIC_PARTITION_WINDOW = 10_000
# R1.31. This was 64 and it was fully consumed: every BAR partition sat at
# exactly 10,064 rows, so a history repair had nowhere to land. Measured on
# `binance-usdm-btcusdt-bar-15m`, a 124-row repair wrote its rows and the trim
# deleted the 125 oldest-by-append from inside the same window, leaving the
# missing count unchanged - the repair moved the hole instead of closing it.
#
# The trim keeps the newest rows by `logical_offset`, and append order is
# deliberately not market order: after a cache rebuild the projector replays a
# recent realtime window before the bar edge backfills older history, which
# `_durable_final_bar_opens` documents as the intended sequence. Headroom does
# not cure that - retention by market time does, and that is a schema change -
# but 2,064 rows of slack is enough for any repair this catalog can need (the
# largest observed hole is 124) and a repaired window survives until the whole
# partition turns over, about 104 days at 15m.
STABLE_SPOOL_LATE_BACKFILL_HEADROOM = 2_064
STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW = (
    STABLE_SPOOL_PUBLIC_PARTITION_WINDOW + STABLE_SPOOL_LATE_BACKFILL_HEADROOM
)

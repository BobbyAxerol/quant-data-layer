"""D48 daily market-cap universe counts for tests that pin catalog/manifest totals.

The universe (``config/v2/universes/crypto-top300-1d.json``, synced into the
committed demand by ``scripts/kn_universe_top300.py --sync``) adds one BAR 1d
row per bar-only symbol to each venue's alpha consumer. Tests add these counts
to their five-liquid baselines instead of pinning a universe revision's size.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from qdl.runtime.universal_demand import bar_family_symbols

ROOT = Path(__file__).resolve().parents[1]
_DEMAND = yaml.safe_load((ROOT / "config/v2/stable-crypto-demand.yaml").read_text(encoding="utf-8"))
# Full-bar-family symbols (the five liquid per venue) are the baseline, not the universe: the shared predicate
# `bar_family_symbols` ignores execution-only demand (v2.2.6 stage A QUOTE + MARK), so the universe counts stay a
# property of the universe file alone.
_EXECUTION = bar_family_symbols(_DEMAND)
UNIVERSE_SYMBOLS = {
    venue: sorted({
        str(row["native_symbol"])
        for consumer in _DEMAND["consumers"] for row in consumer["requirements"]
        if str(row["feed"]).upper() == "BAR" and str(row["venue"]).upper() == venue
        and (venue, str(row["native_symbol"])) not in _EXECUTION
    })
    for venue in ("BINANCE", "OKX")
}
UNIVERSE_PER_VENUE = {venue: len(symbols) for venue, symbols in UNIVERSE_SYMBOLS.items()}
UNIVERSE_TOTAL = sum(UNIVERSE_PER_VENUE.values())
UNIVERSE_MEMBERS = json.loads(
    (ROOT / "config/v2/universes/crypto-top300-1d.json").read_text(encoding="utf-8"))["members"]

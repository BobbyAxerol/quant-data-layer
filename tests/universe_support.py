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

ROOT = Path(__file__).resolve().parents[1]
_DEMAND = yaml.safe_load((ROOT / "config/v2/stable-crypto-demand.yaml").read_text(encoding="utf-8"))
_EXECUTION = {
    (str(row["venue"]).upper(), str(row["native_symbol"]))
    for consumer in _DEMAND["consumers"] for row in consumer["requirements"]
    if str(row["feed"]).upper() != "BAR"
}
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

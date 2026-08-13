from __future__ import annotations

import json
from pathlib import Path

from qdl.canonical.market import canonicalize_binance_usdm_bar, canonicalize_binance_usdm_bbo
from qdl.canonical.trade import TradeContext


ROOT = Path(__file__).resolve().parents[1]


def generate(fixture_name: str, golden_name: str) -> None:
    fixture = json.loads((ROOT / "tests/fixtures/phase2" / fixture_name).read_text())
    context = TradeContext(**fixture["context"])
    function = (
        canonicalize_binance_usdm_bbo
        if fixture["provider_kind"] == "binance_usdm_bbo"
        else canonicalize_binance_usdm_bar
    )
    payload = function(fixture["raw"], context).SerializeToString(deterministic=True)
    destination = ROOT / "contracts/golden/phase2" / golden_name
    destination.write_bytes(payload)


if __name__ == "__main__":
    generate("binance_usdm_bbo.json", "binance-usdm-bbo.bin")
    generate("binance_usdm_bar.json", "binance-usdm-bar.bin")

from __future__ import annotations

import argparse
import json
from pathlib import Path

from qdl.canonical.market import canonicalize_binance_usdm_bar, canonicalize_binance_usdm_bbo
from qdl.canonical.trade import TradeContext


ROOT = Path(__file__).resolve().parents[1]


def generate(fixture_name: str, golden_name: str, *, output_dir: Path) -> None:
    fixture = json.loads((ROOT / "tests/fixtures/phase2" / fixture_name).read_text())
    context = TradeContext(**fixture["context"])
    function = (
        canonicalize_binance_usdm_bbo
        if fixture["provider_kind"] == "binance_usdm_bbo"
        else canonicalize_binance_usdm_bar
    )
    payload = function(fixture["raw"], context).SerializeToString(deterministic=True)
    destination = output_dir / golden_name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "contracts/golden/phase2",
    )
    args = parser.parse_args()
    generate(
        "binance_usdm_bbo.json", "binance-usdm-bbo.bin", output_dir=args.output_dir
    )
    generate(
        "binance_usdm_bar.json", "binance-usdm-bar.bin", output_dir=args.output_dir
    )

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from qdl.canonical.trade import (
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_okx_trade,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2"
OUTPUT = ROOT / "contracts" / "golden" / "phase2"


def load(name: str) -> tuple[dict, TradeContext]:
    fixture = json.loads((FIXTURES / name).read_text())
    return fixture, TradeContext(**fixture["context"])


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    cases = []
    for fixture_name, output_name, canonicalizer in (
        ("binance_usdm_trade.json", "binance-usdm-trade.bin", canonicalize_binance_usdm_trade),
        ("okx_trade.json", "okx-swap-trade.bin", canonicalize_okx_trade),
    ):
        fixture, context = load(fixture_name)
        payload = canonicalizer(fixture["raw"], context).SerializeToString(deterministic=True)
        (OUTPUT / output_name).write_bytes(payload)
        cases.append(
            {
                "fixture": fixture_name,
                "golden": output_name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    (OUTPUT / "manifest.json").write_text(
        json.dumps({"schema": "qdl.phase2.golden.v1", "cases": cases}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()

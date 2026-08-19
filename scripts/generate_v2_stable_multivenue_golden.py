#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.canonical.market import (  # noqa: E402
    canonicalize_binance_usdm_bar,
    canonicalize_binance_usdm_bbo,
    canonicalize_dnse_bar,
    canonicalize_okx_bar,
    canonicalize_okx_bbo,
)
from qdl.canonical.trade import (  # noqa: E402
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_dnse_trade,
    canonicalize_okx_trade,
)


FIXTURES = ROOT / "tests/fixtures/phase2"
OUTPUT = ROOT / "contracts/golden/phase2"
MANIFEST = OUTPUT / "manifest-v2-stable-multivenue.json"
Canonicalizer = Callable[[dict, TradeContext], object]
CASES: tuple[tuple[str, str, Canonicalizer], ...] = (
    ("binance_usdm_trade.json", "binance-usdm-trade.bin", canonicalize_binance_usdm_trade),
    ("binance_usdm_bbo.json", "binance-usdm-bbo.bin", canonicalize_binance_usdm_bbo),
    ("binance_usdm_bar.json", "binance-usdm-bar.bin", canonicalize_binance_usdm_bar),
    ("binance_spot_trade.json", "binance-spot-trade.bin", canonicalize_binance_usdm_trade),
    ("binance_spot_bbo.json", "binance-spot-bbo.bin", canonicalize_binance_usdm_bbo),
    ("binance_spot_bar.json", "binance-spot-bar.bin", canonicalize_binance_usdm_bar),
    ("okx_trade.json", "okx-swap-trade.bin", canonicalize_okx_trade),
    ("okx_bbo.json", "okx-swap-bbo.bin", canonicalize_okx_bbo),
    ("okx_bar.json", "okx-swap-bar.bin", canonicalize_okx_bar),
    ("okx_spot_trade.json", "okx-spot-trade.bin", canonicalize_okx_trade),
    ("okx_spot_bbo.json", "okx-spot-bbo.bin", canonicalize_okx_bbo),
    ("okx_spot_bar.json", "okx-spot-bar.bin", canonicalize_okx_bar),
    ("dnse_derivative_trade.json", "dnse-derivative-trade.bin", canonicalize_dnse_trade),
    ("dnse_derivative_bar.json", "dnse-derivative-bar.bin", canonicalize_dnse_bar),
    ("dnse_equity_trade.json", "dnse-equity-trade.bin", canonicalize_dnse_trade),
    ("dnse_equity_bar.json", "dnse-equity-bar.bin", canonicalize_dnse_bar),
    ("vnstock_equity_bar.json", "vnstock-equity-bar.bin", canonicalize_dnse_bar),
)


def render(fixture_name: str, canonicalize: Canonicalizer) -> bytes:
    fixture = json.loads((FIXTURES / fixture_name).read_text())
    envelope = canonicalize(fixture["raw"], TradeContext(**fixture["context"]))
    return envelope.SerializeToString(deterministic=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    manifest_cases = []
    for fixture_name, golden_name, canonicalize in CASES:
        payload = render(fixture_name, canonicalize)
        golden = OUTPUT / golden_name
        if args.check:
            if not golden.is_file() or golden.read_bytes() != payload:
                raise RuntimeError(f"stale multivenue golden artifact: {golden_name}")
        else:
            golden.write_bytes(payload)
        manifest_cases.append({
            "fixture": fixture_name,
            "golden": golden_name,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
    manifest = {
        "schema": "qdl.v2.stable-multivenue-golden.v1",
        "provenance": "DETERMINISTIC_PROVIDER_FIXTURE",
        "cases": manifest_cases,
    }
    rendered = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if args.check:
        if not MANIFEST.is_file() or MANIFEST.read_text() != rendered:
            raise RuntimeError("stale multivenue golden manifest")
    else:
        MANIFEST.write_text(rendered)
    print(json.dumps({"status": "PASS", "mode": "CHECK" if args.check else "WRITE", "cases": len(CASES)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

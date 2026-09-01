#!/usr/bin/env python3
"""Reduce a provider metadata capture to the symbols a demand manifest declares.

A full Binance Spot `exchangeInfo` is about 17 MB, which is not a reasonable
thing to commit or to review. The rows themselves are what matter, so the
capture is reduced to exactly the demanded symbols and stored verbatim: no field
is rewritten, so `fabricated_metadata=false` stays checkable by reading the file.

Provenance records both hashes. The full-response hash says what was fetched and
when; the filtered hash is what regeneration actually consumes and what the
committed catalog must reproduce.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from qdl.runtime.production_catalog import ProductionDemandManifest


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def filter_binance(payload: dict, symbols: set[str]) -> dict:
    kept = [item for item in payload.get("symbols", []) if item.get("symbol") in symbols]
    missing = symbols - {item.get("symbol") for item in kept}
    if missing:
        raise ValueError(f"capture is missing demanded symbols: {sorted(missing)}")
    return {"serverTime": payload.get("serverTime", 0), "symbols": kept}


def filter_okx(payload: dict, symbols: set[str]) -> dict:
    rows = payload["data"] if isinstance(payload, dict) else payload
    kept = [item for item in rows if item.get("instId") in symbols]
    missing = symbols - {item.get("instId") for item in kept}
    if missing:
        raise ValueError(f"capture is missing demanded symbols: {sorted(missing)}")
    return {"code": "0", "msg": "", "data": kept}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand", type=Path, action="append", required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--venue", choices=["binance", "okx"], required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    demand = ProductionDemandManifest.load_many(args.demand)
    venue = args.venue.upper()
    symbols = {
        item.native_symbol
        for item in demand.demands
        if item.venue == venue and item.market == args.market.upper()
    }
    if not symbols:
        raise SystemExit(f"no demanded symbol for {venue}/{args.market}")

    raw = args.capture.read_bytes()
    payload = json.loads(raw)
    reduced = (
        filter_binance(payload, symbols)
        if args.venue == "binance"
        else filter_okx(payload, symbols)
    )
    encoded = (json.dumps(reduced, sort_keys=True, separators=(",", ":")) + "\n").encode()
    args.output.write_bytes(encoded)
    print(json.dumps({
        "schema": "qdl.v2.metadata-capture-provenance.v1",
        "venue": venue,
        "market": args.market.upper(),
        "symbols": sorted(symbols),
        "full_response_sha256": _sha256(raw),
        "full_response_bytes": len(raw),
        "filtered_capture_sha256": _sha256(encoded),
        "filtered_capture_bytes": len(encoded),
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

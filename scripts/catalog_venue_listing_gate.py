#!/usr/bin/env python3
"""DL v2.4.2: refuse a catalogue that names instruments the venue no longer lists (delisted / closed / renamed).

Purpose: on 2026-11-19 OKX delisted `RVN-USDT-SWAP` while catalogue r16 still declared it; nothing noticed until the
bar edge blocked on it. The declaration path is where an instrument enters and leaves the catalogue, so the check
belongs there: every catalogue instrument of a venue/market covered by a capture of that venue's live instruments
list must be listed and live. Venue truth (the capture), not a provider error code, decides.

Live criteria (venue documentation):
- OKX `GET /api/v5/public/instruments?instType=SWAP|FUTURES|SPOT`: present by `instId` with `state` = `live`
  (https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments; states live, suspend, preopen, test).
- Binance USD-M `GET /fapi/v1/exchangeInfo`: present by `symbol` with `status` = `TRADING`
  (https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information).
- Binance spot `GET /api/v3/exchangeInfo`: `status` = `TRADING`.

Boundary: reads a catalogue and capture files and prints a JSON verdict (exit 0 PASS, 1 FAIL). Used by
`scripts/dl_declaration.py gate` when a declaration lists `venue_listings`, and standalone:

  python3 -B scripts/catalog_venue_listing_gate.py --catalog stable-source-bindings.yaml \
      --listing OKX:SWAP:okx-instruments-swap.json --listing BINANCE:USDM:binance-usdm-exchangeinfo.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

LIVE_STATE = {"OKX": ("state", "live"), "BINANCE": ("status", "TRADING")}


def load_listing(venue: str, market: str, path: Path) -> dict[str, str]:
    """native symbol -> state/status from one capture."""
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    venue = venue.upper()
    if venue == "OKX":
        if str(document.get("code")) != "0" or not isinstance(document.get("data"), list):
            raise ValueError(f"{path}: not an OKX instruments response")
        return {str(row["instId"]).upper(): str(row.get("state")) for row in document["data"]}
    if venue == "BINANCE":
        if not isinstance(document.get("symbols"), list):
            raise ValueError(f"{path}: not a Binance exchangeInfo response")
        return {str(row["symbol"]).upper(): str(row.get("status")) for row in document["symbols"]}
    raise ValueError(f"no listing rule for venue {venue}")


def listing_problems(instruments: Iterable[Mapping[str, Any]],
                     listings: Mapping[tuple[str, str], Mapping[str, str]]) -> dict[str, Any]:
    """Catalogue instruments missing from, or not live in, a captured venue listing of their venue/market."""
    missing, not_live, checked, uncovered = [], [], 0, {}
    for item in instruments:
        key = (str(item["venue"]).upper(), str(item["market"]).upper())
        listing = listings.get(key)
        if listing is None:
            uncovered[f"{key[0]}/{key[1]}"] = uncovered.get(f"{key[0]}/{key[1]}", 0) + 1
            continue
        checked += 1
        symbol = str(item["native_symbol"]).upper()
        state = listing.get(symbol)
        expected = LIVE_STATE[key[0]][1]
        if state is None:
            missing.append({"instrument_id": item["instrument_id"], "native_symbol": symbol})
        elif state != expected:
            not_live.append({"instrument_id": item["instrument_id"], "native_symbol": symbol, "state": state})
    return {"status": "PASS" if not missing and not not_live else "FAIL", "checked": checked,
            "missing": missing, "not_live": not_live, "not_covered_by_a_listing": dict(sorted(uncovered.items()))}


def _parse_listing(spec: str) -> tuple[str, str, Path]:
    venue, market, path = spec.split(":", 2)
    return venue.upper(), market.upper(), Path(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--listing", action="append", default=[], help="VENUE:MARKET:capture.json (repeatable)")
    args = parser.parse_args(argv)
    instruments = json.loads(args.catalog.read_text(encoding="utf-8"))["instruments"]
    listings, sources = {}, {}
    for spec in args.listing:
        venue, market, path = _parse_listing(spec)
        listings[(venue, market)] = load_listing(venue, market, path)
        sources[f"{venue}/{market}"] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    result = listing_problems(instruments, listings)
    result["listings"] = sources
    print(json.dumps(result, indent=1, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

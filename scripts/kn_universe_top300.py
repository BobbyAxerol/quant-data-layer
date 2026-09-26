#!/usr/bin/env python3
"""Market-cap universe for daily bars: the top <=300 bases listed on both Binance USD-M and OKX.

Owner decision D48 (2026-09-26): the Data Layer serves one crypto daily-bar
universe, capped at 300 symbols, chosen by market cap among bases that trade
as USDT-margined perpetuals on BOTH Binance USD-M and OKX; ineligible symbols
leave, every change is logged from the first signed membership on, and only
1d bars are admitted for it (warmup in batches). This script owns the
selection rule and its history - nothing else:

  inputs   Binance /fapi/v1/exchangeInfo, OKX /api/v5/public/instruments?instType=SWAP,
           CoinGecko /api/v3/coins/markets (market cap, top ``--market-pages`` x 250)
  outputs  config/v2/universes/crypto-top300-1d.json          current signed membership
           config/v2/universes/crypto-top300-1d.changes.jsonl one line per revision
  sync     (``--sync``) the membership becomes demand: BAR 1d rows for the alpha
           consumers of each venue (only for symbols without other realtime
           demand - an execution symbol keeps its full bar family), and the
           metadata captures gain the verbatim provider rows of new symbols
           (existing rows kept byte for byte; provenance records the capture).

Rules (``RULES``; a backtest applies the same rules and reads membership as of a
date with ``members_as_of``, so no survivor bias):
  1. eligible = Binance PERPETUAL, quote USDT, status TRADING
                AND OKX SWAP, linear, settle USDT, state live;
     bases compare after removing a 1000/1000000/1M contract-size prefix.
  2. excluded: stable/fiat-pegged and wrapped assets (``EXCLUDED_BASES``),
     listed < ``MIN_LISTING_DAYS`` on either venue, no market cap found.
  3. rank by CoinGecko market cap (a symbol shared by several coins takes the
     largest; flagged ``ambiguous_market_cap_symbol``).
  4. incumbents stay while eligible and ranked <= ``KEEP_RANK``; newcomers
     enter by rank while fewer than ``CAP`` members.
A new revision is written only when the membership changes. Default is a dry
run that prints the change; ``--apply`` writes. Reads public APIs only; no
credential, runtime or provider-admission lane is touched.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import sys
import time
from typing import Any, Iterable, Mapping, Sequence
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
UNIVERSE = ROOT / "config/v2/universes/crypto-top300-1d.json"
CHANGES = ROOT / "config/v2/universes/crypto-top300-1d.changes.jsonl"
SCHEMA = "qdl.v2.market-cap-universe.v1"
CAP = 300
KEEP_RANK = 330
MIN_LISTING_DAYS = 30
INTERVALS = ("1d",)
EXCLUDED_BASES = frozenset({
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDP", "BUSD", "PYUSD", "USDD", "USD1", "RLUSD",
    "EUR", "EURC", "EURI", "AEUR", "XAUT", "PAXG", "WBTC", "WETH", "STETH", "WSTETH", "WEETH", "WBETH",
})
RULES = {
    "cap": CAP, "keep_rank": KEEP_RANK, "min_listing_days": MIN_LISTING_DAYS, "intervals": list(INTERVALS),
    "eligible": "Binance USD-M PERPETUAL/USDT TRADING and OKX SWAP linear/USDT live, same base",
    "base_normalisation": "strip a leading 1000000, 1000 or 1M contract-size prefix",
    "excluded_bases": sorted(EXCLUDED_BASES),
    "ranking": "CoinGecko market cap, largest coin for a shared symbol (flagged ambiguous)",
    "hysteresis": "incumbent kept while eligible and rank <= keep_rank; newcomers by rank while members < cap",
}
BINANCE_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
OKX_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
MARKETS_URL = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc"
               "&per_page=250&page={page}")
_PREFIX = re.compile(r"^(1000000|1000|1M)(.+)$")


def normalise_base(base: str) -> str:
    match = _PREFIX.match(base.upper())
    return match.group(2) if match else base.upper()


def binance_listings(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("symbols", ()):
        if (item.get("contractType") == "PERPETUAL" and item.get("quoteAsset") == "USDT"
                and item.get("status") == "TRADING"):
            out[normalise_base(str(item["baseAsset"]))] = {
                "symbol": item["symbol"], "listed_ms": int(item.get("onboardDate") or 0)}
    return out


def okx_listings(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in payload.get("data", ()):
        if item.get("ctType") == "linear" and item.get("settleCcy") == "USDT" and item.get("state") == "live":
            out[normalise_base(str(item["instFamily"]).split("-")[0])] = {
                "inst_id": item["instId"], "listed_ms": int(item.get("listTime") or 0)}
    return out


def market_caps(coins: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Symbol -> the largest coin with it (input is market-cap ordered); ambiguity flagged."""
    out: dict[str, dict[str, Any]] = {}
    for coin in coins:
        symbol = str(coin.get("symbol", "")).upper()
        cap = coin.get("market_cap")
        if not symbol or not cap:
            continue
        if symbol in out:
            out[symbol]["ambiguous"] = True
            continue
        out[symbol] = {"market_cap_usd": int(cap), "coingecko_id": coin.get("id"), "ambiguous": False}
    return out


def select(*, binance: Mapping[str, Any], okx: Mapping[str, Any], coins: Sequence[Mapping[str, Any]],
           incumbents: Iterable[str], now_ms: int) -> dict[str, Any]:
    """Apply ``RULES``: the new membership, and why every base left or stayed out."""
    listed_b, listed_o, caps = binance_listings(binance), okx_listings(okx), market_caps(coins)
    min_age_ms = MIN_LISTING_DAYS * 86_400_000
    reasons: dict[str, str] = {}
    eligible = []
    for base in sorted(set(listed_b) | set(listed_o) | set(incumbents)):
        if base not in listed_b:
            reasons[base] = "NOT_TRADING_BINANCE"
        elif base not in listed_o:
            reasons[base] = "NOT_LIVE_OKX"
        elif base in EXCLUDED_BASES:
            reasons[base] = "EXCLUDED_ASSET"
        elif min(listed_b[base]["listed_ms"], listed_o[base]["listed_ms"]) <= 0 or \
                now_ms - max(listed_b[base]["listed_ms"], listed_o[base]["listed_ms"]) < min_age_ms:
            reasons[base] = "TOO_NEW"
        elif base not in caps:
            reasons[base] = "NO_MARKET_CAP"
        else:
            eligible.append(base)
    eligible.sort(key=lambda base: (-caps[base]["market_cap_usd"], base))
    rank = {base: position + 1 for position, base in enumerate(eligible)}
    kept = [base for base in incumbents if base in rank and rank[base] <= KEEP_RANK]
    for base in incumbents:
        if base in rank and rank[base] > KEEP_RANK:
            reasons[base] = "RANK_BELOW_BAND"
    members = set(kept)
    for base in eligible:
        if len(members) >= CAP:
            break
        members.add(base)
    for base in eligible:
        if base not in members:
            reasons.setdefault(base, "CAP_FULL")
    rows = [{
        "base": base, "market_cap_rank": rank[base], "market_cap_usd": caps[base]["market_cap_usd"],
        "coingecko_id": caps[base]["coingecko_id"], "ambiguous_market_cap_symbol": caps[base]["ambiguous"],
        "binance_symbol": listed_b[base]["symbol"], "okx_inst_id": listed_o[base]["inst_id"],
        "binance_listed_ms": listed_b[base]["listed_ms"], "okx_listed_ms": listed_o[base]["listed_ms"],
    } for base in sorted(members, key=lambda base: rank[base])]
    return {"members": rows, "eligible": len(eligible), "reasons": reasons}


def membership_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    identity = [[row["base"], row["binance_symbol"], row["okx_inst_id"]] for row in rows]
    return hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()


def change_record(*, revision: int, effective_date: str, before: Mapping[str, Mapping[str, Any]],
                  after: Sequence[Mapping[str, Any]], reasons: Mapping[str, str],
                  sources: Mapping[str, str]) -> dict[str, Any]:
    now = {row["base"]: row for row in after}
    return {
        "revision": revision, "effective_date": effective_date, "members": len(after),
        "membership_sha256": membership_sha256(after), "sources_sha256": dict(sources),
        "added": [{"base": base, "market_cap_rank": now[base]["market_cap_rank"],
                   "reason": "SIGNED" if not before else "ENTERED_BY_RANK"}
                  for base in sorted(set(now) - set(before), key=lambda base: now[base]["market_cap_rank"])],
        "removed": [{"base": base, "reason": reasons.get(base, "UNKNOWN")} for base in sorted(set(before) - set(now))],
    }


def members_as_of(changes: Iterable[Mapping[str, Any]], date: str) -> set[str]:
    """Point-in-time membership for a backtest: replay revisions effective on or before ``date``."""
    members: set[str] = set()
    for record in sorted(changes, key=lambda item: item["revision"]):
        if record["effective_date"] > date:
            break
        members |= {item["base"] for item in record["added"]}
        members -= {item["base"] for item in record["removed"]}
    return members


DEMAND = ROOT / "config/v2/stable-crypto-demand.yaml"
CAPTURES = ROOT / "config/v2/captures"
UNIVERSE_CONSUMERS = {
    "BINANCE": {"consumer_id": "alpha.binance.paper.stable", "market": "USDM", "field": "binance_symbol",
                "capture": "binance-usdm-exchangeinfo.filtered.json", "rows": "symbols", "key": "symbol"},
    "OKX": {"consumer_id": "alpha.okx.paper.stable", "market": "SWAP", "field": "okx_inst_id",
            "capture": "okx-instruments-swap.filtered.json", "rows": "data", "key": "instId"},
}


def active_symbols(demand: Mapping[str, Any]) -> set[tuple[str, str]]:
    """(venue, native symbol) with any non-BAR demand: these keep every bar interval."""
    return {(str(row["venue"]).upper(), str(row["native_symbol"]))
            for consumer in demand["consumers"] for row in consumer["requirements"]
            if str(row["feed"]).upper() != "BAR"}


def sync_demand(demand: Mapping[str, Any], members: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], dict]:
    """Universe members -> BAR 1d demand of the venue's alpha consumer; departed
    universe-only rows removed. Execution symbols are never touched."""
    import copy

    result = copy.deepcopy(dict(demand))
    active = active_symbols(result)
    summary: dict[str, dict[str, int]] = {}
    for venue, spec in UNIVERSE_CONSUMERS.items():
        consumer = next(item for item in result["consumers"] if item["consumer_id"] == spec["consumer_id"])
        wanted = [str(row[spec["field"]]) for row in members if (venue, str(row[spec["field"]])) not in active]
        rows = consumer["requirements"]
        universe_rows = {(row["native_symbol"]) for row in rows
                         if row["feed"] == "BAR" and row["interval"] in INTERVALS
                         and (venue, row["native_symbol"]) not in active and str(row["venue"]).upper() == venue}
        keep = [row for row in rows if not (row["feed"] == "BAR" and str(row["venue"]).upper() == venue
                                            and (venue, row["native_symbol"]) not in active
                                            and row["native_symbol"] not in set(wanted))]
        added = 0
        for symbol in wanted:
            if symbol not in universe_rows:
                keep.append({"venue": venue, "market": spec["market"], "product_type": "PERPETUAL",
                             "native_symbol": symbol, "feed": "BAR", "interval": INTERVALS[0],
                             "source_policy_id": "crypto_primary_v2"})
                added += 1
        summary[venue] = {"added": added, "removed": len(rows) + added - len(keep), "universe_rows": len(wanted)}
        consumer["requirements"] = keep
    if any(item["added"] or item["removed"] for item in summary.values()):
        result["revision"] = int(result["revision"]) + 1
    return result, summary


def merge_capture(existing: Mapping[str, Any], full: Mapping[str, Any], *, rows_field: str, key: str,
                  wanted: Iterable[str]) -> tuple[dict[str, Any], list[str]]:
    """Existing filtered rows unchanged; the full capture's verbatim rows for the missing symbols appended."""
    have = {row[key] for row in existing[rows_field]}
    source = full[rows_field] if isinstance(full, Mapping) else full
    by_key = {row[key]: row for row in source}
    missing = sorted(set(wanted) - have)
    absent = [symbol for symbol in missing if symbol not in by_key]
    if absent:
        raise ValueError(f"provider capture lacks universe symbols: {absent}")
    merged = dict(existing)
    merged[rows_field] = [*existing[rows_field], *(by_key[symbol] for symbol in missing)]
    return merged, missing


def _capture_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _get_json(url: str, *, attempts: int = 5) -> tuple[Any, str]:
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "qdl-universe"}),
                                        timeout=30) as response:
                body = response.read()
            return json.loads(body), hashlib.sha256(body).hexdigest()
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt == attempts - 1:
                raise
            time.sleep(20 * (attempt + 1))  # public market-cap API: back off, never hammer
    raise RuntimeError("unreachable")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--market-pages", type=int, default=6, help="CoinGecko pages of 250 coins")
    parser.add_argument("--apply", action="store_true", help="write a new revision when membership changes")
    parser.add_argument("--sync", action="store_true",
                        help="turn the current membership into BAR 1d demand and metadata captures")
    args = parser.parse_args(argv)
    if args.sync:
        return _sync(apply=args.apply)
    binance, binance_sha = _get_json(BINANCE_URL)
    okx, okx_sha = _get_json(OKX_URL)
    coins, coin_hashes = [], []
    for page in range(1, args.market_pages + 1):
        rows, digest = _get_json(MARKETS_URL.format(page=page))
        coins.extend(rows)
        coin_hashes.append(digest)
        time.sleep(6)
    current = json.loads(UNIVERSE.read_text(encoding="utf-8")) if UNIVERSE.exists() else None
    before = {row["base"]: row for row in (current or {}).get("members", ())}
    now_ms = int(time.time() * 1000)
    result = select(binance=binance, okx=okx, coins=coins, incumbents=list(before), now_ms=now_ms)
    sources = {"binance_exchange_info": binance_sha, "okx_swap_instruments": okx_sha,
               "coingecko_markets": hashlib.sha256("".join(coin_hashes).encode()).hexdigest()}
    revision = int((current or {}).get("revision", 0)) + 1
    record = change_record(revision=revision, effective_date=dt.datetime.fromtimestamp(now_ms / 1000, dt.timezone.utc)
                           .strftime("%Y-%m-%d"), before=before, after=result["members"],
                           reasons=result["reasons"], sources=sources)
    changed = bool(record["added"] or record["removed"])
    print(json.dumps({"changed": changed, "revision": revision if changed else (current or {}).get("revision"),
                      "members": len(result["members"]), "eligible": result["eligible"],
                      "added": len(record["added"]), "removed": record["removed"],
                      "excluded": {reason: sum(1 for value in result["reasons"].values() if value == reason)
                                   for reason in sorted(set(result["reasons"].values()))}}, sort_keys=True))
    if changed and args.apply:
        UNIVERSE.write_text(json.dumps({
            "schema": SCHEMA, "revision": revision, "effective_date": record["effective_date"],
            "rules": RULES, "sources_sha256": sources, "membership_sha256": record["membership_sha256"],
            "members": result["members"]}, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        with CHANGES.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    return 0


def _sync(*, apply: bool) -> int:
    import yaml

    universe = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    demand, summary = sync_demand(yaml.safe_load(DEMAND.read_text(encoding="utf-8")), universe["members"])
    provenance_path = CAPTURES / "provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    captures = {}
    for venue, spec in UNIVERSE_CONSUMERS.items():
        path = CAPTURES / spec["capture"]
        existing = json.loads(path.read_bytes())
        if _capture_bytes(existing) != path.read_bytes():
            raise RuntimeError(f"{path.name} does not round-trip byte for byte; refusing to rewrite it")
        wanted = [str(row[spec["field"]]) for row in universe["members"]]
        full, full_sha = _get_json(BINANCE_URL if venue == "BINANCE" else OKX_URL)
        merged, added = merge_capture(existing, full, rows_field=spec["rows"], key=spec["key"], wanted=wanted)
        captures[venue] = (path, merged, added, full_sha)
    print(json.dumps({"demand_revision": demand["revision"], "demand": summary,
                      "capture_rows_added": {venue: len(item[2]) for venue, item in captures.items()}},
                     sort_keys=True))
    if not apply:
        return 0
    DEMAND.write_bytes(yaml.safe_dump(demand, sort_keys=False, allow_unicode=False).encode("utf-8"))
    captured_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for venue, (path, merged, added, full_sha) in captures.items():
        if not added:
            continue
        encoded = _capture_bytes(merged)
        path.write_bytes(encoded)
        entry = next(item for item in provenance["captures"]
                     if item["filtered_capture"] == f"config/v2/captures/{path.name}")
        entry["filtered_capture_sha256"] = hashlib.sha256(encoded).hexdigest()
        entry["filtered_capture_bytes"] = len(encoded)
        entry.setdefault("appended_captures", []).append({
            "captured_at_utc": captured_at, "full_response_sha256": full_sha,
            "symbols_added": len(added), "universe_revision": universe["revision"],
            "note": "verbatim rows of new universe symbols appended; earlier rows unchanged"})
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

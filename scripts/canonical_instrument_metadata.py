#!/usr/bin/env python3
"""DL v2.4.1: render and gate `canonical-instrument-metadata.json` (schema qdl.v2.canonical-instrument-metadata.v1).

Purpose: one versioned runtime file that states, for every catalogue instrument, how one venue unit relates to its
underlying asset (underlying_asset, price_multiplier, quantity_multiplier, cross-venue aliases), sourced from public
venue endpoints with a capture and from the reviewed rules in `config/v2/canonical-underlying-rules.yaml`. Instruments
no VERIFIED rule covers get status UNVERIFIED and no multipliers (fail closed).

Subcommands:
  capture --catalog C --out DIR       read-only public GETs: Binance /fapi/v1/constituents per USDM instrument,
                                      Binance /fapi/v1/premiumIndex (all), OKX /api/v5/public/instruments?instType=SWAP
                                      and /api/v5/public/mark-price?instType=SWAP; writes DIR/*.json + SHA256SUMS
  render --catalog C --captures DIR --rules R --revision N --out FILE
  gate FILE --catalog C --captures DIR   coverage, rule consistency, alias price-ratio cross-check

Boundary: `capture` makes public, unauthenticated reads only; `render` and `gate` read files and write one output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Kept in sync with qdl.runtime.canonical_metadata (imported lazily: `capture` runs on a plain host Python).
SCHEMA = "qdl.v2.canonical-instrument-metadata.v1"
STATUS_VERIFIED = "VERIFIED"
STATUS_UNVERIFIED = "UNVERIFIED"


def _runtime():
    from qdl.runtime import canonical_metadata
    assert canonical_metadata.SCHEMA == SCHEMA
    return canonical_metadata

BINANCE = "https://fapi.binance.com"
OKX = "https://www.okx.com"
_SUFFIX = re.compile(r"^(?P<pair>[A-Z0-9]+?)(?:\*(?P<n>[0-9]+))?$")
_PREFIX_SPELLINGS = {1000: ("1000",), 10000: ("10000",), 1000000: ("1000000", "1M")}
# The cross-check catches identity and unit errors (orders of magnitude, a reused ticker), not basis: 10 %.
ALIAS_PRICE_TOLERANCE = Decimal("0.10")


def _get(url: str) -> tuple[bytes, dict]:
    request = urllib.request.Request(url, headers={"User-Agent": "qdl-canonical-metadata-capture/1"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 (fixed public venue hosts)
        raw = response.read()
    return raw, json.loads(raw)


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _catalog(path: Path) -> list[dict]:
    return json.loads(Path(path).read_text(encoding="utf-8"))["instruments"]


def capture(catalog: Path, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=False)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    index: dict = {"captured_at_utc": stamp, "files": {}}

    def save(name: str, url: str, *, optional: bool = False) -> None:
        try:
            raw, _ = _get(url)
        except urllib.error.HTTPError as error:
            if not optional:
                raise
            # Recorded, not fatal: render marks the instrument UNVERIFIED (no capture).
            index["files"][name] = {"url": url, "http_status": error.code, "body": error.read()[:300].decode(
                "utf-8", "replace")}
            return
        (out / name).write_bytes(raw)
        index["files"][name] = {"url": url, "sha256": _sha(raw), "bytes": len(raw)}

    for item in _catalog(catalog):
        if item["venue"] == "BINANCE" and item["market"] == "USDM":
            symbol = item["native_symbol"]
            save(f"binance-constituents-{symbol}.json",
                 f"{BINANCE}/fapi/v1/constituents?{urllib.parse.urlencode({'symbol': symbol})}", optional=True)
            time.sleep(0.25)  # weight 2 per call; far below the IP limit
    save("binance-premium-index.json", f"{BINANCE}/fapi/v1/premiumIndex")
    save("okx-instruments-swap.json", f"{OKX}/api/v5/public/instruments?instType=SWAP")
    save("okx-mark-price-swap.json", f"{OKX}/api/v5/public/mark-price?instType=SWAP")
    (out / "capture-index.json").write_text(json.dumps(index, indent=1, sort_keys=True) + "\n")
    (out / "SHA256SUMS").write_text("".join(
        f"{_sha((out / name).read_bytes())}  {name}\n" for name in sorted(p.name for p in out.iterdir())
        if name != "SHA256SUMS"))
    return {"files": len(index["files"]), "captured_at_utc": stamp}


def _rules(path: Path) -> dict:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if document.get("schema") != "qdl.v2.canonical-underlying-rules.v1":
        raise ValueError("rules schema is not qdl.v2.canonical-underlying-rules.v1")
    return {rule["id"]: rule for rule in document["rules"]}


def _source(rule: dict, endpoint: str, captured_at: str, sha: str, evidence: str) -> dict:
    return {"endpoint": endpoint, "doc_url": rule["doc_urls"][0] if rule["doc_urls"] else "-",
            "captured_at_utc": captured_at, "response_sha256": sha, "evidence": evidence}


def _unverified(item: dict, rule: dict, captured_at: str, evidence: str) -> dict:
    return {"instrument_uid": item["instrument_uid"], "instrument_id": item["instrument_id"],
            "venue": item["venue"], "market": item["market"], "native_symbol": item["native_symbol"],
            "status": STATUS_UNVERIFIED, "rule_id": rule["id"], "underlying_asset": None,
            "underlying_instrument_uid": None, "price_multiplier": None, "quantity_multiplier": None,
            "aliases": [], "source": _source(rule, rule["endpoint"], captured_at, "-", evidence)}


def _verified(item: dict, rule: dict, underlying: str, price_m: Decimal, qty_m: Decimal, endpoint: str,
              captured_at: str, sha: str, evidence: str) -> dict:
    return {"instrument_uid": item["instrument_uid"], "instrument_id": item["instrument_id"],
            "venue": item["venue"], "market": item["market"], "native_symbol": item["native_symbol"],
            "status": STATUS_VERIFIED, "rule_id": rule["id"], "underlying_asset": underlying,
            "underlying_instrument_uid": f"CRYPTO:{underlying}", "price_multiplier": format(price_m, "f"),
            "quantity_multiplier": format(qty_m.normalize(), "f"), "aliases": [],
            "source": _source(rule, endpoint, captured_at, sha, evidence)}


def _binance(item: dict, rules: dict, captures: Path, captured_at: str) -> dict:
    rule = rules["BINANCE_USDM_INDEX_CONSTITUENT"]
    path = captures / f"binance-constituents-{item['native_symbol']}.json"
    if item["product_type"] not in rule["applies_to"]["product_types"] or not path.is_file():
        return _unverified(item, rule, captured_at, "no constituents capture")
    raw = path.read_bytes()
    document = json.loads(raw)
    own = [c for c in document.get("constituents") or [] if c.get("exchange") == "binance"]
    if document.get("symbol") != item["native_symbol"] or len(own) != 1:
        return _unverified(item, rule, captured_at, f"binance constituents: {len(own)}")
    match = _SUFFIX.match(str(own[0]["symbol"]).upper())
    quote = item["quote_asset"].upper()
    if not match or not match.group("pair").endswith(quote):
        return _unverified(item, rule, captured_at, f"constituent {own[0]['symbol']!r} not <BASE>{quote}[*N]")
    base = match.group("pair")[: -len(quote)]
    n = int(match.group("n") or 1)
    venue_base = item["base_asset"].upper()
    endpoint = f"GET {BINANCE}/fapi/v1/constituents?symbol={item['native_symbol']}"
    evidence = f"binance constituent {own[0]['symbol']}; venue baseAsset {venue_base}"
    spelled = {base} if n == 1 else {prefix + base for prefix in _PREFIX_SPELLINGS.get(n, ())}
    if venue_base in spelled:
        return _verified(item, rule, base, Decimal(n), Decimal(n), endpoint, captured_at, _sha(raw), evidence)
    rename = next((r for r in rules.values() if r["status"] == STATUS_VERIFIED
                   and r["applies_to"].get("venue") == "BINANCE" and r["applies_to"].get("base_asset") == venue_base
                   and r["applies_to"].get("underlying_asset") == base and n == 1), None)
    if rename is not None:
        return _verified(item, rename, base, Decimal(1), Decimal(1), endpoint, captured_at, _sha(raw), evidence)
    return _unverified(item, rule, captured_at, f"{evidence}: spelling not covered by a reviewed rule")


def _okx(item: dict, rules: dict, okx_rows: dict, okx_sha: str, captured_at: str) -> dict:
    attributes = item.get("attributes") or {}
    if attributes.get("ctType") == "inverse":
        return _unverified(item, rules["OKX_INVERSE_CONTRACT"], captured_at, "inverse contract")
    if item["market"] == "FUTURES":
        return _unverified(item, rules["OKX_FUTURES_LINEAR"], captured_at, "futures not captured")
    rule = rules["OKX_SWAP_LINEAR_CONTRACT_VALUE"]
    row = okx_rows.get(item["native_symbol"])
    if item["market"] != "SWAP" or row is None:
        return _unverified(item, rule, captured_at, "no instruments row")
    ccy, settle = str(row.get("ctValCcy") or ""), str(row.get("settleCcy") or "")
    if row.get("ctType") != "linear" or not ccy or row.get("uly") != f"{ccy}-{settle}":
        return _unverified(item, rule, captured_at, f"ctType {row.get('ctType')} uly {row.get('uly')} ctValCcy {ccy}")
    qty = Decimal(str(row["ctVal"])) * Decimal(str(row["ctMult"]))
    if qty <= 0:
        return _unverified(item, rule, captured_at, "non-positive contract value")
    return _verified(item, rule, ccy.upper(), Decimal(1), qty, f"GET {OKX}/api/v5/public/instruments?instType=SWAP",
                     captured_at, okx_sha, f"ctVal {row['ctVal']} ctMult {row['ctMult']} ctValCcy {ccy}")


def _marks(captures: Path) -> dict[tuple[str, str], Decimal]:
    marks = {("BINANCE", row["symbol"]): Decimal(row["markPrice"]) for row in json.loads(
        (captures / "binance-premium-index.json").read_text()) if Decimal(row["markPrice"]) > 0}
    marks.update({("OKX", row["instId"]): Decimal(row["markPx"]) for row in json.loads(
        (captures / "okx-mark-price-swap.json").read_text())["data"] if row.get("markPx") and Decimal(row["markPx"]) > 0})
    return marks


def _group_ratio(members: list[dict], marks: dict) -> Decimal | None:
    """max/min underlying-unit mark price across the group's venues, or None with fewer than two priced members."""
    prices = [marks[(m["venue"], m["native_symbol"])] / Decimal(m["price_multiplier"]) for m in members
              if (m["venue"], m["native_symbol"]) in marks]
    if len(prices) < 2:
        return None
    return max(prices) / min(prices)


def render(catalog: Path, captures: Path, rules_path: Path, revision: int) -> dict:
    rules = _rules(rules_path)
    index = json.loads((captures / "capture-index.json").read_text())
    captured_at = index["captured_at_utc"]
    okx_raw = (captures / "okx-instruments-swap.json").read_bytes()
    okx_rows = {row["instId"]: row for row in json.loads(okx_raw)["data"]}
    entries = []
    for item in sorted(_catalog(catalog), key=lambda i: i["instrument_uid"]):
        if item["venue"] == "BINANCE" and item["market"] == "USDM":
            entries.append(_binance(item, rules, captures, captured_at))
        elif item["venue"] == "OKX" and item["market"] in ("SWAP", "FUTURES"):
            entries.append(_okx(item, rules, okx_rows, _sha(okx_raw), captured_at))
        elif item["market"] == "SPOT":
            entries.append(_unverified(item, rules["SPOT_BASE_ASSET"], captured_at, "spot not captured"))
        else:
            entries.append(_unverified(item, rules["NO_RULE"], captured_at, "no rule"))
    groups: dict[str, list[dict]] = {}
    for entry in entries:
        if entry["status"] == STATUS_VERIFIED:
            groups.setdefault(entry["underlying_instrument_uid"], []).append(entry)
    # Cross-venue identity check on captured mark prices: an underlying claimed by two venues must price the same
    # within ALIAS_PRICE_TOLERANCE after the multipliers; otherwise every member is UNVERIFIED (which one is wrong
    # cannot be decided from prices).
    marks = _marks(captures)
    for uid, members in list(groups.items()):
        ratio = _group_ratio(members, marks)
        if ratio is not None and abs(ratio - 1) > ALIAS_PRICE_TOLERANCE:
            for entry in members:
                rule = rules[entry["rule_id"]]
                replaced = _unverified(entry, rule, captured_at,
                                       f"cross-venue price ratio {ratio:.4f} for {uid} exceeds {ALIAS_PRICE_TOLERANCE}")
                entry.clear(); entry.update(replaced)
            del groups[uid]
    for members in groups.values():
        for entry in members:
            entry["aliases"] = sorted(
                ({"venue": other["venue"], "market": other["market"], "native_symbol": other["native_symbol"],
                  "instrument_uid": other["instrument_uid"]} for other in members if other is not entry),
                key=lambda a: (a["venue"], a["market"], a["native_symbol"]))
    document = {"schema": SCHEMA, "revision": revision, "rules_revision": json.loads(
        rules_path.read_text())["revision"], "captured_at_utc": captured_at,
        "capture_index_sha256": _sha((captures / "capture-index.json").read_bytes()), "instruments": entries}
    runtime = _runtime()
    document["sha256"] = runtime.document_sha256(document)
    runtime.CanonicalMetadataIndex(document)  # the runtime loader must accept it
    return document


def gate(path: Path, catalog: Path, captures: Path) -> dict:
    document = json.loads(path.read_text())
    index = _runtime().CanonicalMetadataIndex(document)
    problems = []
    uids = {item["instrument_uid"] for item in _catalog(catalog)}
    covered = {entry["instrument_uid"] for entry in document["instruments"]}
    if uids != covered:
        problems.append(f"coverage: missing {len(uids - covered)}, extra {len(covered - uids)}")
    marks = _marks(captures)
    groups: dict[str, list[dict]] = {}
    for entry in document["instruments"]:
        if entry["status"] == STATUS_VERIFIED:
            groups.setdefault(entry["underlying_instrument_uid"], []).append(entry)
    checked = 0
    for uid, members in groups.items():
        ratio = _group_ratio(members, marks)
        if ratio is None:
            continue
        checked += 1
        if abs(ratio - 1) > ALIAS_PRICE_TOLERANCE:
            problems.append(f"cross-venue price ratio {ratio:.4f} for {uid}")
        if len({m["venue"] for m in members}) != len(members) and any(
                m["market"] == other["market"] and m["venue"] == other["venue"]
                for m in members for other in members if m is not other):
            problems.append(f"{uid}: two instruments of one venue/market claim the same underlying")
    counts: dict[str, int] = {}
    for entry in document["instruments"]:
        key = f"{entry['venue']}/{entry['market']}/{entry['status']}"
        counts[key] = counts.get(key, 0) + 1
    multipliers = sorted({(e["native_symbol"], e["price_multiplier"]) for e in document["instruments"]
                          if e["status"] == STATUS_VERIFIED and e["price_multiplier"] != "1"})
    return {"status": "PASS" if not problems else "FAIL", "problems": problems, "instruments": len(index),
            "by_venue_market_status": dict(sorted(counts.items())), "cross_venue_price_checks": checked,
            "price_multipliers": multipliers, "sha256": document["sha256"], "revision": document["revision"]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    c = sub.add_parser("capture"); c.add_argument("--catalog", type=Path, required=True)
    c.add_argument("--out", type=Path, required=True)
    r = sub.add_parser("render"); r.add_argument("--catalog", type=Path, required=True)
    r.add_argument("--captures", type=Path, required=True)
    r.add_argument("--rules", type=Path, default=ROOT / "config/v2/canonical-underlying-rules.yaml")
    r.add_argument("--revision", type=int, required=True); r.add_argument("--out", type=Path, required=True)
    g = sub.add_parser("gate"); g.add_argument("file", type=Path)
    g.add_argument("--catalog", type=Path, required=True); g.add_argument("--captures", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "capture":
        print(json.dumps(capture(args.catalog, args.out), indent=1)); return 0
    if args.command == "render":
        document = render(args.catalog, args.captures, args.rules, args.revision)
        args.out.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n")
        print(json.dumps({"sha256": document["sha256"], "instruments": len(document["instruments"])})); return 0
    result = gate(args.file, args.catalog, args.captures)
    print(json.dumps(result, indent=1)); return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

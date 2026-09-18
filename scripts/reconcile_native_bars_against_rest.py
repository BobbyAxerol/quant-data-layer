#!/usr/bin/env python3
"""Compare the canonical 1m bars the native lane published against the venue's REST row.

Read-only, on both sides. It reads the durable spool through the query role and
reads Binance REST; it publishes nothing, revises nothing and touches no
runtime.

Why it is a separate reader and not a publisher. R1.28 moves Binance USD-M 1m
from the REST edge to the venue's own `@kline_1m` frame. The REST read is not
deleted: it becomes reconciliation. But *publishing* a revision when the two
disagree is a consumer-contract change - a strategy that acted on a closed bar
which is later revised must keep the revision it saw, and decision history is
never rewritten - and that contract lives in the trading system, not here.
So this answers the question first: how often, and by how much, do they
actually disagree? On the admission run of 2026-09-18 the answer was that REST
converges onto the websocket bar within 6 s in 15 of 15 cases, which is why the
REST edge needed a settlement guard and the native lane does not.

Comparison identity is venue + product + symbol + interval + `open_time`, asked
for with an explicit `startTime`, never "the last row": a REST replica that is
still catching up must be visible as a disagreement on a named bar, not
silently accepted as a newer one.

    python3 -B scripts/reconcile_native_bars_against_rest.py
    python3 -B scripts/reconcile_native_bars_against_rest.py --bars 20 --json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.request

QUERY_ROLE = "qdl_v2_stable_candidate-query_v2_1-1"
SPOOL = "/var/lib/qdl-stable/shared/canonical-cache.sqlite3"
REST_KLINES = (
    "https://fapi.binance.com/fapi/v1/klines"
    "?symbol={symbol}&interval={interval}&startTime={start}&limit=1"
)
# The fields both sides carry for one bar. Close time is derived from the
# interval and is identical by construction, so it is not a comparison.
FIELDS = ("open", "high", "low", "close", "volume", "quote_volume", "trade_count")

PROBE = r'''
import json, sqlite3, sys
sys.path.insert(0, "/app/generated/python")
from qdl.marketdata.v2 import market_data_pb2 as md

envelope = md.EventEnvelope
connection = sqlite3.connect("file:%(spool)s?mode=ro", uri=True)
seek = ("select committed_at_ns, payload from events "
        "where stream = ? and partition_key = ? order by logical_offset desc limit %(bars)d")
out = []
for stream, key in connection.execute("select stream, partition_key from partitions"):
    if "/bar/" not in key or "binance" not in key:
        continue
    for committed_at_ns, payload in connection.execute(seek, (stream, key)):
        message = envelope.FromString(payload)
        bar = message.bar
        def scalar(value):
            # A canonical decimal carries the provider's own text alongside a
            # mantissa/scale pair. The provider text is what a field-by-field
            # comparison against the venue must use; the pair is the fallback
            # when a producer did not preserve it.
            if value.source_text:
                return value.source_text
            if value.mantissa_text:
                return value.mantissa_text
            if value.scale:
                return str(value.mantissa / (10 ** value.scale))
            return str(value.mantissa)
        if bar.interval != "%(interval)s":
            # The interval lives on the bar, not in the partition key. One
            # Binance binding is pinned to `...-bar-stable-001` rather than the
            # generated `...-bar-1m-primary-v2`, and a key-substring filter
            # silently drops it - which is how BTCUSDT went missing from the
            # first run of this script.
            break
        out.append({
            "partition_key": key,
            "symbol": message.native_symbol,
            "interval": bar.interval,
            "open_time_ms": bar.open_time_ns // 1_000_000,
            "committed_at_ns": committed_at_ns,
            "lifecycle": bar.lifecycle,
            "origin": bar.origin,
            "revision": bar.revision,
            "open": scalar(bar.open), "high": scalar(bar.high), "low": scalar(bar.low),
            "close": scalar(bar.close), "volume": scalar(bar.volume),
            "quote_volume": scalar(bar.quote_volume),
            "trade_count": str(bar.trade_count),
        })
print(json.dumps(out))
'''


def read_canonical(role: str, interval: str, bars: int) -> list[dict]:
    script = PROBE % {"spool": SPOOL, "bars": bars, "interval": interval}
    result = subprocess.run(
        ["docker", "exec", "-i", role, "python", "-"],
        input=script, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise SystemExit(f"spool probe failed: {result.stderr.strip()[:400]}")
    return json.loads(result.stdout)


def rest_bar(symbol: str, interval: str, open_ms: int) -> dict[str, str] | None:
    url = REST_KLINES.format(symbol=symbol, interval=interval, start=open_ms)
    with urllib.request.urlopen(url, timeout=10) as response:
        rows = json.load(response)
    if not rows or int(rows[0][0]) != open_ms:
        return None
    row = rows[0]
    return {
        "open": row[1], "high": row[2], "low": row[3], "close": row[4],
        "volume": row[5], "quote_volume": row[7], "trade_count": str(row[8]),
    }


def equal(left: str | None, right: str | None) -> bool:
    """Decimal equality, not string equality.

    The venue returns `61200.00` where the canonical decimal may carry a
    different scale for the same number. A scale difference is not a
    disagreement and reporting it as one would bury the disagreements that are.
    """
    if left is None or right is None:
        return left == right
    try:
        from decimal import Decimal

        return Decimal(left) == Decimal(right)
    except Exception:                                  # noqa: BLE001 - compared as text
        return left == right


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", default=QUERY_ROLE)
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--bars", type=int, default=10, help="newest bars per symbol")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    canonical = read_canonical(args.role, args.interval, args.bars)
    if not canonical:
        print(f"  no canonical Binance {args.interval} bars in the spool")
        return 1

    findings: list[dict] = []
    for row in canonical:
        try:
            venue_row = rest_bar(row["symbol"], args.interval, row["open_time_ms"])
        except Exception as exc:                        # noqa: BLE001 - reported
            findings.append({**row, "rest": None, "error": f"{type(exc).__name__}: {exc}"})
            continue
        if venue_row is None:
            findings.append({**row, "rest": None, "missing": True})
            continue
        differs = [f for f in FIELDS if not equal(row.get(f), venue_row.get(f))]
        findings.append({**row, "rest": venue_row, "differs": differs})

    if args.json:
        print(json.dumps(findings, indent=2, sort_keys=True))
    else:
        by_symbol: dict[str, list[dict]] = {}
        for item in findings:
            by_symbol.setdefault(item["symbol"], []).append(item)
        print(f"  {'symbol':10} {'bars':>5} {'agree':>6} {'differ':>7} {'missing':>8}  fields that differed")
        for symbol in sorted(by_symbol):
            rows = by_symbol[symbol]
            differ = [r for r in rows if r.get("differs")]
            missing = [r for r in rows if r.get("missing") or r.get("error")]
            agree = len(rows) - len(differ) - len(missing)
            fields = sorted({f for r in differ for f in r["differs"]})
            print(f"  {symbol:10} {len(rows):5} {agree:6} {len(differ):7} {len(missing):8}  "
                  f"{','.join(fields) if fields else '-'}")
        for item in findings:
            if item.get("differs"):
                detail = ", ".join(
                    f"{f}: canonical={item.get(f)} rest={item['rest'][f]}"
                    for f in item["differs"]
                )
                print(f"    DIFFERS {item['symbol']} open_ms={item['open_time_ms']}  {detail}")
            elif item.get("missing"):
                print(f"    NO REST ROW {item['symbol']} open_ms={item['open_time_ms']}")
            elif item.get("error"):
                print(f"    REST ERROR  {item['symbol']} open_ms={item['open_time_ms']}  {item['error']}")

    disagreements = sum(1 for item in findings if item.get("differs") or item.get("missing"))
    print(f"\n{len(findings)} canonical bars compared, {disagreements} disagreed with REST")
    return 1 if disagreements else 0


if __name__ == "__main__":
    raise SystemExit(main())

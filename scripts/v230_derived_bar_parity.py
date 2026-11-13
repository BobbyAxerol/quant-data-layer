"""DL v2.3.0 R-cert 1: offline parity of DERIVED_FINAL bars against OKX native candles.

Purpose: capture OKX public candles (1m plus every native interval, and native
1Dutc plus 2Dutc/3Dutc/1Wutc) for N complete UTC days per instrument. Each
capture is written in the format of
``tests/fixtures/okx_derived_bar_parity_ldo_20261112.json``, one file per
(instrument, day block). The script then compares, through the repo's own
``resample_final_bars``, every native bar with the aggregate of the derivation
chain. The Rust core path runs on the same directory through the ignored test
``qdl-core derived_bar::tests::okx_parity_directory``
(``QDL_DERIVED_PARITY_DIR=<out>``).

Boundary: public REST only (``/api/v5/market/history-candles``, no login),
paced at ``--rps`` (default 5, the planner's OKX policy). The script reads and
writes nothing outside ``--out``. It is a certification tool: it changes no
runtime, and a mismatch is reported, never repaired.

Usage (``--rm`` container, 1 CPU):
  python -B scripts/v230_derived_bar_parity.py --out DIR --days 30 \
      --end-day 2026-11-12 BTC-USDT-SWAP ETH-USDT-SWAP ...
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DAY = 86_400_000
SPAN = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1H": 3_600_000, "2H": 7_200_000, "4H": 14_400_000, "6Hutc": 21_600_000,
    "12Hutc": 43_200_000, "1Dutc": DAY, "2Dutc": 2 * DAY, "3Dutc": 3 * DAY, "1Wutc": 7 * DAY,
}
INTRADAY = {
    "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "2h": "2H",
    "4h": "4H", "6h": "6Hutc", "12h": "12Hutc", "1d": "1Dutc",
}
CALENDAR = {"2d": "2Dutc", "3d": "3Dutc", "1w": "1Wutc"}
BASE = "https://www.okx.com/api/v5/market/history-candles"


def fetch(client, inst: str, bar: str, start: int, end: int, pause: float) -> list[list[str]]:
    rows: dict[int, list[str]] = {}
    after = end
    for _ in range(2_000):
        reply = client.get(BASE, params={"instId": inst, "bar": bar, "after": str(after), "limit": "100"}).json()
        time.sleep(pause)
        if reply.get("code") != "0":
            raise RuntimeError(f"{inst} {bar}: {reply}")
        data = reply.get("data") or []
        if not data:
            break
        for row in data:
            opened = int(row[0])
            if start <= opened and opened + SPAN[bar] <= end:
                rows[opened] = row
        oldest = min(int(row[0]) for row in data)
        if oldest <= start:
            break
        after = oldest
    return [rows[key] for key in sorted(rows)]


def capture(client, inst: str, day_start: int, pause: float) -> dict:
    day_end = day_start + DAY
    monday = day_start - ((day_start // DAY + 3) % 7) * DAY
    week_start = monday - 28 * DAY
    doc = {
        "schema": "qdl.test.okx-derived-bar-parity.v1", "instId": inst,
        "source": "OKX public GET /api/v5/market/history-candles (no login)",
        "captured_at_ms": int(time.time() * 1000),
        "intraday_window_ms": [day_start, day_end], "calendar_window_ms": [week_start, monday],
        "row_fields": ["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"],
        "candles": {},
    }
    for bar in ("1m", *INTRADAY.values()):
        doc["candles"][bar] = fetch(client, inst, bar, day_start, day_end, pause)
    doc["candles"]["1Dutc_calendar"] = fetch(client, inst, "1Dutc", week_start, monday, pause)
    for bar in CALENDAR.values():
        doc["candles"][bar] = fetch(client, inst, bar, week_start, monday, pause)
    return doc


def okx_row_item(row: list[str], interval: str):
    """One OKX SWAP candle row as a final canonical BAR item (contract units)."""

    from qdl.query import FeedType
    from qdl.query.lifecycle import BarLifecycle
    from qdl.query.results import ContractMetadata, MarketDataItem, QualityMetadata, SourceMetadata
    from qdl.adapters.intervals import canonical_interval_ms

    open_ms = int(row[0])
    step = canonical_interval_ms(interval)
    return MarketDataItem(
        instrument_uid="parity", instrument_id="OKX:SWAP:PERPETUAL:PARITY", instrument_revision=1,
        feed=FeedType.BAR, interval=interval, observed_at_ns=(open_ms + step) * 1_000_000 - 1,
        payload={
            "open_time_ns": open_ms * 1_000_000, "close_time_ns": (open_ms + step - 1) * 1_000_000,
            "open": row[1], "high": row[2], "low": row[3], "close": row[4], "volume": row[5],
            "volume_unit": "CONTRACT", "base_volume": row[6], "quote_volume": row[7],
            "contract_volume": row[5], "trade_count": 0, "origin": "VENUE_NATIVE", "is_final": True,
        },
        source=SourceMetadata("OKX", "OKX_DIRECT", "parity", "PRIMARY", True),
        quality=QualityMetadata("LIVE", 0, False, True, True, "crypto_primary_v2"),
        contract=ContractMetadata("a" * 64, "2.0.0", "parity", "parity", 1, 1, 1, 1, "parity"),
        bar_lifecycle=BarLifecycle.FINAL,
    )


def same_values(item, native: list[str]) -> bool:
    fields = ("open", "high", "low", "close", "volume", "base_volume", "quote_volume")
    return all(Decimal(item.payload[name]) == Decimal(native[index])
               for index, name in enumerate(fields, start=1))


def compare(doc: dict) -> dict:
    """Native vs ``resample_final_bars`` for one capture (the warmup path)."""

    from qdl.warmup.handoff import resample_final_bars

    _okx_item, _same = okx_row_item, same_values

    result = {"compared": 0, "mismatches": [], "missing_1m": 0, "zero_volume_1m": 0,
              "unconfirmed": 0}
    minutes = doc["candles"]["1m"]
    result["missing_1m"] = 1_440 - len(minutes)
    result["zero_volume_1m"] = sum(1 for row in minutes if Decimal(row[5]) == 0)
    result["unconfirmed"] = sum(1 for rows in doc["candles"].values() for row in rows if row[8] != "1")
    sources = {
        "1m": tuple(_okx_item(row, "1m") for row in minutes),
        "1d": tuple(_okx_item(row, "1d") for row in doc["candles"]["1Dutc_calendar"]),
    }
    for table, base in ((INTRADAY, "1m"), (CALENDAR, "1d")):
        for interval, native_bar in table.items():
            try:
                derived = {item.payload["open_time_ns"] // 1_000_000: item
                           for item in resample_final_bars(sources[base], target_interval=interval)}
            except ValueError as error:  # a hole in the base: reported, not hidden
                result["mismatches"].append({"interval": interval, "error": str(error)})
                continue
            for native in doc["candles"][native_bar]:
                item = derived.get(int(native[0]))
                result["compared"] += 1
                if item is None or not _same(item, native):
                    result["mismatches"].append({
                        "interval": interval, "open_ms": int(native[0]), "native": native,
                        "derived": None if item is None else item.payload,
                    })
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--end-day", required=True, help="last complete UTC day, YYYY-MM-DD")
    parser.add_argument("--rps", type=float, default=5.0)
    parser.add_argument("--compare-only", action="store_true")
    parser.add_argument("instruments", nargs="+")
    args = parser.parse_args()
    if not 1 <= args.days <= 92 or not 0.5 <= args.rps <= 8:
        raise SystemExit("--days must be 1..92 and --rps 0.5..8")
    last = date.fromisoformat(args.end_day)
    last_ms = int(datetime(last.year, last.month, last.day, tzinfo=timezone.utc).timestamp() * 1000)
    args.out.mkdir(parents=True, exist_ok=True)
    summary = {"end_day": args.end_day, "days": args.days, "files": {}, "compared": 0, "mismatches": 0}
    client = None
    if not args.compare_only:
        import httpx

        client = httpx.Client(timeout=20.0, trust_env=False)
    for inst in args.instruments:
        # One capture per day: its 1m block is that day; its calendar block is
        # the 28 days before that day's Monday (overlap between days is fine).
        for offset in range(args.days):
            day_start = last_ms - offset * DAY
            path = args.out / f"{inst}-{day_start}.json"
            if not path.exists():
                if client is None:
                    continue
                path.write_text(json.dumps(capture(client, inst, day_start, 1 / args.rps),
                                           separators=(",", ":")), encoding="utf-8")
            result = compare(json.loads(path.read_text(encoding="utf-8")))
            summary["files"][path.name] = {k: v for k, v in result.items() if k != "mismatches"} | {
                "mismatches": len(result["mismatches"]), "examples": result["mismatches"][:3]}
            summary["compared"] += result["compared"]
            summary["mismatches"] += len(result["mismatches"])
            print(path.name, result["compared"], len(result["mismatches"]), flush=True)
    if client is not None:
        client.close()
    (args.out / "summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    print(json.dumps({"compared": summary["compared"], "mismatches": summary["mismatches"]}))
    return 0 if summary["mismatches"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

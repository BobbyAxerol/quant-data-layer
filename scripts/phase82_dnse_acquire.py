#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from app.database.dnse_fallback import fetch_dnse_ohlcv_direct


def canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    frame = fetch_dnse_ohlcv_direct(
        "VN30F1M", args.date, args.date, resolution="1"
    )
    if frame.empty:
        raise RuntimeError(f"DNSE returned no authentic rows for {args.date}")
    rows = []
    for row in frame.itertuples(index=False):
        opened = row.time.to_pydatetime().replace(
            tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")
        )
        open_ms = int(opened.timestamp() * 1000)
        rows.append({
            "symbol": "VN30F1M", "interval": "1m",
            "open_time_ms": open_ms, "close_time_ms": open_ms + 59_999,
            "o": str(row.open), "h": str(row.high), "l": str(row.low),
            "c": str(row.close), "v": str(row.volume), "is_final": True,
            "trade_count_available": False, "revision": 0,
        })
    payload = {
        "schema": "qdl.phase8.dnse-sdk-delivery.v1",
        "status": "PASS",
        "provenance": "REAL_DNSE_PUBLIC_MARKETDATA_READ_ONLY",
        "production_writes": 0,
        "symbol": "VN30F1M",
        "trading_date": args.date,
        "rows": rows,
        "rows_sha256": hashlib.sha256(canonical_json(rows)).hexdigest(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_json(payload))
    print(json.dumps({
        "status": "PASS", "rows": len(rows),
        "rows_sha256": payload["rows_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

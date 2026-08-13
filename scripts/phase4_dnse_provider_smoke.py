from __future__ import annotations

import argparse
import json
from datetime import datetime, time, timedelta
from pathlib import Path

from app.database.dnse_fallback import fetch_dnse_ohlcv_direct


def expected_provider_times(day: datetime) -> set[datetime]:
    result = set()
    current = day.replace(hour=9, minute=0, second=0, microsecond=0)
    morning_end = day.replace(hour=11, minute=30, second=0, microsecond=0)
    while current < morning_end:
        result.add(current)
        current += timedelta(minutes=1)
    current = day.replace(hour=13, minute=0, second=0, microsecond=0)
    afternoon_end = day.replace(hour=14, minute=30, second=0, microsecond=0)
    while current < afternoon_end:
        result.add(current)
        current += timedelta(minutes=1)
    result.add(day.replace(hour=14, minute=45, second=0, microsecond=0))
    return result


def run(day_text: str, output: Path) -> dict:
    day = datetime.strptime(day_text, "%Y-%m-%d")
    if day.weekday() >= 5:
        raise ValueError("DNSE bounded provider smoke requires a completed weekday")
    frame = fetch_dnse_ohlcv_direct("VN30F1M", day_text, day_text, resolution="1")
    if frame.empty:
        raise RuntimeError("DNSE real provider returned no VN30F1M rows")
    observed = {value.to_pydatetime() for value in frame["time"]}
    expected = expected_provider_times(day)
    missing = sorted(expected.difference(observed))
    outside = sorted(observed.difference(expected))
    report = {
        "schema": "qdl.phase4.dnse-provider-coverage.v1",
        "status": "PASS" if not missing and not outside else "FAIL",
        "provenance": "REAL_DNSE_PUBLIC_MARKETDATA_READ_ONLY",
        "production_writes": 0,
        "symbol": "VN30F1M",
        "trading_date": day_text,
        "provider_bar_session": "09:00-11:29,13:00-14:29,14:45 Asia/Ho_Chi_Minh",
        "market_preopen_note": "08:45 market session is not represented as DNSE OHLCV bars",
        "observed_rows": len(frame),
        "expected_rows": len(expected),
        "observed_first": min(observed).isoformat(),
        "observed_last": max(observed).isoformat(),
        "missing_expected_rows": len(missing),
        "outside_session_rows": len(outside),
        "missing_sample": [item.isoformat() for item in missing[:10]],
        "outside_sample": [item.isoformat() for item in outside[:10]],
        "fabricated_rows": 0,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if report["status"] != "PASS":
        raise RuntimeError("DNSE provider session coverage failed")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.output), sort_keys=True))


if __name__ == "__main__":
    main()

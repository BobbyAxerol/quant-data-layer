#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from qdl.runtime.canary_source import CanarySourceBinding, CanarySourceCatalog


def _decimal_source(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, dict) or not isinstance(value.get("source_text"), str):
        raise AssertionError(f"V2 {field} has no exact decimal source_text")
    return value["source_text"]


def validate_sample(
    v1: dict[str, Any],
    v2: dict[str, Any],
    binding: CanarySourceBinding,
) -> dict[str, int]:
    identity = {
        "provider": v1.get("provider") == "binance",
        "market": str(v1.get("market", "")).lower() == "usdm",
        "symbol": str(v1.get("symbol", "")).upper()
        == binding.instrument.native_symbol,
        "interval": v1.get("requested_interval") == binding.interval,
        "schema": v2.get("schema") == "qdl.marketdata.warmup.v2",
    }
    failed = [name for name, passed in identity.items() if not passed]
    if failed:
        raise AssertionError(f"V1/V2 response identity mismatch: {failed}")

    rows = v1.get("data")
    events = v2.get("data")
    if not isinstance(rows, list) or not isinstance(events, list) or not events:
        raise AssertionError("V1 rows and non-empty V2 data are required")
    if v2.get("count") != len(events):
        raise AssertionError("V2 count does not match data length")
    expected = {int(row[0]) * 1_000_000: row for row in rows}
    if len(expected) != len(rows):
        raise AssertionError("V1 source contains duplicate open times")

    open_times: list[int] = []
    for item in events:
        payload = item.get("payload", {})
        source = item.get("source", {})
        quality = item.get("quality", {})
        open_time = int(payload.get("open_time_ns", 0))
        row = expected.get(open_time)
        if row is None:
            raise AssertionError("V2 bar does not correspond to a V1 source row")
        checks = {
            "instrument_uid": item.get("instrument_uid")
            == binding.instrument.instrument_uid,
            "instrument_id": item.get("instrument_id")
            == binding.instrument.instrument_id,
            "feed": item.get("feed") == "BAR" and payload.get("feed") == "BAR",
            "interval": item.get("interval") == binding.interval
            and payload.get("interval") == binding.interval,
            "open": _decimal_source(payload, "open") == str(row[1]),
            "high": _decimal_source(payload, "high") == str(row[2]),
            "low": _decimal_source(payload, "low") == str(row[3]),
            "close": _decimal_source(payload, "close") == str(row[4]),
            "volume": _decimal_source(payload, "volume") == str(row[5]),
            "close_time": int(payload.get("close_time_ns", 0))
            == int(row[6]) * 1_000_000,
            "trade_count": int(payload.get("trade_count", -1)) == int(row[8]),
            "final": payload.get("lifecycle") == "FINAL",
            "source_id": source.get("source_id") == binding.source_id,
            "source_role": source.get("source_role") == binding.source_role,
            "authoritative": source.get("authoritative") is binding.authoritative,
            "policy": quality.get("policy_id") == binding.source_policy_id,
            "complete": quality.get("complete") is True,
            "gap_closed": quality.get("gap_open") is False,
            "execution_forbidden": quality.get("execution_eligible") is False,
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise AssertionError(f"V1/V2 canonical parity failed: {failed}")
        open_times.append(open_time)

    if open_times != sorted(open_times) or len(open_times) != len(set(open_times)):
        raise AssertionError("V2 bars are duplicated or not strictly ordered")
    watermark = int(v2.get("watermark_offset", -1))
    if watermark < len(events):
        raise AssertionError("V2 watermark is below the returned history length")
    return {
        "count": len(events),
        "first_open_time_ns": open_times[0],
        "last_open_time_ns": open_times[-1],
        "watermark_offset": watermark,
    }


def validate_window(first: dict[str, int], second: dict[str, int]) -> None:
    delta = second["watermark_offset"] - first["watermark_offset"]
    if delta < 0 or delta > 1:
        raise AssertionError(
            f"continuous bridge watermark advanced outside one 1m-bar window: {delta}"
        )
    if second["last_open_time_ns"] < first["last_open_time_ns"]:
        raise AssertionError("continuous bridge moved the latest bar backwards")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bindings", type=Path, required=True)
    parser.add_argument("--v1-first", type=Path, required=True)
    parser.add_argument("--v2-first", type=Path, required=True)
    parser.add_argument("--v1-second", type=Path, required=True)
    parser.add_argument("--v2-second", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binding = CanarySourceCatalog.load(args.source_bindings).bindings[0]
    first = validate_sample(
        json.loads(args.v1_first.read_text()),
        json.loads(args.v2_first.read_text()),
        binding,
    )
    second = validate_sample(
        json.loads(args.v1_second.read_text()),
        json.loads(args.v2_second.read_text()),
        binding,
    )
    validate_window(first, second)
    result = {
        "schema": "qdl.phase9.0-b.bridge-parity.v1",
        "status": "PASS",
        "authority": "V1_SHADOW_READ_ONLY",
        "source": "REAL_V1_PROVIDER_DATA",
        "generated_market_events": 0,
        "slice": {
            "venue": "BINANCE",
            "market": "USDM",
            "product_type": "PERPETUAL",
            "symbol": binding.instrument.native_symbol,
            "feed": binding.feed,
            "interval": binding.interval,
        },
        "first": first,
        "second": second,
        "watermark_delta": second["watermark_offset"] - first["watermark_offset"],
        "canonical_mismatches": 0,
        "duplicate_open_times": 0,
        "non_final_bars": 0,
        "execution_eligible_events": 0,
    }
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()

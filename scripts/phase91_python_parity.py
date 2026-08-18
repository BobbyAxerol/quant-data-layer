#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import statistics
import sys
import time
from dataclasses import fields
from typing import Any

from qdl.canonical.trade import TradeContext, canonicalize_binance_usdm_trade


def percentile(values: list[float], ratio: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, round((len(ordered) - 1) * ratio))]


def context(value: dict[str, Any]) -> TradeContext:
    known = {item.name for item in fields(TradeContext)}
    unknown = set(value) - known
    if unknown:
        raise ValueError(f"unknown TradeContext fields: {sorted(unknown)}")
    payload = dict(value)
    payload["raw_capture_id"] = bytes(payload.get("raw_capture_id", []))
    payload["raw_frame_sha256"] = bytes(payload.get("raw_frame_sha256", []))
    return TradeContext(**payload)


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: phase91_python_parity.py BUNDLE")
    payload = json.loads(pathlib.Path(sys.argv[1]).read_text())
    if set(payload) != {"fixtures", "repeat"}:
        raise ValueError("Phase 9.1 replay bundle fields are invalid")
    fixtures = payload["fixtures"]
    repeat = int(payload["repeat"])
    if not fixtures or repeat <= 0:
        raise ValueError("Phase 9.1 replay bundle must be non-empty")
    aggregate = hashlib.sha256()
    first_record_hashes: list[str] = []
    latencies_ms: list[float] = []
    started = time.perf_counter()
    for iteration in range(repeat):
        for fixture in fixtures:
            if fixture.get("provider_kind") != "binance_usdm_trade":
                raise ValueError("Phase 9.1 candidate accepts Binance USD-M TRADE only")
            event_started = time.perf_counter_ns()
            event = canonicalize_binance_usdm_trade(
                fixture["raw"], context(fixture["context"])
            )
            encoded = event.SerializeToString(deterministic=True)
            latencies_ms.append((time.perf_counter_ns() - event_started) / 1_000_000)
            aggregate.update(len(encoded).to_bytes(8, "big"))
            aggregate.update(encoded)
            if iteration == 0:
                first_record_hashes.append(hashlib.sha256(encoded).hexdigest())
    elapsed = time.perf_counter() - started
    print(json.dumps({
        "schema": "qdl.phase91.python-parity.v1",
        "status": "PASS",
        "events": len(fixtures) * repeat,
        "fixture_count": len(fixtures),
        "repeat": repeat,
        "aggregate_sha256": aggregate.hexdigest(),
        "record_sha256": first_record_hashes,
        "events_per_second": len(fixtures) * repeat / max(elapsed, 1e-9),
        "latency_ms": {
            "p50": percentile(latencies_ms, 0.50),
            "p95": percentile(latencies_ms, 0.95),
            "p99": percentile(latencies_ms, 0.99),
            "max": max(latencies_ms),
            "mean": statistics.fmean(latencies_ms),
        },
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

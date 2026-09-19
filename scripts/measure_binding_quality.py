#!/usr/bin/env python3
"""Bounded, read-only consumer-quality matrix for declared V2 bindings.

This probe measures what an authenticated consumer actually sees.  It never
prints market payloads, contacts a venue directly, writes a cursor, or submits
an order.  ``feed_status`` records typed quality; an optional snapshot read
confirms whether that same quality is usable by the public SDK.

The default rate is deliberately below the declared 1,500 request/minute
Trading-System consumer quota when status and snapshot are both enabled.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from collections import Counter, defaultdict

from measure_consumer_request_latency import requirements, transports
from qdl_sdk.client import AsyncDataLayerClient


_MAX_FLAGS_PER_VIOLATION = 16


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int(len(ordered) * fraction))], 3)


def _summary(values: list[float]) -> dict[str, float | int | None]:
    return {
        "n": len(values),
        "p50_ms": round(statistics.median(values), 3) if values else None,
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "max_ms": round(max(values), 3) if values else None,
    }


def _quality_is_usable(requirement, quality) -> bool:
    """Apply the declared strict/quiet policy without inventing a raw-age rule."""

    event_freshness_required = (
        requirement.effective_event_recency_policy.value != "OBSERVE"
    )
    return bool(
        quality.state == "LIVE"
        and quality.complete
        and quality.execution_eligible
        and not quality.gap_open
        and (
            not event_freshness_required
            or requirement.max_freshness_ms is None
            or quality.freshness_ms <= requirement.max_freshness_ms
        )
    )


async def _run(
    *,
    duration_seconds: float,
    period_seconds: float,
    include_snapshot: bool,
    max_violation_evidence: int,
) -> dict[str, object]:
    query, stream = transports()
    client = AsyncDataLayerClient(
        query_transport=query,
        stream_transport=stream,
        consumer_id=os.environ.get(
            "QDL_CONSUMER_ID", "trading-system.paper.stable"
        ),
    )
    per_binding: dict[str, dict[str, object]] = {}
    selected = requirements()
    started = time.monotonic()
    rounds = 0
    try:
        while time.monotonic() - started < duration_seconds:
            cycle_started = time.monotonic()
            for label, requirement in selected:
                key = f"{requirement.instrument_uid}:{requirement.feed.value}:{requirement.interval or '-'}"
                row = per_binding.setdefault(key, {
                    "instrument_uid": requirement.instrument_uid,
                    "feed": requirement.feed.value,
                    "interval": requirement.interval,
                    "status_latency_ms": [],
                    "snapshot_latency_ms": [],
                    "quality_states": Counter(),
                    "event_recency_states": Counter(),
                    "session_states": Counter(),
                    "snapshot_outcomes": Counter(),
                    "errors": Counter(),
                    "strict_violation_count": 0,
                    "violations": [],
                })
                started_call = time.perf_counter()
                try:
                    status = await client.feed_status(requirement)
                    row["status_latency_ms"].append(
                        (time.perf_counter() - started_call) * 1000.0
                    )
                    quality = status.quality
                    row["quality_states"][quality.state] += 1
                    row["event_recency_states"][quality.event_recency_state] += 1
                    row["session_states"][quality.provider_session_state] += 1
                    strict_ok = _quality_is_usable(requirement, quality)
                    if not strict_ok:
                        row["strict_violation_count"] += 1
                        violations = row["violations"]
                        assert isinstance(violations, list)
                        if len(violations) < max_violation_evidence:
                            violations.append({
                                "observed_at_ns": time.time_ns(),
                                "max_freshness_ms": requirement.max_freshness_ms,
                                "quality_state": quality.state,
                                "freshness_ms": quality.freshness_ms,
                                "event_recency_state": quality.event_recency_state,
                                "provider_session_state": quality.provider_session_state,
                                "provider_session_liveness_ms": (
                                    quality.provider_session_liveness_ms
                                ),
                                "gap_open": quality.gap_open,
                                "complete": quality.complete,
                                "execution_eligible": quality.execution_eligible,
                                "flags": sorted(quality.flags)[:_MAX_FLAGS_PER_VIOLATION],
                            })
                    if include_snapshot and strict_ok:
                        started_snapshot = time.perf_counter()
                        try:
                            await client.snapshot(requirement)
                        except Exception as error:  # noqa: BLE001 - typed output below
                            row["snapshot_outcomes"]["REJECTED"] += 1
                            row["errors"][type(error).__name__] += 1
                        else:
                            row["snapshot_latency_ms"].append(
                                (time.perf_counter() - started_snapshot) * 1000.0
                            )
                            row["snapshot_outcomes"]["USABLE"] += 1
                    elif include_snapshot:
                        row["snapshot_outcomes"]["NOT_ATTEMPTED_STALE"] += 1
                except Exception as error:  # noqa: BLE001 - typed output below
                    row["errors"][type(error).__name__] += 1
                    row["snapshot_outcomes"]["STATUS_REJECTED"] += 1
                    row["strict_violation_count"] += 1
                    violations = row["violations"]
                    assert isinstance(violations, list)
                    if len(violations) < max_violation_evidence:
                        violations.append({
                            "observed_at_ns": time.time_ns(),
                            "error_class": type(error).__name__,
                        })
            rounds += 1
            remaining = period_seconds - (time.monotonic() - cycle_started)
            if remaining > 0:
                await asyncio.sleep(remaining)
    finally:
        await client.close()

    rows = []
    for key, row in sorted(per_binding.items()):
        rows.append({
            "binding": key,
            "instrument_uid": row["instrument_uid"],
            "feed": row["feed"],
            "interval": row["interval"],
            "status_latency": _summary(row["status_latency_ms"]),
            "snapshot_latency": _summary(row["snapshot_latency_ms"]),
            "quality_states": dict(sorted(row["quality_states"].items())),
            "event_recency_states": dict(sorted(row["event_recency_states"].items())),
            "session_states": dict(sorted(row["session_states"].items())),
            "snapshot_outcomes": dict(sorted(row["snapshot_outcomes"].items())),
            "errors": dict(sorted(row["errors"].items())),
            "strict_violation_count": row["strict_violation_count"],
            "violations": row["violations"],
        })
    return {
        "schema": "qdl.binding-quality-consumer-matrix.v1",
        "duration_seconds": duration_seconds,
        "period_seconds": period_seconds,
        "rounds": rounds,
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--period-seconds", type=float, default=2.0)
    parser.add_argument("--without-snapshot", action="store_true")
    parser.add_argument("--max-violation-evidence", type=int, default=10)
    args = parser.parse_args()
    if (
        args.duration_seconds <= 0
        or args.period_seconds < 1.0
        or not 1 <= args.max_violation_evidence <= 100
    ):
        raise SystemExit("duration must be positive and period must be at least one second")
    payload = asyncio.run(_run(
        duration_seconds=args.duration_seconds,
        period_seconds=args.period_seconds,
        include_snapshot=not args.without_snapshot,
        max_violation_evidence=args.max_violation_evidence,
    ))
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0 if all(
        not row["strict_violation_count"] and not row["errors"]
        for row in payload["rows"]
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())

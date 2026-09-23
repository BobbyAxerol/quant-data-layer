#!/usr/bin/env python3
"""Repair a bounded final-BAR history hole through the normal V2 data plane."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.runtime.stable_bar_edge import (
    build_from_environment,
    build_readonly_repair_probe_from_environment,
)


CONFIRM = "REPAIR_QDL_STABLE_FINAL_BAR_HISTORY"


def _expected_missing(values: list[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        binding_id, separator, count = value.partition("=")
        if not separator or not binding_id or not count.isdigit():
            raise ValueError("--expected-missing must use binding_id=non_negative_integer")
        if binding_id in result:
            raise ValueError("--expected-missing binding appears more than once")
        result[binding_id] = int(count)
    return result


def _summary(plan, *, remaining_rows: int | None = None, published_rows: int | None = None) -> dict:
    result = {
        "binding_id": plan.source.binding_id,
        "venue": plan.acquisition.runtime,
        "window_rows": len(plan.envelopes),
        "missing_rows": len(plan.missing_envelopes),
        "first_open_ms": min(plan.expected_opens),
        "last_open_ms": max(plan.expected_opens),
    }
    if published_rows is not None:
        result["published_rows"] = published_rows
    if remaining_rows is not None:
        result["remaining_rows"] = remaining_rows
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", action="append", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--observed-ms", type=int, help="pin the provider history window to this UTC epoch millisecond")
    parser.add_argument("--expected-open", action="append", default=[], help="optional exact missing open: binding_id=UTC_epoch_ms (repeatable)")
    parser.add_argument("--expected-missing", action="append", required=True)
    parser.add_argument("--wait-seconds", type=float, default=180.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="use a publisher-disabled provider/cache inspection client",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args(argv)

    bindings = tuple(dict.fromkeys(args.binding))
    if len(bindings) != len(args.binding):
        raise SystemExit("--binding may not repeat")
    if not 1 <= args.rows <= 10_000:
        raise SystemExit("--rows must be between 1 and 10000")
    if args.wait_seconds <= 0 or args.poll_seconds <= 0:
        raise SystemExit("wait and poll durations must be positive")
    try:
        expected = _expected_missing(args.expected_missing)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    if set(expected) != set(bindings):
        raise SystemExit("--expected-missing must name exactly the requested bindings")
    if args.dry_run and args.apply:
        raise SystemExit("--dry-run and --apply are mutually exclusive")
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")

    if args.observed_ms is not None and not 0 < args.observed_ms <= time.time_ns() // 1_000_000:
        raise SystemExit("--observed-ms must be a positive past UTC epoch millisecond")
    approved_opens: dict[str, set[int]] = {}
    for value in args.expected_open:
        binding, separator, timestamp = value.partition("=")
        if not separator or binding not in bindings or not timestamp.isdigit() or int(timestamp) <= 0:
            raise SystemExit("--expected-open must name a requested binding and positive UTC epoch millisecond")
        opens = approved_opens.setdefault(binding, set())
        if int(timestamp) in opens:
            raise SystemExit("--expected-open may not repeat an open")
        opens.add(int(timestamp))
    if approved_opens and (set(approved_opens) != set(bindings) or any(len(approved_opens[key]) != expected[key] for key in bindings)):
        raise SystemExit("--expected-open must match every binding and approved missing count")

    edge = (
        build_readonly_repair_probe_from_environment()
        if args.dry_run
        else build_from_environment(
            client_id=f"qdl-v2-final-bar-repair-{os.getpid()}",
            repair_only=True,
        )
    )
    try:
        plans = tuple(
            edge.prepare_history_repair(
                binding_id, rows=args.rows,
                **({"observed_ms": args.observed_ms} if args.observed_ms is not None else {}),
            )
            for binding_id in bindings
        )
        for plan in plans:
            actual = len(plan.missing_envelopes)
            required = expected[plan.source.binding_id]
            if actual != required:
                raise RuntimeError(
                    "stable BAR repair missing-row count differs from approved scope "
                    f"binding={plan.source.binding_id} expected={required} actual={actual}"
                )
            if approved_opens:
                actual_opens = {edge._open_time_ms(plan.acquisition, item) for item in plan.missing_envelopes}
                if actual_opens != approved_opens[plan.source.binding_id]:
                    raise RuntimeError("stable BAR repair missing opens differ from approved scope")
        if not args.apply:
            print(json.dumps({
                "schema": "qdl.stable-final-bar-history-repair.v1",
                "status": "DRY_RUN",
                "production_mutations": 0,
                "repairs": [_summary(plan) for plan in plans],
            }, sort_keys=True))
            return 0

        published = {
            plan.source.binding_id: edge.apply_history_repair(
                plan,
                expected_missing_rows=expected[plan.source.binding_id],
            )
            for plan in plans
        }
        deadline = time.monotonic() + args.wait_seconds
        remaining = {plan.source.binding_id: -1 for plan in plans}
        while time.monotonic() < deadline:
            remaining = {
                plan.source.binding_id: edge.history_repair_remaining_rows(plan)
                for plan in plans
            }
            if all(value == 0 for value in remaining.values()):
                print(json.dumps({
                    "schema": "qdl.stable-final-bar-history-repair.v1",
                    "status": "CONVERGED",
                    "production_mutations": sum(published.values()),
                    "repairs": [
                        _summary(
                            plan,
                            published_rows=published[plan.source.binding_id],
                            remaining_rows=remaining[plan.source.binding_id],
                        )
                        for plan in plans
                    ],
                }, sort_keys=True))
                return 0
            time.sleep(args.poll_seconds)
        raise RuntimeError(
            "stable BAR repair did not converge before deadline remaining="
            + json.dumps(remaining, sort_keys=True)
        )
    finally:
        edge.stop()


if __name__ == "__main__":
    raise SystemExit(main())

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

from qdl.runtime.stable_bar_edge import build_from_environment


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
    parser.add_argument("--expected-missing", action="append", required=True)
    parser.add_argument("--wait-seconds", type=float, default=180.0)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
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
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f"--apply requires --confirm {CONFIRM}")

    edge = build_from_environment(
        client_id=f"qdl-v2-final-bar-repair-{os.getpid()}",
        repair_only=True,
    )
    try:
        plans = tuple(
            edge.prepare_history_repair(binding_id, rows=args.rows)
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

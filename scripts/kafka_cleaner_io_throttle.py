#!/usr/bin/env python3
"""Cluster-default Kafka log-cleaner I/O throttle for the stable broker (P18.4 G4 follow-up).

Purpose: set, verify or roll back the ONE semantics-free broker knob that bounds log-cleaner burst
cost, ``log.cleaner.io.max.bytes.per.second`` as a cluster-wide dynamic default (no broker restart),
after checking that compaction still keeps up at the requested rate.

Boundary:

* only that key, only ``--entity-type brokers --entity-default``; never a topic config (in particular
  never ``delete.retention.ms``, which is the KN tombstone-lifetime contract in
  ``config/v2/kn-v220-candidate-budget.json``), never ``md.canonical.v2``;
* dry-run by default: prints the exact describe / alter / verify / rollback argv and the keep-up
  arithmetic, runs nothing; ``--apply`` refuses while a foreign host-measurement lock is active,
  refuses a rate below the keep-up floor, then runs describe (before), alter, describe (after) and
  checks the value; ``--rollback-to`` restores the value read before (``--delete-config`` when the
  key had no dynamic default);
* the Kafka CLI runs in a throwaway container equal to compose ``stable_admin`` through
  ``scripts/kn_state_topics_packet.AdminContainerRunner`` (same image digest, network
  ``stable_internal``, read-only, cert dir mounted ro, ``--command-config admin.properties``);
  key material is never read here.

Keep-up arithmetic (measured 2026-10-15 from log-cleaner.log on all three brokers, 54 md.latest.v2
passes in 28.8 h): the throttle counts bytes read plus written, and a pass "processes" about
``PROCESSED_PER_THROTTLE_BYTE`` = 0.49 of the throttled bytes (1.97 MB/s processed at 4 MiB/s).
Compaction keeps up when the processed demand (``--demand-mb-s``, default md.latest.v2 0.89 MB/s +
internal topics 0.13 MB/s per broker) stays below ``MAX_DUTY`` of the processed capacity.

Exit codes: 0 OK (dry-run printed / applied and verified), 1 verify mismatch, 4 refused, 5 command error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import re
import sys
from typing import Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kn_state_topics_packet import AdminContainerRunner, CommandResult  # noqa: E402

KEY = "log.cleaner.io.max.bytes.per.second"
LOCK = Path.home() / ".local/state/host-measurement-window.lock"
PROCESSED_PER_THROTTLE_BYTE = 0.49
DEFAULT_DEMAND_MB_S = 0.89 + 0.13
MAX_DUTY = 0.8
MIN_RATE = 1 << 20          # 1 MiB/s: below this the cleaner cannot keep up at any measured load
MAX_RATE = 64 << 20         # 64 MiB/s: above this the knob no longer bounds a burst


class Refused(Exception):
    pass


def describe_cmd() -> list[str]:
    return ["kafka-configs.sh", "--describe", "--entity-type", "brokers", "--entity-default"]


def alter_cmd(rate: int) -> list[str]:
    validate_rate(rate)
    return ["kafka-configs.sh", "--alter", "--entity-type", "brokers", "--entity-default",
            "--add-config", f"{KEY}={rate}"]


def rollback_cmd(previous: int | None) -> list[str]:
    """Restore the value read before the change; no dynamic default before -> delete it."""
    if previous is None:
        return ["kafka-configs.sh", "--alter", "--entity-type", "brokers", "--entity-default",
                "--delete-config", KEY]
    return alter_cmd(previous)


def validate_rate(rate: int) -> None:
    if not isinstance(rate, int) or isinstance(rate, bool) or not MIN_RATE <= rate <= MAX_RATE:
        raise Refused(f"{KEY} must be an integer in [{MIN_RATE}, {MAX_RATE}] bytes/s, got {rate!r}")


def keep_up(rate: int, demand_mb_s: float = DEFAULT_DEMAND_MB_S) -> dict:
    """Processed capacity at ``rate`` and the duty cycle the measured demand needs."""
    capacity = rate * PROCESSED_PER_THROTTLE_BYTE / 1e6
    duty = demand_mb_s / capacity
    return {"rate_bytes_s": rate, "processed_capacity_mb_s": round(capacity, 3),
            "demand_mb_s": demand_mb_s, "duty": round(duty, 3), "keeps_up": duty <= MAX_DUTY}


_VALUE = re.compile(rf"^\s*{re.escape(KEY)}=(\S+) .*DYNAMIC_DEFAULT_BROKER_CONFIG:{re.escape(KEY)}=(\d+)", re.M)


def parse_dynamic_default(stdout: str) -> int | None:
    """The cluster dynamic default of KEY from ``--describe --entity-default`` output, or None."""
    match = _VALUE.search(stdout)
    return int(match.group(2)) if match else None


def foreign_lock(now: dt.datetime | None = None) -> dict | None:
    try:
        body = json.loads(LOCK.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return {"unparseable": True}
    now = now or dt.datetime.now(dt.timezone.utc)
    try:
        return body if dt.datetime.fromisoformat(body["until_utc"]) > now else None
    except (KeyError, ValueError):
        return {"unparseable": True}


def plan(rate: int | None, rollback_to: int | None, delete: bool, demand: float) -> dict:
    out: dict = {"describe": describe_cmd()}
    if rate is not None:
        out["alter"] = alter_cmd(rate)
        out["keep_up"] = keep_up(rate, demand)
        out["rollback"] = "the alter of the value read by describe before the change (or --delete-config if none)"
    if rollback_to is not None or delete:
        out["alter"] = rollback_cmd(None if delete else rollback_to)
    return out


def run(runner, command: Sequence[str]) -> CommandResult:
    result = runner.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{command[1]} failed rc={result.returncode}: {result.stderr.strip()[-400:]}")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--rate", type=int, help=f"new {KEY} (bytes/s)")
    group.add_argument("--rollback-to", type=int, help="restore this previous value")
    group.add_argument("--rollback-delete", action="store_true", help="remove the dynamic default")
    parser.add_argument("--demand-mb-s", type=float, default=DEFAULT_DEMAND_MB_S)
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); required with --apply")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.rollback_to is not None:
            validate_rate(args.rollback_to)
        steps = plan(args.rate, args.rollback_to, args.rollback_delete, args.demand_mb_s)
        if args.rate is not None and not steps["keep_up"]["keeps_up"]:
            raise Refused(f"compaction would not keep up: {steps['keep_up']}")
        if not args.apply:
            runner = AdminContainerRunner(args.cert_dir or Path("${QDL_STABLE_CERT_DIR}"))
            steps["docker_argv"] = {name: runner.argv(steps[name]) for name in ("describe", "alter") if name in steps}
            if args.rate is not None:
                steps["docker_argv"]["rollback_if_before_was_4194304"] = runner.argv(rollback_cmd(4194304))
                steps["docker_argv"]["rollback_if_no_dynamic_default"] = runner.argv(rollback_cmd(None))
            print(json.dumps({"mode": "dry-run", **steps}, indent=1))
            return 0
        if "alter" not in steps:
            raise Refused("--apply needs --rate, --rollback-to or --rollback-delete")
        if args.cert_dir is None:
            raise Refused("--apply needs --cert-dir")
        lock = foreign_lock()
        if lock is not None:
            raise Refused(f"host-measurement lock active: {lock}")
        runner = AdminContainerRunner(args.cert_dir)
        runner.preflight()
        before = parse_dynamic_default(run(runner, steps["describe"]).stdout)
        run(runner, steps["alter"])
        after = parse_dynamic_default(run(runner, steps["describe"]).stdout)
        expected = None if args.rollback_delete else (args.rate if args.rate is not None else args.rollback_to)
        report = {"mode": "applied", "before": before, "after": after, "expected": expected,
                  "rollback": rollback_cmd(before), "at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
        print(json.dumps(report, indent=1))
        return 0 if after == expected else 1
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except RuntimeError as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""md.latest.v2 transaction-marker lifetime: the ONE dynamic topic config change of decision A' (P18.4 G4).

Purpose: move ``md.latest.v2`` ``delete.retention.ms`` from the bars tombstone lifetime
(604,800,000 ms, the pre-A' value) to the marker lifetime frozen in
``config/v2/kn-v220-candidate-budget.json`` ``retention.state_topics["md.latest.v2"]`` (21,600,000 ms),
verify it, or roll it back. md.latest.v2 carries no tombstones (budget
``retention.state_topic_tombstones``, Rust guard ``md_latest_writer_never_emits_a_tombstone``), so on
this topic the setting only bounds how long the log cleaner keeps transaction markers; the cleaner I/O
of the old marker-only segments goes away once they pass the shorter horizon.

Boundary:

* exactly one topic (``md.latest.v2``) and one key (``delete.retention.ms``); the only values are the
  budget target and the rollback value (= ``tombstone_lifetime_ms``); never ``md.bars.v2`` (whose
  tombstone lifetime is a correctness contract), never ``md.canonical.v2``, never a broker config;
* dry-run by default: prints the exact describe / alter / rollback argv and the preconditions, runs
  nothing; ``--apply`` refuses while a foreign host-measurement lock is active, refuses unless the budget
  still declares the no-tombstone invariant and the D21 margin (marker lifetime >= margin x one replay
  pass of the largest partition), then runs describe (before), checks every other override of the topic
  equals the budget plan (``kn_state_topics_packet.planned_topic_configs``) and the current value is one
  of the two allowed values, alters, describes (after) and checks the value and that no other override
  moved; already at the requested value -> no alter (idempotent);
* ``--rollback-to 604800000`` restores the pre-A' value through the same path;
* the Kafka CLI runs in a throwaway container equal to compose ``stable_admin`` through
  ``scripts/kn_state_topics_packet.AdminContainerRunner`` (same image digest, network
  ``stable_internal``, read-only, cert dir mounted ro, ``--command-config admin.properties``); key
  material is never read here. A dynamic topic config change needs no broker or projector restart.

Exit codes: 0 OK (dry-run printed / applied and verified / already at the value), 1 verify mismatch,
4 refused, 5 command error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kn_state_topics_packet import (  # noqa: E402
    BUDGET,
    AdminContainerRunner,
    CommandResult,
    parse_topic_overrides,
    planned_topic_configs,
)

TOPIC = "md.latest.v2"
KEY = "delete.retention.ms"
LOCK = Path.home() / ".local/state/host-measurement-window.lock"


class Refused(Exception):
    pass


def contract(budget_path: Path = BUDGET) -> dict[str, Any]:
    """Target, rollback value and the A' preconditions, all read from the budget."""
    retention = json.loads(budget_path.read_text(encoding="utf-8"))["retention"]
    spec = retention["state_topics"][TOPIC]
    block = retention.get("state_topic_tombstones")
    if not isinstance(block, Mapping) or block.get("schema") != "qdl.kn.v220.state-topic-tombstones.v1":
        raise Refused("budget lacks retention.state_topic_tombstones v1 (the A' invariant)")
    latest = block["topics"][TOPIC]
    if latest.get("carries_tombstones") is not False:
        raise Refused(f"budget no longer declares that {TOPIC} carries no tombstones")
    if TOPIC in block.get("tombstone_lifetime_ms_applies_to", []):
        raise Refused(f"budget applies the tombstone lifetime to {TOPIC}")
    target = int(spec[KEY])
    rollback = int(retention["tombstone_lifetime_ms"])
    bound_ms = int(latest["d21_replay_pass_upper_bound_s"]) * 1000
    margin = int(latest["d21_min_margin"])
    if target >= rollback:
        raise Refused(f"budget {TOPIC} {KEY}={target} is not below the tombstone lifetime {rollback}")
    if target < margin * bound_ms:
        raise Refused(f"D21: {KEY}={target} < {margin} x replay pass {bound_ms} ms")
    return {"target": target, "rollback": rollback, "d21_replay_pass_upper_bound_ms": bound_ms,
            "d21_min_margin": margin, "planned_overrides": planned_topic_configs(spec)}


def describe_cmd() -> list[str]:
    # Same read form as kn_state_topics_packet._Session.read (topic overrides only).
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", TOPIC, "--describe"]


def alter_cmd(value: int, allowed: Sequence[int]) -> list[str]:
    if not isinstance(value, int) or isinstance(value, bool) or value not in allowed:
        raise Refused(f"{KEY} on {TOPIC} may only be one of {sorted(allowed)}, got {value!r}")
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", TOPIC, "--alter",
            "--add-config", f"{KEY}={value}"]


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


def check_before(overrides: Mapping[str, str], terms: Mapping[str, Any]) -> int:
    """Refuse unless the topic is the budget plan except for KEY, and KEY is an allowed value."""
    planned = dict(terms["planned_overrides"])
    others_now = {k: v for k, v in overrides.items() if k != KEY}
    others_planned = {k: v for k, v in planned.items() if k != KEY}
    if others_now != others_planned:
        drift = {k: {"planned": others_planned.get(k), "actual": others_now.get(k)}
                 for k in sorted(set(others_now) | set(others_planned))
                 if others_now.get(k) != others_planned.get(k)}
        raise Refused(f"{TOPIC} overrides drifted from the budget plan: {drift}")
    if KEY not in overrides:
        raise Refused(f"{TOPIC} has no {KEY} override (expected {terms['rollback']} or {terms['target']})")
    try:
        current = int(overrides[KEY])
    except ValueError:
        raise Refused(f"{TOPIC} {KEY}={overrides[KEY]!r} is not an integer") from None
    if current not in (terms["target"], terms["rollback"]):
        raise Refused(f"{TOPIC} {KEY}={current} is neither {terms['rollback']} nor {terms['target']}")
    return current


def run(runner, command: Sequence[str]) -> CommandResult:
    result = runner.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{command[0]} {command[5]} failed rc={result.returncode}: {result.stderr.strip()[-400:]}")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rollback-to", type=int, help="restore this value (only the tombstone lifetime)")
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); required with --apply")
    parser.add_argument("--budget", type=Path, default=BUDGET)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        terms = contract(args.budget)
        allowed = (terms["target"], terms["rollback"])
        if args.rollback_to is not None and args.rollback_to != terms["rollback"]:
            raise Refused(f"--rollback-to must be the pre-A' value {terms['rollback']}")
        wanted = terms["target"] if args.rollback_to is None else args.rollback_to
        steps = {"topic": TOPIC, "key": KEY, "requested": wanted, "contract": terms,
                 "describe": describe_cmd(), "alter": alter_cmd(wanted, allowed),
                 "rollback": alter_cmd(terms["rollback"] if wanted == terms["target"] else terms["target"],
                                       allowed)}
        if not args.apply:
            runner = AdminContainerRunner(args.cert_dir or Path("${QDL_STABLE_CERT_DIR}"))
            steps["docker_argv"] = {name: runner.argv(steps[name]) for name in ("describe", "alter", "rollback")}
            print(json.dumps({"mode": "dry-run", **steps}, indent=1))
            return 0
        if args.cert_dir is None:
            raise Refused("--apply needs --cert-dir")
        lock = foreign_lock()
        if lock is not None:
            raise Refused(f"host-measurement lock active: {lock}")
        runner = AdminContainerRunner(args.cert_dir)
        runner.preflight()
        before_overrides = parse_topic_overrides(run(runner, steps["describe"]).stdout, TOPIC)
        before = check_before(before_overrides, terms)
        altered = before != wanted
        if altered:
            run(runner, steps["alter"])
        after_overrides = parse_topic_overrides(run(runner, steps["describe"]).stdout, TOPIC)
        moved = sorted(k for k in set(before_overrides) | set(after_overrides)
                       if k != KEY and before_overrides.get(k) != after_overrides.get(k))
        after = after_overrides.get(KEY)
        ok = after == str(wanted) and not moved
        report = {"mode": "applied" if altered else "already_at_value", "topic": TOPIC, "key": KEY,
                  "before": before, "after": after, "expected": wanted, "other_overrides_moved": moved,
                  "rollback": alter_cmd(before, allowed),
                  "at_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
        print(json.dumps(report, indent=1))
        return 0 if ok else 1
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except RuntimeError as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

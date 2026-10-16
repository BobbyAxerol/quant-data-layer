#!/usr/bin/env python3
"""Coordinator-state compaction of the two Kafka internal topics: decision C of P18.4 G12 (owner, 2026-10-16).

Purpose: bound the bytes/records a new group or transaction coordinator must replay when its partition of
``__consumer_offsets`` / ``__transaction_state`` changes leader (auto leader rebalance, broker restart,
controlled election). The realtime core's transactional commit (``send_offsets_to_transaction`` +
``commit_transaction``, pipeline depth 1) waits for that load, so every feed stalls for its duration
(G12 broker-restart RCA, 32d2cf6). Kafka 4.2 facts this tool relies on (source read at tag 4.2.0):

* ``CoordinatorLoaderImpl.doLoad`` / ``TransactionStateManager.loadTransactionMetadata`` read the WHOLE
  partition from the log start to the log end, control batches included (``numRecords++`` per marker);
* a COMMIT marker in ``__consumer_offsets`` is discardable only once the cleaner has already removed every
  data batch of its transaction (``CleanedTransactionMetadata.onControlBatchRead``); the pass that finds it
  so stamps ``deleteHorizonMs = now + delete.retention.ms`` (``MemoryRecords.filterTo``), a later pass
  removes it (``Cleaner`` ``checkBatchRetention``). ``delete.retention.ms`` therefore bounds the markers'
  (and tombstones') lifetime; it never removes a live offset (compaction keeps the latest value of a key;
  committed offsets expire only through ``offsets.retention.minutes``, a coordinator-side rule);
* the active segment is never cleaned (``LogCleanerManager.cleanableOffsets``); ``segment.ms`` rolls it so
  its records become cleanable. ``__transaction_state`` holds no markers and, between expirations, no
  tombstones: its replay size is the not-yet-cleaned head, bounded by ``segment.ms`` (not by
  ``delete.retention.ms``).

Boundary:

* ``PLAN`` is PROPOSED until measured (``PLAN_STATUS``); a production ``--apply`` refuses a PROPOSED plan;
* exactly two topics and the keys of ``PLAN`` (``delete.retention.ms``, ``segment.ms``); each key is either
  ABSENT (the pre-C state: the broker default applies - ``log.cleaner.delete.retention.ms`` 86,400,000 and
  ``log.roll.ms`` 604,800,000) or the ``PLAN`` target; no other value can be written;
* dry-run by default: prints describe / alter / rollback argv, runs nothing. ``--apply`` refuses while a
  foreign host-measurement lock is active, describes each topic, refuses when any other override differs from
  ``FROZEN_OTHERS`` (production snapshot 2026-10-16) or a planned key holds a value that is neither absent nor
  the target - for every selected topic before the first alter - then alters (one ``--add-config`` per
  topic, all keys at once), describes again and verifies the
  planned keys and that no other override moved; already at the target -> no alter (idempotent);
* ``--rollback`` restores the pre-C state through the same path (``--delete-config`` of the planned keys);
* ``--topic`` limits the run to one topic (staged apply); the order is ``__transaction_state`` first (no
  correctness coupling), then ``__consumer_offsets``;
* the Kafka CLI runs in a throwaway container equal to compose ``stable_admin``
  (``scripts/kn_state_topics_packet.AdminContainerRunner``, network ``stable_internal``, read-only, cert dir
  mounted ro, ``--command-config admin.properties``); ``--isolated-container`` + ``--bootstrap`` drive a
  disposable plaintext broker instead (``DockerExecRunner``, refuses a production compose container). Key
  material is never read. A dynamic topic config needs no broker restart; the effect arrives with the next
  segment roll and cleaner passes (markers already stamped keep their old horizon).

Exit codes: 0 OK (dry-run printed / applied and verified / already at the target), 1 verify mismatch,
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
from scripts.kafka_md_latest_marker_retention import foreign_lock  # noqa: E402
from scripts.kn_state_topics_packet import (  # noqa: E402
    AdminContainerRunner,
    CommandResult,
    DockerExecRunner,
    Refused as PacketRefused,
    parse_topic_overrides,
)

TOPICS = ("__transaction_state", "__consumer_offsets")
# PROPOSED targets (Codex K1 2026-10-16): not measured yet. ``PLAN_STATUS`` becomes "MEASURED" (with
# ``PLAN_EVIDENCE``) only from the aged-state comparison of scripts/p184_g12_coordinator_load.py; ``--apply`` refuses
# a PROPOSED plan. Values are strings, as kafka-configs prints them.
PLAN_STATUS = "PROPOSED"
PLAN_EVIDENCE: str | None = None
PLAN: dict[str, dict[str, str]] = {
    "__consumer_offsets": {"delete.retention.ms": "3600000", "segment.ms": "600000"},
    "__transaction_state": {"segment.ms": "600000"},
}
# Production overrides of the two topics on 2026-10-16 (kafka-topics --describe, leader-rebalance scratch);
# anything else means the topic changed since this tool was reviewed.
_COMMON = {"min.insync.replicas": "2", "cleanup.policy": "compact", "segment.bytes": "104857600",
           "retention.ms": "86400000", "unclean.leader.election.enable": "false"}
FROZEN_OTHERS: dict[str, dict[str, str]] = {
    "__consumer_offsets": {"compression.type": "producer", **_COMMON},
    "__transaction_state": {"compression.type": "uncompressed", **_COMMON},
}
ABSENT = None


class Refused(Exception):
    pass


def describe_cmd(topic: str) -> list[str]:
    _known(topic)
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--describe"]


def alter_cmd(topic: str) -> list[str]:
    _known(topic)
    pairs = ",".join(f"{key}={value}" for key, value in sorted(PLAN[topic].items()))
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--alter",
            "--add-config", pairs]


def rollback_cmd(topic: str) -> list[str]:
    _known(topic)
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--alter",
            "--delete-config", ",".join(sorted(PLAN[topic]))]


def _known(topic: str) -> None:
    if topic not in TOPICS:
        raise Refused(f"topic must be one of {TOPICS}, got {topic!r}")


def wanted_state(topic: str, rollback: bool) -> dict[str, str | None]:
    return {key: ABSENT if rollback else value for key, value in PLAN[topic].items()}


def check_before(topic: str, overrides: Mapping[str, str]) -> dict[str, str | None]:
    """Refuse unless the non-planned overrides equal the frozen snapshot and each planned key is absent/target."""
    others = {k: v for k, v in overrides.items() if k not in PLAN[topic]}
    frozen = FROZEN_OTHERS[topic]
    if others != frozen:
        drift = {k: {"frozen": frozen.get(k), "actual": others.get(k)}
                 for k in sorted(set(others) | set(frozen)) if others.get(k) != frozen.get(k)}
        raise Refused(f"{topic} overrides drifted from the frozen snapshot: {drift}")
    current = {key: overrides.get(key) for key in PLAN[topic]}
    for key, value in current.items():
        if value not in (ABSENT, PLAN[topic][key]):
            raise Refused(f"{topic} {key}={value!r} is neither absent nor the target {PLAN[topic][key]}")
    return current


def run(runner, command: Sequence[str]) -> CommandResult:
    result = runner.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{command[0]} {command[4]} {command[5]} failed rc={result.returncode}: "
                           f"{result.stderr.strip()[-400:]}")
    return result


def apply_topic(runner, topic: str, rollback: bool) -> dict[str, Any]:
    before_all = parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic)
    before = check_before(topic, before_all)
    wanted = wanted_state(topic, rollback)
    altered = before != wanted
    if altered:
        run(runner, rollback_cmd(topic) if rollback else alter_cmd(topic))
    after_all = parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic)
    after = {key: after_all.get(key) for key in PLAN[topic]}
    moved = sorted(k for k in set(before_all) | set(after_all)
                   if k not in PLAN[topic] and before_all.get(k) != after_all.get(k))
    return {"topic": topic, "mode": ("rolled_back" if rollback else "applied") if altered else "already_at_state",
            "before": before, "after": after, "expected": wanted, "other_overrides_moved": moved,
            "ok": after == wanted and not moved,
            "undo": rollback_cmd(topic) if not rollback else alter_cmd(topic)}


def selected(topic: str | None) -> tuple[str, ...]:
    if topic is None:
        return TOPICS
    _known(topic)
    return (topic,)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--topic", choices=TOPICS, help="only this topic (default: both, txn state first)")
    parser.add_argument("--rollback", action="store_true", help="restore the pre-C state (delete the overrides)")
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); required with --apply")
    parser.add_argument("--isolated-container", help="disposable plaintext broker container (tests/rehearsal)")
    parser.add_argument("--bootstrap", default="localhost:9092", help="with --isolated-container")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        topics = selected(args.topic)
        if not args.apply:
            runner = AdminContainerRunner(args.cert_dir or Path("${QDL_STABLE_CERT_DIR}"))
            steps = {topic: {"plan": PLAN[topic], "frozen_other_overrides": FROZEN_OTHERS[topic],
                             "describe": runner.argv(describe_cmd(topic)),
                             "apply": runner.argv(alter_cmd(topic)),
                             "rollback": runner.argv(rollback_cmd(topic))} for topic in topics}
            print(json.dumps({"mode": "dry-run", "plan_status": PLAN_STATUS, "plan_evidence": PLAN_EVIDENCE,
                              "requested": "rollback" if args.rollback else "apply",
                              "topics": steps}, indent=1))
            return 0
        lock = foreign_lock()
        if lock is not None:
            raise Refused(f"host-measurement lock active: {lock}")
        if PLAN_STATUS != "MEASURED" and not args.isolated_container:
            raise Refused(f"PLAN is {PLAN_STATUS}: production apply needs the aged-state measurement (PLAN_EVIDENCE)")
        if args.isolated_container:
            runner = DockerExecRunner(args.isolated_container, args.bootstrap)
        elif args.cert_dir is None:
            raise Refused("--apply needs --cert-dir (or --isolated-container)")
        else:
            runner = AdminContainerRunner(args.cert_dir)
        runner.preflight()
        for topic in topics:  # every selected topic is checked before the first alter
            check_before(topic, parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic))
        reports = []
        for topic in topics:
            report = apply_topic(runner, topic, args.rollback)
            reports.append(report)
            if not report["ok"]:
                break  # never touch the next topic after a mismatch
        body = {"at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "runner": runner.describe(),
                "topics": reports}
        print(json.dumps(body, indent=1))
        return 0 if all(r["ok"] for r in reports) and len(reports) == len(topics) else 1
    except (Refused, PacketRefused) as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except RuntimeError as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

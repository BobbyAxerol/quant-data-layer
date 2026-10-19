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
* ``TEMP_OVERRIDES`` (owner decision 2026-10-16): a temporary ``min.cleanable.dirty.ratio`` 0.05 on
  ``__consumer_offsets`` only, so the cleaner reaches the already-stamped marker backlog sooner. It is separate from
  ``PLAN``: ``--apply-temp`` / ``--remove-temp`` (dry-run unless ``--apply``) add or delete exactly that key with the
  same refusals (frozen others, planned keys absent/target, the temp key absent/its target); while present at its
  target it is not drift for ``--apply``/``--rollback``/``--observe`` (any other value is). ``--observe`` reports it
  with the size of ``TEMP_WATCH_PARTITION`` and ``remove_recommended`` once every ASSIGNED replica (``kafka-topics
  --describe``) has an error-free, non-negative size at or below ``TEMP_REMOVE_AT_BYTES`` (a missing / errored /
  future replica is ``UNKNOWN_HOLD``, never zero; Codex K1); the removal itself is the coordinator's declared step.
  Rollback = ``--remove-temp``;
* ``--lock-owner`` names the caller's own host-measurement lock (it does not block ``--apply``);
* ``--topic`` limits the run to one topic (staged apply); the order is ``__transaction_state`` first (no
  correctness coupling), then ``__consumer_offsets``;
* ``--observe`` (read-only, no alter): the effective overrides of both topics, the per-broker size and the
  start/end offsets of the coordinator partitions (default ``__consumer_offsets-43``, the core group's), so the
  convergence after an apply can be tracked; offsets are an offset span, not a record count (compaction leaves
  gaps) - the replayed record count is the broker's ``Finished loading`` line;
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
import re
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
# Targets from the aged-state measurement (scripts/p184_g12_coordinator_load.py, 2026-10-16; evidence
# /home/bobby/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/: fit-output.txt, harness/move*.jsonl,
# harness/c1-dump-agg.json). The first proposal (Codex K1: offsets delete.retention 3,600,000 / segment 600,000)
# projected a steady-state load max ~2.2 s unloaded; these values project max ~126k records, ~308 ms unloaded /
# ~540 ms loaded, ~49 h convergence from the 2026-10-16 state. ``PLAN_STATUS`` stays PROPOSED until the owner's
# approval is recorded here (with ``PLAN_EVIDENCE``); ``--apply`` refuses a plan that is not MEASURED.
# Values are strings, as kafka-configs prints them.
PLAN_STATUS = "MEASURED"
PLAN_EVIDENCE: str | None = ("owner decision 2026-10-16 via AskUserQuestion: 'B + C 10 phut'; aged-state measurement /home/bobby/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/ (fit-output.txt, harness/move*.jsonl, harness/c1-dump-agg.json; DL ee2c359); projected max ~308 ms unloaded / ~540 ms loaded after ~49 h convergence")
PLAN: dict[str, dict[str, str]] = {
    "__consumer_offsets": {"delete.retention.ms": "600000", "segment.ms": "60000"},
    "__transaction_state": {"segment.ms": "600000"},
}
# Coordinator partitions --observe reports by default: the realtime core group's (production 2026-10-16).
OBSERVE_PARTITIONS = ("__consumer_offsets:43",)
# Production dynamic overrides of the two topics: ``kafka-configs --describe`` through the admin container,
# 2026-10-16T07:20Z (the first snapshot, taken from a kafka-topics describe, listed retention.ms and, for
# __consumer_offsets, min.insync.replicas / unclean.leader.election.enable, which production does not carry as
# topic overrides). Anything else means the topic changed since this tool was reviewed.
FROZEN_OTHERS: dict[str, dict[str, str]] = {
    "__consumer_offsets": {"cleanup.policy": "compact", "compression.type": "producer", "segment.bytes": "104857600"},
    "__transaction_state": {"cleanup.policy": "compact", "compression.type": "uncompressed",
                            "min.insync.replicas": "2", "segment.bytes": "104857600",
                            "unclean.leader.election.enable": "false"},
}
ABSENT = None
# Temporary key (owner decision 2026-10-16, AskUserQuestion): dynamic, no restart; rollback = delete the override.
# Removal point: the watched coordinator partition back near the C steady state. 32 MiB ~= 3x the projected C
# steady-state maximum (126k records, fit-output.txt; on-disk ~80 B per marker batch and ~177 B per 2-record data
# batch, harness/c1-dump-agg.json -> ~11 MB) and ~0.4 M markers (~0.13 s at the fitted 0.334 us/marker).
TEMP_OVERRIDES: dict[str, dict[str, str]] = {"__consumer_offsets": {"min.cleanable.dirty.ratio": "0.05"}}
TEMP_EVIDENCE = ("owner decision 2026-10-16 via AskUserQuestion: temporary min.cleanable.dirty.ratio=0.05 on "
                 "__consumer_offsets only, remove once partition 43 has shrunk")
TEMP_WATCH_PARTITION = "__consumer_offsets-43"
TEMP_REMOVE_AT_BYTES = 32 * 1024 * 1024


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


def temp_keys(topic: str) -> dict[str, str]:
    return TEMP_OVERRIDES.get(topic, {})


def temp_alter_cmd(topic: str) -> list[str]:
    _known(topic)
    if not temp_keys(topic):
        raise Refused(f"{topic} has no temporary override")
    pairs = ",".join(f"{key}={value}" for key, value in sorted(temp_keys(topic).items()))
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--alter", "--add-config", pairs]


def temp_remove_cmd(topic: str) -> list[str]:
    _known(topic)
    if not temp_keys(topic):
        raise Refused(f"{topic} has no temporary override")
    return ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--alter",
            "--delete-config", ",".join(sorted(temp_keys(topic)))]


def split_overrides(topic: str, overrides: Mapping[str, str]) -> tuple[dict, dict]:
    """(others, temp): the temp key at its target (or absent) is reported apart and never counts as drift; a temp key
    holding any other value stays in ``others`` and therefore is drift."""
    temp = {key: overrides.get(key) for key in temp_keys(topic)}
    others = {k: v for k, v in overrides.items()
              if k not in PLAN[topic] and not (k in temp_keys(topic) and v == temp_keys(topic)[k])}
    return others, temp


def wanted_state(topic: str, rollback: bool) -> dict[str, str | None]:
    return {key: ABSENT if rollback else value for key, value in PLAN[topic].items()}


def check_before(topic: str, overrides: Mapping[str, str]) -> dict[str, str | None]:
    """Refuse unless the non-planned overrides equal the frozen snapshot and each planned key is absent/target."""
    others, _ = split_overrides(topic, overrides)
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


def apply_temp(runner, topic: str, remove: bool) -> dict[str, Any]:
    """Add (or delete) exactly the temporary key(s) of ``topic``; same refusals as the PLAN path."""
    before_all = parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic)
    check_before(topic, before_all)
    before = {key: before_all.get(key) for key in temp_keys(topic)}
    wanted = {key: ABSENT if remove else value for key, value in temp_keys(topic).items()}
    altered = before != wanted
    if altered:
        run(runner, temp_remove_cmd(topic) if remove else temp_alter_cmd(topic))
    after_all = parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic)
    after = {key: after_all.get(key) for key in temp_keys(topic)}
    moved = sorted(k for k in set(before_all) | set(after_all)
                   if k not in temp_keys(topic) and before_all.get(k) != after_all.get(k))
    return {"topic": topic, "temp": True, "evidence": TEMP_EVIDENCE,
            "mode": ("removed" if remove else "applied") if altered else "already_at_state",
            "before": before, "after": after, "expected": wanted, "other_overrides_moved": moved,
            "ok": after == wanted and not moved,
            "undo": temp_alter_cmd(topic) if remove else temp_remove_cmd(topic)}


def temp_removal(topics: Mapping[str, Any], parts: Mapping[str, Any],
                 assigned: Sequence[str] | None) -> dict[str, Any]:
    """When the temporary key can go: present + a complete, error-free, non-negative size observation of EVERY
    assigned replica of the watched partition, each <= TEMP_REMOVE_AT_BYTES (Codex K1, 2026-10-16).

    ``assigned``: the partition's replica broker ids from ``kafka-topics --describe`` (None: not read). A replica that
    is assigned but not observed, a log-dir error, a future (moving) replica or an invalid size is UNKNOWN -> HOLD,
    never zero; ``remove_recommended`` is then false whatever the other replicas show."""
    present = any(t.get("temp_state") == "PRESENT" for t in topics.values())
    part = parts.get(TEMP_WATCH_PARTITION) or {}
    sizes = part.get("size_bytes_by_broker") or {}
    problems = list(part.get("observation_problems") or [])
    expected = sorted(str(b) for b in (assigned or ()))
    if not expected:
        problems.append("assigned replicas of the watched partition not read")
    missing = [b for b in expected if b not in sizes]
    if missing:
        problems.append(f"assigned replicas without a size observation: {missing}")
    unexpected = sorted(b for b in sizes if b not in expected)
    if expected and unexpected:
        problems.append(f"size observed on brokers outside the assignment: {unexpected}")
    complete = not problems
    largest = max(sizes[b] for b in expected) if complete else None
    observed_max = max(sizes.values()) if sizes else None
    if not present:
        status = "TEMP_NOT_PRESENT"
    elif not complete:
        status = "UNKNOWN_HOLD"
    elif largest <= TEMP_REMOVE_AT_BYTES:
        status = "REMOVE_RECOMMENDED"
    else:
        status = "ABOVE_THRESHOLD_HOLD"
    return {"present": present, "watch_partition": TEMP_WATCH_PARTITION, "assigned_replicas": expected,
            "observed_replicas": sorted(sizes), "observation_complete": complete, "observation_problems": problems,
            "max_size_bytes": largest, "observed_max_size_bytes": observed_max,
            "threshold_bytes": TEMP_REMOVE_AT_BYTES, "status": status,
            "remove_recommended": status == "REMOVE_RECOMMENDED",
            "remove_command": "kafka_internal_topic_compaction.py --remove-temp --apply --cert-dir <cert>"}


def topic_describe_cmd(topic: str) -> list[str]:
    _known(topic)
    return ["kafka-topics.sh", "--describe", "--topic", topic]


_ASSIGNMENT = re.compile(r"^\s*Topic:\s*(\S+)\s+Partition:\s*(\d+)\s+Leader:\s*\S+\s+Replicas:\s*([\d,]+)\b")


def parse_assigned_replicas(text: str, partition: str) -> list[str]:
    """``kafka-topics --describe`` -> the replica broker ids of ``partition`` (``topic-N``); refuses when the
    partition row is absent or listed twice."""
    topic, _, number = partition.rpartition("-")
    rows = [m.group(3) for m in map(_ASSIGNMENT.match, text.splitlines())
            if m and m.group(1) == topic and m.group(2) == number]
    if len(rows) != 1:
        raise RuntimeError(f"kafka-topics printed {len(rows)} assignment rows for {partition}")
    replicas = [b for b in rows[0].split(",") if b]
    if not replicas or len(set(replicas)) != len(replicas):
        raise RuntimeError(f"invalid replica list for {partition}: {rows[0]!r}")
    return replicas


def log_dirs_cmd() -> list[str]:
    return ["kafka-log-dirs.sh", "--describe", "--topic-list", ",".join(TOPICS)]


def offsets_cmd(partitions: Sequence[str], time_arg: str) -> list[str]:
    return ["kafka-get-offsets.sh", "--topic-partitions", ",".join(partitions), "--time", time_arg]


def _checked(runner, command: Sequence[str]) -> str:
    result = runner.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{command[0]} failed rc={result.returncode}: {result.stderr.strip()[-400:]}")
    return result.stdout


def parse_log_dirs(text: str, wanted: Sequence[str]) -> dict[str, dict[str, Any]]:
    """``kafka-log-dirs --describe`` JSON line -> {partition: {"size_bytes_by_broker": {broker id: bytes},
    "observation_problems": [...]}} for the wanted partitions.

    Only a current (non-future) replica in an error-free log dir with an integer size >= 0 is a size; a log-dir
    error on a broker, a future replica, an invalid/negative size or a duplicate replica is recorded as a problem
    (Codex K1, 2026-10-16) and the replica is not given a size."""
    line = next((row for row in text.splitlines() if row.startswith("{")), None)
    if line is None:
        raise RuntimeError("kafka-log-dirs printed no JSON")
    out: dict[str, dict[str, Any]] = {name: {"size_bytes_by_broker": {}, "observation_problems": []}
                                      for name in wanted}
    for broker in json.loads(line)["brokers"]:
        node = str(broker["broker"])
        for log_dir in broker.get("logDirs") or []:
            if log_dir.get("error") not in (None, "", "none", "NONE"):
                for name in wanted:
                    out[name]["observation_problems"].append(
                        f"broker {node} log dir {log_dir.get('logDir')!r} error {str(log_dir['error'])[:120]!r}")
                continue
            for part in log_dir.get("partitions") or []:
                name = part.get("partition")
                if name not in out:
                    continue
                problems = out[name]["observation_problems"]
                if part.get("isFuture"):
                    problems.append(f"broker {node} holds a future replica of {name}")
                    continue
                size = part.get("size")
                if type(size) is not int or size < 0:
                    problems.append(f"broker {node} invalid size {size!r} for {name}")
                    continue
                if node in out[name]["size_bytes_by_broker"]:
                    problems.append(f"broker {node} reports {name} twice")
                    continue
                out[name]["size_bytes_by_broker"][node] = size
    return out


def parse_offsets(text: str) -> dict[str, int]:
    found = {}
    for row in text.split():
        bits = row.rsplit(":", 2)
        if len(bits) == 3 and bits[1].isdigit() and bits[2].lstrip("-").isdigit():
            found[f"{bits[0]}-{bits[1]}"] = int(bits[2])
    return found


def observe(runner, partitions: Sequence[str]) -> dict[str, Any]:
    """Read-only convergence view: overrides (planned keys vs PLAN, frozen others), sizes, offset span."""
    wanted = [p.replace(":", "-") for p in partitions]
    topics = {}
    for topic in TOPICS:
        overrides = parse_topic_overrides(_checked(runner, describe_cmd(topic)), topic)
        planned = {key: overrides.get(key) for key in PLAN[topic]}
        others, temp = split_overrides(topic, overrides)
        state = ("APPLIED" if planned == PLAN[topic] else
                 "ABSENT" if all(v is None for v in planned.values()) else "PARTIAL_OR_FOREIGN")
        frozen = FROZEN_OTHERS[topic]
        drift = {k: {"frozen": frozen.get(k), "actual": others.get(k)}
                 for k in sorted(set(others) | set(frozen)) if others.get(k) != frozen.get(k)}
        temp_state = ("NONE" if not temp else "PRESENT" if temp == temp_keys(topic) else
                      "ABSENT" if all(v is None for v in temp.values()) else "FOREIGN")
        topics[topic] = {"planned_keys": planned, "c_state": state, "temp_keys": temp, "temp_state": temp_state,
                         "other_overrides_match_frozen": not drift, "drift": drift}
    if TEMP_WATCH_PARTITION not in wanted:
        wanted.append(TEMP_WATCH_PARTITION)
        partitions = [*partitions, TEMP_WATCH_PARTITION.replace("-", ":")]
    sizes = parse_log_dirs(_checked(runner, log_dirs_cmd()), wanted)
    end = parse_offsets(_checked(runner, offsets_cmd(partitions, "-1")))
    start = parse_offsets(_checked(runner, offsets_cmd(partitions, "-2")))
    watch_topic = TEMP_WATCH_PARTITION.rpartition("-")[0]
    assigned = parse_assigned_replicas(_checked(runner, topic_describe_cmd(watch_topic)), TEMP_WATCH_PARTITION)
    parts = {name: {**sizes[name], "log_start_offset": start.get(name),
                    "log_end_offset": end.get(name),
                    "offset_span": (end[name] - start[name]) if name in end and name in start else None}
             for name in wanted}
    parts[TEMP_WATCH_PARTITION]["assigned_replicas"] = assigned
    return {"mode": "observe", "at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "runner": runner.describe(),
            "plan_status": PLAN_STATUS, "topics": topics, "partitions": parts,
            "temp_removal": temp_removal(topics, parts, assigned),
            "note": "offset_span is not a record count; replayed records = broker 'Finished loading' line"}


def selected(topic: str | None) -> tuple[str, ...]:
    if topic is None:
        return TOPICS
    _known(topic)
    return (topic,)


def run_temp(args: argparse.Namespace, remove: bool) -> int:
    topics = tuple(t for t in TOPICS if temp_keys(t))
    if not args.apply:
        runner = AdminContainerRunner(args.cert_dir or Path("${QDL_STABLE_CERT_DIR}"))
        print(json.dumps({"mode": "dry-run", "requested": "remove-temp" if remove else "apply-temp",
                          "evidence": TEMP_EVIDENCE, "remove_at_bytes": TEMP_REMOVE_AT_BYTES,
                          "watch_partition": TEMP_WATCH_PARTITION,
                          "topics": {t: {"temp": temp_keys(t), "describe": runner.argv(describe_cmd(t)),
                                         "apply": runner.argv(temp_alter_cmd(t)),
                                         "remove": runner.argv(temp_remove_cmd(t))} for t in topics}}, indent=1))
        return 0
    lock = foreign_lock(own_owner=args.lock_owner)
    if lock is not None:
        raise Refused(f"host-measurement lock active: {lock}")
    if args.isolated_container:
        runner = DockerExecRunner(args.isolated_container, args.bootstrap)
    elif args.cert_dir is None:
        raise Refused("--apply needs --cert-dir (or --isolated-container)")
    else:
        runner = AdminContainerRunner(args.cert_dir)
    runner.preflight()
    for topic in topics:
        check_before(topic, parse_topic_overrides(run(runner, describe_cmd(topic)).stdout, topic))
    reports = [apply_temp(runner, topic, remove) for topic in topics]
    print(json.dumps({"at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "runner": runner.describe(),
                      "topics": reports}, indent=1))
    return 0 if all(r["ok"] for r in reports) else 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--topic", choices=TOPICS, help="only this topic (default: both, txn state first)")
    parser.add_argument("--rollback", action="store_true", help="restore the pre-C state (delete the overrides)")
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); required with --apply")
    parser.add_argument("--isolated-container", help="disposable plaintext broker container (tests/rehearsal)")
    parser.add_argument("--bootstrap", default="localhost:9092", help="with --isolated-container")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--lock-owner", help="host-measurement lock owner of this window: a lock held by this owner does not block --apply")
    parser.add_argument("--observe", action="store_true", help="read-only overrides/sizes/offsets (no alter)")
    parser.add_argument("--apply-temp", action="store_true",
                        help="TEMP_OVERRIDES only: add the temporary key(s) (dry-run unless --apply)")
    parser.add_argument("--remove-temp", action="store_true",
                        help="TEMP_OVERRIDES only: delete the temporary key(s) (dry-run unless --apply)")
    parser.add_argument("--partition", action="append", metavar="TOPIC:N",
                        help=f"--observe partitions (default {', '.join(OBSERVE_PARTITIONS)})")
    args = parser.parse_args(argv)
    try:
        temp_mode = "apply" if args.apply_temp else "remove" if args.remove_temp else None
        if args.apply_temp and args.remove_temp:
            raise Refused("--apply-temp and --remove-temp are exclusive")
        if temp_mode and (args.observe or args.rollback or args.topic):
            raise Refused("--apply-temp/--remove-temp take no --observe/--rollback/--topic (TEMP_OVERRIDES names "
                          "the topic)")
        if args.observe:
            if args.apply or args.rollback:
                raise Refused("--observe is read-only: no --apply/--rollback")
            partitions = tuple(args.partition or OBSERVE_PARTITIONS)
            for item in partitions:
                name, _, number = item.partition(":")
                _known(name)
                if not number.isdigit():
                    raise Refused(f"--partition must be TOPIC:N, got {item!r}")
            if args.isolated_container:
                runner = DockerExecRunner(args.isolated_container, args.bootstrap)
            elif args.cert_dir is None:
                raise Refused("--observe needs --cert-dir (or --isolated-container)")
            else:
                runner = AdminContainerRunner(args.cert_dir)
            print(json.dumps(observe(runner, partitions), indent=1))
            return 0
        if temp_mode:
            return run_temp(args, temp_mode == "remove")
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
        lock = foreign_lock(own_owner=args.lock_owner)
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

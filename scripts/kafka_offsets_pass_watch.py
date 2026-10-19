#!/usr/bin/env python3
"""Read-only watch of a log-cleaner pass over one ``__consumer_offsets`` partition (P18.4 G12, 2026-10-16).

Purpose: let the operator of the temporary ``min.cleanable.dirty.ratio`` window
(``scripts/kafka_internal_topic_compaction.py --apply-temp/--remove-temp``) prove that the forced cleaner pass over
the core group's coordinator partition (default ``__consumer_offsets-43``) removed only expired transaction
markers: per replica, whether a pass started / finished and what it reported, the partition's size and segment
count, the cleaner and log-start checkpoints; for the cluster, every consumer group's committed offsets and state.
``--snapshot`` writes one JSON document; ``--compare BEFORE AFTER`` diffs two of them.

Sources (all read-only; nothing is written to Kafka, a volume or a container):

* cleaner log - the broker's ``LogCleaner`` logs to ``/opt/kafka/logs/log-cleaner.log[.YYYY-MM-DD-HH]`` inside the
  container, not to stdout (``docker logs`` carries no cleaner line); the hourly files of the window are streamed
  with ``docker cp <container>:<file> -`` (the archive API: no process starts in the broker);
* partition directory - ``stat`` of the segment files and the two checkpoint files through a throwaway
  ``docker run --rm --network none --read-only`` ``alpine:3.20`` container mounting the broker's data volume ``:ro``;
* groups - ``kafka-consumer-groups.sh --describe --all-groups --offsets`` and ``--state`` through the compose
  ``stable_admin`` path (``scripts/kn_state_topics_packet.AdminContainerRunner``; cert dir mounted ro, key material
  never read).

A snapshot starts containers, so it refuses while a host-measurement lock of another owner is active
(``--lock-owner`` names the caller's own window). ``--compare`` reads two files only.

Read errors (Codex K2, 2026-10-16): a required read that fails stops the snapshot (exit 5, nothing written) - the
partition-directory script runs under ``set -e`` and must print every section marker, a listing with no segment or an
unparseable line is an error, and a failed ``docker cp`` of the LIVE cleaner log is an error; only a rotated hourly
file that the daemon reports as absent (``Could not find the file``) is optional, and it is recorded per replica
(``cleaner_log_absent``). Snapshots carry ``read_contract`` (``READ_CONTRACT``); an older snapshot is not a valid
observation.

Compare reports three SEPARATE outcomes (none of them is permission for the next broker by itself):

* ``observation_valid``: both snapshots follow ``READ_CONTRACT``, the BEFORE replica set is non-empty and every
  replica is present after, each with at least one segment;
* ``offsets_preserved``: the BEFORE baseline holds at least one committed (group, topic, partition) and every one of
  them is still committed after with an offset that did not decrease (live commits may advance it; an idle group may
  legitimately keep its offset unchanged); group states and sizes are reported, not judged. ``ok`` is this offset
  safety on a valid observation - it does NOT mean the cleaner finished;
* ``cleaner_completed_all_replicas``: on a valid observation, every replica shows a pass over the partition that
  began after the BEFORE snapshot's baseline completion (``baseline_completed_utc``, after its committed
  offsets were captured; D3) and whose summary block (``cleaned log ... dirty section`` + ``End size``) followed.

Exit codes: 0 all three, 2 valid + offsets preserved but the cleaner pass not complete on every replica, 1 offset
violation or empty baseline, 6 observation not valid, 4 refused, 5 command error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from typing import Any, Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kafka_md_latest_marker_retention import foreign_lock  # noqa: E402
from scripts.kn_state_topics_packet import AdminContainerRunner, PRODUCTION_PROJECT  # noqa: E402

PARTITION = "__consumer_offsets-43"
BROKERS = (1, 2, 3)
READER_IMAGE = "alpine:3.20"
CLEANER_LOG = "/opt/kafka/logs/log-cleaner.log"
DATA_MOUNT = "/data"  # the broker's /var/lib/kafka/data (KAFKA_LOG_DIRS) mounted read-only
TIMEOUT = 120.0
READ_CONTRACT = "p184.pass-watch.read-errors.v2"
ABSENT_FILE = "Could not find the file"  # docker cp: the path does not exist in the container (daemon message)


class Refused(Exception):
    pass


def container(node: int) -> str:
    return f"{PRODUCTION_PROJECT}-kafka{node}-1"


def volume(node: int) -> str:
    return f"{PRODUCTION_PROJECT}_kafka{node}_data"


def _partition_name(partition: str) -> str:
    if not re.fullmatch(r"__consumer_offsets-\d+", partition):
        raise Refused(f"partition must be __consumer_offsets-N, got {partition!r}")
    return partition


# ------------------------------------------------------------------------------------------------ cleaner log

_TS = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(\d{3})\]")
_SIZE = re.compile(r"^\s*(Start|End) size: ([\d,.]+) MB \(([\d,]+) messages\)")


def _ts(line: str) -> str | None:
    match = _TS.match(line)
    if not match:
        return None
    return dt.datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(
        microsecond=int(match.group(2)) * 1000, tzinfo=dt.timezone.utc).isoformat()


def parse_cleaner_passes(text: str, partition: str) -> list[dict[str, Any]]:
    """Cleaner log text -> the passes over ``partition`` in log order.

    A pass opens on ``Beginning cleaning of log <p>`` and closes on the summary block
    ``Log cleaner thread N cleaned log <p> (dirty section = [a, b])`` with its ``Start size`` / ``End size`` lines;
    the summary's time is the ``[kafka-log-cleaner-thread-N]:`` header line just above it. Segment swaps are
    counted, and those that left a segment empty (``Swapping in cleaned segment LogSegment(..., size=0``)."""
    begin = f"Beginning cleaning of log {partition} "
    summary = re.compile(rf"cleaned log {re.escape(partition)} \(dirty section = \[(\d+), (\d+)\]\)")
    swap = f"in log Log(dir="
    passes: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    last_ts: str | None = None
    in_summary = False
    for line in text.splitlines():
        stamp = _ts(line)
        if stamp is not None:
            last_ts = stamp
            in_summary = False
        if begin in line:
            current = {"started_utc": stamp, "finished_utc": None, "dirty_section": None, "swaps": 0,
                       "emptied_segments": 0, "start_mb": None, "start_messages": None, "end_mb": None,
                       "end_messages": None}
            passes.append(current)
            continue
        if current is None:
            continue
        if "Swapping in cleaned segment" in line and swap in line and f"/{partition}," in line:
            current["swaps"] += 1
            if re.search(r"Swapping in cleaned segment LogSegment\(baseOffset=\d+, size=0,", line):
                current["emptied_segments"] += 1
            continue
        found = summary.search(line)
        if found:
            current["finished_utc"] = last_ts
            current["dirty_section"] = [int(found.group(1)), int(found.group(2))]
            in_summary = True
            continue
        sized = _SIZE.match(line)
        if sized and in_summary:
            key = sized.group(1).lower()
            current[f"{key}_mb"] = float(sized.group(2).replace(",", ""))
            current[f"{key}_messages"] = int(sized.group(3).replace(",", ""))
            if key == "end":
                current = None  # the pass is complete; a later Beginning opens the next one
                in_summary = False
    return passes


def cleaner_files(now: dt.datetime, hours: int) -> list[str]:
    """The rotated hourly files covering the last ``hours`` hours (oldest first), then the live file."""
    names = []
    for back in range(hours, 0, -1):
        hour = (now - dt.timedelta(hours=back)).astimezone(dt.timezone.utc)
        names.append(f"{CLEANER_LOG}.{hour:%Y-%m-%d-%H}")
    return names + [CLEANER_LOG]


def _untar_one(blob: bytes, name: str) -> str:
    with tarfile.open(fileobj=io.BytesIO(blob)) as archive:
        member = next((m for m in archive.getmembers() if m.isfile()), None)
        handle = archive.extractfile(member) if member is not None else None
        if handle is None:
            raise RuntimeError(f"docker cp of {name} returned no regular file")
        return handle.read().decode("utf-8", "replace")


def read_cleaner_log(node: int, now: dt.datetime, hours: int) -> tuple[str, list[str], list[str]]:
    """(text, files read, rotated files absent). Only a rotated hourly file the daemon reports as absent is skipped
    (an hour without a rotation: quiet cleaner or before the container started); any other failure, and any failure
    of the live file, raises."""
    texts, read, absent = [], [], []
    for name in cleaner_files(now, hours):
        done = subprocess.run(["docker", "cp", f"{container(node)}:{name}", "-"], capture_output=True,
                              timeout=TIMEOUT, check=False)
        if done.returncode != 0:
            err = (done.stderr or b"").decode("utf-8", "replace").strip()
            if name != CLEANER_LOG and ABSENT_FILE in err:
                absent.append(Path(name).name)
                continue
            raise RuntimeError(f"docker cp {container(node)}:{name} failed rc={done.returncode}: {err[-300:]}")
        texts.append(_untar_one(done.stdout, name))
        read.append(Path(name).name)
    return "\n".join(texts), read, absent


# ------------------------------------------------------------------------------------------------ partition dir


def dir_script(partition: str) -> str:
    """``set -e``: a failed cd / stat / cat ends the script non-zero (no later command can mask it); ``#end`` proves
    the last read completed."""
    return (f"set -e; cd {DATA_MOUNT}/{partition}; stat -c '%n %s %Y' *.log; echo '#cleaner'; "
            f"cat {DATA_MOUNT}/cleaner-offset-checkpoint; echo '#start'; cat {DATA_MOUNT}/log-start-offset-checkpoint; "
            "echo '#end'")


def dir_argv(node: int, partition: str) -> list[str]:
    return ["docker", "run", "--rm", "--network", "none", "--read-only", "--security-opt", "no-new-privileges:true",
            "-v", f"{volume(node)}:{DATA_MOUNT}:ro", "--entrypoint", "/bin/sh", READER_IMAGE, "-c",
            dir_script(_partition_name(partition))]


def _checkpoint(lines: Iterable[str], partition: str) -> int | None:
    """Kafka offset checkpoint: version ``0``, entry count, then ``topic partition offset`` rows; a malformed file
    raises (a partial read is not an absent entry)."""
    lines = [line for line in lines if line.strip()]
    if len(lines) < 2 or lines[0].strip() != "0" or not lines[1].strip().isdigit() \
            or int(lines[1]) != len(lines) - 2 or any(len(line.split()) != 3 for line in lines[2:]):
        raise RuntimeError("malformed offset checkpoint file")
    topic, _, number = partition.rpartition("-")
    for line in lines[2:]:
        bits = line.split()
        if len(bits) == 3 and bits[0] == topic and bits[1] == number:
            return int(bits[2])
    return None


def parse_partition_dir(text: str, partition: str) -> dict[str, Any]:
    """``dir_script`` output -> segments (base offset, bytes, mtime), totals and the two checkpoints. Every section
    marker must appear in order, every listing line must be a segment, and at least one segment must exist (a
    partition always has its active segment); otherwise the read failed and this raises."""
    section, segments, cleaner, start = "segments", [], [], []
    order = {"#cleaner": ("segments", "cleaner"), "#start": ("cleaner", "start"), "#end": ("start", "end")}
    for line in text.splitlines():
        if line in order:
            if section != order[line][0]:
                raise RuntimeError(f"partition dir read: marker {line} out of order")
            section = order[line][1]
            continue
        if section == "segments":
            match = re.fullmatch(r"(\d{20})\.log (\d+) (\d+)", line.strip())
            if not match:
                raise RuntimeError(f"partition dir read: unparseable listing line {line[:80]!r}")
            segments.append({"base_offset": int(match.group(1)), "bytes": int(match.group(2)),
                             "mtime": int(match.group(3))})
        elif section == "cleaner":
            cleaner.append(line)
        elif section == "start":
            start.append(line)
        elif line.strip():
            raise RuntimeError("partition dir read: output after #end")
    if section != "end":
        raise RuntimeError("partition dir read incomplete (no #end marker)")
    if not segments:
        raise RuntimeError(f"partition dir read: no segment of {partition}")
    segments.sort(key=lambda s: s["base_offset"])
    return {"segments": segments, "segment_count": len(segments), "size_bytes": sum(s["bytes"] for s in segments),
            "empty_segments": sum(1 for s in segments if s["bytes"] == 0),
            "cleaner_checkpoint": _checkpoint(cleaner, partition),
            "log_start_checkpoint": _checkpoint(start, partition)}


# ------------------------------------------------------------------------------------------------ groups

OFFSETS_CMD = ["kafka-consumer-groups.sh", "--describe", "--all-groups", "--offsets"]
STATE_CMD = ["kafka-consumer-groups.sh", "--describe", "--all-groups", "--state"]


def parse_group_offsets(text: str) -> dict[str, dict[str, int | None]]:
    """``--describe --all-groups --offsets`` -> {group: {"topic:partition": committed offset or None}}.

    Columns (Kafka 4.2 ``ConsumerGroupCommand``): GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...;
    ``-`` is a missing value. Header and blank lines are skipped."""
    groups: dict[str, dict[str, int | None]] = {}
    for line in text.splitlines():
        bits = line.split()
        if len(bits) < 6 or bits[0] == "GROUP" or not bits[2].isdigit():
            continue
        group, topic, part, current = bits[0], bits[1], bits[2], bits[3]
        if topic == "-":
            groups.setdefault(group, {})
            continue
        groups.setdefault(group, {})[f"{topic}:{part}"] = int(current) if current.lstrip("-").isdigit() else None
    return groups


def parse_group_states(text: str) -> dict[str, dict[str, Any]]:
    """``--describe --all-groups --state`` -> {group: {state, members, coordinator}}.

    Row: GROUP ``host:port  (id)`` ASSIGNMENT-STRATEGY STATE #MEMBERS; the coordinator column holds a space, so the
    row is read from both ends."""
    states: dict[str, dict[str, Any]] = {}
    for line in text.splitlines():
        bits = line.split()
        if len(bits) < 5 or bits[0] == "GROUP" or not bits[-1].isdigit():
            continue
        coordinator = next((b.strip("()") for b in bits if re.fullmatch(r"\(\d+\)", b)), None)
        states[bits[0]] = {"state": bits[-2], "members": int(bits[-1]), "coordinator": coordinator}
    return states


# ------------------------------------------------------------------------------------------------ snapshot


def _run(argv: Sequence[str]) -> str:
    try:
        done = subprocess.run(list(argv), capture_output=True, text=True, timeout=TIMEOUT, check=False)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{argv[0]} {argv[1]} timed out") from None
    if done.returncode != 0:
        raise RuntimeError(f"{' '.join(argv[:3])} failed rc={done.returncode}: {done.stderr.strip()[-300:]}")
    return done.stdout


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def snapshot(admin: AdminContainerRunner, partition: str, hours: int, brokers: Sequence[int] = BROKERS,
             now: dt.datetime | None = None, clock=None) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    replicas = {}
    for node in brokers:
        text, files, absent = read_cleaner_log(node, now, hours)
        body = parse_partition_dir(_run(dir_argv(node, partition)), partition)
        body.update({"container": container(node), "volume": volume(node), "cleaner_log_files": files,
                     "cleaner_log_absent": absent, "passes": parse_cleaner_passes(text, partition)})
        replicas[str(node)] = body
    offsets = admin.run(OFFSETS_CMD)
    if offsets.returncode != 0:
        raise RuntimeError(f"consumer-groups --offsets failed rc={offsets.returncode}: {offsets.stderr[-300:]}")
    offsets_utc = (clock or _utcnow)()
    states = admin.run(STATE_CMD)
    if states.returncode != 0:
        raise RuntimeError(f"consumer-groups --state failed rc={states.returncode}: {states.stderr[-300:]}")
    # D3 (Codex follow-up 2026-10-16): the committed offsets are captured LAST; a pass that began while the baseline
    # was being collected must not be credited against it, so compare() counts passes after baseline_completed_utc.
    return {"kind": "kafka_offsets_pass_watch.snapshot", "read_contract": READ_CONTRACT, "at_utc": now.isoformat(),
            "offsets_captured_utc": offsets_utc.isoformat(),
            "baseline_completed_utc": (clock or _utcnow)().isoformat(),
            "partition": partition,
            "runner": admin.describe(), "reader_image": READER_IMAGE, "replicas": replicas,
            "group_offsets": parse_group_offsets(offsets.stdout), "group_states": parse_group_states(states.stdout)}


# ------------------------------------------------------------------------------------------------ compare


def compare_offsets(before: Mapping[str, Mapping[str, int | None]],
                    after: Mapping[str, Mapping[str, int | None]]) -> list[dict[str, Any]]:
    """Violations: a group or a committed (topic, partition) that disappeared, or an offset that decreased.
    An offset that advanced (live commits) or a new group/partition is allowed."""
    violations = []
    for group, parts in sorted(before.items()):
        if group not in after:
            if any(v is not None for v in parts.values()):
                violations.append({"group": group, "kind": "group_disappeared"})
            continue
        for key, offset in sorted(parts.items()):
            if offset is None:
                continue
            now = after[group].get(key)
            if now is None:
                violations.append({"group": group, "partition": key, "kind": "offset_disappeared", "before": offset})
            elif now < offset:
                violations.append({"group": group, "partition": key, "kind": "offset_decreased", "before": offset,
                                   "after": now})
    return violations


def _after(stamp: str | None, since: str) -> bool:
    return stamp is not None and dt.datetime.fromisoformat(stamp) > dt.datetime.fromisoformat(since)


def _completed(item: Mapping[str, Any]) -> bool:
    return bool(item.get("finished_utc")) and item.get("end_messages") is not None


def baseline_bound(before: Mapping[str, Any]) -> tuple[str, bool]:
    """(the instant a pass must start after to be credited, exact?): the BEFORE snapshot's baseline completion (after
    its committed offsets were captured); a snapshot without it (pre-D3) falls back to its start, reported inexact."""
    done = before.get("baseline_completed_utc")
    return (done, True) if isinstance(done, str) else (before["at_utc"], False)


def observation_problems(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[str]:
    problems = []
    for label, doc in (("before", before), ("after", after)):
        if doc.get("read_contract") != READ_CONTRACT:
            problems.append(f"{label} snapshot does not follow {READ_CONTRACT} (read errors may be masked)")
        for node, replica in sorted((doc.get("replicas") or {}).items()):
            if not replica.get("segment_count"):
                problems.append(f"{label} replica {node} has no segment")
    if not before.get("replicas"):
        problems.append("before snapshot has no replica")
    missing = sorted(set(before.get("replicas") or {}) - set(after.get("replicas") or {}))
    if missing:
        problems.append(f"replicas missing after: {missing}")
    return problems


def compare(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    if before.get("partition") != after.get("partition"):
        raise Refused("snapshots are of different partitions")
    since, exact = baseline_bound(before)
    replicas = {}
    for node, old in sorted(before["replicas"].items()):
        new = after["replicas"].get(node)
        if new is None:
            replicas[node] = {"missing_after": True}
            continue
        window = [p for p in new["passes"] if _after(p["started_utc"], since)]
        during = [p for p in new["passes"] if _after(p["started_utc"], before["at_utc"])
                  and not _after(p["started_utc"], since)]
        replicas[node] = {
            "pass_started": bool(window), "pass_finished": any(_completed(p) for p in window),
            "passes_in_window": window, "passes_during_baseline": during,
            "size_bytes": {"before": old["size_bytes"], "after": new["size_bytes"],
                           "delta": new["size_bytes"] - old["size_bytes"]},
            "segment_count": {"before": old["segment_count"], "after": new["segment_count"]},
            "empty_segments": {"before": old["empty_segments"], "after": new["empty_segments"]},
            "cleaner_checkpoint": {"before": old["cleaner_checkpoint"], "after": new["cleaner_checkpoint"]},
            "log_start_checkpoint": {"before": old["log_start_checkpoint"], "after": new["log_start_checkpoint"]}}
    violations = compare_offsets(before["group_offsets"], after["group_offsets"])
    state_changes = {g: {"before": s.get("state"), "after": after["group_states"].get(g, {}).get("state")}
                     for g, s in sorted(before["group_states"].items())
                     if s.get("state") != after["group_states"].get(g, {}).get("state")}
    advanced = sum(1 for g, parts in before["group_offsets"].items() for k, v in parts.items()
                   if v is not None and (after["group_offsets"].get(g, {}).get(k) or -1) > v)
    baseline = sum(1 for parts in before["group_offsets"].values() for v in parts.values() if v is not None)
    problems = observation_problems(before, after)
    valid = not problems
    preserved = baseline > 0 and not violations
    completed = valid and bool(replicas) and all(r.get("pass_finished") for r in replicas.values())
    return {"kind": "kafka_offsets_pass_watch.compare", "partition": before["partition"],
            "before_utc": before["at_utc"], "after_utc": after["at_utc"], "replicas": replicas,
            "pass_window_from_utc": since, "baseline_bound_exact": exact,
            "offsets_captured_utc": before.get("offsets_captured_utc"),
            "groups_before": len(before["group_offsets"]), "groups_after": len(after["group_offsets"]),
            "baseline_committed_offsets": baseline,
            "offsets_advanced": advanced, "offset_violations": violations, "group_state_changes": state_changes,
            "observation_valid": valid, "observation_problems": problems,
            "offsets_preserved": preserved,
            "cleaner_completed_all_replicas": completed,
            "ok": valid and preserved,
            "note": "ok = offset safety on a valid observation; it is NOT cleaner completion and none of the three "
                    "outcomes alone authorizes the next broker"}


def compare_exit(report: Mapping[str, Any]) -> int:
    if not report["observation_valid"]:
        return 6
    if not report["offsets_preserved"]:
        return 1
    return 0 if report["cleaner_completed_all_replicas"] else 2


# ------------------------------------------------------------------------------------------------ main


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--snapshot", type=Path, metavar="OUT.json", help="write a read-only snapshot")
    mode.add_argument("--compare", type=Path, nargs=2, metavar=("BEFORE", "AFTER"))
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro into the admin container)")
    parser.add_argument("--partition", default=PARTITION)
    parser.add_argument("--hours", type=int, default=6, help="rotated cleaner-log hours to read (default 6)")
    parser.add_argument("--lock-owner", help="the caller's own host-measurement lock owner (does not block)")
    args = parser.parse_args(argv)
    try:
        if args.compare:
            before, after = (json.loads(p.read_text(encoding="utf-8")) for p in args.compare)
            report = compare(before, after)
            print(json.dumps(report, indent=1))
            return compare_exit(report)
        partition = _partition_name(args.partition)
        if not 0 <= args.hours <= 48:
            raise Refused("--hours must be 0..48")
        if args.cert_dir is None or not args.cert_dir.is_dir():
            raise Refused("--snapshot needs --cert-dir (a directory)")
        lock = foreign_lock(own_owner=args.lock_owner)
        if lock is not None:
            raise Refused(f"host-measurement lock active: {lock}")
        if args.snapshot.exists():
            raise Refused(f"{args.snapshot} exists (snapshots are never overwritten)")
        body = snapshot(AdminContainerRunner(args.cert_dir, lock_guard=True, lock_owner=args.lock_owner),
                        partition, args.hours)
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(body, indent=1) + "\n", encoding="utf-8")
        summary = {node: {k: r[k] for k in ("size_bytes", "segment_count", "empty_segments", "cleaner_checkpoint",
                                            "log_start_checkpoint")} | {"passes": len(r["passes"])}
                   for node, r in body["replicas"].items()}
        print(json.dumps({"snapshot": str(args.snapshot), "at_utc": body["at_utc"], "replicas": summary,
                          "groups": len(body["group_offsets"])}, indent=1))
        return 0
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except (RuntimeError, subprocess.SubprocessError, OSError, tarfile.TarError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

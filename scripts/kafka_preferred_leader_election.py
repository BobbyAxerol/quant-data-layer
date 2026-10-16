#!/usr/bin/env python3
"""Controlled preferred-leader election: the runbook step that replaces Kafka's automatic leader rebalance.

Purpose (P18.4 G12, option B): with the broker default ``auto.leader.rebalance.enable=true`` the KRaft
controller's periodic ``electPreferred`` task (every ``leader.imbalance.check.interval.seconds``, 300 s,
first run one full period after controller activation) moves every partition whose leader is not its
first replica back to the preferred broker once that broker is in the ISR again. When the moved set
contains the ``__consumer_offsets`` / ``__transaction_state`` partition that hosts the realtime core's
group or transaction coordinator, the new coordinator first loads that partition, and the core's
``send_offsets_to_transaction`` / ``commit_transaction`` (pipeline depth 1) cannot finish meanwhile, so
every feed stalls together (G12 rc18 receipts: 1.7-2.5 s). With automatic rebalance switched off
(a static controller config, owner decision), leadership comes back only through this tool, run at a
declared moment outside any measurement window, and its stall is recorded, never hidden.

Boundary:

* read-only by default: ``--observe`` describes every topic (internal ones included) and reports the
  partitions whose leader is not the preferred replica, split into coordinator partitions
  (``__consumer_offsets``, ``__transaction_state``: a coordinator move with a state load) and data
  partitions; without ``--observe`` or ``--apply`` it only prints the argv it would run (dry-run);
* ``--apply`` refuses while a foreign host-measurement lock is active, refuses when any in-scope
  imbalanced partition has its preferred replica outside the ISR or no leader (the election would fail
  for it), writes the exact partition list to a JSON file, runs ONE
  ``kafka-leader-election.sh --election-type preferred --path-to-json-file`` and describes again to
  verify that no in-scope partition is still imbalanced; nothing to elect -> no election (idempotent);
* ``--scope`` = ``all`` (default) | ``data`` | ``coordinators``: ``data`` restores data-partition
  balance without moving any coordinator;
* ``--window-out`` appends the election as a DECLARED_MAINTENANCE_INTERRUPTION window (start / return /
  verification clocks, moved coordinators and their server-side load lines from the new leaders' logs) for the
  stage samplers' first-valid / caught-up computation; this tool proves leadership, not stall duration;
* never an unclean election, never a broker/topic config change, never a reassignment; there is no
  rollback of an election other than another election (leadership is not data);
* the Kafka CLI runs in a throwaway container equal to compose ``stable_admin``
  (``scripts/kn_state_topics_packet.AdminContainerRunner``: pinned digest, network ``stable_internal``,
  read-only, cert dir mounted ro, ``--command-config admin.properties``); key material is never read.

Exit codes: 0 OK (dry-run printed / observed / elected and verified / nothing to elect),
1 verify mismatch, 4 refused, 5 command error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import re
import sys
import tempfile
from typing import Any, Iterable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kafka_md_latest_marker_retention import foreign_lock  # noqa: E402
from scripts.kn_state_topics_packet import AdminContainerRunner, CommandResult  # noqa: E402

COORDINATOR_TOPICS = frozenset({"__consumer_offsets", "__transaction_state"})
PRODUCTION_BROKER = "qdl_v2_stable_candidate-kafka{node}-1"
# Kafka 4.2 coordinator load lines (CoordinatorRuntime / TransactionStateManager), server-side load duration.
GROUP_LOAD_LINE = re.compile(
    r"\[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})\] INFO .*?Finished loading of metadata from "
    r"(?P<tp>__consumer_offsets-\d+) with epoch (?P<epoch>\d+) in (?P<ms>\d+)ms where (?P<sched>\d+)ms was spent in "
    r"the scheduler\. Loaded (?P<records>\d+) records which total to (?P<bytes>\d+) bytes")
TXN_LOAD_LINE = re.compile(
    r"\[(?P<ts>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})\] INFO \[Transaction State Manager \d+\]: Finished loading "
    r"(?P<ids>\d+) transaction metadata from (?P<tp>__transaction_state-\d+) in (?P<ms>\d+) milliseconds, of which "
    r"(?P<sched>\d+) milliseconds was spent in the scheduler")
SCOPES = ("all", "data", "coordinators")
ELECTION_FILE = "/run/qdl-election/partitions.json"

_PARTITION = re.compile(
    r"^\s*Topic:\s*(?P<topic>\S+)\s+Partition:\s*(?P<partition>\d+)\s+Leader:\s*(?P<leader>-?\d+|none)"
    r"\s+Replicas:\s*(?P<replicas>[\d,]*)\s+Isr:\s*(?P<isr>[\d,]*)")


class Refused(Exception):
    pass


def describe_cmd() -> list[str]:
    # No --topic: every topic, internal ones included (no --exclude-internal).
    return ["kafka-topics.sh", "--describe"]


def election_cmd() -> list[str]:
    return ["kafka-leader-election.sh", "--election-type", "preferred", "--path-to-json-file", ELECTION_FILE]


def _ids(text: str) -> list[int]:
    return [int(item) for item in text.split(",") if item]


def parse_partitions(text: str) -> list[dict[str, Any]]:
    """Partition lines of ``kafka-topics.sh --describe`` (Kafka 4.x layout)."""
    rows = []
    for line in text.splitlines():
        match = _PARTITION.match(line)
        if not match:
            continue
        leader = match.group("leader")
        rows.append({"topic": match.group("topic"), "partition": int(match.group("partition")),
                     "leader": -1 if leader == "none" else int(leader),
                     "replicas": _ids(match.group("replicas")), "isr": _ids(match.group("isr"))})
    return rows


def in_scope(topic: str, scope: str) -> bool:
    if scope not in SCOPES:
        raise Refused(f"--scope must be one of {SCOPES}")
    coordinator = topic in COORDINATOR_TOPICS
    return scope == "all" or (scope == "coordinators") == coordinator


def imbalance(rows: Iterable[Mapping[str, Any]], scope: str = "all") -> dict[str, Any]:
    """Partitions whose leader is not the preferred (first) replica, by kind, plus the blockers."""
    report: dict[str, Any] = {"coordinator": [], "data": [], "blocked": [], "partitions": 0, "leaders": {}}
    for row in rows:
        report["partitions"] += 1
        report["leaders"][str(row["leader"])] = report["leaders"].get(str(row["leader"]), 0) + 1
        if not row["replicas"] or not in_scope(row["topic"], scope):
            continue
        preferred = row["replicas"][0]
        if row["leader"] == preferred:
            continue
        item = {"topic": row["topic"], "partition": row["partition"], "leader": row["leader"],
                "preferred": preferred}
        report["coordinator" if row["topic"] in COORDINATOR_TOPICS else "data"].append(item)
        if row["leader"] < 0 or preferred not in row["isr"]:
            report["blocked"].append({**item, "isr": list(row["isr"])})
    return report


def election_document(report: Mapping[str, Any]) -> dict[str, Any]:
    if report["blocked"]:
        raise Refused(f"preferred replica outside the ISR or no leader: {report['blocked']}")
    partitions = [{"topic": item["topic"], "partition": item["partition"]}
                  for item in [*report["coordinator"], *report["data"]]]
    if len({(p["topic"], p["partition"]) for p in partitions}) != len(partitions):
        raise Refused("duplicate topic-partition in the election list")
    return {"partitions": sorted(partitions, key=lambda p: (p["topic"], p["partition"]))}


class ElectionRunner(AdminContainerRunner):
    """AdminContainerRunner plus the election file mounted read-only at ELECTION_FILE."""

    def __init__(self, cert_dir: Path, election_file: Path, **kwargs: Any) -> None:
        super().__init__(cert_dir, **kwargs)
        self.election_file = election_file

    def argv(self, command: Sequence[str]) -> list[str]:
        argv = super().argv(command)
        if command[0] == "kafka-leader-election.sh":
            at = argv.index("--entrypoint")
            argv[at:at] = ["-v", f"{self.election_file}:{ELECTION_FILE}:ro"]
        return argv


def run(runner, command: Sequence[str]) -> CommandResult:
    result = runner.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"{command[0]} failed rc={result.returncode}: {result.stderr.strip()[-400:]}")
    return result


def observe(runner, scope: str) -> dict[str, Any]:
    return imbalance(parse_partitions(run(runner, describe_cmd()).stdout), scope)


def _summary(report: Mapping[str, Any]) -> dict[str, Any]:
    return {"coordinator": len(report["coordinator"]), "data": len(report["data"]),
            "blocked": len(report["blocked"])}


def coordinator_loads(moves: Sequence[Mapping[str, Any]], since_utc: dt.datetime,
                      logs=None) -> list[dict[str, Any]]:
    """Server-side load lines of the moved coordinator partitions, read from the new leader's container log
    (``docker logs --since``, read-only). A move without a line is reported with ``load: None``."""
    logs = logs or _docker_logs
    found = []
    for move in moves:
        text = logs(PRODUCTION_BROKER.format(node=move["preferred"]), since_utc)
        pattern = GROUP_LOAD_LINE if move["topic"] == "__consumer_offsets" else TXN_LOAD_LINE
        tp = f"{move['topic']}-{move['partition']}"
        lines = [m.groupdict() for m in pattern.finditer(text) if m.group("tp") == tp]
        found.append({"partition": tp, "new_leader": move["preferred"], "load": lines[-1] if lines else None})
    return found


def _docker_logs(container: str, since_utc: dt.datetime) -> str:
    import subprocess
    result = subprocess.run(["docker", "logs", "--since", since_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), container],
                            capture_output=True, text=True, timeout=120)
    return result.stdout + result.stderr


def window_record(report: Mapping[str, Any]) -> dict[str, Any]:
    """The declared maintenance interruption as a fault window (Codex K1): consumer-side first-valid / caught-up
    clocks are computed by the stage samplers (G11 four timestamps) with ``injected_utc`` = election start and
    ``cleared_utc`` = verification; this tool proves leadership only, never stall duration."""
    return {"schema": "qdl.kafka.preferred-election-window.v1", "kind": "DECLARED_MAINTENANCE_INTERRUPTION",
            "injected_utc": report.get("election_started_utc"), "election_returned_utc":
            report.get("election_returned_utc"), "cleared_utc": report.get("verified_utc"),
            "coordinator_moves": [f"{m['topic']}-{m['partition']}" for m in report.get("coordinator_moves", [])],
            "coordinator_loads": report.get("coordinator_loads", []), "scope": report.get("scope"),
            "consumer_clocks": "NOT_MEASURED_BY_THIS_TOOL"}


def elect(runner, scope: str, workdir: Path) -> dict[str, Any]:
    before = observe(runner, scope)
    document = election_document(before)
    report: dict[str, Any] = {"scope": scope, "before": _summary(before),
                              "coordinator_moves": before["coordinator"], "election": document}
    if not document["partitions"]:
        report["mode"] = "nothing_to_elect"
        return report
    path = workdir / "partitions.json"
    path.write_text(json.dumps(document) + "\n", encoding="utf-8")
    path.chmod(0o644)
    runner.election_file = path
    started = dt.datetime.now(dt.timezone.utc)
    output = run(runner, election_cmd())
    returned = dt.datetime.now(dt.timezone.utc)
    after = observe(runner, scope)
    report.update({"mode": "elected", "election_started_utc": started.isoformat(),
                   "election_returned_utc": returned.isoformat(),
                   "verified_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                   "election_stdout_tail": output.stdout.strip()[-800:], "after": _summary(after),
                   "still_imbalanced": [*after["coordinator"], *after["data"]],
                   "leaders_after": after["leaders"]})
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--scope", choices=SCOPES, default="all")
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); required to run")
    parser.add_argument("--window-out", type=Path,
                        help="--apply: append the election as a declared-interruption window (JSONL)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--observe", action="store_true", help="read-only describe and imbalance report")
    mode.add_argument("--apply", action="store_true", help="run the preferred election once and verify")
    args = parser.parse_args(argv)
    try:
        if not (args.observe or args.apply):
            runner = ElectionRunner(args.cert_dir or Path("${QDL_STABLE_CERT_DIR}"),
                                    Path("${WORKDIR}/partitions.json"))
            print(json.dumps({"mode": "dry-run", "scope": args.scope,
                              "describe": runner.argv(describe_cmd()),
                              "elect": runner.argv(election_cmd()),
                              "coordinator_topics": sorted(COORDINATOR_TOPICS)}, indent=1))
            return 0
        if args.cert_dir is None:
            raise Refused("--observe/--apply need --cert-dir")
        if args.apply:
            lock = foreign_lock()
            if lock is not None:
                raise Refused(f"host-measurement lock active: {lock}")
        with tempfile.TemporaryDirectory(prefix="qdl-election-") as tmp:
            runner = ElectionRunner(args.cert_dir, Path(tmp) / "partitions.json")
            runner.preflight()
            if args.observe:
                report = observe(runner, args.scope)
                print(json.dumps({"mode": "observed", "scope": args.scope, "summary": _summary(report),
                                  **report}, indent=1))
                return 0
            report = elect(runner, args.scope, Path(tmp))
        if report.get("mode") == "elected" and report["coordinator_moves"]:
            try:
                report["coordinator_loads"] = coordinator_loads(
                    report["coordinator_moves"], dt.datetime.fromisoformat(report["election_started_utc"])
                    - dt.timedelta(seconds=2))
            except Exception as error:  # noqa: BLE001 - evidence only; the election already happened
                report["coordinator_loads"] = [{"error": f"{type(error).__name__}: {error}"[:300]}]
        report["at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        if args.window_out:
            with args.window_out.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(window_record(report), sort_keys=True) + "\n")
        print(json.dumps(report, indent=1))
        return 0 if not report.get("still_imbalanced") else 1
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except RuntimeError as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())

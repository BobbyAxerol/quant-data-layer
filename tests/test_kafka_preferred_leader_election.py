"""Preferred-leader election runbook tool: read-only observe, dry-run default, apply refuses a foreign
lock / a preferred replica outside the ISR, elects exactly the imbalanced in-scope partitions once,
verifies, and never touches a coordinator partition under --scope data."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_preferred_leader_election as tool
from scripts.kn_state_topics_packet import CommandResult

# Kafka 4.2 kafka-topics.sh --describe layout (tab separated, Elr columns present).
LINE = "\tTopic: {t}\tPartition: {p}\tLeader: {l}\tReplicas: {r}\tIsr: {i}\tElr: \tLastKnownElr: "
HEADER = "Topic: {t}\tTopicId: AAAAAAAAAAAAAAAAAAAAAA\tPartitionCount: {n}\tReplicationFactor: 3\tConfigs: x=y"


def describe(state: dict[tuple[str, int], tuple[int, str, str]]) -> str:
    out, seen = [], set()
    for (topic, part), (leader, replicas, isr) in sorted(state.items()):
        if topic not in seen:
            seen.add(topic)
            out.append(HEADER.format(t=topic, n=sum(1 for k in state if k[0] == topic)))
        out.append(LINE.format(t=topic, p=part, l=leader, r=replicas, i=isr))
    return "\n".join(out) + "\n"


def cluster(over=None):
    state = {("md.canonical.v2", 0): (2, "1,2,3", "2,3,1"),     # data, imbalanced
             ("md.canonical.v2", 1): (2, "2,3,1", "2,3,1"),     # balanced
             ("__consumer_offsets", 7): (3, "1,3,2", "3,2,1"),  # coordinator, imbalanced
             ("__transaction_state", 4): (2, "2,1,3", "2,1,3")}
    state.update(over or {})
    return state


class Broker:
    """Answers describe / election like the Kafka CLI; the election moves leaders to the first replica."""

    def __init__(self, state, election_rc=0):
        self.state, self.election_rc, self.calls, self.election_file = dict(state), election_rc, [], None

    def run(self, command):
        self.calls.append(list(command))
        if command[0] == "kafka-leader-election.sh":
            document = json.loads(Path(self.election_file).read_text())
            self.elected = document["partitions"]
            if self.election_rc == 0:
                for item in document["partitions"]:
                    key = (item["topic"], item["partition"])
                    _, replicas, isr = self.state[key]
                    self.state[key] = (int(replicas.split(",")[0]), replicas, isr)
            return CommandResult(self.election_rc, "Successfully completed leader election (PREFERRED)\n",
                                 "" if self.election_rc == 0 else "boom")
        return CommandResult(0, describe(self.state), "")


class Parse(unittest.TestCase):
    def test_describe_partition_lines(self):
        rows = tool.parse_partitions(describe(cluster()))
        self.assertEqual(len(rows), 4)
        row = next(r for r in rows if r["topic"] == "__consumer_offsets")
        self.assertEqual(row, {"topic": "__consumer_offsets", "partition": 7, "leader": 3,
                               "replicas": [1, 3, 2], "isr": [3, 2, 1]})

    def test_no_leader(self):
        rows = tool.parse_partitions(describe({("t", 0): ("none", "1,2,3", "")}))
        self.assertEqual(rows[0]["leader"], -1)

    def test_imbalance_by_kind_and_scope(self):
        rows = tool.parse_partitions(describe(cluster()))
        report = tool.imbalance(rows)
        self.assertEqual([i["topic"] for i in report["coordinator"]], ["__consumer_offsets"])
        self.assertEqual([(i["topic"], i["partition"]) for i in report["data"]], [("md.canonical.v2", 0)])
        self.assertEqual(report["leaders"], {"2": 3, "3": 1})
        self.assertEqual(tool.imbalance(rows, "data")["coordinator"], [])
        self.assertEqual(tool.imbalance(rows, "coordinators")["data"], [])

    def test_blocked_when_preferred_outside_isr(self):
        rows = tool.parse_partitions(describe(cluster({("md.raw.realtime.v2", 0): (2, "1,2,3", "2,3")})))
        report = tool.imbalance(rows)
        self.assertEqual([b["topic"] for b in report["blocked"]], ["md.raw.realtime.v2"])
        with self.assertRaises(tool.Refused):
            tool.election_document(report)


class Argv(unittest.TestCase):
    def test_election_file_mounted_read_only_and_command_config(self):
        runner = tool.ElectionRunner(Path("/certs"), Path("/w/partitions.json"), image="img@sha256:x")
        argv = runner.argv(tool.election_cmd())
        self.assertIn(f"/w/partitions.json:{tool.ELECTION_FILE}:ro", argv)
        self.assertLess(argv.index(f"/w/partitions.json:{tool.ELECTION_FILE}:ro"), argv.index("--entrypoint"))
        self.assertIn("--command-config", argv)
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[argv.index("--election-type") + 1], "preferred")
        self.assertNotIn("unclean", argv)
        describe_argv = runner.argv(tool.describe_cmd())
        self.assertNotIn(f"/w/partitions.json:{tool.ELECTION_FILE}:ro", describe_argv)
        self.assertNotIn("--exclude-internal", describe_argv)


class Elect(unittest.TestCase):
    def _elect(self, broker, scope):
        with tempfile.TemporaryDirectory() as tmp:
            return tool.elect(broker, scope, Path(tmp))

    def test_elects_exactly_the_imbalanced_partitions_once_and_verifies(self):
        broker = Broker(cluster())
        report = self._elect(broker, "all")
        self.assertEqual(report["mode"], "elected")
        self.assertEqual(broker.elected, [{"topic": "__consumer_offsets", "partition": 7},
                                          {"topic": "md.canonical.v2", "partition": 0}])
        self.assertEqual([c[0] for c in broker.calls],
                         ["kafka-topics.sh", "kafka-leader-election.sh", "kafka-topics.sh"])
        self.assertEqual(report["still_imbalanced"], [])
        self.assertEqual(len(report["coordinator_moves"]), 1)

    def test_data_scope_never_moves_a_coordinator(self):
        broker = Broker(cluster())
        report = self._elect(broker, "data")
        self.assertEqual(broker.elected, [{"topic": "md.canonical.v2", "partition": 0}])
        self.assertEqual(broker.state[("__consumer_offsets", 7)][0], 3)
        self.assertEqual(report["coordinator_moves"], [])

    def test_nothing_to_elect_runs_no_election(self):
        broker = Broker(cluster({("md.canonical.v2", 0): (1, "1,2,3", "1,2,3"),
                                   ("__consumer_offsets", 7): (1, "1,3,2", "1,3,2")}))
        report = self._elect(broker, "all")
        self.assertEqual(report["mode"], "nothing_to_elect")
        self.assertEqual([c[0] for c in broker.calls], ["kafka-topics.sh"])

    def test_blocked_partition_refuses_before_any_election(self):
        broker = Broker(cluster({("md.canonical.v2", 0): (2, "1,2,3", "2,3")}))
        with self.assertRaises(tool.Refused):
            self._elect(broker, "all")
        self.assertEqual([c[0] for c in broker.calls], ["kafka-topics.sh"])

    def test_failed_election_is_a_command_error(self):
        broker = Broker(cluster(), election_rc=1)
        with self.assertRaises(RuntimeError):
            self._elect(broker, "all")


class Main(unittest.TestCase):
    def _main(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = tool.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_default_is_dry_run_and_runs_nothing(self):
        with mock.patch("scripts.kn_state_topics_packet._subprocess") as sub:
            code, out, _ = self._main([])
        self.assertEqual(code, 0)
        sub.assert_not_called()
        body = json.loads(out)
        self.assertEqual(body["mode"], "dry-run")
        self.assertIn("kafka-leader-election.sh", " ".join(body["elect"]))

    def test_run_needs_cert_dir(self):
        code, _, err = self._main(["--observe"])
        self.assertEqual(code, 4)
        self.assertIn("cert-dir", err)

    def test_apply_refuses_foreign_lock(self):
        lock = {"owner": "other", "until_utc": (dt.datetime.now(dt.timezone.utc)
                                                + dt.timedelta(hours=1)).isoformat()}
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", Path(tmp) / "lock"), \
                mock.patch("scripts.kn_state_topics_packet._subprocess") as sub:
            (Path(tmp) / "lock").write_text(json.dumps(lock))
            code, _, err = self._main(["--apply", "--cert-dir", tmp])
        self.assertEqual(code, 4)
        self.assertIn("lock", err)
        sub.assert_not_called()

    def test_observe_does_not_check_the_lock_and_never_elects(self):
        broker = Broker(cluster())

        def fake(argv, timeout):
            entry = next(a for a in argv if a.startswith("/opt/kafka/bin/"))
            return broker.run([entry.rsplit("/", 1)[1]])

        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch("scripts.kn_state_topics_packet._subprocess", side_effect=fake):
            code, out, _ = self._main(["--observe", "--cert-dir", tmp])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["summary"], {"coordinator": 1, "data": 1, "blocked": 0})
        self.assertEqual([c[0] for c in broker.calls], ["kafka-topics.sh"])


class Window(unittest.TestCase):
    """Codex K1: the election is recorded as a declared interruption with clocks and server-side loads."""

    LOG = ("[2026-10-16 07:00:01,200] INFO [GroupCoordinator id=1] Finished loading of metadata from "
           "__consumer_offsets-7 with epoch 5 in 812ms where 1ms was spent in the scheduler. Loaded 9000 records "
           "which total to 700000 bytes. (x)\n"
           "[2026-10-16 07:00:01,300] INFO [GroupCoordinator id=1] Finished loading of metadata from "
           "__consumer_offsets-8 with epoch 5 in 3ms where 1ms was spent in the scheduler. Loaded 0 records "
           "which total to 0 bytes. (x)\n")

    def test_coordinator_loads_pick_the_moved_partition_on_the_new_leader(self):
        seen = []

        def logs(container, since):
            seen.append(container)
            return self.LOG
        moves = [{"topic": "__consumer_offsets", "partition": 7, "leader": 3, "preferred": 1},
                 {"topic": "__transaction_state", "partition": 4, "leader": 3, "preferred": 2}]
        found = tool.coordinator_loads(moves, dt.datetime(2026, 10, 16, 7, tzinfo=dt.timezone.utc), logs)
        self.assertEqual(seen, ["qdl_v2_stable_candidate-kafka1-1", "qdl_v2_stable_candidate-kafka2-1"])
        self.assertEqual((found[0]["load"]["ms"], found[0]["load"]["records"]), ("812", "9000"))
        self.assertIsNone(found[1]["load"])

    def test_window_out_is_appended_after_an_apply(self):
        broker = Broker(cluster())

        def fake(argv, timeout):
            entry = next(a for a in argv if a.startswith("/opt/kafka/bin/"))
            mounts = [a for a in argv if a.endswith(f":{tool.ELECTION_FILE}:ro")]
            if mounts:
                broker.election_file = mounts[0].split(":")[0]
            return broker.run([entry.rsplit("/", 1)[1]])

        with tempfile.TemporaryDirectory() as tmp:
            window = Path(tmp) / "windows.jsonl"
            out, err = io.StringIO(), io.StringIO()
            with mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", Path(tmp) / "lock"), \
                    mock.patch("scripts.kn_state_topics_packet._subprocess", side_effect=fake), \
                    mock.patch.object(tool, "_docker_logs", return_value=self.LOG), \
                    contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = tool.main(["--apply", "--cert-dir", tmp, "--window-out", str(window)])
            self.assertEqual(code, 0, err.getvalue())
            record = json.loads(window.read_text().splitlines()[0])
        self.assertEqual(record["kind"], "DECLARED_MAINTENANCE_INTERRUPTION")
        self.assertEqual(record["coordinator_moves"], ["__consumer_offsets-7"])
        self.assertEqual(record["coordinator_loads"][0]["load"]["ms"], "812")
        self.assertEqual(record["consumer_clocks"], "NOT_MEASURED_BY_THIS_TOOL")
        self.assertTrue(record["injected_utc"] <= record["election_returned_utc"] <= record["cleared_utc"])


if __name__ == "__main__":
    unittest.main()

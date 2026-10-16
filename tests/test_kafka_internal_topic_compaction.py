"""Decision C tool: two internal topics, the PLAN keys only (absent or target), dry-run by default; apply reads
before, refuses drift / foreign values / a foreign lock, alters once per topic, verifies, stops after a mismatch,
and rolls back by deleting exactly the planned overrides."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_internal_topic_compaction as tool
from scripts.kn_state_topics_packet import AdminContainerRunner, CommandResult


def describe(topic: str, configs: dict[str, str]) -> str:
    lines = [f"Dynamic configs for topic {topic} are:"]
    lines += [f"  {k}={v} sensitive=false synonyms={{DYNAMIC_TOPIC_CONFIG:{k}={v}}}" for k, v in configs.items()]
    return "\n".join(lines) + "\n"


class Broker:
    """kafka-configs.sh describe/alter for the two internal topics."""

    def __init__(self, extra=None, planned=None):
        self.configs = {t: dict(tool.FROZEN_OTHERS[t]) for t in tool.TOPICS}
        for topic, values in (planned or {}).items():
            self.configs[topic].update(values)
        for topic, values in (extra or {}).items():
            self.configs[topic].update(values)
        self.calls = []

    def run(self, command):
        self.calls.append(list(command))
        topic = command[command.index("--entity-name") + 1]
        if "--add-config" in command:
            for pair in command[-1].split(","):
                key, value = pair.split("=", 1)
                self.configs[topic][key] = value
            return CommandResult(0, f"Completed updating config for topic {topic}.\n", "")
        if "--delete-config" in command:
            for key in command[-1].split(","):
                self.configs[topic].pop(key, None)
            return CommandResult(0, f"Completed updating config for topic {topic}.\n", "")
        return CommandResult(0, describe(topic, self.configs[topic]), "")

    def verbs(self):
        return [(c[c.index("--entity-name") + 1], c[5]) for c in self.calls]


class Commands(unittest.TestCase):
    def test_alter_and_rollback_touch_only_planned_keys_of_known_topics(self):
        for topic in tool.TOPICS:
            alter = tool.alter_cmd(topic)
            self.assertEqual(alter[:6], ["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic,
                                         "--alter"])
            keys = {pair.split("=")[0] for pair in alter[-1].split(",")}
            self.assertEqual(keys, set(tool.PLAN[topic]))
            self.assertEqual(set(tool.rollback_cmd(topic)[-1].split(",")), set(tool.PLAN[topic]))
        for bad in ("md.latest.v2", "md.bars.v2", "__cluster_metadata"):
            with self.assertRaises(tool.Refused):
                tool.alter_cmd(bad)

    def test_plan_is_bounded(self):
        allowed = {"delete.retention.ms", "segment.ms"}
        for topic, keys in tool.PLAN.items():
            self.assertTrue(set(keys) <= allowed, topic)
            for key, value in keys.items():
                self.assertGreaterEqual(int(value), 60_000, (topic, key))  # never a sub-minute value
        self.assertNotIn("delete.retention.ms", tool.PLAN["__transaction_state"])  # no markers there
        # A tombstone/marker must outlive one full cleaner cycle of the head it was written in.
        offsets = tool.PLAN["__consumer_offsets"]
        self.assertGreater(int(offsets["delete.retention.ms"]), int(offsets["segment.ms"]))

    def test_frozen_snapshot_matches_the_2026_10_16_describe(self):
        self.assertEqual(tool.FROZEN_OTHERS["__consumer_offsets"],
                         {"cleanup.policy": "compact", "compression.type": "producer", "segment.bytes": "104857600"})
        self.assertNotIn("retention.ms", tool.FROZEN_OTHERS["__transaction_state"])
        self.assertEqual(tool.FROZEN_OTHERS["__consumer_offsets"]["compression.type"], "producer")
        self.assertEqual(tool.FROZEN_OTHERS["__transaction_state"]["compression.type"], "uncompressed")
        for topic in tool.TOPICS:
            self.assertEqual(tool.FROZEN_OTHERS[topic]["cleanup.policy"], "compact")
            self.assertFalse(set(tool.FROZEN_OTHERS[topic]) & set(tool.PLAN[topic]))


class DryRun(unittest.TestCase):
    def test_dry_run_prints_apply_and_rollback_and_runs_nothing(self):
        out = io.StringIO()
        with mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(tool.main([]), 0)
        body = json.loads(out.getvalue())
        self.assertEqual(body["mode"], "dry-run")
        self.assertEqual(body["plan_status"], tool.PLAN_STATUS)
        self.assertEqual(list(body["topics"]), list(tool.TOPICS))
        argv = body["topics"]["__consumer_offsets"]["apply"]
        self.assertEqual(argv[:3], ["docker", "run", "--rm"])
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[argv.index("--command-config") + 1], "/etc/kafka/secrets/admin.properties")
        self.assertIn("--delete-config", body["topics"]["__transaction_state"]["rollback"])

    def test_dry_run_one_topic(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(tool.main(["--topic", "__transaction_state", "--rollback"]), 0)
        body = json.loads(out.getvalue())
        self.assertEqual((list(body["topics"]), body["requested"]), (["__transaction_state"], "rollback"))


class Apply(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(tool, "PLAN_STATUS", "MEASURED")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_production_apply_refuses_a_proposed_plan(self):
        broker = Broker()
        with mock.patch.object(tool, "PLAN_STATUS", "PROPOSED"):
            code, _, err = self._main(broker, [])
        self.assertEqual(code, 4)
        self.assertIn("PROPOSED", err)
        self.assertEqual(broker.calls, [])

    def _main(self, broker, argv, lock=None):
        out, err = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as certs:
            lock_path = Path(certs) / "lock"
            if lock is not None:
                lock_path.write_text(json.dumps(lock))
            with mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", lock_path), \
                    mock.patch.object(AdminContainerRunner, "run", broker.run), \
                    contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = tool.main([*argv, "--apply", "--cert-dir", certs])
        return code, out.getvalue(), err.getvalue()

    def test_apply_both_topics_txn_state_first_and_verifies(self):
        broker = Broker()
        code, out, _ = self._main(broker, [])
        self.assertEqual(code, 0, out)
        report = json.loads(out)
        self.assertEqual([r["topic"] for r in report["topics"]], ["__transaction_state", "__consumer_offsets"])
        self.assertTrue(all(r["mode"] == "applied" and r["ok"] for r in report["topics"]))
        self.assertEqual(broker.verbs(), [("__transaction_state", "--describe"), ("__consumer_offsets", "--describe"),
                                          ("__transaction_state", "--describe"), ("__transaction_state", "--alter"),
                                          ("__transaction_state", "--describe"),
                                          ("__consumer_offsets", "--describe"), ("__consumer_offsets", "--alter"),
                                          ("__consumer_offsets", "--describe")])
        for topic in tool.TOPICS:
            for key, value in tool.PLAN[topic].items():
                self.assertEqual(broker.configs[topic][key], value)
            self.assertEqual({k: v for k, v in broker.configs[topic].items() if k not in tool.PLAN[topic]},
                             tool.FROZEN_OTHERS[topic])

    def test_idempotent_and_rollback_restores_the_pre_c_state(self):
        broker = Broker(planned=tool.PLAN)
        code, out, _ = self._main(broker, [])
        self.assertEqual(code, 0)
        self.assertTrue(all(r["mode"] == "already_at_state" for r in json.loads(out)["topics"]))
        self.assertNotIn("--alter", [v for _, v in broker.verbs()])
        code, out, _ = self._main(broker, ["--rollback"])
        self.assertEqual(code, 0, out)
        for topic in tool.TOPICS:
            self.assertEqual(broker.configs[topic], tool.FROZEN_OTHERS[topic])
        self.assertTrue(all(r["mode"] == "rolled_back" for r in json.loads(out)["topics"]))

    def test_partial_state_is_completed(self):
        broker = Broker(planned={"__consumer_offsets": {"delete.retention.ms":
                                                        tool.PLAN["__consumer_offsets"]["delete.retention.ms"]}})
        code, _, _ = self._main(broker, ["--topic", "__consumer_offsets"])
        self.assertEqual(code, 0)
        self.assertEqual({k: broker.configs["__consumer_offsets"][k] for k in tool.PLAN["__consumer_offsets"]},
                         tool.PLAN["__consumer_offsets"])

    def test_refuses_drift_or_a_foreign_value_before_any_alter(self):
        for broker in (Broker(extra={"__transaction_state": {"min.insync.replicas": "1"}}),
                       Broker(extra={"__consumer_offsets": {"min.cleanable.dirty.ratio": "0.1"}}),
                       Broker(extra={"__transaction_state": {"segment.ms": "1000"}}),
                       Broker(extra={"__consumer_offsets": {"delete.retention.ms": "86400000"}})):
            code, _, err = self._main(broker, [])
            self.assertEqual(code, 4, err)
            self.assertNotIn("--alter", [v for _, v in broker.verbs()])

    def test_refuses_under_a_foreign_lock(self):
        until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat()
        broker = Broker()
        code, _, err = self._main(broker, [], lock={"owner": "other", "until_utc": until})
        self.assertEqual(code, 4)
        self.assertIn("lock", err)
        self.assertEqual(broker.calls, [])

    def test_apply_needs_a_target(self):
        with contextlib.redirect_stderr(io.StringIO()), \
                mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", Path("/nonexistent/lock")):
            self.assertEqual(tool.main(["--apply"]), 4)

    def test_isolated_runner_refuses_a_production_container(self):
        with contextlib.redirect_stderr(io.StringIO()), \
                mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", Path("/nonexistent/lock")):
            self.assertEqual(tool.main(["--apply", "--isolated-container", "qdl_v2_stable_candidate-kafka1-1"]), 4)

    def test_mismatch_stops_before_the_next_topic(self):
        class Stuck(Broker):
            def run(self, command):
                if "--add-config" in command:
                    self.calls.append(list(command))
                    return CommandResult(0, "Completed\n", "")  # accepted but not taken
                return super().run(command)
        broker = Stuck()
        code, out, _ = self._main(broker, [])
        self.assertEqual(code, 1)
        report = json.loads(out)
        self.assertEqual([r["topic"] for r in report["topics"]], ["__transaction_state"])
        self.assertFalse(report["topics"][0]["ok"])
        self.assertNotIn(("__consumer_offsets", "--alter"), broker.verbs())

    def test_command_error_is_exit_5(self):
        class Down(Broker):
            def run(self, command):
                self.calls.append(list(command))
                return CommandResult(1, "", "TimeoutException")
        code, _, err = self._main(Down(), [])
        self.assertEqual(code, 5)
        self.assertIn("TimeoutException", err)


class Observe(unittest.TestCase):
    LOG_DIRS = ("Querying brokers for log directories information\nReceived log directory information from brokers 1,2,3\n"
                + json.dumps({"version": 1, "brokers": [
                    {"broker": b, "logDirs": [{"logDir": "/var/lib/kafka/data", "error": None, "partitions": [
                        {"partition": "__consumer_offsets-43", "size": 1650188753, "offsetLag": 0, "isFuture": False},
                        {"partition": "__consumer_offsets-0", "size": 1, "offsetLag": 0, "isFuture": False}]}]}
                    for b in (1, 2, 3)]}) + "\n")

    class ReadOnly(Broker):
        def run(self, command):
            if command[0] == "kafka-log-dirs.sh":
                self.calls.append(list(command))
                return CommandResult(0, Observe.LOG_DIRS, "")
            if command[0] == "kafka-get-offsets.sh":
                self.calls.append(list(command))
                value = 357710070 if command[-1] == "-1" else 0
                return CommandResult(0, f"__consumer_offsets:43:{value}\n", "")
            if "--alter" in command:
                raise AssertionError("observe altered")
            return super().run(command)

    def test_observe_is_read_only_and_reports_partition_and_overrides(self):
        broker = self.ReadOnly(planned={"__transaction_state": {"segment.ms": tool.PLAN["__transaction_state"]["segment.ms"]}})
        with mock.patch.object(AdminContainerRunner, "run", side_effect=broker.run):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(tool.main(["--observe", "--cert-dir", "/nonexistent"]), 0)
        body = json.loads(out.getvalue())
        part = body["partitions"]["__consumer_offsets-43"]
        self.assertEqual(part["size_bytes_by_broker"], {"1": 1650188753, "2": 1650188753, "3": 1650188753})
        self.assertEqual((part["log_end_offset"], part["offset_span"]), (357710070, 357710070))
        self.assertEqual(body["topics"]["__consumer_offsets"]["c_state"], "ABSENT")
        self.assertEqual(body["topics"]["__transaction_state"]["c_state"], "APPLIED")
        self.assertTrue(all(t["other_overrides_match_frozen"] for t in body["topics"].values()))
        self.assertFalse(any("--alter" in c for c in broker.calls))

    def test_observe_refuses_mutating_flags_and_unknown_partitions(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(tool.main(["--observe", "--apply", "--cert-dir", "/x"]), 4)
            self.assertEqual(tool.main(["--observe", "--partition", "md.latest.v2:0", "--cert-dir", "/x"]), 4)
            self.assertEqual(tool.main(["--observe"]), 4)

    def test_measured_values_from_the_aged_state_run(self):
        self.assertEqual(tool.PLAN["__consumer_offsets"], {"delete.retention.ms": "600000", "segment.ms": "60000"})
        self.assertEqual(tool.PLAN["__transaction_state"], {"segment.ms": "600000"})


class TempOverride(unittest.TestCase):
    """The temporary min.cleanable.dirty.ratio key: own apply/remove, not drift while at its target."""

    TEMP = tool.TEMP_OVERRIDES["__consumer_offsets"]

    def _main(self, broker, argv, lock=None):
        return Apply._main(self, broker, argv, lock)  # same harness: patched lock path and runner

    def test_temp_is_one_key_on_offsets_only_and_disjoint_from_plan_and_frozen(self):
        self.assertEqual(tool.TEMP_OVERRIDES, {"__consumer_offsets": {"min.cleanable.dirty.ratio": "0.05"}})
        for topic, keys in tool.TEMP_OVERRIDES.items():
            self.assertFalse(set(keys) & (set(tool.PLAN[topic]) | set(tool.FROZEN_OTHERS[topic])))
        self.assertEqual(tool.temp_alter_cmd("__consumer_offsets")[-2:],
                         ["--add-config", "min.cleanable.dirty.ratio=0.05"])
        self.assertEqual(tool.temp_remove_cmd("__consumer_offsets")[-2:],
                         ["--delete-config", "min.cleanable.dirty.ratio"])
        with self.assertRaises(tool.Refused):
            tool.temp_alter_cmd("__transaction_state")

    def test_dry_run_runs_nothing(self):
        out = io.StringIO()
        with mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(tool.main(["--apply-temp"]), 0)
        body = json.loads(out.getvalue())
        self.assertEqual((body["mode"], list(body["topics"])), ("dry-run", ["__consumer_offsets"]))

    def test_apply_then_remove_touches_only_the_temp_key(self):
        broker = Broker(planned=tool.PLAN)
        code, out, _ = self._main(broker, ["--apply-temp"])
        self.assertEqual(code, 0, out)
        self.assertEqual(json.loads(out)["topics"][0]["mode"], "applied")
        self.assertEqual(broker.configs["__consumer_offsets"]["min.cleanable.dirty.ratio"], "0.05")
        self.assertEqual(broker.configs["__transaction_state"],
                         {**tool.FROZEN_OTHERS["__transaction_state"], **tool.PLAN["__transaction_state"]})
        code, out, _ = self._main(broker, ["--apply-temp"])
        self.assertEqual(json.loads(out)["topics"][0]["mode"], "already_at_state")
        code, out, _ = self._main(broker, ["--remove-temp"])
        self.assertEqual(code, 0, out)
        self.assertEqual(broker.configs["__consumer_offsets"],
                         {**tool.FROZEN_OTHERS["__consumer_offsets"], **tool.PLAN["__consumer_offsets"]})

    def test_temp_at_target_is_not_drift_for_plan_apply_and_rollback(self):
        broker = Broker(extra={"__consumer_offsets": dict(self.TEMP)})
        code, out, _ = self._main(broker, [])
        self.assertEqual(code, 0, out)
        code, out, _ = self._main(broker, ["--rollback"])
        self.assertEqual(code, 0, out)
        self.assertEqual(broker.configs["__consumer_offsets"]["min.cleanable.dirty.ratio"], "0.05")

    def test_foreign_temp_value_is_refused_before_any_alter(self):
        broker = Broker(extra={"__consumer_offsets": {"min.cleanable.dirty.ratio": "0.2"}})
        for argv in (["--apply-temp"], ["--remove-temp"], []):
            code, _, err = self._main(broker, argv)
            self.assertEqual(code, 4, err)
        self.assertNotIn("--alter", [v for _, v in broker.verbs()])

    def test_exclusive_flags(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tool.main(["--apply-temp", "--remove-temp"]), 4)
            self.assertEqual(tool.main(["--apply-temp", "--rollback"]), 4)
            self.assertEqual(tool.main(["--remove-temp", "--observe", "--cert-dir", "/x"]), 4)

    def test_own_lock_does_not_block_a_foreign_one_does(self):
        until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat()
        broker = Broker()
        code, _, err = self._main(broker, ["--apply-temp", "--lock-owner", "claude-kafka-b"],
                                  lock={"owner": "claude-kafka-b", "until_utc": until})
        self.assertEqual(code, 0, err)
        code, _, err = self._main(Broker(), ["--apply-temp", "--lock-owner", "claude-kafka-b"],
                                  lock={"owner": "someone-else", "until_utc": until})
        self.assertEqual(code, 4)

    def test_observe_reports_temp_and_the_removal_point(self):
        for size, recommended in ((1650188753, False), (tool.TEMP_REMOVE_AT_BYTES, True)):
            log_dirs = Observe.LOG_DIRS.replace("1650188753", str(size))

            class ReadOnly(Observe.ReadOnly):
                def run(self, command):
                    if command[0] == "kafka-log-dirs.sh":
                        return CommandResult(0, log_dirs, "")
                    return super().run(command)
            broker = ReadOnly(planned=tool.PLAN, extra={"__consumer_offsets": dict(self.TEMP)})
            out = io.StringIO()
            with mock.patch.object(AdminContainerRunner, "run", side_effect=broker.run), \
                    contextlib.redirect_stdout(out):
                self.assertEqual(tool.main(["--observe", "--cert-dir", "/nonexistent"]), 0)
            body = json.loads(out.getvalue())
            offsets = body["topics"]["__consumer_offsets"]
            self.assertEqual((offsets["temp_state"], offsets["c_state"]), ("PRESENT", "APPLIED"))
            self.assertTrue(offsets["other_overrides_match_frozen"])
            self.assertEqual(body["temp_removal"]["max_size_bytes"], size)
            self.assertIs(body["temp_removal"]["remove_recommended"], recommended)
            self.assertEqual(body["topics"]["__transaction_state"]["temp_state"], "NONE")

    def test_observe_flags_a_foreign_temp_value_as_drift(self):
        broker = Observe.ReadOnly(extra={"__consumer_offsets": {"min.cleanable.dirty.ratio": "0.2"}})
        out = io.StringIO()
        with mock.patch.object(AdminContainerRunner, "run", side_effect=broker.run), contextlib.redirect_stdout(out):
            tool.main(["--observe", "--cert-dir", "/nonexistent"])
        offsets = json.loads(out.getvalue())["topics"]["__consumer_offsets"]
        self.assertEqual(offsets["temp_state"], "FOREIGN")
        self.assertFalse(offsets["other_overrides_match_frozen"])


if __name__ == "__main__":
    unittest.main()

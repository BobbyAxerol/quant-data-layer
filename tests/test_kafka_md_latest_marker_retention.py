"""A' marker-retention tool: one topic, one key, two values from the budget; dry-run by default;
apply reads before, refuses drift / a foreign lock / a budget without the no-tombstone invariant,
alters once, verifies, and rolls back to the pre-A' value."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_md_latest_marker_retention as tool
from scripts.kn_state_topics_packet import BUDGET, AdminContainerRunner, CommandResult

PRE = 604_800_000
TARGET = 21_600_000


def describe(retention: int | str | None, **extra: str) -> str:
    configs = {"cleanup.policy": "compact", "compression.type": "producer", "min.insync.replicas": "2",
               "segment.bytes": "134217728", "segment.ms": "3600000", "unclean.leader.election.enable": "false"}
    if retention is not None:
        configs["delete.retention.ms"] = str(retention)
    configs.update(extra)
    lines = ["Dynamic configs for topic md.latest.v2 are:"]
    lines += [f"  {k}={v} sensitive=false synonyms={{DYNAMIC_TOPIC_CONFIG:{k}={v}}}" for k, v in configs.items()]
    return "\n".join(lines) + "\n"


class Broker:
    """Answers describe/alter for md.latest.v2 like kafka-configs.sh."""

    def __init__(self, retention, **extra):
        self.retention, self.extra, self.calls = retention, extra, []

    def run(self, command):
        self.calls.append(list(command))
        if "--alter" in command:
            self.retention = int(command[-1].split("=", 1)[1])
            return CommandResult(0, "Completed updating config for topic md.latest.v2.\n", "")
        return CommandResult(0, describe(self.retention, **self.extra), "")


def _budget(tmp: str, mutate) -> Path:
    data = json.loads(BUDGET.read_text())
    mutate(data["retention"])
    path = Path(tmp) / "budget.json"
    path.write_text(json.dumps(data))
    return path


class Contract(unittest.TestCase):
    def test_values_come_from_the_budget(self):
        terms = tool.contract()
        self.assertEqual((terms["target"], terms["rollback"]), (TARGET, PRE))
        self.assertEqual(terms["planned_overrides"]["delete.retention.ms"], str(TARGET))
        self.assertGreaterEqual(terms["target"], terms["d21_min_margin"] * terms["d21_replay_pass_upper_bound_ms"])

    def test_refuses_a_budget_without_the_invariant_or_margin(self):
        mutations = [
            lambda r: r.pop("state_topic_tombstones"),
            lambda r: r["state_topic_tombstones"]["topics"]["md.latest.v2"].update(carries_tombstones=True),
            lambda r: r["state_topic_tombstones"]["tombstone_lifetime_ms_applies_to"].append("md.latest.v2"),
            lambda r: r["state_topic_tombstones"]["topics"]["md.latest.v2"].update(
                d21_replay_pass_upper_bound_s=6000),
            lambda r: r["state_topics"]["md.latest.v2"].update({"delete.retention.ms": PRE}),
        ]
        for mutate in mutations:
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(tool.Refused):
                tool.contract(_budget(tmp, mutate))

    def test_alter_is_one_topic_one_key_two_values(self):
        allowed = (TARGET, PRE)
        self.assertEqual(tool.alter_cmd(TARGET, allowed), [
            "kafka-configs.sh", "--entity-type", "topics", "--entity-name", "md.latest.v2", "--alter",
            "--add-config", "delete.retention.ms=21600000"])
        for bad in (86_400_000, 0, True, "21600000"):
            with self.assertRaises(tool.Refused):
                tool.alter_cmd(bad, allowed)
        flat = tool.alter_cmd(PRE, allowed) + tool.describe_cmd()
        for other in ("md.bars.v2", "md.canonical.v2", "brokers", "--delete-config"):
            self.assertNotIn(other, flat)


class DryRun(unittest.TestCase):
    def test_dry_run_prints_apply_and_rollback_and_runs_nothing(self):
        out = io.StringIO()
        with mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(tool.main([]), 0)
        body = json.loads(out.getvalue())
        self.assertEqual(body["mode"], "dry-run")
        self.assertEqual(body["alter"][-1], "delete.retention.ms=21600000")
        self.assertEqual(body["rollback"][-1], "delete.retention.ms=604800000")
        argv = body["docker_argv"]["alter"]
        self.assertEqual(argv[:3], ["docker", "run", "--rm"])
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[argv.index("--command-config") + 1], "/etc/kafka/secrets/admin.properties")

    def test_rollback_only_to_the_pre_a_prime_value(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tool.main(["--rollback-to", "86400000"]), 4)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(tool.main(["--rollback-to", str(PRE)]), 0)
        self.assertEqual(json.loads(out.getvalue())["alter"][-1], "delete.retention.ms=604800000")


class Apply(unittest.TestCase):
    def _main(self, broker, argv):
        out, err = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as certs, mock.patch.object(tool, "LOCK", Path(certs) / "none"), \
                mock.patch.object(AdminContainerRunner, "run", broker.run), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = tool.main([*argv, "--apply", "--cert-dir", certs])
        return code, out.getvalue(), err.getvalue()

    def test_apply_reads_before_alters_once_and_verifies(self):
        broker = Broker(PRE)
        code, out, _ = self._main(broker, [])
        self.assertEqual(code, 0)
        report = json.loads(out)
        self.assertEqual((report["mode"], report["before"], report["after"]), ("applied", PRE, str(TARGET)))
        self.assertEqual(report["rollback"][-1], "delete.retention.ms=604800000")
        self.assertEqual([c[5] for c in broker.calls], ["--describe", "--alter", "--describe"])

    def test_apply_is_idempotent(self):
        broker = Broker(TARGET)
        code, out, _ = self._main(broker, [])
        self.assertEqual((code, json.loads(out)["mode"]), (0, "already_at_value"))
        self.assertNotIn("--alter", [c[5] for c in broker.calls])

    def test_rollback_apply(self):
        broker = Broker(TARGET)
        code, out, _ = self._main(broker, ["--rollback-to", str(PRE)])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["after"], str(PRE))
        self.assertEqual(broker.retention, PRE)

    def test_apply_refuses_drift_or_an_unknown_value_before_any_alter(self):
        for broker in (Broker(PRE, **{"min.insync.replicas": "1"}), Broker(PRE, **{"retention.ms": "1000"}),
                       Broker(86_400_000), Broker(None)):
            code, _, err = self._main(broker, [])
            self.assertEqual(code, 4, err)
            self.assertNotIn("--alter", [c[5] for c in broker.calls])

    def test_apply_refuses_under_a_foreign_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "lock"
            until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
            lock.write_text(json.dumps({"owner": "other", "until_utc": until.isoformat()}))
            err = io.StringIO()
            with mock.patch.object(tool, "LOCK", lock), \
                    mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                    contextlib.redirect_stderr(err):
                self.assertEqual(tool.main(["--apply", "--cert-dir", tmp]), 4)
            self.assertIn("lock", err.getvalue())

    def test_apply_needs_cert_dir(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tool.main(["--apply"]), 4)

    def test_verify_mismatch_is_exit_1(self):
        class Stuck(Broker):
            def run(self, command):
                result = super().run(command)
                if "--alter" in command:
                    self.retention = PRE  # the broker did not take the value
                return result
        code, out, _ = self._main(Stuck(PRE), [])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["after"], str(PRE))


if __name__ == "__main__":
    unittest.main()

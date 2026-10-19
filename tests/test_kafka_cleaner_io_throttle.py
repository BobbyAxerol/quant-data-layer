"""The cleaner-throttle tool builds exactly the cluster-default kafka-configs argv, refuses rates
outside its bounds or below the keep-up floor, and rolls back to the value read before."""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_cleaner_io_throttle as tool
from scripts.kn_state_topics_packet import AdminContainerRunner, CommandResult

DESCRIBE_4MIB = (
    "Default configs for brokers in the cluster are:\n"
    "  log.cleaner.io.max.bytes.per.second=4194304 sensitive=false "
    "synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.cleaner.io.max.bytes.per.second=4194304}\n"
    "  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2}\n"
)
DESCRIBE_NONE = (
    "Default configs for brokers in the cluster are:\n"
    "  min.insync.replicas=2 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=2}\n"
)


class Args(unittest.TestCase):
    def test_alter_is_cluster_default_only_one_key(self):
        self.assertEqual(tool.alter_cmd(3145728), [
            "kafka-configs.sh", "--alter", "--entity-type", "brokers", "--entity-default",
            "--add-config", "log.cleaner.io.max.bytes.per.second=3145728"])
        self.assertNotIn("--entity-name", tool.alter_cmd(3145728))
        self.assertEqual(tool.describe_cmd(),
                         ["kafka-configs.sh", "--describe", "--entity-type", "brokers", "--entity-default"])

    def test_rollback_restores_previous_or_deletes(self):
        self.assertEqual(tool.rollback_cmd(4194304)[-1], "log.cleaner.io.max.bytes.per.second=4194304")
        self.assertEqual(tool.rollback_cmd(None)[-2:], ["--delete-config", "log.cleaner.io.max.bytes.per.second"])

    def test_rate_bounds(self):
        for bad in (0, (1 << 20) - 1, (64 << 20) + 1, True, 2.5):
            with self.assertRaises(tool.Refused):
                tool.alter_cmd(bad)

    def test_admin_runner_argv_is_stable_admin_with_command_config(self):
        argv = AdminContainerRunner(Path("/certs"), image="apache/kafka@sha256:x").argv(tool.alter_cmd(3145728))
        self.assertEqual(argv[:4], ["docker", "run", "--rm", "--network"])
        self.assertIn("/certs:/etc/kafka/secrets:ro", argv)
        self.assertIn("--read-only", argv)
        self.assertEqual(argv[argv.index("--command-config") + 1], "/etc/kafka/secrets/admin.properties")
        self.assertEqual(argv[-2:], ["--add-config", "log.cleaner.io.max.bytes.per.second=3145728"])


class KeepUp(unittest.TestCase):
    def test_measured_rate_keeps_up_and_2mib_does_not(self):
        self.assertAlmostEqual(tool.keep_up(4 << 20)["processed_capacity_mb_s"], 2.055, places=2)
        self.assertTrue(tool.keep_up(4 << 20)["keeps_up"])
        self.assertTrue(tool.keep_up(3 << 20)["keeps_up"])
        self.assertFalse(tool.keep_up(2 << 20)["keeps_up"])

    def test_dry_run_refuses_a_rate_that_cannot_keep_up(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(tool.main(["--rate", str(2 << 20)]), 4)
        self.assertIn("would not keep up", err.getvalue())

    def test_dry_run_prints_and_runs_nothing(self):
        out = io.StringIO()
        with mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(tool.main(["--rate", str(3 << 20)]), 0)
        body = json.loads(out.getvalue())
        self.assertEqual(body["mode"], "dry-run")
        self.assertEqual(body["alter"][-1], "log.cleaner.io.max.bytes.per.second=3145728")


class Parse(unittest.TestCase):
    def test_parse_dynamic_default(self):
        self.assertEqual(tool.parse_dynamic_default(DESCRIBE_4MIB), 4194304)
        self.assertIsNone(tool.parse_dynamic_default(DESCRIBE_NONE))


class Apply(unittest.TestCase):
    def _runner(self, outputs):
        calls = []

        def run(_self, command):
            calls.append(list(command))
            return CommandResult(0, outputs.pop(0), "")
        return calls, run

    def test_apply_reads_before_alters_and_verifies(self):
        calls, run = self._runner([DESCRIBE_4MIB, "Completed updating default config", DESCRIBE_4MIB.replace(
            "4194304", "3145728")])
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as certs, mock.patch.object(tool, "LOCK", Path(certs) / "none"), \
                mock.patch.object(AdminContainerRunner, "run", run), contextlib.redirect_stdout(out):
            self.assertEqual(tool.main(["--rate", str(3 << 20), "--apply", "--cert-dir", certs]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual((report["before"], report["after"]), (4194304, 3145728))
        self.assertEqual(report["rollback"][-1], "log.cleaner.io.max.bytes.per.second=4194304")
        self.assertEqual([c[1] for c in calls], ["--describe", "--alter", "--describe"])

    def test_apply_refuses_under_a_foreign_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "lock"
            until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
            lock.write_text(json.dumps({"owner": "other", "until_utc": until.isoformat()}))
            err = io.StringIO()
            with mock.patch.object(tool, "LOCK", lock), \
                    mock.patch.object(AdminContainerRunner, "run", side_effect=AssertionError("ran")), \
                    contextlib.redirect_stderr(err):
                self.assertEqual(tool.main(["--rate", str(3 << 20), "--apply", "--cert-dir", tmp]), 4)
            self.assertIn("lock", err.getvalue())

    def test_apply_needs_cert_dir(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tool.main(["--rate", str(3 << 20), "--apply"]), 4)


if __name__ == "__main__":
    unittest.main()

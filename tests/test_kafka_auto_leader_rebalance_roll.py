"""Decision B roll tool: packet from docker inspect (only the one env key differs), read-only check, dry-run by
default, one broker per apply, refusal on drift / lock / unhealthy cluster, verification, automatic per-broker
rollback on failure."""

from __future__ import annotations

import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import kafka_auto_leader_rebalance_roll as tool

LINE = "\tTopic: {t}\tPartition: {p}\tLeader: {l}\tReplicas: 1,2,3\tIsr: {i}\tElr: \tLastKnownElr: "


def describe(urp=False):
    rows = [LINE.format(t="__consumer_offsets", p=43, l=3, i="3,1" if urp else "1,2,3"),
            LINE.format(t="md.canonical.v2", p=0, l=1, i="1,2,3")]
    return "Topic: x\tTopicId: A\tPartitionCount: 1\tReplicationFactor: 3\tConfigs: \n" + "\n".join(rows) + "\n"


def inspect_item(broker, extra_env=None, healthy=True, cpus=1750000000):
    node = broker[-1]
    env = [f"KAFKA_NODE_ID={node}", "KAFKA_SSL_PRINCIPAL_MAPPING_RULES=RULE:^CN=([^,]+).*$/$1/,DEFAULT",
           "KAFKA_HEAP_OPTS=-Xms512m -Xmx768m", "PATH=/usr/bin"] + list(extra_env or [])
    return {
        "Id": f"{node}" * 64, "Name": f"/qdl_v2_stable_candidate-{broker}-1", "Image": "sha256:" + "9" * 64,
        "State": {"Running": healthy, "Health": {"Status": "healthy" if healthy else "starting"}},
        "Config": {"Env": env, "Cmd": ["/etc/kafka/docker/run"], "Entrypoint": ["/__cacert_entrypoint.sh"],
                   "User": "appuser", "WorkingDir": "/",
                   "Healthcheck": {"Test": ["CMD-SHELL", "probe"], "Interval": 5000000000, "Retries": 36}},
        "HostConfig": {"ReadonlyRootfs": False, "RestartPolicy": {"Name": "unless-stopped"},
                       "LogConfig": {"Type": "json-file", "Config": {"max-size": "50m"}},
                       "Memory": 1610612736, "NanoCpus": cpus, "SecurityOpt": ["no-new-privileges:true"],
                       "Tmpfs": {"/tmp": "rw,nosuid,nodev,exec,size=32m"}},
        "Mounts": [{"Type": "volume", "Name": f"qdl_v2_stable_candidate_{broker}_data", "Source": "/x",
                    "Destination": "/var/lib/kafka/data", "RW": True},
                   {"Type": "volume", "Name": "a" * 64, "Source": "/y", "Destination": "/mnt/shared/config",
                    "RW": True},
                   {"Type": "bind", "Source": "/certs", "Destination": "/etc/kafka/secrets", "RW": False}],
        "NetworkSettings": {"Networks": {"qdl_v2_stable_candidate_stable_internal": {
            "Aliases": [broker, f"qdl_v2_stable_candidate-{broker}-1"]}}},
    }


def live_inspects(**overrides):
    return {b: overrides.get(b) or inspect_item(b) for b in tool.BROKERS}


def prepared(tmp, inspects=None):
    inspects = inspects or live_inspects()
    out = Path(tmp) / "packet"
    tool.prepare(out, inspect=lambda name: inspects[name.split("-")[1]])
    return out


class FakeLive:
    """Cluster double: recreate swaps the broker's env per the compose file used."""

    def __init__(self, inspects, packet, urp=False, fail_health_on=None, config=None):
        self.inspects, self.packet, self.urp = copy.deepcopy(inspects), packet, urp
        self.fail_health_on, self.config = fail_health_on, config
        self.executed, self.clock = [], 0.0

    def inspect(self, broker):
        return self.inspects[broker]

    def admin(self, command):
        if command[0] == "kafka-topics.sh":
            return describe(self.urp)
        if command[0] == "kafka-metadata-quorum.sh":
            return "ClusterId: x\nLeaderId: 2\nLeaderEpoch: 9\n"
        broker = "kafka" + command[command.index("--entity-name") + 1]
        has = any(e == f"{tool.ENV_KEY}=false" for e in self.inspects[broker]["Config"]["Env"])
        value = self.config or ("false" if has else "true")
        return f"All configs for broker {broker[-1]} are:\n  {tool.CONFIG_KEY}={value} sensitive=false synonyms={{}}\n"

    def execute(self, argv):
        self.executed.append(list(argv))
        if argv[:2] == ["docker", "compose"]:
            broker, which = argv[-1], Path(argv[argv.index("-f") + 1]).name
            env = [e for e in self.inspects[broker]["Config"]["Env"] if not e.startswith(tool.ENV_KEY + "=")]
            if which == "candidate.compose.json":
                env.append(f"{tool.ENV_KEY}=false")
            self.inspects[broker]["Config"]["Env"] = env
            self.inspects[broker]["State"]["Health"]["Status"] = (
                "unhealthy" if self.fail_health_on == which else "healthy")

    def sleep(self, seconds):
        self.clock += seconds

    def now(self):
        return self.clock


class Prepare(unittest.TestCase):
    def test_packet_differs_only_by_the_env_key_and_keeps_inspected_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            receipt = tool.load_packet(out)
            self.assertEqual(receipt["status"], "PREPARED_NOT_APPLIED")
            self.assertEqual(oct((out / "candidate.compose.json").stat().st_mode & 0o777), "0o600")
            cand = json.loads((out / "candidate.compose.json").read_text())
            back = json.loads((out / "rollback.compose.json").read_text())
            for broker in tool.BROKERS:
                c, r = cand["services"][broker], back["services"][broker]
                self.assertEqual(c["environment"].pop(tool.ENV_KEY), "false")
                self.assertEqual(c, r)
                self.assertEqual(r["cpus"], 1.75)  # live docker-update drift, not the old compose file
                self.assertIn("$$1", r["environment"]["KAFKA_SSL_PRINCIPAL_MAPPING_RULES"])
                volumes = {v["target"]: v["source"] for v in r["volumes"]}
                self.assertEqual(volumes["/mnt/shared/config"], "a" * 64)  # anonymous volume kept by name
            self.assertTrue(cand["volumes"]["a" * 64]["external"])

    def test_prepare_refuses_unhealthy_foreign_value_or_overwrite(self):
        for inspects in (live_inspects(kafka2=inspect_item("kafka2", healthy=False)),
                         live_inspects(kafka3=inspect_item("kafka3", [f"{tool.ENV_KEY}=true"]))):
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(tool.Refused):
                prepared(tmp, inspects)
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            with self.assertRaises(tool.Refused):
                tool.prepare(out, inspect=lambda name: inspect_item(name.split("-")[1]))

    def test_tampered_packet_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            path = out / "candidate.compose.json"
            path.chmod(0o600)
            path.write_text(path.read_text().replace('"1.75"', '"4"').replace("1.75", "4.0"))
            with self.assertRaises(tool.Refused):
                tool.load_packet(out)


class Pure(unittest.TestCase):
    def test_health_parsing_and_order(self):
        self.assertTrue(tool.cluster_health(describe())["ok"])
        self.assertEqual(tool.cluster_health(describe(urp=True))["under_replicated"], ["__consumer_offsets-43"])
        self.assertFalse(tool.cluster_health("")["ok"])
        self.assertEqual(tool.quorum_leader("LeaderId: 3\n"), 3)
        self.assertEqual(tool.recommended_order(1), ["kafka2", "kafka3", "kafka1"])
        self.assertEqual(tool.broker_setting(f"  {tool.CONFIG_KEY}=false sensitive=false synonyms={{}}"), "false")

    def test_fingerprint_ignores_only_the_key(self):
        a = tool.fingerprint(inspect_item("kafka1"))
        self.assertEqual(tool.diff(a, tool.fingerprint(inspect_item("kafka1", [f"{tool.ENV_KEY}=false"]))), [])
        self.assertEqual(tool.diff(a, tool.fingerprint(inspect_item("kafka1", cpus=1250000000))), ["nano_cpus"])
        self.assertEqual(tool.diff(a, tool.fingerprint(inspect_item("kafka1", ["X=1"]))), ["env"])


class Main(unittest.TestCase):
    def _run(self, argv, live, lock=None):
        out, err = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as certs:
            lock_path = Path(certs) / "lock"
            if lock:
                lock_path.write_text(json.dumps(lock))
            with mock.patch("scripts.kafka_md_latest_marker_retention.LOCK", lock_path), \
                    contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = tool.main([*argv, "--cert-dir", certs], live_factory=lambda _: live)
        return code, out.getvalue(), err.getvalue()

    def test_dry_run_runs_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            live = FakeLive(live_inspects(), out)
            code, text, _ = self._run(["--roll", str(out), "--broker", "kafka1"], live)
            self.assertEqual(code, 0)
            body = json.loads(text)
            self.assertEqual(body["steps"][0][:4], ["docker", "stop", "--time", "120"])
            self.assertEqual(body["steps"][1][-3:], ["--no-deps", "--force-recreate", "kafka1"])
            self.assertEqual(live.executed, [])

    def test_check_reports_order_with_the_quorum_leader_last(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            code, text, _ = self._run(["--check", str(out)], FakeLive(live_inspects(), out))
            self.assertEqual(code, 0)
            body = json.loads(text)
            self.assertTrue(body["ok"])
            self.assertEqual(body["recommended_order"], ["kafka1", "kafka3", "kafka2"])

    def test_roll_then_rollback_one_broker(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            live = FakeLive(live_inspects(), out)
            code, text, _ = self._run(["--roll", str(out), "--broker", "kafka1", "--apply"], live)
            self.assertEqual(code, 0, text)
            report = json.loads(text)
            self.assertEqual((report["env_after"], report["broker_config"], report["fingerprint_diff"]),
                             ("false", "false", []))
            self.assertEqual([a[1] for a in live.executed], ["stop", "compose"])
            code, _, err = self._run(["--roll", str(out), "--broker", "kafka1", "--apply"], live)
            self.assertEqual(code, 4, err)  # already rolled
            code, text, _ = self._run(["--rollback", str(out), "--broker", "kafka1", "--apply"], live)
            self.assertEqual(code, 0, text)
            self.assertEqual(json.loads(text)["broker_config"], "true")

    def test_refusals_before_any_side_effect(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            cases = [(FakeLive(live_inspects(), out, urp=True), None),
                     (FakeLive(live_inspects(kafka2=inspect_item("kafka2", cpus=1000000000)), out), None),
                     (FakeLive(live_inspects(), out), {"owner": "x", "until_utc": "2999-01-01T00:00:00+00:00"})]
            for live, lock in cases:
                code, _, err = self._run(["--roll", str(out), "--broker", "kafka3", "--apply"], live, lock)
                self.assertEqual(code, 4, err)
                self.assertEqual(live.executed, [])

    def test_failed_roll_rolls_the_broker_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            live = FakeLive(live_inspects(), out, config="true")  # broker ignores the key
            code, text, _ = self._run(["--roll", str(out), "--broker", "kafka2", "--apply"], live)
            self.assertEqual(code, 1)
            report = json.loads(text)
            self.assertEqual(report["auto_rollback"], "RESTORED_VERIFIED")
            self.assertTrue(report["rollback_postconditions"]["ok"])
            self.assertEqual(Path(live.executed[-1][live.executed[-1].index("-f") + 1]).name,
                             "rollback.compose.json")
            self.assertNotIn(f"{tool.ENV_KEY}=false", live.inspects["kafka2"]["Config"]["Env"])

    def test_unhealthy_after_recreate_times_out_then_rolls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            live = FakeLive(live_inspects(), out, fail_health_on="candidate.compose.json")
            code, text, _ = self._run(["--roll", str(out), "--broker", "kafka1", "--apply"], live)
            self.assertEqual(code, 1)
            report = json.loads(text)
            self.assertIn("timeout", report["error"])
            self.assertEqual(report["auto_rollback"], "RESTORED_VERIFIED")

    def test_rollback_that_does_not_restore_is_unverified_exit_3(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = prepared(tmp)
            # the broker ignores the key (forward check fails); after the rollback the ISR never returns
            live = FakeLive(live_inspects(), out, config="true")
            original = live.execute

            def execute(argv):
                original(argv)
                if argv[:2] == ["docker", "compose"] and argv[argv.index("-f") + 1].endswith("rollback.compose.json"):
                    live.urp = True  # ISR never restored after the rollback
            live.execute = execute
            code, text, _ = self._run(["--roll", str(out), "--broker", "kafka1", "--apply"], live)
            self.assertEqual(code, 3)
            report = json.loads(text)
            self.assertEqual(report["auto_rollback"], "BROKER_STARTED_RESTORATION_UNVERIFIED")
            self.assertFalse(report["rollback_postconditions"]["ok"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.phase103_apply_shared_primary_broker_scope import (
    _compose,
    apply_broker_scope,
    broker_scope_commands,
)
from scripts.phase103_prepare_shared_primary_packet import (
    prepare_shared_primary_packet,
)


class SharedPrimaryBrokerScopeTests(unittest.TestCase):
    def prepared(self, root: Path):
        output = root / "packet"
        packet = prepare_shared_primary_packet(
            output_dir=output,
            host_runtime_dir=output / "runtime",
            rust_image_digest="sha256:" + "b" * 64,
            python_image_digest="sha256:" + "c" * 64,
            source_commit="0123456789abcdef",
            actor="BobbyAxerol",
            change_ticket="QDL-PHASE103-TEST",
            observation_seconds=300,
        )
        env_file = root / "stable.env"
        env_file.write_text("QDL_TEST_ONLY=1\n", encoding="ascii")
        return packet, output / "runtime", env_file

    def test_review_is_offline_and_has_only_the_sealed_allowlist(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-broker-") as directory:
            packet, runtime_dir, env_file = self.prepared(Path(directory))
            report = apply_broker_scope(
                packet=packet,
                runtime_dir=runtime_dir,
                env_file=env_file,
                apply=False,
                confirmation=None,
            )
            self.assertEqual(report["status"], "REVIEW_REQUIRED")
            self.assertEqual(report["production_mutations"], 0)
            self.assertEqual(report["command_count"], 9)
            commands = broker_scope_commands(packet)
            self.assertEqual(tuple(tuple(item) for item in report["commands"]), commands)
            rendered = " ".join(" ".join(item) for item in commands)
            self.assertIn("md.raw.realtime.v2", rendered)
            self.assertIn("qdl-v2-realtime-core-v2", rendered)
            self.assertNotIn("production_core", rendered)
            self.assertNotIn("--delete", rendered)
            self.assertNotIn("--reset-offsets", rendered)
            self.assertNotIn("md.raw.stable.v1", rendered)

    def test_apply_requires_exact_token_and_verifies_topic_policy(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-broker-") as directory:
            packet, runtime_dir, env_file = self.prepared(Path(directory))
            with self.assertRaisesRegex(ValueError, "--confirm"):
                apply_broker_scope(
                    packet=packet,
                    runtime_dir=runtime_dir,
                    env_file=env_file,
                    apply=True,
                    confirmation="wrong",
                )

            calls = []

            compose_environments = []

            def fake_kafka(_env_file, command, compose_environment):
                calls.append(tuple(command))
                compose_environments.append(dict(compose_environment))
                if command[0] == "kafka-topics.sh" and "--describe" in command:
                    return (
                        "Topic: md.raw.realtime.v2 PartitionCount: 6 "
                        "ReplicationFactor: 3 Configs: min.insync.replicas=2,"
                        "unclean.leader.election.enable=false,compression.type=producer,"
                        "cleanup.policy=delete"
                    )
                return ""

            with patch(
                "scripts.phase103_apply_shared_primary_broker_scope.kafka",
                fake_kafka,
            ):
                report = apply_broker_scope(
                    packet=packet,
                    runtime_dir=runtime_dir,
                    env_file=env_file,
                    apply=True,
                    confirmation=packet["confirmation_token"],
                )
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["production_mutations"], 9)
            self.assertTrue(report["topic_verified"])
            commands = broker_scope_commands(packet)
            self.assertEqual(calls[0], commands[0])
            self.assertEqual(calls[1][:4], ("kafka-topics.sh", "--describe", "--topic", "md.raw.realtime.v2"))
            self.assertEqual(calls[2:], list(commands[1:]))
            self.assertEqual(
                compose_environments,
                [packet["compose_environment"]] * len(calls),
            )

    def test_existing_topic_policy_mismatch_stops_before_acl_grants(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-broker-") as directory:
            packet, runtime_dir, env_file = self.prepared(Path(directory))
            calls = []

            def fake_kafka(_env_file, command, _compose_environment):
                calls.append(tuple(command))
                if command[0] == "kafka-topics.sh" and "--describe" in command:
                    return "Topic: md.raw.realtime.v2 PartitionCount: 1 ReplicationFactor: 1"
                return ""

            with patch(
                "scripts.phase103_apply_shared_primary_broker_scope.kafka",
                fake_kafka,
            ), self.assertRaisesRegex(RuntimeError, "topic policy"):
                apply_broker_scope(
                    packet=packet,
                    runtime_dir=runtime_dir,
                    env_file=env_file,
                    apply=True,
                    confirmation=packet["confirmation_token"],
                )
            commands = broker_scope_commands(packet)
            self.assertEqual(calls, [commands[0], ("kafka-topics.sh", "--describe", "--topic", "md.raw.realtime.v2")])

    def test_compose_subprocess_receives_only_the_sealed_overlay(self):
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-broker-") as directory:
            packet, _runtime_dir, env_file = self.prepared(Path(directory))
            with patch(
                "scripts.phase103_apply_shared_primary_broker_scope.subprocess.run"
            ) as run:
                _compose(env_file, packet["compose_environment"], "config", "-q")
            environment = run.call_args.kwargs["env"]
            for key, value in packet["compose_environment"].items():
                self.assertEqual(environment[key], value)
            self.assertEqual(run.call_args.kwargs["cwd"], Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    unittest.main()

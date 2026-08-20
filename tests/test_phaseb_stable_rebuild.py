from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.rebuild_v2_stable_projection_cache import (
    CACHE_FILES,
    CANONICAL_TOPIC,
    CONFIRM_TOKEN,
    EXPECTED_CANONICAL_PARTITIONS,
    MAX_ACCEPTED_LAG,
    PROJECT_NAME,
    PROJECTOR_GROUP,
    QUERY_SERVICES,
    REPLAY_TAIL_RECORDS_PER_PARTITION,
    STOP_SERVICES,
    STREAM_SERVICES,
    _env_value,
    _reset_projector_to_bounded_tail,
    _stable_client_ssl_context,
    _start_services,
    _validate_project,
    compose_command,
    execute_rebuild,
    lag_sample_acceptable,
    parse_canonical_lag,
    rebuild_plan,
    require_authorization,
)


class StableProjectionCacheRebuildTests(unittest.TestCase):
    def test_plan_is_exact_isolated_and_v1_safe(self):
        env = Path("/tmp/stable.env")
        plan = rebuild_plan(env)
        self.assertEqual(plan["project"], PROJECT_NAME)
        self.assertEqual(plan["stop_services"], list(STOP_SERVICES))
        self.assertEqual(plan["delete_files"], list(CACHE_FILES))
        self.assertEqual(plan["reset_group"], PROJECTOR_GROUP)
        self.assertEqual(plan["reset_topic"], CANONICAL_TOPIC)
        self.assertEqual(
            plan["replay_tail_records_per_partition"],
            REPLAY_TAIL_RECORDS_PER_PARTITION,
        )
        self.assertEqual(
            plan["lag_gate"]["expected_partitions"],
            EXPECTED_CANONICAL_PARTITIONS,
        )
        self.assertEqual(
            plan["lag_gate"]["max_total_records"],
            MAX_ACCEPTED_LAG,
        )
        self.assertEqual(
            plan["start_order"],
            [list(STREAM_SERVICES), ["projector_v2"], list(QUERY_SERVICES)],
        )
        self.assertFalse(plan["touches_v1"])
        self.assertFalse(plan["apply"])

    def test_apply_requires_exact_confirmation(self):
        require_authorization(apply=False, confirm=None)
        with self.assertRaisesRegex(ValueError, CONFIRM_TOKEN):
            require_authorization(apply=True, confirm=None)
        with self.assertRaisesRegex(ValueError, CONFIRM_TOKEN):
            require_authorization(apply=True, confirm="WRONG")
        require_authorization(apply=True, confirm=CONFIRM_TOKEN)

    def test_tls_identity_is_loaded_from_exact_env_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = root / "identity"
            identity.mkdir()
            env = root / "stable.env"
            env.write_text(
                f"QDL_STABLE_TRADING_SYSTEM_CERT_DIR={identity}\n"
            )
            self.assertEqual(
                _env_value(env, "QDL_STABLE_TRADING_SYSTEM_CERT_DIR"),
                str(identity),
            )
            with self.assertRaises(FileNotFoundError):
                _stable_client_ssl_context(env)
            env.write_text(
                "QDL_STABLE_TRADING_SYSTEM_CERT_DIR=/first\n"
                "QDL_STABLE_TRADING_SYSTEM_CERT_DIR=/second\n"
            )
            with self.assertRaisesRegex(ValueError, "exactly one"):
                _env_value(env, "QDL_STABLE_TRADING_SYSTEM_CERT_DIR")

    def test_compose_command_is_pinned_to_stable_manifest(self):
        command = compose_command(Path("/tmp/stable.env"), "config")
        self.assertEqual(command[:2], ["docker", "compose"])
        self.assertIn("docker-compose.v2-stable.yml", command[5])
        self.assertEqual(command[-1], "config")
        self.assertNotIn("docker-compose.yml", command[5])

    def test_recovery_starts_roles_without_dependency_traversal(self):
        env = Path("/tmp/stable.env")
        with patch(
            "scripts.rebuild_v2_stable_projection_cache._compose"
        ) as compose:
            _start_services(env, "stream_v2_active", "stream_v2_passive")
        compose.assert_called_once_with(
            env,
            "up",
            "-d",
            "--no-deps",
            "stream_v2_active",
            "stream_v2_passive",
        )
        with self.assertRaisesRegex(ValueError, "at least one"):
            _start_services(env)

    def test_bounded_tail_reset_matches_spool_partition_retention(self):
        env = Path("/tmp/stable.env")
        with patch(
            "scripts.rebuild_v2_stable_projection_cache._kafka_group"
        ) as kafka_group:
            _reset_projector_to_bounded_tail(env)
        kafka_group.assert_any_call(
            env,
            "--group", PROJECTOR_GROUP,
            "--topic", CANONICAL_TOPIC,
            "--reset-offsets", "--to-latest", "--execute",
        )
        kafka_group.assert_any_call(
            env,
            "--group", PROJECTOR_GROUP,
            "--topic", CANONICAL_TOPIC,
            "--reset-offsets", "--shift-by",
            f"-{REPLAY_TAIL_RECORDS_PER_PARTITION}", "--execute",
        )
        self.assertEqual(kafka_group.call_count, 2)

    def test_lag_parser_requires_real_canonical_partitions(self):
        output = """GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
stable-projector-v1 md.canonical.v2 0 10 12 2 - - -
stable-projector-v1 md.canonical.v2 1 20 20 0 - - -
stable-projector-v1 another.topic 2 0 99 99 - - -
"""
        self.assertEqual(parse_canonical_lag(output), (2, 2))
        with self.assertRaisesRegex(RuntimeError, "no partitions"):
            parse_canonical_lag("GROUP TOPIC PARTITION")

    def test_lag_gate_requires_all_partitions_and_fixed_bound(self):
        self.assertTrue(
            lag_sample_acceptable(
                MAX_ACCEPTED_LAG, EXPECTED_CANONICAL_PARTITIONS
            )
        )
        self.assertFalse(
            lag_sample_acceptable(
                MAX_ACCEPTED_LAG + 1, EXPECTED_CANONICAL_PARTITIONS
            )
        )
        self.assertFalse(
            lag_sample_acceptable(
                0, EXPECTED_CANONICAL_PARTITIONS - 1
            )
        )

    def test_wrong_compose_project_fails_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / "stable.env"
            env.write_text("placeholder=true\n")
            completed = subprocess.CompletedProcess(
                args=[], returncode=0, stdout='{"name":"wrong"}', stderr=""
            )
            with patch(
                "scripts.rebuild_v2_stable_projection_cache._compose",
                return_value=completed,
            ):
                with self.assertRaisesRegex(RuntimeError, "isolated stable candidate"):
                    _validate_project(env)

    def test_running_cache_user_blocks_before_delete_or_flush(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / "stable.env"
            env.write_text("placeholder=true\n")
            calls = []

            def fake_validate(_env):
                return None

            def fake_compose(_env, *arguments, **_kwargs):
                calls.append(arguments)
                if arguments[:4] == ("ps", "--services", "--status", "running"):
                    return subprocess.CompletedProcess(
                        args=[], returncode=0, stdout="projector_v2\n", stderr=""
                    )
                return subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="", stderr=""
                )

            with (
                patch(
                    "scripts.rebuild_v2_stable_projection_cache._validate_project",
                    fake_validate,
                ),
                patch(
                    "scripts.rebuild_v2_stable_projection_cache._compose",
                    fake_compose,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "still running"):
                    execute_rebuild(env, timeout_seconds=10)
            flattened = " ".join(" ".join(call) for call in calls)
            self.assertNotIn("FLUSHDB", flattened)
            self.assertNotIn("canonical-cache.sqlite3", flattened)


if __name__ == "__main__":
    unittest.main()

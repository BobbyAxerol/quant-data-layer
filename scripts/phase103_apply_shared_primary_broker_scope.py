#!/usr/bin/env python3
"""Apply only the sealed Phase 10.3 shared-primary Kafka scope.

Without ``--apply`` this is an offline packet/bundle review. The apply path is
intentionally limited to one topic plus the exact producer/core ACLs. It never
resets offsets, seeks, deletes topics, flushes state, or starts a service.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.phase103_packet_contract import (
    COMPOSE_ENVIRONMENT_KEYS,
    SHARED_REALTIME_CORE_GROUP_ID,
    SHARED_REALTIME_CORE_ID_PREFIX,
    validate_prepared_shared_primary_bundle,
)


COMPOSE = ROOT / "docker-compose.v2-stable.yml"
BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
ADMIN_CONFIG = "/etc/kafka/secrets/admin.properties"
CANONICAL_TOPIC = "md.canonical.v2"
QUARANTINE_TOPIC = "md.quarantine.stable.v1"


def _load_packet(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"packet cannot be read as JSON: {path}") from error
    if not isinstance(payload, dict):
        raise ValueError("packet root must be an object")
    return payload


def _topic_command(topic: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        "kafka-topics.sh",
        "--create",
        "--if-not-exists",
        "--topic",
        str(topic["name"]),
        "--partitions",
        str(topic["partitions"]),
        "--replication-factor",
        str(topic["replication_factor"]),
        "--config",
        f"min.insync.replicas={topic['min_insync_replicas']}",
        "--config",
        "unclean.leader.election.enable=false",
        "--config",
        "compression.type=producer",
        "--config",
        "cleanup.policy=delete",
    )


def _acl_command(
    principal: str,
    operations: Sequence[str],
    resource: Sequence[str],
) -> tuple[str, ...]:
    command = [
        "kafka-acls.sh",
        "--add",
        "--allow-principal",
        f"User:{principal}",
    ]
    for operation in operations:
        command.extend(("--operation", operation))
    command.extend(resource)
    return tuple(command)


def broker_scope_commands(packet: Mapping[str, Any]) -> tuple[tuple[str, ...], ...]:
    """Return the immutable allowlist of mutation commands, without running it."""
    topic = packet["deployment"]["topic"]
    intent = packet["deployment"]["acl_intent"]
    raw_topic = str(topic["name"])
    producer = str(intent["producer"])
    core = str(intent["core"])
    core_group = str(intent["core_group_id"])
    transactional_prefix = str(intent["core_transactional_id_prefix"])
    if (
        core_group != SHARED_REALTIME_CORE_GROUP_ID
        or transactional_prefix != f"{SHARED_REALTIME_CORE_ID_PREFIX}-"
    ):
        raise ValueError("packet core identity is not the shared realtime core")
    return (
        _topic_command(topic),
        _acl_command(producer, ("WRITE", "DESCRIBE"), ("--topic", raw_topic)),
        _acl_command(producer, ("IdempotentWrite",), ("--cluster",)),
        _acl_command(core, ("READ", "DESCRIBE"), ("--topic", raw_topic)),
        _acl_command(core, ("READ",), ("--group", core_group)),
        _acl_command(core, ("WRITE", "DESCRIBE"), ("--topic", CANONICAL_TOPIC)),
        _acl_command(core, ("WRITE", "DESCRIBE"), ("--topic", QUARANTINE_TOPIC)),
        _acl_command(
            core,
            ("WRITE", "DESCRIBE"),
            (
                "--transactional-id",
                transactional_prefix,
                "--resource-pattern-type",
                "prefixed",
            ),
        ),
        _acl_command(core, ("IdempotentWrite",), ("--cluster",)),
    )


def _sealed_compose_environment(packet: Mapping[str, Any]) -> dict[str, str]:
    """Return the exact non-secret Compose overlay sealed in the packet."""
    raw = packet.get("compose_environment")
    if not isinstance(raw, Mapping):
        raise ValueError("shared primary packet Compose environment is invalid")
    values: dict[str, str] = {}
    for key in COMPOSE_ENVIRONMENT_KEYS:
        value = raw.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError("shared primary packet Compose environment is invalid")
        values[key] = value
    return values


def _compose(
    env_file: Path,
    compose_environment: Mapping[str, str],
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(compose_environment)
    return subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_file),
            "-f",
            str(COMPOSE),
            "--profile",
            "stable-admin",
            *arguments,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
        env=environment,
    )


def kafka(
    env_file: Path,
    command: Sequence[str],
    compose_environment: Mapping[str, str],
) -> str:
    result = _compose(
        env_file,
        compose_environment,
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        f"/opt/kafka/bin/{command[0]}",
        "stable_admin",
        "--bootstrap-server",
        BOOTSTRAP,
        "--command-config",
        ADMIN_CONFIG,
        *command[1:],
    )
    return result.stdout


def _topic_is_exact(described: str, topic: Mapping[str, Any]) -> bool:
    expected = (
        f"Topic: {topic['name']}",
        f"PartitionCount: {topic['partitions']}",
        f"ReplicationFactor: {topic['replication_factor']}",
        f"min.insync.replicas={topic['min_insync_replicas']}",
        "unclean.leader.election.enable=false",
        "compression.type=producer",
        "cleanup.policy=delete",
    )
    return all(item in described for item in expected)


def apply_broker_scope(
    *,
    packet: Mapping[str, Any],
    runtime_dir: Path,
    env_file: Path,
    apply: bool,
    confirmation: str | None,
) -> dict[str, Any]:
    """Validate then optionally execute only the packet's broker allowlist."""
    bundle = validate_prepared_shared_primary_bundle(packet, runtime_dir=runtime_dir)
    if not env_file.is_file():
        raise ValueError(f"stable environment file is missing: {env_file}")
    compose_environment = _sealed_compose_environment(packet)
    commands = broker_scope_commands(packet)
    report: dict[str, Any] = {
        "schema": "qdl.v2.phase103-shared-primary-broker-scope.v1",
        "packet_sha256": bundle["packet_sha256"],
        "command_count": len(commands),
        "commands": [list(item) for item in commands],
        "apply_requested": apply,
        "production_mutations": 0,
        "forbidden_operations": [
            "kafka_offset_reset",
            "kafka_seek",
            "kafka_topic_delete",
            "redis_flush",
            "sqlite_delete",
            "service_start",
            "v1_restart",
        ],
    }
    if not apply:
        report["status"] = "REVIEW_REQUIRED"
        return report
    if confirmation != packet["confirmation_token"]:
        raise ValueError("--confirm must equal the sealed packet confirmation token")
    topic_command, *acl_commands = commands
    kafka(env_file, topic_command, compose_environment)
    topic = packet["deployment"]["topic"]
    described = kafka(
        env_file,
        ("kafka-topics.sh", "--describe", "--topic", topic["name"]),
        compose_environment,
    )
    if not _topic_is_exact(described, topic):
        raise RuntimeError("shared primary raw topic policy does not match sealed packet")
    for command in acl_commands:
        kafka(env_file, command, compose_environment)
    report.update({
        "status": "PASS",
        "production_mutations": len(commands),
        "topic_verified": True,
    })
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    report = apply_broker_scope(
        packet=_load_packet(args.packet),
        runtime_dir=args.runtime_dir,
        env_file=args.env_file,
        apply=args.apply,
        confirmation=args.confirm,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

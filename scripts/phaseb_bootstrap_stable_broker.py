#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.v2-stable.yml"
BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
ADMIN_CONFIG = "/etc/kafka/secrets/admin.properties"
V2_REALTIME_RAW_TOPIC = "md.raw.realtime.v2"
LEGACY_RAW_TOPIC = "md.raw.stable.v1"
TOPIC_POLICIES = {
    # The V2 Rust primary ingress is one shared multi-venue topic. It is
    # intentionally separate from the retained legacy raw topic so an old
    # broad-universe producer cannot starve or silently filter V2 demand.
    V2_REALTIME_RAW_TOPIC: "delete",
    LEGACY_RAW_TOPIC: "delete",
    "md.canonical.v2": "delete",
    "md.quarantine.stable.v1": "delete",
    "qdl.authority.v1": "compact",
    "qdl.target-checkpoint.v1": "compact",
    "md.canary.canonical.v2": "delete",
    "md.projector.public.v2": "delete",
    "md.projector.legacy.v1": "delete",
}
TOPICS = tuple(TOPIC_POLICIES)

# Exact Kafka ACL namespaces used by bounded R1/R2 control-plane readers.
# They deliberately do not overlap with the active generic-core group.
CORE_GROUP_PREFIXES = (
    "qdl-v2-production-core-v1-",
    "qdl-v2-production-core-r1-",
)
READ_ONLY_AUDIT_GROUP_PREFIXES = (
    "qdl-r1-reference-parity-",
    "qdl-c40-handoff-",
)
READ_ONLY_AUDIT_EXTRA_TOPICS = (
    "md.canary.canonical.v2",
    "qdl.target-checkpoint.v1",
)


def compose(env_file: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker", "compose", "--env-file", str(env_file),
            "-f", str(COMPOSE), "--profile", "stable-admin", *arguments,
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )


def kafka(env_file: Path, executable: str, *arguments: str) -> str:
    result = compose(
        env_file,
        "run", "--rm", "--no-deps", "--entrypoint", f"/opt/kafka/bin/{executable}",
        "stable_admin",
        "--bootstrap-server", BOOTSTRAP,
        "--command-config", ADMIN_CONFIG,
        *arguments,
    )
    return result.stdout


def add_acl(
    env_file: Path,
    principal: str,
    operations: tuple[str, ...],
    resource_arguments: tuple[str, ...],
) -> None:
    arguments = [
        "--add", "--allow-principal", f"User:{principal}",
    ]
    for operation in operations:
        arguments.extend(("--operation", operation))
    arguments.extend(resource_arguments)
    kafka(env_file, "kafka-acls.sh", *arguments)


def bootstrap(env_file: Path) -> dict[str, object]:
    for topic, cleanup_policy in TOPIC_POLICIES.items():
        kafka(
            env_file,
            "kafka-topics.sh",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "6",
            "--replication-factor", "3",
            "--config", "min.insync.replicas=2",
            "--config", "unclean.leader.election.enable=false",
            "--config", "compression.type=producer",
            "--config", f"cleanup.policy={cleanup_policy}",
        )

    add_acl(
        env_file, "phase8-producer", ("WRITE", "DESCRIBE"),
        ("--topic", TOPICS[0]),
    )
    add_acl(
        env_file, "phase8-producer", ("IdempotentWrite",),
        ("--cluster",),
    )
    for topic, operations in (
        (TOPICS[0], ("READ", "DESCRIBE")),
        (TOPICS[1], ("WRITE", "DESCRIBE")),
        (TOPICS[2], ("WRITE", "DESCRIBE")),
    ):
        add_acl(env_file, "phase8-core", operations, ("--topic", topic))
    add_acl(
        env_file, "phase8-core", ("READ",),
        ("--group", "qdl-v2-stable-core-v1"),
    )
    for topic in ("qdl.authority.v1", "qdl.target-checkpoint.v1"):
        add_acl(
            env_file, "phase8-core", ("READ", "DESCRIBE"),
            ("--topic", topic),
        )
    for topic in (
        "qdl.target-checkpoint.v1",
        "md.canary.canonical.v2",
        "md.projector.public.v2",
        "md.projector.legacy.v1",
    ):
        add_acl(
            env_file, "phase8-core", ("WRITE", "DESCRIBE"),
            ("--topic", topic),
        )
    for group_prefix in CORE_GROUP_PREFIXES:
        add_acl(
            env_file, "phase8-core", ("READ",),
            (
                "--group", group_prefix,
                "--resource-pattern-type", "prefixed",
            ),
        )
    for topic in READ_ONLY_AUDIT_EXTRA_TOPICS:
        add_acl(
            env_file, "phase8-consumer", ("READ", "DESCRIBE"),
            ("--topic", topic),
        )
    for group_prefix in READ_ONLY_AUDIT_GROUP_PREFIXES:
        add_acl(
            env_file, "phase8-consumer", ("READ",),
            (
                "--group", group_prefix,
                "--resource-pattern-type", "prefixed",
            ),
        )
    add_acl(env_file, "phase8-core", ("IdempotentWrite",), ("--cluster",))
    for transactional_prefix in (
        "qdl-v2-stable-core-", "qdl-v2-production-core-"
    ):
        add_acl(
            env_file, "phase8-core", ("WRITE", "DESCRIBE"),
            (
                "--transactional-id", transactional_prefix,
                "--resource-pattern-type", "prefixed",
            ),
        )
    add_acl(
        env_file, "stable-authority-dispatcher", ("WRITE", "DESCRIBE"),
        ("--topic", "qdl.authority.v1"),
    )
    add_acl(
        env_file, "stable-authority-dispatcher", ("IdempotentWrite",),
        ("--cluster",),
    )
    for topic in (TOPICS[0], TOPICS[1]):
        add_acl(
            env_file, "phase8-consumer", ("READ", "DESCRIBE"),
            ("--topic", topic),
        )
    add_acl(
        env_file, "phase8-consumer", ("READ",),
        ("--group", "stable-projector-v1"),
    )

    described = kafka(
        env_file, "kafka-topics.sh", "--describe",
        "--topic", V2_REALTIME_RAW_TOPIC,
    )
    if (
        "ReplicationFactor: 3" not in described
        or "PartitionCount: 6" not in described
        or "min.insync.replicas=2" not in described
    ):
        raise RuntimeError("stable Kafka topic policy verification failed")
    return {
        "schema": "qdl.v2.stable-broker-bootstrap.v1",
        "status": "PASS",
        "topics": list(TOPICS),
        "partitions": 6,
        "replication_factor": 3,
        "min_insync_replicas": 2,
        "tls_client_auth": "required",
        "topic_policies": dict(TOPIC_POLICIES),
        "principals": [
            "phase8-producer", "phase8-core", "phase8-consumer",
            "stable-authority-dispatcher",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args()
    result = bootstrap(args.env_file)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

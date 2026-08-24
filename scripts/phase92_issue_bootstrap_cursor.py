#!/usr/bin/env python3
"""Read Kafka tails and issue a signed, generation-bound Phase 9.2 cursor.

The command is dry-run by default. It never calls a Kafka offset-reset API.
`--apply` atomically writes only the signed cursor file after proving the exact
new consumer group has no committed offsets for every signed raw partition.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.control.phase92_bootstrap import (
    BootstrapPartition,
    BootstrapScope,
    build_signed_cursor,
    envelope_sha256,
    verify_signed_cursor,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope, StableAcquisitionPlan


CONFIRM = "ISSUE_QDL_PHASE92_BOOTSTRAP_CURSOR"


def load_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise ValueError(f"stable env line has no '=': {line!r}")
        key, value = stripped.split("=", 1)
        if not key or not key.replace("_", "").isalnum():
            raise ValueError(f"stable env variable is invalid: {key!r}")
        value = value.strip()
        if len(value) >= 2 and value[:1] == value[-1:] and value[:1] in {"'", '"'}:
            value = value[1:-1]
        result[key] = value
    return result


def _required(env: dict[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        raise ValueError(f"stable env is missing {key}")
    return value


def _validate_digest(value: str, name: str) -> str:
    if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return value


def _tls_config(env: dict[str, str], *, group_id: str) -> dict[str, str]:
    root = _required(env, "QDL_STABLE_CORE_CERT_DIR")
    return {
        "bootstrap.servers": _required(env, "QDL_PHASE92_KAFKA_BOOTSTRAP_SERVERS")
        if env.get("QDL_PHASE92_KAFKA_BOOTSTRAP_SERVERS")
        else "kafka1:9092,kafka2:9092,kafka3:9092",
        "group.id": group_id,
        "client.id": f"phase92-bootstrap-readonly-{group_id}",
        "security.protocol": "ssl",
        "ssl.ca.location": str(Path(root) / "ca.crt"),
        "ssl.certificate.location": str(Path(root) / "client.crt"),
        "ssl.key.location": str(Path(root) / "client.key"),
        "enable.auto.commit": "false",
        "enable.auto.offset.store": "false",
        "auto.offset.reset": "error",
        "isolation.level": "read_committed",
    }


def collect_tails(
    *, env: dict[str, str], group_id: str, raw_topic: str
) -> list[BootstrapPartition]:
    """Read high watermarks and assert the fresh group has no commits.

    This never subscribes or polls, so it never joins the group and never
    commits an offset. The temporary Consumer is only a TLS metadata client.
    """

    try:
        from confluent_kafka import Consumer, TopicPartition
    except ImportError as error:  # pragma: no cover - deployment image gate
        raise RuntimeError("confluent_kafka is required for real cursor issuance") from error
    consumer = Consumer(_tls_config(env, group_id=group_id))
    try:
        metadata = consumer.list_topics(raw_topic, timeout=10)
        topic = metadata.topics.get(raw_topic)
        if topic is None or topic.error is not None or not topic.partitions:
            raise RuntimeError("raw topic metadata is unavailable or has no partitions")
        requested = [TopicPartition(raw_topic, partition) for partition in sorted(topic.partitions)]
        committed = consumer.committed(requested, timeout=10)
        if len(committed) != len(requested):
            raise RuntimeError("Kafka committed-offset response is incomplete")
        tails: list[BootstrapPartition] = []
        for item, stored in zip(requested, committed, strict=True):
            if stored.offset is not None and stored.offset >= 0:
                raise RuntimeError(
                    f"refusing cursor issue: group {group_id} already has committed "
                    f"offset for {raw_topic}[{item.partition}]"
                )
            low, high = consumer.get_watermark_offsets(item, timeout=10)
            if low < 0 or high < low:
                raise RuntimeError(f"Kafka watermark is invalid for {raw_topic}[{item.partition}]")
            tails.append(BootstrapPartition(raw_topic, item.partition, high))
        return tails
    finally:
        consumer.close()


def prepare(
    *, env_file: Path, candidate_digest: str, generation: int, ttl_seconds: int,
    collector=collect_tails, now_ns=time.time_ns,
) -> tuple[dict[str, Any], dict[str, str], Path]:
    env = load_env(env_file)
    runtime_dir = Path(_required(env, "QDL_STABLE_RUNTIME_DIR"))
    core = json.loads((runtime_dir / "production-core-001.json").read_text(encoding="utf-8"))
    raw_topics = core.get("topics", {}).get("raw_inputs")
    if not isinstance(raw_topics, list) or len(raw_topics) != 1 or not isinstance(raw_topics[0], str):
        raise ValueError("production core must declare exactly one raw topic")
    group_base = _required(env, "QDL_PHASE92_BOOTSTRAP_GROUP_ID")
    if group_base.endswith("-phase92-raw"):
        raise ValueError("bootstrap group base must not include the phase92 suffix")
    group_id = f"{group_base}-phase92-raw"
    scope = BootstrapScope(
        consumer_group_id=group_id,
        raw_topics=tuple(raw_topics),
        promotion_scope_digest=_validate_digest(str(core.get("promotion_scope_digest", "")), "promotion_scope_digest"),
        candidate_digest=_validate_digest(candidate_digest, "candidate_digest"),
        partition_plan_epoch=int(core.get("partition_plan_epoch", 0)),
    )
    keyring = json.loads(_required(env, "QDL_PHASE92_BOOTSTRAP_CURSOR_KEYS_JSON"))
    if not isinstance(keyring, dict):
        raise ValueError("QDL_PHASE92_BOOTSTRAP_CURSOR_KEYS_JSON must be an object")
    key_id = _required(env, "QDL_PHASE92_BOOTSTRAP_CURSOR_ACTIVE_KEY_ID")
    tails = collector(env=env, group_id=group_id, raw_topic=scope.raw_topics[0])
    envelope = build_signed_cursor(
        key_id=key_id,
        keyring={str(key): str(value) for key, value in keyring.items()},
        scope=scope,
        partitions=tails,
        generation=generation,
        issued_at_ns=now_ns(),
        ttl_seconds=ttl_seconds,
    )
    payload = verify_signed_cursor(
        envelope,
        keyring={str(key): str(value) for key, value in keyring.items()},
        scope=scope,
    )
    target = runtime_dir / "production-bootstrap.json"
    report = {
        "schema": "qdl.phase92.bootstrap-cursor-issuance.v1",
        "status": "PASS",
        "cursor_sha256": envelope_sha256(envelope),
        "cursor_id": payload["cursor_id"],
        "generation": generation,
        "group_id": group_id,
        "raw_topic": scope.raw_topics[0],
        "partition_count": len(tails),
        "promotion_scope_digest": scope.promotion_scope_digest,
        "candidate_digest": scope.candidate_digest,
        "partition_plan_epoch": scope.partition_plan_epoch,
        "issued_at_ns": payload["issued_at_ns"],
        "expires_at_ns": payload["expires_at_ns"],
        "target": str(target),
        "kafka_offset_mutations": 0,
    }
    return report, envelope, target


def atomic_write(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".production-bootstrap-", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        # The HMAC key remains in stable.env (0600); the signed envelope is a
        # non-secret, read-only runtime input for qdl-production-core (UID 10001).
        temporary_path.chmod(0o644)
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--candidate-digest", required=True)
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--ttl-seconds", type=int, default=1_800)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm")
    args = parser.parse_args()
    report, envelope, target = prepare(
        env_file=args.env_file,
        candidate_digest=args.candidate_digest,
        generation=args.generation,
        ttl_seconds=args.ttl_seconds,
    )
    if not args.apply:
        print(json.dumps({**report, "confirmation_token": CONFIRM}, sort_keys=True))
        return 0
    if args.confirm != CONFIRM:
        raise RuntimeError("bootstrap cursor confirmation token differs")
    atomic_write(target, envelope)
    print(json.dumps({**report, "status": "APPLIED", "confirmation_token": CONFIRM}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

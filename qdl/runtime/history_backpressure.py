"""Downstream backpressure for venue history fills (KN-4 D47-3).

Purpose: a full provider re-bootstrap once left a 601,622-record projector
backlog. History (never live data) is admitted only while every pipeline stage
keeps up: per stage, records its consumer group has not yet consumed on its
input topic (end offset - committed offset, summed over partitions). The
stages are the canonical core (raw topic), projector stage A (canonical topic,
``kn-projector-v3-a``) and stage B (bars topic, ``kn-projector-v3-b``). Above a
stage's limit the gate closes; the edge stops fetching more history until the
backlog drains, and resumes from its checkpoint.

Boundary: read-only Kafka metadata (group offsets, watermarks); never joins or
commits a group; no venue call.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class Stage:
    name: str
    group: str
    topic: str
    limit_records: int


def stage_backlog(committed: Mapping[int, int], ends: Mapping[int, int]) -> int:
    """Unconsumed records of one group on one topic; a partition never committed
    counts from its beginning (0)."""

    return sum(max(0, end - max(0, committed.get(partition, 0))) for partition, end in ends.items())


class KafkaStageBacklog:
    def __init__(self, stages: Sequence[Stage], *, read_committed: Callable[[str, str], Mapping[int, int]],
                 read_ends: Callable[[str], Mapping[int, int]]) -> None:
        if not stages or any(stage.limit_records < 1 for stage in stages):
            raise ValueError("backpressure needs stages with positive limits")
        self.stages = tuple(stages)
        self._read_committed = read_committed
        self._read_ends = read_ends

    def backlog(self) -> dict[str, int]:
        return {stage.name: stage_backlog(self._read_committed(stage.group, stage.topic),
                                          self._read_ends(stage.topic)) for stage in self.stages}

    def __call__(self) -> tuple[bool, dict[str, Any]]:
        """The edge's history gate: (admitted, detail). Unreadable -> closed."""

        try:
            backlog = self.backlog()
        except Exception as error:  # noqa: BLE001 - an unknown backlog never admits history
            return False, {"error": f"{type(error).__name__}: {error}"[:200]}
        over = {stage.name: backlog[stage.name] for stage in self.stages if backlog[stage.name] > stage.limit_records}
        return not over, {"backlog": backlog, "over": over}


def kafka_backlog_from_environment(environ: Mapping[str, str], stages: Sequence[Stage]) -> KafkaStageBacklog:
    """Production metadata reads share the BAR publisher's mTLS identity."""
    root = environ.get("QDL_KAFKA_CERT_ROOT", "").strip()
    if not root:
        raise ValueError("history backpressure requires the Kafka TLS cert root")
    security = {"security.protocol": "ssl"}
    for option, filename in (("ssl.ca.location", "ca.crt"),
                             ("ssl.certificate.location", "client.crt"),
                             ("ssl.key.location", "client.key")):
        path = Path(root) / filename
        if not path.is_file():
            raise ValueError(f"history backpressure TLS file unavailable: {path}")
        security[option] = str(path)
    return kafka_backlog_from_config(environ["QDL_STABLE_BAR_BACKPRESSURE_BOOTSTRAP"], stages, security)


def kafka_backlog_from_config(bootstrap: str, stages: Sequence[Stage],
                              security: Mapping[str, str] | None = None) -> KafkaStageBacklog:
    """Wire the gate to a broker with confluent-kafka (admin group offsets + watermarks)."""

    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    config = {"bootstrap.servers": bootstrap, **dict(security or {})}
    admin = AdminClient(config)
    metadata = Consumer({**config, "group.id": "kn-history-backpressure-metadata", "enable.auto.commit": False})
    partitions: dict[str, list[int]] = {}

    def topic_partitions(topic: str) -> list[int]:
        if topic not in partitions:
            partitions[topic] = sorted(metadata.list_topics(topic, timeout=10).topics[topic].partitions)
        return partitions[topic]

    def read_committed(group: str, topic: str) -> dict[int, int]:
        request = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in topic_partitions(topic)])
        result = admin.list_consumer_group_offsets([request])[group].result(10)
        return {item.partition: item.offset for item in result.topic_partitions if item.offset >= 0}

    def read_ends(topic: str) -> dict[int, int]:
        return {p: metadata.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
                for p in topic_partitions(topic)}

    return KafkaStageBacklog(stages, read_committed=read_committed, read_ends=read_ends)

#!/usr/bin/env python3
"""Read a bounded Kafka tail and prove raw-ingress scope without mutation.

The inspector intentionally reports only aggregate binding counters, offsets and
SHA-256 identifiers. It never commits offsets, writes a topic, or emits raw
provider frames. It is used before a V2 Rust-primary handoff to prove that a
dedicated realtime raw topic contains the declared acquisition scope.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


SCHEMA = "qdl.v2.realtime-raw-scope-evidence.v1"


def _sha256(value: str | bytes) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


@dataclass(slots=True)
class ScopeTally:
    """Bounded aggregate state for the declared raw-ingress contract."""

    expected_by_subscription: dict[str, tuple[Any, Any]]
    expected_authority_revision: int
    expected_catalog_revision: int
    accepted_by_binding: Counter[str] = field(default_factory=Counter)
    malformed_count: int = 0
    out_of_scope_count: int = 0
    identity_mismatch_count: int = 0
    revision_mismatch_count: int = 0
    test_provenance_count: int = 0
    unknown_subscription_hashes: Counter[str] = field(default_factory=Counter)

    @classmethod
    def from_catalog(
        cls,
        *,
        catalog: StableSourceCatalog,
        acquisition: StableAcquisitionPlan,
    ) -> "ScopeTally":
        enabled = {
            item.binding_id for item in acquisition.bindings if item.enabled
        }
        acquisition_by_id = {
            item.binding_id: item for item in acquisition.bindings
        }
        expected = {
            item.source_id: (
                item,
                acquisition_by_id[item.binding_id],
            )
            for item in catalog.bindings
            if item.binding_id in enabled and item.binding_id in acquisition_by_id
        }
        if not expected:
            raise ValueError("acquisition has no enabled raw bindings")
        return cls(
            expected_by_subscription=expected,
            expected_authority_revision=catalog.authority_revision,
            expected_catalog_revision=catalog.catalog_revision,
        )

    @staticmethod
    def _identity_matches(raw: Any, scoped_binding: tuple[Any, Any]) -> bool:
        binding, acquisition = scoped_binding
        identity = binding.instrument.identity
        return (
            raw.provider == binding.provider
            and raw.venue == identity.venue
            and raw.market == identity.market
            and raw.product_type == identity.product_type.value
            and raw.native_symbol == binding.instrument.native_symbol
            and raw.native_channel == acquisition.native_channel
        )

    def observe(self, payload: bytes) -> None:
        raw = raw_provider_pb2.RawProviderEnvelope()
        try:
            raw.ParseFromString(payload)
            validate_raw_envelope(raw)
        except Exception:
            self.malformed_count += 1
            return
        scoped_binding = self.expected_by_subscription.get(raw.subscription_id)
        if scoped_binding is None:
            self.out_of_scope_count += 1
            self.unknown_subscription_hashes[_sha256(raw.subscription_id)] += 1
            return
        if raw.test_provenance:
            self.test_provenance_count += 1
            return
        if not self._identity_matches(raw, scoped_binding):
            self.identity_mismatch_count += 1
            return
        if (
            raw.authority_revision != self.expected_authority_revision
            or raw.instrument_catalog_revision != self.expected_catalog_revision
        ):
            self.revision_mismatch_count += 1
            return
        binding, _acquisition = scoped_binding
        self.accepted_by_binding[binding.binding_id] += 1

    def evidence(self, *, required_bindings: Iterable[str]) -> dict[str, Any]:
        required = tuple(sorted(set(required_bindings)))
        expected_bindings = sorted(
            item.binding_id for item, _acquisition in self.expected_by_subscription.values()
        )
        unknown = dict(self.unknown_subscription_hashes.most_common(32))
        return {
            "schema": SCHEMA,
            "expected_binding_count": len(expected_bindings),
            "expected_bindings": expected_bindings,
            "accepted_by_binding": {
                key: self.accepted_by_binding.get(key, 0)
                for key in expected_bindings
            },
            "missing_required_bindings": [
                item for item in required if self.accepted_by_binding.get(item, 0) == 0
            ],
            "malformed_count": self.malformed_count,
            "out_of_scope_count": self.out_of_scope_count,
            "identity_mismatch_count": self.identity_mismatch_count,
            "revision_mismatch_count": self.revision_mismatch_count,
            "test_provenance_count": self.test_provenance_count,
            "unknown_subscription_hashes": unknown,
        }


def _group_id(prefix: str) -> str:
    if (
        not prefix.startswith("qdl-")
        or len(prefix) > 80
        or any(item in prefix for item in ("..", "/", "\\", "*"))
    ):
        raise ValueError("group-prefix must be a bounded qdl audit prefix")
    return f"{prefix}{uuid.uuid4().hex}"


def _consumer(args: argparse.Namespace):
    try:
        from confluent_kafka import Consumer
    except ImportError as error:  # pragma: no cover - production dependency gate
        raise RuntimeError("raw scope inspection requires confluent-kafka") from error
    return Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": f"qdl-phase103-raw-scope-{uuid.uuid4().hex[:12]}",
        "group.id": _group_id(args.group_prefix),
        "security.protocol": "ssl",
        "ssl.ca.location": str(args.ca),
        "ssl.certificate.location": str(args.certificate),
        "ssl.key.location": str(args.key),
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "latest",
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
    })


def collect(args: argparse.Namespace) -> dict[str, Any]:
    if not 1 <= args.records_per_partition <= 10_000:
        raise ValueError("records-per-partition must be within 1..10000")
    if not 1 <= args.timeout_seconds <= 120:
        raise ValueError("timeout-seconds must be within 1..120")
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    topic = args.topic or acquisition.raw_topic
    if topic != acquisition.raw_topic:
        raise ValueError("topic must equal the governed acquisition raw topic")
    required = [
        item.binding_id
        for item in acquisition.bindings
        if item.enabled
    ]
    tally = ScopeTally.from_catalog(catalog=catalog, acquisition=acquisition)
    consumer = _consumer(args)
    try:
        from confluent_kafka import KafkaError, TopicPartition

        metadata = consumer.list_topics(topic=topic, timeout=15)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic metadata unavailable: {topic}")
        assignments = []
        bounds: dict[int, dict[str, int]] = {}
        remaining: set[int] = set()
        for partition in sorted(topic_metadata.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=15
            )
            start = max(low, high - args.records_per_partition)
            bounds[partition] = {"low": low, "start": start, "high": high}
            if start < high:
                assignments.append(TopicPartition(topic, partition, start))
                remaining.add(partition)
        if assignments:
            consumer.assign(assignments)
        scanned_by_partition: Counter[int] = Counter()
        deadline = time.monotonic() + args.timeout_seconds
        while remaining:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "raw scope tail scan timed out "
                    f"remaining_partitions={sorted(remaining)}"
                )
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    remaining.discard(message.partition())
                    continue
                raise RuntimeError(str(message.error()))
            partition = message.partition()
            if partition not in remaining:
                continue
            if message.offset() >= bounds[partition]["high"]:
                remaining.discard(partition)
                continue
            tally.observe(bytes(message.value() or b""))
            scanned_by_partition[partition] += 1
            if message.offset() >= bounds[partition]["high"] - 1:
                remaining.discard(partition)
        evidence = tally.evidence(required_bindings=required)
        evidence.update({
            "topic": topic,
            "records_per_partition_limit": args.records_per_partition,
            "bounds": {str(key): value for key, value in sorted(bounds.items())},
            "scanned_by_partition": {
                str(key): value for key, value in sorted(scanned_by_partition.items())
            },
            "read_only": True,
            "offsets_committed": False,
        })
        if args.require_all_bindings and evidence["missing_required_bindings"]:
            raise RuntimeError(
                "declared bindings absent from bounded raw tail: "
                + ",".join(evidence["missing_required_bindings"])
            )
        return evidence
    finally:
        consumer.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--acquisition", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--ca", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--topic")
    parser.add_argument("--records-per-partition", type=int, default=500)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--group-prefix", default="qdl-phase103-raw-scope-")
    parser.add_argument("--require-all-bindings", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    evidence = collect(args)
    encoded = json.dumps(evidence, sort_keys=True, indent=2) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        target = args.output.resolve()
        if target.exists():
            raise FileExistsError(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(encoded, encoding="utf-8")
        print(json.dumps({"evidence_path": str(target), "sha256": _sha256(encoded)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

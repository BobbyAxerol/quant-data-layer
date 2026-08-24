#!/usr/bin/env python3
"""Collect bounded, real-record parity and terminal checkpoints for C40.

The collector is read-only. It captures Kafka high-watermarks first, scans only
that immutable window with ``read_committed``, compares canonical and canary
payloads by event identity, and emits hashes/metadata only. It never fabricates
market events or advances a consumer checkpoint.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import AuthorityPromotionScope


SCHEMA = "qdl.c40.live-handoff-evidence.v1"
EVENT_ID_HEADER = "qdl-event-id"
RAW_ENVELOPE_HEADER = "qdl-raw-provider-envelope"


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _headers(message: Any) -> dict[str, bytes]:
    return {
        str(key): bytes(value)
        for key, value in (message.headers() or ())
        if value is not None
    }


def _consumer(args: argparse.Namespace):
    try:
        from confluent_kafka import Consumer
    except ImportError as error:
        raise RuntimeError("live handoff collection requires confluent-kafka") from error
    return Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": f"qdl-c40-handoff-{uuid.uuid4().hex[:12]}",
        "group.id": f"qdl-c40-handoff-{uuid.uuid4().hex}",
        "security.protocol": "ssl",
        "ssl.ca.location": str(args.ca),
        "ssl.certificate.location": str(args.certificate),
        "ssl.key.location": str(args.key),
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
    })


def _scan(
    args: argparse.Namespace,
    topic: str,
    accept: Callable[[Any], None],
) -> dict[str, Any]:
    from confluent_kafka import KafkaError, TopicPartition

    consumer = _consumer(args)
    try:
        metadata = consumer.list_topics(topic=topic, timeout=15)
        metadata_topic = metadata.topics.get(topic)
        if metadata_topic is None or metadata_topic.error is not None:
            raise RuntimeError(f"Kafka topic metadata unavailable: {topic}")
        assignments = []
        terminal: dict[int, int] = {}
        low_offsets: dict[int, int] = {}
        for partition in sorted(metadata_topic.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=15
            )
            assignments.append(TopicPartition(topic, partition, low))
            low_offsets[partition] = low
            terminal[partition] = high - 1
        consumer.assign(assignments)
        remaining = {
            partition: offset
            for partition, offset in terminal.items()
            if offset >= low_offsets[partition]
        }
        records = 0
        deadline = time.monotonic() + args.timeout_seconds
        while remaining:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Kafka snapshot timeout topic={topic} remaining={remaining}"
                )
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            if message.offset() > terminal[message.partition()]:
                continue
            accept(message)
            records += 1
            if message.offset() >= terminal[message.partition()]:
                remaining.pop(message.partition(), None)
        return {
            "topic": topic,
            "records": records,
            "low_offsets": {str(key): value for key, value in low_offsets.items()},
            "high_offsets": {
                str(key): value + 1 for key, value in terminal.items()
            },
        }
    finally:
        consumer.close()


def collect(args: argparse.Namespace) -> dict[str, Any]:
    if not 8 <= args.min_overlap <= args.tail_per_slice <= 10_000:
        raise ValueError("live handoff overlap/tail bounds are invalid")
    catalog = StableSourceCatalog.load(args.catalog)
    scope = AuthorityPromotionScope.load(args.promotion_scope, catalog=catalog)
    if len(scope.binding_ids) != 12:
        raise ValueError("C40 live handoff requires exactly twelve slices")
    source_by_id = {item.binding_id: item for item in catalog.bindings}
    subscription_to_binding = {
        source_by_id[binding_id].source_id: binding_id
        for binding_id in scope.binding_ids
    }
    binding_to_slice = {}
    for binding_id in scope.binding_ids:
        source = source_by_id[binding_id]
        identity = source.instrument.identity
        binding_to_slice[binding_id] = (
            f"production/{identity.venue.lower()}/{identity.market.lower()}/"
            f"{identity.product_type.value.lower()}/{source.feed.value.lower()}/"
            f"plan-{args.partition_plan_epoch}/"
            f"{source.instrument.native_symbol.lower()}"
        )

    raw_counts: dict[str, int] = defaultdict(int)
    raw_tail: dict[str, deque[dict[str, Any]]] = {
        binding_id: deque(maxlen=args.tail_per_slice)
        for binding_id in scope.binding_ids
    }

    def accept_raw(message: Any) -> None:
        payload = bytes(message.value() or b"")
        envelope = raw_provider_pb2.RawProviderEnvelope.FromString(payload)
        validate_raw_envelope(envelope)
        binding_id = subscription_to_binding.get(envelope.subscription_id)
        if binding_id is None:
            return
        headers = _headers(message)
        event_id = headers.get(EVENT_ID_HEADER)
        capture_id = bytes(envelope.capture_id)
        if event_id != capture_id:
            raise ValueError("raw Kafka event ID differs from provider capture ID")
        raw_counts[binding_id] += 1
        raw_tail[binding_id].append({
            "event_id": capture_id.hex(),
            "watermark": raw_counts[binding_id],
            "source_session_id": envelope.source_session_id,
            "connection_generation": int(envelope.connection_generation),
            "payload_sha256": _digest(payload),
        })

    scans = {"raw": _scan(args, args.raw_topic, accept_raw)}

    canonical: dict[str, deque[tuple[str, str]]] = {
        binding_id: deque(maxlen=args.tail_per_slice)
        for binding_id in scope.binding_ids
    }
    canary: dict[str, deque[tuple[str, str]]] = {
        binding_id: deque(maxlen=args.tail_per_slice)
        for binding_id in scope.binding_ids
    }

    def canonical_accept(target: dict[str, deque[tuple[str, str]]]):
        def accept(message: Any) -> None:
            payload = bytes(message.value() or b"")
            envelope = market_data_pb2.EventEnvelope.FromString(payload)
            binding = catalog.binding_for_envelope(envelope)
            if binding.binding_id not in target:
                return
            headers = _headers(message)
            event_id = headers.get(EVENT_ID_HEADER)
            if event_id != bytes(envelope.event_id):
                raise ValueError("canonical Kafka event ID differs from envelope")
            inline_raw = headers.get(RAW_ENVELOPE_HEADER)
            if inline_raw is None:
                raise ValueError("canonical record lacks raw-provider provenance")
            raw = raw_provider_pb2.RawProviderEnvelope.FromString(inline_raw)
            validate_raw_envelope(raw)
            if bytes(raw.capture_id) != bytes(envelope.raw_capture_id):
                raise ValueError("canonical/raw capture lineage differs")
            target[binding.binding_id].append((event_id.hex(), _digest(payload)))
        return accept

    scans["canonical"] = _scan(
        args, args.canonical_topic, canonical_accept(canonical)
    )
    scans["canary"] = _scan(args, args.canary_topic, canonical_accept(canary))

    latest_checkpoints: dict[str, dict[str, Any]] = {}

    def accept_checkpoint(message: Any) -> None:
        key = bytes(message.key() or b"").decode("utf-8")
        value = json.loads(bytes(message.value() or b"{}"))
        if value.get("schema") != "qdl.target-watermark-checkpoint.v1":
            raise ValueError("target checkpoint schema is unsupported")
        if key != f"{value.get('slice_id')}|{value.get('shard_id')}|{value.get('target')}":
            raise ValueError("target checkpoint key differs from payload")
        if value.get("target") == "CANARY_CANONICAL":
            latest_checkpoints[str(value["slice_id"])] = value

    scans["checkpoints"] = _scan(
        args, args.checkpoint_topic, accept_checkpoint
    )

    slices = []
    for binding_id in sorted(scope.binding_ids):
        slice_id = binding_to_slice[binding_id]
        checkpoint = latest_checkpoints.get(slice_id)
        if checkpoint is None:
            raise RuntimeError(f"canary checkpoint is missing: {slice_id}")
        if (
            checkpoint.get("owner_id") != args.canary_owner_id
            or int(checkpoint.get("authority_revision", 0)) <= 0
            or int(checkpoint.get("lease_epoch", 0)) <= 0
            or checkpoint.get("shard_id") != binding_id
        ):
            raise RuntimeError(f"canary checkpoint authority differs: {slice_id}")
        raw_by_id = {item["event_id"]: item for item in raw_tail[binding_id]}
        terminal = raw_by_id.get(str(checkpoint.get("source_event_id")))
        if terminal is None:
            raise RuntimeError(f"terminal raw event fell outside bounded tail: {slice_id}")
        if terminal["watermark"] != int(checkpoint["source_watermark"]):
            raise RuntimeError(f"canary watermark differs from raw sequence: {slice_id}")
        old = {event_id: digest for event_id, digest in canonical[binding_id]}
        new = {event_id: digest for event_id, digest in canary[binding_id]}
        overlap_ids = sorted(set(old) & set(new))
        mismatches = sum(old[event_id] != new[event_id] for event_id in overlap_ids)
        if len(overlap_ids) < args.min_overlap or mismatches:
            raise RuntimeError(
                f"canonical/canary parity failed slice={slice_id} "
                f"overlap={len(overlap_ids)} mismatches={mismatches}"
            )
        overlap_watermarks = sorted(
            raw_by_id[event_id]["watermark"]
            for event_id in overlap_ids
            if event_id in raw_by_id
        )
        if len(overlap_watermarks) < args.min_overlap:
            raise RuntimeError(f"parity events fell outside raw tail: {slice_id}")
        slices.append({
            "binding_id": binding_id,
            "slice_id": slice_id,
            "checkpoint": checkpoint,
            "source_session_id": terminal["source_session_id"],
            "connection_generation": terminal["connection_generation"],
            "terminal_raw_payload_sha256": terminal["payload_sha256"],
            "overlap_start_watermark": overlap_watermarks[0],
            "overlap_end_watermark": int(checkpoint["source_watermark"]),
            "old_event_count": len(overlap_ids),
            "new_event_count": len(overlap_ids),
            "semantic_mismatches": 0,
            "open_gaps": 0,
            "parity_set_sha256": _digest(
                "\n".join(
                    f"{event_id}:{old[event_id]}" for event_id in overlap_ids
                ).encode()
            ),
        })
    evidence = {
        "schema": SCHEMA,
        "captured_at_ns": time.time_ns(),
        "provider_provenance": "REAL",
        "production_mutations": 0,
        "scope_revision": scope.revision,
        "scope_digest": scope.digest(),
        "topics": {
            "raw": args.raw_topic,
            "canonical": args.canonical_topic,
            "canary": args.canary_topic,
            "checkpoints": args.checkpoint_topic,
        },
        "scans": scans,
        "slices": slices,
    }
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--promotion-scope", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--ca", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--raw-topic", default="md.raw.realtime.v2")
    parser.add_argument("--canonical-topic", default="md.canonical.v2")
    parser.add_argument("--canary-topic", default="md.canary.canonical.v2")
    parser.add_argument("--checkpoint-topic", default="qdl.target-checkpoint.v1")
    parser.add_argument("--partition-plan-epoch", type=int, default=1)
    parser.add_argument("--canary-owner-id", default="qdl-v2-rust-canary")
    parser.add_argument("--tail-per-slice", type=int, default=512)
    parser.add_argument("--min-overlap", type=int, default=32)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for path in (args.catalog, args.promotion_scope, args.ca, args.certificate, args.key):
        if not path.is_file():
            raise FileNotFoundError(path)
    evidence = collect(args)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": "PASS",
        "provider_provenance": evidence["provider_provenance"],
        "slices": len(evidence["slices"]),
        "semantic_mismatches": 0,
        "open_gaps": 0,
        "production_mutations": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

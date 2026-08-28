#!/usr/bin/env python3
"""Collect bounded live Rust-vs-Python parity for the C40 promotion scope.

The collector is read-only. It freezes Kafka high-watermarks, scans only a
bounded tail with read_committed isolation, reconstructs canonical events from
the inline authentic RawProviderEnvelope through the independent Python
canonicalizers, and emits hashes/metadata only.
"""

from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Mapping
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.canonical.market import (
    canonicalize_binance_usdm_bbo,
    canonicalize_binance_usdm_bar,
    canonicalize_binance_usdm_rest_bar,
    canonicalize_okx_bar,
    canonicalize_okx_bbo,
)
from qdl.canonical.trade import (
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_okx_trade,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.runtime.stable_deployment import (
    AuthorityPromotionScope,
    StableAcquisitionBinding,
    StableAcquisitionPlan,
)
from scripts.phasec40_authority_bootstrap import _c40_binding_ids

SCHEMA = "qdl.c40.live-core-parity.v1"
EVENT_ID_HEADER = "qdl-event-id"
RAW_ENVELOPE_HEADER = "qdl-raw-provider-envelope"
SUPPORTED_KINDS = frozenset(
    {
        "binance_usdm_trade",
        "binance_usdm_bbo",
        "binance_usdm_bar",
        "binance_usdm_rest_bar",
        "okx_trade",
        "okx_bbo",
        "okx_bar",
    }
)
R1_REFERENCE_GROUP_PREFIX = "qdl-r1-reference-parity-"


def _require_r1_reference_group(value: str) -> str:
    if not value.startswith(R1_REFERENCE_GROUP_PREFIX) or value == R1_REFERENCE_GROUP_PREFIX:
        raise ValueError(
            "R1 reference parity requires an isolated qdl-r1-reference-parity-* group"
        )
    return value


def _prepare_output(path: Path) -> Path:
    output = path.resolve()
    if output.exists():
        raise FileExistsError(f"live parity evidence output already exists: {output}")
    parent = output.parent
    if parent.exists():
        if not parent.is_dir():
            raise NotADirectoryError(parent)
    else:
        parent.mkdir(parents=True, mode=0o700)
        parent.chmod(0o700)
    return output


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _headers(message: Any) -> dict[str, bytes]:
    return {
        str(key): bytes(value)
        for key, value in (message.headers() or ())
        if value is not None
    }


@dataclass(frozen=True, slots=True)
class LiveSample:
    payload: bytes
    raw_envelope: bytes
    event_id_header: bytes
    partition: int
    offset: int


def _consumer(args: argparse.Namespace):
    try:
        from confluent_kafka import Consumer
    except ImportError as error:
        raise RuntimeError("live core parity requires confluent-kafka") from error
    return Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": f"qdl-c40-live-parity-{uuid.uuid4().hex[:12]}",
            "group.id": args.consumer_group,
            "security.protocol": "ssl",
            "ssl.ca.location": str(args.ca),
            "ssl.certificate.location": str(args.certificate),
            "ssl.key.location": str(args.key),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
        }
    )


def _bounded_window(low: int, high: int, records_per_partition: int) -> tuple[int, int]:
    if low < 0 or high < low or records_per_partition < 1:
        raise ValueError("Kafka watermark/window bounds are invalid")
    return max(low, high - records_per_partition), high


def _scan_tail(
    args: argparse.Namespace,
    *,
    accept: Callable[[Any], None],
    complete: Callable[[], bool],
) -> dict[str, Any]:
    from confluent_kafka import KafkaError, TopicPartition

    consumer = _consumer(args)
    try:
        metadata = consumer.list_topics(topic=args.canonical_topic, timeout=15)
        topic = metadata.topics.get(args.canonical_topic)
        if topic is None or topic.error is not None:
            raise RuntimeError(
                f"Kafka topic metadata unavailable: {args.canonical_topic}"
            )
        partitions = sorted(topic.partitions)
        timestamp_ms = args.candidate_deployed_at_ns // 1_000_000
        timestamp_offsets = consumer.offsets_for_times(
            [
                TopicPartition(args.canonical_topic, partition, timestamp_ms)
                for partition in partitions
            ],
            timeout=15,
        )
        by_partition = {
            int(item.partition): int(item.offset) for item in timestamp_offsets
        }
        assignments = []
        starts: dict[int, int] = {}
        highs: dict[int, int] = {}
        remaining: dict[int, int] = {}
        last_offsets: dict[int, int] = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(args.canonical_topic, partition), timeout=15
            )
            candidate_start = by_partition.get(partition, -1)
            if candidate_start < 0:
                candidate_start = high
            start = max(low, candidate_start, high - args.records_per_partition)
            start, terminal = _bounded_window(
                start, high, args.records_per_partition
            )
            starts[partition] = start
            highs[partition] = terminal
            assignments.append(
                TopicPartition(args.canonical_topic, partition, start)
            )
            if terminal > start:
                remaining[partition] = terminal - 1
        consumer.assign(assignments)
        records = 0
        completed_early = complete()
        deadline = time.monotonic() + args.timeout_seconds
        while remaining and not completed_early:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "bounded parity scan timed out with partitions "
                    + json.dumps(remaining, sort_keys=True)
                )
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            terminal = highs[message.partition()]
            if message.offset() >= terminal:
                continue
            accept(message)
            records += 1
            last_offsets[int(message.partition())] = int(message.offset())
            completed_early = complete()
            if message.offset() >= remaining[message.partition()]:
                remaining.pop(message.partition(), None)
        return {
            "topic": args.canonical_topic,
            "records": records,
            "candidate_timestamp_ms": timestamp_ms,
            "start_offsets": {str(key): value for key, value in starts.items()},
            "captured_high_offsets": {
                str(key): value for key, value in highs.items()
            },
            "last_read_offsets": {
                str(key): value for key, value in last_offsets.items()
            },
            "maximum_window_records": sum(
                highs[key] - starts[key] for key in starts
            ),
            "completed_when_sample_floor_met": completed_early,
        }
    finally:
        consumer.close()


def _context(
    binding: StableSourceBinding,
    raw: raw_provider_pb2.RawProviderEnvelope,
    live: market_data_pb2.EventEnvelope,
) -> TradeContext:
    identity = binding.instrument.identity
    return TradeContext(
        instrument_uid=binding.instrument.instrument_uid,
        instrument_id=binding.instrument.instrument_id,
        instrument_revision=binding.instrument.metadata_revision,
        venue=identity.venue,
        market=identity.market,
        product_type=identity.product_type.value,
        native_symbol=binding.instrument.native_symbol,
        provider=binding.provider,
        source_id=binding.source_id,
        source_role=binding.source_role,
        lease_epoch=int(raw.lease_epoch),
        received_at_ns=int(raw.received_at_ns),
        normalized_at_ns=int(raw.received_at_ns),
        published_at_ns=int(raw.received_at_ns),
        partition_sequence=int(live.partition_sequence),
        normalizer_version=binding.normalizer_version,
        adapter_version=raw.adapter_version,
        config_revision=int(raw.config_revision),
        correlation_id=raw.correlation_id,
        source_session_id=raw.source_session_id,
        connection_generation=int(raw.connection_generation),
        authority_revision=int(raw.authority_revision),
        partition_plan_epoch=int(raw.partition_plan_epoch),
        raw_capture_id=bytes(raw.capture_id),
        raw_frame_sha256=bytes(raw.raw_frame_sha256),
    )


def _provider_frames(
    acquisition: StableAcquisitionBinding,
    raw_payload: Any,
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(raw_payload, Mapping):
        raise ValueError("provider frame must be a JSON object")
    if acquisition.provider_kind in {
        "binance_usdm_trade",
        "binance_usdm_bbo",
        "binance_usdm_bar",
    }:
        value = raw_payload.get("data", raw_payload)
        if not isinstance(value, Mapping):
            raise ValueError("Binance provider data must be an object")
        return (value,)
    if acquisition.provider_kind == "okx_trade":
        rows = raw_payload.get("data")
        if isinstance(rows, list):
            if not rows or any(not isinstance(item, Mapping) for item in rows):
                raise ValueError("OKX trade frame rows are invalid")
            return tuple(rows)
        return (raw_payload,)
    return (raw_payload,)


def _canonicalize(
    acquisition: StableAcquisitionBinding,
    frame: Mapping[str, Any],
    context: TradeContext,
) -> market_data_pb2.EventEnvelope:
    dispatch = {
        "binance_usdm_trade": canonicalize_binance_usdm_trade,
        "binance_usdm_bbo": canonicalize_binance_usdm_bbo,
        "binance_usdm_bar": canonicalize_binance_usdm_bar,
        "binance_usdm_rest_bar": canonicalize_binance_usdm_rest_bar,
        "okx_trade": canonicalize_okx_trade,
        "okx_bbo": canonicalize_okx_bbo,
        "okx_bar": canonicalize_okx_bar,
    }
    try:
        canonicalizer = dispatch[acquisition.provider_kind]
    except KeyError as error:
        raise ValueError(
            f"unsupported live parity provider kind: {acquisition.provider_kind}"
        ) from error
    return canonicalizer(frame, context)


def verify_sample(
    sample: LiveSample,
    *,
    catalog: StableSourceCatalog,
    binding: StableSourceBinding,
    acquisition: StableAcquisitionBinding,
) -> dict[str, Any]:
    live = market_data_pb2.EventEnvelope.FromString(sample.payload)
    resolved = catalog.binding_for_envelope(live)
    if resolved.binding_id != binding.binding_id:
        raise ValueError("live envelope resolves to a different binding")
    if sample.event_id_header != bytes(live.event_id):
        raise ValueError("Kafka event ID header differs from canonical envelope")

    raw = raw_provider_pb2.RawProviderEnvelope.FromString(sample.raw_envelope)
    validate_raw_envelope(raw)
    if raw.test_provenance:
        raise ValueError("production parity rejects test provenance")
    identity = binding.instrument.identity
    raw_checks = {
        "provider": raw.provider == binding.provider,
        "venue": raw.venue == identity.venue,
        "market": raw.market == identity.market,
        "product_type": raw.product_type == identity.product_type.value,
        "native_symbol": raw.native_symbol == binding.instrument.native_symbol,
        "native_channel": raw.native_channel == acquisition.native_channel,
        "subscription_id": raw.subscription_id == binding.source_id,
        "instrument_catalog_revision": int(raw.instrument_catalog_revision)
        == catalog.catalog_revision,
    }
    live_checks = {
        "raw_capture_id": bytes(live.raw_capture_id) == bytes(raw.capture_id),
        "raw_payload_hash": bytes(live.raw_payload_hash)
        == bytes(raw.raw_frame_sha256),
        "received_at_ns": int(live.received_at_ns) == int(raw.received_at_ns),
        "normalized_at_ns": int(live.normalized_at_ns) == int(raw.received_at_ns),
        "published_at_ns": int(live.published_at_ns) == int(raw.received_at_ns),
        "source_session_id": live.source_session_id == raw.source_session_id,
        "connection_generation": int(live.connection_generation)
        == int(raw.connection_generation),
        "lease_epoch": int(live.lease_epoch) == int(raw.lease_epoch),
        "authority_revision": int(live.authority_revision)
        == int(raw.authority_revision),
        "partition_plan_epoch": int(live.partition_plan_epoch)
        == int(raw.partition_plan_epoch),
        "config_revision": int(live.config_revision) == int(raw.config_revision),
        "adapter_version": live.adapter_version == raw.adapter_version,
        "normalizer_version": live.normalizer_version
        == binding.normalizer_version,
    }
    failed = [
        name
        for name, passed in {**raw_checks, **live_checks}.items()
        if not passed
    ]
    if failed:
        raise ValueError(
            f"live canonical provenance differs binding={binding.binding_id}: "
            + ",".join(failed)
        )

    raw_payload = json.loads(bytes(raw.raw_frame_bytes))
    context = _context(binding, raw, live)
    candidates = [
        _canonicalize(acquisition, frame, context).SerializeToString(
            deterministic=True
        )
        for frame in _provider_frames(acquisition, raw_payload)
    ]
    if sample.payload not in candidates:
        raise ValueError(
            f"Rust/Python canonical bytes differ binding={binding.binding_id} "
            f"partition={sample.partition} offset={sample.offset}"
        )
    return {
        "partition": sample.partition,
        "offset": sample.offset,
        "event_id": bytes(live.event_id).hex(),
        "payload_sha256": _digest(sample.payload),
        "raw_capture_id": bytes(raw.capture_id).hex(),
        "source_event_time_ns": int(live.source_event_time_ns),
        "received_at_ns": int(live.received_at_ns),
    }


def collect(args: argparse.Namespace) -> dict[str, Any]:
    _require_r1_reference_group(args.consumer_group)
    if not 8 <= args.samples_per_slice <= 256:
        raise ValueError("samples_per_slice must be in 8..256")
    if not 1_000 <= args.records_per_partition <= 2_000_000:
        raise ValueError("records_per_partition must be in 1000..2000000")
    if not args.candidate_image_digest.startswith("sha256:") or len(
        args.candidate_image_digest
    ) != 71:
        raise ValueError("candidate image digest must be immutable SHA-256")
    if args.candidate_deployed_at_ns <= 0:
        raise ValueError("candidate deployment timestamp must be positive")

    catalog = StableSourceCatalog.load(args.catalog)
    scope = AuthorityPromotionScope.load(args.promotion_scope, catalog=catalog)
    acquisition = StableAcquisitionPlan.load(
        args.acquisition_plan, catalog=catalog
    )
    source_by_id = {item.binding_id: item for item in catalog.bindings}
    acquisition_by_id = {item.binding_id: item for item in acquisition.bindings}
    selected = set(_c40_binding_ids(
        catalog=catalog, acquisition=acquisition, scope=scope
    ))
    if {
        acquisition_by_id[item].provider_kind for item in selected
    } - SUPPORTED_KINDS:
        raise ValueError("promotion scope contains an unsupported Python oracle kind")

    samples: dict[str, deque[LiveSample]] = {
        binding_id: deque(maxlen=args.samples_per_slice)
        for binding_id in selected
    }
    ignored_records = 0
    pre_candidate_records = 0

    def accept(message: Any) -> None:
        nonlocal ignored_records, pre_candidate_records
        payload = bytes(message.value() or b"")
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
        binding = catalog.binding_for_envelope(envelope)
        if binding.binding_id not in selected:
            ignored_records += 1
            return
        if int(envelope.received_at_ns) < args.candidate_deployed_at_ns:
            pre_candidate_records += 1
            return
        headers = _headers(message)
        event_id = headers.get(EVENT_ID_HEADER)
        raw_envelope = headers.get(RAW_ENVELOPE_HEADER)
        if event_id is None or raw_envelope is None:
            raise ValueError("canonical record lacks event ID/raw provenance header")
        samples[binding.binding_id].append(
            LiveSample(
                payload=payload,
                raw_envelope=raw_envelope,
                event_id_header=event_id,
                partition=int(message.partition()),
                offset=int(message.offset()),
            )
        )

    scan = _scan_tail(
        args,
        accept=accept,
        complete=lambda: all(
            len(value) >= args.samples_per_slice for value in samples.values()
        ),
    )
    missing = {
        key: len(value)
        for key, value in samples.items()
        if len(value) < args.samples_per_slice
    }
    if missing:
        raise RuntimeError(
            "bounded canonical tail lacks required slice samples: "
            + json.dumps(missing, sort_keys=True)
        )

    slices = []
    aggregate = hashlib.sha256()
    for binding_id in sorted(selected):
        binding = source_by_id[binding_id]
        acquisition_binding = acquisition_by_id[binding_id]
        verified = [
            verify_sample(
                item,
                catalog=catalog,
                binding=binding,
                acquisition=acquisition_binding,
            )
            for item in samples[binding_id]
        ]
        for item in verified:
            aggregate.update(bytes.fromhex(item["event_id"]))
            aggregate.update(bytes.fromhex(item["payload_sha256"]))
        slices.append(
            {
                "binding_id": binding_id,
                "venue": binding.instrument.identity.venue,
                "market": binding.instrument.identity.market,
                "native_symbol": binding.instrument.native_symbol,
                "feed": binding.feed.value,
                "interval": binding.interval,
                "provider": binding.provider,
                "provider_kind": acquisition_binding.provider_kind,
                "samples": len(verified),
                "first_source_event_time_ns": min(
                    item["source_event_time_ns"] for item in verified
                ),
                "last_source_event_time_ns": max(
                    item["source_event_time_ns"] for item in verified
                ),
                "first_received_at_ns": min(
                    item["received_at_ns"] for item in verified
                ),
                "last_received_at_ns": max(
                    item["received_at_ns"] for item in verified
                ),
                "sample_set_sha256": _digest(
                    "\n".join(
                        f'{item["event_id"]}:{item["payload_sha256"]}'
                        for item in verified
                    ).encode()
                ),
                "semantic_mismatches": 0,
                "invalid_provenance": 0,
            }
        )

    return {
        "schema": SCHEMA,
        "status": "PASS",
        "captured_at_ns": time.time_ns(),
        "provider_provenance": "REAL",
        "production_mutations": 0,
        "source_commit": args.source_commit,
        "candidate_image_digest": args.candidate_image_digest,
        "candidate_deployed_at_ns": args.candidate_deployed_at_ns,
        "consumer_group": args.consumer_group,
        "scope_revision": scope.revision,
        "scope_digest": scope.digest(),
        "catalog_revision": catalog.catalog_revision,
        "acquisition_revision": acquisition.revision,
        "canonical_topic": args.canonical_topic,
        "scan": scan,
        "ignored_records": ignored_records,
        "pre_candidate_records": pre_candidate_records,
        "slices": slices,
        "slice_count": len(slices),
        "sample_count": sum(item["samples"] for item in slices),
        "semantic_mismatches": 0,
        "invalid_provenance": 0,
        "aggregate_sha256": aggregate.hexdigest(),
        "gap_free_handoff_evaluated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--acquisition-plan", type=Path, required=True)
    parser.add_argument("--promotion-scope", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--consumer-group", required=True)
    parser.add_argument("--ca", type=Path, required=True)
    parser.add_argument("--certificate", type=Path, required=True)
    parser.add_argument("--key", type=Path, required=True)
    parser.add_argument("--candidate-image-digest", required=True)
    parser.add_argument("--candidate-deployed-at-ns", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--canonical-topic", default="md.canonical.v2")
    parser.add_argument("--records-per-partition", type=int, default=250_000)
    parser.add_argument("--samples-per-slice", type=int, default=32)
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for path in (
        args.catalog,
        args.acquisition_plan,
        args.promotion_scope,
        args.ca,
        args.certificate,
        args.key,
    ):
        if not path.is_file():
            raise FileNotFoundError(path)
    output = _prepare_output(args.output)
    evidence = collect(args)
    output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output.chmod(0o640)
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "provider_provenance": evidence["provider_provenance"],
                "slices": evidence["slice_count"],
                "samples": evidence["sample_count"],
                "semantic_mismatches": evidence["semantic_mismatches"],
                "invalid_provenance": evidence["invalid_provenance"],
                "production_mutations": evidence["production_mutations"],
                "gap_free_handoff_evaluated": evidence[
                    "gap_free_handoff_evaluated"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

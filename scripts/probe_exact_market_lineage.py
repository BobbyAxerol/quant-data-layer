#!/usr/bin/env python3
"""Bounded read-only raw/canonical/exact Query evidence; never an acceptance gate."""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
from pathlib import Path
import time


VIEW_FIELDS = ("instrument_uid", "instrument_id", "feed", "interval", "quality",
               "source", "contract", "observed_at_ns", "received_at_ns",
               "watermark_offset", "revision")


def exact_view(response):
    """Allowlist metadata from the actual response; no payload or signed cursor."""
    row = response.get("data", {})
    return {key: row[key] for key in VIEW_FIELDS if key in row}


def trade_state(*, raw_count, canonical_count, query_matches, coverage_complete):
    if not coverage_complete:
        return "UNASSESSED_INCOMPLETE_CAPTURE"
    if raw_count == 0:
        return "NO_TRADE_IN_CAPTURE_WINDOW_NOT_SESSION_PROOF"
    if canonical_count == 0:
        return "RAW_PRESENT_CANONICAL_MISSING_IN_WINDOW"
    if not query_matches:
        return "CANONICAL_PRESENT_QUERY_JOIN_MISSING"
    return "SOURCE_CANONICAL_QUERY_OBSERVED"


def exact_join(query, canonical):
    """Match original lineage; the public handoff boundary may exceed row offset."""
    view = query.get("exact_view") or {}
    event = canonical["event"]
    return (
        query["uid"] == event["instrument_uid"]
        and query["feed"].lower() == canonical["feed"]
        and view.get("watermark_offset", -1) >= canonical["offset"]
        and view.get("observed_at_ns") == int(event["source_event_time_ns"])
        and view.get("received_at_ns") == int(event["received_at_ns"])
        and view.get("contract", {}).get("correlation_id") == event["correlation_id"]
    )


def newer_before_request(query, selected, candidate):
    """Positive lag evidence only: same product/generation, observed before call."""
    old, new = selected["event"], candidate["event"]
    return (
        exact_join(query, selected)
        and old["instrument_uid"] == new["instrument_uid"]
        and selected["feed"] == candidate["feed"]
        and selected["partition"] == candidate["partition"]
        and candidate["offset"] > selected["offset"]
        and candidate["captured_at_ns"] < query["request_started_at_ns"]
        and old["source_session_id"] == new["source_session_id"]
        and old["connection_generation"] == new["connection_generation"]
        and old["authority_revision"] == new["authority_revision"]
        and int(new["received_at_ns"]) > int(old["received_at_ns"])
    )


class Evidence:
    def __init__(self, path, max_bytes=48 * 1024 * 1024):
        self.file = Path(path).open("x")
        self.size = 0
        self.max_bytes = max_bytes
        self.count = 0

    def put(self, kind, **row):
        line = json.dumps(dict(kind=kind, captured_at_ns=time.time_ns(), **row),
                          sort_keys=True, separators=(",", ":")) + "\n"
        size = len(line.encode())
        if self.size + size > self.max_bytes:
            raise RuntimeError("EVIDENCE_BYTE_LIMIT")
        self.file.write(line)
        self.file.flush()
        self.size += size
        self.count += 1

    def close(self):
        self.file.close()


def raw_metadata(raw):
    from qdl.raw.envelope import validate_raw_envelope
    validate_raw_envelope(raw)
    body = json.loads(raw.raw_frame_bytes)
    data = body.get("data", body)
    rows = data if isinstance(data, list) else [data]
    # Native IDs and clocks only: retain hashes, not provider payloads.
    clocks = [{k: item[k] for k in ("ts", "T", "E", "tradeId", "t", "a", "instId", "s")
               if k in item} for item in rows if isinstance(item, dict)]
    return dict(capture_id=bytes(raw.capture_id).hex(),
                raw_frame_sha256=bytes(raw.raw_frame_sha256).hex(),
                provider=raw.provider, native_symbol=raw.native_symbol,
                native_channel=raw.native_channel, received_at_ns=raw.received_at_ns,
                source_session_id=raw.source_session_id,
                connection_generation=raw.connection_generation,
                authority_revision=raw.authority_revision,
                test_provenance=raw.test_provenance, original_clocks=clocks)


def canonical_metadata(event):
    from google.protobuf.json_format import MessageToDict
    from qdl.runtime.mark_index_lineage import paired_mark_index_lineage
    result = MessageToDict(event, preserving_proto_field_name=True)
    result.pop(event.WhichOneof("payload"), None)
    for key in ("event_id", "raw_capture_id", "raw_payload_hash", "canonical_payload_hash"):
        result[key] = bytes(getattr(event, key)).hex()
    if event.WhichOneof("payload") == "mark_index_price":
        try:
            pair = dataclasses.asdict(paired_mark_index_lineage(event))
            result["components"] = {k: v.hex() if isinstance(v, bytes) else v
                                    for k, v in pair.items()}
        except ValueError:
            result["component_lineage"] = "NOT_DERIVED_PAIR"
    return result


async def run(args):
    import yaml
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    from qdl_sdk.tls import WorkloadTlsConfig
    from qdl_sdk.transport import GrpcStreamTransport, RestQueryTransport
    from scripts.accept_consumer_realms import requirement
    from scripts.kn_canonical_mirror import source_config

    if not 1 <= args.seconds <= 180:
        raise ValueError("duration must be 1..180 seconds")
    if not 0 <= args.tail_records <= 2000:
        raise ValueError("tail must be 0..2000 records per partition")
    packet = Path(args.packet)
    policy = json.loads((packet / "reader-public/jwt-config.json").read_text())
    catalog = yaml.safe_load(Path(args.catalog).read_text())
    wanted = set(args.uid)
    instruments = [r for r in catalog["instruments"] if r["instrument_uid"] in wanted]
    if {r["instrument_uid"] for r in instruments} != wanted:
        raise ValueError("UID absent from source catalog")
    symbols = {r["native_symbol"] for r in instruments}
    # OKX's index component is named BTC-USDT, not BTC-USDT-SWAP.
    symbols |= {s.removesuffix("-SWAP") for s in symbols}
    evidence = Evidence(args.output)
    consumer = None
    clients = []
    start = time.monotonic()
    deadline = start + args.seconds
    counters = {"raw": 0, "canonical": 0, "query": 0, "query_refused": 0}
    starts, positions, eof = {}, {}, {}
    errors = []

    class ExactTransport(RestQueryTransport):
        last = None

        async def snapshot(self, requirement, *, consumer_id):
            self.last = None
            value = await super().snapshot(requirement, consumer_id=consumer_id)
            self.last = exact_view(value)
            return value

    try:
        evidence.put("scope", schema="qdl.exact-market-lineage.v1",
                     provenance="production_read_only", test_provenance=False,
                     instruments=instruments, duration_seconds=args.seconds,
                     source_catalog_sha256=hashlib.sha256(Path(args.catalog).read_bytes()).hexdigest(),
                     kafka_commits=False, order_actions=0,
                     classification_rule="No quiet conclusion from provider_session_state")
        config = source_config(argparse.Namespace(source_bootstrap=args.bootstrap,
                               group_prefix="qdl-r1-reference-parity-", ca=args.ca,
                               cert=args.cert, key=args.key))
        config["enable.partition.eof"] = True
        config["client.id"] = "exact-market-lineage"
        consumer = Consumer(config)
        assignments = []
        for topic in ("md.raw.realtime.v2", "md.canonical.v2"):
            metadata = consumer.list_topics(topic, timeout=5).topics[topic]
            if metadata.error:
                errors.append(dict(code="TOPIC_METADATA_UNAVAILABLE", topic=topic, kafka_code=metadata.error.code()))
                continue
            for partition in sorted(metadata.partitions):
                tp = TopicPartition(topic, partition)
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                offset = max(low, high - args.tail_records)
                starts[f"{topic}:{partition}"] = dict(low=low, high=high, start=offset)
                assignments.append(TopicPartition(topic, partition, offset))
        consumer.assign(assignments)
        evidence.put("kafka_start", partitions=starts, group_id=config["group.id"],
                     isolation="read_committed", manual_assignment=True)
        for realm in ("live", "sandbox"):
            cid = f"trading-system.{realm}.stable"
            path = packet / "manifests-crypto" / f"{cid}.yaml"
            manifest = yaml.safe_load(path.read_text())
            meta = manifest["metadata"]
            identity = Path(args.identities) / cid
            creds = RotatingJwtCredentialProvider(private_key_file=identity / "private.key",
                key_id=cid + "-rs256-v1", algorithm="RS256", issuer=policy["issuer"],
                audience=policy["audience"], subject=meta["subject"], environment=realm,
                roles=("market_data_reader", "historical_reader", "stream_consumer"),
                consumer_manifest_revision=meta["revision"], lifetime_seconds=300,
                refresh_before_seconds=30)
            tls = WorkloadTlsConfig(identity / "ca.crt", identity / "client.crt", identity / "client.key")
            selected = [requirement(r) for r in manifest["spec"]["requirements"]
                        if r["instrument_uid"] in wanted and r["feed"] in {"TRADE", "MARK_INDEX_PRICE"}]
            for replica in (1, 2):
                transport = ExactTransport(f"https://query_v2_{replica}:8200",
                                          credential_provider=creds, tls=tls, timeout_seconds=3)
                stream = GrpcStreamTransport("unused.invalid:8210", credential_provider=creds, tls=tls)
                client = AsyncDataLayerClient(query_transport=transport, stream_transport=stream, consumer_id=cid)
                clients.append((cid, replica, selected, transport, client))

        async def queries():
            while time.monotonic() < deadline:
                for cid, replica, selected, transport, client in clients:
                    for req in selected:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            return
                        row = dict(consumer_id=cid, replica=replica, uid=req.instrument_uid,
                                   feed=req.feed.value, request_started_at_ns=time.time_ns())
                        try:
                            async with asyncio.timeout(min(3.5, remaining)):
                                await client.snapshot(req)
                            row["status"] = "TYPED_RESPONSE"
                        except Exception as error:
                            row.update(status="REFUSED", code=getattr(error, "code", type(error).__name__),
                                       diagnostics=getattr(error, "diagnostics", None))
                            counters["query_refused"] += 1
                        row["exact_view"] = transport.last
                        evidence.put("query", **row)
                        counters["query"] += 1
                        await asyncio.sleep(.025)
                await asyncio.sleep(.5)

        async def kafka():
            while time.monotonic() < deadline:
                # Bounded microbatches yield to Query; no unbounded queue/list.
                for _ in range(100):
                    msg = consumer.poll(0)
                    if msg is None:
                        break
                    key = f"{msg.topic()}:{msg.partition()}"
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            positions[key] = msg.offset()
                            eof[key] = dict(offset=msg.offset(), observed_at_ns=time.time_ns())
                            continue
                        errors.append(dict(code=msg.error().code(), topic=msg.topic()))
                        raise RuntimeError("KAFKA_READ_ERROR")
                    positions[key] = msg.offset() + 1
                    where = dict(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
                                 kafka_timestamp_ms=msg.timestamp()[1])
                    if msg.topic() == "md.raw.realtime.v2":
                        raw = RawProviderEnvelope.FromString(msg.value())
                        if raw.native_symbol not in symbols or not any(
                                word in raw.native_channel.lower() for word in ("trade", "mark", "index")):
                            continue
                        evidence.put("raw", **where, **raw_metadata(raw))
                        counters["raw"] += 1
                    else:
                        event = EventEnvelope.FromString(msg.value())
                        if event.instrument_uid not in wanted or event.WhichOneof("payload") not in {"trade", "mark_index_price"}:
                            continue
                        record = canonical_metadata(event)
                        headers = dict(msg.headers() or ())
                        inline = headers.get("qdl-raw-provider-envelope")
                        if inline:
                            record["inline_raw"] = raw_metadata(RawProviderEnvelope.FromString(inline))
                        evidence.put("canonical", **where, feed=event.WhichOneof("payload"), event=record)
                        counters["canonical"] += 1
                await asyncio.sleep(.01)

        tasks = [asyncio.create_task(queries()), asyncio.create_task(kafka())]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as error:
        errors.append(dict(code=type(error).__name__))
    finally:
        for _, _, _, _, client in clients:
            await client.close()
        if consumer is not None:
            consumer.close()
        try:
            evidence.put("summary", counts=counters, errors=errors, start_offsets=starts,
                         next_positions=positions, last_partition_eof=eof, elapsed_seconds=time.monotonic() - start,
                         historical_receipt_reconstructed=False,
                         quiet_certified=False, production_writes=0)
        finally:
            evidence.close()
    print(json.dumps(dict(counts=counters, errors=errors, evidence_bytes=evidence.size)))
    return not errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("packet", "identities", "catalog", "output", "ca", "cert", "key"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--uid", action="append", required=True)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--tail-records", type=int, default=400)
    parser.add_argument("--bootstrap", default="kafka1:9092,kafka2:9092,kafka3:9092")
    raise SystemExit(0 if asyncio.run(run(parser.parse_args())) else 1)

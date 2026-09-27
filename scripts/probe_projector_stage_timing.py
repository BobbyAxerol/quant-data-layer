#!/usr/bin/env python3
"""Read-only bounded canonical/state/Redis timing; never change runtime tuning."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import time

from scripts.probe_exact_market_lineage import Evidence


def admitted_query_timeout(remaining_seconds):
    # Stop admitting at the observation boundary, but do not shorten an RPC's
    # normal timeout and misclassify window cancellation as service failure.
    return 2.5 if remaining_seconds > 0 else None


def reference_metadata(response):
    results = []
    for item in response.get("results", []):
        out = {k: item[k] for k in ("instrument_uid", "product", "status", "problem") if k in item}
        data = item.get("data")
        if data:
            out["data"] = {k: data[k] for k in ("status", "received_at_ns", "coverage", "lineage",
                "error_code", "error_detail", "cache_hit", "coalesced", "quality") if k in data}
            allowed = {"native_symbol", "execution_view", "freshness_basis", "source_event_time_ns",
                "provider_confirmation_ns", "connection_generation", "gateway_lease_epoch", "delivery_stage",
                "spool_watermark_offset", "event_recency_policy", "recency_mode", "provider_session_state",
                "provider_session_liveness_ms", "provider_session_checked_at_ns",
                "component_mark_received_at_ns", "component_index_received_at_ns",
                "component_mark_quiet_after_ms", "component_index_quiet_after_ms"}
            out["data"]["observations"] = [dict(observed_at_ns=o.get("observed_at_ns"),
                labels={k:v for k,v in o.get("labels", {}).items() if k in allowed},
                field_names=[f["name"] for f in o.get("fields", [])]) for o in data.get("observations", [])]
        results.append(out)
    return dict(partial=response.get("partial"), results=results)


async def query_probe(args, evidence, deadline):
    import yaml
    from scripts.accept_consumer_realms import requirement
    from scripts.probe_exact_market_lineage import exact_view
    from qdl_sdk.client import AsyncDataLayerClient
    from qdl_sdk.credentials import RotatingJwtCredentialProvider
    from qdl_sdk.reference import ReferenceRequirement, ReferenceProduct
    from qdl_sdk.models import Grade
    from qdl_sdk.tls import WorkloadTlsConfig
    from qdl_sdk.transport import RestQueryTransport, GrpcStreamTransport
    packet = Path(args.packet)
    cid = "trading-system.live.stable"
    policy = json.loads((packet / "reader-public/jwt-config.json").read_text())
    manifest = yaml.safe_load((packet / "manifests-crypto" / (cid + ".yaml")).read_text())
    req = requirement(next(r for r in manifest["spec"]["requirements"]
        if r["instrument_uid"] == args.uid and r["feed"] == "MARK_INDEX_PRICE"))
    ref = ReferenceRequirement(instrument_uid=req.instrument_uid, product=ReferenceProduct.MARK_INDEX_PRICE,
        consumer_grade=Grade.EXECUTION, source_policy_id=req.source_policy_id, limit=1, page_size=1, max_pages=1,
        max_freshness_ms=req.max_freshness_ms, event_recency_policy=req.event_recency_policy,
        max_session_liveness_ms=req.max_session_liveness_ms, require_full_coverage=True)
    evidence.put("query_scope", reference_requirement=ref.model_dump(mode="json"), require_all=False,
                 generic_snapshot_is_not_ts_reference_path=True)
    identity_path = Path(args.identity)
    creds = RotatingJwtCredentialProvider(private_key_file=identity_path / "private.key",
        key_id=cid + "-rs256-v1", algorithm="RS256", issuer=policy["issuer"], audience=policy["audience"],
        subject=manifest["metadata"]["subject"], environment="live",
        roles=("market_data_reader", "historical_reader", "stream_consumer"),
        consumer_manifest_revision=manifest["metadata"]["revision"], lifetime_seconds=300, refresh_before_seconds=30)
    tls = WorkloadTlsConfig(identity_path / "ca.crt", identity_path / "client.crt", identity_path / "client.key")
    class ExactTransport(RestQueryTransport):
        last = None
        async def snapshot(self, requirement, *, consumer_id):
            self.last = None
            value = await super().snapshot(requirement, consumer_id=consumer_id)
            self.last = exact_view(value)
            return value
        async def reference_batch(self, requirements, *, consumer_id, require_all):
            self.last = None
            value = await super().reference_batch(requirements, consumer_id=consumer_id, require_all=require_all)
            self.last = reference_metadata(value)
            return value
    clients = []
    try:
        for replica in (1, 2):
            transport = ExactTransport(f"https://query_v2_{replica}:8200", credential_provider=creds, tls=tls, timeout_seconds=2)
            stream = GrpcStreamTransport("unused.invalid:8210", credential_provider=creds, tls=tls)
            clients.append((replica, transport, AsyncDataLayerClient(query_transport=transport, stream_transport=stream, consumer_id=cid)))
        pair_id = 0
        while time.monotonic() < deadline:
            pair_id += 1
            for replica, transport, client in clients:
                for path in ("generic_snapshot", "ts_reference_batch"):
                    remaining = deadline - time.monotonic()
                    request_timeout = admitted_query_timeout(remaining)
                    if request_timeout is None:
                        return
                    row = dict(pair_id=pair_id, replica=replica, path=path,
                               request_started_at_ns=time.time_ns(), request_timeout_seconds=request_timeout,
                               observation_remaining_seconds=remaining)
                    try:
                        async with asyncio.timeout(request_timeout):
                            if path == "generic_snapshot":
                                await client.snapshot(req)
                            else:
                                await client.reference_batch((ref,), require_all=False)
                        row["status"] = "TYPED_RESPONSE"
                    except Exception as error:
                        row.update(status="REFUSED", code=getattr(error, "code", type(error).__name__), diagnostics=getattr(error, "diagnostics", None))
                    evidence.put("query", **row, response_at_ns=time.time_ns(), exact_view=transport.last,
                                 completed_after_observation_window=time.monotonic() > deadline)
            await asyncio.sleep(.5)
    finally:
        for _, _, client in clients:
            await client.close()


def classify_stage(canonical_ns, state_ns, redis_ns):
    if state_ns is None:
        return "CANONICAL_TO_REDIS_ONLY_STATE_UNAVAILABLE"
    if state_ns < canonical_ns or redis_ns < state_ns:
        return "OBSERVER_ORDER_INVERSION_NOT_NEGATIVE_LATENCY"
    return "OBSERVED_STAGE_INTERVALS_NOT_BROKER_COMMIT_TIMES"


def counter_rate(before, after, name):
    elapsed = after["at_ms"] - before["at_ms"]
    delta = after["stage_a"][name] - before["stage_a"][name]
    if elapsed <= 0 or delta < 0 or before["started_ms"] != after["started_ms"]:
        return None
    return delta * 1000 / elapsed


def identity(event, payload):
    return dict(uid=event.instrument_uid, event_id=bytes(event.event_id).hex(),
                canonical_sha256=hashlib.sha256(payload).hexdigest(),
                source_event_time_ns=event.source_event_time_ns,
                received_at_ns=event.received_at_ns,
                source_session_id=event.source_session_id,
                connection_generation=event.connection_generation,
                correlation_id=event.correlation_id)


async def run(args):
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    import redis.asyncio as redis
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    from qdl.projection.kn_state_codec import decode_frame, decode_latest_value, state_partition
    from qdl.projection.state_contract import LogicalProductKey
    from scripts.kn_canonical_mirror import source_config

    if not 1 <= args.seconds <= 180:
        raise ValueError("duration must be 1..180 seconds")
    lpk = LogicalProductKey("paper", "OKX", "SWAP", args.uid, "MARK_INDEX_PRICE")
    product = lpk.encode()
    partition = state_partition(lpk, 6)
    prefix = "kn3:paper:"
    pointer = prefix + "ptr:" + product
    ckpt = prefix + "ckpt:md.latest.v2:" + str(partition)
    evidence = Evidence(args.output, max_bytes=24 * 1024 * 1024)
    config = source_config(argparse.Namespace(source_bootstrap=args.bootstrap,
        group_prefix="qdl-r1-reference-parity-", ca=args.ca, cert=args.cert, key=args.key))
    # These settings affect only this observer, never a native runtime consumer.
    config.update({"fetch.wait.max.ms": 20, "fetch.queue.backoff.ms": 10,
                   "enable.partition.eof": True, "client.id": "projector-stage-observer"})
    consumer = Consumer(config)
    cache = redis.Redis.from_url(args.redis_url, socket_timeout=3, socket_connect_timeout=3)
    errors = []
    counts = {"canonical": 0, "state": 0, "redis": 0, "status": 0}
    started = time.monotonic()
    deadline = started + args.seconds
    eof = {}
    try:
        evidence.put("scope", schema="qdl.projector-stage-timing.v1", product=product,
            duration_seconds=args.seconds, test_provenance=False, production_writes=0,
            kafka_commits=False, state_partition=partition,
            script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            observer_fetch_wait_ms=20, observer_fetch_queue_backoff_ms=10,
            timing_semantics="local observations, not exact broker commit/apply timestamps")
        assignments = []
        for topic in ("md.canonical.v2", "md.latest.v2"):
            metadata = consumer.list_topics(topic, timeout=5).topics[topic]
            if metadata.error:
                error = dict(code="TOPIC_AUTHORIZATION_FAILED" if metadata.error.code() == 29 else "TOPIC_UNAVAILABLE",
                             kafka_code=metadata.error.code(), topic=topic)
                errors.append(error)
                evidence.put("access", **error)
                continue
            partitions = sorted(metadata.partitions) if topic == "md.canonical.v2" else [partition]
            for p in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
                offset = max(low, high - 100)
                assignments.append(TopicPartition(topic, p, offset))
                evidence.put("start", topic=topic, partition=p, low=low, high=high, offset=offset)
        consumer.assign(assignments)

        async def kafka_loop():
            while time.monotonic() < deadline:
                for _ in range(100):
                    msg = consumer.poll(0)
                    if msg is None:
                        break
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            eof[f"{msg.topic()}:{msg.partition()}"] = dict(offset=msg.offset(), observed_at_ns=time.time_ns())
                            continue
                        raise RuntimeError("KAFKA_READ_ERROR")
                    observed = time.time_ns()
                    if msg.topic() == "md.canonical.v2":
                        body = msg.value()
                        event = EventEnvelope.FromString(body)
                        if event.instrument_uid != args.uid or event.WhichOneof("payload") != "mark_index_price":
                            continue
                        kind = "canonical"
                        source = dict(partition=msg.partition(), offset=msg.offset())
                    else:
                        if msg.key() != product.encode() or not msg.value():
                            continue
                        frame = decode_frame(msg.value())
                        body = frame.envelope
                        event = EventEnvelope.FromString(body)
                        source = dict(topic_id=frame.source.topic_id, partition=frame.source.partition,
                                      offset=frame.source.offset)
                        kind = "state"
                    evidence.put(kind, observed_at_ns=observed, topic=msg.topic(),
                        partition=msg.partition(), offset=msg.offset(), source=source,
                        kafka_timestamp_type=msg.timestamp()[0], kafka_timestamp_ms=msg.timestamp()[1],
                        **identity(event, body))
                    counts[kind] += 1
                await asyncio.sleep(.005)

        async def redis_loop():
            while time.monotonic() < deadline:
                begin = time.time_ns()
                ready = await cache.hmget(pointer, "ready", "fence")
                if ready[0] is None:
                    raise RuntimeError("REDIS_NO_GENERATION")
                key = prefix + "l:" + ready[0].decode() + ":" + product
                async with cache.pipeline(transaction=False) as pipe:
                    pipe.hmget(key, "v", "t", "p", "o")
                    pipe.hmget(pointer, "ready", "fence")
                    pipe.hmget(ckpt, "next", "at_ms", "fence")
                    values, after, checkpoint = await pipe.execute()
                end = time.time_ns()
                if ready != after:
                    evidence.put("redis_generation_changed", request_started_at_ns=begin)
                    continue
                if values[0] is None:
                    raise RuntimeError("REDIS_LATEST_MISSING")
                decoded = decode_latest_value(values[0])
                if decoded.source_offset != int(values[3]):
                    raise RuntimeError("REDIS_SOURCE_OFFSET_MISMATCH")
                event = EventEnvelope.FromString(decoded.canonical)
                evidence.put("redis", request_started_at_ns=begin, response_at_ns=end,
                    generation=int(ready[0]), fence=int(ready[1] or 0),
                    source=dict(topic_id=values[1].decode(), partition=int(values[2]), offset=int(values[3])),
                    state_checkpoint_next_offset=int(checkpoint[0]) if checkpoint[0] else None,
                    state_checkpoint_at_ms=int(checkpoint[1]) if checkpoint[1] else None,
                    state_checkpoint_fence=int(checkpoint[2]) if checkpoint[2] else None,
                    **identity(event, decoded.canonical))
                counts["redis"] += 1
                await asyncio.sleep(.025)

        async def status_loop():
            previous = {}
            while time.monotonic() < deadline:
                for i, path in enumerate(args.status):
                    status = json.loads(Path(path).read_text())
                    if status["at_ms"] != previous.get(i):
                        evidence.put("status", replica=i + 1, status=status)
                        counts["status"] += 1
                        previous[i] = status["at_ms"]
                await asyncio.sleep(.5)

        tasks = [asyncio.create_task(query_probe(args, evidence, deadline))]
        tasks.extend(asyncio.create_task(fn()) for fn in (kafka_loop, redis_loop, status_loop))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as error:
        errors.append(dict(code=type(error).__name__))
    finally:
        consumer.close()
        await cache.aclose()
        evidence.put("summary", counts=counts, errors=errors, last_eof=eof,
            elapsed_seconds=time.monotonic() - started, production_writes=0,
            stage_split_proven=False)
        evidence.close()
    print(json.dumps(dict(counts=counts, errors=errors)))
    return not errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("ca", "cert", "key", "output", "packet", "identity"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--status", action="append", default=[])
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--uid", default="fb26214c-7b9b-5961-95b2-55154755af0f")
    parser.add_argument("--redis-url", default="redis://market_cache:6379/0")
    parser.add_argument("--bootstrap", default="kafka1:9092,kafka2:9092,kafka3:9092")
    raise SystemExit(0 if asyncio.run(run(parser.parse_args())) else 1)

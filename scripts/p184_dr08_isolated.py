#!/usr/bin/env python3
"""DR08 driver for a parent-owned, disposable Kafka/core/Stream packet.

Host entrypoint --orchestrate NEW_EXTERNAL_DIRECTORY --development creates the
bounded disposable stack using existing images and public core configuration.
The inner --packet/--output driver runs only in its isolated container namespace.
Fresh TLS/JWT/cursor material is generated locally; Slice is reused with explicit
isolated manifest overrides. A real Query read view behind
Stream GetSnapshot is required; hot-read or locally signed offsets are NOT a
replacement for fresh snapshot recovery.

Packet JSON keys: namespace, development, ts_source_sha, ts_image_digest,
images (kafka/core/stream/python/query immutable sha256 IDs), query_provenance
(candidate image_ref and inspected actual_image_digest), restart_samples (final
5-10, development 1-10; defaults 5 and 1), bootstrap (localhost:9092),
raw_topic, raw_template (base64 RawProviderEnvelope, test_provenance=true),
consumer_id, fast_consumer_id, instrument_uid, metrics_path, and slice (the
existing Slice args: bundle, cursor_keys, profile, probes, topic_id,
route_generation, target=localhost:8210). Metrics must be a parent-owned bounded
JSONL capture of this isolated Stream's stdout, beginning before this driver.
Both consumers must have the same BINANCE USDM trade requirement, quota1000.
Stream must retain max_replay_events=10000, report metrics every second, and
use a queue byte budget <=32MiB. No production credentials or mounts are allowed.

Image set: IMAGES / QUERY_IMAGE_REF below are DEFAULTS only. --orchestrate accepts
--image ROLE=sha256:<id> (repeatable; roles query/core/stream/python/kafka/redis) and
--query-image-ref qdl-v2-python:<tag>; the parent passes the resolved set into every
container as P184_DR08_IMAGES (JSON) and P184_DR08_QUERY_IMAGE_REF, and the in-container
checks compare the packet against those attested values (defaults when absent).

Parent bounds: Kafka 1CPU/1536MiB; core .5CPU/256MiB; Stream .5CPU/512MiB;
Python .75CPU/512MiB; quota Redis .1CPU/64MiB; Query .5CPU/512MiB. --rm, no published ports, no shared mounts, tmpfs/bounded logs,
ten-minute deadline. The orchestrator validates labels and cleans only its owned names.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import signal
import statistics
import sys
import time
from types import SimpleNamespace
import uuid

SCHEMA = "p184.surface-evidence-provenance.v1"
CELLS = tuple("DR08." + name + "@ANY" for name in (
    "SLOW_CONSUMER_BOUNDED_QUEUE", "CONSUMER_RESTART_RECOVERY_MEASURED",
    "NO_CROSS_CONSUMER_STARVATION", "ALERT_PATH_OBSERVABLE"))
QUEUE = 1000
REPLAY = 10000
MAX_METRICS_BYTES = 8 * 1024 * 1024
QUEUE_BYTES = 32 * 1024 * 1024
QUERY_IMAGE_REF = "qdl-v2-python:2.2.3-rc2-43bbcf0"
QUERY_IMAGE_DIGEST = "sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f"
MIN_FINAL_RESTARTS = 5
MAX_RESTARTS = 10


def restart_count(packet):
    count = packet.get("restart_samples", 1 if packet.get("development") is True else MIN_FINAL_RESTARTS)
    minimum = 1 if packet.get("development") is True else MIN_FINAL_RESTARTS
    if type(count) is not int or not minimum <= count <= MAX_RESTARTS:
        raise ValueError(f"restart_samples must be an integer in [{minimum}, {MAX_RESTARTS}]")
    return count


ENV_IMAGES = "P184_DR08_IMAGES"
ENV_QUERY_IMAGE_REF = "P184_DR08_QUERY_IMAGE_REF"
IMAGE_ROLES = ("query", "core", "stream", "python", "kafka", "redis")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
QUERY_REF = re.compile(r"qdl-v2-python:[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")


def parse_image_overrides(values):
    """--image ROLE=sha256:<64hex> (repeatable) -> {role: id}; unknown roles and duplicates refused."""
    out = {}
    for value in values or ():
        role, sep, image = str(value).partition("=")
        if not sep or role not in IMAGE_ROLES or role in out or not IMAGE_ID.fullmatch(image):
            raise ValueError(f"--image ROLE=sha256:<64hex> with ROLE in {IMAGE_ROLES}, once each: {value!r}")
        out[role] = image
    return out


def resolve_images(overrides=None):
    """The pinned IMAGES with explicit overrides applied; every role must be an immutable image ID."""
    images = dict(IMAGES)
    for role, image in (overrides or {}).items():
        if role not in IMAGE_ROLES:
            raise ValueError("unknown image role: " + str(role))
        images[role] = image
    for role in IMAGE_ROLES:
        if not isinstance(images.get(role), str) or not IMAGE_ID.fullmatch(images[role]):
            raise ValueError(f"immutable {role} image ID required")
    return images


def resolve_query_ref(images, query_image_ref=None):
    """The Query image label must name the Query digest it runs: a changed digest needs its own ref."""
    ref = query_image_ref or QUERY_IMAGE_REF
    if not QUERY_REF.fullmatch(ref):
        raise ValueError("--query-image-ref must be qdl-v2-python:<tag>")
    if (images["query"] == QUERY_IMAGE_DIGEST) != (ref == QUERY_IMAGE_REF):
        raise ValueError("query image digest and --query-image-ref must change together")
    return ref


def expected_query(environ=None):
    """(image_ref, digest) the parent attested via env, else the pinned defaults."""
    environ = os.environ if environ is None else environ
    images = json.loads(environ[ENV_IMAGES]) if environ.get(ENV_IMAGES) else IMAGES
    return environ.get(ENV_QUERY_IMAGE_REF) or QUERY_IMAGE_REF, images["query"]


def validate_query_provenance(packet, expected=None):
    ref, digest = expected or expected_query()
    query = packet.get("query_provenance", {})
    if (not isinstance(query, dict) or packet.get("images", {}).get("query") != digest
            or query.get("image_ref") != ref
            or query.get("actual_image_digest") != digest):
        raise ValueError(f"Query must be verified against candidate {ref.split(':', 1)[-1]}, not production v2.2.2")


def recovery_summary(samples):
    """Reject copied samples and require ordered, independently timed restarts."""
    if not isinstance(samples, list) or not 1 <= len(samples) <= MAX_RESTARTS:
        raise ValueError("bounded measured restart sample list required")
    previous_end, previous_trade = 0, 0
    values = []
    for sample in samples:
        start, end, trade = (sample.get(key) for key in ("started_ns", "first_event_ns", "trade_id"))
        if (any(type(value) is not int for value in (start, end, trade))
                or not previous_end < start < end or trade <= previous_trade
                or sample.get("code") != "OK" or sample.get("count") != 1):
            raise ValueError("independent successful consumer restart measurements required")
        elapsed = (end - start) / 1_000_000
        if not 0 < elapsed < 60000 or sample.get("restart_to_first_event_ms") != elapsed:
            raise ValueError("invalid measured restart latency")
        values.append(elapsed)
        previous_end, previous_trade = end, trade
    return {"measurement_n": len(values), "restart_samples": samples,
            "restart_to_first_event_ms_samples": values,
            "restart_to_first_event_ms_p50": statistics.median(values),
            "restart_to_first_event_ms_max": max(values)}


def measure_restarts(packet, producer, start, token, trade_id):
    samples = []
    for index in range(restart_count(packet)):
        if index:
            producer.publish(1)
            trade_id = producer.next_id - 1
            token, _ = asyncio.run(snapshot(packet, packet["consumer_id"], trade_id))
        child = start(packet["consumer_id"], token, stop_after=1)
        try:
            child.wait("live")
            producer.publish(1)
            recovered = child.wait("done")
            if recovered.get("last") != trade_id + 1:
                raise ValueError("restart recovered the wrong trade")
            first_ns = recovered.get("first_ns", 0)
            samples.append({"started_ns": child.started_ns, "first_event_ns": first_ns,
                "trade_id": recovered["last"], "code": recovered["code"], "count": recovered["count"],
                "restart_to_first_event_ms": (first_ns - child.started_ns) / 1_000_000})
        finally:
            child.close()
    return recovery_summary(samples)


def validate_packet(packet, *, development):
    """Reject accidental shared endpoints and ambiguous provenance before I/O."""
    namespace = packet.get("namespace", "")
    if not re.fullmatch(r"p184-dr08-[a-z0-9-]{8,64}", namespace):
        raise ValueError("a unique p184-dr08 namespace is required")
    if os.environ.get("P184_DR08_ISOLATED_NAMESPACE") != namespace:
        raise ValueError("parent must attest the isolated container namespace")
    if type(packet.get("development")) is not bool or packet["development"] != development:
        raise ValueError("packet and execution mode differ; prepare a fresh packet")
    if packet.get("bootstrap") != "localhost:9092" or packet["slice"].get("target") != "localhost:8210":
        raise ValueError("only the isolated namespace's loopback endpoints are allowed")
    if packet.get("consumer_id") == packet.get("fast_consumer_id"):
        raise ValueError("two distinct consumer identities are required")
    for role in ("kafka", "core", "stream", "python"):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", packet.get("images", {}).get(role, "")):
            raise ValueError(f"immutable {role} image ID required")
    if os.environ.get(ENV_IMAGES) and packet.get("images") != json.loads(os.environ[ENV_IMAGES]):
        raise ValueError("packet images differ from the parent-attested image set")
    restart_count(packet)
    validate_query_provenance(packet)
    if packet.get("stream_binary_override"):
        declared = packet["stream_binary_override"]
        if declared != candidate_stream_provenance("/work/candidate-stream", development=development):
            raise ValueError("candidate binary digest mismatch")
    if not development:
        sha, image = packet.get("ts_source_sha", ""), packet.get("ts_image_digest", "")
        if not re.fullmatch(r"[0-9a-f]{40}", sha) or len(set(sha)) == 1:
            raise ValueError("final TS source SHA required")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image) or len(set(image[7:])) == 1:
            raise ValueError("final TS image digest required")
    if not packet.get("raw_topic") or not packet.get("metrics_path"):
        raise ValueError("isolated raw topic and metrics capture required")


def evidence_rows(packet, facts, *, development, run_id):
    """Development rows deliberately cannot satisfy the TS matrix checker."""
    validate_query_provenance(packet)
    if packet.get("stream_binary_override") and not development:
        raise ValueError("binary overlay cannot emit qualification evidence")
    mode = "DEVELOPMENT" if development else "QUALIFICATION"
    header = {"provenance": {
        "schema": SCHEMA, "source_sha": packet["ts_source_sha"],
        "image_digest": packet["ts_image_digest"], "mode": mode, "run_id": run_id,
        "data_provenance": "SYNTHETIC_TEST_ONLY", "images": packet["images"],
        "query_provenance": packet.get("query_provenance", {}),
        "stream_binary_override": packet.get("stream_binary_override"),
        "packet_sha256": hashlib.sha256(json.dumps(packet, sort_keys=True).encode()).hexdigest(),
        "scope": "same-host isolated DL fault test; not TS consumer or real-provider certification",
    }}
    rows = [header]
    for cell in CELLS:
        fact = facts.get(cell, {})
        observed = fact.get("observed") is True
        passed = observed and fact.get("passed") is True
        if cell == CELLS[1] and not development:
            try:
                summary = recovery_summary(fact.get("restart_samples"))
                passed = passed and summary["measurement_n"] == restart_count(packet)
                fact = {**fact, **summary}
            except (ValueError, TypeError, AttributeError):
                passed = False
            if not passed:
                observed = False
        rows.append({"cell": cell, "level": "ISOLATED_FAULT", "mode": mode,
                     "run_id": run_id, "ok": passed and not development,
                     "outcome": "OBSERVED" if observed and not development else "NOT_OBSERVED",
                     "development_pass": passed if development else None,
                     "measurement": fact})
    return rows


def read_metrics(path, *, since_ns):
    path = Path(path)
    if path.stat().st_size > MAX_METRICS_BYTES:
        raise ValueError("metrics capture exceeds 8MiB bound")
    rows = []
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue  # Rust startup text and an in-progress final line.
        if row.get("event") == "qdl_kn_gateway_metrics":
            rows.append(row)
    # since_ns is carried in evidence; the parent starts an exclusive capture
    # before the run. Counter deltas, not file timestamps, delimit the test.
    if not rows:
        raise ValueError(f"no native Stream metrics available for run {since_ns}")
    return rows


def slow_queue_observation(path, consumer, *, subscription_id, wire_code):
    path = Path(path)
    if path.stat().st_size > MAX_METRICS_BYTES:
        raise ValueError("metrics capture exceeds bound")
    if wire_code != "RESOURCE_EXHAUSTED":
        return None
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(row, dict) and row.get("event") == "qdl_kn_subscription_closed"
                and row.get("consumer_id") == consumer and row.get("subscription_id") == subscription_id
                and row.get("outcome") in ("backpressure", "Some resource has been exhausted")
                and row.get("queued") == QUEUE and 0 < row.get("queued_bytes", 0) <= QUEUE_BYTES):
            return {key: row[key] for key in ("subscription_id", "queued", "queued_bytes", "outcome")}
    return None


def wait_metrics(path, *, since_ns, timeout=20):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return read_metrics(path, since_ns=since_ns)
        except (FileNotFoundError, ValueError):
            if time.monotonic() >= deadline:
                raise TimeoutError("isolated metrics baseline unavailable")
            time.sleep(.1)


def metric_verdict(before, after):
    required = ("overflow", "expired", "queue_bytes", "rss_bytes", "cpu_seconds")
    if any(key not in row for row in (before, after) for key in required):
        return False
    return (after["overflow"] > before["overflow"] and after["expired"] > before["expired"]
            and 0 <= after["queue_bytes"] <= QUEUE_BYTES
            and after["cpu_seconds"] >= before["cpu_seconds"])


def harness(packet):
    # Keep all profile, manifest, JWT and cursor behavior in the native harness.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from kn_native_slice_probe import Slice
    from qdl.consumer.manifest import ConsumerManifestLoader
    runner = Slice(SimpleNamespace(**packet["slice"]))
    runner.manifests = {m.consumer_id: m for m in (
        ConsumerManifestLoader.load(Path(path)) for path in packet["isolated_manifest_paths"])}
    required = {packet["consumer_id"], packet["fast_consumer_id"]}
    if set(runner.manifests) != required or set(runner.identities) != required:
        raise ValueError("only the two disposable consumer identities may be loaded")
    if any(not cid.startswith(packet["namespace"] + ".") for cid in required):
        raise ValueError("consumer identities must belong to this isolated namespace")
    return runner


async def channel_for(runner, consumer):
    import grpc
    identity = runner.identities[consumer]["tls"]
    credentials = grpc.ssl_channel_credentials(
        Path(identity["ca_file"]).read_bytes(), Path(identity["key_file"]).read_bytes(),
        Path(identity["cert_file"]).read_bytes())
    channel = grpc.aio.secure_channel(runner.args.target, credentials, options=(
        ("grpc.http2.bdp_probe", 0), ("grpc.enable_retries", 0), ("grpc.enable_http_proxy", 0),
        ("grpc.max_receive_message_length", 1024 * 1024)))
    try:
        await asyncio.wait_for(channel.channel_ready(), 20)
    except TimeoutError:
        state = channel.get_state().name
        await channel.close()
        raise TimeoutError("CHANNEL_READY_TIMEOUT:" + state) from None
    return channel


def rpc_inputs(runner, packet, consumer):
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    domain = runner.requirement(consumer, packet["instrument_uid"], "TRADE")
    proto = sdk_requirement(SimpleNamespace(requirement=domain)).to_proto()
    metadata = (("authorization", "Bearer " + runner.jwt(consumer)),
                ("x-qdl-consumer-id", consumer), ("x-qdl-purpose", "INTERNAL_ALPHA"))
    return domain, proto, metadata


async def snapshot(packet, consumer, expected_trade_id):
    import grpc
    from qdl.query.v2 import query_pb2 as pb
    runner = harness(packet)
    domain, requirement, metadata = rpc_inputs(runner, packet, consumer)
    channel = await channel_for(runner, consumer)
    try:
        rpc = channel.unary_unary("/qdl.query.v2.MarketDataStreamService/GetSnapshot",
            request_serializer=pb.GetSnapshotRequest.SerializeToString,
            response_deserializer=pb.GetSnapshotResponse.FromString)
        deadline = time.monotonic() + 20
        last_refusal = "NO_MATCHING_MARKER"
        while True:
            try:
                result = await rpc(pb.GetSnapshotRequest(consumer_id=consumer, requirement=requirement),
                    metadata=metadata, timeout=min(3, max(.01, deadline - time.monotonic())))
                if result.events and int(result.events[-1].trade.native_trade_id) == expected_trade_id:
                    break
            except grpc.aio.AioRpcError as error:
                last_refusal = error.code().name + ":" + error.details()[:160]
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.FAILED_PRECONDITION):
                    raise
            if time.monotonic() >= deadline:
                raise TimeoutError("SNAPSHOT_MARKER_TIMEOUT:" + last_refusal)
            await asyncio.sleep(.1)
        if not result.events or not runner.cursor_valid(result.stream_cursor, consumer, domain):
            raise ValueError("fresh snapshot requires actual events and a valid server cursor")
        if any(event.instrument_uid != packet["instrument_uid"] or not event.HasField("trade")
               for event in result.events):
            raise ValueError("snapshot product mismatch")
        return result.stream_cursor, result.watermark_offset
    finally:
        await channel.close()


async def consume(packet, consumer, token, pipe, *, stop_after, stall):
    import grpc
    from qdl.query.v2 import query_pb2 as pb
    runner = harness(packet)
    _, requirement, metadata = rpc_inputs(runner, packet, consumer)
    channel = await channel_for(runner, consumer)
    count, last, controls, first_ns = 0, None, [], None
    try:
        rpc = channel.unary_stream("/qdl.query.v2.MarketDataStreamService/Subscribe",
            request_serializer=pb.SubscribeRequest.SerializeToString,
            response_deserializer=pb.SubscribeResponse.FromString)
        call = rpc(pb.SubscribeRequest(consumer_id=consumer, requirement=requirement,
                   cursor_token=token, max_buffer_events=QUEUE), metadata=metadata, timeout=180)
        try:
            async for response in call:
                record = response.record
                if record.HasField("control"):
                    controls.append(record.control.code)
                    if record.control.code == "LIVE":
                        pipe.send({"kind": "live", "count": count})
                        if stall:
                            os.kill(os.getpid(), signal.SIGSTOP)
                    continue
                if not record.HasField("event") or not record.event.HasField("trade"):
                    raise ValueError("unexpected stream product")
                if record.event.instrument_uid != packet["instrument_uid"]:
                    raise ValueError("stream identity mismatch")
                current = int(record.event.trade.native_trade_id)
                if last is not None and current != last + 1:
                    raise ValueError("trade identity loss, duplicate or reordering")
                last = current
                count += 1
                first_ns = first_ns or time.monotonic_ns()
                if count % 100 == 0:
                    pipe.send({"kind": "progress", "count": count, "last": last})
                if stop_after and count >= stop_after:
                    break
            pipe.send({"kind": "done", "count": count, "last": last,
                       "first_ns": first_ns, "controls": controls, "code": "OK"})
        except grpc.aio.AioRpcError as error:
            pipe.send({"kind": "done", "count": count, "last": last, "controls": controls,
                       "code": error.code().name, "detail": error.details()[:256]})
        finally:
            call.cancel()
    finally:
        await channel.close()


def child_main(packet, consumer, token, pipe, stop_after, stall):
    try:
        asyncio.run(consume(packet, consumer, token, pipe, stop_after=stop_after, stall=stall))
    except Exception as error:
        # No exception strings: third-party errors can include auth material.
        pipe.send({"kind": "error", "type": type(error).__name__})
    finally:
        pipe.close()


class Consumer:
    def __init__(self, packet, consumer, token, *, stop_after=0, stall=False):
        ctx = multiprocessing.get_context("spawn")
        self.pipe, sender = ctx.Pipe(duplex=False)
        self.process = ctx.Process(target=child_main,
            args=(packet, consumer, token, sender, stop_after, stall))
        self.started_ns = time.monotonic_ns()
        self.process.start()
        sender.close()
        self.progress = 0

    def wait(self, kind, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.pipe.poll(min(.2, max(0, deadline - time.monotonic()))):
                continue
            row = self.pipe.recv()
            self.progress = max(self.progress, row.get("count", 0))
            if row["kind"] == kind:
                return row
            if row["kind"] in ("done", "error"):
                raise RuntimeError(f"consumer ended before {kind}: {row}")
        raise TimeoutError(f"consumer did not reach {kind}")

    def resume(self):
        os.kill(self.process.pid, signal.SIGCONT)

    def close(self):
        if self.process.is_alive():
            self.resume()
            self.process.terminate()
        self.process.join(5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)
        self.pipe.close()


class RawProducer:
    def __init__(self, packet):
        from confluent_kafka import Producer
        from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
        from qdl.raw.envelope import validate_raw_envelope
        self.packet = packet
        self.template = RawProviderEnvelope.FromString(base64.b64decode(packet["raw_template"], validate=True))
        validate_raw_envelope(self.template)
        if not self.template.test_provenance or self.template.venue != "BINANCE" or self.template.market != "USDM":
            raise ValueError("requires explicitly test-only BINANCE USDM raw template")
        if not self.template.native_channel.lower().endswith(("@aggtrade", "@trade")):
            raise ValueError("requires a native trade template")
        self.producer = Producer({**kafka_security(packet),
            "enable.idempotence": True, "message.timeout.ms": 20000,
            "queue.buffering.max.messages": 1000, "queue.buffering.max.kbytes": 4096})
        self.next_id = 1

    def publish(self, count):
        from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
        errors = []
        def delivered(error, _message):
            if error is not None:
                errors.append(str(error.code()))
        for _ in range(count):
            raw = RawProviderEnvelope()
            raw.CopyFrom(self.template)
            index, now = self.next_id, time.time_ns()
            self.next_id += 1
            raw.capture_id = uuid.uuid4().bytes
            raw.received_at_ns = now
            raw.raw_frame_bytes = json.dumps({"e": "trade" if raw.native_channel.lower().endswith("@trade") else "aggTrade", "E": now // 1_000_000,
                "T": now // 1_000_000, "s": raw.native_symbol, "a": index, "t": index,
                "p": "100", "q": "1", "f": index, "l": index, "m": False}).encode()
            raw.raw_frame_sha256 = hashlib.sha256(raw.raw_frame_bytes).digest()
            self.producer.produce(self.packet["raw_topic"], key=raw.subscription_id.encode(),
                                  value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)], on_delivery=delivered)
            self.producer.poll(0)
        if self.producer.flush(25) or errors:
            raise RuntimeError("isolated raw publication failed")


def wait_seed(packet):
    from confluent_kafka import Consumer as KafkaConsumer, TopicPartition
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    topic = json.loads(Path(packet["slice"]["bundle"]).read_text())["catalog"]["canonical_stream"]
    consumer = KafkaConsumer({**kafka_security(packet), "group.id": "kn-" + packet["namespace"] + "-oracle",
        "enable.auto.commit": False, "enable.auto.offset.store": False, "isolation.level": "read_committed"})
    started = time.monotonic()
    try:
        consumer.assign([TopicPartition(topic, 0, 0)])
        while time.monotonic() - started < 25:
            message = consumer.poll(.2)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("isolated canonical seed read failed")
            event = EventEnvelope.FromString(message.value())
            if event.instrument_uid != packet["instrument_uid"] or event.trade.native_trade_id != "1":
                raise ValueError("isolated canonical seed identity mismatch")
            from qdl.runtime.stable_catalog import StableSourceCatalog
            catalog = StableSourceCatalog.load(Path(packet["slice"]["bundle"]).parent / "catalog.yaml")
            catalog.binding_for_envelope(event)
            return {"partition": message.partition(), "offset": message.offset(),
                    "committed_seed_wait_ms": (time.monotonic() - started) * 1000}
        raise TimeoutError("SEED_CANONICAL_COMMIT_TIMEOUT")
    finally:
        consumer.close()


def replay_boundary(start, consumer, token, producer, baseline_id):
    """A full replay must reach LIVE before a new marker proves handoff."""
    child = start(consumer, token, stop_after=REPLAY + 1)
    try:
        live = child.wait("live", 90)
        if live.get("count") != REPLAY:
            raise ValueError("exact replay did not reach LIVE at the expected boundary")
        producer.publish(1)
        done = child.wait("done")
        passed = (done.get("code") == "OK" and done.get("count") == REPLAY + 1
                  and done.get("last") == baseline_id + REPLAY + 1
                  and "LIVE" in done.get("controls", []))
        if not passed:
            raise ValueError("post-replay live marker missing or out of order")
        return {**done, "replay_count_at_live": live["count"], "live_marker_count": 1}
    finally:
        child.close()


def run_faults(packet):
    facts, children = {}, []
    stage = "baseline_metrics"
    started = time.time_ns()
    slow_id, fast_id = packet["consumer_id"], packet["fast_consumer_id"]
    def start(consumer, token, **kwargs):
        child = Consumer(packet, consumer, token, **kwargs)
        children.append(child)
        return child
    try:
        before = wait_metrics(packet["metrics_path"], since_ns=started)[-1]
        stage = "raw_producer"
        producer = RawProducer(packet)
        producer.publish(1)
        stage = "seed_canonical_commit"
        seed = wait_seed(packet)
        # Cold assignment is outside the fault window; keep catalog freshness.
        producer.publish(1)
        baseline_id = producer.next_id - 1
        stage = "initial_snapshot"
        old_token, old_offset = asyncio.run(snapshot(packet, slow_id, baseline_id))
        fast_token, _ = asyncio.run(snapshot(packet, fast_id, baseline_id))
        stage = "slow_subscribe"
        slow = start(slow_id, old_token, stall=True)
        slow.wait("live")
        stage = "fast_subscribe"
        fast = start(fast_id, fast_token, stop_after=REPLAY + 1)
        fast.wait("live")
        stage = "stalled_consumer_live_progress"
        progress_samples = []
        stalled_started_ns = time.monotonic_ns()
        slow_resumed = False
        for _ in range(REPLAY // 100):
            producer.publish(100)
            # Backpressure the test producer on the independent fast consumer,
            # not the stalled one. This also bounds IPC and records progress.
            progress = fast.wait("progress")
            if not slow_resumed:
                progress_samples.append({"count": progress["count"],
                    "elapsed_ms": (time.monotonic_ns() - stalled_started_ns) / 1_000_000})
                samples = read_metrics(packet["metrics_path"], since_ns=started)
                if samples and samples[-1].get("overflow", 0) > before.get("overflow", 0):
                    slow.resume()
                    slow_resumed = True
        stage = "slow_terminal"
        if not slow_resumed:
            slow.resume()
        slow_end = slow.wait("done")
        slow.close()
        bounded = slow_end["code"] == "RESOURCE_EXHAUSTED" and "RATE_LIMITED" in slow_end["controls"]
        facts[CELLS[0]] = {"observed": True, "passed": False,
            "configured_queue_events": QUEUE, "slow_terminal": slow_end, "seed_commit": seed}
        stage = "exact_10000_replay"
        replay_end = replay_boundary(start, slow_id, old_token, producer, baseline_id)
        exact_replay = replay_end["replay_count_at_live"] == REPLAY
        fast_end = fast.wait("done")
        fast.close()
        stage = "out_of_range"
        expired = start(slow_id, old_token)
        range_end = expired.wait("done")
        range_observed_ns = time.monotonic_ns()
        expired.close()
        out_of_range = range_end["code"] == "OUT_OF_RANGE" and "backlog" in range_end.get("detail", "")
        stage = "fresh_snapshot"
        producer.publish(1)
        fresh_trade_id = producer.next_id - 1
        snapshot_started_ns = time.monotonic_ns()
        fresh_token, fresh_offset = asyncio.run(snapshot(packet, slow_id, fresh_trade_id))
        snapshot_ms = (time.monotonic_ns() - snapshot_started_ns) / 1_000_000
        stage = "consumer_restart"
        recovery = measure_restarts(packet, producer, start, fresh_token, fresh_trade_id)
        facts[CELLS[1]] = {"observed": True,
            "passed": exact_replay and out_of_range and fresh_offset > old_offset,
            "replayed": replay_end["replay_count_at_live"], "replay_handoff": replay_end, "range_code": range_end["code"],
            "old_snapshot_offset": old_offset, "fresh_snapshot_offset": fresh_offset,
            "baseline_trade_id": baseline_id, "fresh_snapshot_trade_id": fresh_trade_id,
            **recovery, "restart_kind": "consumer_process",
            "snapshot_request_ms": snapshot_ms, "snapshot_request_measurement_n": 1,
            "range_observed_to_first_event_ms": (recovery["restart_samples"][0]["first_event_ns"] - range_observed_ns) / 1_000_000,
            "range_recovery_measurement_n": 1,
            "ack_measured": False,
            "fresh_snapshot_rpc": "MarketDataStreamService/GetSnapshot",
            "snapshot_path": "isolated candidate Query " + packet["query_provenance"]["image_ref"].split(":", 1)[-1]
                + " to canonical hot read; not production v2.2.2 or projector recovery proof"}
        facts[CELLS[2]] = {"observed": True,
            "passed": fast_end["code"] == "OK" and fast_end["count"] == REPLAY + 1 and fast_end["last"] == baseline_id + REPLAY + 1
                and bool(progress_samples) and slow_resumed,
            "fast_events": fast_end["count"], "distinct_identities": True,
            "identity_order_checked": True, "progress_checked_every": 100,
            "progress_while_slow_stopped": progress_samples, "measurement_n": len(progress_samples)}
        time.sleep(2)
        stage = "final_metrics"
        metrics = read_metrics(packet["metrics_path"], since_ns=started)
        after = metrics[-1]
        max_bytes = max(row.get("queue_bytes", QUEUE_BYTES + 1) for row in metrics)
        closed = slow_queue_observation(packet["metrics_path"], slow_id,
            subscription_id=before["subscriptions_opened"] + 1, wire_code=slow_end["code"])
        facts[CELLS[0]]["passed"] = bounded and max_bytes <= QUEUE_BYTES and closed is not None
        facts[CELLS[0]]["observed_closed_queue"] = closed
        facts[CELLS[0]]["sampled_max_queue_bytes"] = max_bytes
        facts[CELLS[3]] = {"observed": True,
            "passed": bounded and out_of_range and metric_verdict(before, after),
            "overflow_delta": after.get("overflow", 0) - before.get("overflow", 0),
            "expired_delta": after.get("expired", 0) - before.get("expired", 0),
            "metrics_samples": len(metrics), "alert_scope": "native controls, status and structured counters; not paging delivery"}
    except Exception as error:
        # Keep any completed observations without turning an incomplete run green.
        for cell in CELLS:
            facts.setdefault(cell, {"observed": False, "passed": False,
                                    "failure_type": type(error).__name__, "failure_stage": stage,
                                    "failure_detail": str(error)[:128] if isinstance(error, TimeoutError) else None,
                                    "failure_code": error.code().name if callable(getattr(error, "code", None)) else None})
    finally:
        for child in children:
            child.close()
    return facts


# ------------------------------------------------- live-budget isolation mode
# Stream gateway live-budget isolation A/B (DL e3b5552, stage-100 2026-10-15
# RCA): several deep lossless subscriptions on DISTINCT keys must not make
# idle readers on other keys overflow. Same isolated stack as DR08 (one
# broker, deployed core, Stream, Query, quota Redis), different scenario:
# three stalled TRADE readers (BTC/ETH/SOL, deep buffers) and idle QUOTE and
# MARK_INDEX_PRICE readers (BNB/DOGE) under the production 32 MiB live budget.
# It writes facts only; the campaign (--isolation-campaign) preregisters the
# acceptance before any run and applies it to both images unchanged.
ISOLATION_MODE = "live_budget_isolation"
ISOLATION_DEEP = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
ISOLATION_IDLE = ("BNBUSDT", "DOGEUSDT")
ISOLATION_IDLE_FEEDS = ("QUOTE", "MARK_INDEX_PRICE")
IDLE_READERS_PER_ROUTE = 8
DEEP_BUFFER = 3000
DEEP_ROUNDS = 12
DEEP_PER_ROUND = 250
# Idle traffic while the deep readers hold the budget (first A/B 20261015T155507Z
# published one idle record per route then and could not discriminate).
IDLE_HOLD_SECONDS = 8
IDLE_HOLD_RATE_HZ = 50
LIVE_RESERVE_BYTES = QUEUE_BYTES // 8  # rust/qdl-stream-gateway/src/memory.rs LIVE_RESERVE_DIVISOR
ISOLATION_ACCEPTANCE = {
    "C1_candidate_idle_closures": "== 0 (client terminal statuses and Stream closures of the idle identity)",
    "C1b_candidate_idle_progress": "every idle reader receives a record published after the last deep burst",
    "C2_candidate_reserve_exhausted": "== 0 (queue_isolation.reserve_exhausted)",
    "C3_candidate_over_floor_refused": "> 0, and every abnormal Stream closure belongs to the deep identity",
    "C4_candidate_deep_resume": "every deep reader resumes from its last resume token and receives every later "
                                "trade id of its key, contiguous, to the last published",
    "D1_baseline_discriminates": "baseline idle closures >= 1",
    "report_only": "peak queue bytes and peak reserve bytes used (1 s metric samples)",
}


def isolation_feed(channel):
    channel = channel.lower()
    for suffix, feed in (("@trade", "TRADE"), ("@bookticker", "QUOTE"), ("@markprice@1s", "MARK_INDEX_PRICE")):
        if channel.endswith(suffix):
            return feed
    return None


def isolation_routes():
    return ([(symbol, "TRADE") for symbol in ISOLATION_DEEP]
            + [(symbol, feed) for symbol in ISOLATION_IDLE for feed in ISOLATION_IDLE_FEEDS])


def isolation_bindings(bindings):
    """The deployed BINANCE USDM core bindings of the scenario, exactly one per route."""
    wanted = set(isolation_routes())
    found = {}
    for row in bindings:
        if row.get("venue") != "BINANCE" or row.get("market") != "USDM":
            continue
        route = (row.get("native_symbol"), isolation_feed(row.get("native_channel", "")))
        if route in wanted:
            if route in found:
                raise ValueError("ambiguous deployed binding for " + repr(route))
            found[route] = row
    missing = sorted(wanted - set(found))
    if missing:
        raise ValueError("deployed core lacks isolation routes: " + repr(missing))
    return [found[route] for route in isolation_routes()]


def isolation_frame(feed, symbol, index, now_ns):
    """A synthetic Binance frame the deployed core normalizes for `feed`."""
    ms = now_ns // 1_000_000
    if feed == "TRADE":
        return {"e": "trade", "E": ms, "T": ms, "s": symbol, "a": index, "t": index,
                "p": "100", "q": "1", "f": index, "l": index, "m": False}
    if feed == "QUOTE":
        return {"e": "bookTicker", "u": index, "E": ms, "T": ms, "s": symbol,
                "b": str(100 + index % 7), "B": "1", "a": str(101 + index % 7), "A": "1"}
    if feed == "MARK_INDEX_PRICE":
        return {"e": "markPriceUpdate", "E": ms, "T": ms, "s": symbol, "p": f"{100 + index / 1000:.3f}",
                "i": "100.000", "P": "100", "r": "0.0001"}
    raise ValueError("unknown isolation feed " + str(feed))


class IsolationProducer:
    """One raw template per route (sealed by --prepare-material); ids per route from 1."""

    def __init__(self, packet):
        from confluent_kafka import Producer
        from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
        from qdl.raw.envelope import validate_raw_envelope
        self.packet = packet
        self.templates = {}
        for route in packet["isolation_routes"]:
            template = RawProviderEnvelope.FromString(base64.b64decode(route["raw_template"], validate=True))
            validate_raw_envelope(template)
            if not template.test_provenance:
                raise ValueError("isolation requires test-only raw templates")
            self.templates[(route["symbol"], route["feed"])] = template
        self.producer = Producer({**kafka_security(packet),
            "enable.idempotence": True, "message.timeout.ms": 20000,
            "queue.buffering.max.messages": 20000, "queue.buffering.max.kbytes": 16384})
        self.next_id = {route: 1 for route in self.templates}

    def publish(self, plan):
        """plan: {(symbol, feed): count}; returns once every record is acknowledged."""
        from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
        errors = []
        def delivered(error, _message):
            if error is not None:
                errors.append(str(error.code()))
        for route, count in plan.items():
            template = self.templates[route]
            for _ in range(count):
                raw = RawProviderEnvelope()
                raw.CopyFrom(template)
                index, now = self.next_id[route], time.time_ns()
                self.next_id[route] += 1
                raw.capture_id = uuid.uuid4().bytes
                raw.received_at_ns = now
                raw.raw_frame_bytes = json.dumps(isolation_frame(route[1], route[0], index, now)).encode()
                raw.raw_frame_sha256 = hashlib.sha256(raw.raw_frame_bytes).digest()
                self.producer.produce(self.packet["raw_topic"], key=raw.subscription_id.encode(),
                    value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                    on_delivery=delivered)
                self.producer.poll(0)
        if self.producer.flush(30) or errors:
            raise RuntimeError("isolated raw publication failed")


def isolation_wait_seed(packet, routes):
    """Offset of the last seed record once every route has one canonical record."""
    from confluent_kafka import Consumer as KafkaConsumer, TopicPartition
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    topic = json.loads(Path(packet["slice"]["bundle"]).read_text())["catalog"]["canonical_stream"]
    wanted = {(route["instrument_uid"], route["payload"]) for route in routes}
    consumer = KafkaConsumer({**kafka_security(packet), "group.id": "kn-" + packet["namespace"] + "-seed",
        "enable.auto.commit": False, "enable.auto.offset.store": False, "isolation.level": "read_committed"})
    started, seen, last = time.monotonic(), set(), None
    try:
        consumer.assign([TopicPartition(topic, 0, 0)])
        while time.monotonic() - started < 45:
            message = consumer.poll(.2)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("isolated canonical seed read failed")
            event = EventEnvelope.FromString(message.value())
            seen.add((event.instrument_uid, event.WhichOneof("payload")))
            last = message.offset()
            if wanted <= seen:
                return {"offset": last, "seed_wait_ms": (time.monotonic() - started) * 1000}
        raise TimeoutError("ISOLATION_SEED_TIMEOUT:" + repr(sorted(wanted - seen)))
    finally:
        consumer.close()


def isolation_token(runner, consumer, uid, feed, offset):
    domain = runner.requirement(consumer, uid, feed)
    return runner.codec.encode(runner.claims(consumer, domain, offset, partition=0))


async def isolation_stream(packet, runner, channel, consumer, route, token, buffer, on_event, *, stall=False):
    """One Subscribe; returns its terminal facts. on_event(record) per event record."""
    import grpc
    from qdl.query.v2 import query_pb2 as pb
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    domain = runner.requirement(consumer, route["instrument_uid"], route["feed"])
    requirement = sdk_requirement(SimpleNamespace(requirement=domain)).to_proto()
    metadata = (("authorization", "Bearer " + runner.jwt(consumer)),
                ("x-qdl-consumer-id", consumer), ("x-qdl-purpose", "INTERNAL_ALPHA"))
    rpc = channel.unary_stream("/qdl.query.v2.MarketDataStreamService/Subscribe",
        request_serializer=pb.SubscribeRequest.SerializeToString,
        response_deserializer=pb.SubscribeResponse.FromString)
    call = rpc(pb.SubscribeRequest(consumer_id=consumer, requirement=requirement,
               cursor_token=token, max_buffer_events=buffer), metadata=metadata, timeout=300)
    controls, last_token = [], token
    try:
        async for response in call:
            for record in ([response.record] if response.HasField("record") else []) + list(response.records):
                if record.resume_token:
                    last_token = record.resume_token
                if record.HasField("control"):
                    controls.append(record.control.code)
                    if record.control.code == "LIVE":
                        on_event(None)  # LIVE announced
                        if stall:
                            os.kill(os.getpid(), signal.SIGSTOP)
                    continue
                on_event(record)
        return {"code": "OK", "controls": controls, "token": last_token}
    except grpc.aio.AioRpcError as error:
        return {"code": error.code().name, "detail": error.details()[:200], "controls": controls,
                "token": last_token}
    finally:
        call.cancel()


class _Reached(Exception):
    """A resumed deep reader received the last published trade id."""


def isolation_deep_main(packet, route, token, stall, until_id, pipe):
    """Child: one deep TRADE reader. Stalled: SIGSTOP at LIVE until the parent resumes it."""
    async def run():
        runner = harness(packet)
        consumer = packet["consumer_id"]
        channel = await channel_for(runner, consumer)
        state = {"count": 0, "first": None, "last": None, "contiguous": True}
        def on_event(record):
            if record is None:
                pipe.send({"kind": "live"})
                return
            if not record.event.HasField("trade") or record.event.instrument_uid != route["instrument_uid"]:
                raise ValueError("deep stream product mismatch")
            current = int(record.event.trade.native_trade_id)
            if state["last"] is not None and current != state["last"] + 1:
                state["contiguous"] = False
            state["first"] = state["first"] or current
            state["last"] = current
            state["count"] += 1
            if until_id and current >= until_id:
                raise _Reached
        try:
            end = await isolation_stream(packet, runner, channel, consumer, route, token, DEEP_BUFFER,
                                         on_event, stall=stall)
        except _Reached:
            end = {"code": "OK_REACHED", "controls": []}
        finally:
            await channel.close()
        pipe.send({"kind": "done", **state, **end})
    try:
        asyncio.run(run())
    except Exception as error:
        pipe.send({"kind": "error", "type": type(error).__name__})
    finally:
        pipe.close()


def isolation_idle_main(packet, routes, tokens, pipe):
    """Child: every idle reader as one task on one channel; stops on the parent's request."""
    async def run():
        runner = harness(packet)
        consumer = packet["fast_consumer_id"]
        channel = await channel_for(runner, consumer)
        readers = []
        for route in routes:
            for slot in range(IDLE_READERS_PER_ROUTE):
                readers.append({"route": route["symbol"] + "/" + route["feed"], "slot": slot,
                                "events": 0, "last_ns": 0, "end": None})
        async def read(reader, route, token):
            def on_event(record):
                if record is None:
                    reader["live"] = True
                    return
                reader["events"] += 1
                reader["last_ns"] = time.monotonic_ns()
            reader["end"] = await isolation_stream(packet, runner, channel, consumer, route, token, 100,
                                                   on_event)
        tasks, index = [], 0
        for route in routes:
            for _ in range(IDLE_READERS_PER_ROUTE):
                tasks.append(asyncio.create_task(read(readers[index], route, tokens[route["key"]])))
                index += 1
        live_deadline = time.monotonic() + 45
        while time.monotonic() < live_deadline and not all(r.get("live") or r["end"] for r in readers):
            await asyncio.sleep(.1)
        pipe.send({"kind": "started", "readers": len(readers),
                   "live": sum(1 for r in readers if r.get("live"))})
        loop = asyncio.get_running_loop()
        request = await loop.run_in_executor(None, pipe.recv)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not all(
                r["end"] is not None or r["last_ns"] > request["after_ns"] for r in readers):
            await asyncio.sleep(.1)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await channel.close()
        for reader in readers:
            reader["progressed"] = reader["last_ns"] > request["after_ns"]
            reader["terminal"] = None if reader["end"] is None else reader["end"]["code"]
            reader.pop("end")
        pipe.send({"kind": "done", "readers": readers})
    try:
        asyncio.run(run())
    except Exception as error:
        pipe.send({"kind": "error", "type": type(error).__name__})
    finally:
        pipe.close()


class IsolationChild:
    def __init__(self, target, *args):
        ctx = multiprocessing.get_context("spawn")
        self.pipe, sender = ctx.Pipe(duplex=True)
        self.process = ctx.Process(target=target, args=(*args, sender))
        self.process.start()

    def wait(self, kind, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.pipe.poll(min(.2, max(0, deadline - time.monotonic()))):
                row = self.pipe.recv()
                if row["kind"] == kind:
                    return row
                if row["kind"] in ("done", "error"):
                    raise RuntimeError(f"isolation child ended before {kind}: {row}")
        raise TimeoutError(f"isolation child did not reach {kind}")

    def resume(self):
        os.kill(self.process.pid, signal.SIGCONT)

    def close(self):
        if self.process.is_alive():
            self.resume()
            self.process.terminate()
        self.process.join(5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(5)
        self.pipe.close()


def isolation_closures(path):
    """Stream closure outcomes per consumer identity (abnormal = not 'ended')."""
    rows = {}
    for line in Path(path).read_text().splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get("event") == "qdl_kn_subscription_closed":
            per = rows.setdefault(row.get("consumer_id"), {})
            per[row.get("outcome")] = per.get(row.get("outcome"), 0) + 1
    return rows


def isolation_metric_summary(metrics):
    peak = max((row.get("queue_bytes", 0) for row in metrics), default=0)
    isolation = metrics[-1].get("queue_isolation") if metrics else None
    return {"peak_queue_bytes": peak, "queue_bytes_total": QUEUE_BYTES,
            "peak_reserve_bytes_used": max(0, peak - (QUEUE_BYTES - LIVE_RESERVE_BYTES)),
            "reserve_bytes": LIVE_RESERVE_BYTES, "queue_isolation": isolation,
            "overflow": metrics[-1].get("overflow") if metrics else None, "samples": len(metrics),
            "sampling": "1 s gateway metrics; a peak between samples is not seen"}


def run_isolation(packet):
    """Driver of ISOLATION_MODE inside the isolated namespace; returns facts, never a verdict."""
    facts, children, stage = {"observed": False}, [], "baseline_metrics"
    started = time.time_ns()
    try:
        wait_metrics(packet["metrics_path"], since_ns=started)
        routes = packet["isolation_routes"]
        deep_routes = [r for r in routes if r["feed"] == "TRADE"]
        idle_routes = [r for r in routes if r["feed"] != "TRADE"]
        stage = "seed"
        producer = IsolationProducer(packet)
        producer.publish({(r["symbol"], r["feed"]): 1 for r in routes})
        seed = isolation_wait_seed(packet, routes)
        runner = harness(packet)
        tokens = {r["key"]: isolation_token(runner, packet["fast_consumer_id"] if r["feed"] != "TRADE"
                  else packet["consumer_id"], r["instrument_uid"], r["feed"], seed["offset"]) for r in routes}
        stage = "deep_subscribe"
        deep = {}
        for route in deep_routes:
            child = IsolationChild(isolation_deep_main, packet, route, tokens[route["key"]], True, 0)
            children.append(child)
            child.wait("live", 60)
            deep[route["key"]] = child
        stage = "idle_subscribe"
        idle = IsolationChild(isolation_idle_main, packet, idle_routes, tokens)
        children.append(idle)
        idle_started = idle.wait("started", 60)
        if idle_started["live"] != idle_started["readers"]:
            raise RuntimeError("not every idle reader reached LIVE: " + json.dumps(idle_started))
        stage = "bursts"
        burst_started = time.monotonic()
        for _ in range(DEEP_ROUNDS):
            plan = {(r["symbol"], r["feed"]): DEEP_PER_ROUND for r in deep_routes}
            plan.update({(r["symbol"], r["feed"]): 1 for r in idle_routes})
            producer.publish(plan)
        burst_seconds = time.monotonic() - burst_started
        stage = "idle_under_pressure"
        hold_started, idle_published = time.monotonic(), 0
        while time.monotonic() - hold_started < IDLE_HOLD_SECONDS:
            step = time.monotonic()
            producer.publish({(r["symbol"], r["feed"]): IDLE_HOLD_RATE_HZ // 10 for r in idle_routes})
            idle_published += len(idle_routes) * (IDLE_HOLD_RATE_HZ // 10)
            time.sleep(max(0, .1 - (time.monotonic() - step)))
        hold_seconds = time.monotonic() - hold_started
        stage = "idle_marker"
        after_ns = time.monotonic_ns()
        producer.publish({(r["symbol"], r["feed"]): 1 for r in idle_routes})
        idle.pipe.send({"after_ns": after_ns})
        idle_end = idle.wait("done", 60)
        stage = "deep_terminal"
        time.sleep(6)  # with the hold: > QDL_KN_SLOW_CONSUMER_MS (10 s), a blocked send ends typed
        deep_end = {}
        for key, child in deep.items():
            child.resume()
            deep_end[key] = child.wait("done", 60)
        stage = "deep_resume"
        resumed = {}
        for route in deep_routes:
            end = deep_end[route["key"]]
            published_last = producer.next_id[(route["symbol"], route["feed"])] - 1
            if end.get("last") == published_last:
                resumed[route["key"]] = {"needed": False, "lossless": end.get("contiguous") is True}
                continue
            child = IsolationChild(isolation_deep_main, packet, route, end["token"], False, published_last)
            children.append(child)
            done = child.wait("done", 120)
            resumed[route["key"]] = {"needed": True, "first": done.get("first"), "last": done.get("last"),
                "expected_first": (end.get("last") or 1) + 1, "published_last": published_last,
                "contiguous": done.get("contiguous"), "code": done.get("code"),
                "lossless": (end.get("contiguous") is True and done.get("contiguous") is True
                             and done.get("first") == (end.get("last") or 1) + 1
                             and done.get("last") == published_last)}
        time.sleep(2)
        stage = "final_metrics"
        metrics = read_metrics(packet["metrics_path"], since_ns=started)
        deep_trades = DEEP_ROUNDS * DEEP_PER_ROUND
        facts = {"observed": True, "seed": seed,
            "burst": {"deep_routes": len(deep_routes), "trades_per_deep_route": deep_trades,
                      "seconds": burst_seconds,
                      "trade_rate_per_s": len(deep_routes) * deep_trades / burst_seconds if burst_seconds else None,
                      "idle_hold_seconds": hold_seconds, "idle_records_during_hold": idle_published},
            "idle": idle_end["readers"], "deep": deep_end, "resume": resumed,
            "closures": isolation_closures(packet["metrics_path"]),
            "deep_identity": packet["consumer_id"], "idle_identity": packet["fast_consumer_id"],
            "metrics": isolation_metric_summary(metrics)}
    except Exception as error:
        facts.update(failure_type=type(error).__name__, failure_stage=stage,
                     failure_detail=str(error)[:200] if isinstance(error, (TimeoutError, ValueError, RuntimeError)) else None)
    finally:
        for child in children:
            child.close()
    return facts


def isolation_outcome(facts):
    """Per-run measures the preregistered acceptance reads (no thresholds here)."""
    if not facts.get("observed"):
        return {"observed": False}
    idle = facts["idle"]
    closures = facts.get("closures", {})
    abnormal = {cid: sum(n for outcome, n in per.items() if outcome != "ended")
                for cid, per in closures.items()}
    isolation = facts["metrics"].get("queue_isolation") or {}
    return {"observed": True,
        "idle_readers": len(idle),
        "idle_client_terminals": sum(1 for r in idle if r.get("terminal") is not None),
        "idle_stream_abnormal_closures": abnormal.get(facts["idle_identity"], 0),
        "idle_not_progressed": sum(1 for r in idle if not r.get("progressed")),
        "deep_stream_abnormal_closures": abnormal.get(facts["deep_identity"], 0),
        "other_abnormal_closures": sum(n for cid, n in abnormal.items()
                                       if cid not in (facts["idle_identity"], facts["deep_identity"])),
        "reserve_exhausted": isolation.get("reserve_exhausted"),
        "over_floor_refused": isolation.get("over_floor_refused"),
        "reserve_admitted": isolation.get("reserve_admitted"),
        "deep_resume_lossless": all(r.get("lossless") for r in facts["resume"].values()),
        "deep_resumed": sum(1 for r in facts["resume"].values() if r.get("needed")),
        "peak_queue_bytes": facts["metrics"]["peak_queue_bytes"],
        "peak_reserve_bytes_used": facts["metrics"]["peak_reserve_bytes_used"],
        "trade_rate_per_s": facts["burst"]["trade_rate_per_s"]}


def isolation_verdict(candidate, baseline):
    """Apply ISOLATION_ACCEPTANCE (preregistered) to the two runs' outcomes."""
    c, b = candidate, baseline
    idle_closures = (c.get("idle_client_terminals", 1) + c.get("idle_stream_abnormal_closures", 1))
    checks = {
        "C1_candidate_idle_closures": c.get("observed") is True and idle_closures == 0,
        "C1b_candidate_idle_progress": c.get("observed") is True and c.get("idle_not_progressed") == 0,
        "C2_candidate_reserve_exhausted": c.get("reserve_exhausted") == 0,
        "C3_candidate_over_floor_refused": (isinstance(c.get("over_floor_refused"), int)
            and c["over_floor_refused"] > 0 and c.get("deep_stream_abnormal_closures", 0) > 0
            and c.get("other_abnormal_closures", 1) == 0),
        "C4_candidate_deep_resume": c.get("deep_resume_lossless") is True,
        "D1_baseline_discriminates": b.get("observed") is True and (
            b.get("idle_client_terminals", 0) + b.get("idle_stream_abnormal_closures", 0)) >= 1,
    }
    return {"checks": checks, "verdict": "PASS" if all(checks.values()) else "FAIL",
            "peak_reserve_bytes_used": c.get("peak_reserve_bytes_used"),
            "reserve_bytes": LIVE_RESERVE_BYTES}


def dependency_smoke():
    """Exercise real imports and protobuf construction, without runtime traffic."""
    import grpc
    from confluent_kafka import Producer, TopicCollection
    from qdl.runtime.stable_source import build_stable_query_stack
    from qdl.runtime.kn_read_view import install_kn_read_view
    from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend, KnCursorSettings, KnCursorV3Issuer
    from qdl.runtime.kn_market_cache import reader_from_environment
    from qdl.runtime.kn_hot_view import CanonicalHotClient
    import uvicorn
    from qdl.raw.envelope import build_raw_envelope, validate_raw_envelope
    from qdl.provider.v1 import raw_provider_pb2 as raw_pb
    from qdl.query.v2 import query_pb2 as pb
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from kn_native_slice_probe import Slice
    raw = build_raw_envelope(capture_id=uuid.uuid4().bytes, provider="BINANCE_DIRECT",
        venue="BINANCE", market="USDM", product_type="PERPETUAL", native_symbol="BTCUSDT",
        native_channel="btcusdt@aggTrade", subscription_id="test-only", source_session_id="test-only",
        connection_generation=1, lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
        received_at_ns=time.time_ns(), transport_protocol=raw_pb.TRANSPORT_PROTOCOL_WEBSOCKET,
        transport_compression=raw_pb.TRANSPORT_COMPRESSION_NONE,
        capture_boundary=raw_pb.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
        raw_frame_bytes=b'{"e":"aggTrade","a":1}', adapter_version="p184-test-only",
        config_revision=1, instrument_catalog_revision=1, correlation_id="p184-test-only",
        test_provenance=True)
    validate_raw_envelope(raw_pb.RawProviderEnvelope.FromString(raw.SerializeToString()))
    from unittest.mock import patch, MagicMock
    fake = MagicMock()
    fake.flush.return_value = 0
    with patch("confluent_kafka.Producer", return_value=fake):
        driver = RawProducer({"raw_template": base64.b64encode(raw.SerializeToString()).decode(),
            "bootstrap": "localhost:9092", "raw_topic": "test-only", "client_tls": {
                "ca": "/test/ca", "cert": "/test/cert", "key": "/test/key"}})
        driver.publish(1)
    sent = fake.produce.call_args.kwargs
    parsed_raw = raw_pb.RawProviderEnvelope.FromString(sent["value"])
    validate_raw_envelope(parsed_raw)
    assert sent["headers"] == [("qdl-event-id", parsed_raw.capture_id)]
    assert parsed_raw.test_provenance and json.loads(parsed_raw.raw_frame_bytes)["t"] == 1
    event = EventEnvelope(instrument_uid="test-only")
    event.trade.native_trade_id = "1"
    answer = pb.GetSnapshotResponse(stream_cursor="test-only", watermark_offset=1, events=[event])
    decoded = pb.GetSnapshotResponse.FromString(answer.SerializeToString())
    assert decoded.events[0].trade.native_trade_id == "1" and decoded.watermark_offset == 1
    request = pb.SubscribeRequest(consumer_id="test-only", max_buffer_events=QUEUE)
    assert request.max_buffer_events == QUEUE
    return {"imports": "PASS", "raw_proto_roundtrip": "PASS", "snapshot_proto_roundtrip": "PASS",
            "runtime_tested": False}


IMAGES = {
    "query": QUERY_IMAGE_DIGEST,
    "core": "sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023",
    "stream": "sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9",
    "python": "sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f",
    "kafka": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
    "redis": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
}


def isolation_route_rows(bindings, catalog):
    """Route rows (symbol, feed, uid, policy, payload, key) for the selected core bindings."""
    payloads = {"TRADE": "trade", "QUOTE": "quote", "MARK_INDEX_PRICE": "mark_index_price"}
    rows = []
    for binding in bindings:
        feed = isolation_feed(binding["native_channel"])
        entry = next(x for x in catalog.bindings if x.instrument.instrument_uid == binding["instrument_uid"]
                     and x.feed.value == feed and x.source_id == binding["source_id"])
        rows.append({"symbol": binding["native_symbol"], "feed": feed, "instrument_uid": binding["instrument_uid"],
                     "source_policy_id": entry.source_policy_id, "payload": payloads[feed],
                     "key": binding["native_symbol"] + "/" + feed, "binding": binding})
    return rows


def isolation_manifest(mapping, label, routes):
    """Deep: the TRADE routes, DEEP_BUFFER events. Idle: QUOTE/MARK routes, enough streams."""
    base = mapping["spec"]["requirements"][0]
    selected = [r for r in routes if (r["feed"] == "TRADE") == (label == "deep")]
    mapping["spec"]["requirements"] = [{**base, "instrument_uid": r["instrument_uid"], "feed": r["feed"],
                                        "source_policy_id": r["source_policy_id"]} for r in selected]
    streams = len(selected) * (1 if label == "deep" else IDLE_READERS_PER_ROUTE)
    mapping["spec"]["quotas"].update(max_streams=streams + 2,
        max_buffer_events=DEEP_BUFFER if label == "deep" else 100)
    return mapping


def prepare_material(work):
    """Inside the network-disabled Python image; only fresh test credentials."""
    import secrets
    import shutil
    import yaml
    from datetime import datetime, timedelta, timezone
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
    from qdl.raw.envelope import build_raw_envelope
    from tests.phase7_support import manifest_mapping
    from scripts.kn_gateway_bundle import compile_bundle
    from scripts.kn_native_slice_probe import ISSUER, AUDIENCE

    def save(name, value, private=False):
        path = work / name
        with path.open("x") as handle:
            path.chmod(0o600 if private else 0o644)
            handle.write(value if isinstance(value, str) else json.dumps(value))

    cfg = json.loads((work / "core.json").read_text())
    packet = json.loads((work / "packet.json").read_text())
    b = cfg["core"]["bindings"][0]
    isolation = packet.get("mode") == ISOLATION_MODE
    namespace = packet["namespace"]
    root = Path(__file__).resolve().parents[1]
    catalog = yaml.safe_load((root / "config/v2/stable-source-bindings.yaml").read_text())
    catalog["canonical_stream"] = cfg["core"]["canonical_stream"]
    # The synthetic adapter has its own declared lineage in this test-only catalog.
    source_ids = {row["source_id"] for row in cfg["core"]["bindings"]}
    for row in catalog["bindings"]:
        if row["source"]["source_id"] in source_ids:
            row["source"]["adapter_version"] = "p184-synthetic-test-only"
    save("catalog.yaml", yaml.safe_dump(catalog))
    loaded = StableSourceCatalog.load(work / "catalog.yaml")
    binding = next(x for x in loaded.bindings if x.instrument.instrument_uid == b["instrument_uid"]
                   and x.feed.value == "TRADE" and x.source_id == b["source_id"])
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, namespace)])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
          .public_key(key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), True)
          .sign(key, hashes.SHA256()))
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf = (x509.CertificateBuilder().subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])).issuer_name(name)
            .public_key(leaf_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(hours=2))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                  ExtendedKeyUsageOID.CLIENT_AUTH]), False)
            .sign(key, hashes.SHA256()))
    pem = serialization.Encoding.PEM
    save("ca.crt", ca.public_bytes(pem).decode())
    save("client.crt", leaf.public_bytes(pem).decode())
    save("client.key", leaf_key.private_bytes(pem, serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption()).decode(), True)
    password = secrets.token_hex(16)
    with (work / "server.p12").open("xb") as handle:
        (work / "server.p12").chmod(0o600)
        handle.write(pkcs12.serialize_key_and_certificates(b"dr08", leaf_key, leaf, [ca],
                     serialization.BestAvailableEncryption(password.encode())))
    props = """process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=SSL://:9092,CONTROLLER://:9093
advertised.listeners=SSL://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/server.p12
ssl.keystore.password=PASSWORD
ssl.key.password=PASSWORD
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca.crt
ssl.client.auth=required
log.dirs=/data
log.segment.bytes=16777216
metadata.log.segment.bytes=16777216
log.index.size.max.bytes=1048576
offsets.topic.num.partitions=1
transaction.state.log.num.partitions=1
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"""
    save("broker.properties", props.replace("PASSWORD", password), True)
    manifests, identities, keys, subjects = [], [], {}, {}
    routes = isolation_route_rows(cfg["core"]["bindings"], loaded) if isolation else []
    for label in (("deep", "idle") if isolation else ("slow", "fast")):
        cid = namespace + "." + label
        subject = "spiffe://qdl/paper/" + cid
        mapping = manifest_mapping(consumer_id=cid, subject=subject, instrument_uid=b["instrument_uid"],
            feed="TRADE", interval=None, source_policy_id=binding.source_policy_id)
        mapping["spec"]["quotas"]["max_buffer_events"] = QUEUE
        mapping["spec"]["requirements"][0].update(warmup_limit=1, require_full_coverage=False,
            require_final_bars=False, max_freshness_ms=60000)
        if isolation:
            isolation_manifest(mapping, label, routes)
        manifest_path = work / (label + ".yaml")
        save(label + ".yaml", yaml.safe_dump(mapping))
        manifests.append(manifest_path)
        jwt_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        save(label + ".key", jwt_key.private_bytes(pem, serialization.PrivateFormat.PKCS8,
             serialization.NoEncryption()).decode(), True)
        keys[label] = jwt_key.public_key().public_bytes(pem, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        subjects[label] = subject
        identities.append({"id": cid, "tls": {"ca_file": "/work/ca.crt", "cert_file": "/work/client.crt",
            "key_file": "/work/client.key"}, "jwt": {"private_key_file": "/work/" + label + ".key", "key_id": label}})
    save("jwt-config.json", {"issuer": ISSUER, "audience": AUDIENCE, "algorithms": "RS256",
        "keys": keys, "subjects": subjects, "environments": {k: "paper" for k in keys}, "max_lifetime_seconds": 900})
    save("profile.json", {"identities": identities})
    save("bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml", manifest_paths=manifests))
    save("cursor-keys.json", {"dr08": secrets.token_hex(32)}, True)
    save("read-view.secret", secrets.token_hex(32), True)
    save("probes.json", [])
    def template(b):
        return build_raw_envelope(capture_id=uuid.uuid4().bytes,
            **{k: b[k] for k in ("provider", "venue", "market", "product_type", "native_symbol", "native_channel")},
            subscription_id=b["source_id"], source_session_id=namespace, connection_generation=1,
            lease_epoch=1, authority_revision=cfg["authority"]["revision"], partition_plan_epoch=1,
            received_at_ns=time.time_ns(), transport_protocol=1, transport_compression=1, capture_boundary=2,
            raw_frame_bytes=b'{"e":"aggTrade","a":1}', adapter_version="p184-synthetic-test-only",
            config_revision=1, instrument_catalog_revision=b["instrument_catalog_revision"],
            correlation_id=namespace, test_provenance=True)
    raw = template(b)
    packet.update(consumer_id=namespace + ".slow", fast_consumer_id=namespace + ".fast",
        instrument_uid=b["instrument_uid"], raw_template=base64.b64encode(raw.SerializeToString()).decode(),
        raw_topic=cfg["raw_topics"][0], metrics_path="/work/stream-runtime.log",
        client_tls={"ca": "/work/ca.crt", "cert": "/work/client.crt", "key": "/work/client.key"},
        isolated_manifest_paths=["/work/slow.yaml", "/work/fast.yaml"],
        slice={"bundle": "/work/bundle.json", "cursor_keys": "/work/cursor-keys.json",
            "profile": "/work/profile.json", "probes": "/work/probes.json",
            "topic_id": "pending", "route_generation": namespace, "target": "localhost:8210"})
    if isolation:
        for route in routes:
            route["raw_template"] = base64.b64encode(template(route.pop("binding")).SerializeToString()).decode()
        packet.update(consumer_id=namespace + ".deep", fast_consumer_id=namespace + ".idle",
            isolation_routes=routes, isolated_manifest_paths=["/work/deep.yaml", "/work/idle.yaml"])
    (work / "packet.json").write_text(json.dumps(packet))
    # Keep all newly generated private material owned by the container's host UID.


def kafka_security(packet):
    tls = packet["client_tls"]
    return {"bootstrap.servers": packet["bootstrap"], "security.protocol": "SSL",
            "ssl.ca.location": tls["ca"], "ssl.certificate.location": tls["cert"],
            "ssl.key.location": tls["key"]}


def prepare_topics(work):
    from confluent_kafka.admin import AdminClient, NewTopic
    packet = json.loads((work / "packet.json").read_text())
    cfg = json.loads((work / "core.json").read_text())
    admin = AdminClient(kafka_security(packet))
    deadline = time.monotonic() + 60
    while True:
        try:
            admin.list_topics(timeout=2)
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise TimeoutError("isolated TLS broker readiness")
            time.sleep(.5)
    topics = cfg["raw_topics"] + [cfg["core"]["canonical_stream"], cfg["core"]["quarantine_stream"]]
    for future in admin.create_topics([NewTopic(name, 1, 1) for name in topics]).values():
        future.result(20)
    # describe_topics exposes the broker's immutable topic UUID (not a guessed ID).
    from confluent_kafka import TopicCollection
    result = admin.describe_topics(TopicCollection([cfg["core"]["canonical_stream"]]))
    packet["slice"]["topic_id"] = str(result[cfg["core"]["canonical_stream"]].result(20).topic_id)
    (work / "packet.json").write_text(json.dumps(packet))


def serve_query(work, *, public_app_factory=None):
    """Real KN Query read-view stack, backed by the isolated canonical hot view."""
    import ssl
    import uvicorn
    from fastapi import FastAPI
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from qdl.runtime.stable_source import build_stable_query_stack
    from qdl.runtime.kn_query_backend import KnMarketCacheQueryBackend, KnCursorSettings, KnCursorV3Issuer
    from qdl.runtime.kn_market_cache import reader_from_environment
    from qdl.runtime.kn_hot_view import CanonicalHotClient
    from qdl.runtime.kn_read_view import install_kn_read_view

    packet = json.loads((work / "packet.json").read_text())
    catalog = StableSourceCatalog.load(work / "catalog.yaml")
    settings = KnCursorSettings.from_environment(os.environ, environment="paper")
    expectation = dict(environment="paper", stream=catalog.canonical_stream,
        source_topic_id=settings.topic_id, partition_plan_epoch=1,
        source_policy_revision=catalog.source_policy_revision, catalog_revision=catalog.catalog_revision,
        route_generation=settings.route_generation, schema_major=2)
    hot = CanonicalHotClient.connect(targets=("localhost:8210",),
        ca=(work / "ca.crt").read_bytes(), cert=(work / "client.crt").read_bytes(),
        key=(work / "client.key").read_bytes(), expectation=expectation,
        secret=bytes.fromhex((work / "read-view.secret").read_text()))
    reader = reader_from_environment({"QDL_KN3_MARKET_CACHE_URL": "redis://localhost:6379/1",
                                     "QDL_KN3_ENVIRONMENT": "paper"})
    backend = KnMarketCacheQueryBackend(reader, catalog, schema_digest="a" * 64,
                                        topic_id=settings.topic_id, hot_client=hot)
    issuer = KnCursorV3Issuer(settings, catalog)
    service, _, _ = build_stable_query_stack(spool=None, catalog=catalog, schema_digest="a" * 64,
        handoff=None, cursor_ttl_seconds=3600, backend=backend, issuer=issuer)
    app = (public_app_factory(work, service, issuer)
           if public_app_factory is not None else FastAPI())
    install_kn_read_view(app, service=service, backend=backend, issuer=issuer,
                        secret=bytes.fromhex((work / "read-view.secret").read_text()))
    try:
        uvicorn.run(app, host="127.0.0.1", port=8200, access_log=False,
            ssl_keyfile=str(work / "client.key"), ssl_certfile=str(work / "client.crt"),
            ssl_ca_certs=str(work / "ca.crt"), ssl_cert_reqs=ssl.CERT_REQUIRED)
    finally:
        hot.close()


def candidate_stream_provenance(path, *, development):
    if path is None:
        return None
    if not development:
        raise ValueError("binary overlay is development-only, not an image qualification")
    path = Path(path).resolve(strict=True)
    if not path.is_file() or not 0 < path.stat().st_size <= 512 * 1024 * 1024:
        raise ValueError("bounded candidate binary required")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "bytes": path.stat().st_size,
            "container_path": "/work/candidate-stream", "scope": "development binary overlay; base image unchanged"}


def orchestrate(work, *, development, source_sha, image_digest, restart_samples=None, stream_binary=None,
                images=None, query_image_ref=None, isolation=False):
    """Adapt measure_l2_top_materialization's isolated broker/core packet.

    images: optional {role: sha256 id} overrides of IMAGES; query_image_ref names the Query image."""
    import shutil
    import subprocess
    root = Path(__file__).resolve().parents[1]
    run_images = resolve_images(images)
    query_ref = resolve_query_ref(run_images, query_image_ref)
    attested = {ENV_IMAGES: json.dumps(run_images, sort_keys=True), ENV_QUERY_IMAGE_REF: query_ref}
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest):
        raise ValueError("TS provenance must use full SHA/digest syntax even in development")
    if not development:
        if not re.fullmatch(r"[0-9a-f]{40}", source_sha) or len(set(source_sha)) == 1:
            raise ValueError("final TS source SHA required before setup")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest) or len(set(image_digest[7:])) == 1:
            raise ValueError("final TS image digest required before setup")
    count = restart_count({"development": development, **(
        {"restart_samples": restart_samples} if restart_samples is not None else {})})
    candidate = candidate_stream_provenance(stream_binary, development=development)
    work = work.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    namespace = "p184-dr08-" + uuid.uuid4().hex[:16]
    print(json.dumps({"namespace": namespace, "roles": ["prepare", "kafka", "topics", "quota", "core", "stream", "query", "driver"], "mode": "DEVELOPMENT" if development else "QUALIFICATION"}), flush=True)
    owned, log_process, log_file = [], None, None
    disk_before = shutil.disk_usage(work).free
    def command(args, timeout=90):
        return subprocess.run(args, check=True, text=True, capture_output=True, timeout=timeout).stdout.strip()
    def inventory():
        ids = command(["docker", "ps", "-q"]).splitlines()
        rows = json.loads(command(["docker", "inspect", *ids])) if ids else []
        return {r["Id"]: {"image": r["Image"], "started": r["State"]["StartedAt"],
                         "restarts": r["RestartCount"]} for r in rows if r["Name"].startswith("/qdl_v2_stable_candidate-")
                or r["Name"] == "/market_data_service_p184w"}
    before = inventory()
    try:
        for image in run_images.values():
            actual = command(["docker", "image", "inspect", image, "--format", "{{.Id}}"])
            if actual != image:
                raise ValueError("local image digest mismatch")
        runtime = json.loads(command(["docker", "inspect", "qdl_v2_stable_candidate-rust_core-1"]))[0]
        if runtime["Image"] != run_images["core"]:
            raise ValueError("deployed core image drift")
        cfg = json.loads(command(["docker", "exec", runtime["Id"], "cat", "/runtime/core.json"]))
        selected = [b for b in cfg["core"]["bindings"] if b["venue"] == "BINANCE" and b["market"] == "USDM"
                    and b["native_symbol"] == "BTCUSDT" and b["native_channel"].lower().endswith(("@aggtrade", "@trade"))]
        if isolation:
            selected = isolation_bindings(cfg["core"]["bindings"])
        elif len(selected) != 1:
            raise ValueError("expected one deployed BTCUSDT trade binding")
        cfg["core"].update(bindings=selected, allow_test_provenance=True,
                            canonical_stream=namespace + ".canonical", quarantine_stream=namespace + ".quarantine")
        cfg.update(raw_topics=[namespace + ".raw"], transactional_id=namespace, shard_id=namespace,
                   max_events=0, metrics_every_batches=10)
        cfg["authority"].update(mode="RUST_SHADOW", slice_id=namespace, approved_by="isolated DR08 test")
        (work / "core.json").write_text(json.dumps(cfg))
        packet = {"namespace": namespace, "development": development, "ts_source_sha": source_sha,
            "ts_image_digest": image_digest, "images": run_images, "bootstrap": "localhost:9092",
            "restart_samples": count, "query_provenance": {"image_ref": query_ref,
                "scope": "isolated candidate Query; not production v2.2.2"}}
        if isolation:
            packet["mode"] = ISOLATION_MODE
        if candidate:
            shutil.copyfile(stream_binary, work / "candidate-stream")
            (work / "candidate-stream").chmod(0o555)
            copied = candidate_stream_provenance(work / "candidate-stream", development=True)
            if copied != candidate:
                raise ValueError("candidate changed during staging")
            packet["stream_binary_override"] = candidate
        (work / "packet.json").write_text(json.dumps(packet))
        def launch(role, image, args, *, memory, cpus, network, env=None, detach=True, extra=()):
            name = namespace + "-" + role
            cmd = ["docker", "run", "--rm", "--name", name, "--label", "p184.dr08=" + namespace,
                "--user", str(os.getuid()) + ":" + str(os.getgid()), "--cpus", str(cpus), "--memory", memory,
                "--pids-limit", "128", "--network", network, "--read-only",
                "--tmpfs", ("/tmp:rw,exec,size=64m,mode=1777" if role == "kafka" else "/tmp:rw,size=64m,mode=1777"), "--log-opt", "max-size=4m",
                "--log-opt", "max-file=1", "-v", str(work) + ":/work",
                "-v", str(root) + ":/app:ro", "-w", "/app"]
            if detach:
                cmd.insert(3, "-d")
            for k, v in (env or {}).items():
                cmd += ["-e", k + "=" + v]
            cmd += list(extra)
            if detach:
                # Redirect before exec: --rm can erase Docker logs before inspection.
                args = ["/bin/sh", "-c", 'ulimit -f ' + ("65536" if role == "kafka" else "8192") + '; exec "$@" > /work/' + role + '-runtime.log 2>&1',
                        "dr08-log", *args]
            cmd += ["--entrypoint", args[0], image, *args[1:]]
            # Register before Docker: a failed or timed-out CLI can leave a live container.
            owned.append((name, name))
            try:
                return command(cmd, timeout=660 if role == "driver" else 100)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                diagnostic = (getattr(error, "stderr", None) or "")
                if isinstance(diagnostic, bytes):
                    diagnostic = diagnostic.decode(errors="replace")
                (work / (role + "-command-error.log")).write_text(diagnostic[-16384:])
                raise
        penv = {"PYTHONPATH": "/app:/app/generated/python", "PYTHONDONTWRITEBYTECODE": "1",
                "P184_DR08_ISOLATED_NAMESPACE": namespace, **attested}
        script = ["python", "-B", "scripts/p184_dr08_isolated.py"]
        launch("prepare", run_images["python"], script + ["--prepare-material", "/work"],
               memory="512m", cpus=.5, network="none", env=penv, detach=False)
        broker = launch("kafka", run_images["kafka"], ["/bin/sh", "-c",
            "/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /work/broker.properties"
            " && exec /opt/kafka/bin/kafka-server-start.sh /work/broker.properties"],
            memory="1536m", cpus=1, network="none", env={"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": "/tmp/kafka-logs"},
            extra=("--hostname", "localhost", "--tmpfs", "/data:rw,size=512m,mode=1777"))
        net = "container:" + broker
        launch("topics", run_images["python"], script + ["--prepare-topics", "/work"],
               memory="256m", cpus=.5, network=net, env=penv, detach=False)
        packet = json.loads((work / "packet.json").read_text())
        launch("quota", run_images["redis"], ["redis-server", "--save", "", "--appendonly", "no",
            "--maxmemory", "48mb", "--maxmemory-policy", "noeviction"], memory="64m", cpus=.1, network=net)
        common = {"QDL_ENVIRONMENT": "paper", "QDL_KN_CURSOR_KEYS_FILE": "/work/cursor-keys.json",
            "QDL_KN_CURSOR_ACTIVE_KEY_ID": "dr08", "QDL_KN_TOPIC_ID": packet["slice"]["topic_id"],
            "QDL_KN_ROUTE_GENERATION": namespace, "QDL_KN_READ_VIEW_SECRET_FILE": "/work/read-view.secret"}
        launch("core", run_images["core"], ["/usr/local/bin/qdl-realtime-core", "/work/core.json"],
            memory="256m", cpus=.5, network=net, env={"QDL_KAFKA_CERT_ROOT": "/work",
            "QDL_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", "QDL_KAFKA_CLIENT_ID": namespace,
            "QDL_KAFKA_GROUP_ID": namespace, "QDL_PROVIDER_ADMISSION_ENABLED": "false"})
        stream = launch("stream", run_images["stream"], ["/work/candidate-stream" if candidate else "/usr/local/bin/qdl-stream-gateway", "serve"],
            memory="512m", cpus=.5, network=net, env={**common,
            "QDL_KN_BUNDLE_FILE": "/work/bundle.json", "QDL_KN_JWT_CONFIG_FILE": "/work/jwt-config.json",
            "QDL_KN_QUOTA_REDIS_URL": "redis://localhost:6379/0", "QDL_KN_QUOTA_PREFIX": "qdl:stable:v2:paper:" + namespace,
            "QDL_KN_TLS_CERT_FILE": "/work/client.crt", "QDL_KN_TLS_KEY_FILE": "/work/client.key",
            "QDL_KN_TLS_CLIENT_CA_FILE": "/work/ca.crt", "QDL_KN_KAFKA_BOOTSTRAP": "localhost:9092",
            "QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
            "QDL_KN_KAFKA_KEY": "/work/client.key", "QDL_KN_KAFKA_TOPIC": cfg["core"]["canonical_stream"],
            "QDL_KN_KAFKA_GROUP": "kn-" + namespace + "-stream", "QDL_KN_KAFKA_CLIENT_ID": namespace + "-stream",
            "QDL_KN_READ_VIEW_URLS": "https://localhost:8200", "QDL_KN_READ_VIEW_CA_FILE": "/work/ca.crt",
            "QDL_KN_HOT_READ_ENABLED": "true", "QDL_KN_METRICS_SECONDS": "1",
            "QDL_KN_MAX_REPLAY_EVENTS": str(REPLAY), "QDL_KN_QUEUE_BYTES_TOTAL": str(QUEUE_BYTES)})
        query = launch("query", run_images["query"], script + ["--query", "/work"], memory="512m", cpus=.5,
               network=net, env={**penv, **common})
        query_state = json.loads(command(["docker", "inspect", query]))[0]
        packet["query_provenance"]["actual_image_digest"] = query_state["Image"]
        validate_query_provenance(packet, expected=(query_ref, run_images["query"]))
        # Query may be reading its startup packet; publish the attestation atomically.
        attested_packet = work / "packet.attested.json"
        attested_packet.write_text(json.dumps(packet))
        attested_packet.replace(work / "packet.json")
        ready_deadline = time.monotonic() + 30
        while True:
            core_state = json.loads(command(["docker", "inspect", namespace + "-core"]))[0]
            if not core_state["State"]["Running"]:
                raise RuntimeError("isolated core exited before driver")
            core_log = work / "core-runtime.log"
            core_ready = core_log.exists() and '"event":"qdl_realtime_core_started"' in core_log.read_text()
            state = json.loads(command(["docker", "inspect", stream]))[0]
            if not state["State"]["Running"]:
                raise RuntimeError("isolated Stream exited before startup")
            log = work / "stream-runtime.log"
            text = log.read_text() if log.exists() else ""
            if '"event":"qdl_kn_gateway_error"' in text:
                raise RuntimeError("isolated Stream startup configuration refused; see stream-runtime.log")
            if core_ready and '"event":"qdl_kn_gateway_start"' in text and '"event":"qdl_kn_gateway_metrics"' in text:
                break
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("isolated Stream startup deadline")
            time.sleep(.1)
        driver_args = script + ["--packet", "/work/packet.json", "--output", "/work/evidence.jsonl"]
        if development:
            driver_args += ["--development"]
        launch("driver", run_images["python"], driver_args, memory="1g" if isolation else "512m",
               cpus=1 if isolation else .75, network=net, env=penv, detach=False)
    finally:
        if log_process is not None:
            log_process.terminate()
            log_process.wait(timeout=5)
        if log_file is not None:
            log_file.close()
        cleanup_failures = []
        for name, cid in reversed(owned):
            try:
                row = json.loads(command(["docker", "inspect", cid]))[0]
                if row["Config"]["Labels"].get("p184.dr08") != namespace:
                    raise ValueError("cleanup ownership mismatch")
                logs = subprocess.run(["docker", "logs", "--tail", "100", row["Id"]],
                    text=True, capture_output=True, timeout=10)
                (work / (name + "-tail.log")).write_text((logs.stdout + logs.stderr)[-32768:])
                command(["docker", "stop", "--time", "5", row["Id"]], timeout=20)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                if command(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"]):
                    cleanup_failures.append(name)
        (work / "candidate-stream").unlink(missing_ok=True)
        for filename in ("client.key", "server.p12", "slow.key", "fast.key", "deep.key", "idle.key", "cursor-keys.json",
                         "read-view.secret", "broker.properties"):
            (work / filename).unlink(missing_ok=True)
        after = inventory()
        unchanged = all(after.get(cid) == state for cid, state in before.items())
        (work / "cleanup.json").write_text(json.dumps({"shared_runtime_unchanged": unchanged,
            "cleanup_failures": cleanup_failures, "owned_containers": [name for name, _ in owned],
            "disk_free_before": disk_before, "disk_free_after": shutil.disk_usage(work).free,
            "images_built": 0, "images_removed": 0}))
        if cleanup_failures or not unchanged:
            raise RuntimeError("cleanup or shared runtime verification failed")



def isolation_prereg(*, candidate, baseline, images, query_image_ref, source_sha, now_utc):
    """The acceptance and scenario, fixed before any run (written exclusively, hashed)."""
    for name, value in (("candidate", candidate), ("baseline", baseline)):
        if not isinstance(value, str) or not IMAGE_ID.fullmatch(value):
            raise ValueError(f"immutable {name} Stream image ID required")
    if candidate == baseline:
        raise ValueError("an A/B needs two different Stream images")
    if not isinstance(source_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("full DL source SHA required")
    if "stream" in (images or {}):
        raise ValueError("the Stream image is the A/B variable; pass it as candidate/baseline")
    return {"schema": "p184.dl.live-budget-isolation.prereg.v1", "created_utc": now_utc,
        "source_sha": source_sha, "candidate_stream": candidate, "baseline_stream": baseline,
        "shared_images": dict(sorted((images or {}).items())), "query_image_ref": query_image_ref,
        "order": ["candidate", "baseline"], "acceptance": ISOLATION_ACCEPTANCE,
        "scenario": {"deep_trade_symbols": list(ISOLATION_DEEP), "idle_symbols": list(ISOLATION_IDLE),
            "idle_feeds": list(ISOLATION_IDLE_FEEDS), "idle_readers_per_route": IDLE_READERS_PER_ROUTE,
            "idle_readers": len(ISOLATION_IDLE) * len(ISOLATION_IDLE_FEEDS) * IDLE_READERS_PER_ROUTE,
            "deep_buffer_events": DEEP_BUFFER, "rounds": DEEP_ROUNDS, "trades_per_round_per_deep": DEEP_PER_ROUND,
            "idle_hold_seconds": IDLE_HOLD_SECONDS, "idle_hold_rate_hz_per_route": IDLE_HOLD_RATE_HZ,
            "live_budget_bytes": QUEUE_BYTES, "reserve_bytes": LIVE_RESERVE_BYTES,
            "slow_consumer_ms": 10000, "data": "SYNTHETIC_TEST_ONLY"}}


def isolation_campaign(directory, *, candidate, baseline, images, query_image_ref, source_sha):
    """Preregister, run candidate then baseline with everything but Stream equal, then judge."""
    from datetime import datetime, timezone
    prereg = isolation_prereg(candidate=candidate, baseline=baseline, images=images,
        query_image_ref=query_image_ref, source_sha=source_sha,
        now_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    directory = directory.resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    text = json.dumps(prereg, sort_keys=True, indent=1)
    with (directory / "preregistration.json").open("x") as handle:
        handle.write(text)
    prereg_sha = hashlib.sha256(text.encode()).hexdigest()
    outcomes, errors = {}, {}
    for label, stream in (("candidate", candidate), ("baseline", baseline)):
        try:
            orchestrate(directory / label, development=True, source_sha=source_sha, image_digest=stream,
                        images={**images, "stream": stream}, query_image_ref=query_image_ref, isolation=True)
        except Exception as error:  # the facts file (if any) still decides; errors are recorded
            errors[label] = type(error).__name__ + ":" + str(error)[:200]
        try:
            row = json.loads((directory / label / "evidence.jsonl").read_text().splitlines()[0])
            outcomes[label] = row["outcome"]
        except (FileNotFoundError, IndexError, KeyError, json.JSONDecodeError):
            outcomes[label] = {"observed": False}
    verdict = {"preregistration_sha256": prereg_sha, "outcomes": outcomes, "errors": errors,
               **isolation_verdict(outcomes["candidate"], outcomes["baseline"])}
    with (directory / "verdict.json").open("x") as handle:
        handle.write(json.dumps(verdict, sort_keys=True, indent=1))
    return verdict


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dependency-smoke", action="store_true")
    parser.add_argument("--orchestrate", type=Path)
    parser.add_argument("--prepare-material", type=Path)
    parser.add_argument("--prepare-topics", type=Path)
    parser.add_argument("--query", type=Path)
    parser.add_argument("--ts-source-sha", default="0" * 40)
    parser.add_argument("--ts-image-digest", default="sha256:" + "0" * 64)
    parser.add_argument("--packet", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--development", action="store_true")
    parser.add_argument("--stream-binary", type=Path, help="development-only isolated candidate executable")
    parser.add_argument("--restart-samples", type=int,
                        help="orchestrator restart count: development 1-10, final 5-10")
    parser.add_argument("--image", action="append", default=[], metavar="ROLE=sha256:ID",
                        help="orchestrator image override (repeatable); IMAGES are the defaults")
    parser.add_argument("--query-image-ref", help="orchestrator Query image label, required with a new query digest")
    parser.add_argument("--isolation-campaign", type=Path,
                        help="NEW directory: preregister, then run the live-budget isolation A/B")
    parser.add_argument("--candidate-stream", help="isolation campaign candidate Stream image sha256 ID")
    parser.add_argument("--baseline-stream", help="isolation campaign baseline Stream image sha256 ID")
    parser.add_argument("--source-sha", help="isolation campaign DL source SHA under test")
    args = parser.parse_args(argv)
    if args.isolation_campaign:
        def campaign_deadline(_signum, _frame):
            raise TimeoutError("whole isolation campaign deadline")
        previous = signal.signal(signal.SIGALRM, campaign_deadline)
        signal.alarm(1500)
        try:
            verdict = isolation_campaign(args.isolation_campaign, candidate=args.candidate_stream,
                baseline=args.baseline_stream, images=parse_image_overrides(args.image),
                query_image_ref=args.query_image_ref, source_sha=args.source_sha)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)
        print(json.dumps(verdict, sort_keys=True))
        return 0 if verdict["verdict"] == "PASS" else 1
    if args.orchestrate:
        def orchestration_deadline(_signum, _frame):
            raise TimeoutError("whole DR08 orchestration deadline")
        previous = signal.signal(signal.SIGALRM, orchestration_deadline)
        signal.alarm(600)
        try:
            orchestrate(args.orchestrate, development=args.development, source_sha=args.ts_source_sha, image_digest=args.ts_image_digest, restart_samples=args.restart_samples, stream_binary=args.stream_binary,
                        images=parse_image_overrides(args.image), query_image_ref=args.query_image_ref)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)
        return 0
    if args.stream_binary is not None:
        parser.error("--stream-binary requires --orchestrate")
    if args.restart_samples is not None:
        parser.error("--restart-samples requires --orchestrate; inner driver uses its sealed packet")
    if args.image or args.query_image_ref:
        parser.error("--image/--query-image-ref require --orchestrate; inner containers read the attested env")
    for directory, function in ((args.prepare_material, prepare_material), (args.prepare_topics, prepare_topics), (args.query, serve_query)):
        if directory is not None:
            function(directory)
            return 0
    if args.dependency_smoke:
        print(json.dumps(dependency_smoke(), sort_keys=True))
        return 0
    if args.packet is None or args.output is None:
        parser.error("--packet and --output are required")
    packet = json.loads(args.packet.read_text())
    validate_packet(packet, development=args.development)
    if packet.get("mode") == ISOLATION_MODE:
        facts = run_isolation(packet)
        with args.output.open("x", encoding="utf-8") as output:
            output.write(json.dumps({"mode": ISOLATION_MODE, "facts": facts,
                                     "outcome": isolation_outcome(facts)}, sort_keys=True) + "\n")
            output.flush()
            os.fsync(output.fileno())
        return 0
    # Exclusive creation happens BEFORE any traffic. No resume/relabel operation.
    with args.output.open("x", encoding="utf-8") as output:
        run_id = str(uuid.uuid4())
        def deadline(_signum, _frame):
            raise TimeoutError("DR08 ten-minute deadline")
        previous = signal.signal(signal.SIGALRM, deadline)
        signal.alarm(600)
        try:
            facts = run_faults(packet)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)
        rows = evidence_rows(packet, facts, development=args.development, run_id=run_id)
        for row in rows:
            output.write(json.dumps(row, sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())
    return 0 if all(row["development_pass"] if args.development else row["ok"] for row in rows[1:]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

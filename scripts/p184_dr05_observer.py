"""Bounded DR05 capture through committed canonical and real Query/Stream RPCs.

Test-only development evidence, never a standalone DR acceptance certificate.
Run only inside p184_dr05_dl_fault's disposable broker network namespace.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.p184_dr05_dl_fault import frame, namespace, schedule
from scripts.p184_dr05_raw_publisher import RawPublisher


def record_summary(topic, offset, wire, canonical, *, partition=0):
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord, QuarantineReason

    row = {"topic": topic, "partition": partition, "offset": offset, "sha256": hashlib.sha256(wire).hexdigest()}
    if canonical:
        event = EventEnvelope.FromString(wire)
        row.update(kind="canonical", capture_id=event.raw_capture_id.hex(),
            instrument_uid=event.instrument_uid, venue=event.venue, generation=event.connection_generation,
            sequence=event.source_sequence, payload=event.WhichOneof("payload"),
            event_b64=base64.b64encode(wire).decode())
    else:
        event = QuarantineRecord.FromString(wire)
        row.update(kind="quarantine", capture_id=event.raw.capture_id.hex(), venue=event.raw.venue,
            generation=event.raw.connection_generation, reason=QuarantineReason.Name(event.reason),
            summary=event.safe_summary)
    return row


class CanonicalObserver:
    def __init__(self, packet, config):
        from confluent_kafka import Consumer, TopicPartition
        from scripts.p184_dr08_isolated import kafka_security

        self.tp = TopicPartition
        self.raw = config["raw_topics"][0]
        self.canonical = config["core"]["canonical_stream"]
        self.topics = [self.canonical, config["core"]["quarantine_stream"]]
        self.consumer = Consumer({**kafka_security(packet), "group.id": packet["namespace"] + ".observer",
            "enable.auto.commit": False, "isolation.level": "read_committed", "enable.partition.eof": True,
            "fetch.wait.max.ms": 10})
        self.positions = {topic: 0 for topic in self.topics}
        self.consumer.assign([TopicPartition(topic, 0, 0) for topic in self.topics])
        # committed() reads offsets without subscribe/join or offset mutation.
        self.offset_reader = Consumer({**kafka_security(packet), "group.id": packet["namespace"],
                                      "enable.auto.commit": False})

    def observe(self, receipt, timeout=8):
        from confluent_kafka import KafkaError

        deadline, records, eof = time.monotonic() + timeout, [], set()
        committed = -1
        next_check = 0
        after_commit = False
        while time.monotonic() < deadline:
            message = self.consumer.poll(.05)
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        raise RuntimeError(str(message.error()))
                    eof.add(message.topic())
                    self.positions[message.topic()] = message.offset()
                else:
                    eof.discard(message.topic())
                    self.positions[message.topic()] = message.offset() + 1
                    records.append(record_summary(message.topic(), message.offset(), message.value(),
                                                   message.topic() == self.canonical, partition=message.partition()))
                    if len(records) > 64:
                        raise RuntimeError("unexpected unbounded canonical output")
            if time.monotonic() >= next_check:
                next_check = time.monotonic() + .2
                committed = self.offset_reader.committed([self.tp(self.raw, 0)], timeout=1)[0].offset
            if committed > receipt["offset"] and not after_commit:
                # Fetch both partitions again AFTER the transaction committed.
                # An EOF observed before the raw offset commit proves nothing.
                self.consumer.assign([self.tp(topic, 0, self.positions[topic]) for topic in self.topics])
                eof.clear()
                after_commit = True
            if after_commit and set(self.topics) <= eof:
                return {"raw_committed_next": committed, "read_committed_eof": sorted(eof),
                    "records": records, "matched": [r for r in records if r["capture_id"] == receipt["capture_id"]]}
        raise TimeoutError(f"canonical barrier missing: committed={committed}, raw={receipt['offset']}, eof={sorted(eof)}")

    def close(self):
        self.consumer.close()
        self.offset_reader.close()


def reader_harness(packet):
    from scripts.kn_native_slice_probe import Slice
    from qdl.consumer.manifest import ConsumerManifestLoader

    runner = Slice(SimpleNamespace(**packet["slice"]))
    runner.manifests = {m.consumer_id: m for m in (
        ConsumerManifestLoader.load(Path(path)) for path in packet["isolated_manifest_paths"])}
    expected = {packet["namespace"] + ".binance", packet["namespace"] + ".okx"}
    if set(runner.manifests) != expected or set(runner.identities) != expected:
        raise ValueError("isolated manifest/identity roster mismatch")
    return runner


async def read_views(runner, packet, binding):
    import grpc
    from google.protobuf.json_format import MessageToDict
    from qdl.query.v2 import query_pb2 as pb
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    from scripts.p184_dr08_isolated import channel_for

    cid = packet["namespace"] + "." + binding["venue"].lower()
    channel = await channel_for(runner, cid)
    result = {}
    try:
        for feed in ("BOOK_SNAPSHOT", "BOOK_DELTA"):
            domain = runner.requirement(cid, binding["instrument_uid"], feed)
            requirement = sdk_requirement(SimpleNamespace(requirement=domain)).to_proto()
            metadata = (("authorization", "Bearer " + runner.jwt(cid)), ("x-qdl-consumer-id", cid),
                        ("x-qdl-purpose", "INTERNAL_EXECUTION"))
            row = {"requested_at_ns": time.time_ns(), "instrument_uid": binding["instrument_uid"]}
            for method, request_type, response_type in (
                ("GetFeedStatus", pb.GetFeedStatusRequest, pb.GetFeedStatusResponse),
                ("GetSnapshot", pb.GetSnapshotRequest, pb.GetSnapshotResponse)):
                rpc = channel.unary_unary("/qdl.query.v2.MarketDataStreamService/" + method,
                    request_serializer=request_type.SerializeToString,
                    response_deserializer=response_type.FromString)
                try:
                    value = await rpc(request_type(consumer_id=cid, requirement=requirement), metadata=metadata, timeout=2)
                    if method == "GetSnapshot":
                        row[method] = {"code": "OK", "events_b64": [base64.b64encode(e.SerializeToString()).decode()
                            for e in value.events], "watermark_offset": value.watermark_offset,
                            "cursor_valid": runner.cursor_valid(value.stream_cursor, cid, domain),
                            "stream_cursor": value.stream_cursor,
                            "data_as_of_ns": value.data_as_of_ns}
                    else:
                        row[method] = {"code": "OK", "status": MessageToDict(value, preserving_proto_field_name=True,
                                                                      always_print_fields_with_no_presence=True)}
                except grpc.aio.AioRpcError as error:
                    row[method] = {"code": error.code().name, "detail": error.details()[:1000]}
            row["completed_at_ns"] = time.time_ns()
            result[feed] = row
    finally:
        await channel.close()
    return result


async def run(work, venue, fault):
    packet = json.loads((work / "packet.json").read_text())
    config = json.loads((work / "core.json").read_text())
    namespace(packet["namespace"])
    producer = RawPublisher(packet, config)
    observer = CanonicalObserver(packet, config)
    runner = reader_harness(packet)
    binding = producer.bindings[venue]
    packet_base = ts_packet(work, runner, packet, binding, [])
    rows = []
    try:
        for step in schedule(venue, fault):
            values = {key: value for key, value in step.items() if key != "phase"}
            raw = frame(**values, at_ns=time.time_ns())
            receipt = producer.publish(raw)
            observed = observer.observe(receipt)
            views = await read_views(runner, packet, binding)
            row = {"phase": step["phase"], "publication": receipt, "canonical": observed, "views": views}
            import urllib.request
            live_input = ts_packet(work, runner, packet, binding, [row], base=packet_base)
            live_input["namespace"] = packet["namespace"]
            live_input["requirements"] = {"BOOK_SNAPSHOT": live_input["requirements"]["BOOK_SNAPSHOT"]}
            live_input["observations"][0]["views"] = {"BOOK_SNAPSHOT": row["views"]["BOOK_SNAPSHOT"]}
            body = json.dumps(live_input).encode()
            request = urllib.request.Request("http://127.0.0.1:8260/validate", data=body,
                headers={"Content-Type": "application/json", "X-P184-Namespace": packet["namespace"]}, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    row["ts_live"] = json.loads(response.read(4 * 1024 * 1024))
            except Exception as error:
                row["ts_live"] = {"error": type(error).__name__, "detail": str(error)[:500], "acceptance": False}
            rows.append(row)
            (work / "observations.json").write_text(json.dumps(rows))
    finally:
        observer.close()
    (work / "ts-input.json").write_text(json.dumps(ts_packet(work, runner, packet, binding, rows)))
    (work / "ts-input.json").chmod(0o600)
    return {"schema": "p184.dr05.observation.v1", "venue": venue, "fault": fault,
            "namespace": packet["namespace"], "provenance": "TEST_ONLY", "acceptance": False, "observations": rows}


def ts_packet(work, runner, packet, binding, rows, base=None):
    venue = binding["venue"]
    cid = packet["namespace"] + "." + venue.lower()
    if base is None:
        from dataclasses import asdict
        from qdl.runtime.stable_catalog import StableSourceCatalog
        from qdl.certification.phase103_consumer_acceptance import sdk_requirement
        catalog = StableSourceCatalog.load(work / "catalog.yaml")
        instrument = catalog.instrument_for(binding["instrument_uid"])
        cid = packet["namespace"] + "." + venue.lower()
        requirements = {}
        for feed in ("BOOK_SNAPSHOT", "BOOK_DELTA"):
            sdk = sdk_requirement(SimpleNamespace(requirement=runner.requirement(cid, binding["instrument_uid"], feed)))
            requirements[feed] = asdict(sdk)
        base = {"schema_version": 1, "side": "BUY", "requirements": requirements,
            "instrument": {**instrument.identity.__dict__, "metadata_revision": instrument.metadata_revision,
                "asset_class": instrument.asset_class.value, "native_symbol": instrument.native_symbol,
                "status": instrument.status.value}}
    handoffs = {}
    path = work / "query-handoffs.jsonl"
    if path.exists():
        for line in path.read_text().splitlines():
            handoff = json.loads(line)
            if handoff["consumer"] == cid:
                handoffs.setdefault(handoff["feed"], handoff["view"])
    ts_input = {**base, "handoffs": handoffs,
        "observations": [{"id": row["phase"], "views": row["views"]} for row in rows]}
    return json.loads(json.dumps(ts_input, default=lambda value: value.value))


def diagnostic_query(work):
    # Capture the REAL Query handoff from the same RPC, without changing it.
    from qdl.runtime.kn_query_backend import KnCursorV3Issuer
    from qdl.api_v2.router import _market_item
    from scripts.p184_dr08_isolated import serve_query
    original = KnCursorV3Issuer.bind_history
    def bind(self, requirement, history, *, consumer_id):
        bound = original(self, requirement, history, consumer_id=consumer_id)
        item = _market_item(bound.items[-1]).model_dump(mode="json")
        with (work / "query-handoffs.jsonl").open("a") as handle:
            handle.write(json.dumps({"consumer": consumer_id, "feed": requirement.feed.value,
                "cursor_sha256": hashlib.sha256(bound.stream_cursor.encode()).hexdigest(),
                "view": item}) + "\n")
        return bound
    KnCursorV3Issuer.bind_history = bind
    serve_query(work)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--query":
        diagnostic_query(Path(sys.argv[2]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--venue", choices=("BINANCE", "OKX"), required=True)
    parser.add_argument("--fault", choices=("GAP_REFUSAL", "GENERATION_CHANGE_FENCED"), required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.work, args.venue, args.fault))))


if __name__ == "__main__":
    main()

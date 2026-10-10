"""Test-only DR05 publisher; broker ACK is not core/TS acceptance."""
from __future__ import annotations

import hashlib
import json
import os
import time
import uuid

from scripts.p184_dr05_dl_fault import VENUES, namespace, scope_of


def raw_envelope(binding, frame, ns):
    from qdl.raw.envelope import build_raw_envelope

    namespace(ns)
    if frame.get("test_provenance") is not True or frame.get("provenance") != "TEST_ONLY":
        raise ValueError("explicit test provenance required")
    if (binding["venue"], binding["native_symbol"]) != (frame["venue"], frame["native_symbol"]):
        raise ValueError("frame/binding identity mismatch")
    if frame.get("adapter_version") != "p184-synthetic-test-only":
        raise ValueError("unexpected test adapter")
    if frame.get("transport") not in ("HTTP", "WEBSOCKET"):
        raise ValueError("explicit transport required")
    if frame["transport"] == "HTTP" and not (frame["venue"] == "BINANCE" and frame["snapshot"] is True):
        raise ValueError("HTTP only allowed for Binance snapshot")
    payload = json.dumps(frame["payload"], separators=(",", ":"), allow_nan=False).encode()
    return build_raw_envelope(
        capture_id=uuid.uuid4().bytes, provider=binding["provider"], venue=binding["venue"],
        market=binding["market"], product_type=binding["product_type"],
        native_symbol=binding["native_symbol"], native_channel=binding["native_channel"],
        subscription_id=binding["source_id"], source_session_id=ns + "." + _session_label(binding),
        connection_generation=frame["connection_generation"], lease_epoch=1,
        authority_revision=1, partition_plan_epoch=1, received_at_ns=frame["received_at_ns"],
        transport_protocol=2 if frame["transport"] == "HTTP" else 1,
        transport_compression=1, capture_boundary=2, raw_frame_bytes=payload,
        adapter_version=frame["adapter_version"], config_revision=1,
        instrument_catalog_revision=binding["instrument_catalog_revision"],
        correlation_id=str(uuid.uuid4()), test_provenance=True)


def _session_label(binding):
    """Linear scopes keep their venue label; another scope on the same venue gets its own session."""
    scope = scope_of(binding["venue"], binding["native_symbol"])
    return binding["venue"] if scope in VENUES else scope


class RawPublisher:
    """Single-flight, one attempt; never retry an ambiguous delivery automatically.

    Caller must run inside the disposable broker network namespace and attest it
    through P184_DR05_ISOLATED_NAMESPACE. No externally supplied Kafka endpoint
    or TLS override is accepted. Positive offsets alone cannot qualify a cell.
    """

    def __init__(self, packet, config):
        from confluent_kafka import Producer

        self.ns = namespace(packet["namespace"])
        if os.environ.get("P184_DR05_ISOLATED_NAMESPACE") != self.ns:
            raise ValueError("disposable namespace attestation required")
        if packet.get("bootstrap") != "localhost:9092":
            raise ValueError("isolated loopback broker required")
        if config.get("raw_topics") != [self.ns + ".raw"] or config["core"].get("allow_test_provenance") is not True:
            raise ValueError("isolated test-only raw topic required")
        self.topic = config["raw_topics"][0]
        bindings = config["core"]["bindings"]
        self.bindings = {scope_of(b["venue"], b["native_symbol"]): b for b in bindings}
        if (len(bindings) != len(self.bindings) or None in self.bindings
                or not set(VENUES) <= set(self.bindings)):
            raise ValueError("exact scope binding roster required (both linear venues, optional inverse)")
        self.producer = Producer({"bootstrap.servers": "localhost:9092", "security.protocol": "SSL",
            "ssl.ca.location": "/work/ca.crt", "ssl.certificate.location": "/work/client.crt",
            "ssl.key.location": "/work/client.key", "enable.idempotence": True, "acks": "all",
            "message.timeout.ms": 5000, "queue.buffering.max.messages": 8,
            "queue.buffering.max.kbytes": 1024, "linger.ms": 0})
        self.failed = False

    def publish(self, frame, *, timeout=5):
        if self.failed:
            raise RuntimeError("publisher fenced after unsuccessful/ambiguous delivery")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 5:
            raise ValueError("publication deadline must be in (0, 5] seconds")
        raw = raw_envelope(self.bindings[scope_of(frame["venue"], frame["native_symbol"])], frame, self.ns)
        wire = raw.SerializeToString()
        delivered = []
        start = time.monotonic()

        def ack(error, message):
            if error is not None:
                delivered.append({"error": str(error.code())})
            elif message.topic() != self.topic or message.partition() != 0 or message.offset() < 0:
                delivered.append({"error": "BROKER_OFFSET_OR_IDENTITY_UNKNOWN"})
            else:
                delivered.append({"topic": message.topic(), "partition": message.partition(),
                                  "offset": message.offset()})

        try:
            self.producer.produce(self.topic, partition=0, key=raw.subscription_id.encode(),
                value=wire, headers=[("qdl-event-id", raw.capture_id)], on_delivery=ack)
            deadline = start + timeout
            while not delivered:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("raw delivery unknown; do not resend this attempt")
                self.producer.poll(min(.1, remaining))
            if len(delivered) != 1 or "error" in delivered[0]:
                raise RuntimeError("raw publication failed: " + str(delivered))
        except BaseException:
            self.failed = True
            raise
        return {**delivered[0], "raw_sha256": hashlib.sha256(wire).hexdigest(),
                "capture_id": raw.capture_id.hex(), "generation": raw.connection_generation,
                "broker_ack_ms": (time.monotonic() - start) * 1000,
                "provenance": "TEST_ONLY", "acceptance": False}

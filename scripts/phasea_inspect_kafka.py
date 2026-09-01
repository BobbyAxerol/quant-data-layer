#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

from confluent_kafka import Consumer

from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--canonical-topic", required=True)
    parser.add_argument("--quarantine-topic", required=True)
    parser.add_argument("--cert-root", type=Path, required=True)
    parser.add_argument("--expected-canonical", type=int, required=True)
    parser.add_argument("--expected-quarantine", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--required-products", default="")
    args = parser.parse_args()
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"phase8-phase-a-inspect-{time.time_ns()}",
        "security.protocol": "ssl",
        "ssl.ca.location": str(args.cert_root / "ca.crt"),
        "ssl.certificate.location": str(args.cert_root / "client.crt"),
        "ssl.key.location": str(args.cert_root / "client.key"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    consumer.subscribe([args.canonical_topic, args.quarantine_topic])
    canonical = []
    quarantines = []
    deadline = time.monotonic() + args.timeout_seconds
    while time.monotonic() < deadline and (
        len(canonical) < args.expected_canonical
        or len(quarantines) < args.expected_quarantine
    ):
        message = consumer.poll(0.5)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(str(message.error()))
        if message.topic() == args.canonical_topic:
            envelope = market_data_pb2.EventEnvelope.FromString(message.value())
            canonical.append({
                "event_id": envelope.event_id.hex(),
                "venue": envelope.venue,
                "market": envelope.market,
                "product_type": envelope.product_type,
                "payload": envelope.WhichOneof("payload"),
                "source_role": envelope.source_role,
            })
        else:
            value = raw_provider_pb2.QuarantineRecord.FromString(message.value())
            raw = value.raw
            quarantines.append({
                "reason": value.reason,
                "evidence": value.evidence_sha256.hex(),
                "provider": raw.provider if raw else None,
                "venue": raw.venue if raw else None,
                "market": raw.market if raw else None,
                "native_symbol": raw.native_symbol if raw else None,
                "native_channel": raw.native_channel if raw else None,
                "safe_summary": value.safe_summary,
            })
    consumer.close()
    if len(canonical) != args.expected_canonical or len(quarantines) != args.expected_quarantine:
        raise RuntimeError(
            f"unexpected visible counts canonical={len(canonical)} quarantine={len(quarantines)}"
        )
    event_ids = [item["event_id"] for item in canonical]
    if len(event_ids) != len(set(event_ids)):
        raise RuntimeError("committed canonical topic contains duplicate event ids")
    venues = sorted({item["venue"] for item in canonical})
    products = sorted({
        f"{item['venue']}/{item['market']}/{item['product_type']}"
        for item in canonical
    })
    required_products = {
        value for value in args.required_products.split(",") if value
    }
    if required_products and not required_products.issubset(products):
        raise RuntimeError(
            f"multivenue canonical product coverage is incomplete: {products}"
        )
    print(json.dumps({
        "status": "PASS",
        "canonical": len(canonical),
        "quarantines": len(quarantines),
        "venues": venues,
        "products": products,
        "canonical_digest": hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "quarantine_reasons": [item["reason"] for item in quarantines],
        "quarantine_details": quarantines,
        "production_writes": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

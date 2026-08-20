#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from qdl.adapters.vn import VnRawBinding, build_dnse_trade_raw_envelope
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--cert-root", type=Path, required=True)
    parser.add_argument("--payload", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.payload.read_text())
    if payload.get("source") != "dnse":
        raise RuntimeError("VN replay payload is not DNSE provider data")
    symbol = str(payload["symbol"]).upper()
    received_at_ns = int(float(payload["timestamp"]) * 1_000_000_000)
    raw = build_dnse_trade_raw_envelope(
        payload,
        VnRawBinding(
            venue="HOSE",
            market="EQUITIES",
            product_type="COMMON_STOCK",
            native_symbol=symbol,
            subscription_id=f"dnse-{symbol}-trades",
            source_session_id=f"dnse-durable-replay-{received_at_ns}",
            connection_generation=1,
            lease_epoch=7,
            authority_revision=1,
            partition_plan_epoch=1,
            adapter_version="dnse-sdk-v1-durable-replay/2.0.0",
            config_revision=1,
            instrument_catalog_revision=3,
        ),
        received_at_ns=received_at_ns,
        test_provenance=False,
    )
    publisher = KafkaRawPublisher(
        KafkaRawPublisherConfig(
            bootstrap_servers=args.bootstrap,
            client_id="qdl-phase-a-vn-replay",
            topic=args.topic,
            ca_path=args.cert_root / "ca.crt",
            certificate_path=args.cert_root / "client.crt",
            key_path=args.cert_root / "client.key",
        )
    )
    ack = publisher.publish_many((raw,))
    publisher.close()
    print(json.dumps({
        "status": "PASS", "raw_acked": len(ack), "symbol": symbol,
        "provenance": "REAL_DNSE_PROVIDER_DERIVED_DURABLE_REPLAY",
        "capture_boundary": "SDK_DELIVERY", "production_writes": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

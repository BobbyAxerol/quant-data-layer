#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from qdl.adapters.binance import BinanceBarRawBinding, fetch_latest_closed_bar_raw_envelope
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--cert-root", type=Path, required=True)
    args = parser.parse_args()
    now = time.time_ns()
    envelopes = []
    rows = []
    for market, product in (("USDM", "PERPETUAL"), ("SPOT", "SPOT")):
        envelope = fetch_latest_closed_bar_raw_envelope(
            BinanceBarRawBinding(
                market=market,
                product_type=product,
                native_symbol="BTCUSDT",
                interval="1m",
                subscription_id=f"phase-a-binance-{market.lower()}-rest-bar",
                source_session_id=f"phase-a-binance-{market.lower()}-rest-{now}",
                connection_generation=1,
                lease_epoch=7,
                authority_revision=1,
                partition_plan_epoch=1,
                adapter_version="binance-rest/2.0.0",
                config_revision=1,
                instrument_catalog_revision=3,
            ),
            attempts=4,
            test_provenance=False,
        )
        payload = json.loads(envelope.raw_frame_bytes)
        rows.append({"market": market, "open_time_ms": payload["row"][0], "close_time_ms": payload["row"][6]})
        envelopes.append(envelope)
    publisher = KafkaRawPublisher(
        KafkaRawPublisherConfig(
            bootstrap_servers=args.bootstrap,
            client_id="qdl-phase-a-binance-rest-bars",
            topic=args.topic,
            ca_path=args.cert_root / "ca.crt",
            certificate_path=args.cert_root / "client.crt",
            key_path=args.cert_root / "client.key",
        )
    )
    acknowledgements = publisher.publish_many(envelopes)
    publisher.close()
    print(json.dumps({
        "status": "PASS", "raw_acked": len(acknowledgements),
        "rows": rows, "provenance": "REAL_BINANCE_REST_LATEST_CLOSED",
        "production_writes": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

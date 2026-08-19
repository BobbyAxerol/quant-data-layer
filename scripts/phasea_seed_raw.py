#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from qdl.raw.capture import capture_exact_frame
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


def raw_envelope(case: dict, *, authority_revision: int = 1):
    raw_bytes = json.dumps(
        case["raw"], sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return capture_exact_frame(
        provider=case["provider"],
        venue=case["venue"],
        market=case["market"],
        product_type=case["product_type"],
        native_symbol=case["native_symbol"],
        native_channel=case["native_channel"],
        subscription_id=f"phase-a-{case['native_channel']}",
        source_session_id=case["source_session_id"],
        connection_generation=case["generation"],
        lease_epoch=7,
        authority_revision=authority_revision,
        partition_plan_epoch=1,
        received_at_ns=case["received_at_ns"],
        raw_frame_bytes=raw_bytes,
        adapter_version=case["adapter_version"],
        config_revision=1,
        instrument_catalog_revision=3,
        correlation_id=case["correlation_id"],
        test_provenance=True,
    )


def cases():
    started = 1_786_352_400_123_456_000
    binance_10 = {
        "s": "BTCUSDT", "t": 10, "p": "60000.1", "q": "0.01", "T": 3, "m": False,
    }
    common_binance = {
        "provider": "BINANCE_DIRECT", "venue": "BINANCE", "market": "USDM",
        "product_type": "PERPETUAL", "native_symbol": "BTCUSDT",
        "native_channel": "trade", "source_session_id": "binance-phase-a-1",
        "generation": 1, "adapter_version": "binance-json/2.0.0",
    }
    result = [
        {**common_binance, "raw": binance_10, "received_at_ns": started, "correlation_id": "binance-10"},
        {**common_binance, "raw": {**binance_10, "t": 11, "T": 4}, "received_at_ns": started + 1, "correlation_id": "binance-11"},
        {**common_binance, "raw": binance_10, "received_at_ns": started, "correlation_id": "binance-10-duplicate"},
    ]
    result.append({
        "provider": "OKX_DIRECT", "venue": "OKX", "market": "SWAP",
        "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP",
        "native_channel": "trades", "source_session_id": "okx-phase-a-1",
        "generation": 1, "adapter_version": "okx-json/2.0.0",
        "received_at_ns": started + 2, "correlation_id": "okx-trade",
        "raw": {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
            {"instId": "BTC-USDT-SWAP", "tradeId": "20", "px": "60001", "sz": "2", "side": "buy", "ts": "5"}
        ]},
    })
    result.append({
        "provider": "DNSE_DIRECT", "venue": "HNX", "market": "VN_DERIVATIVES",
        "product_type": "FUTURE", "native_symbol": "VN30F1M",
        "native_channel": "trades", "source_session_id": "dnse-phase-a-1",
        "generation": 1, "adapter_version": "dnse-sdk/2.0.0",
        "received_at_ns": started + 3, "correlation_id": "dnse-trade",
        "raw": {"symbol": "VN30F1M", "price": "1820.7", "quantity": "12"},
    })
    result.append({
        "provider": "VNSTOCK", "venue": "HOSE", "market": "EQUITIES",
        "product_type": "COMMON_STOCK", "native_symbol": "FPT",
        "native_channel": "ohlcv/1m", "source_session_id": "vnstock-phase-a-1",
        "generation": 1, "adapter_version": "vnstock/2.0.0",
        "received_at_ns": started + 4, "correlation_id": "vnstock-bar",
        "raw": {"symbol": "FPT", "interval": "1m", "open_time_ms": 1,
                "close_time_ms": 2, "o": "1", "h": "2", "l": "1", "c": "2",
                "v": "100", "is_final": True, "trade_count_available": False, "revision": 0},
    })
    bbo_common = {
        "provider": "OKX_DIRECT", "venue": "OKX", "market": "SWAP",
        "product_type": "PERPETUAL", "native_symbol": "BTC-USDT-SWAP",
        "native_channel": "bbo-tbt", "source_session_id": "okx-bbo-phase-a-1",
        "generation": 1, "adapter_version": "okx-json/2.0.0",
    }
    for index, sequence in enumerate((10, 12), start=5):
        result.append({
            **bbo_common,
            "received_at_ns": started + index,
            "correlation_id": f"okx-bbo-{sequence}",
            "raw": {"arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
                    "data": [{"bids": [["1", "2", "0", "1"]],
                              "asks": [["2", "3", "0", "1"]],
                              "seqId": sequence, "ts": str(10 + index)}]},
        })
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--cert-root", type=Path, required=True)
    args = parser.parse_args()
    publisher = KafkaRawPublisher(
        KafkaRawPublisherConfig(
            bootstrap_servers=args.bootstrap,
            client_id="qdl-phase-a-seed",
            topic=args.topic,
            ca_path=args.cert_root / "ca.crt",
            certificate_path=args.cert_root / "client.crt",
            key_path=args.cert_root / "client.key",
        )
    )
    values = tuple(raw_envelope(case) for case in cases())
    acknowledgements = publisher.publish_many(values)
    publisher.close()
    print(json.dumps({
        "status": "PASS", "raw_acked": len(acknowledgements),
        "capture_ids": [item.capture_id.hex() for item in acknowledgements],
        "production_writes": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

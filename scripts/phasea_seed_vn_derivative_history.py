#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import capture_exact_frame
from qdl.transport.kafka_raw import KafkaRawPublisher, KafkaRawPublisherConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--cert-root", type=Path, required=True)
    parser.add_argument("--parquet", type=Path, required=True)
    args = parser.parse_args()
    payload_bytes = args.parquet.read_bytes()
    frame = pd.read_parquet(args.parquet)
    if frame.empty:
        raise RuntimeError("VN derivative canonical Parquet is empty")
    row = frame.sort_values("time").iloc[-1]
    local_time = pd.Timestamp(row["time"]).to_pydatetime().replace(
        tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")
    )
    open_time_ms = int(local_time.timestamp() * 1000)
    raw = {
        "symbol": "VN30F1M",
        "interval": "1m",
        "open_time_ms": open_time_ms,
        "close_time_ms": open_time_ms + 59_999,
        "o": str(row["open"]),
        "h": str(row["high"]),
        "l": str(row["low"]),
        "c": str(row["close"]),
        "v": str(row["volume"]),
        "is_final": True,
        "trade_count_available": False,
        "revision": 0,
    }
    raw_bytes = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    now = time.time_ns()
    envelope = capture_exact_frame(
        provider="DNSE_DIRECT",
        venue="HNX",
        market="VN_DERIVATIVES",
        product_type="FUTURE",
        native_symbol="VN30F1M",
        native_channel="ohlcv/1m",
        subscription_id="phase-a-vn30f1m-history",
        source_session_id=f"dnse-parquet-replay-{hashlib.sha256(payload_bytes).hexdigest()[:16]}",
        connection_generation=1,
        lease_epoch=7,
        authority_revision=1,
        partition_plan_epoch=1,
        received_at_ns=now,
        raw_frame_bytes=raw_bytes,
        adapter_version="dnse-canonical-parquet-replay/2.0.0",
        config_revision=1,
        instrument_catalog_revision=3,
        correlation_id=f"dnse:VN30F1M:1m:{open_time_ms}",
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_FILE_REPLAY,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_REPLAY_BYTES,
        test_provenance=False,
    )
    publisher = KafkaRawPublisher(
        KafkaRawPublisherConfig(
            bootstrap_servers=args.bootstrap,
            client_id="qdl-phase-a-vn-derivative-history",
            topic=args.topic,
            ca_path=args.cert_root / "ca.crt",
            certificate_path=args.cert_root / "client.crt",
            key_path=args.cert_root / "client.key",
        )
    )
    ack = publisher.publish_many((envelope,))
    publisher.close()
    print(json.dumps({
        "status": "PASS",
        "raw_acked": len(ack),
        "symbol": "VN30F1M",
        "open_time_local": local_time.isoformat(),
        "parquet_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "provenance": "REAL_DNSE_PROVIDER_DERIVED_CANONICAL_PARQUET_REPLAY",
        "capture_boundary": "REPLAY_BYTES",
        "production_writes": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

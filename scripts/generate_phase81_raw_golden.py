#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import build_raw_envelope


GOLDEN = ROOT / "contracts/golden/phase8/raw-provider-envelope.bin"
EVIDENCE = ROOT / "upgrade/evidence/phase8-raw-envelope-golden.json"


def build() -> raw_provider_pb2.RawProviderEnvelope:
    return build_raw_envelope(
        capture_id=bytes(range(16)),
        provider="BINANCE_DIRECT",
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        native_channel="btcusdt@aggTrade",
        subscription_id="sub-1",
        source_session_id="session-1",
        connection_generation=2,
        lease_epoch=3,
        authority_revision=4,
        partition_plan_epoch=5,
        received_at_ns=1_000_000,
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_WEBSOCKET,
        transport_compression=raw_provider_pb2.TRANSPORT_COMPRESSION_NONE,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_POST_DECOMPRESSION,
        raw_frame_bytes=b'{"e":"aggTrade","s":"BTCUSDT","a":1}',
        adapter_version="binance/2.0.0",
        config_revision=6,
        instrument_catalog_revision=7,
        correlation_id="corr-1",
        test_provenance=True,
    )


def main() -> int:
    payload = build().SerializeToString(deterministic=True)
    GOLDEN.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN.write_bytes(payload)
    evidence = {
        "schema": "qdl.phase8.raw-envelope-golden.v1",
        "status": "PASS",
        "provenance": "DETERMINISTIC_FIXTURE",
        "bytes": len(payload),
        "protobuf_sha256": hashlib.sha256(payload).hexdigest(),
        "raw_frame_sha256": hashlib.sha256(build().raw_frame_bytes).hexdigest(),
        "capture_boundary": "POST_DECOMPRESSION",
        "python_rust_golden_path": str(GOLDEN.relative_to(ROOT)),
    }
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

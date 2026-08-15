from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from qdl.canonical.trade import TradeContext, canonicalize_binance_usdm_trade
from qdl.marketdata.v2 import market_data_pb2


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--image-id", required=True)
    args = parser.parse_args()
    lines = (args.state_dir / "binance-shadow.wal.jsonl").read_text().splitlines()
    if len(lines) != 3:
        raise RuntimeError(f"expected exactly three bounded Rust events, got {len(lines)}")
    checksums = []
    event_ids = []
    source_times = []
    for line in lines:
        record = json.loads(line)
        if record.get("provenance") != "REAL_PROVIDER":
            raise RuntimeError("Rust WAL record is not marked REAL_PROVIDER")
        canonical = bytes.fromhex(record["canonical_hex"])
        if hashlib.sha256(canonical).hexdigest() != record["canonical_sha256"]:
            raise RuntimeError("Rust WAL canonical checksum mismatch")
        envelope = market_data_pb2.EventEnvelope.FromString(canonical)
        raw = json.loads(record["raw_frame"])["data"]
        context = TradeContext(
            instrument_uid=envelope.instrument_uid,
            instrument_id=envelope.instrument_id,
            instrument_revision=envelope.instrument_revision,
            venue=envelope.venue,
            market=envelope.market,
            product_type=envelope.product_type,
            native_symbol=envelope.native_symbol,
            provider=envelope.provider,
            source_id=envelope.source_id,
            lease_epoch=envelope.lease_epoch,
            received_at_ns=envelope.received_at_ns,
            normalized_at_ns=envelope.normalized_at_ns,
            published_at_ns=envelope.published_at_ns,
            partition_sequence=envelope.partition_sequence,
            normalizer_version=envelope.normalizer_version,
            adapter_version=envelope.adapter_version,
            config_revision=envelope.config_revision,
            correlation_id=envelope.correlation_id,
        )
        python_bytes = canonicalize_binance_usdm_trade(
            raw, context
        ).SerializeToString(deterministic=True)
        if python_bytes != canonical:
            raise RuntimeError("Python/Rust real-frame canonical bytes diverged")
        checksums.append(record["canonical_sha256"])
        event_ids.append(envelope.event_id.hex())
        source_times.append(envelope.source_event_time_ns)
    result = {
        "schema": "qdl.phase3.rust-binance-real-parity.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "production_writes": 0,
        "event_count": len(lines),
        "exact_python_rust_parity": True,
        "canonical_sha256": checksums,
        "event_ids": event_ids,
        "source_event_time_ns": source_times,
        "image_id": args.image_id,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

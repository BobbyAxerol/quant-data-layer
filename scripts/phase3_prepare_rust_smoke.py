from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

from qdl.domain.instrument import InstrumentIdentity, ProductType


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    response = requests.get(
        "https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=10
    )
    response.raise_for_status()
    exchange_info = response.json()
    if not any(
        item.get("symbol") == "BTCUSDT" and item.get("status") == "TRADING"
        for item in exchange_info.get("symbols", [])
    ):
        raise RuntimeError("real Binance exchangeInfo does not declare BTCUSDT TRADING")
    identity = InstrumentIdentity.create(
        venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
        canonical_symbol="BTCUSDT",
    )
    now = time.time_ns()
    config = {
        "context": {
            "instrument_uid": identity.instrument_uid,
            "instrument_id": identity.instrument_id,
            "instrument_revision": 1,
            "venue": "BINANCE",
            "market": "USDM",
            "product_type": "PERPETUAL",
            "native_symbol": "BTCUSDT",
            "provider": "BINANCE_DIRECT",
            "source_id": "binance-usdm-rust-phase3-real-smoke",
            "lease_epoch": 1,
            "received_at_ns": now,
            "normalized_at_ns": now,
            "published_at_ns": now,
            "partition_sequence": 0,
            "normalizer_version": "qdl-normalizer/2.0.0-phase3",
            "adapter_version": "qdl-rust-binance/0.1.0",
            "config_revision": 1,
            "correlation_id": "phase3-rust-real-provider-smoke"
        },
        "streams": ["btcusdt@trade"],
        "exchange_info_path": "/state/exchangeInfo.json",
        "wal_path": "/state/binance-shadow.wal.jsonl",
        "max_events": 3,
        "timeout_seconds": 30
    }
    (args.state_dir / "exchangeInfo.json").write_text(
        json.dumps(exchange_info, sort_keys=True, separators=(",", ":")) + "\n"
    )
    (args.state_dir / "config.json").write_text(
        json.dumps(config, indent=2, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GENERATED_PYTHON = ROOT / "generated" / "python"
GOLDEN_DIR = ROOT / "contracts" / "golden" / "canonical"
sys.path.insert(0, str(GENERATED_PYTHON))

from qdl.common.v1 import common_pb2  # noqa: E402
from qdl.marketdata.v2 import market_data_pb2  # noqa: E402


def decimal(mantissa: int, scale: int, source_text: str):
    return common_pb2.DecimalValue(
        mantissa=mantissa,
        scale=scale,
        source_text=source_text,
    )


def build_trade_envelope():
    return market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.trade",
        schema_major=2,
        schema_minor=0,
        event_id=bytes(range(16)),
        instrument_uid="85ad7cb6-7ebf-5c81-9d82-12c4c10ca85c",
        instrument_id="BINANCE.USDM.PERPETUAL.BTC-USDT",
        instrument_revision=7,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol="BTCUSDT",
        provider="BINANCE_DIRECT",
        source_id="binance-usdm-trade-003",
        source_role=common_pb2.SOURCE_ROLE_PRIMARY,
        lease_epoch=42,
        source_event_time_ns=1_786_352_400_123_000_000,
        received_at_ns=1_786_352_400_123_456_000,
        normalized_at_ns=1_786_352_400_123_500_000,
        published_at_ns=1_786_352_400_123_700_000,
        source_sequence="9876543210123456789",
        partition_sequence=1234,
        normalizer_version="qdl-normalizer/2.0.0",
        adapter_version="binance/1.0.0",
        raw_payload_hash=bytes(range(16, 48)),
        correlation_id="phase1-golden-trade",
        config_revision=9,
        trade=market_data_pb2.Trade(
            native_trade_id="184467440737095516160",
            price=decimal(6_123_410, 2, "61234.10"),
            quantity=decimal(125, 3, "0.125"),
            aggressor_side=common_pb2.AGGRESSOR_SIDE_BUY,
            quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET,
            identity_kind=market_data_pb2.TRADE_IDENTITY_KIND_NATIVE,
        ),
    )


def main() -> None:
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_trade_envelope().SerializeToString(deterministic=True)
    target = GOLDEN_DIR / "trade-envelope.bin"
    target.write_bytes(payload)
    (GOLDEN_DIR / "trade-envelope.json").write_text(
        json.dumps(
            {
                "schema": "qdl.marketdata.trade",
                "schema_major": 2,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "producer": "generated/python protobuf 6.31.1",
                "rust_consumer": "qdl-contracts prost 0.13.5",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()


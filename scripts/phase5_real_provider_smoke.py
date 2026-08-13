from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import requests

from qdl.canonical.trade import TradeContext, canonicalize_binance_usdm_trade
from qdl.domain.instrument import InstrumentIdentity, ProductType


def run(base_url: str, *, timeout_seconds: float) -> dict:
    root = base_url.rstrip("/")
    health = requests.get(f"{root}/v1/health", timeout=timeout_seconds)
    health.raise_for_status()
    binance_response = requests.get(
        f"{root}/v1/binance/price-last/BTCUSDT",
        params={"market": "usdm"}, timeout=timeout_seconds,
    )
    binance_response.raise_for_status()
    binance = binance_response.json()["snapshot"]
    raw = binance["raw"]
    identity = InstrumentIdentity.create(
        venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
        canonical_symbol="BTC-USDT",
    )
    now_ns = time.time_ns()
    envelope = canonicalize_binance_usdm_trade(raw, TradeContext(
        identity.instrument_uid, identity.instrument_id, 1, "BINANCE", "USDM",
        "PERPETUAL", "BTCUSDT", "BINANCE_DIRECT", "phase5-read-only", 1,
        now_ns, now_ns, now_ns, 1, "phase5-smoke", "legacy-v1-read", 1,
    ))
    okx_response = requests.get(
        f"{root}/v1/crypto/ohlcv/okx/BTC-USDT-SWAP",
        params={"interval": "1m", "limit": 5}, timeout=timeout_seconds,
    )
    okx_response.raise_for_status()
    okx = okx_response.json()
    rows = okx.get("data")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("OKX V5 returned no provider-authentic history rows")
    timestamps = [int(row[0]) for row in rows]
    if len(timestamps) != len(set(timestamps)):
        raise RuntimeError("OKX V5 returned duplicate candle timestamps")
    parity = {
        "price": float(envelope.trade.price.source_text) == float(binance["price"]),
        "quantity": float(envelope.trade.quantity.source_text) == float(binance["quantity"]),
        "trade_id": envelope.trade.native_trade_id == str(binance["trade_id"]),
        "event_time": envelope.source_event_time_ns // 1_000_000 == int(binance["event_time"]),
    }
    if not all(parity.values()):
        raise RuntimeError(f"Binance V1/canonical V2 value parity failed: {parity}")
    return {
        "schema": "qdl.phase5.real-provider-shadow-smoke.v1",
        "status": "PASS",
        "authority": "V1_UNCHANGED",
        "production_writes": 0,
        "health_status": health.json().get("status"),
        "binance": {
            "market": binance.get("market"),
            "provider": binance.get("provider"),
            "canonical_parity": parity,
        },
        "okx": {
            "instId": okx.get("instId"),
            "provider_interval": okx.get("provider_interval"),
            "rows": len(rows),
            "cached": okx.get("cached"),
            "stored": okx.get("stored"),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--timeout-seconds", type=float, default=20)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.base_url, timeout_seconds=args.timeout_seconds)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()

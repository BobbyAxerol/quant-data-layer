from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path

import requests

from qdl.canonical.trade import (
    TradeContext,
    canonical_event,
    canonicalize_binance_usdm_trade,
    raw_trade_event,
)
from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.pipeline import ShadowCanonicalPipeline
from qdl.projection import InMemoryProjectionTarget, TradeProjector
from qdl.transport import SQLiteDurableSpool, SpoolConfig


def context(symbol: str, index: int, now_ns: int) -> TradeContext:
    base = symbol.removesuffix("USDT")
    identity = InstrumentIdentity.create(
        venue="BINANCE",
        market="USDM",
        product_type=ProductType.PERPETUAL,
        canonical_symbol=f"{base}-USDT",
    )
    return TradeContext(
        instrument_uid=identity.instrument_uid,
        instrument_id=identity.instrument_id,
        instrument_revision=1,
        venue="BINANCE",
        market="USDM",
        product_type="PERPETUAL",
        native_symbol=symbol,
        provider="BINANCE_DIRECT",
        source_id="binance-usdm-trade-phase2-shadow",
        lease_epoch=1,
        received_at_ns=now_ns,
        normalized_at_ns=now_ns + 1,
        published_at_ns=now_ns + 2,
        partition_sequence=index + 1,
        normalizer_version="qdl-normalizer/2.0.0",
        adapter_version="binance-json/phase2-shadow",
        config_revision=1,
        correlation_id=f"phase2-live-shadow-{symbol.lower()}",
    )


def run(base_url: str, symbols: list[str], timeout_seconds: float) -> dict:
    snapshots = []
    for symbol in symbols:
        response = requests.get(
            f"{base_url.rstrip('/')}/v1/binance/price-last/{symbol}",
            params={"market": "usdm"},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()
        snapshot = body.get("snapshot")
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("raw"), dict):
            raise RuntimeError(f"missing raw V1 trade snapshot for {symbol}")
        snapshots.append((symbol, snapshot))

    with tempfile.TemporaryDirectory(prefix="qdl-phase2-live-shadow.") as directory:
        with SQLiteDurableSpool(
            SpoolConfig(
                path=Path(directory) / "shadow.sqlite3",
                max_records=100,
                max_payload_bytes=1_000_000,
                max_event_bytes=100_000,
                min_free_disk_bytes=0,
            )
        ) as spool:
            canonical_rows = []
            for index, (symbol, snapshot) in enumerate(snapshots):
                trade_context = context(symbol, index, time.time_ns())
                raw = raw_trade_event(
                    snapshot["raw"],
                    context=trade_context,
                    accepted_at_ns=trade_context.received_at_ns,
                )

                def canonicalizer(raw_event, selected_context=trade_context):
                    envelope = canonicalize_binance_usdm_trade(
                        json.loads(raw_event.payload), selected_context
                    )
                    return canonical_event(
                        envelope,
                        accepted_at_ns=selected_context.normalized_at_ns,
                        raw_event=raw_event,
                    )

                _, canonical_result = ShadowCanonicalPipeline(
                    spool,
                    consumer_id=f"phase2-canonicalizer-{symbol.lower()}",
                    canonicalizer=canonicalizer,
                ).accept(raw)
                canonical_rows.extend(
                    spool.read(
                        stream="md.canonical.v2.trade",
                        partition_key=canonical_result.cursor.partition_key,
                    )
                )

            target = InMemoryProjectionTarget()
            projector = TradeProjector(
                target,
                raw_resolver=lambda stream, event_id: (
                    found.event.payload
                    if (found := spool.find_event(stream=stream, event_id=event_id))
                    else None
                ),
            )
            for row in canonical_rows:
                if not projector.project(row):
                    raise RuntimeError("unexpected duplicate in first shadow projection")
            spool_stats = spool.stats()

        return {
            "schema": "qdl.phase2.live-shadow-smoke.v1",
            "status": "PASS",
            "authority": "LOCAL_SHADOW_ONLY",
            "symbols": symbols,
            "v1_reads": len(snapshots),
            "raw_events": len(snapshots),
            "canonical_events": len(canonical_rows),
            "projected_keys": len(target.latest),
            "projection_checksum": target.checksum(),
            "spool_storage_bytes": spool_stats.storage_bytes,
            "production_writes": 0,
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    result = run(args.base_url, symbols, args.timeout_seconds)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()

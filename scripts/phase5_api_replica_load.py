from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path

import httpx

from qdl.api_v2 import create_v2_app
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass, InstrumentIdentity, InstrumentRecord, InstrumentRegistry, ProductType,
)
from qdl.query import (
    AccessPurpose, ConsumerGrade, DataProduct, DataRequirement, EntitlementGrant,
    EntitlementPolicy, FeedType, InstrumentQuery, MarketDataItem,
    MemoryMarketDataBackend, QualityMetadata, SourceMetadata, V2QueryService,
)


def _service():
    identity = InstrumentIdentity.create(
        venue="BINANCE", market="USDM", product_type=ProductType.PERPETUAL,
        canonical_symbol="BTC-USDT",
    )
    record = InstrumentRecord(
        identity=identity, metadata_revision=1, asset_class=AssetClass.DERIVATIVE,
        native_symbol="BTCUSDT", base_asset="BTC", quote_asset="USDT",
        settlement_asset="USDT", price_tick=CanonicalDecimal.from_text("0.1"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
    )
    registry = InstrumentRegistry()
    registry.register(record, [])
    requirement = DataRequirement(
        record.instrument_uid, FeedType.TRADE, ConsumerGrade.EXECUTION,
        "execution_binance_usdm_v1", max_freshness_ms=1000,
    )
    backend = MemoryMarketDataBackend()
    backend.put_latest(requirement, MarketDataItem(
        record.instrument_uid, record.instrument_id, 1, FeedType.TRADE,
        time.time_ns(), {"price": "60000.1", "quantity": "0.01"},
        SourceMetadata("BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True),
        QualityMetadata("LIVE", 1, False, True, True, "execution_binance_usdm_v1"),
        cursor="signed-shadow-cursor", snapshot_id="snapshot", watermark_offset=0,
    ))
    service = V2QueryService(
        instruments=InstrumentQuery(registry), backend=backend,
        entitlements=EntitlementPolicy((EntitlementGrant(
            "BINANCE_DIRECT", "public-v1",
            frozenset({AccessPurpose.INTERNAL_EXECUTION}),
            frozenset({DataProduct.CANONICAL_SNAPSHOT}), 0,
        ),)),
    )
    return service, record.instrument_uid


async def run(*, replicas: int, requests: int, concurrency: int) -> dict:
    if replicas < 1 or requests < 1 or concurrency < 1:
        raise ValueError("replicas, requests and concurrency must be positive")
    clients = []
    uid = ""
    for index in range(replicas):
        service, uid = _service()
        app = create_v2_app(service)
        if app.state.runtime_manifest["owns_venue_connections"]:
            raise RuntimeError(f"API replica {index} unexpectedly owns venue connections")
        clients.append(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=f"http://replica-{index}",
        ))
    semaphore = asyncio.Semaphore(concurrency)
    latencies = []

    async def request(index: int):
        async with semaphore:
            started = time.perf_counter_ns()
            response = await clients[index % replicas].get(
                f"/v2/market-data/{uid}/snapshot",
                params={
                    "feed": "TRADE", "consumer_grade": "EXECUTION",
                    "source_policy_id": "execution_binance_usdm_v1",
                    "max_freshness_ms": 1000,
                },
                headers={"X-QDL-Purpose": "INTERNAL_EXECUTION"},
            )
            response.raise_for_status()
            latencies.append((time.perf_counter_ns() - started) / 1_000_000)

    started = time.perf_counter()
    await asyncio.gather(*(request(index) for index in range(requests)))
    elapsed = time.perf_counter() - started
    for client in clients:
        await client.aclose()
    ordered = sorted(latencies)
    p99 = ordered[max(0, int(len(ordered) * 0.99) - 1)]
    return {
        "schema": "qdl.phase5.api-replica-load.v1",
        "status": "MEASURED",
        "replicas": replicas,
        "requests": requests,
        "concurrency": concurrency,
        "requests_per_second": round(requests / elapsed, 2),
        "latency_ms": {
            "p50": round(statistics.median(ordered), 3),
            "p99": round(p99, 3),
            "max": round(max(ordered), 3),
        },
        "venue_connection_attempts": 0,
        "live_ingestion_owners": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--replicas", type=int, default=8)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--min-rps", type=float, default=250)
    parser.add_argument("--max-p99-ms", type=float, default=500)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(run(
        replicas=args.replicas, requests=args.requests, concurrency=args.concurrency
    ))
    if result["requests_per_second"] < args.min_rps:
        raise SystemExit(f"API throughput gate failed: {result}")
    if result["latency_ms"]["p99"] > args.max_p99_ms:
        raise SystemExit(f"API latency gate failed: {result}")
    result["status"] = "PASS"
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()

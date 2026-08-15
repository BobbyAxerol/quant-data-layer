from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
import uuid
from pathlib import Path

import httpx
import jwt

from qdl.api_v2 import create_v2_app
from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass, InstrumentIdentity, InstrumentRecord, InstrumentRegistry, ProductType,
)
from qdl.query import (
    AccessPurpose, ConsumerGrade, ContractMetadata, DataProduct, DataRequirement,
    EntitlementGrant, EntitlementPolicy, FeedType, InstrumentQuery, MarketDataItem,
    MemoryMarketDataBackend, QualityMetadata, SourceMetadata, V2QueryService,
)
from qdl.security import DataPlaneIdentityService, DataPlaneSecurityConfig


_CONSUMER_ID = "phase5-api-replica-load"
_SUBJECT = "spiffe://qdl/test/phase5-api-replica-load"
_KEY_ID = "phase5-load"
_SECRET = b"phase5-load-secret-material-32bytes"
_ISSUER = "https://identity.qdl.load"
_AUDIENCE = "qdl-v2-load"


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
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=1,
        feed=FeedType.TRADE,
        observed_at_ns=time.time_ns(),
        payload={
            "native_trade_id": "phase5-load-trade",
            "price": "60000.1",
            "quantity": "0.01",
            "aggressor_side": "BUY",
            "is_block_trade": False,
            "is_buyer_maker": False,
        },
        source=SourceMetadata(
            "BINANCE", "BINANCE_DIRECT", "BINANCE_DIRECT", "PRIMARY", True
        ),
        quality=QualityMetadata(
            "LIVE", 1, False, True, False, "execution_binance_usdm_v1"
        ),
        contract=ContractMetadata(
            schema_digest="5" * 64,
            contract_version="2.0.0-beta.1",
            normalizer_version="phase5-load-v2",
            adapter_version="phase5-load-v2",
            instrument_catalog_revision=1,
            source_policy_revision=1,
            authority_revision=1,
            config_revision=1,
            correlation_id="phase5-replica-load",
        ),
        cursor="signed-shadow-cursor",
        snapshot_id="snapshot",
        watermark_offset=0,
    ))
    service = V2QueryService(
        instruments=InstrumentQuery(registry), backend=backend,
        entitlements=EntitlementPolicy((EntitlementGrant(
            "BINANCE_DIRECT", "public-v1",
            frozenset({AccessPurpose.INTERNAL_EXECUTION}),
            frozenset({DataProduct.CANONICAL_SNAPSHOT}), 0,
        ),)),
    )
    manifest = ConsumerManifestLoader.from_mapping({
        "apiVersion": "qdl/v2",
        "kind": "DataRequirement",
        "metadata": {
            "id": _CONSUMER_ID,
            "owner": "qdl-load-gate",
            "subject": _SUBJECT,
            "environment": "paper",
            "revision": 1,
        },
        "spec": {
            "sdk_major": 2,
            "rollback_contract": "V1",
            "execution_dependency": "PAPER_ONLY",
            "permissions": ["snapshot:read"],
            "purposes": ["INTERNAL_EXECUTION"],
            "quotas": {
                "requests_per_minute": 100_000,
                "max_batch_items": 1,
                "max_warmup_rows": 1,
                "max_streams": 1,
                "max_buffer_events": 1,
            },
            "requirements": [{
                "instrument_uid": record.instrument_uid,
                "feed": "TRADE",
                "consumer_grade": "EXECUTION",
                "source_policy_id": "execution_binance_usdm_v1",
                "warmup_limit": 0,
                "max_freshness_ms": 1000,
                "require_full_coverage": True,
                "require_final_bars": True,
                "stale_policy": "BLOCK",
                "gap_policy": "BLOCK",
                "recovery": "SNAPSHOT_AND_REPLAY",
                "bar_revision_policy": "LATEST",
            }],
        },
    })
    identity_service = DataPlaneIdentityService(
        DataPlaneSecurityConfig(
            environment="paper",
            issuer=_ISSUER,
            audience=_AUDIENCE,
            keys_by_id={_KEY_ID: _SECRET},
            algorithms=("HS256",),
        ),
        ConsumerManifestRegistry((manifest,)),
    )
    now = int(time.time())
    token = jwt.encode(
        {
            "sub": _SUBJECT,
            "iss": _ISSUER,
            "aud": _AUDIENCE,
            "iat": now,
            "exp": now + 300,
            "jti": str(uuid.uuid4()),
            "environment": "paper",
            "roles": ["market_data_reader"],
            "consumer_manifest_revision": 1,
        },
        _SECRET,
        algorithm="HS256",
        headers={"kid": _KEY_ID},
    )
    return service, record.instrument_uid, identity_service, token


async def run(*, replicas: int, requests: int, concurrency: int) -> dict:
    if replicas < 1 or requests < 1 or concurrency < 1:
        raise ValueError("replicas, requests and concurrency must be positive")
    clients = []
    uid = ""
    for index in range(replicas):
        service, uid, identity_service, token = _service()
        app = create_v2_app(service, identity_service=identity_service)
        if app.state.runtime_manifest["owns_venue_connections"]:
            raise RuntimeError(f"API replica {index} unexpectedly owns venue connections")
        clients.append(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=f"http://replica-{index}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-QDL-Consumer-ID": _CONSUMER_ID,
                "X-QDL-Purpose": "INTERNAL_EXECUTION",
            },
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

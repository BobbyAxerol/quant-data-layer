#!/usr/bin/env python3
"""Bounded, read-only public-provider smoke for Phase 10.4-B.

The program intentionally prints only semantic counts/times and SHA-256 of
the normalized result. It never writes a response, cache, spool or runtime
state. It is not a deployment command and has no V1/V2 route effect.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from dataclasses import asdict

from qdl.adapters.binance.reference import BinanceUsdmReferenceAdapter
from qdl.adapters.okx.client import OkxRestClient
from qdl.adapters.okx.reference import OkxSwapReferenceAdapter
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentIdentity,
    InstrumentRecord,
    ProductType,
)
from qdl.reference import (
    BasisSeries,
    LongShortKind,
    ReferenceBatch,
    ReferenceProduct,
    ReferenceRequest,
    ReferenceStatus,
)


def _instrument(
    *, venue: str, market: str, native_symbol: str, base: str, attributes: dict[str, str]
) -> InstrumentRecord:
    return InstrumentRecord(
        identity=InstrumentIdentity.create(
            venue=venue,
            market=market,
            product_type=ProductType.PERPETUAL,
            canonical_symbol=f"{base}-USDT",
        ),
        metadata_revision=1,
        asset_class=AssetClass.DERIVATIVE,
        native_symbol=native_symbol,
        base_asset=base,
        quote_asset="USDT",
        settlement_asset="USDT",
        price_tick=CanonicalDecimal.from_text("0.01"),
        quantity_step=CanonicalDecimal.from_text("0.001"),
        contract_multiplier=CanonicalDecimal.from_text("1"),
        session_calendar_id="CRYPTO_24X7",
        attributes=attributes,
    )


def _digest(result) -> str:
    material = {
        "request": {
            "instrument_uid": result.request.instrument.instrument_uid,
            "revision": result.request.instrument.metadata_revision,
            "product": result.request.product.value,
        },
        "status": result.status.value,
        "coverage": asdict(result.coverage),
        "lineage": [asdict(item) for item in result.lineage],
        "observations": [
            {
                "at_ns": item.observed_at_ns,
                "fields": [
                    (field.name, field.value.source_text, field.unit)
                    for field in item.fields
                ],
                "labels": item.labels,
            }
            for item in result.observations
        ],
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _summary(result) -> dict[str, object]:
    endpoints = [lineage.provider_endpoint for lineage in result.lineage]
    observed = [item.observed_at_ns // 1_000_000 for item in result.observations]
    return {
        "venue": result.request.instrument.identity.venue,
        "market": result.request.instrument.identity.market,
        "native_symbol": result.request.instrument.native_symbol,
        "product": result.request.product.value,
        "status": result.status.value,
        "endpoint": endpoints,
        "observation_count": len(result.observations),
        "observed_min_ms": min(observed) if observed else None,
        "observed_max_ms": max(observed) if observed else None,
        "coverage": result.coverage.terminal_reason,
        "error_code": result.error_code,
        "semantic_sha256": _digest(result),
    }


async def _run(window_hours: int) -> tuple[dict[str, object], ...]:
    end_ms = int(time.time() * 1000) - 60_000
    start_ms = end_ms - window_hours * 60 * 60 * 1000
    binance = tuple(
        _instrument(
            venue="BINANCE", market="USDM", native_symbol=symbol, base=base, attributes={}
        )
        for symbol, base in (("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"))
    )
    okx = tuple(
        _instrument(
            venue="OKX", market="SWAP", native_symbol=symbol, base=base,
            attributes={"instFamily": f"{base}-USDT"},
        )
        for symbol, base in (("BTC-USDT-SWAP", "BTC"), ("ETH-USDT-SWAP", "ETH"))
    )
    batch = ReferenceBatch({
        ("BINANCE", "USDM"): BinanceUsdmReferenceAdapter(),
        ("OKX", "SWAP"): OkxSwapReferenceAdapter(OkxRestClient()),
    })
    requests = []
    for item in binance:
        requests.extend((
            ReferenceRequest(item, ReferenceProduct.FUNDING_RATE, start_ms=start_ms, end_ms=end_ms, limit=32),
            ReferenceRequest(item, ReferenceProduct.OPEN_INTEREST),
            ReferenceRequest(item, ReferenceProduct.OPEN_INTEREST, start_ms=start_ms, end_ms=end_ms, interval="1h", limit=96),
            ReferenceRequest(item, ReferenceProduct.LONG_SHORT_RATIO, start_ms=start_ms, end_ms=end_ms, interval="1h", limit=96, long_short_kind=LongShortKind.GLOBAL_ACCOUNT),
            ReferenceRequest(item, ReferenceProduct.TAKER_FLOW, start_ms=start_ms, end_ms=end_ms, interval="1h", limit=96),
            ReferenceRequest(item, ReferenceProduct.MARK_INDEX_PRICE),
            ReferenceRequest(item, ReferenceProduct.CONTRACT_METADATA),
        ))
    requests.append(ReferenceRequest(
        binance[0], ReferenceProduct.BASIS, start_ms=start_ms, end_ms=end_ms,
        interval="1h", limit=96, basis_series=BasisSeries.CONTINUOUS,
        basis_contract_type="CURRENT_QUARTER",
    ))
    for item in okx:
        requests.extend((
            ReferenceRequest(item, ReferenceProduct.FUNDING_RATE, start_ms=start_ms, end_ms=end_ms, limit=32),
            ReferenceRequest(item, ReferenceProduct.OPEN_INTEREST),
            ReferenceRequest(item, ReferenceProduct.MARK_INDEX_PRICE),
            ReferenceRequest(item, ReferenceProduct.CONTRACT_METADATA),
        ))
    # These three should fail closed without an HTTP request; they demonstrate
    # that the shared batch does not silently substitute a Binance-like value.
    requests.extend((
        ReferenceRequest(okx[0], ReferenceProduct.LONG_SHORT_RATIO, start_ms=start_ms, end_ms=end_ms, interval="1h", long_short_kind=LongShortKind.GLOBAL_ACCOUNT),
        ReferenceRequest(okx[0], ReferenceProduct.TAKER_FLOW, start_ms=start_ms, end_ms=end_ms, interval="1h"),
        ReferenceRequest(okx[0], ReferenceProduct.BASIS, start_ms=start_ms, end_ms=end_ms, interval="1h", basis_contract_type="CURRENT_QUARTER"),
    ))
    return tuple(_summary(result) for result in await batch.fetch(tuple(requests)))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-hours", type=int, default=48)
    args = parser.parse_args()
    if not 1 <= args.window_hours <= 168:
        parser.error("--window-hours must be between 1 and 168")
    summaries = asyncio.run(_run(args.window_hours))
    print(json.dumps({"provenance": "READ_ONLY_PUBLIC_PROVIDER", "results": summaries}, sort_keys=True))
    failures = [item for item in summaries if item["status"] == ReferenceStatus.ERROR.value]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

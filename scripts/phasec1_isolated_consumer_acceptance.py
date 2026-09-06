#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import jwt

from qdl_sdk import (
    AsyncDataLayerClient,
    ControlEvent,
    DataRequirement,
    Feed,
    FileCursorStore,
    GapPolicy,
    Grade,
    GrpcStreamTransport,
    RecoveryPolicy,
    RestQueryTransport,
    StalePolicy,
    StaticBearerCredential,
    WorkloadTlsConfig,
    StreamEvent,
    market_data_view_from_stream,
)


@dataclass(frozen=True)
class VenueCase:
    venue: str
    symbol: str
    instrument_uid: str
    consumer_id: str
    subject: str
    provider: str


CASES = (
    VenueCase(
        "BINANCE",
        "BTCUSDT",
        "a953e16e-7138-5562-b5e8-c337a44d0b65",
        "trading-system.paper.stable",
        "spiffe://qdl/paper/trading-system-stable",
        "BINANCE_DIRECT",
    ),
    VenueCase(
        "BINANCE",
        "ETHUSDT",
        "ee93fabf-68df-5b50-8924-51bf25a5a757",
        "trading-system.paper.stable",
        "spiffe://qdl/paper/trading-system-stable",
        "BINANCE_DIRECT",
    ),
    VenueCase(
        "OKX",
        "BTC-USDT-SWAP",
        "fb26214c-7b9b-5961-95b2-55154755af0f",
        "trading-system.paper.stable",
        "spiffe://qdl/paper/trading-system-stable",
        "OKX_DIRECT",
    ),
    VenueCase(
        "OKX",
        "ETH-USDT-SWAP",
        "e49b54ae-c23d-5351-9e64-47934aac28f8",
        "trading-system.paper.stable",
        "spiffe://qdl/paper/trading-system-stable",
        "OKX_DIRECT",
    ),
)


def token_claims(
    subject: str,
    *,
    issuer: str,
    audience: str,
    manifest_revision: int,
    now: int,
) -> dict[str, object]:
    return {
        "sub": subject,
        "iss": issuer,
        "aud": audience,
        "iat": now,
        "nbf": now - 1,
        "exp": now + 300,
        "jti": str(uuid.uuid4()),
        "environment": "paper",
        "roles": [
            "market_data_reader",
            "historical_reader",
            "stream_consumer",
        ],
        "consumer_manifest_revision": manifest_revision,
    }


def token(
    subject: str,
    *,
    issuer: str,
    audience: str,
    manifest_revision: int,
) -> str:
    private_key_path = Path(os.environ["QDL_STABLE_JWT_PRIVATE_KEY_FILE"])
    key_id = os.environ["QDL_STABLE_JWT_KEY_ID"]
    return jwt.encode(
        token_claims(
            subject,
            issuer=issuer,
            audience=audience,
            manifest_revision=manifest_revision,
            now=int(time.time()),
        ),
        private_key_path.read_bytes(),
        algorithm="RS256",
        headers={"kid": key_id},
    )


def trade_requirement_for_case(case: VenueCase) -> DataRequirement:
    """Match the governed quiet-trade entitlement exactly.

    Trade event recency is observed while provider-session liveness remains
    fail-closed.  These fields belong to the manifest, so the acceptance client
    must not omit or override them.
    """

    return DataRequirement(
        case.instrument_uid,
        Feed.TRADE,
        Grade.EXECUTION,
        "crypto_primary_v2",
        warmup_limit=0,
        max_freshness_ms=3_000,
        event_recency_policy=StalePolicy.OBSERVE,
        max_session_liveness_ms=45_000,
        stale_policy=StalePolicy.BLOCK,
        gap_policy=GapPolicy.BLOCK,
        recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    )


def client(
    *,
    base_url: str,
    grpc_target: str,
    consumer_id: str,
    bearer: str,
    cursor_path: Path,
    tls: WorkloadTlsConfig,
) -> AsyncDataLayerClient:
    credential = StaticBearerCredential(bearer)
    return AsyncDataLayerClient(
        query_transport=RestQueryTransport(
            base_url,
            timeout_seconds=8,
            credential_provider=credential,
            tls=tls,
        ),
        stream_transport=GrpcStreamTransport(
            grpc_target,
            tls=tls,
            credential_provider=credential,
        ),
        consumer_id=consumer_id,
        cursor_store=FileCursorStore(cursor_path),
        max_buffer_events=64,
        max_reconnect_attempts=2,
    )


def _decimal(value) -> Decimal:
    parsed = Decimal(value.source_text)
    coefficient = Decimal(value.coefficient).scaleb(-value.scale)
    if parsed != coefficient:
        raise AssertionError("decimal coefficient/scale differs from source text")
    return parsed


def bar_fingerprint(response) -> list[dict[str, object]]:
    values = []
    previous_open_ns = None
    for item in response.data:
        if item.feed is not Feed.BAR:
            raise AssertionError("warmup returned a non-BAR item")
        if item.payload.lifecycle not in {"FINAL", "REVISED"}:
            raise AssertionError("warmup returned a non-final BAR")
        if (
            item.source.authoritative is not True
            or item.quality.complete is not True
            or item.quality.gap_open is not False
            or item.quality.state not in {"LIVE", "STALE"}
        ):
            raise AssertionError(
                "warmup BAR failed authority/coverage gate: "
                f"source={item.source.model_dump(mode='json')} "
                f"quality={item.quality.model_dump(mode='json')}"
            )
        payload = item.payload
        open_price = _decimal(payload.open)
        high_price = _decimal(payload.high)
        low_price = _decimal(payload.low)
        close_price = _decimal(payload.close)
        volume = _decimal(payload.volume)
        if not (
            low_price <= open_price <= high_price
            and low_price <= close_price <= high_price
            and low_price > 0
            and volume >= 0
        ):
            raise AssertionError("warmup BAR failed OHLCV domain invariants")
        interval_ns = payload.close_time_ns - payload.open_time_ns + 1_000_000
        if interval_ns != 60_000_000_000:
            raise AssertionError("warmup BAR does not span exactly one minute")
        if previous_open_ns is not None:
            if payload.open_time_ns - previous_open_ns != 60_000_000_000:
                raise AssertionError("warmup BAR window is duplicated or gapped")
        previous_open_ns = payload.open_time_ns
        values.append(
            {
                "instrument_uid": item.instrument_uid,
                "instrument_id": item.instrument_id,
                "observed_at_ns": item.observed_at_ns,
                "payload": payload.model_dump(mode="json"),
                "source": item.source.model_dump(mode="json"),
                "contract": item.contract.model_dump(mode="json"),
            }
        )
    if not values:
        raise AssertionError("warmup returned no BAR data")
    if response.count != len(values) or response.coverage != "FULL":
        raise AssertionError("warmup count/coverage contract is inconsistent")
    if response.data_as_of_ns != values[-1]["payload"]["close_time_ns"]:
        raise AssertionError("warmup data_as_of does not match final closed BAR")
    latest_quality = response.data[-1].quality
    if latest_quality.state != "LIVE" or not latest_quality.execution_eligible:
        raise AssertionError(
            "latest closed BAR is not live/execution-eligible: "
            f"{latest_quality.model_dump(mode='json')}"
        )
    if response.watermark_offset < 1 or not response.stream_cursor:
        raise AssertionError("warmup omitted its replay watermark/cursor")
    return values


async def next_data(session) -> tuple[StreamEvent, list[str]]:
    controls: list[str] = []
    for _ in range(8):
        item = await asyncio.wait_for(session.__anext__(), timeout=12)
        if isinstance(item, ControlEvent):
            controls.append(item.code)
            continue
        if not isinstance(item, StreamEvent):
            raise AssertionError("stream returned an unknown SDK event")
        return item, controls
    raise AssertionError("stream did not reach a market-data event")


async def certify_case(
    case: VenueCase,
    *,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    issuer: str,
    audience: str,
    manifest_revision: int,
    state_dir: Path,
    tls: WorkloadTlsConfig,
) -> dict[str, object]:
    bearer = token(
        case.subject,
        issuer=issuer,
        audience=audience,
        manifest_revision=manifest_revision,
    )
    bar_requirement = DataRequirement(
        case.instrument_uid,
        Feed.BAR,
        Grade.EXECUTION,
        "crypto_primary_v2",
        interval="1m",
        warmup_limit=5,
        max_freshness_ms=180_000,
        stale_policy=StalePolicy.BLOCK,
        gap_policy=GapPolicy.BLOCK,
        recovery=RecoveryPolicy.SNAPSHOT_AND_REPLAY,
    )
    primary = client(
        base_url=primary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=(
            state_dir
            / f"{case.venue.lower()}-{case.symbol.lower()}-query-primary.json"
        ),
        tls=tls,
    )
    secondary = client(
        base_url=secondary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=(
            state_dir
            / f"{case.venue.lower()}-{case.symbol.lower()}-query-secondary.json"
        ),
        tls=tls,
    )
    try:
        started = time.perf_counter()
        first = await primary.warmup(bar_requirement)
        primary_warmup_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        second = await secondary.warmup(bar_requirement)
        secondary_warmup_ms = (time.perf_counter() - started) * 1000
    finally:
        await primary.close()
        await secondary.close()
    first_rows = bar_fingerprint(first)
    second_rows = bar_fingerprint(second)
    if first_rows != second_rows:
        raise AssertionError(f"{case.venue} query replicas diverged")

    trade_requirement = trade_requirement_for_case(case)
    cursor_path = state_dir / f"{case.venue.lower()}-{case.symbol.lower()}-stream.json"
    first_client = client(
        base_url=primary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=cursor_path,
        tls=tls,
    )
    try:
        started = time.perf_counter()
        async with first_client.warmup_then_stream(trade_requirement) as session:
            first_event, first_controls = await next_data(session)
            first_stream_ms = (time.perf_counter() - started) * 1000
            first_view = market_data_view_from_stream(
                first_event,
                template=session.warmup.data[-1],
                requirement=trade_requirement,
            )
            if (
                first_view.source.provider != case.provider
                or not first_view.source.authoritative
                or not first_view.quality.complete
                or first_view.quality.gap_open
            ):
                raise AssertionError(f"{case.venue} first stream event failed quality")
            session.acknowledge(first_event)
    finally:
        await first_client.close()

    resumed_client = client(
        base_url=secondary_url,
        grpc_target=grpc_target,
        consumer_id=case.consumer_id,
        bearer=bearer,
        cursor_path=cursor_path,
        tls=tls,
    )
    try:
        started = time.perf_counter()
        async with resumed_client.warmup_then_stream(
            trade_requirement,
            resume_restored_state=True,
        ) as session:
            resumed_event, resumed_controls = await next_data(session)
            resumed_stream_ms = (time.perf_counter() - started) * 1000
            resumed_view = market_data_view_from_stream(
                resumed_event,
                template=session.warmup.data[-1],
                requirement=trade_requirement,
            )
            if resumed_event.logical_offset != first_event.logical_offset + 1:
                raise AssertionError(
                    f"{case.venue} cursor resume was not contiguous: "
                    f"{first_event.logical_offset}->{resumed_event.logical_offset}"
                )
            if resumed_view.source.provider != case.provider:
                raise AssertionError(f"{case.venue} resumed source changed")
            session.acknowledge(resumed_event)
    finally:
        await resumed_client.close()

    persisted = json.loads(cursor_path.read_text(encoding="utf-8"))
    offsets = [
        int(item["offset"])
        for item in persisted["items"].values()
    ]
    if offsets != [resumed_event.logical_offset]:
        raise AssertionError(f"{case.venue} durable cursor was not ACKed")

    return {
        "venue": case.venue,
        "symbol": case.symbol,
        "instrument_uid": case.instrument_uid,
        "provider": case.provider,
        "warmup_rows": len(first_rows),
        "coverage": first.coverage,
        "primary_warmup_ms": round(primary_warmup_ms, 3),
        "secondary_warmup_ms": round(secondary_warmup_ms, 3),
        "first_stream_ms": round(first_stream_ms, 3),
        "resumed_stream_ms": round(resumed_stream_ms, 3),
        "final_bar_close_time_ns": first_rows[-1]["payload"]["close_time_ns"],
        "first_stream_offset": first_event.logical_offset,
        "resumed_stream_offset": resumed_event.logical_offset,
        "first_controls": first_controls,
        "resumed_controls": resumed_controls,
        "cursor_persisted": True,
        "secret_values_recorded": False,
    }


async def run(args: argparse.Namespace) -> dict[str, object]:
    tls = WorkloadTlsConfig(
        args.tls_ca_file,
        args.tls_certificate_file,
        args.tls_private_key_file,
    )
    with tempfile.TemporaryDirectory(prefix="qdl-c1-sdk-") as temporary:
        state_dir = Path(temporary)
        results = []
        for case in CASES:
            results.append(
                await certify_case(
                    case,
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=args.grpc_target,
                    issuer=args.issuer,
                    audience=args.audience,
                    manifest_revision=args.manifest_revision,
                    state_dir=state_dir,
                    tls=tls,
                )
            )
    return {
        "schema": "qdl.phase-c1.isolated-consumer-acceptance.v1",
        "status": "PASS",
        "contract_version": "2.0.0",
        "consumer_manifest_revision": args.manifest_revision,
        "authority_expected": "RUST_SHADOW",
        "cases": results,
        "secret_values_recorded": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-url", default="https://localhost:18201")
    parser.add_argument("--secondary-url", default="https://localhost:18202")
    parser.add_argument("--grpc-target", required=True)
    parser.add_argument(
        "--issuer", default="https://identity.qdl.stable.internal"
    )
    parser.add_argument("--audience", default="qdl-v2-stable")
    parser.add_argument("--manifest-revision", type=int, default=2)
    parser.add_argument("--tls-ca-file", required=True)
    parser.add_argument("--tls-certificate-file", required=True)
    parser.add_argument("--tls-private-key-file", required=True)
    args = parser.parse_args()
    required_env = {
        "QDL_STABLE_JWT_PRIVATE_KEY_FILE",
        "QDL_STABLE_JWT_KEY_ID",
    }
    missing = sorted(required_env - os.environ.keys())
    if missing:
        raise SystemExit(f"required JWT signer environment is missing: {missing}")
    print(json.dumps(asyncio.run(run(args)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

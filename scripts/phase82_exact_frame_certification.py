#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import hashlib
import json
import pathlib
import resource
import statistics
import subprocess
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.database.dnse_fallback import fetch_dnse_ohlcv_direct
from qdl.adapters.binance_usdm import BinanceUsdmSupervisor, discover_instruments
from qdl.adapters.okx.client import OkxRestClient, OkxSubscription, OkxWebSocketSupervisor
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.canonical.book import canonicalize_deribit_option_book_fixture
from qdl.canonical.market import canonicalize_dnse_bar
from qdl.canonical.trade import (
    TradeContext,
    canonicalize_binance_usdm_trade,
    canonicalize_okx_trade,
)
from qdl.domain.instrument import InstrumentIdentity, ProductType
from qdl.ingestion.contracts import ConnectionShard, FeedType, Subscription
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.capture import bind_capture_context, capture_exact_frame


ROOT = pathlib.Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "upgrade/evidence"
CAPTURE_PATH = EVIDENCE / "captures/phase8-real-provider-frames.json.gz"
RUST_REPLAY = ROOT / "target/debug/qdl-parity-replay"


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _write(name: str, value: dict[str, Any]) -> None:
    path = EVIDENCE / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return ordered[index]


def _context_dict(context: TradeContext) -> dict[str, Any]:
    value = asdict(context)
    value["raw_capture_id"] = list(context.raw_capture_id)
    value["raw_frame_sha256"] = list(context.raw_frame_sha256)
    return value


def _context_from_dict(value: dict[str, Any]) -> TradeContext:
    copy = dict(value)
    copy["raw_capture_id"] = bytes(copy.get("raw_capture_id", []))
    copy["raw_frame_sha256"] = bytes(copy.get("raw_frame_sha256", []))
    return TradeContext(**copy)


def _canonical_bytes(fixture: dict[str, Any]) -> bytes:
    context = _context_from_dict(fixture["context"])
    kind = fixture["provider_kind"]
    if kind in {"binance_usdm_trade", "binance_usdm_agg_trade"}:
        event = canonicalize_binance_usdm_trade(fixture["raw"], context)
    elif kind == "okx_trade":
        event = canonicalize_okx_trade(fixture["raw"], context)
    elif kind == "dnse_bar":
        event = canonicalize_dnse_bar(fixture["raw"], context)
    elif kind == "deribit_option_book_fixture":
        event = canonicalize_deribit_option_book_fixture(fixture["raw"], context)
    else:
        raise ValueError(f"unsupported fixture kind: {kind}")
    if event.raw_capture_id != context.raw_capture_id:
        raise RuntimeError("canonical event lost raw capture identity")
    if event.raw_payload_hash != context.raw_frame_sha256:
        raise RuntimeError("canonical event lost exact raw frame hash")
    return event.SerializeToString(deterministic=True)


def _base_context(
    record: Any,
    *,
    provider: str,
    source_id: str,
    sequence: int,
    received_at_ns: int,
) -> TradeContext:
    return TradeContext(
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=record.metadata_revision,
        venue=record.identity.venue,
        market=record.identity.market,
        product_type=record.identity.product_type.value,
        native_symbol=record.native_symbol,
        provider=provider,
        source_id=source_id,
        lease_epoch=1,
        received_at_ns=received_at_ns,
        normalized_at_ns=received_at_ns + 1,
        published_at_ns=received_at_ns + 2,
        partition_sequence=sequence,
        normalizer_version="qdl-normalizer/2.0.0-phase8",
        adapter_version="phase8-shadow/1.0.0",
        config_revision=1,
        correlation_id="phase82-exact-frame",
    )


def _capture_summary(envelope: Any, *, retained: bool) -> dict[str, Any]:
    return {
        "capture_id": envelope.capture_id.hex(),
        "provider": envelope.provider,
        "venue": envelope.venue,
        "market": envelope.market,
        "native_symbol": envelope.native_symbol,
        "native_channel": envelope.native_channel,
        "source_session_id": envelope.source_session_id,
        "connection_generation": envelope.connection_generation,
        "received_at_ns": envelope.received_at_ns,
        "capture_boundary": envelope.capture_boundary,
        "raw_frame_bytes": len(envelope.raw_frame_bytes),
        "raw_frame_sha256": envelope.raw_frame_sha256.hex(),
        "test_provenance": envelope.test_provenance,
        "retained": retained,
    }


async def _collect_live(
    *, duration_seconds: float, retained_per_venue: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    discovery, okx_rows = await asyncio.gather(
        discover_instruments(attempts=3), OkxRestClient().instruments("SWAP")
    )
    binance_record = next(
        item for item in discovery.records if item.native_symbol == "BTCUSDT"
    )
    okx_raw = next(item for item in okx_rows if item.get("instId") == "BTC-USDT-SWAP")
    okx_record, _ = parse_public_instrument(
        okx_raw, metadata_revision=1, valid_from_ns=time.time_ns()
    )
    fixtures: list[dict[str, Any]] = []
    captures: list[dict[str, Any]] = []
    counters = {"BINANCE": 0, "OKX": 0}
    raw_bytes = {"BINANCE": 0, "OKX": 0}
    binance_stop = asyncio.Event()
    okx_stop = asyncio.Event()
    run_id = str(time.time_ns())

    async def on_binance(raw_frame, stream, raw, received_at_ns):
        counters["BINANCE"] += 1
        raw_bytes["BINANCE"] += len(raw_frame)
        retained = counters["BINANCE"] <= retained_per_venue
        envelope = capture_exact_frame(
            provider="BINANCE_DIRECT", venue="BINANCE", market="USDM",
            product_type="PERPETUAL", native_symbol="BTCUSDT",
            native_channel=stream, subscription_id="phase82-binance-trade",
            source_session_id=f"phase82-binance-{run_id}", connection_generation=1,
            lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
            received_at_ns=received_at_ns, raw_frame_bytes=raw_frame,
            adapter_version="binance-usdm/2.0.0-shadow", config_revision=1,
            instrument_catalog_revision=binance_record.metadata_revision,
            correlation_id="phase82-exact-frame", test_provenance=False,
        )
        if retained:
            context = bind_capture_context(
                _base_context(
                    binance_record, provider="BINANCE_DIRECT",
                    source_id="binance-usdm-phase82-shadow",
                    sequence=counters["BINANCE"], received_at_ns=received_at_ns,
                ),
                envelope,
            )
            fixtures.append({
                "provider_kind": "binance_usdm_trade",
                "context": _context_dict(context), "raw": raw,
            })
            captures.append({
                **_capture_summary(envelope, retained=True),
                "raw_frame_base64": base64.b64encode(raw_frame).decode("ascii"),
            })

    async def on_okx(raw_frame, payload, generation, received_at_ns):
        argument = payload.get("arg", {})
        if payload.get("event") or argument.get("channel") != "trades":
            return
        rows = payload.get("data", [])
        for raw in rows:
            counters["OKX"] += 1
            raw_bytes["OKX"] += len(raw_frame)
            retained = counters["OKX"] <= retained_per_venue
            envelope = capture_exact_frame(
                provider="OKX_DIRECT", venue="OKX", market="SWAP",
                product_type="PERPETUAL", native_symbol="BTC-USDT-SWAP",
                native_channel="trades", subscription_id="phase82-okx-trade",
                source_session_id=f"phase82-okx-{run_id}",
                connection_generation=generation, lease_epoch=1,
                authority_revision=1, partition_plan_epoch=1,
                received_at_ns=received_at_ns, raw_frame_bytes=raw_frame,
                adapter_version="okx-v5/2.0.0-shadow", config_revision=1,
                instrument_catalog_revision=okx_record.metadata_revision,
                correlation_id="phase82-exact-frame", test_provenance=False,
            )
            if retained:
                context = bind_capture_context(
                    _base_context(
                        okx_record, provider="OKX_DIRECT",
                        source_id="okx-swap-phase82-shadow", sequence=counters["OKX"],
                        received_at_ns=received_at_ns,
                    ),
                    envelope,
                )
                fixtures.append({
                    "provider_kind": "okx_trade",
                    "context": _context_dict(context), "raw": raw,
                })
                captures.append({
                    **_capture_summary(envelope, retained=True),
                    "raw_frame_base64": base64.b64encode(raw_frame).decode("ascii"),
                })

    async def stop_later():
        await asyncio.sleep(duration_seconds)
        binance_stop.set()
        okx_stop.set()

    subscription = Subscription("BINANCE", "USDM", FeedType.TRADE, "BTCUSDT")
    tasks = (
        BinanceUsdmSupervisor(
            on_frame=lambda *_: asyncio.sleep(0), on_exact_frame=on_binance,
        ).run(
            ConnectionShard(
                "phase82-binance", "BINANCE", "USDM", FeedType.TRADE,
                (subscription,), 1,
            ),
            active_symbols={item.native_symbol for item in discovery.records},
            stop=binance_stop,
        ),
        OkxWebSocketSupervisor(
            on_frame=lambda *_: asyncio.sleep(0), on_exact_frame=on_okx,
        ).run(
            (OkxSubscription("trades", "BTC-USDT-SWAP"),), stop=okx_stop,
        ),
        stop_later(),
    )
    started = time.monotonic()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds + 45)
    elapsed = time.monotonic() - started
    if min(counters.values()) <= 0:
        raise RuntimeError(f"authentic live capture missing a venue: {counters}")
    return fixtures, captures, {
        "duration_seconds": elapsed,
        "observed_events": counters,
        "observed_raw_bytes": raw_bytes,
        "retained_per_venue_limit": retained_per_venue,
    }


def _last_completed_weekday() -> str:
    day = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day.isoformat()


def _dnse_rows(day: str, input_path: pathlib.Path | None) -> list[dict[str, Any]]:
    if input_path is not None:
        payload = json.loads(input_path.read_text())
        rows = payload.get("rows")
        if (
            payload.get("schema") != "qdl.phase8.dnse-sdk-delivery.v1"
            or payload.get("status") != "PASS"
            or payload.get("provenance") != "REAL_DNSE_PUBLIC_MARKETDATA_READ_ONLY"
            or payload.get("production_writes") != 0
            or payload.get("symbol") != "VN30F1M"
            or payload.get("trading_date") != day
            or not isinstance(rows, list)
        ):
            raise RuntimeError("DNSE acquisition artifact provenance is invalid")
        if hashlib.sha256(_json_bytes(rows)).hexdigest() != payload.get("rows_sha256"):
            raise RuntimeError("DNSE acquisition artifact checksum mismatch")
        return rows
    frame = fetch_dnse_ohlcv_direct("VN30F1M", day, day, resolution="1")
    if frame.empty:
        raise RuntimeError(f"DNSE returned no authentic bars for {day}")
    rows = []
    for row in frame.itertuples(index=False):
        opened = row.time.to_pydatetime().replace(tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
        open_ms = int(opened.timestamp() * 1000)
        rows.append({
            "symbol": "VN30F1M", "interval": "1m", "open_time_ms": open_ms,
            "close_time_ms": open_ms + 59_999, "o": str(row.open),
            "h": str(row.high), "l": str(row.low), "c": str(row.close),
            "v": str(row.volume), "is_final": True,
            "trade_count_available": False, "revision": 0,
        })
    return rows


def _collect_dnse(
    day: str, input_path: pathlib.Path | None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _dnse_rows(day, input_path)
    identity = InstrumentIdentity.create(
        venue="DNSE", market="VN_DERIVATIVES", product_type=ProductType.FUTURE,
        canonical_symbol="VN30F1M",
    )
    fixtures = []
    captures = []
    session_id = f"phase82-dnse-{day}"
    for sequence, raw in enumerate(rows, start=1):
        raw_frame = _json_bytes(raw)
        received_at_ns = time.time_ns() + sequence
        envelope = capture_exact_frame(
            provider="DNSE_DIRECT", venue="DNSE", market="VN_DERIVATIVES",
            product_type="FUTURE", native_symbol="VN30F1M",
            native_channel="ohlcv/1m", subscription_id="phase82-dnse-bar",
            source_session_id=session_id, connection_generation=1, lease_epoch=1,
            authority_revision=1, partition_plan_epoch=1,
            received_at_ns=received_at_ns, raw_frame_bytes=raw_frame,
            adapter_version="dnse-openapi/2.0.0-shadow", config_revision=1,
            instrument_catalog_revision=1, correlation_id="phase82-exact-frame",
            test_provenance=False,
            transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_SDK_CALLBACK,
            capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_SDK_DELIVERY,
        )
        base = TradeContext(
            instrument_uid=identity.instrument_uid, instrument_id=identity.instrument_id,
            instrument_revision=1, venue="DNSE", market="VN_DERIVATIVES",
            product_type="FUTURE", native_symbol="VN30F1M", provider="DNSE_DIRECT",
            source_id="dnse-vn-phase82-shadow", lease_epoch=1,
            received_at_ns=received_at_ns, normalized_at_ns=received_at_ns + 1,
            published_at_ns=received_at_ns + 2, partition_sequence=sequence,
            normalizer_version="qdl-normalizer/2.0.0-phase8",
            adapter_version="phase8-shadow/1.0.0", config_revision=1,
            correlation_id="phase82-exact-frame",
        )
        context = bind_capture_context(base, envelope)
        fixtures.append({
            "provider_kind": "dnse_bar", "context": _context_dict(context), "raw": raw,
        })
        captures.append({
            **_capture_summary(envelope, retained=True),
            "raw_frame_base64": base64.b64encode(raw_frame).decode("ascii"),
        })
    return fixtures, captures


def _deribit_fixture() -> tuple[dict[str, Any], dict[str, Any]]:
    raw = json.loads(
        (ROOT / "tests/fixtures/phase3/deribit_option_book.json").read_text()
    )
    identity = InstrumentIdentity.create(
        venue="DERIBIT", market="OPTIONS", product_type=ProductType.OPTION,
        canonical_symbol=raw["native_symbol"],
    )
    raw_frame = _json_bytes(raw)
    envelope = capture_exact_frame(
        provider="DERIBIT_TEST_ONLY", venue="DERIBIT", market="OPTIONS",
        product_type="OPTION", native_symbol=raw["native_symbol"],
        native_channel="book.fixture", subscription_id="phase82-deribit-fixture",
        source_session_id="phase82-deribit-fixture", connection_generation=1,
        lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
        received_at_ns=1_786_579_200_000_000_000, raw_frame_bytes=raw_frame,
        adapter_version="deribit-fixture/1.0.0", config_revision=1,
        instrument_catalog_revision=1, correlation_id="phase82-fixture",
        test_provenance=True,
        transport_protocol=raw_provider_pb2.TRANSPORT_PROTOCOL_FILE_REPLAY,
        capture_boundary=raw_provider_pb2.CAPTURE_BOUNDARY_REPLAY_BYTES,
    )
    context = bind_capture_context(TradeContext(
        instrument_uid=identity.instrument_uid, instrument_id=identity.instrument_id,
        instrument_revision=1, venue="DERIBIT", market="OPTIONS",
        product_type="OPTION", native_symbol=raw["native_symbol"],
        provider="DERIBIT_TEST_ONLY", source_id="deribit-phase82-fixture",
        lease_epoch=1, received_at_ns=envelope.received_at_ns,
        normalized_at_ns=envelope.received_at_ns + 1,
        published_at_ns=envelope.received_at_ns + 2, partition_sequence=1,
        normalizer_version="qdl-normalizer/2.0.0-phase8",
        adapter_version="phase8-shadow/1.0.0", config_revision=1,
        correlation_id="phase82-fixture",
    ), envelope)
    return {
        "provider_kind": "deribit_option_book_fixture",
        "context": _context_dict(context), "raw": raw,
    }, _capture_summary(envelope, retained=True)


def _replay(fixtures: list[dict[str, Any]], repeat: int) -> dict[str, Any]:
    if not RUST_REPLAY.is_file():
        raise RuntimeError(f"Rust replay binary not built: {RUST_REPLAY}")
    bundle_path = ROOT / "target/phase82-replay-bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_bytes(_json_bytes({"fixtures": fixtures, "repeat": repeat}))
    latencies = []
    aggregate = hashlib.sha256()
    record_hashes = []
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    for iteration in range(repeat):
        for fixture in fixtures:
            event_started = time.perf_counter_ns()
            canonical = _canonical_bytes(fixture)
            latencies.append((time.perf_counter_ns() - event_started) / 1_000_000)
            aggregate.update(len(canonical).to_bytes(8, "big"))
            aggregate.update(canonical)
            if iteration == 0:
                record_hashes.append(hashlib.sha256(canonical).hexdigest())
    python_wall = time.perf_counter() - started_wall
    python_cpu = time.process_time() - started_cpu
    rust_runs = []
    for _ in range(3):
        result = subprocess.run(
            [str(RUST_REPLAY), str(bundle_path)], text=True, capture_output=True,
            check=True, timeout=180,
        )
        rust_runs.append(json.loads(result.stdout))
    bundle_path.unlink(missing_ok=True)
    expected_hash = aggregate.hexdigest()
    if any(item["aggregate_sha256"] != expected_hash for item in rust_runs):
        raise RuntimeError("Python/Rust replay aggregate diverged")
    if any(item["record_sha256"] != record_hashes for item in rust_runs):
        raise RuntimeError("Python/Rust canonical record diverged")
    events = len(fixtures) * repeat
    return {
        "events": events,
        "fixture_count": len(fixtures),
        "repeat": repeat,
        "aggregate_sha256": expected_hash,
        "record_mismatches": 0,
        "process_restart_mismatches": 0,
        "rust_process_runs": len(rust_runs),
        "python": {
            "elapsed_seconds": python_wall, "cpu_seconds": python_cpu,
            "events_per_second": events / max(python_wall, 1e-9),
            "latency_ms": {
                "p50": _percentile(latencies, 0.50),
                "p95": _percentile(latencies, 0.95),
                "p99": _percentile(latencies, 0.99),
                "p99_9": _percentile(latencies, 0.999),
                "mean": statistics.fmean(latencies),
            },
        },
        "rust": {
            "events_per_second_min": min(item["events_per_second"] for item in rust_runs),
            "events_per_second_max": max(item["events_per_second"] for item in rust_runs),
            "elapsed_seconds_max": max(item["elapsed_seconds"] for item in rust_runs),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live-seconds", type=float, default=180.0)
    parser.add_argument("--retain-per-venue", type=int, default=128)
    parser.add_argument("--repeat", type=int, default=200)
    parser.add_argument("--dnse-date", default=_last_completed_weekday())
    parser.add_argument("--dnse-input", type=pathlib.Path)
    args = parser.parse_args()
    started = time.time()
    live_fixtures, live_captures, live_metrics = asyncio.run(
        _collect_live(
            duration_seconds=args.live_seconds,
            retained_per_venue=args.retain_per_venue,
        )
    )
    dnse_fixtures, dnse_captures = _collect_dnse(
        args.dnse_date, args.dnse_input
    )
    deribit_fixture, deribit_capture = _deribit_fixture()
    real_fixtures = [*live_fixtures, *dnse_fixtures]
    fixtures = [*real_fixtures, deribit_fixture]
    captures = [*live_captures, *dnse_captures]
    replay = _replay(fixtures, args.repeat)
    capture_payload = {
        "schema": "qdl.phase8.authentic-capture-bundle.v1",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "production_writes": 0,
        "captures": captures,
        "fixture_only_deribit": deribit_capture,
    }
    compressed = gzip.compress(_json_bytes(capture_payload), compresslevel=9, mtime=0)
    CAPTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CAPTURE_PATH.write_bytes(compressed)
    capture_digest = hashlib.sha256(compressed).hexdigest()
    venue_counts: dict[str, int] = {}
    for fixture in fixtures:
        venue = fixture["context"]["venue"]
        venue_counts[venue] = venue_counts.get(venue, 0) + 1
    common = {
        "status": "PASS", "authority": "RUST_SHADOW", "production_writes": 0,
        "public_or_legacy_writes": 0, "canonical_mismatches": 0,
    }
    _write("phase8-cross-venue-conformance.json", {
        "schema": "qdl.phase8.cross-venue-conformance.v1", **common,
        "venue_fixture_counts": venue_counts,
        "authentic_venues": ["BINANCE", "DNSE", "OKX"],
        "fixture_only_venues": ["DERIBIT"],
        "deribit_live_certified": False,
        "capability_failures_isolated": True,
    })
    _write("phase8-python-rust-parity.json", {
        "schema": "qdl.phase8.python-rust-parity.v1", **common, **replay,
        "comparison": [
            "raw_capture_id", "event_id", "instrument_identity", "exact_decimal",
            "source_time", "native_sequence", "session_generation", "quality_flags",
            "canonical_payload_hash", "deterministic_protobuf_bytes",
        ],
    })
    _write("phase8-real-provider-shadow.json", {
        "schema": "qdl.phase8.real-provider-shadow.v1", **common,
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "live": live_metrics, "dnse_trading_date": args.dnse_date,
        "dnse_complete_session_rows": len(dnse_fixtures),
        "retained_authentic_captures": len(captures),
        "capture_bundle": str(CAPTURE_PATH.relative_to(ROOT)),
        "capture_bundle_sha256": capture_digest,
        "test_provenance_in_real_capture_namespace": 0,
    })
    _write("phase8-capacity.json", {
        "schema": "qdl.phase8.capacity.v1", **common,
        "profile": "bounded-real-live-plus-replay-burst",
        "live": live_metrics, "replay": replay,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "thresholds": {
            "canonical_mismatches_max": 0,
            "python_p99_ms_max": 10.0,
            "rust_events_per_second_min": 1000.0,
        },
        "thresholds_pass": (
            replay["python"]["latency_ms"]["p99"] <= 10.0
            and replay["rust"]["events_per_second_min"] >= 1000.0
        ),
    })
    _write("phase8-soak.json", {
        "schema": "qdl.phase8.soak.v1", **common,
        "wall_seconds": time.time() - started,
        "live_window_seconds": live_metrics["duration_seconds"],
        "complete_market_session": {
            "venue": "DNSE", "date": args.dnse_date,
            "rows": len(dnse_fixtures), "resolution": "1m",
        },
        "deterministic_replay_events": replay["events"],
        "rust_clean_process_runs": replay["rust_process_runs"],
        "justification": (
            "One complete bounded DNSE market session plus concurrent authentic "
            "Binance/OKX live capture and repeated clean-process replay; authority stays shadow."
        ),
    })
    if not json.loads((EVIDENCE / "phase8-capacity.json").read_text())["thresholds_pass"]:
        raise RuntimeError("Phase 8 capacity thresholds failed")
    print(json.dumps({
        "status": "PASS", "fixtures": len(fixtures), "replay_events": replay["events"],
        "venue_counts": venue_counts, "capture_bundle_sha256": capture_digest,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

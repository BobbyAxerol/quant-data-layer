from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import tempfile
import time
from collections import Counter
from pathlib import Path

from qdl.adapters.binance_usdm import BinanceUsdmSupervisor, discover_instruments, fetch_klines
from qdl.adapters.okx.client import OkxOrderBook, OkxSubscription, OkxWebSocketSupervisor
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.adapters.okx.client import OkxRestClient
from qdl.canonical.book import canonicalize_okx_book
from qdl.canonical.market import (
    canonicalize_binance_usdm_bar,
    canonicalize_binance_usdm_bbo,
    canonicalize_binance_usdm_rest_bar,
)
from qdl.canonical.trade import (
    TradeContext,
    canonical_event,
    canonicalize_binance_usdm_trade,
    canonicalize_okx_trade,
    raw_market_event,
)
from qdl.ingestion.contracts import ConnectionShard, FeedType, Subscription
from qdl.transport.sqlite_spool import SQLiteDurableSpool, SpoolConfig


def context(record, *, source_id: str, sequence: int, received_at_ns: int) -> TradeContext:
    return TradeContext(
        instrument_uid=record.instrument_uid,
        instrument_id=record.instrument_id,
        instrument_revision=record.metadata_revision,
        venue=record.identity.venue,
        market=record.identity.market,
        product_type=record.identity.product_type.value,
        native_symbol=record.native_symbol,
        provider="BINANCE_DIRECT" if record.identity.venue == "BINANCE" else "OKX_DIRECT",
        source_id=source_id,
        lease_epoch=1,
        received_at_ns=received_at_ns,
        normalized_at_ns=time.time_ns(),
        published_at_ns=time.time_ns(),
        partition_sequence=sequence,
        normalizer_version="qdl-normalizer/2.0.0-phase3",
        adapter_version="phase3-real-shadow/1.0.0",
        config_revision=1,
        correlation_id="phase3-real-provider-smoke",
    )


async def run(output: Path, *, timeout_seconds: float) -> dict:
    with tempfile.TemporaryDirectory(prefix="qdl-phase3-real-") as directory:
        spool = SQLiteDurableSpool(SpoolConfig(
            path=Path(directory) / "real-provider.sqlite3",
            max_records=5000,
            max_payload_bytes=32 * 1024 * 1024,
            max_event_bytes=2 * 1024 * 1024,
            min_free_disk_bytes=0,
        ))
        counts: Counter[str] = Counter()
        source_times: dict[str, int] = {}
        raw_hashes: dict[str, str] = {}

        def commit(raw: dict, feed: str, ctx: TradeContext, envelope) -> None:
            raw_event = raw_market_event(raw, context=ctx, feed_type=feed, accepted_at_ns=ctx.received_at_ns)
            raw_result = spool.append(raw_event)
            canonical_result = spool.append(canonical_event(
                envelope, accepted_at_ns=ctx.normalized_at_ns, raw_event=raw_event
            ))
            if raw_result.duplicate or canonical_result.duplicate:
                raise RuntimeError("bounded provider smoke unexpectedly received duplicate event IDs")
            counts[f"{ctx.venue.lower()}_{feed}"] += 1
            source_times[f"{ctx.venue.lower()}_{feed}"] = envelope.source_event_time_ns
            raw_hashes[f"{ctx.venue.lower()}_{feed}"] = hashlib.sha256(raw_event.payload).hexdigest()

        discovery = await discover_instruments(attempts=3)
        binance_record = next(item for item in discovery.records if item.native_symbol == "BTCUSDT")
        binance_sequence = 0

        async def on_binance(stream: str, raw: dict, received_at_ns: int) -> None:
            nonlocal binance_sequence
            binance_sequence += 1
            ctx = context(
                binance_record,
                source_id="binance-usdm-phase3-real-smoke",
                sequence=binance_sequence,
                received_at_ns=received_at_ns,
            )
            if "@trade" in stream or "@aggTrade" in stream:
                if counts["binance_trade"] == 0:
                    commit(raw, "trade", ctx, canonicalize_binance_usdm_trade(raw, ctx))
            elif "@bookTicker" in stream:
                if counts["binance_quote"] == 0:
                    commit(raw, "quote", ctx, canonicalize_binance_usdm_bbo(raw, ctx))
            elif "@kline_1m" in stream:
                if counts["binance_bar"] == 0:
                    commit(raw, "bar", ctx, canonicalize_binance_usdm_bar(raw, ctx))
            if all(counts[name] > 0 for name in ("binance_trade", "binance_quote")):
                binance_stop.set()
        subscriptions = (
            Subscription("BINANCE", "USDM", FeedType.TRADE, "BTCUSDT"),
            Subscription("BINANCE", "USDM", FeedType.BBO, "BTCUSDT"),
        )
        active_symbols = {item.native_symbol for item in discovery.records}
        binance_stop = asyncio.Event()
        try:
            await asyncio.wait_for(
                BinanceUsdmSupervisor(on_frame=on_binance).run(
                    ConnectionShard(
                        "binance-usdm-phase3-real-smoke", "BINANCE", "USDM",
                        FeedType.TRADE, subscriptions, 1,
                    ),
                    active_symbols=active_symbols,
                    stop=binance_stop,
                ),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            missing = [name for name in ("binance_trade", "binance_quote") if not counts[name]]
            raise RuntimeError(f"Binance real smoke timed out feeds={missing}") from exc

        rows = await fetch_klines("BTCUSDT", "1m", limit=3)
        now_ms = int(time.time() * 1000)
        closed = [row for row in rows if int(row[6]) < now_ms]
        if not closed:
            raise RuntimeError("Binance REST returned no closed 1m bar")
        binance_sequence += 1
        bar_context = context(
            binance_record, source_id="binance-usdm-bar-rest-phase3-real-smoke",
            sequence=binance_sequence, received_at_ns=time.time_ns(),
        )
        raw_bar = {"symbol": "BTCUSDT", "interval": "1m", "row": closed[-1]}
        commit(
            raw_bar, "bar", bar_context,
            canonicalize_binance_usdm_rest_bar(raw_bar, bar_context),
        )

        okx_rest = OkxRestClient()
        instrument_rows = await okx_rest.instruments("SWAP")
        raw_instrument = next(item for item in instrument_rows if item.get("instId") == "BTC-USDT-SWAP")
        okx_record, _ = parse_public_instrument(
            raw_instrument, metadata_revision=1, valid_from_ns=time.time_ns()
        )
        okx_book = OkxOrderBook(okx_record.native_symbol)
        okx_stop = asyncio.Event()
        okx_sequence = 0
        last_generation = 0

        async def on_okx(frame: dict, generation: int) -> None:
            nonlocal okx_sequence, last_generation
            if generation != last_generation:
                okx_book.reconnect(generation)
                last_generation = generation
            channel = frame.get("arg", {}).get("channel")
            if channel == "trades":
                for raw in frame.get("data", []):
                    okx_sequence += 1
                    ctx = context(
                        okx_record, source_id="okx-swap-phase3-real-smoke",
                        sequence=okx_sequence, received_at_ns=time.time_ns(),
                    )
                    commit(raw, "trade", ctx, canonicalize_okx_trade(raw, ctx))
            elif channel == "books" and okx_book.apply_ws(frame, generation=generation):
                okx_sequence += 1
                ctx = context(
                    okx_record, source_id="okx-swap-phase3-real-smoke",
                    sequence=okx_sequence, received_at_ns=time.time_ns(),
                )
                feed = "book_snapshot" if frame.get("action") == "snapshot" else "book_delta"
                commit(frame, feed, ctx, canonicalize_okx_book(frame, ctx))
            if counts["okx_trade"] > 0 and (
                counts["okx_book_snapshot"] > 0 or counts["okx_book_delta"] > 0
            ):
                okx_stop.set()

        await asyncio.wait_for(
            OkxWebSocketSupervisor(on_frame=on_okx).run(
                (
                    OkxSubscription("trades", "BTC-USDT-SWAP"),
                    OkxSubscription("books", "BTC-USDT-SWAP"),
                ),
                stop=okx_stop,
                max_events=200,
            ),
            timeout=timeout_seconds,
        )
        stats = spool.stats()
        spool.close()

    required = {"binance_trade", "binance_quote", "binance_bar", "okx_trade"}
    if not required.issubset({name for name, value in counts.items() if value > 0}):
        raise RuntimeError(f"real-provider smoke missing required feeds: {required - set(counts)}")
    if not (counts["okx_book_snapshot"] or counts["okx_book_delta"]):
        raise RuntimeError("real-provider smoke did not establish an OKX WS book")
    result = {
        "schema": "qdl.phase3.real-provider-smoke.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "production_writes": 0,
        "counts": dict(sorted(counts.items())),
        "latest_source_time_ns": source_times,
        "raw_sha256": raw_hashes,
        "transport": {
            "binance_trade": "WEBSOCKET_INDIVIDUAL_TRADE",
            "binance_quote": "WEBSOCKET_BOOK_TICKER",
            "binance_bar": "REST_CLOSED_KLINE",
            "okx_trade": "PUBLIC_WEBSOCKET",
            "okx_book": "PUBLIC_WEBSOCKET_SNAPSHOT_DELTA",
        },
        "durable_records": stats.records,
        "durable_payload_bytes": stats.payload_bytes,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.output, timeout_seconds=args.timeout_seconds)), sort_keys=True))


if __name__ == "__main__":
    main()

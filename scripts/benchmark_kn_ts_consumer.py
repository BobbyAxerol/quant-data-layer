#!/usr/bin/env python3
"""Real TS adapter/bridge and Redis-writer benchmark on isolated KN only.

Run in the existing TS image, with sealed /binding and KN query/stream targets.
No orders/DB/production Redis. The fixed kn5-astra-ts-cache DNS name is required.
Records milliseconds after actual Redis ACK and readback, not callback entry.
Inherited KN4 runner logic; semaphore wait is included, refusals stay separate.
"""
import asyncio, json, os, random, sys, time, traceback


def _dist(values):
    values = sorted(values)
    if not values:
        return {"n": 0}
    pick = lambda q: round(values[min(len(values) - 1, int(q * len(values)))], 1)  # noqa: E731
    return {"n": len(values), "p50": pick(0.5), "p95": pick(0.95) if len(values) >= 20 else None,
            "p99": pick(0.99) if len(values) >= 100 else None, "max": round(values[-1], 1)}


def _error(error):
    out = {"type": type(error).__name__, "code": getattr(error, "code", None), "detail": str(error)[:160]}
    diagnostics = getattr(error, "diagnostics", None)
    if isinstance(diagnostics, dict):
        out["diagnostics"] = diagnostics
    cause = error.__cause__ or error.__context__
    if cause is not None:
        out["cause"] = {"type": type(cause).__name__, "code": getattr(cause, "code", None),
                        "detail": str(cause)[:160]}
    return out


INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
               "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
               "1d": 86_400_000}


def _event_ms(value, interval=None):
    """(event ms, basis) of a consumer view: a bar's close, else its event time."""
    stamp = getattr(value, "timestamp_ms", None)
    if stamp is not None and interval:
        return int(stamp) + INTERVAL_MS.get(interval, 0), "bar_close"
    if stamp is not None:
        return int(stamp), "timestamp_ms"
    observed = getattr(value, "observed_at_ms", None)
    if observed is not None:
        return int(observed), "observed_at_ms"
    return None, None


def _market(venue):
    return ("binance", "futures") if venue == "BINANCE" else ("okx", "swap")


async def ts60(results):
    sys.path.insert(0, "/app")
    from adapters.market_data.data_layer_v2 import Feed, build_versioned_data_layer_client
    from shared.config import settings

    os.makedirs(settings.DATA_LAYER_V2_CURSOR_DIR, exist_ok=True)
    import adapters.market_data.data_layer_v2 as ts_adapter

    original_validate = ts_adapter._validate_identity

    def validate_recording(view, *args, **kwargs):
        # The TS client's own acceptance rule, unchanged; a refusal keeps the
        # quality of the exact view it refused (no second read).
        try:
            return original_validate(view, *args, **kwargs)
        except Exception as error:
            quality = getattr(view, "quality", None)
            error.kn4_view_quality = quality.model_dump(mode="json") if hasattr(quality, "model_dump") else None
            error.kn4_view_source = (view.source.model_dump(mode="json")
                                     if hasattr(getattr(view, "source", None), "model_dump") else None)
            raise

    ts_adapter._validate_identity = validate_recording
    binding = json.load(open("/binding/trading-system.paper.stable.json"))
    products = binding["products"]
    rounds = int(os.environ.get("KN4_TS60_ROUNDS", "5"))
    good = settings.DATA_LAYER_V2_QUERY_URL
    phase_a = []
    for replica in ("query_v2_1", "query_v2_2"):
        settings.DATA_LAYER_V2_QUERY_URL = f"https://{replica}:8200"
        client = build_versioned_data_layer_client(settings, consumer_id="trading-system.paper.stable",
                                                   mode_override="V2_PRIMARY")

        async def read(product, called):
            provider, market = _market(product["venue"])
            symbol, feed = product["native_symbol"], Feed(product["feed"])
            selection = client.route_selection(provider, symbol, feed=feed.value, interval=product["interval"],
                                               market=market)
            route = client._route_requirement(selection)
            started = time.monotonic()
            timing = lambda: {"queue_wait_ms": round((started - called) * 1000, 2),  # noqa: E731
                              "sdk_call_ms": round((time.monotonic() - started) * 1000, 2),
                              "call_to_usable_ms": round((time.monotonic() - called) * 1000, 2)}
            try:
                if feed is Feed.TRADE:
                    value = await client.v2.latest_market(provider, symbol, market=market, route_requirement=route)
                elif feed is Feed.BAR:
                    value = await client.v2.latest_bar(provider, symbol, interval=product["interval"], market=market,
                                                       route_requirement=route)
                else:
                    value = await client.v2.latest_execution_feed(provider, symbol, feed=feed, market=market,
                                                                  route_requirement=route)
                received_ms = time.time_ns() // 1_000_000
                event_ms, basis = _event_ms(value, product["interval"])
                quality = getattr(value, "quality", None) or {}
                return {"product": f"{product['venue']}|{symbol}|{product['feed']}|{product['interval'] or '-'}",
                        "replica": replica, "ok": True, **timing(),
                        "age_ms": None if event_ms is None else received_ms - event_ms, "age_basis": basis,
                        "server_freshness_ms": quality.get("freshness_ms") if isinstance(quality, dict) else None}
            except Exception as error:  # noqa: BLE001 - the typed outcome is the evidence
                return {"product": f"{product['venue']}|{symbol}|{product['feed']}|{product['interval'] or '-'}",
                        "replica": replica, "ok": False, **timing(), "error": _error(error),
                        "rejected_view_quality": getattr(error, "kn4_view_quality", None),
                        "rejected_view_source": getattr(error, "kn4_view_source", None)}

        # Bounded like a consumer process (the hot snapshot lane holds 15
        # pending TS reads per replica and refuses more, typed RATE_LIMITED):
        # at most KN4_TS60_CONCURRENCY in flight; queue wait counts in `ms`.
        gate = asyncio.Semaphore(int(os.environ.get("KN4_TS60_CONCURRENCY", "8")))

        async def bounded(product):
            called = time.monotonic()  # the consumer's own queue counts too
            async with gate:
                return await read(product, called)

        for _ in range(rounds):
            phase_a.extend(await asyncio.gather(*(bounded(product) for product in products)))
            await asyncio.sleep(float(os.environ.get("KN4_TS60_ROUND_PAUSE_S", "3.0")))
        await client.close()
    settings.DATA_LAYER_V2_QUERY_URL = good
    by_product = {}
    for row in phase_a:
        entry = by_product.setdefault(row["product"], {"ok": 0, "failed": 0, "replicas_ok": set(), "errors": []})
        if row["ok"]:
            entry["ok"] += 1
            entry["replicas_ok"].add(row["replica"])
        else:
            entry["failed"] += 1
            if len(entry["errors"]) < 3:
                entry["errors"].append({"replica": row["replica"], **row["error"]})
    feeds = {}
    for row in phase_a:
        feed = row["product"].split("|")[2]
        group = feeds.setdefault(feed, {"queue": [], "sdk": [], "total": [], "age_ms": [], "failed": 0, "reads": 0})
        group["reads"] += 1
        if row["ok"]:
            group["queue"].append(row["queue_wait_ms"])
            group["sdk"].append(row["sdk_call_ms"])
            group["total"].append(row["call_to_usable_ms"])
            if row.get("age_ms") is not None:
                group["age_ms"].append(row["age_ms"])
        else:
            group["failed"] += 1
    per_binding = {}
    for row in phase_a:
        cell = per_binding.setdefault(row["product"], {}).setdefault(
            row["replica"], {"ok": 0, "failed": 0, "reasons": {}, "rejected_view_quality": [], "sdk_ms": [], "total_ms": [], "ages_ms": []})
        if row["ok"]:
            cell["ok"] += 1
            cell["sdk_ms"].append(row["sdk_call_ms"])
            cell["total_ms"].append(row["call_to_usable_ms"])
            if row["age_ms"] is not None:
                cell["ages_ms"].append(row["age_ms"])
        else:
            cell["failed"] += 1
            reason = row["error"].get("code") or row["error"]["detail"][:80]
            cell["reasons"][reason] = cell["reasons"].get(reason, 0) + 1
            if row.get("rejected_view_quality") and len(cell["rejected_view_quality"]) < 3:
                cell["rejected_view_quality"].append(row["rejected_view_quality"])
    for cells in per_binding.values():
        for cell in cells.values():
            for metric in ("sdk_ms", "total_ms", "ages_ms"):
                cell[metric] = _dist(cell[metric])
    results.append({"consumer": "ts60", "phase": "A_snapshots_per_replica", "rounds": rounds,
                    "concurrency_per_replica": int(os.environ.get("KN4_TS60_CONCURRENCY", "8")),
                    "reads": len(phase_a), "failed": sum(1 for row in phase_a if not row["ok"]),
                    "products": len(by_product),
                    "products_ok_on_both_replicas": sum(1 for e in by_product.values()
                                                        if e["replicas_ok"] == {"query_v2_1", "query_v2_2"}),
                    "timing_basis": "call_to_usable = consumer queue (semaphore) + SDK call incl. server queue "
                                    "and client validation; all from the call, per read",
                    "by_feed": {feed: {"reads": g["reads"], "failed": g["failed"],
                                       "queue_wait_ms": _dist(g["queue"]), "sdk_call_ms": _dist(g["sdk"]),
                                       "call_to_usable_ms": _dist(g["total"]),
                                       "age_at_receipt_ms": _dist(g["age_ms"])} for feed, g in sorted(feeds.items())},
                    "per_binding_replica": per_binding,
                    "products_without_a_positive_read_on_both_replicas": sorted(
                        product for product, cells in per_binding.items()
                        if not all(cells.get(r, {}).get("ok") for r in ("query_v2_1", "query_v2_2"))),
                    "failures": {k: {"ok": v["ok"], "failed": v["failed"], "errors": v["errors"]}
                                 for k, v in sorted(by_product.items()) if v["failed"]}})

    # B starts in a fresh quota minute: phase A's reads count against the same
    # 1,500/min TS manifest quota, and a mixed minute would measure the runner.
    await asyncio.sleep(float(os.environ.get("KN4_TS60_PAUSE_S", "65")))

    # B: the real TS bridge, recording projector.
    from adapters.market_data.instrument_loader import SymbolConfig
    from services.market_data.data_layer_bridge import DataLayerMarketDataBridge

    seconds = float(sys.argv[2]) if len(sys.argv) > 2 else 120.0
    receipts = {}
    bridge_started_ms = [0]
    warm_after_ms = 30_000  # receipts in the first 30 s are the start-up handoff, kept apart

    from redis.asyncio import Redis
    from services.market_data.cache_projector import MarketCacheProjector
    redis_client = Redis.from_url("redis://kn5-astra-ts-cache:6379/0", decode_responses=False)
    await redis_client.ping()
    writer_samples = []
    writer_count = 0
    verified_keys = 0
    write_lock = asyncio.Lock()

    class VerifiedProjector(MarketCacheProjector):
        async def _write_to(self, redis_target, operations):
            nonlocal writer_count, verified_keys
            async with write_lock:
                started = time.monotonic()
                await super()._write_to(redis_target, operations)
                ack_ms = (time.monotonic() - started) * 1000
                expected = {op[1]: op[3].encode() for op in operations if op[0] == "setex"}
                if expected:
                    actual = await redis_target.mget(list(expected))
                    if actual != list(expected.values()):
                        raise RuntimeError("TS Redis readback differs from written projection")
                sample = {"ack_ms": ack_ms, "verified_ms": (time.monotonic() - started) * 1000}
                writer_count += 1
                verified_keys += len(expected)
                if len(writer_samples) < 4096:
                    writer_samples.append(sample)
                else:
                    slot = random.randrange(writer_count)
                    if slot < len(writer_samples):
                        writer_samples[slot] = sample

    actual_projector = VerifiedProjector(redis_client)

    class Recorder:
        def _note(self, kind, items):
            now_ms = time.time_ns() // 1_000_000
            for item in items:
                identity = getattr(item, "canonical_identity", None)
                venue = getattr(item, "venue", None) or getattr(identity, "venue", "?")
                symbol = (getattr(item, "venue_symbol", None) or getattr(item, "symbol", None)
                          or getattr(identity, "venue_symbol", None) or "?")
                feed = getattr(item, "feed", None) or kind
                interval = getattr(item, "interval", None)
                event_ms, _basis = _event_ms(item, interval)
                phase = "steady" if now_ms - bridge_started_ms[0] >= warm_after_ms else "startup"
                key = f"{str(venue).upper()}|{symbol}|{feed}|{phase}"
                entry = receipts.setdefault(key, {"count": 0, "aged": 0, "ages": [], "first_ms": now_ms})
                entry["count"] += 1
                if event_ms is not None:
                    # Uniform reservoir (Algorithm R): every receipt of the run
                    # has the same chance to be in the 512 samples.
                    age = now_ms - event_ms
                    entry["aged"] += 1
                    if len(entry["ages"]) < 512:
                        entry["ages"].append(age)
                    else:
                        slot = random.randrange(entry["aged"])
                        if slot < 512:
                            entry["ages"][slot] = age

        async def project_trade(self, item):
            await actual_projector.project_trade(item)
            self._note("TRADE", (item,))
        async def project_trades(self, items):
            await actual_projector.project_trades(items)
            self._note("TRADE", items)
        async def project_quote(self, item):
            await actual_projector.project_quote(item)
            self._note("QUOTE", (item,))
        async def project_quotes(self, items):
            await actual_projector.project_quotes(items)
            self._note("QUOTE", items)
        async def project_bar(self, item):
            await actual_projector.project_bar(item)
            self._note("BAR", (item,))
        async def project_bars(self, items):
            await actual_projector.project_bars(items)
            self._note("BAR", items)
        async def project_execution_feeds(self, items):
            await actual_projector.project_execution_feeds(items)
            self._note("EXEC", items)

    symbols = SymbolConfig(
        binance_symbols=sorted({p["native_symbol"] for p in products if p["venue"] == "BINANCE"}),
        vn_symbols=[],
        okx_symbols=sorted({p["native_symbol"] for p in products if p["venue"] == "OKX"}))
    client = build_versioned_data_layer_client(settings, consumer_id="trading-system.paper.stable",
                                               mode_override="V2_PRIMARY")
    bridge = DataLayerMarketDataBridge(
        data_layer_client=client, data_layer_redis=None, trading_redis=None, symbols=symbols,
        v2_batch_max_items=settings.DATA_LAYER_V2_STREAM_BATCH_MAX_ITEMS,
        v2_batch_max_wait_ms=settings.DATA_LAYER_V2_STREAM_BATCH_MAX_WAIT_MS,
        v2_trade_stale_seconds=settings.DATA_LAYER_V2_TRADE_MAX_FRESHNESS_MS / 1000.0,
        v2_quote_stale_seconds=settings.DATA_LAYER_V2_QUOTE_MAX_FRESHNESS_MS / 1000.0,
        v2_mark_stale_seconds=settings.DATA_LAYER_V2_MARK_MAX_FRESHNESS_MS / 1000.0,
        v2_book_stale_seconds=settings.DATA_LAYER_V2_BOOK_MAX_FRESHNESS_MS / 1000.0,
        v2_book_snapshot_stale_seconds=settings.DATA_LAYER_V2_BOOK_SNAPSHOT_MAX_FRESHNESS_MS / 1000.0,
        v2_mark_refresh_seconds=settings.DATA_LAYER_V2_MARK_REFRESH_SECONDS,
        v2_book_snapshot_refresh_seconds=settings.DATA_LAYER_V2_BOOK_SNAPSHOT_REFRESH_SECONDS,
        v2_bar_stale_seconds=settings.DATA_LAYER_V2_BAR_MAX_FRESHNESS_MS / 1000.0,
        v2_session_stale_seconds=settings.DATA_LAYER_V2_MARKET_MAX_SESSION_LIVENESS_MS / 1000.0)
    bridge.projector = Recorder()
    bridge_started_ms[0] = time.time_ns() // 1_000_000
    runner = asyncio.create_task(bridge.run())
    samples = []
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        await asyncio.sleep(5.0)
        report = bridge.health_snapshot()
        details = report.details
        samples.append({"t_s": round(time.monotonic() - started, 1), "status": report.status,
                        "demanded": details.get("demanded_v2_slices"), "ready": details.get("ready_v2_slices"),
                        "unhealthy": (details.get("reported_unhealthy_slices") or [])[:10]})
    bridge.stop()
    runner.cancel()
    await asyncio.gather(runner, return_exceptions=True)
    await client.close()
    await redis_client.aclose()
    results.append({"phase": "actual_redis_write", "writes": writer_count,
                    "ack_ms": _dist([s["ack_ms"] for s in writer_samples]),
                    "verified_ms": _dist([s["verified_ms"] for s in writer_samples]),
                    "verified_keys": verified_keys})
    warm = [s for s in samples if s["t_s"] >= 30.0]
    by_feed = {}
    for key, entry in receipts.items():
        _venue, _symbol, feed, phase = key.split("|")
        group = by_feed.setdefault(f"{feed}|{phase}", {"slices": 0, "count": 0, "ages": []})
        group["slices"] += 1
        group["count"] += entry["count"]
        group["ages"].extend(entry["ages"])
    results.append({"consumer": "ts60", "phase": "B_bridge_health", "seconds": seconds,
                    "samples": samples, "warm_samples": len(warm),
                    "warm_all_ready": sum(1 for s in warm if s["demanded"] and s["ready"] == s["demanded"]),
                    "receipts_by_feed": {f: {"slices": g["slices"], "views": g["count"],
                                             "age_at_redis_verified_ms": _dist(g["ages"])}
                                         for f, g in sorted(by_feed.items())},
                    "age_basis": "event/close time -> actual TS projector Redis ACK plus readback; "
                                 "uniform reservoir of 512 per slice over the whole run; a feed aggregate weights its slices equally",
                    "slices_with_receipts": len({key.rsplit("|", 1)[0] for key in receipts})})



def main():
    results = []
    try:
        asyncio.run(ts60(results))
    except Exception as error:
        results.append({"status": "SETUP_ERROR", "outcome": _error(error),
                        "trace": traceback.format_exc()[-1500:]})
        print(json.dumps({"schema": "qdl.kn5.ts-consumer-latency.v1", "results": results, "order_actions": 0}, default=str))
        return 1
    print(json.dumps({"schema": "qdl.kn5.ts-consumer-latency.v1", "results": results, "order_actions": 0}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

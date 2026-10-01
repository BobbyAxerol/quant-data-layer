"""Read-only probe, run inside a TS consumer with its existing identity.

No order, stream, ACK or durable cursor mutation. Evidence excludes credentials.
"""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import time


def error_evidence(error):
    return {"error_type": type(error).__name__,
            "error_code": str(getattr(error, "code", "")),
            "diagnostics": getattr(error, "diagnostics", None)}


def classify_trade_observation(view, provider_trade_id):
    """Compare exact native IDs only; caller must select evidence before request."""
    if provider_trade_id is None:
        return "PROVIDER_EVIDENCE_MISSING"
    served = view.get("payload", {}).get("native_trade_id")
    if served is None:
        return "VIEW_IDENTITY_MISSING"
    if str(served) == str(provider_trade_id):
        return "MATCHED_PROVIDER_LAST_TRADE"
    try:
        return ("PIPELINE_BEHIND_PROVIDER" if int(served) < int(provider_trade_id)
                else "OBSERVER_BEHIND_PIPELINE")
    except (ValueError, TypeError):
        return "INCOMPARABLE_TRADE_IDS"


async def provider_capture(stop):
    import aiohttp
    symbols = ("BTC", "ETH", "SOL", "DOGE", "BNB")

    async def venue_capture(venue, route):
        url = ("wss://ws.okx.com:8443/ws/v5/public" if venue == "OKX" else
               f"wss://fstream.binance.com/{route}/ws")
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=15, timeout=10) as ws:
                if venue == "OKX":
                    await ws.send_json({"op": "subscribe", "args": [
                        {"channel": channel, "instId": f"{symbol}-USDT" + suffix}
                        for symbol in symbols for channel, suffix in
                        (("trades", "-SWAP"), ("mark-price", "-SWAP"), ("index-tickers", ""))]})
                else:
                    channel = "trade" if route == "public" else "markPrice@1s"
                    await ws.send_json({"method": "SUBSCRIBE", "id": 1,
                                        "params": [f"{s.lower()}usdt@{channel}" for s in symbols]})
                print(json.dumps({"kind": "provider_connected", "venue": venue,
                                  "received_ns": time.time_ns()}), flush=True)
                captured = 0
                while not stop.is_set():
                    try:
                        message = await ws.receive(timeout=2)
                    except asyncio.TimeoutError:
                        if venue == "OKX":
                            await ws.send_str("ping")
                        continue
                    if message.type != aiohttp.WSMsgType.TEXT:
                        if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            raise RuntimeError("provider websocket closed")
                        continue
                    if message.data == "pong":
                        continue
                    payload = json.loads(message.data)
                    if venue == "BINANCE" and "e" in payload:
                        payload = {"data": payload}
                    if "data" not in payload:
                        if payload.get("event") == "error":
                            raise RuntimeError("provider subscription rejected")
                        continue
                    captured += 1
                    if captured > 25000:
                        raise RuntimeError("bounded provider evidence limit reached")
                    print(json.dumps({"kind": "provider", "venue": venue,
                                      "received_ns": time.time_ns(), "data": payload}), flush=True)

    sources = (("BINANCE", "public"), ("BINANCE", "market"), ("OKX", "public"))
    tasks = [asyncio.create_task(venue_capture(v, route)) for v, route in sources]
    try:
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for (venue, route), result in zip(sources, results):
            if isinstance(result, Exception):
                print(json.dumps({"kind": "provider_error", "venue": venue,
                                  "error_type": type(result).__name__}), flush=True)


async def main():
    from core.config import settings
    from adapters.market_data.data_layer_v2 import (
        build_versioned_data_layer_client, execution_feed_from_v2,
        execution_mark_index_from_reference,
    )
    from qdl_sdk import Feed, Grade, ReferenceProduct, ReferenceRequirement
    binding = json.loads(Path(settings.DATA_LAYER_V2_CONSUMER_BINDING_FILE).read_text())
    rounds = int(os.getenv("QDL_DIAGNOSTIC_ROUNDS", "2"))
    if not 1 <= rounds <= 12:
        raise ValueError("bounded rounds must be 1..12")
    products = [p for p in binding["products"]
                if p["feed"] in ("TRADE", "MARK_INDEX_PRICE")]
    with tempfile.TemporaryDirectory(prefix="qdl-view-diagnostics-") as tmp:
        clients = []
        resolved = {}
        stop = asyncio.Event()
        capture = (asyncio.create_task(provider_capture(stop))
                   if os.getenv("QDL_DIAGNOSTIC_PROVIDER_WS") == "1" else None)
        try:
            if capture is not None:
                await asyncio.sleep(3)
            for replica in (1, 2):
                config = settings.model_copy(update={
                    "DATA_LAYER_V2_QUERY_URL": f"https://query_v2_{replica}:8200",
                    "DATA_LAYER_V2_CURSOR_DIR": tmp + f"/{replica}/cursor",
                    "DATA_LAYER_V2_AUDIT_DIR": tmp + f"/{replica}/audit",
                })
                client = build_versioned_data_layer_client(
                    config, consumer_id=binding["consumer_id"], mode_override="V2_PRIMARY")
                clients.append(client)
            for cycle in range(rounds):
                for product in products:
                    for replica, client in enumerate(clients, 1):
                        feed = Feed(product["feed"])
                        key = (replica, product["instrument_uid"], feed.value)
                        if key not in resolved:
                            instrument = await client.v2.resolve(
                                product["venue"], product["native_symbol"], market=product["market"])
                            selection = client.route_selection(product["venue"], product["native_symbol"],
                                feed=feed.value, interval=product.get("interval"), market=product["market"])
                            requirement = client.v2.requirement(instrument, feed,
                                route_requirement=client._route_requirement(selection))
                            resolved[key] = instrument, requirement
                        instrument, requirement = resolved[key]
                        paths = ("snapshot", "execution_reference") if feed is Feed.MARK_INDEX_PRICE else ("snapshot",)
                        for path in paths:
                            row = {k: product.get(k) for k in ("venue", "market", "native_symbol", "feed", "instrument_uid")}
                            row.update(replica=replica, path=path, cycle=cycle,
                                       consumer=binding["consumer_id"], request_ns=time.time_ns())
                            start = time.perf_counter()
                            try:
                                if path == "snapshot":
                                    response = await client.v2.client.snapshot(requirement)
                                    view = response.data
                                    row["view"] = view.model_dump(mode="json", exclude={"cursor"})
                                    execution_feed_from_v2(view, instrument)
                                else:
                                    response = await client.v2.client.reference_batch((ReferenceRequirement(
                                        instrument_uid=instrument.instrument_uid,
                                        product=ReferenceProduct.MARK_INDEX_PRICE,
                                        consumer_grade=Grade.EXECUTION,
                                        source_policy_id=requirement.source_policy_id,
                                        limit=1, page_size=1, max_pages=1,
                                        max_freshness_ms=requirement.max_freshness_ms,
                                        event_recency_policy=requirement.event_recency_policy,
                                        max_session_liveness_ms=requirement.max_session_liveness_ms,
                                        require_full_coverage=True,
                                    ),), require_all=False)
                                    row["reference"] = response.model_dump(mode="json")
                                    execution_mark_index_from_reference(response, instrument,
                                        source_policy_id=requirement.source_policy_id,
                                        max_freshness_ms=requirement.max_freshness_ms or client.v2.config.mark_max_freshness_ms,
                                        release_manifest_sha256=client.v2.config.release_manifest_sha256,
                                        consumer_manifest_revision=client.v2.config.jwt_manifest_revision)
                                row["usable"] = True
                            except Exception as error:
                                row.update(usable=False, **error_evidence(error))
                                if str(getattr(error, "code", "")) == "RATE_LIMITED":
                                    print(json.dumps(row, default=str), flush=True)
                                    raise RuntimeError("stop probe: preserve shared identity quota") from error
                            row["call_to_outcome_ms"] = (time.perf_counter() - start) * 1000
                            row["completed_ns"] = time.time_ns()
                            print(json.dumps(row, default=str), flush=True)
                            await asyncio.sleep(0.75)
                if cycle + 1 < rounds:
                    await asyncio.sleep(1)
        finally:
            for client in clients:
                await client.v2.client.close()
            stop.set()
            if capture is not None:
                await capture


if __name__ == "__main__":
    asyncio.run(main())

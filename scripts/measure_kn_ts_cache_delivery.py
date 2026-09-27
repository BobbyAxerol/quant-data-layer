#!/usr/bin/env python3
"""Read-only, bounded external observer of actual TS cache availability."""
from __future__ import annotations
import asyncio
from collections import Counter, defaultdict
import json
import math
import os
import time


def describe(channel, payload):
    parts = channel.split(".")
    family = parts[3]
    feed = payload.get("feed", family.upper())
    kind = "execution_" + feed.lower() if family == "execution" else "last_" + family
    interval = payload.get("interval") if family == "bar" else None
    key = "cache:market:v2:" + kind + ":" + payload["canonical_instrument_id"]
    if interval:
        key += ":" + interval
    raw = payload.get("raw", {}).get("v2", {})
    event_ms = payload.get("observed_at_ms", payload.get("timestamp_ms"))
    if family == "bar":
        # TS60 declares BAR1m only; never invent a calendar interval duration.
        if interval != "1m":
            raise ValueError("undeclared BAR interval in TS60 observer")
        event_ms += 60_000
    return key, feed, event_ms, payload.get("watermark_offset", raw.get("watermark_offset"))


def summary(values):
    x = sorted(values)
    def q(p):
        return round(x[math.ceil(len(x) * p) - 1], 3) if x else None
    return {"n": len(x), "p50_ms": q(.5), "p95_ms": q(.95) if len(x) >= 20 else None,
            "p99_ms": q(.99) if len(x) >= 100 else None, "max_ms": q(1)}


async def main():
    import redis.asyncio as redis
    seconds = int(os.environ.get("KN_OBSERVE_SECONDS", "300"))
    if not 1 <= seconds <= 600:
        raise ValueError("bounded observation requires1..600 seconds")
    client = redis.from_url(os.environ["KN_TS_REDIS_URL"], socket_timeout=5,
                            socket_connect_timeout=5, max_connections=3)
    pub = client.pubsub()
    await pub.psubscribe("events.market.v2.*")
    samples, reads, counts, errors, eligible = defaultdict(list), defaultdict(list), Counter(), Counter(), Counter()
    last = {}
    started = time.monotonic()
    try:
        while time.monotonic() - started < seconds:
            item = await pub.get_message(ignore_subscribe_messages=True, timeout=.5)
            if not item:
                continue
            channel = item["channel"].decode()
            now = time.monotonic()
            counts[channel] += 1
            if now - last.get(channel, 0) < .5:
                continue
            last[channel] = now
            try:
                payload = json.loads(item["data"])
                key, feed, event_ms, watermark = describe(channel, payload)
                group = payload["venue"] + "/" + payload["venue_symbol"] + "/" + feed
                t0 = time.perf_counter_ns()
                current_raw = await client.get(key)
                t1 = time.perf_counter_ns()
                if current_raw is None:
                    errors["CACHE_MISSING:" + group] += 1
                    continue
                current = json.loads(current_raw)
                if current["canonical_instrument_id"] != payload["canonical_instrument_id"]:
                    raise ValueError("identity changed at cache readback")
                current_watermark = current.get("watermark_offset", current.get("raw", {}).get("v2", {}).get("watermark_offset"))
                if watermark is not None and (current_watermark is None or current_watermark < watermark):
                    errors["WATERMARK_REGRESSION:" + group] += 1
                    continue
                age = None if event_ms is None else time.time_ns() / 1e6 - event_ms
                if age is not None and age < 0:
                    errors["FUTURE_TIMESTAMP:" + group] += 1
                    continue
                if len(reads[group]) < 2000:
                    reads[group].append((t1 - t0) / 1e6)
                    if age is not None:
                        samples[group].append(age)
                quality = payload.get("quality", payload.get("raw", {}).get("quality", {}))
                eligible[group + ":" + str(quality.get("execution_eligible"))] += 1
            except (KeyError, ValueError, TypeError) as error:
                errors[type(error).__name__] += 1
    finally:
        await pub.aclose()
        await client.aclose()
    print(json.dumps({"schema": "qdl.kn5.actual-ts-cache-availability.v1", "seconds": time.monotonic() - started,
        "boundary": "source event (BAR close) -> external GET after actual TS Redis SET/PUBLISH; not publisher ACK",
        "sampling": "at most2Hz per channel; max2000 samples per product; no source event dedup",
        "event_to_cache_readable": {k: summary(v) for k,v in samples.items()},
        "cache_get": {k: summary(v) for k,v in reads.items()}, "observed_messages": dict(counts),
        "eligibility": dict(eligible), "errors": dict(errors), "writes": 0, "order_actions": 0}))


if __name__ == "__main__":
    asyncio.run(main())

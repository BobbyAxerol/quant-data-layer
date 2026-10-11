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


def compare_observation(channel, payload, current, now_ms):
    """Only identical source observations may supply matched delivery timing."""
    _, feed, event_ms, watermark = describe(channel, payload)
    _, current_feed, current_ms, current_watermark = describe(channel, current)
    if (current.get("canonical_instrument_id") != payload.get("canonical_instrument_id")
            or current_feed != feed or current.get("interval") != payload.get("interval")):
        raise ValueError("IDENTITY_MISMATCH")
    for stamp in (event_ms, current_ms):
        if type(stamp) is not int or stamp <= 0 or stamp > now_ms:
            raise ValueError("INVALID_OR_FUTURE_TIMESTAMP")
    def reference_pair(value):
        observations = value.get("contract", {}).get("reference_observations")
        if not isinstance(observations, list) or len(observations) != 1:
            raise ValueError("REFERENCE_PAIR_MISSING")
        labels = observations[0].get("labels", {})
        pair = []
        for name in ("mark", "index"):
            capture = labels.get(f"component_{name}_capture_id")
            if (not isinstance(capture, str) or len(capture) != 32
                    or any(c not in "0123456789abcdef" for c in capture)):
                raise ValueError("REFERENCE_PAIR_MISSING")
            times = []
            for field in ("source_event_time_ns", "received_at_ns"):
                raw = labels.get(f"component_{name}_{field}")
                if type(raw) not in (str, int) or not str(raw).isascii() or not str(raw).isdecimal():
                    raise ValueError("REFERENCE_PAIR_MISSING")
                stamp = int(raw)
                if stamp <= 0 or stamp / 1e6 > now_ms:
                    raise ValueError("INVALID_OR_FUTURE_TIMESTAMP")
                times.append(stamp)
            price = value.get("payload", {}).get(f"{name}_price")
            if price is None:
                raise ValueError("REFERENCE_PAIR_MISSING")
            pair.append((capture, *times, price))
        generation = labels.get("connection_generation")
        if type(generation) not in (str, int) or not str(generation).isascii() or not str(generation).isdecimal() or int(generation) <= 0:
            raise ValueError("REFERENCE_PAIR_MISSING")
        return (int(generation), pair)
    if feed == "MARK_INDEX_PRICE":
        previous_pair, current_pair = reference_pair(payload), reference_pair(current)
    elif any(type(offset) is not int or offset < 0 for offset in (watermark, current_watermark)):
        raise ValueError("WATERMARK_MISSING")
    def context(value):
        raw = value.get("raw", {}).get("v2", {})
        contract = value.get("contract", raw.get("contract", {}))
        body = value.get("payload", raw.get("payload", {}))
        return (contract.get("config_revision"), contract.get("instrument_catalog_revision"),
                contract.get("authority_revision"), contract.get("source_policy_revision"),
                value.get("source", raw.get("source")), value.get("metadata_version"),
                body.get("book_generation"))
    if context(current) != context(payload):
        match = "CONTEXT_CHANGED"
    elif feed == "MARK_INDEX_PRICE":
        # A paired reference has no scalar cursor. Never infer ordering across
        # independently advancing components or manufacture a watermark.
        if previous_pair != current_pair:
            match = "PAIR_CHANGED_AT_READ"
        elif current_ms != event_ms:
            raise ValueError("EVENT_IDENTITY_MISMATCH")
        else:
            match = "EXACT"
    elif current_watermark < watermark:
        raise ValueError("WATERMARK_REGRESSION")
    elif current_watermark != watermark:
        match = "SUPERSEDED_AT_READ"
    elif current_ms != event_ms:
        raise ValueError("EVENT_IDENTITY_MISMATCH")
    else:
        match = "EXACT"
    return {"match": match, "notification_age_ms": now_ms - event_ms,
            "current_cache_age_ms": now_ms - current_ms,
            "watermark": watermark, "current_watermark": current_watermark}


async def observe(client, pub, seconds, *, period=.5, max_channels=128, max_bytes=1048576, max_pending_bytes=8388608):
    # Draining never awaits Redis GET. One bounded MGET samples the latest
    # notification per channel; this is observer sampling, not feed coalescing.
    pending, counts, errors = {}, Counter(), Counter()
    pending_bytes = peak_pending_bytes = 0
    message_sizes = {}
    if min(max_bytes, max_pending_bytes, max_channels) <= 0:
        raise ValueError("observer bounds must be positive")
    samples, current_ages, notification_ages, residence = (defaultdict(list) for _ in range(4))
    matches, eligible = Counter(), Counter()
    admitted_groups = set()
    batches = []
    started = time.monotonic()
    deadline = started + seconds
    async def drain():
        nonlocal pending_bytes, peak_pending_bytes
        received = 0
        while time.monotonic() < deadline:
            item = await pub.get_message(ignore_subscribe_messages=True, timeout=.25)
            if not item:
                await asyncio.sleep(0)
                continue
            channel = item["channel"].decode()
            if channel not in counts and len(counts) >= max_channels:
                errors["CHANNEL_LIMIT"] += 1
                continue
            counts[channel] += 1
            size = len(item["data"])
            stats = message_sizes.setdefault(channel, {"n": 0, "max_bytes": 0, "over_256k": 0, "rejected": 0})
            stats["n"] += 1
            stats["max_bytes"] = max(stats["max_bytes"], size)
            stats["over_256k"] += int(size > 262144)
            if size > max_bytes:
                errors["MESSAGE_SIZE_LIMIT"] += 1
                stats["rejected"] += 1
                continue
            previous_size = len(pending[channel][0]) if channel in pending else 0
            next_bytes = pending_bytes - previous_size + size
            if next_bytes > max_pending_bytes:
                errors["PENDING_BYTES_LIMIT"] += 1
                stats["rejected"] += 1
                continue
            pending_bytes = next_bytes
            peak_pending_bytes = max(peak_pending_bytes, pending_bytes)
            pending[channel] = (item["data"], time.monotonic())
            received += 1
            if received % 128 == 0:
                await asyncio.sleep(0)
    task = asyncio.create_task(drain())
    try:
        while time.monotonic() < deadline:
            await asyncio.sleep(min(period, max(0, deadline - time.monotonic())))
            if task.done():
                await task
            selected = list(pending.items())
            pending.clear()
            pending_bytes = 0
            decoded = []
            for channel, (raw, received) in selected:
                try:
                    payload = json.loads(raw)
                    key, feed, _, _ = describe(channel, payload)
                    group = payload["venue"] + "/" + payload["venue_symbol"] + "/" + feed + "/" + channel.split(".")[3]
                    if group not in admitted_groups and len(admitted_groups) >= max_channels:
                        errors["GROUP_LIMIT"] += 1
                        continue
                    admitted_groups.add(group)
                    decoded.append((channel, payload, key, group, received))
                except (KeyError, TypeError, ValueError, IndexError):
                    errors["INVALID_NOTIFICATION"] += 1
            if not decoded:
                continue
            t0 = time.perf_counter_ns()
            values = await client.mget([item[2] for item in decoded])
            elapsed = (time.perf_counter_ns() - t0) / 1e6
            if len(values) != len(decoded):
                raise ValueError("MGET_LENGTH_MISMATCH")
            if len(batches) < 2000:
                batches.append(elapsed)
            now_ms, observed = time.time_ns() / 1e6, time.monotonic()
            for (channel, payload, _, group, received), value in zip(decoded, values):
                if value is None:
                    errors["CACHE_MISSING:" + group] += 1
                    continue
                if len(value) > max_bytes:
                    errors["CACHE_SIZE_LIMIT"] += 1
                    continue
                try:
                    current = json.loads(value)
                    result = compare_observation(channel, payload, current, now_ms)
                    matches[group + ":" + result["match"]] += 1
                    if len(current_ages[group]) < 2000:
                        current_ages[group].append(result["current_cache_age_ms"])
                        notification_ages[group].append(result["notification_age_ms"])
                        residence[group].append((observed - received) * 1000)
                        if result["match"] == "EXACT":
                            samples[group].append(result["notification_age_ms"])
                    quality = current.get("quality", current.get("raw", {}).get("quality", {}))
                    eligible[group + ":" + str(quality.get("execution_eligible"))] += 1
                except (KeyError, TypeError, ValueError) as error:
                    code = str(error) if str(error) in {
                        "IDENTITY_MISMATCH", "INVALID_OR_FUTURE_TIMESTAMP", "WATERMARK_MISSING",
                        "WATERMARK_REGRESSION", "EVENT_IDENTITY_MISMATCH", "REFERENCE_PAIR_MISSING"} else "INVALID_CACHE_VIEW"
                    errors[code] += 1
    except Exception as error:
        errors["OBSERVER_FAILED:" + type(error).__name__] += 1
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as error:
            errors.setdefault("OBSERVER_FAILED:" + type(error).__name__, 1)
    if not current_ages:
        errors["NO_VALID_CACHE_OBSERVATIONS"] += 1
    return {"schema": "qdl.kn5.actual-ts-cache-availability.v2",
        "seconds": time.monotonic() - started,
        "status": "INCOMPLETE" if errors else "OBSERVED",
        "boundary": "source event/BAR close -> MGET of identical source observation (watermark or MARK pair); availability upper bound, NOT Redis SET ACK",
        "replica_attribution": "TS Redis only; no Query replica attribution",
        "sampling": {"mode": "latest notification per channel; time samples, not unique events; execution/last paths separate",
                     "period_seconds": period, "max_channels": max_channels,
                     "max_notification_bytes": max_bytes, "max_pending_bytes": max_pending_bytes,
                     "peak_pending_bytes": peak_pending_bytes, "max_samples_per_group": 2000},
        "event_to_cache_readable": {k: summary(v) for k,v in samples.items()},
        "current_cache_age_ms": {k: summary(v) for k,v in current_ages.items()},
        "notification_age_ms": {k: summary(v) for k,v in notification_ages.items()},
        "observer_residence_ms": {k: summary(v) for k,v in residence.items()},
        "cache_mget_batch_ms": summary(batches), "observed_messages": dict(counts),
        "message_sizes": message_sizes, "matches": dict(matches), "eligibility_at_write_not_revalidated": dict(eligible),
        "errors": dict(errors), "writes": 0, "order_actions": 0,
        "redis_set_ack_latency": "NOT_MEASURED", "canonical_commit_latency": "NOT_MEASURED"}


async def main():
    import redis.asyncio as redis
    seconds = int(os.environ.get("KN_OBSERVE_SECONDS", "300"))
    if not 1 <= seconds <= 600:
        raise ValueError("bounded observation requires1..600 seconds")
    client = redis.from_url(os.environ["KN_TS_REDIS_URL"], socket_timeout=5,
                           socket_connect_timeout=5, max_connections=3)
    pub = client.pubsub()
    try:
        await pub.psubscribe("events.market.v2.*")
        result = await observe(client, pub, seconds)
    finally:
        await pub.aclose()
        await client.aclose()
    print(json.dumps(result))
    return 0 if result["status"] == "OBSERVED" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

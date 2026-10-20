#!/usr/bin/env python3
"""IDR-129: isolated Query replica under repeated warmup bursts, for retained-RSS measurement.

Purpose: find what keeps a Query replica's RSS after the TS writer's warmup burst (production query_kn_1 reached
2045 MiB of 2 GiB over 32 h, one step per burst) and prove a fix, where the product runs: the Query container of
the production image over HTTPS mTLS + JWT, driven by the SDK's own redirect/retry pattern (64 concurrent
1,000-row BAR warmups per burst, ``X-QDL-Replica-Redirect: 1`` then a queued retry on ``REPLICA_LANE_BUSY``,
then the cold-read retry on a retryable 429).

Topology (prefix ``qdl-rss-``, label ``qdl.idr129.rss=true``, every container ``--rm``, one ``--internal`` network):
  qdl-rss-cache     private Redis (stack Redis digest, tmpfs): a read-only COPY of the market-cache keys of the
                    burst's products (``ptr``, ``src``, ``bm``, the ``b``/``bd``/``bs`` buckets of the last
                    rows and the bars ``ckpt`` hashes). Copied once by ``setup`` with DUMP/HMGET/HGETALL only
                    (read-only commands) from the production market cache, then RESTOREd here.
  qdl-rss-quota     private quota/lease Redis (the production replica's quota Redis is never used).
  qdl-rss-query     the production Query container spec (env, read-only mounts, uid 10001) except: market cache
                    and quota URLs, a scratch state dir, instance id, ``--cpus 1.0 --memory 2g``; optional source
                    mounted read-only over ``/app/qdl`` (``--src``) or another image (``--image``). A
                    ``sitecustomize`` probe (``/rssprobe``) answers commands from the driver through a shared
                    scratch dir: stats (RSS, RssAnon, threads, gc counts and generation sizes, mallinfo2, row
                    cache), ``gc.collect``, ``malloc_trim(0)``, tracemalloc snapshots and diffs.
  qdl-rss-driver    SDK-shaped burst driver (TS identity, plus the alpha identities for the other products).
Production is read-only: the copy reads the market cache once; nothing is written to a production Redis,
container, network alias, volume or file. Run under the host measurement lock (AGENTS.md 34a).

Usage (host python):
  idr129_query_rss_isolated.py setup --output DIR             (network, private cache copy, quota Redis)
  idr129_query_rss_isolated.py arm --output DIR --name A [--src DIR | --image REF] [--bursts N] [--tracemalloc F]
                                   [--env K=V ...]            (one replica + driver; replica removed after)
  idr129_query_rss_isolated.py cleanup                        (removes every qdl-rss-* container and the network)
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

PREFIX = "qdl-rss-"
LABEL = "qdl.idr129.rss=true"
NETWORK = PREFIX + "net"
STACK = "qdl_v2_stable_candidate"
PROD_QUERY = f"{STACK}-query_kn_1-1"
PROD_INTERNAL = f"{STACK}_stable_internal"
REDIS_IMAGE = "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8"
QUERY_IMAGE = "sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff"
LOCK = Path.home() / ".local/state/host-measurement-window.lock"
MOUNTS = ("/runtime", "/session-liveness", "/consumer-realms", "/kn", "/stable-certs")
IDENTITIES = Path.home() / ".local/state/qdl-v2/r135-local-batch-fair-e905747-20260920T135150Z/identities"
SLICES = 64
ROWS = 1000  # TS BAR warmup rows (manifest warmup_limit)
COPY_ROWS = 10_000  # rows retained in the private copy per product (largest warmup of the stage mix)
STAGE_ALPHA_ROWS = (2500, 5000, 2500, 5000, 2500, 5000, 2500, 10_000)  # stage-100 cold role + KN-4 ladder

# Requirements of the burst: every TS BAR (1m, 1,000 rows), then alpha BAR products of the most retained
# intervals until SLICES products exist (each must retain >= ROWS rows in the cache, checked by the copy).
PLAN = r'''
import json, sys, yaml
manifests = {
    "trading-system.paper.stable": "/consumer-realms/trading-system.paper.stable-r11.yaml",
    "alpha.binance.paper.stable": "/consumer-realms/alpha.binance.paper.stable-r16.yaml",
    "alpha.okx.paper.stable": "/consumer-realms/alpha.okx.paper.stable-r15.yaml",
}
rows = []
for consumer, path in manifests.items():
    spec = yaml.safe_load(open(path))["spec"]
    for item in spec["requirements"]:
        if item["feed"] == "BAR" and item.get("interval") in ("1m", "3m", "5m", "15m", "30m", "1h"):
            rows.append({"consumer": consumer, "requirement": item})
order = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5}
rows.sort(key=lambda r: (r["consumer"] != "trading-system.paper.stable", order[r["requirement"]["interval"]]))
json.dump(rows, sys.stdout)
'''

COPY = r'''
"""Read-only copy of the burst products' market-cache keys (DUMP/HMGET/HGETALL) into the private cache."""
import json, os, sys
import redis
from qdl.adapters.intervals import canonical_interval_ms
from qdl.query import DataRequirement
from qdl.projection.kn_state_codec import state_partition
from qdl.runtime.kn_bar_readback import BUCKET_OPENS, binding_product_key
from qdl.runtime.kn_market_cache import KnMarketCacheReader
from qdl.runtime.stable_catalog import StableSourceCatalog

plan = json.load(open(sys.argv[1]))
need, rows_needed = int(sys.argv[2]), int(sys.argv[3])
src = redis.Redis.from_url(os.environ["SRC_URL"], socket_timeout=5)
dst = redis.Redis.from_url(os.environ["DST_URL"], socket_timeout=5)
catalog = StableSourceCatalog.load("/runtime/stable-source-bindings.yaml")
reader = KnMarketCacheReader(src, "paper")
chosen, keys, size, ckpts = [], 0, 0, set()

def copy(key):
    global keys, size
    blob = src.dump(key)
    if blob is None:
        return False
    dst.restore(key, 0, blob, replace=True)
    keys += 1
    size += len(blob)
    return True

def copy_product(entry, *, tail_only=False, generations={}):
    item = dict(entry["requirement"])
    try:
        binding = catalog.binding_for(DataRequirement.from_mapping({**item, "warmup_limit": 0}))
    except Exception:
        return None
    lpk = binding_product_key(binding, "paper")
    code = lpk.encode()
    ready = src.hmget(reader.pointer_key(lpk), "ready")[0]
    if ready is None:
        return None
    generation = int(ready)
    meta = {k.decode(): v.decode() for k, v in src.hgetall(f"kn3:paper:bm:{generation}:{code}").items()}
    if int(meta.get("rows") or 0) < rows_needed + 5 or not meta.get("last"):
        return None
    step = canonical_interval_ms(binding.interval)
    last = int(meta["last"])
    tail_only = tail_only and generations.get(code) == generation
    generations[code] = generation
    first_needed = last - (2 * BUCKET_OPENS if tail_only else rows_needed + 200) * step
    for key in (reader.pointer_key(lpk), f"kn3:paper:src:{code}", f"kn3:paper:bm:{generation}:{code}"):
        copy(key)
    for bucket in range(first_needed // (BUCKET_OPENS * step), last // (BUCKET_OPENS * step) + 1):
        for kind in ("b", "bd", "bs"):
            copy(f"kn3:paper:{kind}:{generation}:{code}:{bucket}")
    ckpts.add(reader.checkpoint_key(reader.bars_topic, state_partition(lpk, reader.bars_partitions)))
    return {**entry, "binding_id": binding.binding_id, "retained_rows": int(meta["rows"])}

sync_seconds = float(os.environ.get("SYNC_SECONDS", "0"))
if sync_seconds:
    # Keep the copy as fresh as the production cache: re-copy each product's tail every sync_seconds (read-only
    # source commands only) until the deadline; a generation change re-copies the whole window.
    import time
    deadline = time.monotonic() + float(os.environ["SYNC_DEADLINE_SECONDS"])
    rounds = 0
    while time.monotonic() < deadline:
        for entry in plan:
            copy_product(entry, tail_only=rounds > 0)
        for key in sorted(ckpts):
            copy(key)
        rounds += 1
        print(json.dumps({"round": rounds, "keys": keys, "dump_bytes": size}), flush=True)
        time.sleep(sync_seconds)
    sys.exit(0)
for entry in plan:
    if len(chosen) >= need:
        break
    copied = copy_product(entry)
    if copied is not None:
        chosen.append(copied)
for key in sorted(ckpts):
    copy(key)
print(json.dumps({"products": chosen, "keys": keys, "dump_bytes": size}))
'''

PROBE = r'''
"""IDR-129 RSS probe (isolated replica only): answers driver commands written to /probe; no payloads."""
import collections, ctypes, gc, json, os, sys, threading, time, tracemalloc

_snaps = {}

class _Mallinfo2(ctypes.Structure):
    _fields_ = [(n, ctypes.c_size_t) for n in ("arena", "ordblks", "smblks", "hblks", "hblkhd", "usmblks",
                                                 "fsmblks", "uordblks", "fordblks", "keepcost")]

try:
    _libc = ctypes.CDLL("libc.so.6")
    _libc.mallinfo2.restype = _Mallinfo2
    _libc.malloc_trim.argtypes = [ctypes.c_size_t]
except Exception:
    _libc = None

def _status():
    out = {}
    with open("/proc/self/status") as handle:
        for line in handle:
            key, _, value = line.partition(":")
            if key in ("VmRSS", "RssAnon", "RssFile", "VmHWM", "Threads"):
                out[key] = int(value.split()[0])
    return out

_caches = []

def _row_caches():
    # The backend is built before gc.freeze(), so gc.get_objects() never lists it: instances are registered here.
    return [cache.stats() for cache in _caches]

def _register_row_caches():
    try:
        from qdl.query import row_cache
    except Exception:
        return
    original = row_cache.BoundedRowCache.__init__
    def init(self, *args, **kwargs):
        original(self, *args, **kwargs)
        _caches.append(self)
    row_cache.BoundedRowCache.__init__ = init

def stats(types=False):
    s = _status()
    record = {"wall_ms": time.time_ns() // 1_000_000, "rss_mib": round(s["VmRSS"] / 1024, 1),
              "rss_anon_mib": round(s["RssAnon"] / 1024, 1), "hwm_mib": round(s["VmHWM"] / 1024, 1),
              "threads": s["Threads"], "thread_names": collections.Counter(
                  t.name.rstrip("0123456789_-") for t in threading.enumerate()).most_common(),
              "gc_count": gc.get_count(), "gc_collections": [g["collections"] for g in gc.get_stats()],
              "gen_sizes": [len(gc.get_objects(g)) for g in range(3)], "frozen": gc.get_freeze_count(),
              "allocated_blocks": sys.getallocatedblocks(), "row_caches": _row_caches()}
    if _libc is not None:
        m = _libc.mallinfo2()
        record["mallinfo2_mib"] = {n: round(getattr(m, n) / 1048576, 1) for n in ("arena", "hblkhd", "uordblks",
                                                                                  "fordblks", "keepcost")}
    if tracemalloc.is_tracing():
        current, peak = tracemalloc.get_traced_memory()
        record["tracemalloc_mib"] = [round(current / 1048576, 1), round(peak / 1048576, 1)]
    if types:
        record["types_gen2"] = collections.Counter(type(o).__qualname__ for o in gc.get_objects(2)).most_common(25)
    return record

def run(command):
    action = command["action"]
    if action == "stats":
        return stats(types=command.get("types", False))
    if action == "collect":
        before = stats()
        started = time.perf_counter()
        collected = gc.collect()
        return {"before": before, "collected": collected, "ms": round((time.perf_counter() - started) * 1e3, 1),
                "after": stats()}
    if action == "trim":
        before = stats()
        started = time.perf_counter()
        released = _libc.malloc_trim(0) if _libc is not None else None
        return {"before": before, "released": released, "ms": round((time.perf_counter() - started) * 1e3, 1),
                "after": stats()}
    if action == "tm_snapshot":
        _snaps[command["name"]] = tracemalloc.take_snapshot().filter_traces(
            (tracemalloc.Filter(False, tracemalloc.__file__), tracemalloc.Filter(False, __file__)))
        return {"name": command["name"]}
    if action == "tm_diff":
        old, new = _snaps[command["old"]], _snaps[command["new"]]
        key = command.get("key", "lineno")
        diff = new.compare_to(old, key)
        top = [{"size_diff_kib": round(d.size_diff / 1024, 1), "size_kib": round(d.size / 1024, 1),
                "count_diff": d.count_diff, "where": [f"{f.filename.rsplit('/site-packages/', 1)[-1].rsplit('/app/', 1)[-1]}:{f.lineno}"
                                                       for f in d.traceback[:8]]} for d in diff[:40]]
        live = [{"size_kib": round(s.size / 1024, 1), "count": s.count,
                 "where": [f"{f.filename.rsplit('/site-packages/', 1)[-1].rsplit('/app/', 1)[-1]}:{f.lineno}"
                           for f in s.traceback[:8]]} for s in new.statistics(key)[:30]]
        return {"diff": top, "live": live}
    if action == "saveall_collect":
        before = stats()
        gc.set_debug(gc.DEBUG_SAVEALL)
        started = time.perf_counter()
        collected = gc.collect()
        ms = round((time.perf_counter() - started) * 1e3, 1)
        gc.set_debug(0)
        garbage = list(gc.garbage)
        gc.garbage.clear()
        summary = _garbage_summary(garbage)
        del garbage
        # The saved garbage is still cyclic: one more (plain) collection frees it, which is what it was holding.
        held = stats()
        freed = gc.collect()
        return {"before": before, "collected": collected, "ms": ms, "summary": summary, "held": held,
                "freed": freed, "after": stats()}
    if action == "holders":
        return holders(command.get("min_bytes", 262144))
    raise ValueError(action)

def _bulk(value):
    """Rough payload size of a local: bytes/str length, or element count x 1 KiB for long sequences/dicts,
    or the item count of a history-like object (``items``/``history.items``)."""
    if isinstance(value, (bytes, bytearray, str)):
        return len(value)
    if isinstance(value, (list, tuple, dict)):
        return len(value) * 1024 if len(value) >= 64 else sum(_bulk(v) for v in (value.values() if isinstance(value, dict) else value) if isinstance(v, (bytes, bytearray)) )
    for attr in ("items", "body"):
        inner = getattr(value, attr, None)
        if isinstance(inner, (bytes, bytearray)):
            return len(inner)
        if isinstance(inner, tuple):
            return len(inner) * 1024
    history = getattr(value, "history", None) or getattr(value, "result", None)
    if history is not None and history is not value:
        items = getattr(history, "items", None) or getattr(getattr(history, "history", None), "items", None)
        if isinstance(items, tuple):
            return len(items) * 1024
    return 0

def _garbage_summary(objects):
    """Cyclic garbage by type, frame, exception and coroutine, plus the bytes/str payload one level below."""
    types, frames, excs, coros, leaf_types, big_locals = (collections.Counter() for _ in range(6))
    leaf = 0
    for obj in objects:
        name = f"{type(obj).__module__}.{type(obj).__qualname__}"
        types[name] += 1
        if type(obj).__name__ == "frame":
            code = obj.f_code
            where = f"{code.co_filename.rsplit('/app/', 1)[-1].rsplit('/site-packages/', 1)[-1]}:{code.co_qualname}:{obj.f_lineno}"
            frames[where] += 1
            try:
                for local, value in obj.f_locals.items():
                    size = _bulk(value)
                    if size >= 65536:
                        big_locals[f"{where} {local}={type(value).__qualname__}"] += size
            except Exception:
                pass
        elif isinstance(obj, BaseException):
            excs[name] += 1
        elif type(obj).__name__ == "coroutine":
            coros[obj.__qualname__] += 1
        for attr in ("_write_backlog", "_buffer"):  # asyncio sslproto / selector transport pending writes
            pending = getattr(obj, attr, None) if not isinstance(obj, (dict, tuple, list)) else None
            if isinstance(pending, (collections.deque, bytearray)):
                size = len(pending) if isinstance(pending, bytearray) else sum(
                    (x.nbytes if isinstance(x, memoryview) else len(x)) for x in pending
                    if isinstance(x, (bytes, bytearray, memoryview)))
                leaf_types[f"pending {attr} of {type(obj).__qualname__}"] += size
        for ref in gc.get_referents(obj):
            if isinstance(ref, (bytes, bytearray, str, memoryview)):
                size = ref.nbytes if isinstance(ref, memoryview) else sys.getsizeof(ref)
                leaf += size
                leaf_types[f"{type(ref).__name__} via {type(obj).__qualname__}"] += size
    return {"objects": len(objects), "leaf_mib_one_level": round(leaf / 1048576, 1),
            "leaf_by_holder_mib": [(k, round(v / 1048576, 2)) for k, v in leaf_types.most_common(15)],
            "types": types.most_common(30), "frames": frames.most_common(40), "exceptions": excs.most_common(15),
            "coroutines": coros.most_common(15),
            "big_locals_mib": [(k, round(v / 1048576, 2)) for k, v in big_locals.most_common(20)]}

def _describe(obj):
    kind = f"{type(obj).__module__}.{type(obj).__qualname__}"
    if type(obj).__name__ == "frame":
        code = obj.f_code
        return f"frame {code.co_filename.rsplit('/app/', 1)[-1].rsplit('/site-packages/', 1)[-1]}:{code.co_qualname}:{obj.f_lineno}"
    if type(obj).__name__ in ("coroutine", "Task"):
        return f"{kind} {getattr(obj, '__qualname__', '') or getattr(obj, 'get_name', lambda: '')()}"
    if isinstance(obj, BaseException):
        return f"exception {kind}"
    return kind

def holders(min_bytes):
    """Large bytes/bytearray/str reachable one level below a gc-tracked container, with three levels of
    referrers (types, frames and coroutine names only; never the payload)."""
    gc.collect(0)
    seen, found = set(), []
    objects = gc.get_objects()
    for container in objects:
        try:
            refs = gc.get_referents(container)
        except Exception:
            continue
        for ref in refs:
            if isinstance(ref, (bytes, bytearray, str)) and len(ref) >= min_bytes and id(ref) not in seen:
                seen.add(id(ref))
                chain, level = [], [container]
                for _ in range(3):
                    names = collections.Counter(_describe(o) for o in level)
                    chain.append(names.most_common(6))
                    nxt = []
                    for o in level[:4]:
                        nxt.extend(r for r in gc.get_referrers(o)
                                   if r is not objects and r is not level and r is not nxt and r is not refs
                                   and not (type(r) is list and len(r) >= 1000))
                    level = nxt[:8]
                    if not level:
                        break
                found.append({"bytes": len(ref), "type": type(ref).__name__, "chain": chain})
    del objects
    return {"count": len(found), "total_mib": round(sum(f["bytes"] for f in found) / 1048576, 1),
            "items": found[:25]}

def _loop():
    root = "/probe"
    while True:
        try:
            for name in sorted(os.listdir(root)):
                if name.startswith("cmd-") and name.endswith(".json"):
                    path = os.path.join(root, name)
                    command = json.load(open(path))
                    os.unlink(path)
                    try:
                        reply = {"ok": True, "result": run(command)}
                    except Exception as error:
                        reply = {"ok": False, "error": repr(error)}
                    tmp = os.path.join(root, "tmp-" + name)
                    with open(tmp, "w") as handle:
                        json.dump(reply, handle)
                    os.rename(tmp, os.path.join(root, "out-" + name[4:]))
        except FileNotFoundError:
            pass
        time.sleep(0.2)

if os.getpid() == 1 and os.path.isdir("/probe"):
    frames = int(os.environ.get("QDL_RSS_TRACEMALLOC", "0") or 0)
    if frames:
        tracemalloc.start(frames)
    _register_row_caches()
    threading.Thread(target=_loop, name="rss-probe", daemon=True).start()
'''

DRIVER = r'''
"""IDR-129 burst driver: SDK-shaped concurrent BAR warmups against one isolated Query replica."""
import asyncio, json, os, statistics, sys, time
import httpx
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import (BarRevisionPolicy, DataRequirement, Feed, GapPolicy, Grade, RecoveryPolicy,
                            StalePolicy)
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.transport import RestQueryTransport, REPLICA_LANE_BUSY

ISSUER, AUDIENCE = "https://identity.qdl.stable.internal", "qdl-v2-stable"
ROLES = ("historical_reader", "market_data_reader", "stream_consumer")
IDS = {"trading-system.paper.stable": ("trading-system", "stable-trading-system-rs256-v1"),
       "alpha.binance.paper.stable": ("alpha-binance", "stable-alpha-binance-rs256-v1"),
       "alpha.okx.paper.stable": ("alpha-okx", "stable-alpha-okx-rs256-v1")}
SUBJECTS = {"trading-system.paper.stable": ("spiffe://qdl/paper/trading-system-stable", 11),
            "alpha.binance.paper.stable": ("spiffe://qdl/paper/alpha-binance-stable", 16),
            "alpha.okx.paper.stable": ("spiffe://qdl/paper/alpha-okx-stable", 15)}
cfg = json.load(open("/work/driver.json"))
seq = 0

def probe(action, **kw):
    global seq
    seq += 1
    name = f"{seq:05d}"
    tmp = f"/probe/tmpcmd-{name}"
    with open(tmp, "w") as handle:
        json.dump({"action": action, **kw}, handle)
    os.rename(tmp, f"/probe/cmd-{name}.json")
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        out = f"/probe/out-{name}.json"
        if os.path.exists(out):
            reply = json.load(open(out))
            os.unlink(out)
            return reply
        time.sleep(0.1)
    return {"ok": False, "error": "probe timeout"}

def transport(consumer):
    slug, key_id = IDS[consumer]
    subject, revision = SUBJECTS[consumer]
    base = f"/identities/{slug}"
    tls = WorkloadTlsConfig(f"{base}/ca.crt", f"{base}/client.crt", f"{base}/client.key")
    cred = RotatingJwtCredentialProvider(private_key_file=f"/identities/{slug}-jwt/private.key", key_id=key_id,
        algorithm="RS256", issuer=ISSUER, audience=AUDIENCE, subject=subject, environment="paper", roles=ROLES,
        venues=("BINANCE", "OKX"), consumer_manifest_revision=revision, lifetime_seconds=300,
        refresh_before_seconds=60)
    return RestQueryTransport(cfg["url"], timeout_seconds=10.0, credential_provider=cred, tls=tls)

async def slice_warmup(t, consumer, requirement, out):
    """SDK 2.4 shape: up to 3 cold-read attempts per round, each redirect=1 then a queued retry; rounds = the TS
    bridge reconnect loop (1 s backoff)."""
    started = time.perf_counter()
    for round_ in range(cfg["rounds"]):
        for attempt in range(3):
            error = None
            for redirect in (True, False):
                t0 = time.perf_counter()
                try:
                    payload = await t.warmup(requirement, consumer_id=consumer, replica_redirect=redirect)
                except DataLayerError as caught:
                    out["requests"].append(["429" if caught.code == "RATE_LIMITED" else caught.code,
                                            caught.reason, round((time.perf_counter() - t0) * 1e3)])
                    error = caught
                    if redirect and caught.code == "RATE_LIMITED" and caught.reason == REPLICA_LANE_BUSY:
                        continue
                    break
                except httpx.HTTPError as caught:
                    out["requests"].append(["TRANSPORT", type(caught).__name__, round((time.perf_counter() - t0) * 1e3)])
                    error = caught
                    break
                out["requests"].append(["200", len(payload.get("data", ())), round((time.perf_counter() - t0) * 1e3)])
                out["ok"].append(round((time.perf_counter() - started) * 1e3))
                return
            if not isinstance(error, DataLayerError) or error.code != "RATE_LIMITED" or not error.retryable:
                break
            delay = max(250 * (attempt + 1), error.retry_after_ms or 0)
            await asyncio.sleep(delay / 1000)
        await asyncio.sleep(1.0)
    out["failed"] += 1

async def main():
    transports = {c: transport(c) for c in IDS}
    enums = {"feed": Feed, "consumer_grade": Grade, "stale_policy": StalePolicy, "gap_policy": GapPolicy,
             "recovery": RecoveryPolicy, "bar_revision_policy": BarRevisionPolicy, "event_recency_policy": StalePolicy}
    reqs = []
    for p in cfg["products"]:
        fields = {k: (enums[k](v) if k in enums and v is not None else v) for k, v in p["requirement"].items()}
        reqs.append((p["consumer"], DataRequirement(**{**fields, "warmup_limit": p["rows"]})))
    ready_deadline = time.monotonic() + 600
    while True:  # the replica serves HTTPS only after start-up (slower under tracemalloc)
        try:
            await transports["trading-system.paper.stable"]._client.get("/health/live")
            break
        except httpx.HTTPError:
            if time.monotonic() > ready_deadline:
                raise
            await asyncio.sleep(2)
    await asyncio.sleep(5)
    record = {"baseline": probe("stats", types=True), "bursts": []}
    if cfg.get("tracemalloc"):
        probe("tm_snapshot", name="start")
    for burst in range(cfg["bursts"]):
        out = {"requests": [], "ok": [], "failed": 0}
        t0 = time.perf_counter()
        await asyncio.gather(*(slice_warmup(transports[c], c, r, out) for c, r in reqs))
        wall = round(time.perf_counter() - t0, 1)
        await asyncio.sleep(cfg["settle_s"])
        statuses = {}
        for status, _, _ in out["requests"]:
            statuses[status] = statuses.get(status, 0) + 1
        ok200 = [ms for s, _, ms in out["requests"] if s == "200"]
        entry = {"burst": burst + 1, "wall_s": wall, "statuses": statuses, "failed_slices": out["failed"],
                 "slice_ok_ms_p50": statistics.median(out["ok"]) if out["ok"] else None,
                 "slice_ok_ms_max": max(out["ok"], default=None),
                 "req200_ms_p50": statistics.median(ok200) if ok200 else None,
                 "req200_ms_p95": sorted(ok200)[int(0.95 * (len(ok200) - 1))] if ok200 else None,
                 "rows": sorted({n for s, n, _ in out["requests"] if s == "200"}),
                 "stats": probe("stats")}
        if burst == 0 and cfg.get("tracemalloc"):
            probe("tm_snapshot", name="after1")
        record["bursts"].append(entry)
        r = entry["stats"].get("result", {})
        print(json.dumps({k: entry[k] for k in ("burst", "wall_s", "statuses", "failed_slices", "req200_ms_p50")}
                         | {"rss_mib": r.get("rss_mib"), "threads": r.get("threads"), "gen": r.get("gen_sizes")}),
              flush=True)
        await asyncio.sleep(cfg["gap_s"])
    record["idle"] = probe("stats", types=True)
    record["holders"] = probe("holders")
    if cfg.get("tracemalloc"):
        probe("tm_snapshot", name="end")
        record["tm_diff_after1_end"] = probe("tm_diff", old="after1", new="end")
        record["tm_diff_start_end_tb"] = probe("tm_diff", old="start", new="end", key="traceback")
    if cfg.get("tracemalloc"):
        probe("tm_snapshot", name="before_collect")
    record["collect"] = probe("saveall_collect")
    if cfg.get("tracemalloc"):
        probe("tm_snapshot", name="after_collect")
        record["tm_freed_by_collect"] = probe("tm_diff", old="before_collect", new="after_collect", key="traceback")
    record["trim"] = probe("trim")
    json.dump(record, open("/work/result.json", "w"), indent=1)
    for t in transports.values():
        await t.close()

asyncio.run(main())
'''


def sh(*args, check=True, timeout=180):
    result = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"{args[:4]} failed: {result.stderr.strip()[:600]}")
    return result.stdout.strip()


def require_lock(owner: str):
    try:
        lock = json.loads(LOCK.read_text())
    except (OSError, ValueError):
        raise SystemExit("host measurement lock is absent: acquire it first (AGENTS.md 34a)")
    if lock.get("owner") != owner:
        raise SystemExit(f"host measurement lock is held by {lock.get('owner')}, not {owner}")


def prod_spec():
    raw = json.loads(sh("docker", "inspect", PROD_QUERY))[0]
    mounts = {m["Destination"]: m["Source"] for m in raw["Mounts"] if m["Destination"] in MOUNTS}
    if set(mounts) != set(MOUNTS):
        raise RuntimeError(f"production Query mounts differ: {sorted(mounts)}")
    env = dict(item.split("=", 1) for item in raw["Config"]["Env"])
    return raw["Image"], env, mounts, raw["Config"]["Cmd"]


def ro_mounts(mounts: dict) -> list[str]:
    return [a for dst, src in sorted(mounts.items()) for a in ("--mount", f"type=bind,src={src},dst={dst},readonly")]


def setup(args) -> int:
    require_lock(args.lock_owner)
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    image, _env, mounts, _cmd = prod_spec()
    if image != QUERY_IMAGE:
        raise SystemExit(f"production Query image is {image[:19]}, expected {QUERY_IMAGE[:19]}")
    work = output / "work"
    work.mkdir(mode=0o777)
    os.chmod(work, 0o777)
    sh("docker", "network", "create", "--internal", "--label", LABEL, NETWORK)
    for name, memory in (("cache", "768m"), ("quota", "128m")):
        sh("docker", "run", "-d", "--rm", "--name", PREFIX + name, "--label", LABEL, "--network", NETWORK,
           "--cpus", "0.5", "--memory", memory, "--read-only", "--tmpfs", "/data:size=64m",
           REDIS_IMAGE, "redis-server", "--save", "", "--appendonly", "no", "--maxmemory",
           "640mb" if name == "cache" else "96mb")
    plan = sh("docker", "run", "--rm", "--network", "none", "--user", "10001:10001", "--read-only",
              "--cpus", "0.5", *ro_mounts({"/consumer-realms": mounts["/consumer-realms"]}),
              QUERY_IMAGE, "python", "-c", PLAN)
    (work / "plan.json").write_text(plan)
    os.chmod(work / "plan.json", 0o644)
    name = PREFIX + "copy"
    sh("docker", "create", "--rm", "--name", name, "--label", LABEL, "--network", NETWORK, "--user", "10001:10001",
       "--read-only", "--cpus", "0.5", "--memory", "512m", "-e", "PYTHONPATH=/app",
       "-e", "SRC_URL=redis://market_cache:6379/0", "-e", f"DST_URL=redis://{PREFIX}cache:6379/0",
       *ro_mounts({"/runtime": mounts["/runtime"]}),
       "--mount", f"type=bind,src={work},dst=/work,readonly",
       QUERY_IMAGE, "python", "-c", COPY, "/work/plan.json", str(args.slices), str(COPY_ROWS))
    sh("docker", "network", "connect", PROD_INTERNAL, name)
    started = time.time()
    copied = subprocess.run(["docker", "start", "-a", name], capture_output=True, text=True, timeout=600)
    if copied.returncode != 0:
        raise SystemExit(f"copy failed: {copied.stderr[-1500:]}")
    result = json.loads(copied.stdout.strip().splitlines()[-1])
    result["copy_seconds"] = round(time.time() - started, 1)
    result["copied_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    (output / "products.json").write_text(json.dumps(result, indent=1) + "\n")
    print(json.dumps({"products": len(result["products"]), "keys": result["keys"], "dump_bytes": result["dump_bytes"],
                      "copy_seconds": result["copy_seconds"]}))
    return 0 if len(result["products"]) == args.slices else 1


def arm(args) -> int:
    require_lock(args.lock_owner)
    output = args.output.resolve()
    products = json.loads((output / "products.json").read_text())["products"]
    image, env, mounts, cmd = prod_spec()
    run_image = args.image or QUERY_IMAGE
    arm_dir = output / args.name
    arm_dir.mkdir(mode=0o700)
    state, probe_dir, work = arm_dir / "state", arm_dir / "probe", arm_dir / "work"
    for path in (state, state / "runtime", state / "shared", probe_dir, work):
        path.mkdir(mode=0o777)
        os.chmod(path, 0o777)
    (state / "runtime/session-liveness").symlink_to("/session-liveness")
    probe_src = arm_dir / "rssprobe"
    probe_src.mkdir(mode=0o755)
    (probe_src / "sitecustomize.py").write_text(PROBE)
    os.chmod(probe_src / "sitecustomize.py", 0o644)
    env = dict(env)
    env.update({
        "QDL_STABLE_REDIS_URL": f"redis://{PREFIX}quota:6379/0",
        "QDL_KN3_MARKET_CACHE_URL": f"redis://{PREFIX}cache:6379/0",
        "QDL_STABLE_INSTANCE_ID": f"rss-query-{args.name}",
        "QDL_STABLE_CONSUMER_GROUP": f"rss-query-{args.name}",
        "QDL_STABLE_AUDIT_PATH": "/var/lib/qdl-kn/runtime/rss-query-audit.jsonl",
        "PYTHONPATH": "/rssprobe:" + env.get("PYTHONPATH", "/app"),
        "QDL_RSS_TRACEMALLOC": str(args.tracemalloc),
    })
    for item in args.env:
        key, _, value = item.partition("=")
        env[key] = value
    env_file = arm_dir / ".env"
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        for key, value in sorted(env.items()):
            if "\n" in value:
                raise RuntimeError(f"env {key} spans lines")
            handle.write(f"{key}={value}\n")
    query = PREFIX + "query"
    meta = {"arm": args.name, "image": run_image, "src": str(args.src) if args.src else None,
            "tracemalloc": args.tracemalloc, "env_overrides": args.env, "bursts": args.bursts, "mix": args.mix,
            "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        create = ["docker", "create", "--rm", "--name", query, "--label", LABEL, "--network", NETWORK,
                  "--network-alias", "query_v2_1", "--user", "10001:10001", "--read-only",
                  "--tmpfs", "/tmp:rw,noexec,nosuid,size=67108864", "--cpus", "1.0", "--memory", "2g",
                  "--pids-limit", "256", "--security-opt", "no-new-privileges:true", "--cap-drop", "ALL",
                  "--log-opt", "max-size=50m", "--env-file", str(env_file), *ro_mounts(mounts),
                  "--mount", f"type=bind,src={state},dst=/var/lib/qdl-kn",
                  "--mount", f"type=bind,src={probe_dir},dst=/probe",
                  "--mount", f"type=bind,src={probe_src},dst=/rssprobe,readonly"]
        if args.src:
            create += ["--mount", f"type=bind,src={args.src.resolve() / 'qdl'},dst=/app/qdl,readonly"]
        sh(*create, run_image, *cmd)
        env_file.unlink()
        sync = PREFIX + "sync"
        (work / "products.json").write_text(json.dumps(products))
        os.chmod(work / "products.json", 0o644)
        sh("docker", "create", "--rm", "--name", sync, "--label", LABEL, "--network", NETWORK,
           "--user", "10001:10001", "--read-only", "--cpus", "0.25", "--memory", "256m", "-e", "PYTHONPATH=/app",
           "-e", "SRC_URL=redis://market_cache:6379/0", "-e", f"DST_URL=redis://{PREFIX}cache:6379/0",
           "-e", "SYNC_SECONDS=15", "-e", f"SYNC_DEADLINE_SECONDS={args.bursts * (args.gap_s + 90) + 900}",
           *ro_mounts({"/runtime": mounts["/runtime"]}), "--mount", f"type=bind,src={work},dst=/work,readonly",
           QUERY_IMAGE, "python", "-c", COPY, "/work/products.json", str(len(products)), str(COPY_ROWS))
        sh("docker", "network", "connect", PROD_INTERNAL, sync)
        sh("docker", "start", sync)
        sh("docker", "start", query)
        time.sleep(args.startup_s)
        alpha = 0
        for product in products:
            if product["consumer"] == "trading-system.paper.stable" or args.mix == "ts1000":
                product["rows"] = ROWS
            else:
                product["rows"] = STAGE_ALPHA_ROWS[alpha % len(STAGE_ALPHA_ROWS)]
                alpha += 1
        meta["rows"] = [p["rows"] for p in products]
        (work / "driver.json").write_text(json.dumps({
            "url": "https://query_v2_1:8200", "products": products, "bursts": args.bursts,
            "rounds": 4, "settle_s": 5, "gap_s": args.gap_s, "tracemalloc": bool(args.tracemalloc)}))
        os.chmod(work / "driver.json", 0o644)
        identities = [a for slug in ("trading-system", "alpha-binance", "alpha-okx") for kind in ("", "-jwt")
                      for a in ("--mount", f"type=bind,src={IDENTITIES / (slug + kind)},dst=/identities/{slug + kind},readonly")]
        driver = subprocess.run(
            ["docker", "run", "--rm", "--name", PREFIX + "driver", "--label", LABEL, "--network", NETWORK,
             "--user", "10001:10001", "--read-only", "--cpus", "1.0", "--memory", "1g", "-e", "PYTHONPATH=/app",
             *identities, "--mount", f"type=bind,src={work},dst=/work",
             "--mount", f"type=bind,src={probe_dir},dst=/probe", QUERY_IMAGE, "python", "-c", DRIVER],
            capture_output=True, text=True, timeout=args.bursts * (args.gap_s + 120) + 600)
        (arm_dir / "driver.out").write_text(driver.stdout + "\n--stderr--\n" + driver.stderr[-20000:])
        meta["driver_returncode"] = driver.returncode
        print(driver.stdout[-6000:])
        if driver.returncode != 0:
            print(driver.stderr[-3000:], file=sys.stderr)
    finally:
        logs = subprocess.run(["docker", "logs", "-t", query], capture_output=True, text=True, timeout=120)
        (arm_dir / "query.log").write_text(logs.stdout + logs.stderr)
        sh("docker", "rm", "-f", query, check=False)
        sh("docker", "rm", "-f", PREFIX + "driver", check=False)
        sync_logs = subprocess.run(["docker", "logs", PREFIX + "sync"], capture_output=True, text=True, timeout=60)
        (arm_dir / "sync.log").write_text(sync_logs.stdout[-4000:] + sync_logs.stderr[-4000:])
        sh("docker", "rm", "-f", PREFIX + "sync", check=False)
        env_file.unlink(missing_ok=True)
        meta["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (arm_dir / "run.json").write_text(json.dumps(meta, indent=1) + "\n")
    return meta.get("driver_returncode", 1)


def cleanup(_args) -> int:
    names = [n for n in sh("docker", "ps", "-a", "--filter", f"label={LABEL}", "--format", "{{.Names}}",
                           check=False).splitlines() if n.startswith(PREFIX)]
    for name in names:
        sh("docker", "rm", "-f", name, check=False)
    nets = [n for n in sh("docker", "network", "ls", "--filter", f"label={LABEL}", "--format", "{{.Name}}",
                          check=False).splitlines() if n == NETWORK]
    for net in nets:
        sh("docker", "network", "rm", net, check=False)
    print(json.dumps({"removed_containers": names, "removed_networks": nets}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("setup")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--slices", type=int, default=SLICES)
    p.add_argument("--lock-owner", default="claude-idr129-query-rss")
    p.set_defaults(func=setup)
    p = sub.add_parser("arm")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--src", type=Path)
    p.add_argument("--image")
    p.add_argument("--bursts", type=int, default=12)
    p.add_argument("--gap-s", type=int, default=10)
    p.add_argument("--startup-s", type=int, default=25)
    p.add_argument("--tracemalloc", type=int, default=0)
    p.add_argument("--mix", choices=("stage", "ts1000"), default="stage")
    p.add_argument("--env", action="append", default=[])
    p.add_argument("--lock-owner", default="claude-idr129-query-rss")
    p.set_defaults(func=arm)
    p = sub.add_parser("cleanup")
    p.set_defaults(func=cleanup)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

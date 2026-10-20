#!/usr/bin/env python3
"""DL v2.2.5 Query A/B: old image 3e9e4d3a vs new image 753604f8 on one isolated private copy of the market cache.

Purpose (owner decision 2026-10-20, option (b) "Đo luôn rồi tag"): produce the two pieces of evidence the v2.2.5
certificate still lacks, in one isolated session.
  1. G5 D3 (b) "concurrent hot-read latency": a steady open-loop hot-read driver (TS EXECUTION QUOTE/TRADE
     snapshots, the TS writer's request) runs while one Query replica serves repeated 64-warmup bursts, including
     the memory-maintenance runs of the new image (RSS >= its soft limit, reached by the bursts themselves; no
     manual gc/trim in a scored arm). p50/p95/p99/max per arm.
  2. DR byte comparison: for every TS manifest r11 requirement, the warmup response of the old and the new image
     for the same cache state (status, headers, body bytes), with a same-image control request that names the
     per-request volatile fields.
The pass rules are pre-registered (PREREG below) and written to ``prereg.json`` before any container starts.

Boundary: production is read-only. The only production contact is a read-only copy of market-cache keys
(DUMP/HMGET/HGETALL, the IDR-129 harness method) by the ``sync`` container and ``docker inspect`` of query_kn_1 for
its container spec. Every container is ``--rm``, CPU-capped, on one ``--internal`` network (prefix ``qdl-hr-``,
label ``qdl.v225.hotread=true``); ``cleanup`` removes all of them and the network. Host lock: by the owner's
instruction of 2026-10-20 ~04:30Z this session runs CONCURRENTLY with another agent's correctness-only lock
(``--concurrent``): the lock file is never written or removed, foreign activity (other containers, load, CPU PSI,
the lock content) is sampled every 30 s into ``foreign.jsonl``, and the arms are interleaved (A1 B1 A2 B2) so the
rules are judged on paired old-vs-new deltas, not on absolute numbers. Without ``--concurrent`` the lock must be
held by ``--lock-owner``. TS identity = the new mesh generation (root-only files, so the drivers
run as root with DAC_READ_SEARCH only); alpha-okx identity = its phase105b client certificate (still trusted).

Usage (host python):
  v225_query_hot_read_ab.py run --output DIR [--lock-owner O | --concurrent]
  v225_query_hot_read_ab.py analyze --output DIR
  v225_query_hot_read_ab.py cleanup
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("idr129_harness", ROOT / "scripts/idr129_query_rss_isolated.py")
harness = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(harness)

PREFIX = "qdl-hr-"
LABEL = "qdl.v225.hotread=true"
NETWORK = PREFIX + "net"
STACK = "qdl_v2_stable_candidate"
PROD_QUERY = f"{STACK}-query_kn_1-1"
PROD_INTERNAL = f"{STACK}_stable_internal"
OLD_IMAGE = "sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff"  # v2.2.4 Query 31a5155
NEW_IMAGE = "sha256:753604f8abaa866e2de30c7ccaf36168986bc6991550ee326d53e51b83d1550a"  # v2.2.5 Query 9b6747f
REDIS_IMAGE = harness.REDIS_IMAGE
LOCK = harness.LOCK
STATE = Path.home() / ".local/state/qdl-v2"
TS_TLS = STATE / "tls/gen-20261019T084200Z/identities/trading-system"
TS_JWT = STATE / "655d2106d01f/bundle/identities-rotate-20260822T144323Z/trading-system-jwt"
OKX_TLS = STATE / "r135-local-batch-fair-e905747-20260920T135150Z/identities/alpha-okx"
OKX_JWT = STATE / "r135-local-batch-fair-e905747-20260920T135150Z/identities/alpha-okx-jwt"
MOUNTS = harness.MOUNTS
TS = "trading-system.paper.stable"
OKX = "alpha.okx.paper.stable"

PREREG = {
    "schema": "qdl.v225.query-ab-prereg.v2",
    "revision_note": ("v2 (before any run) replaces v1 of commit 7d28d9cb: owner 2026-10-20 ~04:30Z: run concurrently "
                      "with a foreign correctness-only lock, interleave arms, judge paired old-vs-new deltas, record "
                      "foreign activity"),
    "images": {"A_old": OLD_IMAGE, "B_new": NEW_IMAGE},
    "replica": "one Query replica per arm, production container spec of query_kn_1 except private cache/quota Redis, --cpus 1.0 --memory 2g (the IDR-129 arm shape)",
    "order": "byte compare (A and B side by side), then interleaved arms A1 B1 A2 B2 (A = old, B = new), each a fresh replica",
    "concurrency": "runs beside another agent's correctness-only job under its host lock (owner instruction); foreign activity sampled every 30 s",
    "burst": {"slices": 64, "bursts": 6, "gap_s": 10, "settle_s": 5, "rounds": 4,
              "mix": "TS r11 BAR 1m x 1,000 rows (10 slices) + alpha-okx BAR 1m..1h cycled to 54 slices with rows "
                     "2500/5000/2500/5000/2500/5000/2500/10000 (the IDR-129 stage ladder); SDK 2.4 redirect/queued "
                     "retry shape",
              "note": "alpha-binance identities belong to the rotated CA, so alpha slices are alpha-okx only"},
    "hot_reads": {"driver": "open loop, fixed schedule (no coordinated omission), TS identity",
                  "request": "GET /v2/market-data/{uid}/snapshot, TS r11 QUOTE and TRADE requirements (EXECUTION), round robin",
                  "rate_hz": 10.0,
                  "rate_note": "4x the production rate so that a ~150 ms maintenance run overlaps reads (about 1.5 expected per run)",
                  "production_reference": "R4 soak 2026-10-20: 7261 QUOTE + 1800 TRADE in 60 min = 2.5/s over two replicas",
                  "phases": "pre (60 s before burst 1), scored (first burst start .. last burst end), post (60 s)"},
    "maintenance": "the new image's default QDL_QUERY_MEMORY_SOFT_LIMIT_MIB (448); no manual gc/trim in any scored arm",
    "rules": {
        "HR1": "paired deltas over the pairs (A1,B1) and (A2,B2), scored hot reads (any HTTP status; transport errors counted apart): mean(p95_Bi - p95_Ai) <= max(0.10*mean(p95_Ai), 10 ms) and mean(p99_Bi - p99_Ai) <= max(0.10*mean(p99_Ai), 25 ms); each pair is reported",
        "HR2": "coverage: the B arms together have >= 1 memory_maintenance run inside a scored window with >= 1 hot read overlapping it; otherwise INCONCLUSIVE (never PASS)",
        "HR3": "bounded pause: every hot read overlapping a maintenance run has latency <= run_ms + p99 of that arm's non-overlapping scored reads + 50 ms, and every run_ms <= 500",
        "HR4": "transport errors + HTTP 5xx summed over B arms <= summed over A arms + max(2, 0.5 % of the B arms' scored reads)",
        "HR5": "non-200 share of scored reads over the B arms <= over the A arms + 1 percentage point",
        "BC": "for every TS r11 requirement, in each of 2 passes, a valid triple (A, B, A' = second request to A, issued "
              "together with the sync paused) exists within 3 attempts; valid = A and A' differ only in scalar leaves "
              "(V); PASS when for every valid triple: status A == B, content-type A == B, Content-Length header == body "
              "length on A and B, no Transfer-Encoding, the JSON leaf paths where A and B differ are a subset of V, and "
              "the bodies are byte-equal after replacing the raw values at V by one placeholder",
        "verdict": "PASS only when HR1-HR5 and BC all PASS; INCONCLUSIVE when HR2 is not met; FAIL otherwise",
        "secondary": "RSS per burst, warmup latency, 429 counts and failed slices are reported, not gated"},
}

PLAN = r'''
import json, sys, yaml
realm = "/consumer-realms/"
ts = yaml.safe_load(open(realm + "trading-system.paper.stable-r11.yaml"))["spec"]["requirements"]
okx = yaml.safe_load(open(realm + "alpha.okx.paper.stable-r15.yaml"))["spec"]["requirements"]
order = {"1m": 0, "3m": 1, "5m": 2, "15m": 3, "30m": 4, "1h": 5}
bars = [{"consumer": "trading-system.paper.stable", "requirement": r} for r in ts if r["feed"] == "BAR"]
bars += sorted(({"consumer": "alpha.okx.paper.stable", "requirement": r} for r in okx
                if r["feed"] == "BAR" and r.get("interval") in order),
               key=lambda e: (order[e["requirement"]["interval"]], e["requirement"]["instrument_uid"]))
json.dump({"ts_r11": ts, "bars": bars}, sys.stdout)
'''

SYNC = r'''
"""Read-only copy of market-cache keys into the private cache, kept fresh; pauses on /work/pause."""
import json, os, sys, time
import redis
from qdl.adapters.intervals import canonical_interval_ms
from qdl.query import DataRequirement
from qdl.projection.kn_state_codec import state_partition
from qdl.runtime.kn_bar_readback import BUCKET_OPENS, binding_product_key
from qdl.runtime.kn_market_cache import KnMarketCacheReader
from qdl.runtime.stable_catalog import StableSourceCatalog

plan = json.load(open("/work/plan.json"))
src = redis.Redis.from_url(os.environ["SRC_URL"], socket_timeout=5)
dst = redis.Redis.from_url(os.environ["DST_URL"], socket_timeout=5)
catalog = StableSourceCatalog.load("/runtime/stable-source-bindings.yaml")
reader = KnMarketCacheReader(src, "paper")
stats = {"keys": 0, "bytes": 0, "missing": 0}

def atomic(path, text):
    with open(path + ".tmp", "w") as handle:
        handle.write(text)
    os.replace(path + ".tmp", path)

def copy(key):
    blob = src.dump(key)
    if blob is None:
        stats["missing"] += 1
        return False
    dst.restore(key, 0, blob, replace=True)
    stats["keys"] += 1
    stats["bytes"] += len(blob)
    return True

def lpk_of(item):
    binding = catalog.binding_for(DataRequirement.from_mapping({**item, "warmup_limit": 0}))
    return binding, binding_product_key(binding, "paper")

generations, ckpts = {}, set()

def copy_bars(entry, rows_needed, tail_only):
    binding, lpk = lpk_of(entry["requirement"])
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
    tail = tail_only and generations.get(code) == generation
    generations[code] = generation
    first_needed = last - (2 * BUCKET_OPENS if tail else rows_needed + 200) * step
    for key in (reader.pointer_key(lpk), f"kn3:paper:src:{code}", f"kn3:paper:bm:{generation}:{code}"):
        copy(key)
    for bucket in range(first_needed // (BUCKET_OPENS * step), last // (BUCKET_OPENS * step) + 1):
        for kind in ("b", "bd", "bs"):
            copy(f"kn3:paper:{kind}:{generation}:{code}:{bucket}")
    ckpts.add(reader.checkpoint_key(reader.bars_topic, state_partition(lpk, reader.bars_partitions)))
    return {**entry, "binding_id": binding.binding_id, "retained_rows": int(meta["rows"]), "copied_rows": rows_needed}

def copy_latest(lpk):
    ptr = reader.pointer_key(lpk)
    ready = src.hmget(ptr, "ready")[0]
    if ready is None:
        return False
    copy(ptr)
    ok = copy(f"kn3:paper:l:{int(ready)}:{lpk.encode()}")
    copy(reader.checkpoint_key(reader.latest_topic, state_partition(lpk, reader.latest_partitions)))
    return ok

chosen = []
for entry in plan["bars"]:
    rows = 10000 if entry["consumer"] != "trading-system.paper.stable" else None
    got = None
    for need in ((rows,) if rows else (10000, 1000)):
        got = copy_bars(entry, need, False)
        if got is not None:
            break
    if got is not None:
        chosen.append(got)
latest = []
for item in plan["ts_r11"]:
    if item["feed"] == "BAR":
        continue
    try:
        _binding, lpk = lpk_of(item)
    except Exception as error:
        latest.append({"requirement": item, "lpk": None, "copied": False, "error": type(error).__name__})
        continue
    latest.append({"requirement": item, "lpk": lpk, "copied": copy_latest(lpk)})
for key in sorted(ckpts):
    copy(key)
atomic("/work/products.json", json.dumps(chosen))
atomic("/work/latest.json", json.dumps([{**e, "lpk": e["lpk"].encode() if e["lpk"] else None} for e in latest]))
print(json.dumps({"bars": len(chosen), "latest": sum(1 for e in latest if e["copied"]), **stats}), flush=True)
atomic("/work/sync-ready", "1")
deadline = time.monotonic() + float(os.environ["SYNC_DEADLINE_SECONDS"])
last_bars, rnd = time.monotonic(), 0
while time.monotonic() < deadline and not os.path.exists("/work/stop"):
    if os.path.exists("/work/pause"):
        atomic("/work/paused", str(rnd))
        time.sleep(0.02)
        continue
    for entry in latest:
        if entry["lpk"] is not None:
            copy_latest(entry["lpk"])
    if time.monotonic() - last_bars >= 15:
        for entry in chosen:
            copy_bars(entry, entry["copied_rows"], True)
        for key in sorted(ckpts):
            copy(key)
        last_bars = time.monotonic()
    rnd += 1
    if rnd % 120 == 0:
        print(json.dumps({"round": rnd, **stats}), flush=True)
    time.sleep(0.5)
print(json.dumps({"final_round": rnd, **stats}), flush=True)
'''

COMMON = r'''
import asyncio, hashlib, json, os, re, sys, time
import httpx
from qdl_sdk.credentials import RotatingJwtCredentialProvider
from qdl_sdk.errors import DataLayerError
from qdl_sdk.models import (BarRevisionPolicy, DataRequirement, Feed, GapPolicy, Grade, RecoveryPolicy, StalePolicy)
from qdl_sdk.tls import WorkloadTlsConfig
from qdl_sdk.transport import RestQueryTransport, REPLICA_LANE_BUSY

ISSUER, AUDIENCE = "https://identity.qdl.stable.internal", "qdl-v2-stable"
ROLES = ("historical_reader", "market_data_reader", "stream_consumer")
IDS = {"trading-system.paper.stable": ("trading-system", "stable-trading-system-rs256-v1",
                                       "spiffe://qdl/paper/trading-system-stable", 11),
       "alpha.okx.paper.stable": ("alpha-okx", "stable-alpha-okx-rs256-v1", "spiffe://qdl/paper/alpha-okx-stable", 15)}
ENUMS = {"feed": Feed, "consumer_grade": Grade, "stale_policy": StalePolicy, "gap_policy": GapPolicy,
         "recovery": RecoveryPolicy, "bar_revision_policy": BarRevisionPolicy, "event_recency_policy": StalePolicy}

def requirement(item, rows=None):
    fields = {k: (ENUMS[k](v) if k in ENUMS and v is not None else v) for k, v in item.items()}
    if rows is not None:
        fields["warmup_limit"] = rows
    return DataRequirement(**fields)

def transport(consumer, url, timeout=10.0):
    slug, key_id, subject, revision = IDS[consumer]
    tls = WorkloadTlsConfig("/identities/trading-system/ca.crt", f"/identities/{slug}/client.crt",
                            f"/identities/{slug}/client.key")
    cred = RotatingJwtCredentialProvider(private_key_file=f"/identities/{slug}-jwt/private.key", key_id=key_id,
        algorithm="RS256", issuer=ISSUER, audience=AUDIENCE, subject=subject, environment="paper", roles=ROLES,
        venues=("BINANCE", "OKX"), consumer_manifest_revision=revision, lifetime_seconds=600,
        refresh_before_seconds=120)
    return RestQueryTransport(url, timeout_seconds=timeout, credential_provider=cred, tls=tls)

async def wait_live(t, seconds=600):
    deadline = time.monotonic() + seconds
    while True:
        try:
            await t._client.get("/health/live")
            return
        except httpx.HTTPError:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(2)

def now_ms():
    return time.time_ns() / 1e6
'''

CMP_DRIVER = COMMON + r'''
cfg = json.load(open("/work/cmp.json"))
CONSUMER = "trading-system.paper.stable"

def atomic(path, text):
    with open(path + ".tmp", "w") as handle:
        handle.write(text)
    os.replace(path + ".tmp", path)

async def pause():
    for stale in ("/work/paused",):
        if os.path.exists(stale):
            os.unlink(stale)
    atomic("/work/pause", "1")
    deadline = time.monotonic() + 10
    while not os.path.exists("/work/paused"):
        if time.monotonic() > deadline:
            raise RuntimeError("sync did not acknowledge the pause")
        await asyncio.sleep(0.02)

def resume():
    for path in ("/work/pause", "/work/paused"):
        if os.path.exists(path):
            os.unlink(path)

async def fetch(t, req):
    headers = await t._headers(req, CONSUMER)
    t0 = time.perf_counter()
    response = await t._client.get(f"/v2/market-data/{req.instrument_uid}/warmup", params=req.query_params(),
                                   headers=headers)
    body = response.content
    return {"status": response.status_code, "ms": round((time.perf_counter() - t0) * 1e3, 1),
            "content_length": response.headers.get("content-length"),
            "transfer_encoding": response.headers.get("transfer-encoding"),
            "content_type": response.headers.get("content-type"), "body": body}

def leaves(value, path=()):
    if isinstance(value, dict):
        out = {("__keys__",) + path: sorted(value)}
        for k, v in value.items():
            out.update(leaves(v, path + (k,)))
        return out
    if isinstance(value, list):
        out = {("__len__",) + path: len(value)}
        for i, v in enumerate(value):
            out.update(leaves(v, path + (i,)))
        return out
    return {path: value}

def diff(a, b):
    la, lb = leaves(a), leaves(b)
    return sorted({repr(k) for k in set(la) | set(lb) if la.get(k, "<absent>") != lb.get(k, "<absent>")})

def mask(raw, values):
    for value in sorted(values, key=lambda v: -len(v)):
        raw = raw.replace(value, b'"<V>"')
    return raw

async def main():
    ta = transport(CONSUMER, cfg["urls"]["A"], timeout=60.0)
    tb = transport(CONSUMER, cfg["urls"]["B"], timeout=60.0)
    await wait_live(ta)
    await wait_live(tb)
    await asyncio.sleep(5)
    out = []
    for pass_no in range(1, cfg["passes"] + 1):
        for index, item in enumerate(cfg["requirements"]):
            req = requirement(item)
            record = {"pass": pass_no, "index": index, "instrument_uid": item["instrument_uid"], "feed": item["feed"],
                      "interval": item.get("interval"), "warmup_limit": item.get("warmup_limit"), "attempts": []}
            for attempt in range(1, 4):
                await pause()
                try:
                    a, b, a2 = await asyncio.gather(fetch(ta, req), fetch(tb, req), fetch(ta, req))
                finally:
                    resume()
                parsed = {}
                for name, r in (("A", a), ("B", b), ("A2", a2)):
                    try:
                        parsed[name] = json.loads(r["body"])
                    except ValueError:
                        parsed[name] = None
                    digest = hashlib.sha256(r["body"]).hexdigest()
                    path = f"/out/bodies/{digest}.json"
                    if not os.path.exists(path):
                        with open(path, "wb") as handle:
                            handle.write(r["body"])
                    r["sha256"], r["length"] = digest, len(r["body"])
                v_paths = diff(parsed["A"], parsed["A2"]) if None not in (parsed["A"], parsed["A2"]) else ["<unparsed>"]
                structural = [p for p in v_paths if "__len__" in p or "__keys__" in p or p == "<unparsed>"]
                ab_paths = diff(parsed["A"], parsed["B"]) if None not in (parsed["A"], parsed["B"]) else ["<unparsed>"]
                la, lb, la2 = (leaves(parsed[n]) if parsed[n] is not None else {} for n in ("A", "B", "A2"))
                volatile_values = set()
                for p in v_paths:
                    key = eval(p) if p.startswith("(") else None  # our own repr of a tuple path
                    for source in (la, lb, la2):
                        value = source.get(key) if key is not None else None
                        # mask only values that cannot collide with row content: strings, and ints of >= 10 digits
                        if isinstance(value, str) or (isinstance(value, int) and not isinstance(value, bool)
                                                      and abs(value) >= 10**9):
                            volatile_values.add(json.dumps(value).encode())
                na, nb = mask(a["body"], volatile_values), mask(b["body"], volatile_values)
                entry = {"attempt": attempt,
                         **{f"{n}_{k}": r[k] for n, r in (("A", a), ("B", b), ("A2", a2))
                            for k in ("status", "ms", "content_length", "transfer_encoding", "content_type", "sha256", "length")},
                         "volatile_paths": v_paths, "a_vs_b_paths": ab_paths,
                         "valid": not structural,
                         "normalized_equal": na == nb,
                         "normalized_sha256": {"A": hashlib.sha256(na).hexdigest(), "B": hashlib.sha256(nb).hexdigest()},
                         "rows": len(parsed["A"].get("data", [])) if isinstance(parsed["A"], dict) else None}
                record["attempts"].append(entry)
                if entry["valid"]:
                    break
                await asyncio.sleep(1.0)
            out.append(record)
            print(json.dumps({"pass": pass_no, "index": index, "feed": item["feed"],
                              "status": [record["attempts"][-1]["A_status"], record["attempts"][-1]["B_status"]],
                              "valid": record["attempts"][-1]["valid"],
                              "equal": record["attempts"][-1]["normalized_equal"]}), flush=True)
            await asyncio.sleep(1.2)
    json.dump(out, open("/out/byte-compare-raw.json", "w"), indent=1)
    await ta.close()
    await tb.close()

asyncio.run(main())
'''

BURST_DRIVER = COMMON + r'''
cfg = json.load(open("/work/burst.json"))
seq = 0

def event(kind, **kw):
    with open("/work/events.jsonl", "a") as handle:
        handle.write(json.dumps({"kind": kind, "unix_ms": now_ms(), **kw}) + "\n")

def probe(action, **kw):
    global seq
    seq += 1
    name = f"{seq:05d}"
    tmp = f"/probe/tmpcmd-{name}"
    with open(tmp, "w") as handle:
        json.dump({"action": action, **kw}, handle)
    os.rename(tmp, f"/probe/cmd-{name}.json")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        out = f"/probe/out-{name}.json"
        if os.path.exists(out):
            reply = json.load(open(out))
            os.unlink(out)
            return reply
        time.sleep(0.1)
    return {"ok": False, "error": "probe timeout"}

async def slice_warmup(t, consumer, req, out):
    started = time.perf_counter()
    for round_ in range(cfg["rounds"]):
        for attempt in range(3):
            error = None
            for redirect in (True, False):
                t0 = time.perf_counter()
                try:
                    payload = await t.warmup(req, consumer_id=consumer, replica_redirect=redirect)
                except DataLayerError as caught:
                    out["requests"].append(["429" if caught.code == "RATE_LIMITED" else caught.code,
                                            round((time.perf_counter() - t0) * 1e3)])
                    error = caught
                    if redirect and caught.code == "RATE_LIMITED" and caught.reason == REPLICA_LANE_BUSY:
                        continue
                    break
                except httpx.HTTPError as caught:
                    out["requests"].append(["TRANSPORT", round((time.perf_counter() - t0) * 1e3)])
                    error = caught
                    break
                out["requests"].append(["200", round((time.perf_counter() - t0) * 1e3)])
                return
            if not isinstance(error, DataLayerError) or error.code != "RATE_LIMITED" or not error.retryable:
                break
            await asyncio.sleep(max(250 * (attempt + 1), error.retry_after_ms or 0) / 1000)
        await asyncio.sleep(1.0)
    out["failed"] += 1

async def main():
    transports = {c: transport(c, cfg["url"]) for c in IDS}
    reqs = [(p["consumer"], requirement(p["requirement"], p["rows"])) for p in cfg["slices"]]
    await wait_live(transports["trading-system.paper.stable"])
    while not os.path.exists("/work/go"):
        await asyncio.sleep(0.2)
    event("baseline", stats=probe("stats"))
    for burst in range(1, cfg["bursts"] + 1):
        out = {"requests": [], "failed": 0}
        event("burst_start", burst=burst)
        t0 = time.perf_counter()
        await asyncio.gather(*(slice_warmup(transports[c], c, r, out) for c, r in reqs))
        wall = round(time.perf_counter() - t0, 1)
        event("burst_end", burst=burst)
        await asyncio.sleep(cfg["settle_s"])
        statuses = {}
        for status, _ in out["requests"]:
            statuses[status] = statuses.get(status, 0) + 1
        ok = sorted(ms for s, ms in out["requests"] if s == "200")
        stats = probe("stats")
        result = stats.get("result", {})
        event("burst_stats", burst=burst, wall_s=wall, statuses=statuses, failed_slices=out["failed"],
              req200_ms_p50=ok[len(ok) // 2] if ok else None, rss_mib=result.get("rss_mib"),
              uordblks_mib=(result.get("mallinfo2") or {}).get("uordblks_mib"))
        print(json.dumps({"burst": burst, "wall_s": wall, "statuses": statuses, "failed": out["failed"],
                          "rss_mib": result.get("rss_mib")}), flush=True)
        await asyncio.sleep(cfg["gap_s"])
    event("done")
    for t in transports.values():
        await t.close()

asyncio.run(main())
'''

HOT_DRIVER = COMMON + r'''
cfg = json.load(open("/work/hot.json"))
CONSUMER = "trading-system.paper.stable"

async def one(t, req, scheduled_ms, records):
    headers = await t._headers(req, CONSUMER)
    params = req.query_params()
    params.pop("limit", None)
    sent = now_ms()
    t0 = time.perf_counter()
    try:
        response = await t._client.get(f"/v2/market-data/{req.instrument_uid}/snapshot", params=params,
                                       headers=headers)
        status = response.status_code
        code = None
        if status != 200:
            try:
                body = response.json()
                code = (body.get("error") or body).get("code") if isinstance(body, dict) else None
            except ValueError:
                code = None
    except httpx.HTTPError as caught:
        status, code = "TRANSPORT", type(caught).__name__
    records.append([round(scheduled_ms, 1), round(sent, 1), round((time.perf_counter() - t0) * 1e3, 3), status, code,
                    req.feed.value if hasattr(req.feed, "value") else str(req.feed)])

async def main():
    t = transport(CONSUMER, cfg["url"], timeout=10.0)
    reqs = [requirement(item) for item in cfg["requirements"]]
    await wait_live(t)
    await t._headers(reqs[0], CONSUMER)
    records, tasks = [], set()
    period = 1.0 / cfg["rate_hz"]
    open("/work/hot-started", "w").write(str(now_ms()))
    base = time.perf_counter()
    base_ms = now_ms()
    i = 0
    while not os.path.exists("/work/hot-stop"):
        target = base + i * period
        delay = target - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        task = asyncio.create_task(one(t, reqs[i % len(reqs)], base_ms + i * period * 1e3, records))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        i += 1
    if tasks:
        await asyncio.wait(tasks, timeout=15)
    with open("/work/hot.jsonl", "w") as handle:
        for row in records:
            handle.write(json.dumps(row) + "\n")
    await t.close()

asyncio.run(main())
'''


def sh(*args, check=True, timeout=180):
    result = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"{args[:4]} failed: {result.stderr.strip()[:600]}")
    return result.stdout.strip()


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def require_lock(owner: str):
    try:
        lock = json.loads(LOCK.read_text())
    except (OSError, ValueError):
        raise SystemExit("host measurement lock is absent: acquire it first (AGENTS.md 34a)")
    if lock.get("owner") != owner:
        raise SystemExit(f"host measurement lock is held by {lock.get('owner')}, not {owner}")


def prod_spec():
    raw = json.loads(sh("docker", "inspect", PROD_QUERY))[0]
    # nested binds count too: the v2.2.5 compose binds the new generation's query-client-ca-bundle.crt inside
    # /consumer-realms (attempt 1 of 2026-10-20 dropped it and the replica refused the new TS client certificate)
    mounts = {m["Destination"]: m["Source"] for m in raw["Mounts"]
              if m["Destination"] in MOUNTS or any(m["Destination"].startswith(top + "/") for top in MOUNTS)}
    # a volume mount with a subpath (v2.2.5 compose: /session-liveness = stable_state:runtime/session-liveness) shows
    # only the volume root in .Mounts; attempt 2 bound the root and every QUOTE/TRADE read was SESSION_STATE stale
    for spec in raw["HostConfig"].get("Mounts") or []:
        subpath = (spec.get("VolumeOptions") or {}).get("Subpath")
        if subpath and spec.get("Target") in mounts:
            mounts[spec["Target"]] = mounts[spec["Target"]].rstrip("/") + "/" + subpath
    if not set(MOUNTS) <= set(mounts):
        raise RuntimeError(f"production Query mounts differ: {sorted(mounts)}")
    env = dict(item.split("=", 1) for item in raw["Config"]["Env"])
    return env, mounts, raw["Config"]["Cmd"]


def ro(mounts: dict) -> list[str]:
    return [a for dst, src in sorted(mounts.items()) for a in ("--mount", f"type=bind,src={src},dst={dst},readonly")]


def writable_dir(path: Path) -> Path:
    path.mkdir(mode=0o777, parents=True, exist_ok=True)
    os.chmod(path, 0o777)
    return path


def driver_identities(okx: bool) -> list[str]:
    items = {"trading-system": TS_TLS, "trading-system-jwt": TS_JWT}
    if okx:
        items.update({"alpha-okx": OKX_TLS, "alpha-okx-jwt": OKX_JWT})
    return ro({f"/identities/{k}": v for k, v in items.items()})


def driver_cmd(name: str, work: Path, code: str, *, okx=False, cpus="1.0", extra=()) -> list[str]:
    return ["docker", "run", "--rm", "--name", PREFIX + name, "--label", LABEL, "--network", NETWORK,
            "--user", "0:0", "--cap-drop", "ALL", "--cap-add", "DAC_READ_SEARCH", "--read-only",
            "--security-opt", "no-new-privileges:true", "--cpus", cpus, "--memory", "1g",
            "-e", "PYTHONPATH=/app", *driver_identities(okx), "--mount", f"type=bind,src={work},dst=/work",
            *extra, OLD_IMAGE, "python", "-c", code]


def start_query(name: str, alias: str, image: str, arm_dir: Path, env: dict, mounts: dict, cmd: list) -> str:
    state, probe_dir = arm_dir / "state", writable_dir(arm_dir / "probe")
    for path in (state, state / "runtime", state / "shared"):
        writable_dir(path)
    link = state / "runtime/session-liveness"
    if not link.is_symlink():
        link.symlink_to("/session-liveness")
    probe_src = arm_dir / "rssprobe"
    probe_src.mkdir(mode=0o755, exist_ok=True)
    (probe_src / "sitecustomize.py").write_text(harness.PROBE)
    os.chmod(probe_src / "sitecustomize.py", 0o644)
    env = dict(env)
    env.update({
        "QDL_STABLE_REDIS_URL": f"redis://{PREFIX}quota:6379/0",
        "QDL_KN3_MARKET_CACHE_URL": f"redis://{PREFIX}cache:6379/0",
        "QDL_STABLE_INSTANCE_ID": f"hr-{name}", "QDL_STABLE_CONSUMER_GROUP": f"hr-{name}",
        "QDL_STABLE_AUDIT_PATH": "/var/lib/qdl-kn/runtime/hr-query-audit.jsonl",
        "PYTHONPATH": "/rssprobe:" + env.get("PYTHONPATH", "/app"), "QDL_RSS_TRACEMALLOC": "0",
    })
    env_file = arm_dir / f".env-{name}"
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        for key, value in sorted(env.items()):
            if "\n" in value:
                raise RuntimeError(f"env {key} spans lines")
            handle.write(f"{key}={value}\n")
    try:
        sh("docker", "create", "--rm", "--name", PREFIX + name, "--label", LABEL, "--network", NETWORK,
           "--network-alias", alias, "--user", "10001:10001", "--read-only",
           "--tmpfs", "/tmp:rw,noexec,nosuid,size=67108864", "--cpus", "1.0", "--memory", "2g",
           "--pids-limit", "256", "--security-opt", "no-new-privileges:true", "--cap-drop", "ALL",
           "--log-opt", "max-size=50m", "--env-file", str(env_file), *ro(mounts),
           "--mount", f"type=bind,src={state},dst=/var/lib/qdl-kn", "--mount", f"type=bind,src={probe_dir},dst=/probe",
           "--mount", f"type=bind,src={probe_src},dst=/rssprobe,readonly", image, *cmd)
    finally:
        env_file.unlink(missing_ok=True)
    sh("docker", "start", PREFIX + name)
    return PREFIX + name


def stop_query(name: str, arm_dir: Path):
    logs = subprocess.run(["docker", "logs", "-t", name], capture_output=True, text=True, timeout=120)
    (arm_dir / f"{name}.log").write_text(logs.stdout + logs.stderr)
    inspect = subprocess.run(["docker", "inspect", "-f", "{{.Image}} {{.RestartCount}} {{.State.OOMKilled}}", name],
                             capture_output=True, text=True, timeout=30)
    (arm_dir / f"{name}.inspect").write_text(inspect.stdout)
    sh("docker", "rm", "-f", name, check=False)


def wait_file(path: Path, seconds: float):
    deadline = time.time() + seconds
    while not path.exists():
        if time.time() > deadline:
            raise RuntimeError(f"timeout waiting for {path}")
        time.sleep(0.5)


def slices(products: list) -> list:
    ts = [p for p in products if p["consumer"] == TS]
    okx = [p for p in products if p["consumer"] == OKX]
    out = [{**p, "rows": 1000} for p in ts]
    i = 0
    while len(out) < PREREG["burst"]["slices"] and okx:
        out.append({**okx[i % len(okx)], "rows": harness.STAGE_ALPHA_ROWS[i % len(harness.STAGE_ALPHA_ROWS)]})
        i += 1
    return out


def run_arm(label: str, image: str, output: Path, work_root: Path, env, mounts, cmd, plan, products) -> dict:
    arm_dir = output / f"arm-{label}"
    arm_dir.mkdir(mode=0o700)
    work = writable_dir(work_root / f"arm-{label}")
    meta = {"arm": label, "image": image, "started_utc": utc()}
    query = start_query(f"q-{label}", "query_v2_1", image, arm_dir, env, mounts, cmd)
    hot_reqs = [r for r in plan["ts_r11"] if r["feed"] in ("QUOTE", "TRADE")]
    (work / "hot.json").write_text(json.dumps({"url": "https://query_v2_1:8200", "rate_hz": PREREG["hot_reads"]["rate_hz"],
                                               "requirements": hot_reqs}))
    burst_slices = slices(products)
    meta["slices"] = len(burst_slices)
    (work / "burst.json").write_text(json.dumps({"url": "https://query_v2_1:8200", "slices": burst_slices,
                                                 **{k: PREREG["burst"][k] for k in ("bursts", "gap_s", "settle_s", "rounds")}}))
    probe_dir = arm_dir / "probe"
    hot = subprocess.Popen(driver_cmd(f"hot-{label}", work, HOT_DRIVER, cpus="0.5"),
                           stdout=open(arm_dir / "hot.out", "w"), stderr=subprocess.STDOUT)
    burst = None
    try:
        wait_file(work / "hot-started", 300)
        burst = subprocess.Popen(driver_cmd(f"burst-{label}", work, BURST_DRIVER, okx=True,
                                            extra=("--mount", f"type=bind,src={probe_dir},dst=/probe")),
                                 stdout=open(arm_dir / "burst.out", "w"), stderr=subprocess.STDOUT)
        time.sleep(60)  # pre phase: hot reads without bursts
        (work / "go").write_text("1")
        burst.wait(timeout=PREREG["burst"]["bursts"] * 200 + 600)
        meta["burst_rc"] = burst.returncode
        time.sleep(60)  # post phase
    finally:
        (work / "hot-stop").write_text("1")
        try:
            hot.wait(timeout=120)
        except subprocess.TimeoutExpired:
            sh("docker", "rm", "-f", PREFIX + f"hot-{label}", check=False)
        if burst is not None and burst.poll() is None:
            sh("docker", "rm", "-f", PREFIX + f"burst-{label}", check=False)
        stop_query(query, arm_dir)
        for name in ("hot.jsonl", "events.jsonl"):
            if (work / name).exists():
                (arm_dir / name).write_bytes((work / name).read_bytes())
        meta["ended_utc"] = utc()
        (arm_dir / "run.json").write_text(json.dumps(meta, indent=1) + "\n")
    return meta


def foreign_sample() -> dict:
    names = sh("docker", "ps", "--format", "{{.Names}}|{{.Status}}", check=False).splitlines()
    ours = [n for n in names if n.startswith(PREFIX)]
    sample = {"utc": utc(), "unix_ms": time.time_ns() / 1e6, "loadavg": Path("/proc/loadavg").read_text().strip(),
              "running_containers": len(names), "ours": len(ours),
              "foreign_test_like": sorted(n.split("|")[0] for n in names if not n.startswith(PREFIX)
                                          and not n.startswith(STACK) and not n.startswith(("market_data_service",
                                                                                            "portal", "monitor")))}
    for kind in ("cpu", "memory", "io"):
        try:
            sample[f"psi_{kind}"] = Path(f"/proc/pressure/{kind}").read_text().splitlines()[0]
        except OSError:
            pass
    try:
        sample["lock"] = json.loads(LOCK.read_text())
    except (OSError, ValueError):
        sample["lock"] = None
    return sample


def sampler(path: Path, stop) -> None:
    with open(path, "a") as handle:
        while not stop.is_set():
            try:
                handle.write(json.dumps(foreign_sample()) + "\n")
                handle.flush()
            except Exception as error:  # sampling never breaks the run
                handle.write(json.dumps({"utc": utc(), "sampler_error": type(error).__name__}) + "\n")
            stop.wait(30)


def run(args) -> int:
    import threading
    if args.concurrent:
        try:
            lock = json.loads(LOCK.read_text())
        except (OSError, ValueError):
            lock = None
        print(json.dumps({"concurrent_with_lock": lock}), flush=True)
    else:
        require_lock(args.lock_owner)
    output = args.output.resolve()
    compare_only = bool(getattr(args, "compare_only", False))
    output.mkdir(mode=0o700, parents=True, exist_ok=compare_only)
    stop_sampler = threading.Event()
    threading.Thread(target=sampler, args=(output / "foreign.jsonl", stop_sampler), daemon=True).start()
    head = sh("git", "-C", str(ROOT), "rev-parse", "HEAD")
    prereg = {**PREREG, "written_utc": utc(), "tool": "scripts/v225_query_hot_read_ab.py", "tool_commit": head,
              "concurrent_mode": bool(args.concurrent),
              "tool_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    if compare_only:
        # re-run of the BC phase in the session dir of the arms (the first BC output was lost: /out not writable)
        if not (output / "prereg.json").exists():
            raise SystemExit("compare-only needs the session's prereg.json")
        (output / "compare-rerun.json").write_text(json.dumps({**prereg, "rerun_reason": args.compare_only_reason},
                                                              indent=1) + "\n")
    else:
        (output / "prereg.json").write_text(json.dumps(prereg, indent=1) + "\n")
    print(json.dumps({"prereg_sha256": hashlib.sha256((output / "prereg.json").read_bytes()).hexdigest()}), flush=True)
    env, mounts, cmd = prod_spec()
    work_root = writable_dir(output / ("work-cmp-rerun" if compare_only else "work"))
    writable_dir(output / "cmp-out")
    bodies = writable_dir(output / "cmp-out" / "bodies")
    sync = None
    try:
        sh("docker", "network", "create", "--internal", "--label", LABEL, NETWORK)
        for name, memory, maxmem in (("cache", "1g", "896mb"), ("quota", "128m", "96mb")):
            sh("docker", "run", "-d", "--rm", "--name", PREFIX + name, "--label", LABEL, "--network", NETWORK,
               "--cpus", "0.5", "--memory", memory, "--read-only", "--tmpfs", "/data:size=64m", REDIS_IMAGE,
               "redis-server", "--save", "", "--appendonly", "no", "--maxmemory", maxmem)
        plan = json.loads(sh("docker", "run", "--rm", "--network", "none", "--user", "10001:10001", "--read-only",
                             "--cpus", "0.5", *ro({"/consumer-realms": mounts["/consumer-realms"]}), OLD_IMAGE,
                             "python", "-c", PLAN))
        (work_root / "plan.json").write_text(json.dumps(plan))
        sh("docker", "create", "--rm", "--name", PREFIX + "sync", "--label", LABEL, "--network", NETWORK,
           "--user", "10001:10001", "--read-only", "--cpus", "0.5", "--memory", "512m", "-e", "PYTHONPATH=/app",
           "-e", "SRC_URL=redis://market_cache:6379/0", "-e", f"DST_URL=redis://{PREFIX}cache:6379/0",
           "-e", "SYNC_DEADLINE_SECONDS=4200", *ro({"/runtime": mounts["/runtime"]}),
           "--mount", f"type=bind,src={work_root},dst=/work", OLD_IMAGE, "python", "-c", SYNC)
        sh("docker", "network", "connect", PROD_INTERNAL, PREFIX + "sync")
        sync = subprocess.Popen(["docker", "start", "-a", PREFIX + "sync"], stdout=open(output / "sync.out", "w"),
                                stderr=subprocess.STDOUT)
        wait_file(work_root / "sync-ready", 900)
        products = json.loads((work_root / "products.json").read_text())
        latest = json.loads((work_root / "latest.json").read_text())
        (output / "products.json").write_text(json.dumps({"bars": products, "latest": latest, "copied_utc": utc()},
                                                         indent=1) + "\n")
        # 1. byte compare: A and B side by side on the same cache, sync paused per triple
        cmp_dir = output / ("cmp-rerun" if compare_only else "cmp")
        cmp_dir.mkdir(mode=0o700)
        qa = start_query("q-cmp-a", "query_v2_1", OLD_IMAGE, cmp_dir, env, mounts, cmd)
        qb = start_query("q-cmp-b", "query_v2_2", NEW_IMAGE, cmp_dir, env, mounts, cmd)
        (work_root / "cmp.json").write_text(json.dumps({"urls": {"A": "https://query_v2_1:8200",
                                                                  "B": "https://query_v2_2:8200"},
                                                         "requirements": plan["ts_r11"], "passes": 2}))
        try:
            cmp_run = subprocess.run(driver_cmd("cmp", work_root, CMP_DRIVER, extra=(
                "--mount", f"type=bind,src={bodies.parent},dst=/out")), capture_output=True, text=True, timeout=1800)
            (cmp_dir / "driver.out").write_text(cmp_run.stdout[-200000:] + "\n--stderr--\n" + cmp_run.stderr[-20000:])
        finally:
            stop_query(qa, cmp_dir)
            stop_query(qb, cmp_dir)
        # 2. interleaved arms (paired deltas): A1 B1 A2 B2
        for label, image in (() if compare_only else (("A1", OLD_IMAGE), ("B1", NEW_IMAGE), ("A2", OLD_IMAGE), ("B2", NEW_IMAGE))):
            run_arm(label, image, output, work_root, env, mounts, cmd, plan, products)
    finally:
        stop_sampler.set()
        (work_root / "stop").write_text("1")
        if sync is not None:
            try:
                sync.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pass
        cleanup(None)
        with open(output / "foreign.jsonl", "a") as handle:
            handle.write(json.dumps({**foreign_sample(), "phase": "after_cleanup"}) + "\n")
    return 0


# ------------------------------------------------------------------------------------------------ analysis
def pct(values: list, q: float):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(-(-q * len(ordered) // 1)) - 1))]


def summary(rows: list) -> dict:
    lat = [r[2] for r in rows if r[3] != "TRANSPORT"]
    statuses = {}
    for r in rows:
        statuses[str(r[3])] = statuses.get(str(r[3]), 0) + 1
    return {"n": len(rows), "statuses": statuses,
            "p50_ms": pct(lat, 0.50), "p95_ms": pct(lat, 0.95), "p99_ms": pct(lat, 0.99), "max_ms": max(lat, default=None),
            "ok200": {"n": sum(1 for r in rows if r[3] == 200),
                      "p50_ms": pct([r[2] for r in rows if r[3] == 200], 0.50),
                      "p95_ms": pct([r[2] for r in rows if r[3] == 200], 0.95),
                      "p99_ms": pct([r[2] for r in rows if r[3] == 200], 0.99),
                      "max_ms": max((r[2] for r in rows if r[3] == 200), default=None)}}


def maintenance_runs(log_path: Path) -> list:
    runs = []
    if not log_path.exists():
        return runs
    for line in log_path.read_text().splitlines():
        if '"memory_maintenance"' in line and "qdl.stall" in line:
            record = json.loads(line[line.index("{"):])
            run_ms = record["gc_ms"] + record["trim_ms"]
            runs.append({**record, "run_ms": round(run_ms, 3), "start_ms": record["wall_ms"] - run_ms,
                         "end_ms": record["wall_ms"]})
    return runs


def arm_summary(arm_dir: Path) -> dict:
    rows = [json.loads(line) for line in (arm_dir / "hot.jsonl").read_text().splitlines() if line.strip()]
    events = [json.loads(line) for line in (arm_dir / "events.jsonl").read_text().splitlines() if line.strip()]
    starts = [e["unix_ms"] for e in events if e["kind"] == "burst_start"]
    ends = [e["unix_ms"] for e in events if e["kind"] == "burst_end"]
    window = (min(starts), max(ends))
    scored = [r for r in rows if window[0] <= r[1] <= window[1]]
    pre = [r for r in rows if r[1] < window[0]]
    post = [r for r in rows if r[1] > window[1]]
    runs = [r for r in maintenance_runs(next(arm_dir.glob("qdl-hr-q-*.log"), arm_dir / "none"))]
    overlap_ids = set()
    run_rows = []
    for run in runs:
        hits = [i for i, r in enumerate(scored) if r[1] <= run["end_ms"] and r[1] + r[2] >= run["start_ms"]]
        overlap_ids.update(hits)
        run_rows.append({k: run[k] for k in ("wall_ms", "rss_before_mib", "rss_after_mib", "gc_ms", "trim_ms",
                                             "run_ms", "collected")}
                        | {"in_scored_window": window[0] <= run["end_ms"] <= window[1] + 5000,
                           "overlapping_reads": len(hits),
                           "overlapping_latency_ms": sorted(round(scored[i][2], 1) for i in hits)})
    non_overlap = [r for i, r in enumerate(scored) if i not in overlap_ids]
    stats = [e for e in events if e["kind"] == "burst_stats"]
    return {"scored_window_ms": window, "pre": summary(pre), "scored": summary(scored), "post": summary(post),
            "scored_non_overlap": summary(non_overlap),
            "transport_or_5xx": sum(1 for r in scored if r[3] == "TRANSPORT" or (isinstance(r[3], int) and r[3] >= 500)),
            "non200_share": (sum(1 for r in scored if r[3] != 200) / len(scored)) if scored else None,
            "maintenance_runs": run_rows,
            "bursts": [{k: e.get(k) for k in ("burst", "wall_s", "statuses", "failed_slices", "req200_ms_p50",
                                              "rss_mib", "uordblks_mib")} for e in stats],
            "schedule_lag_ms_p99": pct([r[1] - r[0] for r in rows], 0.99)}


_TOKEN = None


def raw_mask(raw: bytes, paths: set) -> bytes:
    """Replace the raw JSON scalar token at each path in ``paths`` by ``"<V>"`` (path-exact, bytes elsewhere kept).

    The BC rule masks the values at the volatile paths V only; a global value replace would also hit equal numbers
    elsewhere (the driver's own mask therefore skipped small integers, which made per-request age fields unmasked).
    """
    import re
    global _TOKEN
    if _TOKEN is None:
        _TOKEN = re.compile(rb'"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}\[\]:,]|\s+')
    out, pos, stack = [], 0, []  # stack items: ["obj", key, expecting_key] or ["arr", index]
    for match in _TOKEN.finditer(raw):
        token = match.group()
        if token[:1].isspace():
            continue
        if token in (b"{", b"["):
            stack.append(["obj", None, True] if token == b"{" else ["arr", 0])
            continue
        if token in (b"}", b"]"):
            stack.pop()
            continue
        if token == b":":
            stack[-1][2] = False
            continue
        if token == b",":
            if stack and stack[-1][0] == "obj":
                stack[-1][2] = True
            elif stack:
                stack[-1][1] += 1
            continue
        if stack and stack[-1][0] == "obj" and stack[-1][2]:
            stack[-1][1] = json.loads(token)
            continue
        path = tuple(item[1] for item in stack)
        if path in paths:
            out.append(raw[pos:match.start()])
            out.append(b'"<V>"')
            pos = match.end()
    out.append(raw[pos:])
    return b"".join(out)


def byte_compare(output: Path) -> dict:
    import ast
    raw = json.loads((output / "cmp-out/byte-compare-raw.json").read_text())
    bodies = output / "cmp-out/bodies"
    for record in raw:
        final = record["attempts"][-1]
        paths = {ast.literal_eval(p) for p in final["volatile_paths"] if p.startswith("(")}
        a = (bodies / f'{final["A_sha256"]}.json').read_bytes()
        b = (bodies / f'{final["B_sha256"]}.json').read_bytes()
        na, nb = raw_mask(a, paths), raw_mask(b, paths)
        final["driver_string_mask_equal"] = final["normalized_equal"]
        final["normalized_equal"] = na == nb
        final["normalized_sha256"] = {"A": hashlib.sha256(na).hexdigest(), "B": hashlib.sha256(nb).hexdigest()}
    per_req, problems = {}, []
    for record in raw:
        final = record["attempts"][-1]
        key = f'{record["index"]:02d}:{record["feed"]}:{record["instrument_uid"]}'
        ok = bool(final["valid"])
        checks = {
            "valid_triple": final["valid"],
            "status_equal": final["A_status"] == final["B_status"],
            "content_type_equal": final["A_content_type"] == final["B_content_type"],
            "content_length_matches_body": (str(final["A_length"]) == str(final["A_content_length"])
                                            and str(final["B_length"]) == str(final["B_content_length"])),
            "no_transfer_encoding": final["A_transfer_encoding"] is None and final["B_transfer_encoding"] is None,
            "a_vs_b_within_volatile": set(final["a_vs_b_paths"]) <= set(final["volatile_paths"]),
            "normalized_bytes_equal": final["normalized_equal"],
        }
        ok = all(checks.values())
        if not ok:
            problems.append({"requirement": key, "pass": record["pass"], "failed": [k for k, v in checks.items() if not v]})
        per_req.setdefault(key, []).append({"pass": record["pass"], "attempts": len(record["attempts"]), "ok": ok,
                                            "status": [final["A_status"], final["B_status"]],
                                            "length": [final["A_length"], final["B_length"]],
                                            "content_length": [final["A_content_length"], final["B_content_length"]],
                                            "rows": final["rows"],
                                            "volatile_path_count": len(final["volatile_paths"]),
                                            "volatile_path_patterns": sorted({__import__("re").sub(r", \d+", ", *", v)
                                                                              for v in final["volatile_paths"]}),
                                            "sha256": {"A": final["A_sha256"], "B": final["B_sha256"]},
                                            "normalized_sha256": final["normalized_sha256"]})
    statuses = {}
    for entries in per_req.values():
        for e in entries:
            statuses[str(e["status"][0])] = statuses.get(str(e["status"][0]), 0) + 1
    volatile = sorted({p for entries in per_req.values() for e in entries for p in e["volatile_path_patterns"]})
    return {"requirements": len(per_req), "triples": len(raw), "status_counts_A": statuses,
            "volatile_paths_seen": volatile, "problems": problems,
            "verdict": "PASS" if not problems and len(per_req) == len({r["index"] for r in raw}) else "FAIL",
            "per_requirement": per_req}


def analyze(args) -> int:
    output = args.output.resolve()
    prereg = json.loads((output / "prereg.json").read_text())
    arms = {label: arm_summary(output / f"arm-{label}") for label in ("A1", "B1", "A2", "B2")}
    pairs = [("A1", "B1"), ("A2", "B2")]
    rules = {}
    per_pair = [{"pair": [a, b],
                 "p95": [arms[a]["scored"]["p95_ms"], arms[b]["scored"]["p95_ms"]],
                 "p99": [arms[a]["scored"]["p99_ms"], arms[b]["scored"]["p99_ms"]],
                 "p50": [arms[a]["scored"]["p50_ms"], arms[b]["scored"]["p50_ms"]],
                 "max": [arms[a]["scored"]["max_ms"], arms[b]["scored"]["max_ms"]],
                 "d_p95": arms[b]["scored"]["p95_ms"] - arms[a]["scored"]["p95_ms"],
                 "d_p99": arms[b]["scored"]["p99_ms"] - arms[a]["scored"]["p99_ms"]} for a, b in pairs]
    mean = lambda xs: sum(xs) / len(xs)
    d95, d99 = mean([p["d_p95"] for p in per_pair]), mean([p["d_p99"] for p in per_pair])
    a95, a99 = mean([p["p95"][0] for p in per_pair]), mean([p["p99"][0] for p in per_pair])
    rules["HR1"] = {"pairs": per_pair, "mean_d_p95": d95, "bound_p95": max(0.10 * a95, 10.0),
                    "mean_d_p99": d99, "bound_p99": max(0.10 * a99, 25.0)}
    rules["HR1"]["pass"] = d95 <= rules["HR1"]["bound_p95"] and d99 <= rules["HR1"]["bound_p99"]
    b_runs = [dict(r, arm=label) for label in ("B1", "B2") for r in arms[label]["maintenance_runs"]]
    covered = [r for r in b_runs if r["in_scored_window"] and r["overlapping_reads"] > 0]
    rules["HR2"] = {"runs_total": len(b_runs), "runs_covered": len(covered), "pass": bool(covered)}
    hr3 = []
    for label in ("B1", "B2"):
        extra = arms[label]["scored_non_overlap"]["p99_ms"] + 50
        for r in arms[label]["maintenance_runs"]:
            worst = max(r["overlapping_latency_ms"], default=0)
            hr3.append({"arm": label, "run_ms": r["run_ms"], "worst_overlapping_ms": worst, "bound_ms": r["run_ms"] + extra,
                        "pass": worst <= r["run_ms"] + extra and r["run_ms"] <= 500})
    rules["HR3"] = {"runs": hr3, "pass": bool(hr3) and all(x["pass"] for x in hr3)}
    errs = {g: sum(arms[l]["transport_or_5xx"] for l in labels) for g, labels in (("A", ("A1", "A2")), ("B", ("B1", "B2")))}
    n_b = sum(arms[l]["scored"]["n"] for l in ("B1", "B2"))
    allowed = errs["A"] + max(2, 0.005 * n_b)
    rules["HR4"] = {**errs, "allowed": allowed, "pass": errs["B"] <= allowed}
    def share(labels):
        n = sum(arms[l]["scored"]["n"] for l in labels)
        bad = sum(arms[l]["non200_share"] * arms[l]["scored"]["n"] for l in labels)
        return bad / n if n else None
    rules["HR5"] = {"A": share(("A1", "A2")), "B": share(("B1", "B2"))}
    rules["HR5"]["pass"] = rules["HR5"]["B"] <= rules["HR5"]["A"] + 0.01
    bc = byte_compare(output)
    rules["BC"] = {"verdict": bc["verdict"], "problems": bc["problems"], "pass": bc["verdict"] == "PASS"}
    if not rules["HR2"]["pass"]:
        verdict = "INCONCLUSIVE"
    else:
        verdict = "PASS" if all(rules[k]["pass"] for k in ("HR1", "HR2", "HR3", "HR4", "HR5", "BC")) else "FAIL"
    foreign = [json.loads(line) for line in (output / "foreign.jsonl").read_text().splitlines() if line.strip()] \
        if (output / "foreign.jsonl").exists() else []
    loads = [float(x["loadavg"].split()[0]) for x in foreign if "loadavg" in x]
    psi = [float(x["psi_cpu"].split("avg10=")[1].split()[0]) for x in foreign if "psi_cpu" in x]
    concurrency = {"mode": "CONCURRENT_WITH_FOREIGN_LOCK" if prereg.get("concurrent_mode") else "EXCLUSIVE_LOCK",
                   "samples": len(foreign), "load1_min_max": [min(loads, default=None), max(loads, default=None)],
                   "psi_cpu_some_avg10_min_max": [min(psi, default=None), max(psi, default=None)],
                   "locks_seen": sorted({json.dumps(x.get("lock"), sort_keys=True) for x in foreign}),
                   "foreign_test_like_containers_seen": sorted({n for x in foreign for n in x.get("foreign_test_like", [])})}
    receipt = {"schema": "qdl.v225.query-ab-receipt.v2", "verdict": verdict, "rules": rules, "concurrency": concurrency,
               "prereg_sha256": hashlib.sha256((output / "prereg.json").read_bytes()).hexdigest(),
               "prereg_written_utc": prereg["written_utc"], "tool_commit": prereg["tool_commit"],
               "arms": {label: {"image": OLD_IMAGE if label.startswith("A") else NEW_IMAGE, **arms[label]} for label in arms},
               "products": json.loads((output / "products.json").read_text()) if (output / "products.json").exists() else None,
               "analyzed_utc": utc()}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=1, default=str) + "\n")
    (output / "byte-compare.json").write_text(json.dumps({"schema": "qdl.v225.query-warmup-byte-compare.v1",
        "old_image": OLD_IMAGE, "new_image": NEW_IMAGE, "manifest": "trading-system.paper.stable r11",
        "prereg_rule": PREREG["rules"]["BC"], "prereg_sha256": receipt["prereg_sha256"], **bc}, indent=1) + "\n")
    print(json.dumps({"verdict": verdict, **{k: v["pass"] for k, v in rules.items()}, "HR1": rules["HR1"],
                      "maintenance_B": b_runs, "bc": bc["verdict"], "bc_problems": bc["problems"][:5],
                      "concurrency": concurrency}, indent=1, default=str))
    return 0


def cleanup(_args) -> int:
    names = [n for n in sh("docker", "ps", "-a", "--filter", f"label={LABEL}", "--format", "{{.Names}}",
                           check=False).splitlines() if n.startswith(PREFIX)]
    for name in names:
        sh("docker", "rm", "-f", name, check=False)
    nets = [n for n in sh("docker", "network", "ls", "--filter", f"label={LABEL}", "--format", "{{.Name}}",
                          check=False).splitlines() if n == NETWORK]
    for net in nets:
        sh("docker", "network", "rm", net, check=False)
    print(json.dumps({"removed_containers": names, "removed_networks": nets}), flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("run")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--lock-owner", default="claude-v225-query-ab")
    p.add_argument("--concurrent", action="store_true",
                   help="owner-instructed run beside another agent's correctness-only lock (never touches the lock)")
    p.add_argument("--compare-only", action="store_true", help="re-run only the BC phase inside an existing session dir")
    p.add_argument("--compare-only-reason", default="")
    p.set_defaults(func=run)
    p = sub.add_parser("analyze")
    p.add_argument("--output", type=Path, required=True)
    p.set_defaults(func=analyze)
    p = sub.add_parser("cleanup")
    p.set_defaults(func=cleanup)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""KN-1 K1.4 resource and retention sizing from real data, not estimates.

Purpose: measure what the Kafka-native design will hold and move - canonical
payload bytes per feed, rendered public rows, a compact BAR row encoding,
their real Redis memory including index overhead, the Data Layer CPU
denominator and Kafka bytes/s - so the candidate budget is frozen on numbers.

Boundary: read-only against production. The spool is opened ``mode=ro`` +
``query_only``; rendered rows come from a bounded copy in a tmpfs scratch
spool inside a ``--rm`` container; Redis measurements use a disposable Redis
of the production image digest with ``--network none``; CPU and Kafka figures
come from ``docker stats`` and ``kafka-log-dirs --describe``. Nothing is
written to production state.

  # inside qdl-v2-python, state volume read-only, tmpfs /tmp
  python -B scripts/kn_resource_sizing.py payloads --out payloads.json --sample sample.json
  # inside qdl-v2-python joined to a disposable Redis network namespace
  python -B scripts/kn_resource_sizing.py redis --sample sample.json --out redis.json
  # on the host
  python3 -B scripts/kn_resource_sizing.py runtime --minutes 10 --out runtime.json
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
SPOOL = "/state/shared/canonical-cache.sqlite3"
PROJECT = "qdl_v2_stable_candidate"
SCHEMA = "qdl.kn.v220.resource-sizing.v1"


def _pct(values: Sequence[int | float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return float(ordered[min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))])


def _dist(values: Sequence[int | float]) -> dict[str, float]:
    return {"n": len(values), "mean": round(statistics.fmean(values), 3) if values else 0.0,
            "p50": _pct(values, 0.5), "p95": _pct(values, 0.95), "max": float(max(values) if values else 0)}


# ------------------------------------------------------------------ payloads

def _compact_bar(envelope) -> bytes:
    """One immutable BAR row as a compact typed record (decimal strings kept exact)."""
    from qdl.runtime.stable_source import _decimal_text

    bar = envelope.bar
    row = {
        "o": _decimal_text(bar.open), "h": _decimal_text(bar.high), "l": _decimal_text(bar.low),
        "c": _decimal_text(bar.close), "v": _decimal_text(bar.volume),
        "qv": _decimal_text(bar.quote_volume) if bar.HasField("quote_volume") else None,
        "n": int(bar.trade_count), "ot": int(bar.open_time_ns), "ct": int(bar.close_time_ns),
        "f": bool(bar.is_final), "r": int(bar.revision),
        "st": int(envelope.source_event_time_ns), "rt": int(envelope.received_at_ns),
        "id": envelope.event_id.hex(),
    }
    return json.dumps(row, separators=(",", ":")).encode()


def payloads(sample_path: Path, rows_per_key: int, render_rows: int) -> dict[str, Any]:
    import sqlite3

    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.runtime.stable_catalog import StableSourceCatalog

    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    live = sqlite3.connect(f"file:{SPOOL}?mode=ro", uri=True, timeout=5)
    live.execute("PRAGMA query_only=ON")
    by_feed: dict[str, dict[str, list[int]]] = {}
    keys = [key for (key,) in live.execute(
        "SELECT partition_key FROM partitions WHERE stream=? ORDER BY partition_key", (catalog.canonical_stream,))]
    for key in keys:
        feed = key.split("/")[1]
        stats = by_feed.setdefault(feed, {"payload": [], "headers": []})
        for payload_len, headers_len in live.execute(
            "SELECT length(payload), length(headers_json) FROM events WHERE stream=? AND partition_key=? "
            "ORDER BY logical_offset DESC LIMIT ?", (catalog.canonical_stream, key, rows_per_key)):
            stats["payload"].append(payload_len)
            stats["headers"].append(headers_len)
    canonical = {feed: {"payload_bytes": _dist(v["payload"]), "header_bytes": _dist(v["headers"])}
                 for feed, v in sorted(by_feed.items())}
    live.close()

    rendered, sample = _render_samples(catalog, render_rows)
    manifests = [ConsumerManifestLoader.load(path) for path in sorted((ROOT / "consumers/stable").glob("*.yaml"))]
    demand: dict[str, int] = {}
    for manifest in manifests:
        for requirement in manifest.requirements:
            feed = getattr(requirement.feed, "value", str(requirement.feed))
            if feed != "BAR":
                continue
            try:
                binding = catalog.binding_for(requirement)
            except Exception:  # noqa: BLE001 - reference/pass-through products have no stable binding
                continue
            demand[binding.binding_id] = max(demand.get(binding.binding_id, 0), int(requirement.warmup_limit or 0))
    sample_path.write_text(json.dumps(sample), encoding="utf-8")
    return {"canonical_by_feed": canonical, "rendered": rendered,
            "bar_demand_rows": {"products": len(demand), "sum_rows": sum(demand.values()),
                                "by_limit": {str(k): sum(1 for v in demand.values() if v == k)
                                             for k in sorted(set(demand.values()))}}}


def _render_samples(catalog, render_rows: int) -> tuple[dict[str, Any], dict[str, Any]]:
    """Render real rows through the Query backend and router on a scratch spool copy."""
    import importlib
    import sqlite3

    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.marketdata.v2 import market_data_pb2
    from qdl.runtime.stable_capacity import STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW
    from qdl.runtime.stable_source import StableSpoolQueryBackend
    from qdl.transport import DurableEvent, SQLiteDurableSpool
    from qdl.transport.sqlite_spool import SpoolConfig

    router = importlib.import_module("qdl.api_v2.router")
    manifest = ConsumerManifestLoader.load(ROOT / "consumers/stable/alpha-okx-paper.yaml")
    wanted = {"BAR": "1m", "TRADE": None, "QUOTE": None, "MARK_INDEX_PRICE": None, "BOOK_SNAPSHOT": None}
    chosen = {}
    for requirement in manifest.requirements:
        feed = requirement.feed.value
        if feed in wanted and feed not in chosen and requirement.interval == wanted[feed]:
            try:
                chosen[feed] = (requirement, catalog.binding_for(requirement))
            except Exception:  # noqa: BLE001
                continue
    live = sqlite3.connect(f"file:{SPOOL}?mode=ro", uri=True, timeout=5)
    live.execute("PRAGMA query_only=ON")
    spool = SQLiteDurableSpool(SpoolConfig(
        path=Path("/tmp/kn-sizing/cache.sqlite3"), max_records=200_000, max_payload_bytes=1 << 30,
        max_storage_bytes=1 << 30, min_free_disk_bytes=1 << 20, retain_partition_windows=True,
        max_partition_records=STABLE_SPOOL_PHYSICAL_PARTITION_WINDOW))
    rendered: dict[str, Any] = {}
    sample: dict[str, Any] = {"bar": {}, "latest": {}}
    try:
        for feed, (requirement, binding) in sorted(chosen.items()):
            limit = render_rows if feed == "BAR" else 64
            rows = live.execute(
                "SELECT stream, partition_key, event_id, payload, accepted_at_ns, headers_json FROM events "
                "WHERE stream=? AND partition_key=? ORDER BY logical_offset DESC LIMIT ?",
                (catalog.canonical_stream, binding.partition_key, limit)).fetchall()
            rows.reverse()
            events = [DurableEvent(stream=s, partition_key=p, event_id=e, payload=pl, accepted_at_ns=a,
                                   headers=json.loads(h)) for s, p, e, pl, a, h in rows]
            for start in range(0, len(events), 1000):
                spool.append_many(events[start:start + 1000])
            backend = StableSpoolQueryBackend(spool, catalog, schema_digest="0" * 64)
            if feed == "BAR":
                sized = dataclasses.replace(requirement, warmup_limit=min(len(rows) - 1, 10_000))
                history = backend.history_many((sized,))[sized]
                if isinstance(history, Exception):
                    raise history
                items = history.items
            else:
                items = [backend.latest(requirement)]
            views = [router._market_item(item) for item in items]
            public = [json.dumps(view.model_dump(mode="json", by_alias=True), separators=(",", ":")).encode()
                      for view in views]
            protobufs = [pl for _s, _p, _e, pl, _a, _h in rows]
            entry: dict[str, Any] = {"binding": binding.binding_id, "items": len(public),
                                     "public_row_bytes": _dist([len(x) for x in public]),
                                     "canonical_row_bytes": _dist([len(x) for x in protobufs])}
            if feed == "BAR":
                compact = []
                for payload in protobufs:
                    envelope = market_data_pb2.EventEnvelope.FromString(payload)
                    if envelope.WhichOneof("payload") == "bar" and envelope.bar.is_final:
                        compact.append(_compact_bar(envelope))
                entry["compact_row_bytes"] = _dist([len(x) for x in compact])
                sample["bar"] = {
                    "binding": binding.binding_id,
                    "open_ms": [int(market_data_pb2.EventEnvelope.FromString(p).bar.open_time_ns // 1_000_000)
                                for p in protobufs],
                    "canonical": [p.hex() for p in protobufs],
                    "public": [x.decode() for x in public],
                    "compact": [x.decode() for x in compact],
                }
            else:
                sample["latest"][feed] = {"canonical": protobufs[-1].hex(), "public": public[-1].decode()}
            rendered[feed] = entry
    finally:
        spool.close()
        live.close()
    return rendered, sample


# --------------------------------------------------------------------- redis

def redis_memory(sample_path: Path, host: str, port: int) -> dict[str, Any]:
    import redis

    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    client = redis.Redis(host=host, port=port)
    client.flushall()  # disposable, isolated Redis only (network namespace of a --rm container)
    info0 = int(client.info("memory")["used_memory"])
    result: dict[str, Any] = {"redis_version": client.info("server")["redis_version"], "baseline_used": info0}
    bar = sample["bar"]
    rows = len(bar["open_ms"])
    for encoding in ("canonical", "public", "compact"):
        values = bar[encoding]
        count = min(rows, len(values))
        before = int(client.info("memory")["used_memory"])
        zkey, hkey = f"kn:z:{encoding}", f"kn:h:{encoding}"
        pipe = client.pipeline(transaction=False)
        for index in range(count):
            member = bytes.fromhex(values[index]) if encoding == "canonical" else values[index].encode()
            # Index: ZSET score = open time ms (exact below 2**53); payload in a HASH field.
            pipe.zadd(zkey, {str(bar["open_ms"][index]): bar["open_ms"][index]})
            pipe.hset(hkey, str(bar["open_ms"][index]), member)
        pipe.execute()
        after = int(client.info("memory")["used_memory"])
        result[f"bar_{encoding}"] = {
            "rows": count,
            "zset_usage": int(client.memory_usage(zkey) or 0),
            "hash_usage": int(client.memory_usage(hkey) or 0),
            "used_memory_delta": after - before,
            "bytes_per_row": round((after - before) / max(1, count), 1),
        }
    # Layout variants for the compact encoding: one ZSET whose member is the
    # row (no separate index), and small per-bucket hashes that stay in
    # Redis's listpack encoding (default hash-max-listpack-entries 128).
    compact = bar["compact"]
    count = min(rows, len(compact))
    before = int(client.info("memory")["used_memory"])
    pipe = client.pipeline(transaction=False)
    for index in range(count):
        pipe.zadd("kn:zonly:compact", {compact[index]: bar["open_ms"][index]})
    pipe.execute()
    after = int(client.info("memory")["used_memory"])
    result["bar_compact_zset_only"] = {"rows": count, "used_memory_delta": after - before,
                                       "bytes_per_row": round((after - before) / max(1, count), 1)}
    for bucket in (64, 120):
        before = int(client.info("memory")["used_memory"])
        pipe = client.pipeline(transaction=False)
        for index in range(count):
            pipe.hset(f"kn:b{bucket}:{index // bucket}", str(bar["open_ms"][index]), compact[index])
        pipe.execute()
        after = int(client.info("memory")["used_memory"])
        encoding = client.object("encoding", f"kn:b{bucket}:0")
        result[f"bar_compact_bucket{bucket}"] = {
            "rows": count, "buckets": (count + bucket - 1) // bucket,
            "encoding": encoding.decode() if isinstance(encoding, bytes) else encoding,
            "used_memory_delta": after - before, "bytes_per_row": round((after - before) / max(1, count), 1)}
    latest = {}
    for feed, value in sample["latest"].items():
        for encoding in ("canonical", "public"):
            key = f"kn:latest:{feed}:{encoding}"
            payload = bytes.fromhex(value["canonical"]) if encoding == "canonical" else value["public"].encode()
            client.set(key, payload)
            latest[f"{feed}:{encoding}"] = int(client.memory_usage(key) or 0)
    result["latest_key_usage"] = latest
    result["final_used"] = int(client.info("memory")["used_memory"])
    client.flushall()
    return result


# ------------------------------------------------------------------- runtime

def _docker(*argv: str, timeout: float = 120) -> str:
    return subprocess.run(["docker", *argv], check=True, capture_output=True, text=True, timeout=timeout).stdout


def runtime(minutes: float, interval: float) -> dict[str, Any]:
    names = _docker("ps", "--filter", f"label=com.docker.compose.project={PROJECT}", "--format", "{{.Names}}").split()
    per_role: dict[str, list[float]] = {name: [] for name in names}
    others: list[float] = []
    totals: list[float] = []
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        started = time.monotonic()
        out = _docker("stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}")
        total = 0.0
        other = 0.0
        for line in out.splitlines():
            name, _, value = line.partition("\t")
            cpu = float(value.strip().rstrip("%") or 0) / 100.0
            if name in per_role:
                per_role[name].append(cpu)
                total += cpu
            else:
                other += cpu
        totals.append(total)
        others.append(other)
        time.sleep(max(0.0, interval - (time.monotonic() - started)))
    roles = {name.removeprefix(f"{PROJECT}-").removesuffix("-1"): _dist(values)
             for name, values in sorted(per_role.items())}
    return {"samples": len(totals), "interval_s": interval, "data_layer_vcpu": _dist(totals),
            "non_data_layer_vcpu": _dist(others), "roles_vcpu": roles, "kafka": kafka_bytes()}


def kafka_bytes() -> dict[str, Any]:
    kafka1 = f"{PROJECT}-kafka1-1"
    image = _docker("inspect", "--format", "{{.Config.Image}}", kafka1).strip()
    cert = _docker("inspect", "--format",
                   '{{range .Mounts}}{{if eq .Destination "/etc/kafka/secrets"}}{{.Source}}{{end}}{{end}}', kafka1).strip()
    script = ("/opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka1:9092 "
              "--command-config /etc/kafka/secrets/admin.properties --describe --broker-list 1 2>/dev/null | tail -1")

    def snapshot() -> dict[str, int]:
        raw = _docker("run", "--rm", "--network", f"{PROJECT}_stable_internal", "-v", f"{cert}:/etc/kafka/secrets:ro",
                      image, "sh", "-c", script, timeout=180)
        document = json.loads(raw)
        sizes: dict[str, int] = {}
        for broker in document["brokers"]:
            for log_dir in broker["logDirs"]:
                for partition in log_dir["partitions"]:
                    topic = partition["partition"].rsplit("-", 1)[0]
                    sizes[topic] = sizes.get(topic, 0) + int(partition["size"])
        return sizes

    # Size deltas over a short window include retention deletions (they can
    # be negative), so the steady ingest rate is size / retention per topic.
    sizes = snapshot()
    retention_s = {"md.canonical.v2": 21_600, "md.raw.realtime.v2": 28_800}
    return {"broker": 1, "method": "retained bytes on broker 1 / topic retention",
            "size_bytes": {topic: size for topic, size in sorted(sizes.items()) if topic.startswith("md.")},
            "retention_avg_bytes_per_s": {topic: round(sizes.get(topic, 0) / seconds, 1)
                                          for topic, seconds in retention_s.items()}}


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="part", required=True)
    p = sub.add_parser("payloads")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--sample", type=Path, required=True)
    p.add_argument("--rows-per-key", type=int, default=500)
    p.add_argument("--render-rows", type=int, default=2000)
    r = sub.add_parser("redis")
    r.add_argument("--sample", type=Path, required=True)
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=6379)
    t = sub.add_parser("runtime")
    t.add_argument("--out", type=Path, required=True)
    t.add_argument("--minutes", type=float, default=10.0)
    t.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.part == "payloads":
        payload = payloads(args.sample, args.rows_per_key, args.render_rows)
    elif args.part == "redis":
        payload = redis_memory(args.sample, args.host, args.port)
    else:
        payload = runtime(args.minutes, args.interval)
    payload = {"schema": SCHEMA, "part": args.part, "collected_at_ns": time.time_ns(), **payload}
    payload["sha256"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(args.out), "sha256": payload["sha256"]}), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

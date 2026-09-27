#!/usr/bin/env python3
"""KN-1 K1.4 resource and retention sizing from real data, not estimates.

Purpose: measure what the Kafka-native design will hold and move - canonical
payload bytes per feed, rendered public rows, a compact BAR row encoding,
their real Redis memory including index overhead, the Data Layer CPU
denominator and Kafka bytes/s - so the candidate budget is frozen on numbers.

Boundary: read-only against production. The spool is opened ``mode=ro`` +
``query_only``; rendered rows come from a bounded copy in a tmpfs scratch
spool inside a ``--rm`` container; Redis measurements use a disposable Redis
of the production image digest with ``--network none``, refuse a non-empty
target, write only under a per-run key namespace and delete exactly the keys
they wrote (never ``FLUSHALL``; KN-1 review F6); CPU and Kafka figures
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


# Only the fields that make up the logical product key (LPK) are left out of a
# BAR cache row: instrument uid, venue, market and the bar interval. They are
# restored from the product's LPK, which is the cache key itself, so no header
# is stored. Every other field - schema version, provider, source id/role,
# native symbol, instrument id/revision and all provenance - can change within
# one product's history (schema/source transitions, repairs) and stays in the
# row (KN-1 review R2 F5: a per-product header copied from one row is not
# lossless for the others).
LPK_DERIVED_ENVELOPE_FIELDS = ("instrument_uid", "venue", "market")
LPK_DERIVED_BAR_FIELDS = ("interval",)
# State-contract integers are sized at their widest encodable value (2^63-1),
# so the measured bytes are an upper bound, not a sample of small numbers.
WIDEST_U63 = 2**63 - 1


def _complete_bar(envelope, payload: bytes) -> bytes:
    """One BAR cache row carrying everything the contract needs per open time.

    Values (exact decimal text), revision/final/origin/lifecycle and the
    replaced event, every provenance and quality field of the envelope, and
    the state contract (canonical content hash, source offset, materializer
    epoch). Only the LPK-derived fields are left out; a proto field that is
    neither mapped here nor LPK-derived fails the measurement.
    """

    from qdl.runtime.stable_source import _decimal_text

    bar = envelope.bar
    optional = lambda name: _decimal_text(getattr(bar, name)) if bar.HasField(name) else None  # noqa: E731
    bar_row = {
        "open": _decimal_text(bar.open), "high": _decimal_text(bar.high), "low": _decimal_text(bar.low),
        "close": _decimal_text(bar.close), "volume": _decimal_text(bar.volume),
        "base_volume": optional("base_volume"), "quote_volume": optional("quote_volume"),
        "contract_volume": optional("contract_volume"), "volume_unit": int(bar.volume_unit),
        "trade_count": int(bar.trade_count), "open_time_ns": int(bar.open_time_ns),
        "close_time_ns": int(bar.close_time_ns), "is_final": bool(bar.is_final), "revision": int(bar.revision),
        "origin": int(bar.origin), "lifecycle": int(bar.lifecycle),
        "supersedes_event_id": bar.supersedes_event_id.hex() if bar.HasField("supersedes_event_id") else None,
    }
    envelope_row = {
        "schema_name": envelope.schema_name, "schema_major": int(envelope.schema_major),
        "schema_minor": int(envelope.schema_minor), "instrument_id": envelope.instrument_id,
        "product_type": envelope.product_type, "native_symbol": envelope.native_symbol,
        "provider": envelope.provider, "source_id": envelope.source_id, "source_role": int(envelope.source_role),
        "event_id": envelope.event_id.hex(), "instrument_revision": int(envelope.instrument_revision),
        "lease_epoch": int(envelope.lease_epoch), "source_event_time_ns": int(envelope.source_event_time_ns),
        "received_at_ns": int(envelope.received_at_ns), "normalized_at_ns": int(envelope.normalized_at_ns),
        "published_at_ns": int(envelope.published_at_ns), "source_sequence": envelope.source_sequence,
        "partition_sequence": int(envelope.partition_sequence),
        "normalizer_version": envelope.normalizer_version, "adapter_version": envelope.adapter_version,
        "quality_flags": [int(flag) for flag in envelope.quality_flags],
        "raw_payload_hash": envelope.raw_payload_hash.hex(), "correlation_id": envelope.correlation_id,
        "config_revision": int(envelope.config_revision), "source_session_id": envelope.source_session_id,
        "connection_generation": int(envelope.connection_generation),
        "authority_revision": int(envelope.authority_revision),
        "partition_plan_epoch": int(envelope.partition_plan_epoch),
        "canonical_payload_hash": envelope.canonical_payload_hash.hex(),
        "raw_capture_id": envelope.raw_capture_id.hex(),
    }
    covered = set(envelope_row) | set(LPK_DERIVED_ENVELOPE_FIELDS) | {"bar"}
    missing = [f.name for f in envelope.DESCRIPTOR.fields
               if f.name not in covered and f.containing_oneof is None]
    missing += [f"bar.{f.name}" for f in bar.DESCRIPTOR.fields
                if f.name not in set(bar_row) | set(LPK_DERIVED_BAR_FIELDS)]
    if missing:
        raise ValueError(f"contract-complete BAR row does not map {missing}")
    state = {"content_sha256": hashlib.sha256(payload).hexdigest(), "source_offset": WIDEST_U63,
             "materializer_epoch": WIDEST_U63}
    return json.dumps({"b": bar_row, "e": envelope_row, "s": state}, separators=(",", ":")).encode()


def _canonical_state(payload: bytes) -> bytes:
    """Lossless alternative: the canonical protobuf bytes unchanged, prefixed
    by a fixed 48-byte state trailer (source offset, materializer epoch as
    big-endian u64 at their widest value, canonical content SHA-256)."""

    return (WIDEST_U63.to_bytes(8, "big") + WIDEST_U63.to_bytes(8, "big")
            + hashlib.sha256(payload).digest() + payload)


def _state_trailer(payload: bytes) -> bytes:
    return WIDEST_U63.to_bytes(8, "big") + WIDEST_U63.to_bytes(8, "big") + hashlib.sha256(payload).digest()


def lpk_row(envelope, payload: bytes, lpk) -> bytes:
    """Encode one BAR row of product ``lpk``: the canonical envelope without
    the LPK-derived fields, behind the 48-byte state trailer.

    A row whose uid/venue/market/interval differ from the key belongs to
    another product and is refused (never silently re-keyed).
    """

    expected = {"instrument_uid": lpk.instrument_uid, "venue": lpk.venue, "market": lpk.market}
    for name, value in expected.items():
        if getattr(envelope, name) != value:
            raise ValueError(f"row {name}={getattr(envelope, name)!r} is not product {lpk.encode()}")
    if envelope.WhichOneof("payload") != "bar" or envelope.bar.interval != lpk.qualifier:
        raise ValueError(f"row is not a BAR of interval {lpk.qualifier}")
    stripped = type(envelope)()
    stripped.CopyFrom(envelope)
    for name in LPK_DERIVED_ENVELOPE_FIELDS:
        stripped.ClearField(name)
    for name in LPK_DERIVED_BAR_FIELDS:
        stripped.bar.ClearField(name)
    return _state_trailer(payload) + stripped.SerializeToString()


def lpk_row_decode(row: bytes, lpk, envelope_type) -> bytes:
    """The shared-key decoder: restore the canonical bytes of any row of the
    product from the LPK alone, and prove them against the stored hash."""

    envelope = envelope_type.FromString(row[48:])
    envelope.instrument_uid = lpk.instrument_uid
    envelope.venue = lpk.venue
    envelope.market = lpk.market
    envelope.bar.interval = lpk.qualifier
    payload = envelope.SerializeToString()
    if hashlib.sha256(payload).digest() != row[16:48]:
        raise ValueError("decoded row does not match its canonical content hash")
    return payload


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
                from qdl.projection.state_contract import LogicalProductKey

                identity = binding.instrument.identity
                lpk = LogicalProductKey.for_product(
                    environment="paper", venue=identity.venue, market=identity.market,
                    instrument_uid=identity.instrument_uid, feed="BAR", interval=binding.interval)
                compact, complete, canonical_state, lpk_rows, open_ms = [], [], [], [], []
                mismatches = 0
                for payload in protobufs:
                    envelope = market_data_pb2.EventEnvelope.FromString(payload)
                    if envelope.WhichOneof("payload") == "bar" and envelope.bar.is_final:
                        compact.append(_compact_bar(envelope))
                        complete.append(_complete_bar(envelope, payload))
                        canonical_state.append(_canonical_state(payload))
                        row = lpk_row(envelope, payload, lpk)
                        lpk_rows.append(row)
                        # Decoded with the one product key shared by every row.
                        try:
                            exact = lpk_row_decode(row, lpk, market_data_pb2.EventEnvelope) == payload
                        except ValueError:
                            exact = False
                        mismatches += 0 if exact else 1
                        open_ms.append(int(envelope.bar.open_time_ns // 1_000_000))
                entry["compact_row_bytes"] = _dist([len(x) for x in compact])
                entry["complete_row_bytes"] = _dist([len(x) for x in complete])
                entry["canonical_state_row_bytes"] = _dist([len(x) for x in canonical_state])
                entry["lpk_state_row_bytes"] = _dist([len(x) for x in lpk_rows])
                entry["lpk_state_shared_key_decode_mismatches"] = mismatches
                entry["lpk_state_distinct_values"] = {
                    name: len({getattr(market_data_pb2.EventEnvelope.FromString(p), name) for p in protobufs})
                    for name in ("schema_minor", "provider", "source_id", "source_role", "native_symbol",
                                 "instrument_revision", "adapter_version", "normalizer_version")}
                sample["bar"] = {
                    "binding": binding.binding_id,
                    "open_ms": [int(market_data_pb2.EventEnvelope.FromString(p).bar.open_time_ns // 1_000_000)
                                for p in protobufs],
                    "canonical": [p.hex() for p in protobufs],
                    "public": [x.decode() for x in public],
                    "compact": [x.decode() for x in compact],
                    # Final bars only, aligned with their own open times.
                    "final_open_ms": open_ms,
                    "complete": [x.decode() for x in complete],
                    "canonical_state": [x.hex() for x in canonical_state],
                    "lpk_state": [x.hex() for x in lpk_rows],
                }
            else:
                sample["latest"][feed] = {"canonical": protobufs[-1].hex(), "public": public[-1].decode()}
            rendered[feed] = entry
    finally:
        spool.close()
        live.close()
    return rendered, sample


# --------------------------------------------------------------------- redis

class NonEmptyTarget(RuntimeError):
    """The sizing Redis already holds keys: it is not a disposable target."""


class _Namespace:
    """Every key this run writes, under one per-run prefix, for exact cleanup."""

    def __init__(self, run_id: str) -> None:
        self.prefix = f"kn-sizing:{run_id}:"
        self.keys: set[str] = set()

    def __call__(self, suffix: str) -> str:
        key = self.prefix + suffix
        self.keys.add(key)
        return key


def _bar_rows(bar: dict[str, Any], encoding: str) -> tuple[list[int], list[bytes]]:
    values = bar[encoding]
    if encoding in ("compact", "complete", "canonical_state", "lpk_state"):
        opens = bar["final_open_ms"] if "final_open_ms" in bar else bar["open_ms"]
    else:
        opens = bar["open_ms"]
    binary = encoding in ("canonical", "canonical_state", "lpk_state")
    rows = [bytes.fromhex(value) if binary else value.encode() for value in values]
    count = min(len(opens), len(rows))
    return opens[:count], rows[:count]


def redis_memory_with(client, sample: dict[str, Any], run_id: str,
                      buckets: Sequence[int] = (64, 120)) -> dict[str, Any]:
    """Measure BAR/latest layouts on ``client``; refuses a non-empty target.

    Deletes exactly the keys it wrote, also on error; ``FLUSHALL``/``FLUSHDB``
    are never sent (KN-1 review F6).
    """

    existing = int(client.dbsize())
    if existing:
        raise NonEmptyTarget(f"refusing to size on a Redis holding {existing} keys; use a disposable instance")
    key = _Namespace(run_id)
    used = lambda: int(client.info("memory")["used_memory"])  # noqa: E731
    result: dict[str, Any] = {"redis_version": client.info("server")["redis_version"], "baseline_used": used(),
                              "namespace": key.prefix}
    bar = sample["bar"]
    try:
        # Index (ZSET, score = open ms) + payload HASH, per encoding.
        for encoding in ("canonical", "public", "compact", "complete", "canonical_state",
                         "lpk_state"):
            if encoding not in bar:
                continue
            opens, rows = _bar_rows(bar, encoding)
            before = used()
            zkey, hkey = key(f"z:{encoding}"), key(f"h:{encoding}")
            pipe = client.pipeline(transaction=False)
            for open_ms, row in zip(opens, rows):
                pipe.zadd(zkey, {str(open_ms): open_ms})
                pipe.hset(hkey, str(open_ms), row)
            pipe.execute()
            after = used()
            result[f"bar_{encoding}"] = {
                "rows": len(rows), "max_row_bytes": max((len(r) for r in rows), default=0),
                "zset_usage": int(client.memory_usage(zkey) or 0),
                "hash_usage": int(client.memory_usage(hkey) or 0),
                "used_memory_delta": after - before,
                "bytes_per_row": round((after - before) / max(1, len(rows)), 1),
            }
        # Compact rows as ZSET members (no separate index).
        opens, rows = _bar_rows(bar, "compact")
        before = used()
        pipe = client.pipeline(transaction=False)
        zonly = key("zonly:compact")
        for open_ms, row in zip(opens, rows):
            pipe.zadd(zonly, {row: open_ms})
        pipe.execute()
        after = used()
        result["bar_compact_zset_only"] = {"rows": len(rows), "used_memory_delta": after - before,
                                           "bytes_per_row": round((after - before) / max(1, len(rows)), 1)}
        # Per-product hash buckets that stay listpack-encoded when every row
        # fits hash-max-listpack-value; the encoding of every bucket is read
        # back, so a row that silently converts a bucket is visible.
        for encoding in ("compact", "complete", "canonical_state", "lpk_state"):
            if encoding not in bar:
                continue
            opens, rows = _bar_rows(bar, encoding)
            # Bucket bytes land in allocator size classes, so bytes/row depends
            # on the bucket size; several sizes are measured, none assumed.
            for bucket in buckets:
                before = used()
                pipe = client.pipeline(transaction=False)
                names = []
                for index, (open_ms, row) in enumerate(zip(opens, rows)):
                    name = key(f"b{bucket}:{encoding}:{index // bucket}")
                    if not names or names[-1] != name:
                        names.append(name)
                    pipe.hset(name, str(open_ms), row)
                pipe.execute()
                after = used()
                encodings = {}
                for name in names:
                    value = client.object("encoding", name)
                    value = value.decode() if isinstance(value, bytes) else str(value)
                    encodings[value] = encodings.get(value, 0) + 1
                label = "" if encoding == "compact" else f"_{encoding}"
                result[f"bar_compact_bucket{bucket}" if not label else f"bar{label}_bucket{bucket}"] = {
                    "rows": len(rows), "buckets": len(names), "bucket_encodings": encodings,
                    "encoding": max(encodings, key=encodings.get) if encodings else None,
                    "max_row_bytes": max((len(r) for r in rows), default=0),
                    "used_memory_delta": after - before,
                    "bytes_per_row": round((after - before) / max(1, len(rows)), 1)}
        latest = {}
        for feed, value in sample["latest"].items():
            for encoding in ("canonical", "public"):
                name = key(f"latest:{feed}:{encoding}")
                payload = bytes.fromhex(value["canonical"]) if encoding == "canonical" else value["public"].encode()
                client.set(name, payload)
                latest[f"{feed}:{encoding}"] = int(client.memory_usage(name) or 0)
        result["latest_key_usage"] = latest
        result["final_used"] = used()
    finally:
        written = sorted(key.keys)
        for start in range(0, len(written), 500):
            client.delete(*written[start:start + 500])
        result["keys_written"] = len(written)
        result["keys_left_after_cleanup"] = int(client.dbsize())
    return result


# Latest-state rows and per-product BAR identity headers are single Redis
# strings: their cost depends only on key and value length, so they are
# measured at the largest observed canonical size of each feed plus the 48-byte
# state trailer (KN-1 review F5: every cache structure is counted).
STATE_TRAILER_BYTES = 48
LPK_KEY_TEMPLATE = "kn-cache:g{generation:020d}:lpk1|paper|BINANCE|USDM|a953e16e-7138-5562-b5e8-c337a44d0b65|{feed}|-"


def string_key_usage_with(client, value_bytes: dict[str, int], run_id: str) -> dict[str, Any]:
    """MEMORY USAGE of one string key per named value size; guarded like
    ``redis_memory_with`` (non-empty target refused, exact keys deleted)."""

    existing = int(client.dbsize())
    if existing:
        raise NonEmptyTarget(f"refusing to size on a Redis holding {existing} keys; use a disposable instance")
    key = _Namespace(run_id)
    result: dict[str, Any] = {"namespace": key.prefix, "usage_bytes": {}, "value_bytes": dict(value_bytes)}
    try:
        for name, length in sorted(value_bytes.items()):
            name_key = key(LPK_KEY_TEMPLATE.format(generation=2**63 - 1, feed=name))
            client.set(name_key, b"\xa5" * length)
            result["usage_bytes"][name] = int(client.memory_usage(name_key) or 0)
    finally:
        written = sorted(key.keys)
        for start in range(0, len(written), 500):
            client.delete(*written[start:start + 500])
        result["keys_written"] = len(written)
        result["keys_left_after_cleanup"] = int(client.dbsize())
    return result


def redis_memory(sample_path: Path, host: str, port: int, buckets: Sequence[int] = (64, 120)) -> dict[str, Any]:
    import redis

    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    client = redis.Redis(host=host, port=port)
    return redis_memory_with(client, sample, run_id=f"{time.time_ns():x}", buckets=buckets)


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
    r.add_argument("--buckets", default="64,120", help="comma-separated opens per hash bucket")
    k = sub.add_parser("redis-keys")
    k.add_argument("--value-bytes", required=True, help="JSON object name -> value length in bytes")
    k.add_argument("--out", type=Path, required=True)
    k.add_argument("--host", default="127.0.0.1")
    k.add_argument("--port", type=int, default=6379)
    t = sub.add_parser("runtime")
    t.add_argument("--out", type=Path, required=True)
    t.add_argument("--minutes", type=float, default=10.0)
    t.add_argument("--interval", type=float, default=10.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.part == "payloads":
        payload = payloads(args.sample, args.rows_per_key, args.render_rows)
    elif args.part == "redis":
        payload = redis_memory(args.sample, args.host, args.port,
                               buckets=tuple(int(item) for item in args.buckets.split(",")))
    elif args.part == "redis-keys":
        import redis

        payload = string_key_usage_with(redis.Redis(host=args.host, port=args.port),
                                        json.loads(args.value_bytes), run_id=f"{time.time_ns():x}")
    else:
        payload = runtime(args.minutes, args.interval)
    payload = {"schema": SCHEMA, "part": args.part, "collected_at_ns": time.time_ns(), **payload}
    payload["sha256"] = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(args.out), "sha256": payload["sha256"]}), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

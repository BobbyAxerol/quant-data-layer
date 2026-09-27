#!/usr/bin/env python3
"""KN-3 K3.6: export BAR history from the canonical SQLite spool and import it
into ``md.bars.v2`` as ``LEGACY_BAR`` frames (plan decision D16a, guide 18.10).

Purpose: when the canonical topic no longer retains the BAR history the edge
and consumers need, the retained spool window is carried into the KN-3 state
topic with ``legacy_import`` provenance, its original event id and content
hash (in the frame header) and its spool lineage (stream, partition key,
logical offset). The spool ``logical_offset`` is lineage only - append order,
never a canonical Kafka offset and never market time; stage B stores the rows
with the trailer offset ``MAX_OFFSET`` ("no canonical coordinate", D13). No
history is re-published as realtime ticks and no offset is invented.

Modes:

* ``export`` (read-only): opens the spool ``file:...?mode=ro`` with
  ``PRAGMA query_only=ON``; for every BAR binding of the catalog (product
  identity from ``scripts/kn_gateway_bundle.compile_bundle``, the one LPK
  owner) reads ``WHERE stream=? AND partition_key=?`` in ``logical_offset``
  keyset pages through the primary-key index only (``INDEXED BY`` + an
  EXPLAIN QUERY PLAN assertion: never a scan, a temp sort or
  ``accepted_at_ns``), inside one read transaction per binding with the
  binding's max ``logical_offset`` captured first as its cutoff. Every row
  must be a BAR of its binding's full identity (the BAR edge predicate
  ``bar_envelope_matches_binding``), carry its stored payload hash and event
  id, and encode as a frame and a stage-B row; otherwise the run stops
  (REFUSED, naming binding and logical offset).
* ``import``: the same export, then per binding the frames go to the bars
  topic with an explicit partition (``state_partition``) in bounded
  transactions (transactional producer, ``transactional.id``
  ``kn-projector-v3-legacy-import-<suffix>``; the suffix is sealed over
  environment/topic/partitions/epoch/catalog/spool identity, so a rerun of
  the same import fences a crashed predecessor), one commit per batch.
  Before publishing it captures the canonical topic's earliest/latest
  (``read_committed``) offsets per partition as the replay cutoff and checks
  the bars topic's partition count. ``--dry-run`` produces the receipt only
  (no Kafka contact). A broker is only written with ``--isolated`` (a
  disposable plaintext broker, never ``kafka1..3``) or ``--confirm TOKEN``
  where TOKEN is sealed over the plan (as ``kn_state_topics_packet.py``).

Rerun is idempotent: keys are content-addressed (``<lpk>|<open>|f<rev>|<sha16>``
or ``|p``), compaction keeps one record per key and stage B treats an equal
fact as DUPLICATE and a lower revision as STALE, so an import never
overwrites a newer correction.

Receipt (``--receipt``): the plan and its token; per binding rows, finals,
in-progress, first/last open ms, spool first/cutoff logical offset, state
partition and SHA-256 over the sorted ``open_ms|revision|content_sha256``
lines (plus the sorted key set); the replay cutoff actually captured; totals.
No secrets (TLS is recorded as a boolean only).

Boundary: never writes the spool, the canonical topic or any Redis; never
creates or alters topics; a production import is an owner-approved step
outside this script's tests.

  python -B scripts/kn_bar_legacy_import.py export --sqlite canonical-cache.sqlite3 \
      --catalog config/v2/stable-source-bindings.yaml --environment paper \
      --partitions 6 --materializer-epoch 1 --receipt receipt.json
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from typing import Any, Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.marketdata.v2 import market_data_pb2  # noqa: E402
from qdl.projection.kn_state_codec import (  # noqa: E402
    LegacyLineage,
    StateCodecError,
    StateFrame,
    decode_frame,
    encode_bar_row,
    legacy_bar_frame,
    state_partition,
)
from qdl.projection.state_contract import MAX_OFFSET, LogicalProductKey  # noqa: E402
from qdl.runtime.kn_bar_readback import bar_envelope_matches_binding, binding_product_key  # noqa: E402

SCHEMA = "qdl.kn.v220.legacy-bar-import.v1"
DEFAULT_CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_BARS_TOPIC = "md.bars.v2"
TRANSACTIONAL_PREFIX = "kn-projector-v3-legacy-import-"
PRODUCTION_HOSTS = frozenset({"kafka1", "kafka2", "kafka3"})
PRODUCTION_PROJECT = "qdl_v2_stable_candidate"
PK_COLUMNS = ["stream", "partition_key", "logical_offset"]
EXIT = {"PASS": 0, "DRY_RUN": 0, "FAIL": 1, "REFUSED": 4, "ERROR": 5}
_BOOTSTRAP = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}:[0-9]{1,5}$")
_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


class Refused(ValueError):
    """The input, a spool row or the invocation is outside the import contract."""


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _load_bundle_module():
    spec = importlib.util.spec_from_file_location("kn_gateway_bundle", ROOT / "scripts/kn_gateway_bundle.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ------------------------------------------------------------------ bindings

@dataclass(frozen=True)
class BarBinding:
    binding: Any  # StableSourceBinding
    lpk: LogicalProductKey
    physical_key: str


def load_bar_bindings(catalog_path: Path, environment: str) -> tuple[list[BarBinding], dict[str, Any]]:
    """Every BAR binding with its LPK taken from the gateway bundle compiler."""

    from qdl.runtime.stable_catalog import StableSourceCatalog

    bundle = _load_bundle_module().compile_bundle(
        environment=environment, catalog_path=catalog_path, manifest_paths=())
    catalog = StableSourceCatalog.load(catalog_path)
    by_id = {item.binding_id: item for item in catalog.bindings}
    result: list[BarBinding] = []
    for item in bundle["catalog"]["bindings"]:
        if item["feed"] != "BAR":
            continue
        binding = by_id[item["binding_id"]]
        lpk = LogicalProductKey.parse(item["product_key"])
        if item["physical_key"] != binding.partition_key:
            raise Refused(f"bundle physical key differs from the catalog binding={binding.binding_id}")
        if binding_product_key(binding, environment) != lpk:
            raise Refused(f"readback LPK rule differs from the gateway bundle binding={binding.binding_id}")
        result.append(BarBinding(binding, lpk, item["physical_key"]))
    physical = [item.physical_key for item in result]
    lpks = [item.lpk for item in result]
    if len(set(physical)) != len(physical) or len(set(lpks)) != len(lpks):
        raise Refused("BAR bindings do not map one-to-one to physical keys and products")
    catalog_info = {
        "path": str(catalog_path),
        "sha256": hashlib.sha256(Path(catalog_path).read_bytes()).hexdigest(),
        "revision": int(catalog.catalog_revision),
        "canonical_stream": catalog.canonical_stream,
        "bar_bindings": len(result),
    }
    return sorted(result, key=lambda item: item.binding.binding_id), catalog_info


# ------------------------------------------------------------------ spool (read-only)

def open_spool_readonly(path: Path) -> sqlite3.Connection:
    """``?mode=ro`` + ``query_only``: this process cannot write the spool."""

    database = Path(path).expanduser().resolve()
    if not database.is_file():
        raise Refused(f"spool database not found: {database}")
    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True, isolation_level=None)
    connection.execute("PRAGMA query_only=ON")
    if connection.execute("PRAGMA query_only").fetchone()[0] != 1:
        connection.close()
        raise Refused("spool connection is not query_only")
    return connection


def primary_key_index(connection: sqlite3.Connection) -> str:
    """The events primary-key index ``(stream, partition_key, logical_offset)``."""

    for row in connection.execute("PRAGMA index_list(events)").fetchall():
        name, origin = row[1], row[3]
        if origin != "pk":
            continue
        columns = [item[2] for item in connection.execute(f'PRAGMA index_info("{name}")').fetchall()]
        if columns == PK_COLUMNS:
            return name
    raise Refused("spool events table has no (stream, partition_key, logical_offset) primary key")


def spool_cache_id(connection: sqlite3.Connection) -> str | None:
    try:
        row = connection.execute("SELECT cache_id FROM cache_identity WHERE singleton = 1").fetchone()
    except sqlite3.Error:
        return None
    return None if row is None else str(row[0])


def page_sql(index: str) -> str:
    return (f'SELECT logical_offset, event_id, payload, payload_sha256 FROM events INDEXED BY "{index}" '
            "WHERE stream = ? AND partition_key = ? AND logical_offset > ? AND logical_offset <= ? "
            "ORDER BY logical_offset LIMIT ?")


def cutoff_sql(index: str) -> str:
    return (f'SELECT MIN(logical_offset), MAX(logical_offset) FROM events INDEXED BY "{index}" '
            "WHERE stream = ? AND partition_key = ?")


def assert_primary_key_plan(connection: sqlite3.Connection, sql: str, params: Sequence[Any], index: str) -> list[str]:
    """EXPLAIN QUERY PLAN must be an index search on the PK - never a scan or sort."""

    details = [str(row[3]) for row in connection.execute("EXPLAIN QUERY PLAN " + sql, tuple(params)).fetchall()]
    uses_pk = any(
        (f"USING INDEX {index} " in detail or f"USING COVERING INDEX {index} " in detail)
        and "stream=? AND partition_key=?" in detail
        for detail in details
    )
    forbidden = any(detail.startswith("SCAN") or "TEMP B-TREE" in detail or "accepted_at_ns" in detail
                    for detail in details)
    if not uses_pk or forbidden:
        raise Refused(f"spool query does not use only the primary key index: {details}")
    return details


# ------------------------------------------------------------------ export

@dataclass(frozen=True)
class ExportedFrame:
    partition: int
    key: str
    value: bytes


def frame_for_row(item: BarBinding, stream: str, logical_offset: int, event_id: bytes, payload: bytes,
                  payload_sha256: str, materializer_epoch: int) -> StateFrame:
    """One spool row -> one LEGACY_BAR frame, or Refused naming binding + offset."""

    where = f"binding={item.binding.binding_id} logical_offset={logical_offset}"
    payload = bytes(payload)
    if hashlib.sha256(payload).hexdigest() != payload_sha256:
        raise Refused(f"spool payload hash differs from the stored hash {where}")
    try:
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
    except Exception as error:  # protobuf DecodeError
        raise Refused(f"spool payload is not an EventEnvelope {where}") from error
    if not bar_envelope_matches_binding(envelope, item.binding):
        raise Refused(f"spool row is not a BAR of its binding identity {where}")
    if bytes(event_id) != envelope.event_id:
        raise Refused(f"spool event id differs from the envelope {where}")
    try:
        frame = legacy_bar_frame(payload, item.lpk, LegacyLineage(stream, item.physical_key, logical_offset),
                                 materializer_epoch)
        # Stage B must be able to store it (D13: legacy rows carry MAX_OFFSET).
        encode_bar_row(payload, item.lpk, MAX_OFFSET, materializer_epoch)
    except StateCodecError as error:
        raise Refused(f"spool row does not encode as a legacy BAR ({error.reason}) {where}") from error
    return frame


def export_binding(connection: sqlite3.Connection, index: str, stream: str, item: BarBinding, *,
                   materializer_epoch: int, partitions: int, page_rows: int,
                   keep_frames: bool, page_transactions: bool = False,
                   page_pause_s: float = 0.0, sleep=time.sleep) -> tuple[dict[str, Any], list[ExportedFrame]]:
    """Read one binding; return its summary (+ frames).

    Default: one read transaction per binding. ``page_transactions`` (KN-4
    D43): a live spool is read in one short transaction per page with a pause
    between pages - a long read snapshot of the production spool held its
    writer back (2026-09-25 14:53: every projector's durable append 0.1 ->
    1-4 s). The range stays bounded by the cutoff captured first; rows are
    append-only, so pages equal one snapshot except rows the spool's
    retention removed meanwhile - the summary records the first offset
    actually read."""

    partition = state_partition(item.lpk, partitions)
    frames: list[ExportedFrame] = []
    facts: list[tuple[int, int, str]] = []
    keys: set[str] = set()
    finals = in_progress = 0
    first_offset = cutoff = first_read = None
    connection.execute("BEGIN")
    try:
        params = (stream, item.physical_key)
        assert_primary_key_plan(connection, cutoff_sql(index), params, index)
        first_offset, cutoff = connection.execute(cutoff_sql(index), params).fetchone()
        after = -1
        if cutoff is not None:
            assert_primary_key_plan(connection, page_sql(index), (*params, after, cutoff, page_rows), index)
        while cutoff is not None:
            if page_transactions:
                connection.execute("COMMIT")
                if page_pause_s > 0:
                    sleep(page_pause_s)
                connection.execute("BEGIN")
            rows = connection.execute(page_sql(index), (*params, after, cutoff, page_rows)).fetchall()
            if not rows:
                break
            if first_read is None:
                first_read = rows[0][0]
            for logical_offset, event_id, payload, payload_sha256 in rows:
                frame = frame_for_row(item, stream, logical_offset, event_id, payload, payload_sha256,
                                      materializer_epoch)
                key = frame.key()
                facts.append((frame.open_time_ms, frame.revision, frame.content_sha256))
                keys.add(key)
                if frame.is_final:
                    finals += 1
                else:
                    in_progress += 1
                if keep_frames:
                    value = frame.encode()
                    if decode_frame(value).key() != key:  # self-check before anything is published
                        raise Refused(f"frame does not round-trip binding={item.binding.binding_id} "
                                      f"logical_offset={logical_offset}")
                    frames.append(ExportedFrame(partition, key, value))
            after = rows[-1][0]
    finally:
        connection.execute("COMMIT")
    facts.sort()
    summary = {
        "binding_id": item.binding.binding_id,
        "lpk": item.lpk.encode(),
        "physical_key": item.physical_key,
        "state_partition": partition,
        "rows": len(facts),
        "finals": finals,
        "in_progress": in_progress,
        "first_open_ms": facts[0][0] if facts else None,
        "last_open_ms": facts[-1][0] if facts else None,
        "spool_first_logical_offset": first_offset,
        "spool_first_logical_offset_read": first_read,
        "spool_cutoff_logical_offset": cutoff,
        "facts_sha256": hashlib.sha256(
            "".join(f"{open_ms}|{revision}|{sha}\n" for open_ms, revision, sha in facts).encode()).hexdigest(),
        "distinct_keys": len(keys),
        "keys_sha256": hashlib.sha256("".join(f"{key}\n" for key in sorted(keys)).encode()).hexdigest(),
    }
    return summary, frames


def export(connection: sqlite3.Connection, bindings: Sequence[BarBinding], stream: str, *,
           materializer_epoch: int, partitions: int, page_rows: int,
           on_binding: Callable[[dict[str, Any], list[ExportedFrame]], None] | None = None,
           page_transactions: bool = False, page_pause_s: float = 0.0,
           completed: Mapping[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Export every binding; ``on_binding`` receives each binding's frames (import).

    ``completed`` (checkpointed import, KN-4 D43): binding id -> the summary of
    a binding an earlier run of the same plan already committed; it is reused
    without reading the spool again."""

    index = primary_key_index(connection)
    summaries = []
    for item in bindings:
        if completed and item.binding.binding_id in completed:
            summaries.append(dict(completed[item.binding.binding_id]))
            continue
        summary, frames = export_binding(connection, index, stream, item, materializer_epoch=materializer_epoch,
                                         partitions=partitions, page_rows=page_rows,
                                         keep_frames=on_binding is not None,
                                         page_transactions=page_transactions, page_pause_s=page_pause_s)
        if on_binding is not None:
            on_binding(summary, frames)
        summaries.append(summary)
    totals = {
        "bindings": len(summaries),
        "bindings_with_rows": sum(1 for item in summaries if item["rows"]),
        "rows": sum(item["rows"] for item in summaries),
        "finals": sum(item["finals"] for item in summaries),
        "in_progress": sum(item["in_progress"] for item in summaries),
        "distinct_keys": sum(item["distinct_keys"] for item in summaries),
    }
    return {
        "primary_key_index": index,
        "bindings": summaries,
        "spool_cutoff": {item["physical_key"]: item["spool_cutoff_logical_offset"] for item in summaries},
        "totals": totals,
        "export_sha256": canonical_sha256({"bindings": summaries, "totals": totals}),
    }


# ------------------------------------------------------------------ plan and seal

def transactional_id(*, environment: str, bars_topic: str, partitions: int, materializer_epoch: int,
                     catalog_revision: int, spool_cache: str | None) -> str:
    suffix = canonical_sha256({
        "environment": environment, "bars_topic": bars_topic, "partitions": partitions,
        "materializer_epoch": materializer_epoch, "catalog_revision": catalog_revision,
        "spool_cache_id": spool_cache,
    })[:16]
    return TRANSACTIONAL_PREFIX + suffix


def build_plan(*, environment: str, bootstrap: str | None, bars_topic: str, canonical_topic: str,
               partitions: int, materializer_epoch: int, catalog_info: Mapping[str, Any], sqlite_path: Path,
               spool_cache: str | None, tls: bool, isolated: bool, batch_frames: int) -> dict[str, Any]:
    return {
        "environment": environment,
        "bootstrap": bootstrap,
        "bars_topic": bars_topic,
        "canonical_topic": canonical_topic,
        "partitions": partitions,
        "materializer_epoch": materializer_epoch,
        "transactional_id": transactional_id(
            environment=environment, bars_topic=bars_topic, partitions=partitions,
            materializer_epoch=materializer_epoch, catalog_revision=catalog_info["revision"],
            spool_cache=spool_cache),
        "catalog": dict(catalog_info),
        "sqlite": {"path": str(Path(sqlite_path).expanduser().resolve()), "cache_id": spool_cache},
        "tls": tls,
        "isolated": isolated,
        "batch_frames": batch_frames,
    }


def seal(plan: Mapping[str, Any]) -> tuple[str, str]:
    digest = canonical_sha256(plan)
    return digest, f"IMPORT_QDL_KN3_LEGACY_BARS_{digest[:16]}"


def validate_target(*, bootstrap: str, bars_topic: str, canonical_topic: str, isolated: bool, tls: bool) -> None:
    hosts = [part.rsplit(":", 1)[0] for part in bootstrap.split(",")]
    if not all(_BOOTSTRAP.match(part) for part in bootstrap.split(",")):
        raise Refused("--bootstrap must be HOST:PORT[,HOST:PORT...]")
    if not _TOPIC.match(bars_topic) or bars_topic == canonical_topic:
        raise Refused("--bars-topic must be a state topic, never the canonical topic")
    if isolated and (tls or any(host in PRODUCTION_HOSTS or PRODUCTION_PROJECT in host for host in hosts)):
        raise Refused("--isolated is for a disposable plaintext broker, never a production broker")


# ------------------------------------------------------------------ Kafka

def kafka_security(args: argparse.Namespace) -> dict[str, str]:
    paths = (args.ca, args.cert, args.key)
    if not any(paths):
        return {}
    if not all(paths):
        raise Refused("TLS needs --ca, --cert and --key together")
    return {"security.protocol": "SSL", "ssl.ca.location": str(args.ca),
            "ssl.certificate.location": str(args.cert), "ssl.key.location": str(args.key)}


def capture_canonical_cutoff(bootstrap: str, topic: str, security: Mapping[str, str],
                             client_id: str) -> dict[str, Any]:
    """Earliest/latest (read_committed) offsets of every canonical partition."""

    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": client_id + "-cutoff",
                         "client.id": client_id + "-cutoff", "enable.auto.commit": False,
                         "isolation.level": "read_committed", **security})
    try:
        metadata = consumer.list_topics(topic, timeout=30)
        entry = metadata.topics.get(topic)
        if entry is None or entry.error is not None or not entry.partitions:
            raise Refused(f"canonical topic {topic} is not readable: {getattr(entry, 'error', None)}")
        partitions = {}
        for partition in sorted(entry.partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
            partitions[str(partition)] = {"earliest": low, "latest": high}
        return {"topic": topic, "isolation": "read_committed", "partitions": partitions}
    finally:
        consumer.close()


class TransactionalImporter:
    """Bounded transactions of explicit-partition frames; aborts on any error."""

    def __init__(self, *, bootstrap: str, topic: str, partitions: int, transactional_id: str,
                 security: Mapping[str, str], batch_frames: int) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.batch_frames = batch_frames
        self.transactions = 0
        self.frames = 0
        self._errors: list[str] = []
        self.producer = Producer({
            "bootstrap.servers": bootstrap, "client.id": transactional_id,
            "transactional.id": transactional_id, "enable.idempotence": True, "acks": "all",
            "linger.ms": 5, "compression.type": "none", **security,
        })
        metadata = self.producer.list_topics(topic, timeout=30)
        entry = metadata.topics.get(topic)
        if entry is None or entry.error is not None:
            raise Refused(f"bars topic {topic} does not exist (this script never creates topics)")
        if len(entry.partitions) != partitions:
            raise Refused(f"bars topic {topic} has {len(entry.partitions)} partitions, plan says {partitions}")
        self.producer.init_transactions(60)

    def _delivered(self, error, _message) -> None:
        if error is not None:
            self._errors.append(str(error))

    def publish(self, frames: Sequence[ExportedFrame]) -> None:
        for start in range(0, len(frames), self.batch_frames):
            batch = frames[start:start + self.batch_frames]
            self.producer.begin_transaction()
            try:
                for frame in batch:
                    while True:
                        try:
                            self.producer.produce(self.topic, key=frame.key.encode("utf-8"), value=frame.value,
                                                  partition=frame.partition, on_delivery=self._delivered)
                            break
                        except BufferError:
                            self.producer.poll(0.5)
                    self.producer.poll(0)
                self.producer.commit_transaction(120)
            except BaseException:
                self.producer.abort_transaction(60)
                raise
            if self._errors:
                raise RuntimeError(f"legacy import delivery failed: {self._errors[:3]}")
            self.transactions += 1
            self.frames += len(batch)

    def close(self) -> None:
        self.producer.flush(30)


# ------------------------------------------------------------------ cli

def _add_export(receipt: dict[str, Any], result: dict[str, Any], stream: str,
                canonical: dict[str, Any]) -> None:
    """Export results + the replay cutoff actually captured (spool and canonical topic)."""

    result = dict(result)
    spool_cutoff = result.pop("spool_cutoff")
    receipt.update(result)
    receipt["replay_cutoff"] = {
        "spool": {"stream": stream, "cutoff_logical_offset_by_partition_key": spool_cutoff},
        "canonical_topic": canonical,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.partitions < 1 or not 1 <= args.materializer_epoch <= MAX_OFFSET:
        raise Refused("--partitions must be >= 1 and --materializer-epoch within 1..2^63-1")
    if not 1 <= args.page_rows <= 10_000 or not 1 <= args.batch_frames <= 10_000:
        raise Refused("--page-rows and --batch-frames must be within 1..10000")
    bindings, catalog_info = load_bar_bindings(args.catalog, args.environment)
    canonical_topic = args.canonical_topic or catalog_info["canonical_stream"]
    stream = catalog_info["canonical_stream"]
    security: dict[str, str] = {}
    if args.mode == "import":
        if not args.bootstrap:
            raise Refused("import needs --bootstrap")
        security = kafka_security(args)
        validate_target(bootstrap=args.bootstrap, bars_topic=args.bars_topic, canonical_topic=canonical_topic,
                        isolated=args.isolated, tls=bool(security))
    connection = open_spool_readonly(args.sqlite)
    try:
        spool_cache = spool_cache_id(connection)
        plan = build_plan(
            environment=args.environment, bootstrap=args.bootstrap if args.mode == "import" else None,
            bars_topic=args.bars_topic, canonical_topic=canonical_topic, partitions=args.partitions,
            materializer_epoch=args.materializer_epoch, catalog_info=catalog_info, sqlite_path=args.sqlite,
            spool_cache=spool_cache, tls=bool(security), isolated=bool(args.isolated),
            batch_frames=args.batch_frames)
        digest, token = seal(plan)
        receipt: dict[str, Any] = {"schema": SCHEMA + ".receipt", "mode": args.mode, "plan": plan,
                                   "plan_sha256": digest, "confirmation_token": token, "mutations": 0}
        common = dict(materializer_epoch=args.materializer_epoch, partitions=args.partitions,
                      page_rows=args.page_rows, page_transactions=args.page_transactions,
                      page_pause_s=args.page_pause_ms / 1000.0)
        if args.mode == "export" or args.dry_run:
            result = export(connection, bindings, stream, **common)
            label = f"{args.mode}{' --dry-run' if args.dry_run else ''}"
            _add_export(receipt, result, stream, {"captured": False, "reason": f"{label} does not contact Kafka"})
            receipt["status"] = "DRY_RUN" if args.dry_run else "PASS"
            return receipt
        if not args.isolated and args.confirm != token:
            receipt.update(status="REFUSED",
                           error="import needs --isolated or --confirm equal to the sealed confirmation token")
            return receipt
        canonical_cutoff = capture_canonical_cutoff(args.bootstrap, canonical_topic, security,
                                                    plan["transactional_id"])
        importer = TransactionalImporter(bootstrap=args.bootstrap, topic=args.bars_topic,
                                         partitions=args.partitions, transactional_id=plan["transactional_id"],
                                         security=security, batch_frames=args.batch_frames)
        published: list[str] = []
        completed = read_progress(args.progress, digest)
        progress = open(args.progress, "a", encoding="utf-8", buffering=1) if args.progress else None

        def publish(summary: dict[str, Any], frames: list[ExportedFrame]) -> None:
            importer.publish(frames)  # committed when it returns
            published.append(summary["binding_id"])
            if progress is not None:
                progress.write(json.dumps({"plan_sha256": digest, "summary": summary}, sort_keys=True) + "\n")

        try:
            result = export(connection, bindings, stream, on_binding=publish, completed=completed, **common)
        except (Refused, RuntimeError, StateCodecError) as error:
            receipt.update(status="REFUSED" if isinstance(error, Refused) else "ERROR", error=str(error),
                           published_bindings=published, mutations=importer.frames,
                           kafka={"transactions": importer.transactions, "frames": importer.frames})
            return receipt
        finally:
            importer.close()
            if progress is not None:
                progress.close()
        _add_export(receipt, result, stream, {"captured": True, **canonical_cutoff})
        expected = sum(item["rows"] for item in result["bindings"] if item["binding_id"] not in completed)
        receipt.update(kafka={"transactions": importer.transactions, "frames": importer.frames},
                       published_bindings=published, mutations=importer.frames,
                       resumed_bindings=len(completed),
                       status="PASS" if importer.frames == expected else "FAIL")
        return receipt
    finally:
        connection.close()


def read_progress(path: Path | None, plan_sha256: str) -> dict[str, dict[str, Any]]:
    """Committed bindings of this plan from a progress file (other plans ignored)."""

    if path is None or not path.exists():
        return {}
    done: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue  # a line cut by a kill: that binding is simply redone
        if entry.get("plan_sha256") == plan_sha256 and isinstance(entry.get("summary"), dict):
            done[entry["summary"]["binding_id"]] = entry["summary"]
    return done


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=("export", "import"))
    parser.add_argument("--sqlite", type=Path, required=True, help="canonical cache SQLite (opened read-only)")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--environment", required=True, help="LPK environment, e.g. paper")
    parser.add_argument("--partitions", type=int, required=True, help="bars topic partition count")
    parser.add_argument("--materializer-epoch", type=int, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--bars-topic", default=DEFAULT_BARS_TOPIC)
    parser.add_argument("--canonical-topic", help="default: the catalog canonical stream")
    parser.add_argument("--bootstrap")
    parser.add_argument("--isolated", action="store_true", help="disposable plaintext test broker")
    parser.add_argument("--confirm")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ca", type=Path)
    parser.add_argument("--cert", type=Path)
    parser.add_argument("--key", type=Path)
    parser.add_argument("--page-rows", type=int, default=1000)
    parser.add_argument("--page-transactions", action="store_true",
                        help="one short read transaction per page (a live production spool, KN-4 D43)")
    parser.add_argument("--page-pause-ms", type=int, default=0, help="pause between pages (page transactions)")
    parser.add_argument("--progress", type=Path,
                        help="import checkpoint: committed bindings of this plan are skipped on a rerun")
    parser.add_argument("--batch-frames", type=int, default=500)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.mode == "export" and (args.bootstrap or args.isolated or args.confirm or args.dry_run):
        parser.error("export is read-only: no --bootstrap/--isolated/--confirm/--dry-run")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        receipt = run(args)
    except Refused as error:
        receipt = {"schema": SCHEMA + ".receipt", "mode": args.mode, "status": "REFUSED",
                   "error": str(error), "mutations": 0}
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    args.receipt.write_text(json.dumps(receipt, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": receipt["status"], "receipt": str(args.receipt),
                      "receipt_sha256": receipt["receipt_sha256"],
                      "totals": receipt.get("totals"), "error": receipt.get("error")}, sort_keys=True))
    return EXIT[receipt["status"]]


if __name__ == "__main__":
    sys.exit(main())

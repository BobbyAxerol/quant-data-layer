#!/usr/bin/env python3
"""KN-3 K3-T07: parity checker for an isolated full-flow market cache.

Purpose: after the Rust projector (stage A + stage B) has materialized an
isolated market cache from the legacy BAR import (``kn_bar_legacy_import.py``)
and a real canonical capture loaded into an isolated ``md.canonical.v2``,
prove that the cache holds exactly what its sources say. The cache layout is
``rust/qdl-projector/src/cache.rs`` ``Layout`` under ``kn3:<env>:``:
``ptr:<lpk>`` {ready, staging, fence}; ``l:<g>:<lpk>`` {v, t, p, o};
``bm:<g>:<lpk>`` {floor, first, last, last_final, rows, conflicts};
``b:<g>:<lpk>:<bucket>`` {<open_ms>: trailer+row}; ``cx:<g>:<lpk>`` conflicts.

Subcommands (each writes a JSON receipt ``--out`` with per-product counts, a
verdict and a failure list; exit 0 only on PASS):

* ``bars`` - oracle = the canonical SQLite spool, read exactly as the importer
  reads it (``kn_bar_legacy_import.export_binding``: ``mode=ro`` +
  ``query_only``, primary-key keyset pages, identity checked per row). Per BAR
  binding the expected set is the spool's final FINAL/REVISED opens (highest
  revision per open). The cache's READY generation is read for exactly those
  opens (bucket ``HMGET``; ``HLEN`` over the meta range for the row count;
  ``LRANGE`` of the bounded conflict list) and each open is ``content_equal``
  (SHA-256 of the ``decode_bar_row`` canonical bytes equals the spool
  ``payload_sha256``), ``content_differs`` (both hashes and whether a higher
  cache revision or a ``cx`` conflict record explains it), ``missing`` or
  ``below_floor``. ``--floor-aware`` excludes opens below ``bm.floor`` from
  the expected set. ``bm.rows`` must equal the counted bucket rows, and
  ``KnBarReadback.durable_final_bar_opens`` over the checked opens must equal
  the covered set (the edge readback against rows the Rust stage B wrote).
  PASS: every binding with spool finals is READY with ``missing == 0``,
  ``content_differs == 0`` and no other failure. ``--import-receipt`` (a PASS
  receipt of the importer; hashes, environment, catalog, bindings and spool
  ``cache_id`` verified) limits each binding to rows at or below its recorded
  ``spool_cutoff_logical_offset`` (rows appended later are read through the
  same primary-key pages and not counted) and reports
  ``trimmed_since_import`` / ``appended_since_import``; rows at import must
  equal checked + trimmed.
* ``latest`` - oracle = the isolated canonical topic read ``read_committed``
  from its earliest offset to the end captured at start (assign mode, no
  group commit); the last record per (Kafka key = physical key, payload feed)
  is mapped to its product through the gateway bundle (the
  ``rust/qdl-projector/src/products.rs`` rule; BAR excluded). The READY
  ``l:<g>:<lpk>`` must decode (``decode_latest_value``) to exactly those
  canonical bytes with trailer offset = record offset and ``o``/``p`` (and
  ``t`` when ``--topic-id`` is given) matching. PASS: every product equal and
  no cache latest without an oracle record.
* ``ready`` - pointer state of every bundle product (READY, REBUILDING =
  ready + staging, STAGING, ABSENT) with a summary per feed. PASS: all READY
  (a rebuilding product still serves READY).

Boundary: read-only everywhere - the spool is opened ``mode=ro`` +
``query_only``; Redis sees only HMGET/HLEN/LRANGE on exact keys (never
SCAN/KEYS, never a write); Kafka is consumed in assign mode without a group
commit. What it cannot prove by itself: that the cache was written by the Rust
stage B (the flow run supplies that); the Python writer used by its tests
mirrors ``apply.lua``.

  python -B scripts/kn3_flow_check.py bars --sqlite canonical-cache.sqlite3 \
      --environment paper --cache-url redis://kn3-cache:6379/0 --out bars.json
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.adapters.intervals import canonical_interval_ms  # noqa: E402
from qdl.marketdata.v2 import market_data_pb2  # noqa: E402
from qdl.projection.kn_state_codec import (  # noqa: E402
    StateCodecError,
    decode_bar_row,
    decode_frame,
    decode_latest_value,
)
from qdl.projection.state_contract import LogicalProductKey  # noqa: E402
from qdl.runtime.kn_bar_readback import (  # noqa: E402
    KnBarReadback,
    bar_envelope_matches_binding,
    bucket_of,
)


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    assert spec is not None and spec.loader is not None
    module = sys.modules.get(name) or importlib.util.module_from_spec(spec)
    if name not in sys.modules:
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


IMPORTER = _load("kn_bar_legacy_import")
Refused = IMPORTER.Refused

SCHEMA = "qdl.kn.v220.kn3-flow-check.v1"
EXIT = {"PASS": 0, "FAIL": 1, "REFUSED": 4, "ERROR": 5}
MAX_LISTED = 50
MAX_FAILURES = 500
MAX_BUCKETS = 100_000
PIPELINE_CHUNK = 64
_FINAL_LIFECYCLES = frozenset({market_data_pb2.BAR_LIFECYCLE_FINAL, market_data_pb2.BAR_LIFECYCLE_REVISED})


# ------------------------------------------------------------------ cache reader (read-only)

def _text(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)


def _int(value: Any) -> int | None:
    text = _text(value)
    if text is None or text == "":
        return None
    if not (text.isdigit() or (text.startswith("-") and text[1:].isdigit())):
        raise Refused(f"cache field is not a decimal: {text!r}")
    return int(text)


@dataclass(frozen=True)
class Pointer:
    ready: int | None
    staging: int | None
    fence: int


def pointer_state(pointer: Pointer) -> str:
    if pointer.ready is not None:
        return "REBUILDING" if pointer.staging is not None else "READY"
    return "STAGING" if pointer.staging is not None else "ABSENT"


class CacheReader:
    """Exact-key reads of the Rust layout; never SCAN/KEYS, never a write."""

    META_FIELDS = ("floor", "first", "last", "last_final", "rows", "conflicts")

    def __init__(self, client: Any, environment: str) -> None:
        self.client = client
        self.prefix = f"kn3:{environment}:"

    def key(self, *parts: Any) -> str:
        return self.prefix + ":".join(str(part) for part in parts)

    def _pipeline(self, commands: Sequence[tuple[str, tuple]]) -> list:
        replies: list = []
        for start in range(0, len(commands), PIPELINE_CHUNK):
            pipe = self.client.pipeline(transaction=False)
            for name, args in commands[start:start + PIPELINE_CHUNK]:
                getattr(pipe, name)(*args)
            replies.extend(pipe.execute())
        if len(replies) != len(commands):
            raise Refused("cache returned a short pipeline reply")
        return replies

    def pointers(self, lpks: Sequence[str]) -> dict[str, Pointer]:
        replies = self._pipeline([("hmget", (self.key("ptr", lpk), ["ready", "staging", "fence"]))
                                  for lpk in lpks])
        return {lpk: Pointer(_int(ready), _int(staging), _int(fence) or 0)
                for lpk, (ready, staging, fence) in zip(lpks, replies, strict=True)}

    def bar_meta(self, generation: int, lpk: str) -> dict[str, int | None]:
        values = self.client.hmget(self.key("bm", generation, lpk), list(self.META_FIELDS))
        return {name: _int(value) for name, value in zip(self.META_FIELDS, values, strict=True)}

    def bar_rows(self, generation: int, lpk: str, interval_ms: int, opens: Iterable[int]) -> dict[int, bytes | None]:
        by_bucket: dict[int, list[int]] = {}
        for open_ms in sorted(opens):
            by_bucket.setdefault(bucket_of(open_ms, interval_ms), []).append(open_ms)
        ordered = sorted(by_bucket.items())
        replies = self._pipeline([("hmget", (self.key("b", generation, lpk, bucket), [str(o) for o in items]))
                                  for bucket, items in ordered])
        rows: dict[int, bytes | None] = {}
        for (_bucket, items), values in zip(ordered, replies, strict=True):
            for open_ms, value in zip(items, values, strict=True):
                rows[open_ms] = None if value is None else bytes(value)
        return rows

    def bucket_row_count(self, generation: int, lpk: str, interval_ms: int, first: int | None,
                         last: int | None) -> int:
        """``cache.rs`` ``bar_row_count``: HLEN over the buckets of first..=last."""

        if first is None or last is None:
            return 0
        low, high = bucket_of(first, interval_ms), bucket_of(last, interval_ms)
        if high - low + 1 > MAX_BUCKETS:
            raise Refused(f"bar meta range spans more than {MAX_BUCKETS} buckets: {lpk}")
        replies = self._pipeline([("hlen", (self.key("b", generation, lpk, bucket),))
                                  for bucket in range(low, high + 1)])
        return sum(int(value) for value in replies)

    def conflicts(self, generation: int, lpk: str) -> list[str]:
        return [_text(item) or "" for item in self.client.lrange(self.key("cx", generation, lpk), 0, 99)]

    def latest(self, lpks_with_generation: Sequence[tuple[str, int]]) -> dict[str, list]:
        replies = self._pipeline([("hmget", (self.key("l", generation, lpk), ["v", "t", "p", "o"]))
                                  for lpk, generation in lpks_with_generation])
        return {lpk: reply for (lpk, _g), reply in zip(lpks_with_generation, replies, strict=True)}


# ------------------------------------------------------------------ BAR comparison (pure)

@dataclass(frozen=True)
class SpoolFact:
    open_ms: int
    revision: int
    content_sha256: str
    logical_offset: int


def spool_expected(facts: Iterable[tuple[int, bool, int, int, str, int]]) -> dict[int, SpoolFact]:
    """Per open the spool's final FINAL/REVISED fact with the highest revision.

    ``facts`` = (open_ms, is_final, lifecycle, revision, content_sha256,
    logical_offset) in spool order; an equal revision keeps the first (stage B
    keeps the first of an equal-revision conflict).
    """

    expected: dict[int, SpoolFact] = {}
    for open_ms, is_final, lifecycle, revision, sha, logical_offset in facts:
        if not is_final or lifecycle not in _FINAL_LIFECYCLES:
            continue
        current = expected.get(open_ms)
        if current is None or revision > current.revision:
            expected[open_ms] = SpoolFact(open_ms, revision, sha, logical_offset)
    return expected


def _conflict_mentions(conflicts: Sequence[str], open_ms: int, sha: str) -> bool:
    for item in conflicts:
        try:
            record = json.loads(item)
        except ValueError:
            continue
        if record.get("open_ms") == open_ms and sha in (record.get("kept"), record.get("refused")):
            return True
    return False


def compare_bars(expected: Mapping[int, SpoolFact], cache_rows: Mapping[int, bytes | None], source: Any,
                 lpk: LogicalProductKey, *, floor: int | None, floor_aware: bool,
                 conflicts: Sequence[str] = ()) -> dict[str, Any]:
    """Classify every expected open against the cache row read for it."""

    result: dict[str, Any] = {"spool_final": len(expected), "checked": 0, "covered": 0, "content_equal": 0,
                              "content_differs": 0, "missing": 0, "below_floor": 0, "undecodable": 0,
                              "identity_mismatch": 0, "covered_opens": set(), "details": []}

    def detail(kind: str, **fields: Any) -> None:
        if len(result["details"]) < MAX_LISTED:
            result["details"].append({"kind": kind, **fields})

    for open_ms in sorted(expected):
        fact = expected[open_ms]
        below = floor is not None and open_ms < floor
        if below:
            result["below_floor"] += 1
            if floor_aware:
                continue
        result["checked"] += 1
        row = cache_rows.get(open_ms)
        if row is None:
            result["missing"] += 1
            detail("missing", open_ms=open_ms, reason="absent", below_floor=below)
            continue
        try:
            decoded = decode_bar_row(row, lpk)
            envelope = market_data_pb2.EventEnvelope.FromString(decoded.canonical)
        except (StateCodecError, ValueError) as error:
            result["undecodable"] += 1
            detail("undecodable", open_ms=open_ms, error=str(error))
            continue
        if not bar_envelope_matches_binding(envelope, source) or envelope.bar.open_time_ns != open_ms * 1_000_000:
            result["identity_mismatch"] += 1
            detail("identity_mismatch", open_ms=open_ms)
            continue
        if not envelope.bar.is_final or envelope.bar.lifecycle not in _FINAL_LIFECYCLES:
            result["missing"] += 1
            detail("missing", open_ms=open_ms, reason="not_final_in_cache", below_floor=below)
            continue
        result["covered"] += 1
        result["covered_opens"].add(open_ms)
        cache_sha = hashlib.sha256(decoded.canonical).hexdigest()
        if cache_sha == fact.content_sha256:
            result["content_equal"] += 1
            continue
        result["content_differs"] += 1
        explained = None
        if envelope.bar.revision > fact.revision:
            explained = "higher_revision"
        elif _conflict_mentions(conflicts, open_ms, fact.content_sha256):
            explained = "conflict_record"
        detail("content_differs", open_ms=open_ms, spool_sha256=fact.content_sha256, cache_sha256=cache_sha,
               spool_revision=fact.revision, cache_revision=envelope.bar.revision, explained_by=explained,
               spool_logical_offset=fact.logical_offset)
    return result


def spool_facts_for_binding(connection, index: str, stream: str, item, page_rows: int) -> tuple[dict, list]:
    """The importer's export of one binding, as (summary, facts) for ``spool_expected``."""

    summary, frames = IMPORTER.export_binding(connection, index, stream, item, materializer_epoch=1,
                                              partitions=1, page_rows=page_rows, keep_frames=True)
    facts = []
    for exported in frames:
        frame = decode_frame(exported.value)
        lifecycle = market_data_pb2.EventEnvelope.FromString(frame.envelope).bar.lifecycle
        facts.append((frame.open_time_ms, bool(frame.is_final), lifecycle, frame.revision, frame.content_sha256,
                      frame.legacy.spool_logical_offset))
    return summary, facts


def load_import_receipt(path: Path, *, environment: str, catalog_info: Mapping[str, Any],
                        bindings: Sequence[Any]) -> dict[str, Any]:
    """A PASS receipt of ``kn_bar_legacy_import.py`` whose hashes and plan match this check."""

    try:
        receipt = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Refused(f"import receipt is unreadable: {error}") from error
    if not isinstance(receipt, dict) or receipt.get("schema") != IMPORTER.SCHEMA + ".receipt":
        raise Refused("import receipt schema differs")
    if receipt.get("mode") not in ("import", "export") or receipt.get("status") != "PASS":
        raise Refused("import receipt is not a PASS export/import receipt")
    body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if IMPORTER.canonical_sha256(body) != receipt.get("receipt_sha256"):
        raise Refused("import receipt_sha256 does not match its content")
    if IMPORTER.canonical_sha256({"bindings": receipt.get("bindings"), "totals": receipt.get("totals")}) \
            != receipt.get("export_sha256"):
        raise Refused("import export_sha256 does not match its bindings/totals")
    plan = receipt.get("plan") or {}
    catalog = plan.get("catalog") or {}
    if plan.get("environment") != environment:
        raise Refused(f"import receipt environment {plan.get('environment')!r} differs from {environment!r}")
    if (catalog.get("sha256"), catalog.get("revision")) != (catalog_info["sha256"], catalog_info["revision"]):
        raise Refused("import receipt catalog (sha256/revision) differs from --catalog")
    by_binding = {item["binding_id"]: item for item in receipt["bindings"]}
    expected = {item.binding.binding_id: item.lpk.encode() for item in bindings}
    if {key: value["lpk"] for key, value in by_binding.items()} != expected:
        raise Refused("import receipt bindings/products differ from the catalog")
    return {"path": str(path), "receipt_sha256": receipt["receipt_sha256"],
            "export_sha256": receipt["export_sha256"], "spool_cache_id": (plan.get("sqlite") or {}).get("cache_id"),
            "by_binding": by_binding}


def import_window(facts: Sequence[tuple], summary: Mapping[str, Any], imported: Mapping[str, Any]
                  ) -> tuple[list[tuple], dict[str, Any]]:
    """Facts at or below the binding's import cutoff, and what changed since the import.

    Spool logical offsets are dense per partition key (``next_offset``; the
    retention trim removes the oldest), so rows trimmed since the import are
    ``current first - import first`` (bounded by the cutoff).
    """

    cutoff = imported["spool_cutoff_logical_offset"]
    first = imported["spool_first_logical_offset"]
    kept = [fact for fact in facts if cutoff is not None and fact[5] <= cutoff]
    trimmed = 0
    if cutoff is not None and first is not None:
        current = summary["spool_first_logical_offset"]
        trimmed = max(0, min(cutoff + 1, current if current is not None else cutoff + 1) - first)
    info = {"import_cutoff_logical_offset": cutoff, "import_first_logical_offset": first,
            "spool_rows_at_import": imported["rows"], "trimmed_since_import": trimmed,
            "appended_since_import": len(facts) - len(kept)}
    info["import_rows_accounted"] = len(kept) + trimmed == imported["rows"]
    return kept, info


def check_bars(*, sqlite_path: Path, catalog_path: Path, environment: str, client: Any, floor_aware: bool,
               page_rows: int = 1000, import_receipt: Path | None = None) -> dict[str, Any]:
    bindings, catalog_info = IMPORTER.load_bar_bindings(catalog_path, environment)
    imported = (None if import_receipt is None else
                load_import_receipt(import_receipt, environment=environment, catalog_info=catalog_info,
                                    bindings=bindings))
    stream = catalog_info["canonical_stream"]
    cache = CacheReader(client, environment)
    readback = KnBarReadback(client, environment)
    products: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    spool_rows_read = 0
    connection = IMPORTER.open_spool_readonly(sqlite_path)
    try:
        index = IMPORTER.primary_key_index(connection)
        if imported is not None and imported["spool_cache_id"] != IMPORTER.spool_cache_id(connection):
            raise Refused("import receipt spool cache_id differs from --sqlite (another spool generation)")
        for item in bindings:
            lpk = item.lpk.encode()
            summary, facts = spool_facts_for_binding(connection, index, stream, item, page_rows)
            spool_rows_read += summary["rows"]
            window: dict[str, Any] = {}
            if imported is not None:
                facts, window = import_window(facts, summary, imported["by_binding"][item.binding.binding_id])
            expected = spool_expected(facts)
            before = cache.pointers([lpk])[lpk]
            entry: dict[str, Any] = {"binding_id": item.binding.binding_id, "lpk": lpk,
                                     "pointer": pointer_state(before), "ready_generation": before.ready,
                                     "spool_rows": len(facts), "spool_final": len(expected), **window}
            product_failures: list[str] = []
            if window and not window["import_rows_accounted"]:
                product_failures.append("import_rows_mismatch")
            if before.ready is None:
                if expected:
                    product_failures.append("not_ready")
                entry.update(checked=0, covered=0, content_equal=0, content_differs=0,
                             missing=len(expected), below_floor=0)
            else:
                interval_ms = canonical_interval_ms(item.binding.interval)
                meta = cache.bar_meta(before.ready, lpk)
                rows = cache.bar_rows(before.ready, lpk, interval_ms, expected)
                conflicts = cache.conflicts(before.ready, lpk) if meta.get("conflicts") else []
                compared = compare_bars(expected, rows, item.binding, item.lpk, floor=meta["floor"],
                                        floor_aware=floor_aware, conflicts=conflicts)
                counted = cache.bucket_row_count(before.ready, lpk, interval_ms, meta["first"], meta["last"])
                covered = compared.pop("covered_opens")
                checked_opens = frozenset(open_ms for open_ms in expected
                                          if not (floor_aware and meta["floor"] is not None
                                                  and open_ms < meta["floor"]))
                try:
                    readback_opens = readback.durable_final_bar_opens(item.binding, checked_opens)
                    readback_equal = readback_opens == covered
                except RuntimeError as error:
                    readback_equal = False
                    entry["readback_error"] = str(error)
                entry.update(compared, meta=meta, counted_bucket_rows=counted,
                             meta_rows_equal=(meta["rows"] or 0) == counted, readback_equal=readback_equal)
                if expected and (compared["missing"] or compared["content_differs"] or compared["undecodable"]
                                 or compared["identity_mismatch"]):
                    product_failures.append("bar_parity")
                if not entry["meta_rows_equal"]:
                    product_failures.append("meta_rows")
                if not readback_equal:
                    product_failures.append("readback")
            after = cache.pointers([lpk])[lpk]
            if after != before:
                product_failures.append("generation_changed")
            entry["failures"] = product_failures
            if product_failures and len(failures) < MAX_FAILURES:
                failures.append({"binding_id": item.binding.binding_id, "lpk": lpk, "failures": product_failures})
            products.append(entry)
    finally:
        connection.close()
    keys = ("spool_rows", "spool_final", "checked", "covered", "content_equal", "content_differs", "missing",
            "below_floor")
    totals = {key: sum(item.get(key, 0) for item in products) for key in keys}
    totals.update(bindings=len(products), bindings_with_spool_final=sum(1 for p in products if p["spool_final"]),
                  failing_products=sum(1 for p in products if p["failures"]), spool_rows_read=spool_rows_read)
    if imported is not None:
        totals.update(trimmed_since_import=sum(p["trimmed_since_import"] for p in products),
                      appended_since_import=sum(p["appended_since_import"] for p in products))
    return {"check": "bars", "inputs": {"sqlite": str(sqlite_path), "catalog": catalog_info,
                                        "environment": environment, "floor_aware": floor_aware,
                                        "primary_key_index": index,
                                        "import_receipt": None if imported is None else {
                                            key: imported[key] for key in ("path", "receipt_sha256",
                                                                           "export_sha256", "spool_cache_id")}},
            "verdict": "PASS" if not failures else "FAIL", "failures": failures, "totals": totals,
            "products": products}


# ------------------------------------------------------------------ bundle

@dataclass(frozen=True)
class BundleProduct:
    binding_id: str
    lpk: str
    feed: str
    physical_key: str


def load_bundle(path: Path, environment: str) -> tuple[dict[str, Any], list[BundleProduct]]:
    """A verified gateway bundle and its products (the ``products.rs`` checks)."""

    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if IMPORTER._load_bundle_module().bundle_sha256(document) != document.get("sha256"):
        raise Refused("gateway bundle hash does not match its content")
    if document.get("environment") != environment:
        raise Refused(f"gateway bundle environment {document.get('environment')!r} differs from {environment!r}")
    products: list[BundleProduct] = []
    seen: set[tuple[str, str]] = set()
    for binding in document["catalog"]["bindings"]:
        lpk = LogicalProductKey.for_product(
            environment=environment, venue=binding["venue"], market=binding["market"],
            instrument_uid=binding["instrument_uid"], feed=binding["feed"], interval=binding["interval"]).encode()
        if lpk != binding["product_key"]:
            raise Refused(f"binding {binding['binding_id']} product key differs from the derivation")
        pair = (binding["physical_key"], binding["feed"])
        if pair in seen:
            raise Refused(f"two bindings share physical key {pair[0]} and feed {pair[1]}")
        seen.add(pair)
        products.append(BundleProduct(binding["binding_id"], lpk, binding["feed"], binding["physical_key"]))
    return document, sorted(products, key=lambda item: item.lpk)


def check_ready(*, bundle_path: Path, environment: str, client: Any) -> dict[str, Any]:
    _document, products = load_bundle(bundle_path, environment)
    pointers = CacheReader(client, environment).pointers([item.lpk for item in products])
    rows = []
    summary: dict[str, dict[str, int]] = {}
    for item in products:
        state = pointer_state(pointers[item.lpk])
        rows.append({"binding_id": item.binding_id, "lpk": item.lpk, "feed": item.feed, "state": state,
                     "ready_generation": pointers[item.lpk].ready, "staging_generation": pointers[item.lpk].staging})
        counts = summary.setdefault(item.feed, {"READY": 0, "REBUILDING": 0, "STAGING": 0, "ABSENT": 0})
        counts[state] += 1
    failures = [{"lpk": row["lpk"], "state": row["state"]} for row in rows
                if row["state"] not in ("READY", "REBUILDING")][:MAX_FAILURES]
    totals = {state: sum(counts[state] for counts in summary.values())
              for state in ("READY", "REBUILDING", "STAGING", "ABSENT")}
    totals["products"] = len(rows)
    return {"check": "ready", "inputs": {"bundle": str(bundle_path), "environment": environment},
            "verdict": "PASS" if not failures else "FAIL", "failures": failures, "totals": totals,
            "per_feed": dict(sorted(summary.items())), "products": rows}


# ------------------------------------------------------------------ latest (pure parts)

@dataclass(frozen=True)
class OracleRecord:
    partition: int
    offset: int
    canonical: bytes


def payload_feed(canonical: bytes) -> str:
    """``state_codec.rs`` ``payload_feed``: the oneof name upper-cased."""

    return (market_data_pb2.EventEnvelope.FromString(canonical).WhichOneof("payload") or "").upper()


def latest_oracle(records: Iterable[tuple[int, int, bytes, bytes]],
                  product_of: Callable[[str, str], str | None]) -> tuple[dict[str, OracleRecord], dict[str, Any]]:
    """Last committed record per product; BAR excluded; unmapped/ambiguous reported."""

    oracle: dict[str, OracleRecord] = {}
    stats: dict[str, Any] = {"records": 0, "bar_records": 0, "unmapped": 0, "ambiguous": [], "unmapped_keys": []}
    for partition, offset, key, value in records:
        stats["records"] += 1
        feed = payload_feed(value)
        if feed == "BAR":
            stats["bar_records"] += 1
            continue
        physical_key = key.decode("utf-8")
        lpk = product_of(physical_key, feed)
        if lpk is None:
            stats["unmapped"] += 1
            if len(stats["unmapped_keys"]) < MAX_LISTED:
                stats["unmapped_keys"].append(f"{physical_key}:{feed}")
            continue
        current = oracle.get(lpk)
        if current is not None and current.partition != partition:
            if lpk not in stats["ambiguous"]:
                stats["ambiguous"].append(lpk)
            continue
        if current is None or offset > current.offset:
            oracle[lpk] = OracleRecord(partition, offset, bytes(value))
    return oracle, stats


def compare_latest(oracle: OracleRecord | None, pointer: Pointer, fields: Sequence[Any] | None,
                   topic_id: str | None) -> tuple[str, dict[str, Any]]:
    """One product: equal | stale | ahead | missing | not_ready | mismatch | unexpected."""

    if pointer.ready is None:
        return ("not_ready" if oracle is not None else "absent"), {}
    value, t, p, o = fields if fields is not None else (None, None, None, None)
    if value is None:
        return ("missing" if oracle is not None else "absent"), {}
    if oracle is None:
        return "unexpected", {"cache_offset": _int(o)}
    try:
        decoded = decode_latest_value(bytes(value))
    except StateCodecError as error:
        return "mismatch", {"error": str(error)}
    info = {"oracle_offset": oracle.offset, "cache_offset": decoded.source_offset,
            "oracle_partition": oracle.partition, "cache_partition": _int(p), "field_offset": _int(o),
            "topic_id": _text(t)}
    if decoded.source_offset < oracle.offset:
        return "stale", info
    if decoded.source_offset > oracle.offset:
        return "ahead", info
    if (decoded.canonical != oracle.canonical or _int(o) != oracle.offset or _int(p) != oracle.partition
            or (topic_id is not None and _text(t) != topic_id)):
        return "mismatch", info
    return "equal", info


def read_canonical_topic(*, bootstrap: str, topic: str, security: Mapping[str, str], timeout_s: float
                         ) -> tuple[Iterable[tuple[int, int, bytes, bytes]], dict[str, Any]]:
    """Every committed record from earliest to the end captured now (assign mode)."""

    from confluent_kafka import Consumer, KafkaError, TopicPartition

    client_id = f"kn3-flow-check-{uuid.uuid4().hex[:12]}"
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": client_id, "client.id": client_id,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "isolation.level": "read_committed", "enable.partition.eof": True, **security})
    metadata = consumer.list_topics(topic, timeout=30)
    entry = metadata.topics.get(topic)
    if entry is None or entry.error is not None or not entry.partitions:
        consumer.close()
        raise Refused(f"canonical topic {topic} is not readable")
    ends: dict[int, tuple[int, int]] = {}
    for partition in sorted(entry.partitions):
        ends[partition] = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
    cutoff = {"topic": topic, "isolation": "read_committed",
              "partitions": {str(p): {"earliest": low, "end": high} for p, (low, high) in ends.items()}}

    def records():
        pending = {p for p, (low, high) in ends.items() if high > low}
        try:
            if not pending:
                return
            consumer.assign([TopicPartition(topic, p, ends[p][0]) for p in sorted(pending)])
            deadline = time.monotonic() + timeout_s
            while pending:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"canonical read did not reach the captured end: {sorted(pending)}")
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        position = message.offset()
                        if position is not None and position >= ends[message.partition()][1]:
                            pending.discard(message.partition())
                        continue
                    raise RuntimeError(str(message.error()))
                partition, offset = message.partition(), message.offset()
                if offset >= ends[partition][1]:
                    pending.discard(partition)
                    continue
                yield partition, offset, message.key() or b"", message.value() or b""
                if offset + 1 >= ends[partition][1]:
                    pending.discard(partition)
        finally:
            consumer.close()

    return records(), cutoff


def check_latest(*, bundle_path: Path, environment: str, client: Any,
                 records: Iterable[tuple[int, int, bytes, bytes]], cutoff: Mapping[str, Any],
                 topic_id: str | None) -> dict[str, Any]:
    _document, products = load_bundle(bundle_path, environment)
    by_pair = {(item.physical_key, item.feed): item.lpk for item in products}
    oracle, stats = latest_oracle(records, lambda key, feed: by_pair.get((key, feed)))
    latest_products = [item for item in products if item.feed != "BAR"]
    cache = CacheReader(client, environment)
    pointers = cache.pointers([item.lpk for item in latest_products])
    ready = [(item.lpk, pointers[item.lpk].ready) for item in latest_products if pointers[item.lpk].ready is not None]
    fields = cache.latest(ready)
    counts = {name: 0 for name in ("equal", "stale", "ahead", "missing", "not_ready", "mismatch", "unexpected")}
    rows, failures = [], []
    for item in latest_products:
        category, info = compare_latest(oracle.get(item.lpk), pointers[item.lpk], fields.get(item.lpk), topic_id)
        if category == "absent":
            continue
        counts[category] += 1
        rows.append({"binding_id": item.binding_id, "lpk": item.lpk, "feed": item.feed, "result": category, **info})
        if category != "equal" and len(failures) < MAX_FAILURES:
            failures.append({"lpk": item.lpk, "result": category, **info})
    for lpk in stats["ambiguous"]:
        failures.append({"lpk": lpk, "result": "ambiguous_partition"})
    if stats["unmapped"]:
        failures.append({"result": "unmapped_records", "count": stats["unmapped"], "keys": stats["unmapped_keys"]})
    totals = {"products": len(oracle), **counts, "canonical_records": stats["records"],
              "bar_records_skipped": stats["bar_records"], "unmapped_records": stats["unmapped"]}
    return {"check": "latest", "inputs": {"bundle": str(bundle_path), "environment": environment,
                                          "topic_id": topic_id, "cutoff": dict(cutoff)},
            "verdict": "PASS" if not failures and counts["equal"] == len(oracle) else "FAIL",
            "failures": failures, "totals": totals, "products": rows}


# ------------------------------------------------------------------ cli

def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="check", required=True)
    for name in ("bars", "latest", "ready"):
        item = sub.add_parser(name)
        item.add_argument("--environment", required=True)
        item.add_argument("--cache-url", required=True, help="the isolated market cache Redis URL")
        item.add_argument("--out", type=Path, required=True)
        if name == "bars":
            item.add_argument("--sqlite", type=Path, required=True)
            item.add_argument("--catalog", type=Path, default=IMPORTER.DEFAULT_CATALOG)
            item.add_argument("--floor-aware", action="store_true")
            item.add_argument("--page-rows", type=int, default=1000)
            item.add_argument("--import-receipt", type=Path,
                              help="check only rows at/below each binding's cutoff in this import receipt")
        else:
            item.add_argument("--bundle", type=Path, required=True)
        if name == "latest":
            item.add_argument("--bootstrap", required=True)
            item.add_argument("--canonical-topic", default="md.canonical.v2")
            item.add_argument("--topic-id", help="expected `t` field (QDL_KN_TOPIC_ID of the projector)")
            item.add_argument("--timeout-s", type=float, default=900.0)
            item.add_argument("--ca", type=Path)
            item.add_argument("--cert", type=Path)
            item.add_argument("--key", type=Path)
    return parser.parse_args(list(argv) if argv is not None else None)


def run(args: argparse.Namespace, client: Any = None) -> dict[str, Any]:
    if client is None:
        import redis

        client = redis.Redis.from_url(args.cache_url, decode_responses=False)
    if args.check == "bars":
        if not 1 <= args.page_rows <= 10_000:
            raise Refused("--page-rows must be within 1..10000")
        return check_bars(sqlite_path=args.sqlite, catalog_path=args.catalog, environment=args.environment,
                          client=client, floor_aware=args.floor_aware, page_rows=args.page_rows,
                          import_receipt=args.import_receipt)
    if args.check == "ready":
        return check_ready(bundle_path=args.bundle, environment=args.environment, client=client)
    security = IMPORTER.kafka_security(args)
    records, cutoff = read_canonical_topic(bootstrap=args.bootstrap, topic=args.canonical_topic,
                                           security=security, timeout_s=args.timeout_s)
    return check_latest(bundle_path=args.bundle, environment=args.environment, client=client, records=records,
                        cutoff=cutoff, topic_id=args.topic_id)


def main(argv: Sequence[str] | None = None, client: Any = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    try:
        receipt = run(args, client)
    except Refused as error:
        receipt = {"check": args.check, "verdict": "REFUSED", "failures": [{"error": str(error)}]}
    except Exception as error:  # redis/Kafka/SQLite errors: typed ERROR receipt, never a PASS
        receipt = {"check": args.check, "verdict": "ERROR",
                   "failures": [{"error": f"{type(error).__name__}: {error}"}]}
    receipt = {"schema": SCHEMA + ".receipt", **receipt, "elapsed_s": round(time.monotonic() - started, 3)}
    receipt["receipt_sha256"] = IMPORTER.canonical_sha256(receipt)
    args.out.write_text(json.dumps(receipt, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"check": args.check, "verdict": receipt["verdict"], "totals": receipt.get("totals"),
                      "elapsed_s": receipt["elapsed_s"], "out": str(args.out)}, sort_keys=True))
    return EXIT[receipt["verdict"]]


if __name__ == "__main__":
    sys.exit(main())

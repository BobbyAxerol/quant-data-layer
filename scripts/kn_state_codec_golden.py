#!/usr/bin/env python3
"""KN-3 state codec golden (``contracts/golden/kn_v220/state_codec.json``).

Purpose: freeze the cross-language oracle for ``qdl/projection/kn_state_codec.py``
and ``rust/qdl-contracts/src/state_codec.rs`` from **real** canonical records
(a read-only sample of the stable spool ``md.canonical.v2``, fields
``physical_key, spool_offset, feed, tag, venue, payload_b64``). Every valid
vector's canonical bytes are real; the Kafka source coordinates, epochs and
every fault vector are generator-assigned and marked ``synthetic``. The
in-progress/revised BAR records are real finals with the lifecycle flipped in
a copy, also marked ``synthetic``.

Boundary: pure computation over the sample file; no network, no runtime
state. Writes one JSON file (``golden``) or, for the full-sample Rust/Python
cross-check, one scratch export and a verification of the Rust output
(``cross-export`` / ``cross-verify``). Run in ``qdl-v2-python`` with
``PYTHONPATH=/src``:

  python -B scripts/kn_state_codec_golden.py golden --sample S.json --out state_codec.json
  python -B scripts/kn_state_codec_golden.py cross-export --sample S.json --out cross.json
  python -B scripts/kn_state_codec_golden.py cross-verify --cross cross.json --rust rust.json
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Callable, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.marketdata.v2 import market_data_pb2  # noqa: E402
from qdl.projection import kn_state_codec as codec  # noqa: E402
from qdl.projection.state_contract import MAX_OFFSET, LogicalProductKey, SourceCoordinate  # noqa: E402

SCHEMA = "qdl.kn.v220.state-codec-golden.v1"
ENVIRONMENT = "paper"
SPOOL_STREAM = "md.canonical.v2"
TOPIC_ID = "ljfjPYApRpWQd79McfTtZg"
PARTITION_COUNTS = (1, 3, 6, 12, 48)
KAFKA_MURMUR2 = (  # org.apache.kafka.common.utils.UtilsTest.testMurmur2
    (b"21", -973932308),
    (b"foobar", -790332482),
    (b"a-little-bit-long-string", -985981536),
    (b"a-little-bit-longer-string", -1486304829),
    (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
    (b"abc", 479470107),
)


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def product_key(canonical: bytes) -> LogicalProductKey:
    """The LPK of a canonical record, exactly as ``kn_resource_sizing`` derives it:
    venue/market/uid from the envelope, feed = upper-case payload name,
    qualifier = bar interval or ``-``."""

    envelope = market_data_pb2.EventEnvelope.FromString(canonical)
    payload = envelope.WhichOneof("payload")
    return LogicalProductKey.for_product(
        environment=ENVIRONMENT, venue=envelope.venue, market=envelope.market,
        instrument_uid=envelope.instrument_uid, feed=payload.upper(),
        interval=envelope.bar.interval if payload == "bar" else None)


def load_sample(path: Path) -> list[dict[str, Any]]:
    records = json.loads(path.read_text(encoding="utf-8"))
    for record in records:
        record["canonical"] = base64.b64decode(record["payload_b64"])
    return records


def _coordinates(index: int) -> tuple[SourceCoordinate, int]:
    """Generator-assigned Kafka coordinate and epoch; covers both range edges."""

    if index == 0:
        return SourceCoordinate(TOPIC_ID, 0, 0), 1
    if index == 1:
        return SourceCoordinate(TOPIC_ID, (1 << 31) - 1, MAX_OFFSET), MAX_OFFSET
    return SourceCoordinate(TOPIC_ID, index % 12, 1_000_000 + index * 7919), 1 + index % 3


def _flip(canonical: bytes, *, is_final: bool, lifecycle: int, revision: int) -> bytes:
    envelope = market_data_pb2.EventEnvelope.FromString(canonical)
    envelope.bar.is_final = is_final
    envelope.bar.lifecycle = lifecycle
    envelope.bar.revision = revision
    return envelope.SerializeToString()


def _encodings(name: str, record: dict[str, Any], canonical: bytes, index: int, *,
               synthetic: bool, tag: str | None = None, legacy: bool = True) -> dict[str, Any]:
    lpk = product_key(canonical)
    source, epoch = _coordinates(index)
    lineage = codec.LegacyLineage(SPOOL_STREAM, record["physical_key"], record["spool_offset"])
    value = codec.encode_latest_value(canonical, source.offset, epoch)
    frames = {"LATEST": codec.latest_frame(canonical, lpk, source, epoch)}
    row = None
    if lpk.feed == "BAR":
        row = codec.encode_bar_row(canonical, lpk, source.offset, epoch)
        frames["BAR_REVISION"] = codec.bar_revision_frame(canonical, lpk, source, epoch)
        legacy_frame = codec.legacy_bar_frame(canonical, lpk, lineage, epoch)
        if legacy_frame.key() != frames["BAR_REVISION"].key():
            raise AssertionError("legacy and canonical BAR facts must share one key")
        if legacy:
            frames["LEGACY_BAR"] = legacy_frame
    headers = {kind: codec.canonical_json(frame.header()).decode("ascii") for kind, frame in frames.items()}
    keys = {"latest": frames["LATEST"].key(), "floor": codec.floor_key(lpk)}
    if "BAR_REVISION" in frames:
        keys["bar"] = frames["BAR_REVISION"].key()
    return {
        "name": name, "synthetic": synthetic, "tag": tag or record["tag"], "venue": record["venue"],
        "spool_stream": SPOOL_STREAM, "spool_partition_key": record["physical_key"],
        "spool_offset": record["spool_offset"], "lpk": lpk.encode(),
        "canonical_b64": _b64(canonical),
        "source": {"topic_id": source.topic_id, "partition": source.partition, "offset": source.offset},
        "materializer_epoch": epoch,
        "latest_value_trailer_hex": value[:codec.TRAILER_BYTES].hex(),
        "bar_row_b64": None if row is None else _b64(row),
        "headers": headers, "keys": keys,
        "partitions": {str(count): codec.state_partition(lpk, count) for count in PARTITION_COUNTS},
    }


def _select(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every feed and every BAR interval at least once, both venues, small."""

    first: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        first.setdefault((record["tag"], record["venue"]), record)
    chosen: list[dict[str, Any]] = []
    intervals = sorted({tag.split("|")[1] for tag, _venue in first if tag.startswith("bar|")},
                       key=lambda item: (item[-1], int(item[:-1])))
    for index, interval in enumerate(intervals):
        venues = sorted(venue for tag, venue in first if tag == f"bar|{interval}|final")
        chosen.append(first[(f"bar|{interval}|final", venues[index % len(venues)])])
    for feed in ("trade", "quote", "mark_index_price", "book_delta"):
        chosen.extend(first[(feed, venue)] for venue in ("BINANCE", "OKX") if (feed, venue) in first)
    snapshots = [record for (tag, _venue), record in first.items() if tag == "book_snapshot"]
    chosen.append(min(snapshots, key=lambda record: len(record["canonical"])))
    return chosen


def _record_name(record: dict[str, Any]) -> str:
    return f"{record['tag'].replace('|', '-')}-{record['venue'].lower()}"


def _frame_bytes(kind: int, header: bytes | str, body: bytes = b"", *, magic: bytes = codec.FRAME_MAGIC,
                 length: int | None = None) -> bytes:
    header = header.encode() if isinstance(header, str) else header
    size = len(header) if length is None else length
    return magic + bytes([kind]) + size.to_bytes(4, "big") + header + body


def _expect(function: Callable[[], Any], reason: str, detail: str, name: str) -> None:
    try:
        function()
    except codec.StateCodecError as error:
        if (error.reason, error.detail) != (reason, detail):
            raise AssertionError(f"{name}: got {error.reason}:{error.detail}, wanted {reason}:{detail}") from error
        return
    raise AssertionError(f"{name}: accepted, wanted {reason}:{detail}")


def _invalid_vectors(bar: dict[str, Any], trade: dict[str, Any], quote: dict[str, Any]) -> dict[str, list]:
    """Fault cases (all ``synthetic``) built from real frames; each reason at least once."""

    bar_canonical = base64.b64decode(bar["canonical_b64"])
    trade_canonical = base64.b64decode(trade["canonical_b64"])
    bar_lpk = LogicalProductKey.parse(bar["lpk"])
    trade_lpk = LogicalProductKey.parse(trade["lpk"])
    source = SourceCoordinate(**bar["source"])
    epoch = bar["materializer_epoch"]
    floor = codec.retention_floor_frame(bar_lpk, 1_790_000_000_000, 7)
    floor_header = codec.canonical_json(floor.header()).decode()
    bar_frame = codec.bar_revision_frame(bar_canonical, bar_lpk, source, epoch)
    bar_header = bar_frame.header()
    trade_frame = codec.latest_frame(trade_canonical, trade_lpk, SourceCoordinate(**trade["source"]), 3)
    trade_header = trade_frame.header()
    legacy_frame = codec.legacy_bar_frame(
        bar_canonical, bar_lpk, codec.LegacyLineage(SPOOL_STREAM, bar["spool_partition_key"], bar["spool_offset"]), 2)
    legacy_header = legacy_frame.header()

    def edited(header: dict[str, Any], **changes: Any) -> dict[str, Any]:
        copy = json.loads(json.dumps(header))
        for key, value in changes.items():
            if value is _DROP:
                copy.pop(key)
            else:
                copy[key] = value
        return copy

    def raw(header: dict[str, Any]) -> str:
        # Canonical layout for arbitrary (possibly invalid) values.
        return json.dumps(header, sort_keys=True, separators=(",", ":"))

    def rehash(header: dict[str, Any], body: bytes) -> dict[str, Any]:
        return edited(header, content_sha256=hashlib.sha256(body).hexdigest())

    tampered = bytearray(trade_canonical)
    tampered[-1] ^= 0x01
    garbage = b"\xff\xff\xff\xff"
    unaligned = market_data_pb2.EventEnvelope.FromString(bar_canonical)
    unaligned.bar.open_time_ns += 1
    unaligned_bytes = unaligned.SerializeToString()
    bar_kind, latest_kind, floor_kind, legacy_kind = 2, 1, 3, 4
    deep = "[" * 126 + "]" * 126  # the header object plus 126 arrays = 127 levels
    too_deep = "[" * 127 + "]" * 127
    frames = [
        ("short_prefix", b"QKS1\x03\x00\x00\x00", "TRUNCATED", ""),
        ("header_past_end", _frame_bytes(floor_kind, floor_header, length=len(floor_header) + 1), "TRUNCATED", ""),
        ("wrong_magic", _frame_bytes(floor_kind, floor_header, magic=b"QKS2"), "MAGIC", ""),
        ("kind_zero", _frame_bytes(0, floor_header), "KIND", ""),
        ("kind_five", _frame_bytes(5, floor_header), "KIND", ""),
        ("header_oversized", _frame_bytes(floor_kind, b"", length=codec.MAX_HEADER_BYTES + 1), "HEADER_SIZE", ""),
        ("header_empty", _frame_bytes(floor_kind, b""), "HEADER_JSON", ""),
        ("header_not_json", _frame_bytes(floor_kind, "{lpk}"), "HEADER_JSON", ""),
        ("header_array", _frame_bytes(floor_kind, "[1]"), "HEADER_JSON", ""),
        ("header_nan", _frame_bytes(floor_kind, floor_header.replace(":7}", ":NaN}")), "HEADER_JSON", ""),
        ("header_invalid_utf8", _frame_bytes(floor_kind, floor_header.encode().replace(b"paper", b"pap\xffr")),
         "HEADER_JSON", ""),
        ("header_bom", _frame_bytes(floor_kind, b"\xef\xbb\xbf" + floor_header.encode()), "HEADER_JSON", ""),
        ("header_lone_surrogate", _frame_bytes(floor_kind, floor_header.replace('"lpk":"', '"lpk":"\\ud800')),
         "HEADER_JSON", ""),
        ("header_float_overflow", _frame_bytes(floor_kind, floor_header.replace(":7}", ":1e400}")),
         "HEADER_JSON", ""),
        ("header_trailing_text", _frame_bytes(floor_kind, floor_header + "x"), "HEADER_JSON", ""),
        ("header_depth_128", _frame_bytes(floor_kind, floor_header.replace(f'"{floor.lpk.encode()}"', too_deep)),
         "HEADER_JSON", ""),
        ("header_depth_127_is_json", _frame_bytes(floor_kind, floor_header.replace(f'"{floor.lpk.encode()}"', deep)),
         "FIELD", "lpk"),
        ("missing_field", _frame_bytes(floor_kind, raw(edited(floor.header(), materializer_epoch=_DROP))),
         "HEADER_FIELDS", "header"),
        ("extra_field", _frame_bytes(floor_kind, raw(edited(floor.header(), extra=1))), "HEADER_FIELDS", "header"),
        ("latest_header_in_bar_frame", _frame_bytes(bar_kind, raw(trade_header), trade_canonical),
         "HEADER_FIELDS", "header"),
        ("source_missing_offset", _frame_bytes(latest_kind, raw(edited(trade_header, source={
            "partition": 0, "topic_id": TOPIC_ID})), trade_canonical), "HEADER_FIELDS", "source"),
        ("legacy_extra_field", _frame_bytes(legacy_kind, raw(edited(legacy_header, legacy={
            **legacy_header["legacy"], "offset": 1})), bar_canonical), "HEADER_FIELDS", "legacy"),
        ("content_hash_upper", _frame_bytes(latest_kind, raw(edited(
            trade_header, content_sha256=trade_header["content_sha256"].upper())), trade_canonical),
         "FIELD", "content_sha256"),
        ("event_id_odd", _frame_bytes(latest_kind, raw(edited(trade_header, event_id="abc")), trade_canonical),
         "FIELD", "event_id"),
        ("event_id_empty", _frame_bytes(latest_kind, raw(edited(trade_header, event_id="")), trade_canonical),
         "FIELD", "event_id"),
        ("is_final_integer", _frame_bytes(bar_kind, raw(edited(bar_header, is_final=1)), bar_canonical),
         "FIELD", "is_final"),
        ("open_time_float", _frame_bytes(bar_kind, raw(edited(bar_header, open_time_ms=float(
            bar_header["open_time_ms"]))), bar_canonical), "FIELD", "open_time_ms"),
        ("open_time_string", _frame_bytes(bar_kind, raw(edited(bar_header, open_time_ms=str(
            bar_header["open_time_ms"]))), bar_canonical), "FIELD", "open_time_ms"),
        ("open_time_bool", _frame_bytes(bar_kind, raw(edited(bar_header, open_time_ms=True)), bar_canonical),
         "FIELD", "open_time_ms"),
        ("revision_above_u32", _frame_bytes(bar_kind, raw(edited(bar_header, revision=1 << 32)), bar_canonical),
         "FIELD", "revision"),
        ("epoch_zero", _frame_bytes(floor_kind, raw(edited(floor.header(), materializer_epoch=0))),
         "FIELD", "materializer_epoch"),
        ("epoch_above_i63", _frame_bytes(floor_kind, raw(edited(floor.header(), materializer_epoch=MAX_OFFSET + 1))),
         "FIELD", "materializer_epoch"),
        ("epoch_above_u64", _frame_bytes(floor_kind, raw(edited(floor.header(), materializer_epoch=1 << 64))),
         "FIELD", "materializer_epoch"),
        ("floor_minus_zero", _frame_bytes(floor_kind, floor_header.replace(
            f":{floor.floor_open_time_ms},", ":-0,")), "FIELD", "floor_open_time_ms"),
        ("floor_negative", _frame_bytes(floor_kind, raw(edited(floor.header(), floor_open_time_ms=-1))),
         "FIELD", "floor_open_time_ms"),
        ("source_partition_above_i31", _frame_bytes(latest_kind, raw(edited(trade_header, source={
            **trade_header["source"], "partition": 1 << 31})), trade_canonical), "FIELD", "source.partition"),
        ("source_offset_above_i63", _frame_bytes(latest_kind, raw(edited(trade_header, source={
            **trade_header["source"], "offset": MAX_OFFSET + 1})), trade_canonical), "FIELD", "source.offset"),
        ("source_topic_charset", _frame_bytes(latest_kind, raw(edited(trade_header, source={
            **trade_header["source"], "topic_id": "a b"})), trade_canonical), "FIELD", "source.topic_id"),
        ("source_not_object", _frame_bytes(latest_kind, raw(edited(trade_header, source=[1])), trade_canonical),
         "FIELD", "source"),
        ("lpk_lowercase_venue", _frame_bytes(latest_kind, raw(edited(
            trade_header, lpk=trade_header["lpk"].replace(trade_lpk.venue, trade_lpk.venue.lower()))),
            trade_canonical), "FIELD", "lpk"),
        ("lpk_wrong_version", _frame_bytes(latest_kind, raw(edited(
            trade_header, lpk=trade_header["lpk"].replace("lpk1", "lpk2"))), trade_canonical), "FIELD", "lpk"),
        ("provenance_wrong", _frame_bytes(legacy_kind, raw(edited(legacy_header, provenance="canonical")),
                                          bar_canonical), "FIELD", "provenance"),
        ("legacy_offset_negative", _frame_bytes(legacy_kind, raw(edited(legacy_header, legacy={
            **legacy_header["legacy"], "spool_logical_offset": -1})), bar_canonical),
         "FIELD", "legacy.spool_logical_offset"),
        ("header_whitespace", _frame_bytes(floor_kind, floor_header.replace(",", ", ")), "NON_CANONICAL", ""),
        ("header_unsorted", _frame_bytes(floor_kind, json.dumps(floor.header(), separators=(",", ":"))),
         "NON_CANONICAL", ""),
        ("header_duplicate_key", _frame_bytes(floor_kind, floor_header[:-1] + ',"materializer_epoch":7}'),
         "NON_CANONICAL", ""),
        ("header_escaped_char", _frame_bytes(floor_kind, floor_header.replace("paper", "p\\u0061per")),
         "NON_CANONICAL", ""),
        ("floor_with_body", _frame_bytes(floor_kind, floor_header, b"\x00"), "BODY", ""),
        ("latest_without_body", _frame_bytes(latest_kind, raw(trade_header)), "BODY", ""),
        ("body_tampered", _frame_bytes(latest_kind, raw(trade_header), bytes(tampered)), "CONTENT_HASH", ""),
        ("body_not_protobuf", _frame_bytes(latest_kind, raw(rehash(trade_header, garbage)), garbage),
         "ENVELOPE", ""),
        ("bar_frame_trade_body", _frame_bytes(bar_kind, raw(edited(rehash(bar_header, trade_canonical),
                                                                    event_id=trade_header["event_id"])),
                                               trade_canonical), "NOT_BAR", ""),
        ("latest_other_uid", _frame_bytes(latest_kind, raw(edited(trade_header, lpk=trade_header["lpk"].replace(
            trade_lpk.instrument_uid, "00000000-0000-0000-0000-000000000000"))), trade_canonical),
         "PRODUCT_MISMATCH", "instrument_uid"),
        ("bar_other_interval", _frame_bytes(bar_kind, raw(edited(bar_header, lpk=bar["lpk"].rsplit("|", 1)[0]
                                                                  + "|7m")), bar_canonical),
         "PRODUCT_MISMATCH", "qualifier"),
        ("floor_not_bar_product", _frame_bytes(floor_kind, raw(edited(floor.header(), lpk=trade["lpk"]))),
         "PRODUCT_MISMATCH", "feed"),
        ("open_time_not_whole_ms", _frame_bytes(bar_kind, raw(rehash(bar_header, unaligned_bytes)), unaligned_bytes),
         "OPEN_TIME", ""),
        ("event_id_mismatch", _frame_bytes(latest_kind, raw(edited(trade_header, event_id="00" * 16)),
                                           trade_canonical), "HEADER_MISMATCH", "event_id"),
        ("open_time_mismatch", _frame_bytes(bar_kind, raw(edited(bar_header, open_time_ms=bar_header[
            "open_time_ms"] + 60_000)), bar_canonical), "HEADER_MISMATCH", "open_time_ms"),
        ("revision_mismatch", _frame_bytes(bar_kind, raw(edited(bar_header, revision=1)), bar_canonical),
         "HEADER_MISMATCH", "revision"),
        ("is_final_mismatch", _frame_bytes(bar_kind, raw(edited(bar_header, is_final=False)), bar_canonical),
         "HEADER_MISMATCH", "is_final"),
        ("legacy_is_final_mismatch", _frame_bytes(legacy_kind, raw(edited(legacy_header, is_final=False)),
                                                  bar_canonical), "HEADER_MISMATCH", "is_final"),
    ]
    bodies = {record["name"]: base64.b64decode(record["canonical_b64"]) for record in (bar, trade, quote)}
    invalid_frames = []
    for name, data, reason, detail in frames:
        _expect(lambda data=data: codec.decode_frame(data), reason, detail, name)
        invalid_frames.append({"name": name, "synthetic": True, **_compact(data, bodies, "frame_b64"),
                               "reason": reason, "detail": detail})
    quote_canonical = base64.b64decode(quote["canonical_b64"])
    quote_header = codec.latest_frame(quote_canonical, LogicalProductKey.parse(quote["lpk"]),
                                      SourceCoordinate(**quote["source"]), 3).header()
    other_feed = _frame_bytes(latest_kind, raw(edited(quote_header, lpk=quote["lpk"].replace("|QUOTE|", "|TRADE|"))),
                              quote_canonical)
    _expect(lambda: codec.decode_frame(other_feed), "PRODUCT_MISMATCH", "feed", "latest_other_feed")
    invalid_frames.append({"name": "latest_other_feed", "synthetic": True,
                           **_compact(other_feed, bodies, "frame_b64"), "reason": "PRODUCT_MISMATCH",
                           "detail": "feed"})

    row = codec.encode_bar_row(bar_canonical, bar_lpk, source.offset, epoch)
    trailer = row[:codec.TRAILER_BYTES]
    rows = [
        ("row_short", row[:47], bar["lpk"], "TRUNCATED", ""),
        ("row_epoch_zero", row[:8] + bytes(8) + row[16:], bar["lpk"], "TRAILER", ""),
        ("row_offset_above_i63", (MAX_OFFSET + 1).to_bytes(8, "big") + row[8:], bar["lpk"], "TRAILER", ""),
        ("row_body_not_protobuf", trailer + garbage, bar["lpk"], "ENVELOPE", ""),
        ("row_trade_body", trailer + trade_canonical, bar["lpk"], "NOT_BAR", ""),
        ("row_unstripped_canonical", trailer + bar_canonical, bar["lpk"], "NON_CANONICAL", ""),
        ("row_other_product", row, bar["lpk"].replace(bar_lpk.instrument_uid,
                                                      "00000000-0000-0000-0000-000000000000"), "CONTENT_HASH", ""),
        ("row_body_tampered", row[:-1] + bytes([row[-1] ^ 1]), bar["lpk"], None, None),
    ]
    invalid_rows = []
    for name, data, lpk_text, reason, detail in rows:
        if reason is None:
            try:
                codec.decode_bar_row(data, LogicalProductKey.parse(lpk_text))
            except codec.StateCodecError as error:
                reason, detail = error.reason, error.detail
            else:
                raise AssertionError(f"{name}: accepted")
        _expect(lambda data=data, lpk_text=lpk_text: codec.decode_bar_row(data, LogicalProductKey.parse(lpk_text)),
                reason, detail, name)
        invalid_rows.append({"name": name, "synthetic": True, **_compact(data, bodies, "row_b64"), "lpk": lpk_text,
                             "reason": reason, "detail": detail})

    value = codec.encode_latest_value(trade_canonical, 5, 1)
    values = [
        ("value_short", value[:10], "TRUNCATED", ""),
        ("value_epoch_zero", value[:8] + bytes(8) + value[16:], "TRAILER", ""),
        ("value_without_body", value[:codec.TRAILER_BYTES], "BODY", ""),
        ("value_tampered", value[:-1] + bytes([value[-1] ^ 1]), "CONTENT_HASH", ""),
    ]
    invalid_values = []
    for name, data, reason, detail in values:
        _expect(lambda data=data: codec.decode_latest_value(data), reason, detail, name)
        invalid_values.append({"name": name, "synthetic": True, "value_b64": _b64(data),
                               "reason": reason, "detail": detail})

    # Field 999 (varint 1) is unknown: prost drops it, so the bytes cannot round-trip.
    unknown_field = bar_canonical + _varint(999 << 3) + b"\x01"
    # schema_minor (field 3) sent a second time with its own value: same message,
    # non-canonical bytes.
    minor = market_data_pb2.EventEnvelope.FromString(bar_canonical).schema_minor
    repeated_scalar = bar_canonical + b"\x18" + _varint(minor)
    trade_source = {"topic_id": TOPIC_ID, "partition": 0, "offset": 5}
    encodes = [
        ("bar_row_trade", "bar_row", trade_canonical, bar["lpk"], {}, "NOT_BAR", ""),
        ("bar_row_other_uid", "bar_row", bar_canonical, bar["lpk"].replace(
            bar_lpk.instrument_uid, "00000000-0000-0000-0000-000000000000"), {}, "PRODUCT_MISMATCH", "instrument_uid"),
        ("bar_row_other_interval", "bar_row", bar_canonical, bar["lpk"].rsplit("|", 1)[0] + "|7m", {},
         "PRODUCT_MISMATCH", "qualifier"),
        ("bar_row_feed_trade", "bar_row", bar_canonical, bar["lpk"].replace("|BAR|", "|TRADE|"), {},
         "PRODUCT_MISMATCH", "feed"),
        ("bar_row_epoch_zero", "bar_row", bar_canonical, bar["lpk"], {"materializer_epoch": 0}, "TRAILER", ""),
        ("bar_row_offset_above_i63", "bar_row", bar_canonical, bar["lpk"], {"source_offset": MAX_OFFSET + 1},
         "TRAILER", ""),
        ("bar_row_garbage", "bar_row", garbage, bar["lpk"], {}, "ENVELOPE", ""),
        ("bar_row_unknown_field", "bar_row", unknown_field, bar["lpk"], {}, "ENVELOPE_NOT_CANONICAL", ""),
        ("bar_row_repeated_scalar", "bar_row", repeated_scalar, bar["lpk"], {}, "ENVELOPE_NOT_CANONICAL", ""),
        ("latest_value_empty", "latest_value", b"", trade["lpk"], {}, "BODY", ""),
        ("latest_value_epoch_above_i63", "latest_value", trade_canonical, trade["lpk"],
         {"materializer_epoch": MAX_OFFSET + 1}, "TRAILER", ""),
        ("latest_frame_empty", "LATEST", b"", trade["lpk"], {"source": trade_source}, "BODY", ""),
        ("latest_frame_other_venue", "LATEST", trade_canonical, trade["lpk"].replace(
            f"|{trade_lpk.venue}|", "|DERIBIT|"), {"source": trade_source}, "PRODUCT_MISMATCH", "venue"),
        ("latest_frame_partition_above_i31", "LATEST", trade_canonical, trade["lpk"],
         {"source": {**trade_source, "partition": 1 << 31}}, "FIELD", "source.partition"),
        ("latest_frame_epoch_zero", "LATEST", trade_canonical, trade["lpk"],
         {"source": trade_source, "materializer_epoch": 0}, "FIELD", "materializer_epoch"),
        ("bar_frame_trade", "BAR_REVISION", trade_canonical, bar["lpk"], {"source": trade_source}, "NOT_BAR", ""),
        ("bar_frame_unaligned_open", "BAR_REVISION", unaligned_bytes, bar["lpk"], {"source": trade_source},
         "OPEN_TIME", ""),
        ("legacy_frame_market", "LEGACY_BAR", bar_canonical, bar["lpk"].replace(
            f"|{bar_lpk.market}|", "|SPOT|"), {}, "PRODUCT_MISMATCH", "market"),
        ("legacy_frame_bad_stream", "LEGACY_BAR", bar_canonical, bar["lpk"], {"spool_stream": "md canonical"},
         "FIELD", "legacy.spool_stream"),
        ("floor_trade_product", "RETENTION_FLOOR", None, trade["lpk"], {"floor_open_time_ms": 0},
         "PRODUCT_MISMATCH", "feed"),
        ("floor_epoch_zero", "RETENTION_FLOOR", None, bar["lpk"], {"floor_open_time_ms": 0, "materializer_epoch": 0},
         "FIELD", "materializer_epoch"),
        ("floor_above_i63", "RETENTION_FLOOR", None, bar["lpk"], {"floor_open_time_ms": MAX_OFFSET + 1},
         "FIELD", "floor_open_time_ms"),
        ("partition_count_zero", "partition", None, bar["lpk"], {"partitions": 0}, "PARTITIONS", ""),
        ("partition_count_above_i31", "partition", None, bar["lpk"], {"partitions": 1 << 31}, "PARTITIONS", ""),
    ]
    refused = []
    for name, op, canonical, lpk_text, extra, reason, detail in encodes:
        case = {"name": name, "synthetic": True, "op": op, **_canonical_ref(canonical, bodies), "lpk": lpk_text,
                "source_offset": 5, "materializer_epoch": 1, "spool_stream": SPOOL_STREAM,
                "spool_partition_key": bar["spool_partition_key"], "spool_offset": bar["spool_offset"], **extra}
        _expect(lambda case=case: _run_encode(case, bodies), reason, detail, name)
        case.update(reason=reason, detail=detail)
        refused.append(case)
    return {"invalid_frames": invalid_frames, "invalid_bar_rows": invalid_rows,
            "invalid_latest_values": invalid_values, "refused_encodes": refused}


_DROP = object()


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _compact(data: bytes, bodies: dict[str, bytes], field: str) -> dict[str, str]:
    """Keep the golden small: a vector that ends with a golden record's canonical
    bytes is stored as its head plus the record name (bytes = head + canonical)."""

    for name, body in bodies.items():
        if data.endswith(body) and len(data) > len(body):
            return {"head_b64": _b64(data[:-len(body)]), "body_record": name}
    return {field: _b64(data)}


def _canonical_ref(canonical: bytes | None, bodies: dict[str, bytes]) -> dict[str, Any]:
    """Canonical input of a refused encode: a golden record plus suffix, raw bytes, or none."""

    if canonical is None:
        return {}
    for name, body in bodies.items():
        if canonical.startswith(body):
            return {"canonical_record": name, "canonical_suffix_hex": canonical[len(body):].hex()}
    return {"canonical_b64": _b64(canonical)}


def resolve_bytes(case: dict[str, Any], field: str, bodies: dict[str, bytes]) -> bytes:
    """Inverse of ``_compact``: the vector bytes from ``field`` or head + record."""

    if "body_record" in case:
        return base64.b64decode(case["head_b64"]) + bodies[case["body_record"]]
    return base64.b64decode(case[field])


def resolve_canonical(case: dict[str, Any], bodies: dict[str, bytes]) -> bytes:
    if "canonical_record" in case:
        return bodies[case["canonical_record"]] + bytes.fromhex(case["canonical_suffix_hex"])
    if "canonical_b64" in case:
        return base64.b64decode(case["canonical_b64"])
    return b""


def _run_encode(case: dict[str, Any], bodies: dict[str, bytes]) -> Any:
    canonical = resolve_canonical(case, bodies)
    lpk = LogicalProductKey.parse(case["lpk"])
    epoch = case["materializer_epoch"]
    op = case["op"]
    if op == "bar_row":
        return codec.encode_bar_row(canonical, lpk, case["source_offset"], epoch)
    if op == "latest_value":
        return codec.encode_latest_value(canonical, case["source_offset"], epoch)
    if op in ("LATEST", "BAR_REVISION"):
        source = case["source"]
        # Rust carries the partition as u32; build the coordinate without the
        # Python dataclass range check so both sides reach the header validation.
        coordinate = object.__new__(SourceCoordinate)
        for key, value in source.items():
            object.__setattr__(coordinate, key, value)
        factory = codec.latest_frame if op == "LATEST" else codec.bar_revision_frame
        return factory(canonical, lpk, coordinate, epoch)
    if op == "LEGACY_BAR":
        return codec.legacy_bar_frame(canonical, lpk, codec.LegacyLineage(
            case["spool_stream"], case["spool_partition_key"], case["spool_offset"]), epoch)
    if op == "RETENTION_FLOOR":
        return codec.retention_floor_frame(lpk, case["floor_open_time_ms"], epoch)
    if op == "partition":
        return codec.state_partition(lpk, case["partitions"])
    raise ValueError(op)


def build_golden(records: list[dict[str, Any]], sample_name: str) -> dict[str, Any]:
    chosen = _select(records)
    # The LEGACY_BAR header is pinned on the first four bars and the 1m bar
    # (it differs from BAR_REVISION only by lineage/provenance), to stay small.
    out = [_encodings(_record_name(record), record, record["canonical"], index, synthetic=False,
                      legacy=index < 4 or record["tag"].startswith("bar|1m|"))
           for index, record in enumerate(chosen)]
    one_minute = next(record for record in chosen if record["tag"].startswith("bar|1m|"))
    base = len(out)
    for offset, (suffix, flip) in enumerate((
        ("in-progress", {"is_final": False, "lifecycle": market_data_pb2.BAR_LIFECYCLE_IN_PROGRESS, "revision": 0}),
        ("revised", {"is_final": True, "lifecycle": market_data_pb2.BAR_LIFECYCLE_REVISED, "revision": 1}),
    )):
        canonical = _flip(one_minute["canonical"], **flip)
        out.append(_encodings(f"{_record_name(one_minute)}-{suffix}", one_minute, canonical, base + offset,
                              synthetic=True, tag=f"bar|1m|{suffix}", legacy=False))
    by_name = {record["name"]: record for record in out}
    trade = min((r for r in out if r["tag"] == "trade"), key=lambda r: len(r["canonical_b64"]))
    bar = next(r for r in out if r["tag"].startswith("bar|1m|") and not r["synthetic"])
    quote = next(r for r in out if r["tag"] == "quote")
    full_frames = []
    for record, kind in ((trade, "LATEST"), (bar, "BAR_REVISION"), (bar, "LEGACY_BAR"),
                         (by_name[f"{bar['name']}-in-progress"], "BAR_REVISION")):
        canonical = base64.b64decode(record["canonical_b64"])
        frame = _frame_bytes(int(codec.FrameKind[kind]), record["headers"][kind], canonical)
        full_frames.append({"record": record["name"], "kind": kind, "frame_b64": _b64(frame)})
    floors = []
    for lpk_text, floor_ms, epoch in ((bar["lpk"], 0, 1), (bar["lpk"], 1_790_000_000_000, MAX_OFFSET)):
        frame = codec.retention_floor_frame(LogicalProductKey.parse(lpk_text), floor_ms, epoch)
        floors.append({"lpk": lpk_text, "floor_open_time_ms": floor_ms, "materializer_epoch": epoch,
                       "frame_b64": _b64(frame.encode()), "key": frame.key()})
    murmur = [{"input_hex": data.hex(), "hash": expected, "source": "kafka UtilsTest.testMurmur2"}
              for data, expected in KAFKA_MURMUR2]
    for data, expected in murmur2_self_check():
        murmur.append({"input_hex": data.hex(), "hash": expected, "source": "cross-language"})
    return {
        "schema": SCHEMA,
        "generated_by": "scripts/kn_state_codec_golden.py",
        "provenance": {
            "sample": sample_name,
            "canonical_bytes": "real md.canonical.v2 records read from the stable spool (read-only sample)",
            "source_coordinates": "generator-assigned; the sample comes from the spool, not from Kafka",
            "legacy_lineage": "real spool stream, partition key and logical offset of each record",
            "synthetic": "records/vectors marked synthetic are derived test fixtures",
        },
        "constants": {"trailer_bytes": codec.TRAILER_BYTES, "frame_magic": codec.FRAME_MAGIC.decode(),
                      "frame_prefix_bytes": codec.FRAME_PREFIX_BYTES, "max_header_bytes": codec.MAX_HEADER_BYTES,
                      "max_json_depth": codec.MAX_JSON_DEPTH, "legacy_provenance": codec.LEGACY_PROVENANCE,
                      "murmur2_seed": codec.KAFKA_MURMUR2_SEED,
                      "kinds": {kind.name: int(kind) for kind in codec.FrameKind}},
        "reasons": list(codec.REASONS),
        "records": out,
        "full_frames": full_frames,
        "floor_frames": floors,
        "murmur2": murmur,
        **_invalid_vectors(bar, trade, quote),
    }


def murmur2_self_check() -> list[tuple[bytes, int]]:
    """Kafka's own vectors must hold before any other hash is recorded."""

    for known, expected in KAFKA_MURMUR2:
        if codec.murmur2(known) != expected:
            raise AssertionError(f"murmur2 disagrees with Kafka on {known!r}")
    inputs = (b"", b"a", b"ab", b"abc1", b"abcde", "lpk1|paper|OKX|SWAP|x|BAR|1m".encode(), bytes(range(256)))
    return [(data, codec.murmur2(data)) for data in inputs]


# ------------------------------------------------------------------ full-sample cross-check

def cross_export(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Python encodings of every sample record, for the Rust side to compare and decode."""

    items = []
    for index, record in enumerate(records):
        item = _encodings(f"{index:03d}-{_record_name(record)}", record, record["canonical"], index, synthetic=False)
        canonical = record["canonical"]
        lpk = LogicalProductKey.parse(item["lpk"])
        source, epoch = _coordinates(index)
        item["latest_value_b64"] = _b64(codec.encode_latest_value(canonical, source.offset, epoch))
        item["frames_b64"] = {kind: _b64(_frame_bytes(int(codec.FrameKind[kind]), header, canonical))
                              for kind, header in item["headers"].items()}
        for kind, frame in item["frames_b64"].items():
            if codec.decode_frame(base64.b64decode(frame)).envelope != canonical:
                raise AssertionError(f"{item['name']} {kind}: Python does not decode its own frame")
        if item["bar_row_b64"] is not None:
            if codec.decode_bar_row(base64.b64decode(item["bar_row_b64"]), lpk).canonical != canonical:
                raise AssertionError(f"{item['name']}: Python does not decode its own row")
        items.append(item)
    return {"schema": SCHEMA + ".cross", "records": items}


def cross_verify(cross: dict[str, Any], rust: dict[str, Any]) -> dict[str, Any]:
    """Decode every Rust-encoded value/row/frame in Python and compare bytes."""

    python = {item["name"]: item for item in cross["records"]}
    counts = {"records": 0, "latest_values": 0, "bar_rows": 0, "frames": 0, "mismatches": []}
    for item in rust["records"]:
        mine = python[item["name"]]
        canonical = base64.b64decode(mine["canonical_b64"])
        lpk = LogicalProductKey.parse(mine["lpk"])
        counts["records"] += 1
        checks = [("latest_value", item["latest_value_b64"], mine["latest_value_b64"],
                   lambda data: codec.decode_latest_value(data).canonical)]
        if mine["bar_row_b64"] is not None:
            checks.append(("bar_row", item["bar_row_b64"], mine["bar_row_b64"],
                           lambda data: codec.decode_bar_row(data, lpk).canonical))
        for kind, frame in mine["frames_b64"].items():
            checks.append((kind, item["frames_b64"][kind], frame, lambda data: codec.decode_frame(data).envelope))
        for label, rust_b64, python_b64, decode in checks:
            rust_bytes = base64.b64decode(rust_b64)
            python_bytes = base64.b64decode(python_b64)
            if rust_bytes != python_bytes:
                first = next((i for i, (a, b) in enumerate(zip(rust_bytes, python_bytes)) if a != b),
                             min(len(rust_bytes), len(python_bytes)))
                counts["mismatches"].append({"record": item["name"], "what": label, "first_differing_byte": first})
                continue
            if decode(rust_bytes) != canonical:
                counts["mismatches"].append({"record": item["name"], "what": label, "decode": "differs"})
                continue
            key = "frames" if label not in ("latest_value", "bar_row") else f"{label}s"
            counts[key] += 1
    return counts


def _dump(document: dict[str, Any]) -> str:
    """One top-level key per line and one list item per line: small and diffable."""

    compact = {"separators": (",", ":")}
    lines = []
    for key, value in document.items():
        if isinstance(value, list):
            items = ",\n".join("  " + json.dumps(item, **compact) for item in value)
            lines.append(f" {json.dumps(key)}:[\n{items}\n ]")
        else:
            lines.append(f" {json.dumps(key)}:{json.dumps(value, **compact)}")
    return "{\n" + ",\n".join(lines) + "\n}\n"


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    golden = commands.add_parser("golden")
    golden.add_argument("--sample", type=Path, required=True)
    golden.add_argument("--out", type=Path, required=True)
    export = commands.add_parser("cross-export")
    export.add_argument("--sample", type=Path, required=True)
    export.add_argument("--out", type=Path, required=True)
    verify = commands.add_parser("cross-verify")
    verify.add_argument("--cross", type=Path, required=True)
    verify.add_argument("--rust", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.command == "cross-verify":
        result = cross_verify(json.loads(args.cross.read_text()), json.loads(args.rust.read_text()))
        print(json.dumps(result, sort_keys=True))
        return 1 if result["mismatches"] else 0
    records = load_sample(args.sample)
    document = (build_golden(records, args.sample.name) if args.command == "golden"
                else cross_export(records))
    args.out.write_text(_dump(document), encoding="utf-8")
    print(json.dumps({"out": str(args.out), "bytes": args.out.stat().st_size,
                      "records": len(document["records"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

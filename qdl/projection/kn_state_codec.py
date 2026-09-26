"""KN-3 state codecs (contract sections 2-5, decisions D2-D4), shared with Rust.

The byte formats the Kafka-native projector writes and every reader decodes.
``rust/qdl-contracts/src/state_codec.rs`` implements the same rules; the
oracle both read is ``contracts/golden/kn_v220/state_codec.json``, built from
real canonical records by ``scripts/kn_state_codec_golden.py``.

* **State trailer** (48 bytes): source offset u64 BE, materializer epoch u64
  BE, SHA-256 of the canonical EventEnvelope bytes.
* **BAR cache row** = trailer + the canonical envelope without the LPK-derived
  fields (instrument uid, venue, market, bar interval). Byte-identical to the
  KN-1 reference ``scripts/kn_resource_sizing.py`` ``lpk_row``. Decoding
  restores the four fields from the LPK and proves the result against the hash.
* **Latest cache value** = trailer + the full canonical bytes.
* **State-topic frame** (D2): ``QKS1`` + kind u8 + header length u32 BE +
  canonical strict JSON header + the canonical envelope bytes unchanged.
* **Keys and partition** (D3): the latest key is the LPK; BAR keys separate
  the in-progress row from each final revision; every record of one product
  goes to ``murmur2(lpk) & 0x7fffffff % partitions`` (Kafka's default hash).

Boundary: pure functions over bytes, no I/O. Every refusal is a
``StateCodecError`` whose ``reason`` is one of ``REASONS``; Rust reports the
same reason for the same input, which the golden pins. JSON parsing is aligned
with ``serde_json`` (integers beyond u64/i64 and ``-0`` become floats, a float that
overflows is a parse error, nesting deeper than ``MAX_JSON_DEPTH`` is a parse
error, lone surrogates are a parse error) so both sides classify every header
identically.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import hashlib
import json
import math
import re
from typing import Any

from google.protobuf.message import DecodeError

from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.state_contract import MAX_OFFSET, LogicalProductKey, SourceCoordinate

TRAILER_BYTES = 48
FRAME_MAGIC = b"QKS1"
FRAME_PREFIX_BYTES = 9
MAX_HEADER_BYTES = 4096
# serde_json's recursion limit: 128 nested arrays/objects is a parse error.
MAX_JSON_DEPTH = 127
MAX_PARTITION = (1 << 31) - 1
MAX_REVISION = (1 << 32) - 1
MAX_PARTITIONS = (1 << 31) - 1
LEGACY_PROVENANCE = "legacy_import"
KAFKA_MURMUR2_SEED = 0x9747B28C
_MURMUR2_M = 0x5BD1E995
_U32 = 0xFFFFFFFF
_U64 = (1 << 64) - 1

# Always ``fullmatch``: ``re.match`` with ``$`` also accepts a trailing "\n".
# Header strings never need JSON escaping, so both encoders agree byte for byte.
_TEXT = re.compile(r"[A-Za-z0-9._:/@|+=-]{1,1024}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_EVENT_ID = re.compile(r"(?:[0-9a-f]{2}){1,512}")

REASONS = (
    "TRUNCATED",               # shorter than its fixed prefix, or header length past the end
    "MAGIC",                   # frame does not start with QKS1
    "KIND",                    # unknown frame kind byte
    "HEADER_SIZE",             # declared header length above MAX_HEADER_BYTES
    "HEADER_JSON",             # header is not a UTF-8 JSON object
    "HEADER_FIELDS",           # header (or a nested object) field set is not exact
    "FIELD",                   # a header field has the wrong type, range or format
    "NON_CANONICAL",           # header bytes or a BAR row body are not the canonical encoding
    "BODY",                    # floor frame with a body, or a state record without one
    "CONTENT_HASH",            # SHA-256 of the canonical bytes differs from the stored hash
    "ENVELOPE",                # body is not a decodable EventEnvelope
    "NOT_BAR",                 # a BAR codec was given another payload
    "PRODUCT_MISMATCH",        # envelope uid/venue/market/feed/interval differ from the LPK
    "OPEN_TIME",               # bar open time negative or not a whole millisecond
    "HEADER_MISMATCH",         # header event id/open time/revision/finality differ from the envelope
    "TRAILER",                 # source offset above 2^63-1 or materializer epoch outside 1..2^63-1
    "ENVELOPE_NOT_CANONICAL",  # canonical bytes would not re-serialize byte-for-byte after decode
    "PARTITIONS",              # partition count outside 1..2^31-1
)


class StateCodecError(ValueError):
    """A typed refusal; ``reason`` is shared with the Rust codec."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"STATE_CODEC:{reason}" + (f":{detail}" if detail else ""))
        self.reason = reason
        self.detail = detail


class FrameKind(IntEnum):
    LATEST = 1
    BAR_REVISION = 2
    RETENTION_FLOOR = 3
    LEGACY_BAR = 4


_SOURCE_FIELDS = ("offset", "partition", "topic_id")
_LEGACY_FIELDS = ("spool_logical_offset", "spool_partition_key", "spool_stream")
_HEADER_FIELDS = {
    FrameKind.LATEST: ("content_sha256", "event_id", "lpk", "materializer_epoch", "source"),
    FrameKind.BAR_REVISION: ("content_sha256", "event_id", "is_final", "lpk", "materializer_epoch",
                             "open_time_ms", "revision", "source"),
    FrameKind.RETENTION_FLOOR: ("floor_open_time_ms", "lpk", "materializer_epoch"),
    FrameKind.LEGACY_BAR: ("content_sha256", "event_id", "is_final", "legacy", "lpk", "materializer_epoch",
                           "open_time_ms", "provenance", "revision"),
}


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# ------------------------------------------------------------------ trailer

@dataclass(frozen=True)
class DecodedState:
    """A cache value proven against its trailer hash."""

    canonical: bytes
    source_offset: int
    materializer_epoch: int


def _check_trailer_numbers(source_offset: object, materializer_epoch: object) -> None:
    if (not _is_integer(source_offset) or not _is_integer(materializer_epoch)
            or not 0 <= source_offset <= MAX_OFFSET or not 1 <= materializer_epoch <= MAX_OFFSET):
        raise StateCodecError("TRAILER")


def state_trailer(canonical: bytes, source_offset: int, materializer_epoch: int) -> bytes:
    _check_trailer_numbers(source_offset, materializer_epoch)
    return (source_offset.to_bytes(8, "big") + materializer_epoch.to_bytes(8, "big")
            + hashlib.sha256(canonical).digest())


def _read_trailer(data: bytes) -> tuple[int, int, bytes]:
    if len(data) < TRAILER_BYTES:
        raise StateCodecError("TRUNCATED")
    source_offset = int.from_bytes(data[:8], "big")
    materializer_epoch = int.from_bytes(data[8:16], "big")
    _check_trailer_numbers(source_offset, materializer_epoch)
    return source_offset, materializer_epoch, bytes(data[16:TRAILER_BYTES])


def encode_latest_value(canonical: bytes, source_offset: int, materializer_epoch: int) -> bytes:
    """Latest cache value: trailer + the full canonical bytes (nothing stripped)."""

    trailer = state_trailer(canonical, source_offset, materializer_epoch)
    if not canonical:
        raise StateCodecError("BODY")
    return trailer + bytes(canonical)


def decode_latest_value(value: bytes) -> DecodedState:
    source_offset, materializer_epoch, digest = _read_trailer(value)
    canonical = bytes(value[TRAILER_BYTES:])
    if not canonical:
        raise StateCodecError("BODY")
    if hashlib.sha256(canonical).digest() != digest:
        raise StateCodecError("CONTENT_HASH")
    return DecodedState(canonical, source_offset, materializer_epoch)


# ------------------------------------------------------------------ product checks

def _parse_envelope(canonical: bytes) -> market_data_pb2.EventEnvelope:
    """Parse like prost: unknown fields are dropped (upb would keep and re-emit them)."""

    try:
        envelope = market_data_pb2.EventEnvelope.FromString(bytes(canonical))
    except DecodeError as error:
        raise StateCodecError("ENVELOPE") from error
    envelope.DiscardUnknownFields()
    return envelope


def _check_product(envelope: market_data_pb2.EventEnvelope, lpk: LogicalProductKey) -> None:
    """The envelope is the product ``lpk`` names: uid, venue, market, feed and qualifier."""

    payload = envelope.WhichOneof("payload")
    qualifier = envelope.bar.interval if payload == "bar" else "-"
    for name, actual, expected in (
        ("instrument_uid", envelope.instrument_uid, lpk.instrument_uid),
        ("venue", envelope.venue, lpk.venue),
        ("market", envelope.market, lpk.market),
        ("feed", (payload or "").upper(), lpk.feed),
        ("qualifier", qualifier, lpk.qualifier),
    ):
        if actual != expected:
            raise StateCodecError("PRODUCT_MISMATCH", name)


def _open_time_ms(envelope: market_data_pb2.EventEnvelope) -> int:
    open_time_ns = envelope.bar.open_time_ns
    if open_time_ns < 0 or open_time_ns % 1_000_000:
        raise StateCodecError("OPEN_TIME")
    return open_time_ns // 1_000_000


# ------------------------------------------------------------------ BAR cache row

def encode_bar_row(canonical: bytes, lpk: LogicalProductKey, source_offset: int, materializer_epoch: int) -> bytes:
    """BAR current-index row: trailer + canonical envelope without the LPK fields.

    Refuses a non-BAR, a row of another product, and canonical bytes that would
    not be reproduced byte-for-byte by ``decode_bar_row``.
    """

    trailer = state_trailer(canonical, source_offset, materializer_epoch)
    envelope = _parse_envelope(canonical)
    if envelope.WhichOneof("payload") != "bar":
        raise StateCodecError("NOT_BAR")
    _check_product(envelope, lpk)
    envelope.ClearField("instrument_uid")
    envelope.ClearField("venue")
    envelope.ClearField("market")
    envelope.bar.ClearField("interval")
    body = envelope.SerializeToString()
    if _restore_bar(envelope, lpk) != bytes(canonical):
        raise StateCodecError("ENVELOPE_NOT_CANONICAL")
    return trailer + body


def _restore_bar(envelope: market_data_pb2.EventEnvelope, lpk: LogicalProductKey) -> bytes:
    envelope.instrument_uid = lpk.instrument_uid
    envelope.venue = lpk.venue
    envelope.market = lpk.market
    envelope.bar.interval = lpk.qualifier
    return envelope.SerializeToString()


def decode_bar_row(row: bytes, lpk: LogicalProductKey, *, expected_open_ms: int | None = None) -> DecodedState:
    source_offset, materializer_epoch, digest = _read_trailer(row)
    envelope = _parse_envelope(row[TRAILER_BYTES:])
    if envelope.WhichOneof("payload") != "bar":
        raise StateCodecError("NOT_BAR")
    if envelope.instrument_uid or envelope.venue or envelope.market or envelope.bar.interval:
        raise StateCodecError("NON_CANONICAL")
    if expected_open_ms is not None and _open_time_ms(envelope) != expected_open_ms:
        raise StateCodecError("OPEN_TIME_MISMATCH")
    canonical = _restore_bar(envelope, lpk)
    if hashlib.sha256(canonical).digest() != digest:
        raise StateCodecError("CONTENT_HASH")
    return DecodedState(canonical, source_offset, materializer_epoch)


# ------------------------------------------------------------------ canonical JSON header

def _parse_int(text: str) -> int | float:
    # serde_json keeps an integer only when it fits u64 (or i64 when negative)
    # and turns ``-0`` into the float ``-0.0``.
    value = int(text)
    if value == 0 and text.startswith("-"):
        return -0.0
    if -(1 << 63) <= value <= _U64:
        return value
    return _parse_float(text)


def _parse_float(text: str) -> float:
    value = float(text)
    if math.isinf(value):
        raise ValueError("number out of range")
    return value


def _reject_constant(name: str) -> Any:
    raise ValueError(name)


def _json_shape_ok(value: Any) -> bool:
    """Depth within serde_json's limit and no lone surrogates (serde_json rejects both)."""

    stack = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, (dict, list)):
            if depth > MAX_JSON_DEPTH:
                return False
            children = list(item.items()) if isinstance(item, dict) else [(None, child) for child in item]
            for key, child in children:
                if isinstance(key, str) and _has_surrogate(key):
                    return False
                stack.append((child, depth + 1))
        elif isinstance(item, str) and _has_surrogate(item):
            return False
    return True


def _has_surrogate(text: str) -> bool:
    return any(0xD800 <= ord(char) <= 0xDFFF for char in text)


def _parse_header(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"), parse_int=_parse_int, parse_float=_parse_float,
                           parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise StateCodecError("HEADER_JSON") from error
    if not isinstance(value, dict) or not _json_shape_ok(value):
        raise StateCodecError("HEADER_JSON")
    return value


def canonical_json(value: dict[str, Any]) -> bytes:
    """Sorted byte-order keys, ``,``/``:``, no whitespace; values already validated."""

    parts = []
    for key in sorted(value, key=lambda item: item.encode()):
        item = value[key]
        if isinstance(item, bool):
            encoded = "true" if item else "false"
        elif isinstance(item, int):
            encoded = str(item)
        elif isinstance(item, str):
            encoded = f'"{item}"'
        elif isinstance(item, dict):
            encoded = canonical_json(item).decode("ascii")
        else:  # pragma: no cover - validation admits no other type
            raise StateCodecError("FIELD", key)
        parts.append(f'"{key}":{encoded}')
    return ("{" + ",".join(parts) + "}").encode("ascii")


def _exact(value: dict[str, Any], names: tuple[str, ...], where: str) -> None:
    if set(value) != set(names):
        raise StateCodecError("HEADER_FIELDS", where)


def _integer(value: Any, minimum: int, maximum: int, name: str) -> None:
    if not _is_integer(value) or not minimum <= value <= maximum:
        raise StateCodecError("FIELD", name)


def _text(value: Any, name: str) -> None:
    if not isinstance(value, str) or not _TEXT.fullmatch(value):
        raise StateCodecError("FIELD", name)


def _validate_header(kind: FrameKind, header: dict[str, Any]) -> LogicalProductKey:
    """Exact field set, then every field in sorted name order (nested too)."""

    names = _HEADER_FIELDS[kind]
    _exact(header, names, "header")
    lpk = None
    for name in names:
        value = header[name]
        if name == "content_sha256":
            if not isinstance(value, str) or not _HEX64.fullmatch(value):
                raise StateCodecError("FIELD", name)
        elif name == "event_id":
            if not isinstance(value, str) or not _EVENT_ID.fullmatch(value):
                raise StateCodecError("FIELD", name)
        elif name in ("floor_open_time_ms", "open_time_ms"):
            _integer(value, 0, MAX_OFFSET, name)
        elif name == "is_final":
            if not isinstance(value, bool):
                raise StateCodecError("FIELD", name)
        elif name == "legacy":
            if not isinstance(value, dict):
                raise StateCodecError("FIELD", name)
            _exact(value, _LEGACY_FIELDS, name)
            _integer(value["spool_logical_offset"], 0, MAX_OFFSET, "legacy.spool_logical_offset")
            _text(value["spool_partition_key"], "legacy.spool_partition_key")
            _text(value["spool_stream"], "legacy.spool_stream")
        elif name == "lpk":
            _text(value, name)
            try:
                lpk = LogicalProductKey.parse(value)
            except ValueError as error:
                raise StateCodecError("FIELD", name) from error
        elif name == "materializer_epoch":
            _integer(value, 1, MAX_OFFSET, name)
        elif name == "provenance":
            if value != LEGACY_PROVENANCE:
                raise StateCodecError("FIELD", name)
        elif name == "revision":
            _integer(value, 0, MAX_REVISION, name)
        elif name == "source":
            if not isinstance(value, dict):
                raise StateCodecError("FIELD", name)
            _exact(value, _SOURCE_FIELDS, name)
            _integer(value["offset"], 0, MAX_OFFSET, "source.offset")
            _integer(value["partition"], 0, MAX_PARTITION, "source.partition")
            _text(value["topic_id"], "source.topic_id")
    assert lpk is not None
    return lpk


# ------------------------------------------------------------------ state-topic frame

@dataclass(frozen=True)
class LegacyLineage:
    """Where a legacy-imported BAR came from; never a canonical Kafka offset."""

    spool_stream: str
    spool_partition_key: str
    spool_logical_offset: int


@dataclass(frozen=True)
class StateFrame:
    """One decoded or to-be-encoded state-topic record (decision D2)."""

    kind: FrameKind
    lpk: LogicalProductKey
    materializer_epoch: int
    envelope: bytes = b""
    content_sha256: str | None = None
    event_id: str | None = None
    source: SourceCoordinate | None = None
    open_time_ms: int | None = None
    revision: int | None = None
    is_final: bool | None = None
    floor_open_time_ms: int | None = None
    legacy: LegacyLineage | None = None

    def header(self) -> dict[str, Any]:
        header: dict[str, Any] = {"lpk": self.lpk.encode(), "materializer_epoch": self.materializer_epoch}
        if self.kind is FrameKind.RETENTION_FLOOR:
            header["floor_open_time_ms"] = self.floor_open_time_ms
            return header
        header["content_sha256"] = self.content_sha256
        header["event_id"] = self.event_id
        if self.kind is FrameKind.LEGACY_BAR:
            legacy = self.legacy
            header["legacy"] = None if legacy is None else {
                "spool_logical_offset": legacy.spool_logical_offset,
                "spool_partition_key": legacy.spool_partition_key,
                "spool_stream": legacy.spool_stream}
            header["provenance"] = LEGACY_PROVENANCE
        else:
            source = self.source
            header["source"] = None if source is None else {
                "offset": source.offset, "partition": source.partition, "topic_id": source.topic_id}
        if self.kind in (FrameKind.BAR_REVISION, FrameKind.LEGACY_BAR):
            header["open_time_ms"] = self.open_time_ms
            header["revision"] = self.revision
            header["is_final"] = self.is_final
        return header

    def encode(self) -> bytes:
        header = self.header()
        _validate_header(self.kind, header)
        encoded = canonical_json(header)
        return FRAME_MAGIC + bytes([int(self.kind)]) + len(encoded).to_bytes(4, "big") + encoded + self.envelope

    def key(self) -> str:
        if self.kind is FrameKind.LATEST:
            return latest_key(self.lpk)
        if self.kind is FrameKind.RETENTION_FLOOR:
            return floor_key(self.lpk)
        assert self.open_time_ms is not None and self.revision is not None and self.content_sha256 is not None
        return bar_key(self.lpk, self.open_time_ms, bool(self.is_final), self.revision, self.content_sha256)


def _envelope_facts(kind: FrameKind, canonical: bytes, lpk: LogicalProductKey) -> dict[str, Any]:
    """What the envelope itself says; the header must agree with every item."""

    if not canonical:
        raise StateCodecError("BODY")
    envelope = _parse_envelope(canonical)
    bar = kind in (FrameKind.BAR_REVISION, FrameKind.LEGACY_BAR)
    if bar and envelope.WhichOneof("payload") != "bar":
        raise StateCodecError("NOT_BAR")
    _check_product(envelope, lpk)
    facts: dict[str, Any] = {"content_sha256": hashlib.sha256(canonical).hexdigest(),
                             "event_id": envelope.event_id.hex()}
    if bar:
        facts.update(open_time_ms=_open_time_ms(envelope), revision=envelope.bar.revision,
                     is_final=envelope.bar.is_final)
    return facts


def _envelope_frame(kind: FrameKind, canonical: bytes, lpk: LogicalProductKey, materializer_epoch: int,
                    *, source: SourceCoordinate | None = None, legacy: LegacyLineage | None = None) -> StateFrame:
    canonical = bytes(canonical)
    facts = _envelope_facts(kind, canonical, lpk)
    frame = StateFrame(kind, lpk, materializer_epoch, canonical, source=source, legacy=legacy, **facts)
    _validate_header(kind, frame.header())
    return frame


def latest_frame(canonical: bytes, lpk: LogicalProductKey, source: SourceCoordinate,
                 materializer_epoch: int) -> StateFrame:
    return _envelope_frame(FrameKind.LATEST, canonical, lpk, materializer_epoch, source=source)


def bar_revision_frame(canonical: bytes, lpk: LogicalProductKey, source: SourceCoordinate,
                       materializer_epoch: int) -> StateFrame:
    return _envelope_frame(FrameKind.BAR_REVISION, canonical, lpk, materializer_epoch, source=source)


def legacy_bar_frame(canonical: bytes, lpk: LogicalProductKey, legacy: LegacyLineage,
                     materializer_epoch: int) -> StateFrame:
    return _envelope_frame(FrameKind.LEGACY_BAR, canonical, lpk, materializer_epoch, legacy=legacy)


def retention_floor_frame(lpk: LogicalProductKey, floor_open_time_ms: int, materializer_epoch: int) -> StateFrame:
    if lpk.feed != "BAR":
        raise StateCodecError("PRODUCT_MISMATCH", "feed")
    frame = StateFrame(FrameKind.RETENTION_FLOOR, lpk, materializer_epoch, floor_open_time_ms=floor_open_time_ms)
    _validate_header(FrameKind.RETENTION_FLOOR, frame.header())
    return frame


def decode_frame(data: bytes) -> StateFrame:
    """Strict decode; the first failing check in the documented order wins."""

    data = bytes(data)
    if len(data) < FRAME_PREFIX_BYTES:
        raise StateCodecError("TRUNCATED")
    if data[:4] != FRAME_MAGIC:
        raise StateCodecError("MAGIC")
    try:
        kind = FrameKind(data[4])
    except ValueError as error:
        raise StateCodecError("KIND") from error
    header_len = int.from_bytes(data[5:9], "big")
    if header_len > MAX_HEADER_BYTES:
        raise StateCodecError("HEADER_SIZE")
    if FRAME_PREFIX_BYTES + header_len > len(data):
        raise StateCodecError("TRUNCATED")
    header_bytes = data[FRAME_PREFIX_BYTES:FRAME_PREFIX_BYTES + header_len]
    body = data[FRAME_PREFIX_BYTES + header_len:]
    header = _parse_header(header_bytes)
    lpk = _validate_header(kind, header)
    if canonical_json(header) != header_bytes:
        raise StateCodecError("NON_CANONICAL")
    if kind is FrameKind.RETENTION_FLOOR:
        if body:
            raise StateCodecError("BODY")
        if lpk.feed != "BAR":
            raise StateCodecError("PRODUCT_MISMATCH", "feed")
        return StateFrame(kind, lpk, header["materializer_epoch"], floor_open_time_ms=header["floor_open_time_ms"])
    if not body:
        raise StateCodecError("BODY")
    if hashlib.sha256(body).hexdigest() != header["content_sha256"]:
        raise StateCodecError("CONTENT_HASH")
    source = legacy = None
    if "source" in header:
        item = header["source"]
        source = SourceCoordinate(item["topic_id"], item["partition"], item["offset"])
    if "legacy" in header:
        item = header["legacy"]
        legacy = LegacyLineage(item["spool_stream"], item["spool_partition_key"], item["spool_logical_offset"])
    facts = _envelope_facts(kind, body, lpk)
    for name in ("event_id", "open_time_ms", "revision", "is_final"):
        if name in facts and header[name] != facts[name]:
            raise StateCodecError("HEADER_MISMATCH", name)
    return StateFrame(kind, lpk, header["materializer_epoch"], body, source=source, legacy=legacy, **facts)


# ------------------------------------------------------------------ keys and partition

def latest_key(lpk: LogicalProductKey) -> str:
    return lpk.encode()


def bar_key(lpk: LogicalProductKey, open_time_ms: int, is_final: bool, revision: int, content_sha256: str) -> str:
    """``<lpk>|<open_ms>|p`` for the one in-progress row, else ``|f<revision>|<sha16>`` per final fact."""

    _integer(open_time_ms, 0, MAX_OFFSET, "open_time_ms")
    if not is_final:
        return f"{lpk.encode()}|{open_time_ms}|p"
    _integer(revision, 0, MAX_REVISION, "revision")
    if not isinstance(content_sha256, str) or not _HEX64.fullmatch(content_sha256):
        raise StateCodecError("FIELD", "content_sha256")
    return f"{lpk.encode()}|{open_time_ms}|f{revision}|{content_sha256[:16]}"


def floor_key(lpk: LogicalProductKey) -> str:
    return f"{lpk.encode()}|floor"


def murmur2(data: bytes) -> int:
    """Kafka ``org.apache.kafka.common.utils.Utils.murmur2`` as a signed 32-bit int."""

    length = len(data)
    h = (KAFKA_MURMUR2_SEED ^ length) & _U32
    for index in range(length // 4):
        k = int.from_bytes(data[4 * index:4 * index + 4], "little")
        k = (k * _MURMUR2_M) & _U32
        k ^= k >> 24
        k = (k * _MURMUR2_M) & _U32
        h = ((h * _MURMUR2_M) & _U32) ^ k
    tail = length & ~3
    remainder = length % 4
    if remainder >= 3:
        h ^= data[tail + 2] << 16
    if remainder >= 2:
        h ^= data[tail + 1] << 8
    if remainder >= 1:
        h ^= data[tail]
        h = (h * _MURMUR2_M) & _U32
    h ^= h >> 13
    h = (h * _MURMUR2_M) & _U32
    h ^= h >> 15
    return h - (1 << 32) if h >= 1 << 31 else h


def state_partition(lpk: LogicalProductKey, partitions: int) -> int:
    """Kafka's default partitioner over the LPK bytes: one product, one partition."""

    if not _is_integer(partitions) or not 1 <= partitions <= MAX_PARTITIONS:
        raise StateCodecError("PARTITIONS")
    return (murmur2(lpk.encode().encode("utf-8")) & 0x7FFFFFFF) % partitions

"""KN-4 K4.1 read-only product views of the Kafka-native market cache (D25-D27).

Purpose: the one place Python Query reads the market cache that the Rust
projector stage B writes (``rust/qdl-projector/src/cache.rs`` + ``apply.lua``).
It answers two questions per logical product, always from the product's
published READY generation and always with a provable source boundary:

* ``latest(lpk)`` - the product's latest entry ``l:<g>:<lpk>`` (trailer +
  full canonical bytes, canonical ``t/p/o``), read by one short read-only Lua
  call together with the pointer and the source watermark;
* ``bars(lpk, interval_ms, ...)`` - the BAR rows of the last ``N`` opens or of
  an open-time range: one read-only Lua call for pointer, product source and
  BAR meta, pipelined ``HGETALL`` of only the buckets the window needs (at
  most ``MAX_BUCKETS_PER_ROUND_TRIP`` per round trip), then the pointer again.

Source boundary (D27): ``offset`` is the highest canonical offset ``X`` of the
product's canonical ``(topic id, partition)`` such that every fact of the
product at ``<= X`` is in the view. Stage B raises the watermark field
``s|<topic id>|<partition>`` of the state partition's ``ckpt`` hash in the
script of each live batch; a state partition receives a product's frames in
canonical order, so the watermark read *before* the data bounds the data.
Latest: ``max(watermark, l:o)`` read atomically. BAR: the watermark read in
the head call; a row whose trailer offset is above it proves the product
changed during the read (its re-delivery on the stream would be a duplicate,
never a gap), so the read is retried and, if it keeps changing, returned with
``changed=True``. A BAR product without a canonical coordinate yet (legacy
rows only) has no provable boundary: ``SOURCE_BOUNDARY_UNKNOWN``.

Consistency (D26): a pointer (ready, fence) change between head and tail is
retried (``attempts``) and then refused typed-retryable; rows of two
generations are never merged. Every row is decoded with the state codec
(trailer hash proven) and must sit under its own open.

Boundary: read-only (Lua scripts carry ``flags=no-writes``; no SCAN/KEYS, no
write), bounded by the requested window; the Redis client is injected and is
never the control/quota Redis. No Kafka access.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from qdl.query.cold_work import cold_yield

from qdl.projection.kn_state_codec import (
    StateCodecError,
    decode_bar_row,
    decode_latest_value,
    state_partition,
)
from qdl.projection.state_contract import MAX_OFFSET, LogicalProductKey
from qdl.runtime.kn_bar_readback import BUCKET_OPENS, bucket_of

MARKET_CACHE_URL_ENV = "QDL_KN3_MARKET_CACHE_URL"
MARKET_CACHE_ENVIRONMENT_ENV = "QDL_KN3_ENVIRONMENT"
LATEST_TOPIC_ENV = "QDL_KN_LATEST_TOPIC"
BARS_TOPIC_ENV = "QDL_KN_BARS_TOPIC"
LATEST_PARTITIONS_ENV = "QDL_KN_LATEST_PARTITIONS"
BARS_PARTITIONS_ENV = "QDL_KN_BARS_PARTITIONS"
MAX_BUCKETS_PER_ROUND_TRIP = 64
DEFAULT_ATTEMPTS = 3

try:  # the client library is present in the runtime image; tests may inject a fake
    from redis.exceptions import RedisError as _RedisError
except ImportError:  # pragma: no cover - host without redis-py
    _RedisError = OSError

# One short read each; never a write (the flag lets Redis run it under OOM).
_LATEST_READ = """#!lua flags=no-writes
local prefix, lpk, ckpt = ARGV[1], ARGV[2], ARGV[3]
local ptr = redis.call('HMGET', prefix .. 'ptr:' .. lpk, 'ready', 'fence')
if not ptr[1] then
  return {'NO_GENERATION'}
end
local entry = redis.call('HMGET', prefix .. 'l:' .. ptr[1] .. ':' .. lpk, 'v', 't', 'p', 'o')
if not entry[1] then
  return {'MISSING', ptr[1], ptr[2] or '0'}
end
local mark = redis.call('HGET', ckpt, 's|' .. entry[2] .. '|' .. entry[3])
return {'OK', ptr[1], ptr[2] or '0', entry[1], entry[2], entry[3], entry[4], mark or ''}
"""

_BAR_HEAD = """#!lua flags=no-writes
local prefix, lpk, ckpt = ARGV[1], ARGV[2], ARGV[3]
local ptr = redis.call('HMGET', prefix .. 'ptr:' .. lpk, 'ready', 'fence')
if not ptr[1] then
  return {'NO_GENERATION'}
end
local src = redis.call('HMGET', prefix .. 'src:' .. lpk, 't', 'p')
local mark = ''
if src[1] and src[2] then
  mark = redis.call('HGET', ckpt, 's|' .. src[1] .. '|' .. src[2]) or ''
end
local meta = redis.call('HMGET', prefix .. 'bm:' .. ptr[1] .. ':' .. lpk,
  'floor', 'first', 'last', 'last_final', 'rows')
return {'OK', ptr[1], ptr[2] or '0', src[1] or '', src[2] or '', mark,
  meta[1] or '', meta[2] or '', meta[3] or '', meta[4] or '', meta[5] or ''}
"""


_BAR_DIAGNOSTIC = """#!lua flags=no-writes
local replies = {}
for _, key in ipairs(KEYS) do
  local opens = redis.call('HKEYS', key)
  local index = redis.call('HGETALL', 'kn3:' .. ARGV[1] .. ':bd:' .. string.sub(key, #('kn3:' .. ARGV[1] .. ':b:') + 1))
  table.insert(replies, {opens, index})
end
return replies
"""


class KnDiagnosticIndexMissing(RuntimeError):
    """Older materializer or incomplete index: use the verified row scanner."""


class KnCacheError(RuntimeError):
    """The market cache cannot answer now (typed retryable)."""


class KnCacheNotReady(KnCacheError):
    """The product has no servable view: ``NO_GENERATION``, ``MISSING`` or
    ``SOURCE_BOUNDARY_UNKNOWN`` (typed DATA_NOT_READY, never an empty OK)."""

    def __init__(self, lpk: LogicalProductKey, state: str) -> None:
        self.lpk = lpk
        self.state = state
        super().__init__(f"market cache product is not ready ({state}): {lpk.encode()}")


class KnCacheViewChanged(KnCacheError):
    """The product's READY generation kept changing during the read."""


class KnCacheIntegrityError(RuntimeError):
    """A cache value failed its trailer/identity check (fail closed)."""


@dataclass(frozen=True)
class SourceBoundary:
    """The view's canonical source coordinate (cursor v3 ``source_*``)."""

    topic_id: str
    partition: int
    offset: int


@dataclass(frozen=True)
class CacheRow:
    """One cache value: canonical bytes and its canonical offset (``None`` for
    a legacy-imported BAR row, which has no canonical coordinate)."""

    canonical: bytes
    source_offset: int | None
    open_ms: int | None = None


@dataclass(frozen=True)
class ProductView:
    lpk: LogicalProductKey
    generation: int
    fence: int
    rows: tuple[CacheRow, ...]
    boundary: SourceBoundary
    # A row above the boundary: the product changed during the read, so the
    # stream may re-deliver facts the rows already hold (never a gap).
    changed: bool = False


def _text(value: Any) -> str:
    if value is None:
        return ""
    return value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)


def _decimal(value: Any, what: str) -> int | None:
    text = _text(value)
    if text == "":
        return None
    if not text.isdigit() or (len(text) > 1 and text[0] == "0"):
        raise KnCacheIntegrityError(f"market cache {what} is not a decimal: {text!r}")
    number = int(text)
    if number > MAX_OFFSET:
        raise KnCacheIntegrityError(f"market cache {what} exceeds 2^63-1")
    return number


class KnMarketCacheReader:
    """Consistent, read-only product views of the KN-3 market cache."""

    def __init__(
        self,
        client: Any,
        environment: str,
        *,
        latest_topic: str = "md.latest.v2",
        bars_topic: str = "md.bars.v2",
        latest_partitions: int = 6,
        bars_partitions: int = 6,
        attempts: int = DEFAULT_ATTEMPTS,
    ) -> None:
        # Validates the environment exactly like an LPK field.
        LogicalProductKey(environment, "V", "M", "u", "BAR", "1m")
        if min(latest_partitions, bars_partitions) < 1 or attempts < 1:
            raise ValueError("market cache partitions and attempts must be positive")
        for topic in (latest_topic, bars_topic):
            if not topic or ":" in topic:
                raise ValueError("market cache state topic name is invalid")
        self.client = client
        self.environment = environment
        self.prefix = f"kn3:{environment}:"
        self.latest_topic = latest_topic
        self.bars_topic = bars_topic
        self.latest_partitions = latest_partitions
        self.bars_partitions = bars_partitions
        self.attempts = attempts
        self._latest_script = client.register_script(_LATEST_READ)
        self._bar_head_script = client.register_script(_BAR_HEAD)
        self._bar_diagnostic_script = client.register_script(_BAR_DIAGNOSTIC)

    # ---------------------------------------------------------------- layout

    def checkpoint_key(self, topic: str, partition: int) -> str:
        return f"{self.prefix}ckpt:{topic}:{partition}"

    def bucket_key(self, generation: int, lpk: LogicalProductKey, bucket: int) -> str:
        return f"{self.prefix}b:{generation}:{lpk.encode()}:{bucket}"

    def pointer_key(self, lpk: LogicalProductKey) -> str:
        return f"{self.prefix}ptr:{lpk.encode()}"

    def _call(self, script, lpk: LogicalProductKey, ckpt: str) -> list[Any]:
        try:
            reply = script(keys=[], args=[self.prefix, lpk.encode(), ckpt])
        except _RedisError as error:
            raise KnCacheError("market cache is unavailable") from error
        if not isinstance(reply, list) or not reply:
            raise KnCacheError("market cache returned a malformed read reply")
        return reply

    # ---------------------------------------------------------------- health

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except _RedisError:
            return False

    def ready_generations(self, lpks: list[LogicalProductKey]) -> list[int | None]:
        """The READY generation of each product (one pipelined round trip)."""

        try:
            pipe = self.client.pipeline(transaction=False)
            for lpk in lpks:
                pipe.hget(self.pointer_key(lpk), "ready")
            replies = pipe.execute()
        except _RedisError as error:
            raise KnCacheError("market cache is unavailable") from error
        return [_decimal(value, "pointer") for value in replies]

    # ---------------------------------------------------------------- latest

    def latest(self, lpk: LogicalProductKey) -> ProductView:
        if lpk.feed == "BAR":
            raise ValueError("a BAR product has no latest entry; read bars()")
        q = state_partition(lpk, self.latest_partitions)
        reply = self._call(self._latest_script, lpk, self.checkpoint_key(self.latest_topic, q))
        status = _text(reply[0])
        if status == "NO_GENERATION":
            raise KnCacheNotReady(lpk, "NO_GENERATION")
        if status == "MISSING":
            raise KnCacheNotReady(lpk, "MISSING")
        if status != "OK" or len(reply) != 8:
            raise KnCacheError("market cache returned a malformed latest reply")
        generation = _decimal(reply[1], "generation")
        fence = _decimal(reply[2], "fence") or 0
        topic_id = _text(reply[4])
        partition = _decimal(reply[5], "latest partition")
        offset = _decimal(reply[6], "latest offset")
        mark = _decimal(reply[7], "source watermark")
        if generation is None or not topic_id or partition is None or offset is None:
            raise KnCacheIntegrityError(f"market cache latest entry is incomplete: {lpk.encode()}")
        try:
            decoded = decode_latest_value(bytes(reply[3]))
        except (StateCodecError, TypeError) as error:
            raise KnCacheIntegrityError(
                f"market cache latest value failed its trailer check: {lpk.encode()}"
            ) from error
        if decoded.source_offset != offset:
            raise KnCacheIntegrityError(
                f"market cache latest trailer offset differs from its coordinate: {lpk.encode()}"
            )
        boundary = SourceBoundary(topic_id, partition, max(offset, mark or 0))
        return ProductView(
            lpk, generation, fence,
            (CacheRow(decoded.canonical, decoded.source_offset),),
            boundary,
        )

    # ---------------------------------------------------------------- bars

    def bars(
        self,
        lpk: LogicalProductKey,
        interval_ms: int,
        *,
        last: int | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
        check_budget: Callable[[], None] | None = None,
    ) -> ProductView:
        """Rows of the last ``last`` opens, or of opens in ``[start_ms, end_ms)``."""

        if lpk.feed != "BAR" or interval_ms < 1:
            raise ValueError("bars() needs a BAR product and a positive interval")
        if (last is None) == (start_ms is None) or (start_ms is None) != (end_ms is None):
            raise ValueError("bars() takes either last or a start/end range")
        if last is not None and last < 1:
            raise ValueError("bars() last must be positive")
        if start_ms is not None and (start_ms < 0 or end_ms <= start_ms):
            raise ValueError("bars() range is empty")
        ckpt = self.checkpoint_key(self.bars_topic, state_partition(lpk, self.bars_partitions))
        view: ProductView | None = None
        for _attempt in range(self.attempts):
            if check_budget is not None:
                check_budget()
            head = self._bar_head(lpk, ckpt)
            generation, fence, boundary, low, high = head
            rows: dict[int, CacheRow] = {}
            if high is not None:
                rows = self._read_rows(lpk, generation, interval_ms, low, high,
                                       last=last, start_ms=start_ms, end_ms=end_ms, check_budget=check_budget)
            tail = self._bar_head(lpk, ckpt)
            if (tail[0], tail[1]) != (generation, fence):
                continue
            # Opens below the retained floor are never served (the floor op
            # deletes them; a reader never depends on that having happened).
            ordered = tuple(rows[open_ms] for open_ms in sorted(rows) if open_ms >= low)
            if last is not None:
                ordered = ordered[-last:]
            changed = any(
                row.source_offset is not None and row.source_offset > boundary.offset
                for row in ordered
            )
            view = ProductView(lpk, generation, fence, ordered, boundary, changed)
            if not changed:
                return view
        if view is None:
            raise KnCacheViewChanged(
                f"market cache READY generation kept changing during the read: {lpk.encode()}"
            )
        return view

    def bar_diagnostics(self, lpk, interval_ms, *, last, check_budget):
        """Exact opens/sequence flags from the atomically written compact index.

        Verify both key sets, not only counts. No payload/price is served here.
        An old/incomplete index falls back to verified canonical row decoding.
        Pointer, retention and source watermark are fenced around the scan.
        """
        if lpk.feed != "BAR" or interval_ms < 1 or last < 1:
            raise ValueError("BAR diagnostics requires a bounded BAR window")
        ckpt = self.checkpoint_key(self.bars_topic, state_partition(lpk, self.bars_partitions))
        for _attempt in range(self.attempts):
            check_budget()
            head = self._bar_head(lpk, ckpt)
            generation, _fence, boundary, low, high = head
            found = {}
            if high is not None:
                top, bottom = bucket_of(high, interval_ms), bucket_of(low, interval_ms)
                while top >= bottom and len(found) < last:
                    check_budget()
                    # Bounded Redis script: <=64 x 112 fields, never a global scan.
                    start = max(bottom, top - MAX_BUCKETS_PER_ROUND_TRIP + 1)
                    buckets = list(range(start, top + 1))
                    keys = [self.bucket_key(generation, lpk, b) for b in buckets]
                    try:
                        replies = self._bar_diagnostic_script(keys=keys, args=[self.environment])
                    except _RedisError as error:
                        raise KnCacheError("market cache diagnostic unavailable") from error
                    if len(replies) != len(buckets):
                        raise KnCacheIntegrityError("short diagnostic bucket reply")
                    for bucket, (opens, flat) in zip(buckets, replies, strict=True):
                        check_budget()
                        if len(flat) % 2:
                            raise KnCacheIntegrityError("malformed diagnostic index")
                        index = dict(zip(flat[::2], flat[1::2], strict=True))
                        if set(opens) != set(index):
                            raise KnDiagnosticIndexMissing(lpk.encode())
                        for raw_open, raw_flag in index.items():
                            check_budget()
                            opened = _decimal(raw_open, "diagnostic open")
                            if opened is None or bucket_of(opened, interval_ms) != bucket:
                                raise KnCacheIntegrityError("diagnostic open/bucket mismatch")
                            flag = _text(raw_flag)
                            if flag != "N" and not flag.startswith("G"):
                                raise KnCacheIntegrityError("malformed diagnostic sequence flag")
                            if opened >= low:
                                found[opened] = None if flag == "N" else flag[1:]
                    top = start - 1
            check_budget()
            if self._bar_head(lpk, ckpt) != head:
                continue
            return boundary, tuple((opened, found[opened]) for opened in sorted(found)[-last:])
        raise KnCacheViewChanged("diagnostic source/generation changed during scan")

    def _bar_head(
        self, lpk: LogicalProductKey, ckpt: str
    ) -> tuple[int, int, SourceBoundary, int | None, int | None]:
        reply = self._call(self._bar_head_script, lpk, ckpt)
        status = _text(reply[0])
        if status == "NO_GENERATION":
            raise KnCacheNotReady(lpk, "NO_GENERATION")
        if status != "OK" or len(reply) != 11:
            raise KnCacheError("market cache returned a malformed BAR head reply")
        generation = _decimal(reply[1], "generation")
        fence = _decimal(reply[2], "fence") or 0
        topic_id = _text(reply[3])
        partition = _decimal(reply[4], "product partition")
        mark = _decimal(reply[5], "source watermark")
        if generation is None:
            raise KnCacheIntegrityError(f"market cache pointer is malformed: {lpk.encode()}")
        if not topic_id or partition is None or mark is None:
            raise KnCacheNotReady(lpk, "SOURCE_BOUNDARY_UNKNOWN")
        floor = _decimal(reply[6], "BAR floor")
        first = _decimal(reply[7], "BAR first")
        high = _decimal(reply[8], "BAR last")
        low = max(value for value in (floor, first, 0) if value is not None)
        return generation, fence, SourceBoundary(topic_id, partition, mark), low, high

    def _read_rows(
        self,
        lpk: LogicalProductKey,
        generation: int,
        interval_ms: int,
        low: int,
        high: int,
        *,
        last: int | None,
        start_ms: int | None,
        end_ms: int | None,
        check_budget: Callable[[], None] | None = None,
    ) -> dict[int, CacheRow]:
        lowest = bucket_of(low, interval_ms)
        rows: dict[int, CacheRow] = {}
        if start_ms is not None:
            first_bucket = max(bucket_of(start_ms, interval_ms), lowest)
            last_bucket = min(bucket_of(max(end_ms - 1, 0), interval_ms), bucket_of(high, interval_ms))
            buckets = list(range(first_bucket, last_bucket + 1))
            for index in range(0, len(buckets), MAX_BUCKETS_PER_ROUND_TRIP):
                self._fetch(lpk, generation, buckets[index:index + MAX_BUCKETS_PER_ROUND_TRIP], rows, interval_ms=interval_ms, check_budget=check_budget)
            return {open_ms: row for open_ms, row in rows.items() if start_ms <= open_ms < end_ms}
        assert last is not None
        top = bucket_of(high, interval_ms)
        # Contiguous opens need ceil(last / BUCKET_OPENS) + 1 buckets; missing
        # opens extend the walk downward, never below the retained floor.
        want = min(-(-last // BUCKET_OPENS) + 1, MAX_BUCKETS_PER_ROUND_TRIP)
        while top >= lowest and len(rows) < last:
            bottom = max(lowest, top - want + 1)
            self._fetch(lpk, generation, list(range(bottom, top + 1)), rows, interval_ms=interval_ms, check_budget=check_budget)
            top = bottom - 1
            want = MAX_BUCKETS_PER_ROUND_TRIP
        return rows

    def _fetch(
        self,
        lpk: LogicalProductKey,
        generation: int,
        buckets: list[int],
        rows: dict[int, CacheRow],
        *,
        interval_ms: int,
        check_budget: Callable[[], None] | None = None,
    ) -> None:
        cold_yield()
        if check_budget is not None:
            check_budget()
        if not buckets:
            return
        try:
            pipe = self.client.pipeline(transaction=False)
            for bucket in buckets:
                pipe.hgetall(self.bucket_key(generation, lpk, bucket))
            replies = pipe.execute()
        except _RedisError as error:
            raise KnCacheError("market cache is unavailable") from error
        if len(replies) != len(buckets):
            raise KnCacheError("market cache returned a short bucket reply")
        for bucket, fields in zip(buckets, replies, strict=True):
            for raw_open, raw_row in fields.items():
                cold_yield()
                if check_budget is not None:
                    check_budget()
                open_ms = _decimal(raw_open, "BAR open")
                try:
                    if open_ms is None or bucket_of(open_ms, interval_ms) != bucket:
                        raise StateCodecError("OPEN_TIME_BUCKET_MISMATCH")
                    decoded = decode_bar_row(bytes(raw_row), lpk, expected_open_ms=open_ms)
                except (StateCodecError, TypeError, ValueError) as error:
                    raise KnCacheIntegrityError(
                        f"market cache BAR row failed its trailer check: {lpk.encode()} open_ms={open_ms}"
                    ) from error
                rows[open_ms] = CacheRow(
                    decoded.canonical,
                    None if decoded.source_offset == MAX_OFFSET else decoded.source_offset,
                    open_ms,
                )


def reader_from_environment(environ: Mapping[str, str]) -> KnMarketCacheReader:
    """Build the reader from ``QDL_KN3_MARKET_CACHE_URL``/``QDL_KN3_ENVIRONMENT``
    and the state-topic names/partition counts (``QDL_KN_*``, projector names)."""

    url = environ.get(MARKET_CACHE_URL_ENV, "").strip()
    environment = environ.get(MARKET_CACHE_ENVIRONMENT_ENV, "").strip()
    if not url or not environment:
        raise ValueError(
            f"the kn3 Query backend requires {MARKET_CACHE_URL_ENV} and {MARKET_CACHE_ENVIRONMENT_ENV}"
        )
    import redis

    client = redis.Redis.from_url(
        url, decode_responses=False, socket_timeout=5.0, socket_connect_timeout=2.0,
        health_check_interval=30,
    )
    return KnMarketCacheReader(
        client, environment,
        latest_topic=environ.get(LATEST_TOPIC_ENV, "md.latest.v2"),
        bars_topic=environ.get(BARS_TOPIC_ENV, "md.bars.v2"),
        latest_partitions=int(environ.get(LATEST_PARTITIONS_ENV, "6")),
        bars_partitions=int(environ.get(BARS_PARTITIONS_ENV, "6")),
    )

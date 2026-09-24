"""KN-3 K3.6 BAR-edge readback from the Kafka-native market cache (decision D16b).

Purpose: the BAR edge asks one question of the durable cache before it
publishes provider history - "which of these opens of this binding are
already durable final bars?" (``StableBinanceBarEdge._durable_final_bar_opens``).
Today the answer comes from the canonical SQLite spool; this adapter answers
the same question, with the same contract, from the market cache that the
Rust projector stage B writes (``rust/qdl-projector/src/cache.rs`` +
``apply.lua``):

* product identity: the binding's ``LogicalProductKey`` built with exactly the
  rule of the gateway bundle (``scripts/kn_gateway_bundle.py``), so edge,
  Stream and projector name one product identically;
* only the published READY generation is read (``ptr:<lpk>`` field ``ready``);
  a product without one is typed ``NOT_READY_NO_GENERATION`` and raises
  ``KnBarReadbackNotReady`` - the same "durable cache unavailable" failure the
  SQLite path raises when its cache cannot be read, never an empty "nothing is
  durable" answer that would make the edge re-publish history;
* only the asked opens are read: one ``HMGET`` per bucket
  ``b:<g>:<lpk>:<open_ms div (116 x interval_ms)>`` (no SCAN/KEYS);
* every returned row is decoded with ``decode_bar_row`` (trailer hash proven)
  and must be a BAR of the binding's full identity (the same predicate as the
  SQLite path, ``bar_envelope_matches_binding``) at the asked open; anything
  else fails closed;
* an open counts only when ``is_final`` and lifecycle FINAL/REVISED;
* the pointer is read before and after; a READY generation (or fence) change
  during the read raises, like ``_assert_canonical_cache_identity``.

``generation_identity`` (``kn3:<env>:<lpk>@<ready generation or ->``) and
``cache_identity`` (32 hex over the sorted identities of a binding set)
replace the SQLite ``cache_id`` in the edge's rebase logic.

Boundary: read-only (``HMGET`` only, never a write), no key enumeration,
bounded by the asked opens. The Redis client is injected; this module never
touches the control/quota Redis. Selected by configuration in
``stable_bar_edge.build_from_environment`` (``QDL_STABLE_BAR_READBACK=kn3``);
the SQLite path stays the default until cutover (KN-5).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
from typing import Any, Iterable, Mapping

from qdl.adapters.intervals import canonical_interval_ms
from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.kn_state_codec import StateCodecError, decode_bar_row
from qdl.projection.state_contract import CacheReadState, LogicalProductKey

# The listpack bucket size of the Rust cache (`cache.rs` BUCKET_OPENS).
BUCKET_OPENS = 116
# Buckets per pipeline round trip; 10,000 opens of any fixed interval span at
# most 88 buckets, so one or two round trips answer a full warmup window.
MAX_BUCKETS_PER_ROUND_TRIP = 64
READBACK_ENV = "QDL_STABLE_BAR_READBACK"
MARKET_CACHE_URL_ENV = "QDL_KN3_MARKET_CACHE_URL"
MARKET_CACHE_ENVIRONMENT_ENV = "QDL_KN3_ENVIRONMENT"
READBACK_BACKENDS = ("sqlite", "kn3")
_FINAL_LIFECYCLES = frozenset({
    market_data_pb2.BAR_LIFECYCLE_FINAL,
    market_data_pb2.BAR_LIFECYCLE_REVISED,
})

try:  # the client library is present in the runtime image; tests may inject a fake
    from redis.exceptions import RedisError as _RedisError
except ImportError:  # pragma: no cover - host without redis-py
    _RedisError = OSError


class KnBarReadbackError(RuntimeError):
    """The market cache cannot prove durable coverage (fail closed)."""


class KnBarReadbackNotReady(KnBarReadbackError):
    """The product has no published READY generation (typed NOT_READY)."""

    def __init__(self, lpk: LogicalProductKey) -> None:
        self.state = CacheReadState.NOT_READY_NO_GENERATION
        self.lpk = lpk
        super().__init__(f"stable BAR market cache product is {self.state.value}: {lpk.encode()}")


def binding_product_key(source: Any, environment: str) -> LogicalProductKey:
    """The binding's LPK: the rule of ``scripts/kn_gateway_bundle.compile_bundle``."""

    identity = source.instrument.identity
    return LogicalProductKey.for_product(
        environment=environment, venue=identity.venue, market=identity.market,
        instrument_uid=identity.instrument_uid, feed=source.feed.value, interval=source.interval,
    )


def bar_envelope_matches_binding(envelope: market_data_pb2.EventEnvelope, source: Any) -> bool:
    """A BAR of exactly this binding (the SQLite path's identity predicate)."""

    if envelope.WhichOneof("payload") != "bar":
        return False
    expected_role = getattr(common_pb2, f"SOURCE_ROLE_{source.source_role}")
    return (
        envelope.instrument_uid == source.instrument.instrument_uid
        and envelope.instrument_id == source.instrument.instrument_id
        and source.accepts_instrument_revision(envelope.instrument_revision)
        and envelope.venue == source.instrument.identity.venue
        and envelope.market == source.instrument.identity.market
        and envelope.product_type == source.instrument.identity.product_type.value
        and envelope.native_symbol == source.instrument.native_symbol
        and envelope.provider == source.provider
        and envelope.source_id == source.source_id
        and envelope.source_role == expected_role
        and envelope.bar.interval == source.interval
    )


def bucket_of(open_ms: int, interval_ms: int) -> int:
    """``cache.rs`` ``bucket_of``: ``open_ms div (116 x interval_ms)``."""

    return open_ms // (BUCKET_OPENS * max(interval_ms, 1))


@dataclass(frozen=True)
class _Pointer:
    ready: int | None
    fence: int


def _decimal(value: Any) -> int | None:
    if value is None:
        return None
    text = value.decode("ascii") if isinstance(value, (bytes, bytearray)) else str(value)
    if not text.isdigit() or (len(text) > 1 and text[0] == "0"):
        raise KnBarReadbackError(f"stable BAR market cache pointer field is not a decimal: {text!r}")
    return int(text)


class KnBarReadback:
    """Read-only durable final-BAR coverage from the KN-3 market cache."""

    def __init__(self, redis_client: Any, environment: str) -> None:
        # Validates the environment exactly like an LPK field.
        LogicalProductKey(environment, "V", "M", "u", "BAR", "1m")
        self.client = redis_client
        self.environment = environment
        self.prefix = f"kn3:{environment}:"

    # -------------------------------------------------------------- layout (cache.rs)

    def _key(self, *parts: str) -> str:
        return self.prefix + ":".join(parts)

    def pointer_key(self, lpk: LogicalProductKey) -> str:
        return self._key("ptr", lpk.encode())

    def bucket_key(self, generation: int, lpk: LogicalProductKey, bucket: int) -> str:
        return self._key("b", str(generation), lpk.encode(), str(bucket))

    # -------------------------------------------------------------- pointers

    def product_key(self, source: Any) -> LogicalProductKey:
        return binding_product_key(source, self.environment)

    def _pointers(self, lpks: Iterable[LogicalProductKey]) -> list[_Pointer]:
        lpks = list(lpks)
        try:
            pipe = self.client.pipeline(transaction=False)
            for lpk in lpks:
                pipe.hmget(self.pointer_key(lpk), ["ready", "fence"])
            replies = pipe.execute()
        except _RedisError as error:
            raise KnBarReadbackError("stable BAR durable coverage is unavailable") from error
        if len(replies) != len(lpks):
            raise KnBarReadbackError("stable BAR market cache returned a short pointer reply")
        return [_Pointer(_decimal(ready), _decimal(fence) or 0) for ready, fence in replies]

    def generation_identity(self, source: Any) -> str:
        """``kn3:<env>:<lpk>@<ready generation>``; ``@-`` when not READY."""

        lpk = self.product_key(source)
        pointer = self._pointers([lpk])[0]
        return self._identity(lpk, pointer)

    def _identity(self, lpk: LogicalProductKey, pointer: _Pointer) -> str:
        ready = "-" if pointer.ready is None else str(pointer.ready)
        return f"{self.prefix}{lpk.encode()}@{ready}"

    def cache_identity(self, sources: Iterable[Any]) -> str:
        """32 hex over the sorted generation identities of ``sources`` (one round trip).

        Stands in for the SQLite ``cache_id``: it changes when any product of
        the set is (re)published or loses its READY generation, which is when
        the edge's watermarks stop proving durable history.
        """

        lpks = sorted({self.product_key(source) for source in sources}, key=lambda item: item.encode())
        if not lpks:
            raise KnBarReadbackError("stable BAR market cache identity needs at least one binding")
        lines = [self._identity(lpk, pointer) for lpk, pointer in zip(lpks, self._pointers(lpks), strict=True)]
        return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()[:32]

    # -------------------------------------------------------------- coverage

    def durable_final_bar_opens(self, source: Any, expected_opens: frozenset[int]) -> frozenset[int]:
        """Opens of ``expected_opens`` that are durable final bars of ``source``."""

        if not expected_opens:
            return frozenset()
        if source.feed.value != "BAR" or not source.interval:
            raise KnBarReadbackError(f"stable BAR readback needs a BAR binding: {source.binding_id}")
        lpk = self.product_key(source)
        interval_ms = canonical_interval_ms(source.interval)
        before = self._pointers([lpk])[0]
        if before.ready is None:
            raise KnBarReadbackNotReady(lpk)
        buckets: dict[int, list[int]] = defaultdict(list)
        for open_ms in sorted(expected_opens):
            if isinstance(open_ms, bool) or not isinstance(open_ms, int) or open_ms < 0:
                raise KnBarReadbackError(f"stable BAR readback open is invalid: {open_ms!r}")
            buckets[bucket_of(open_ms, interval_ms)].append(open_ms)
        covered: set[int] = set()
        ordered = sorted(buckets.items())
        for start in range(0, len(ordered), MAX_BUCKETS_PER_ROUND_TRIP):
            chunk = ordered[start:start + MAX_BUCKETS_PER_ROUND_TRIP]
            try:
                pipe = self.client.pipeline(transaction=False)
                for bucket, opens in chunk:
                    pipe.hmget(self.bucket_key(before.ready, lpk, bucket), [str(item) for item in opens])
                replies = pipe.execute()
            except _RedisError as error:
                raise KnBarReadbackError("stable BAR durable coverage is unavailable") from error
            if len(replies) != len(chunk):
                raise KnBarReadbackError("stable BAR market cache returned a short bucket reply")
            for (_bucket, opens), rows in zip(chunk, replies, strict=True):
                if len(rows) != len(opens):
                    raise KnBarReadbackError("stable BAR market cache returned a short row reply")
                for open_ms, row in zip(opens, rows, strict=True):
                    if row is not None and self._final_row(source, lpk, open_ms, row):
                        covered.add(open_ms)
        after = self._pointers([lpk])[0]
        if after != before:
            raise KnBarReadbackError(
                "stable BAR market cache generation changed during readback "
                f"{self._identity(lpk, before)} -> {self._identity(lpk, after)}"
            )
        return frozenset(covered)

    @staticmethod
    def _final_row(source: Any, lpk: LogicalProductKey, open_ms: int, row: bytes) -> bool:
        try:
            decoded = decode_bar_row(bytes(row), lpk)
            envelope = market_data_pb2.EventEnvelope.FromString(decoded.canonical)
        except (StateCodecError, ValueError) as error:
            raise KnBarReadbackError(
                f"stable BAR market cache row is unreadable binding={source.binding_id} open_ms={open_ms}"
            ) from error
        if not bar_envelope_matches_binding(envelope, source):
            raise KnBarReadbackError(
                f"stable BAR market cache row differs from its binding binding={source.binding_id} open_ms={open_ms}"
            )
        if envelope.bar.open_time_ns != open_ms * 1_000_000:
            raise KnBarReadbackError(
                f"stable BAR market cache row is filed under another open binding={source.binding_id} open_ms={open_ms}"
            )
        return envelope.bar.is_final and envelope.bar.lifecycle in _FINAL_LIFECYCLES


def readback_from_environment(environ: Mapping[str, str]) -> KnBarReadback:
    """Build the market-cache readback from ``QDL_KN3_MARKET_CACHE_URL``/``QDL_KN3_ENVIRONMENT``."""

    url = environ.get(MARKET_CACHE_URL_ENV, "").strip()
    environment = environ.get(MARKET_CACHE_ENVIRONMENT_ENV, "").strip()
    if not url or not environment:
        raise ValueError(
            f"{READBACK_ENV}=kn3 requires {MARKET_CACHE_URL_ENV} and {MARKET_CACHE_ENVIRONMENT_ENV}"
        )
    import redis

    return KnBarReadback(redis.Redis.from_url(url, decode_responses=False), environment)

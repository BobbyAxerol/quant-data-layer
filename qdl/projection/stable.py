from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol

import redis

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.provider.v1 import raw_provider_pb2
from qdl.runtime.mark_index_lineage import (
    validate_derived_mark_index_component,
    validate_single_raw_lineage,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.transport import StoredEvent


_KEY_PATTERNS = tuple(re.compile(value) for value in (
    r"trade:price:(?:last:)?(?:binance_usdm|binance_spot):[A-Z0-9_-]+",
    r"trade:price:(?:last:)?[A-Z0-9_-]+",
    r"kline:(?:last:)?[0-9]+[smhdw]:[A-Z0-9_-]+",
    r"vn:quote:(?:last:)?[A-Z0-9_-]+",
))
_CHANNEL_PATTERNS = tuple(re.compile(value) for value in (
    r"stream:trade:(?:binance_usdm|binance_spot):[A-Z0-9_-]+",
    r"stream:trade:[A-Z0-9_-]+",
    r"stream:kline:[0-9]+[smhdw]:[A-Z0-9_-]+",
    r"stream:vn:[A-Z0-9_-]+",
))


@dataclass(frozen=True, slots=True)
class StableProjectionItem:
    key: str
    payload: bytes
    ttl_seconds: int = 0

    def __post_init__(self) -> None:
        if not self.key or not self.payload or self.ttl_seconds < 0:
            raise ValueError("stable projection item is invalid")


@dataclass(frozen=True, slots=True)
class StableProjectionRecord:
    partition_key: str
    offset: int
    event_id_hex: str
    shard_id: str
    lease_epoch: int
    items: tuple[StableProjectionItem, ...]
    publications: tuple[tuple[str, bytes], ...] = ()

    def __post_init__(self) -> None:
        if (
            not self.partition_key
            or self.offset < 1
            or len(self.event_id_hex) != 32
            or not self.shard_id
            or self.lease_epoch < 1
            or not self.items
        ):
            raise ValueError("stable projection record identity is invalid")


class ProjectionFenced(RuntimeError):
    """Projection lease epoch is older than the committed Redis writer epoch."""


class ProjectionCacheMismatch(RuntimeError):
    """Redis and SQLite do not belong to the same rebuildable cache unit."""


class StableProjectionTarget(Protocol):
    def apply(self, record: StableProjectionRecord) -> bool: ...
    def apply_many(
        self, records: tuple[StableProjectionRecord, ...] | list[StableProjectionRecord]
    ) -> tuple[bool, ...]: ...


class InMemoryStableProjectionTarget:
    def __init__(self) -> None:
        self.latest: dict[str, bytes] = {}
        self.checkpoints: dict[str, tuple[int, str]] = {}
        self.lease_epochs: dict[str, int] = {}
        self.publications: list[tuple[str, bytes]] = []

    def apply(self, record: StableProjectionRecord) -> bool:
        return self.apply_many((record,))[0]

    def apply_many(
        self, records: tuple[StableProjectionRecord, ...] | list[StableProjectionRecord]
    ) -> tuple[bool, ...]:
        results = []
        for record in records:
            observed_epoch = self.lease_epochs.get(record.shard_id, 0)
            if record.lease_epoch < observed_epoch:
                raise ProjectionFenced("stable projection lease epoch is stale")
            current = self.checkpoints.get(record.partition_key)
            if current is not None and record.offset <= current[0]:
                results.append(False)
                continue
            self.lease_epochs[record.shard_id] = record.lease_epoch
            self.latest.update({item.key: item.payload for item in record.items})
            self.publications.extend(record.publications)
            self.checkpoints[record.partition_key] = (
                record.offset, record.event_id_hex
            )
            results.append(True)
        return tuple(results)


_STABLE_CACHE_BIND_LUA = r"""
local current = redis.call('GET', KEYS[1])
if current then
  if current == ARGV[1] then
    return 1
  end
  return -1
end
if ARGV[2] ~= '1' then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"""


_STABLE_APPLY_LUA = r"""
local cache_id = redis.call('GET', KEYS[3])
if not cache_id or cache_id ~= ARGV[4] then
  return -2
end
local current_epoch = redis.call('GET', KEYS[2])
if current_epoch and tonumber(current_epoch) > tonumber(ARGV[3]) then
  return -1
end
local current = redis.call('GET', KEYS[1])
if current then
  local current_offset = string.sub(current, 1, 20)
  if current_offset >= ARGV[1] then
    return 0
  end
end
local item_count = tonumber(ARGV[5])
for index = 1, item_count do
  local argument = 6 + ((index - 1) * 2)
  local ttl = tonumber(ARGV[argument + 1])
  if ttl > 0 then
    redis.call('SETEX', KEYS[index + 3], ttl, ARGV[argument])
  else
    redis.call('SET', KEYS[index + 3], ARGV[argument])
  end
end
local channel_count_index = 6 + (item_count * 2)
local channel_count = tonumber(ARGV[channel_count_index])
for index = 1, channel_count do
  local argument = channel_count_index + 1 + ((index - 1) * 2)
  redis.call('PUBLISH', ARGV[argument], ARGV[argument + 1])
end
redis.call('SET', KEYS[2], ARGV[3])
redis.call('SET', KEYS[1], ARGV[1] .. ':' .. ARGV[2])
return 1
"""


class RedisStableProjectionTarget:
    """Atomic stable latest/V1 projection for a dedicated Redis database."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        namespace: str = "qdl:stable:v2",
        dedicated_database: bool,
    ) -> None:
        self._client = client
        self._namespace = namespace.rstrip(":")
        self._cache_id: str | None = None
        if not self._namespace or not dedicated_database:
            raise ValueError("stable Redis projection requires an isolated database")

    @property
    def cache_identity_key(self) -> str:
        return f"{self._namespace}:projection-cache-id"

    def bind_cache(self, cache_id: str, *, initialize_if_missing: bool) -> None:
        if len(cache_id) != 32 or any(
            character not in "0123456789abcdef" for character in cache_id
        ):
            raise ValueError("stable projection cache identity is invalid")
        result = int(self._client.eval(
            _STABLE_CACHE_BIND_LUA,
            1,
            self.cache_identity_key,
            cache_id,
            "1" if initialize_if_missing else "0",
        ))
        if result < 0:
            raise ProjectionCacheMismatch(
                "stable Redis and SQLite projection cache identities differ"
            )
        if result == 0:
            raise ProjectionCacheMismatch(
                "stable Redis cache identity is missing for a non-empty spool"
            )
        self._cache_id = cache_id

    def cache_is_bound(self) -> bool:
        if self._cache_id is None:
            return False
        value = self._client.get(self.cache_identity_key)
        if isinstance(value, bytes):
            value = value.decode("ascii")
        return value == self._cache_id

    def apply(self, record: StableProjectionRecord) -> bool:
        return self.apply_many((record,))[0]

    def apply_many(
        self, records: tuple[StableProjectionRecord, ...] | list[StableProjectionRecord]
    ) -> tuple[bool, ...]:
        values = tuple(records)
        if not values:
            return ()
        if self._cache_id is None:
            raise ProjectionCacheMismatch("stable projection cache is not bound")
        commands = [self._command(record) for record in values]
        pipeline = self._client.pipeline(transaction=False)
        for keys, arguments in commands:
            pipeline.eval(_STABLE_APPLY_LUA, len(keys), *keys, *arguments)
        raw_results = pipeline.execute()
        results = []
        for result in raw_results:
            value = int(result)
            if value == -2:
                raise ProjectionCacheMismatch(
                    "stable projection cache identity changed during operation"
                )
            if value < 0:
                raise ProjectionFenced("stable projection lease epoch is stale")
            results.append(value > 0)
        return tuple(results)

    def _command(
        self, record: StableProjectionRecord
    ) -> tuple[list[str], list[str | bytes]]:
        for item in record.items:
            if not (
                item.key.startswith(f"{self._namespace}:")
                or any(pattern.fullmatch(item.key) for pattern in _KEY_PATTERNS)
            ):
                raise ValueError("stable projection key escapes its allowlist")
        for channel, payload in record.publications:
            if not payload or not any(
                pattern.fullmatch(channel) for pattern in _CHANNEL_PATTERNS
            ):
                raise ValueError("stable compatibility channel escapes its allowlist")
        partition_digest = hashlib.sha256(record.partition_key.encode()).hexdigest()
        shard_digest = hashlib.sha256(record.shard_id.encode()).hexdigest()
        keys = [
            f"{self._namespace}:checkpoint:{partition_digest}",
            f"{self._namespace}:lease-epoch:{shard_digest}",
            self.cache_identity_key,
            *(item.key for item in record.items),
        ]
        arguments: list[str | bytes] = [
            f"{record.offset:020d}",
            record.event_id_hex,
            str(record.lease_epoch),
            self._cache_id or "",
            str(len(record.items)),
        ]
        for item in record.items:
            arguments.extend((item.payload, str(item.ttl_seconds)))
        arguments.append(str(len(record.publications)))
        for channel, payload in record.publications:
            arguments.extend((channel, payload))
        return keys, arguments


class StableCompatibilityProjector:
    def __init__(
        self,
        catalog: StableSourceCatalog,
        *,
        namespace: str = "qdl:stable:v2",
        latest_ttl_seconds: int = 60,
    ) -> None:
        if latest_ttl_seconds <= 0:
            raise ValueError("stable compatibility TTL must be positive")
        self.catalog = catalog
        self.namespace = namespace.rstrip(":")
        self.latest_ttl_seconds = latest_ttl_seconds

    def build(
        self,
        stored: StoredEvent,
        raw_envelope_bytes: bytes,
        *,
        derived_mark_index_component: bool = False,
    ) -> StableProjectionRecord:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        binding = self.catalog.binding_for_envelope(envelope)
        raw = raw_provider_pb2.RawProviderEnvelope.FromString(raw_envelope_bytes)
        if derived_mark_index_component:
            validate_derived_mark_index_component(envelope, raw, binding)
        else:
            validate_single_raw_lineage(envelope, raw)
        raw_json = json.loads(bytes(raw.raw_frame_bytes))
        if not isinstance(raw_json, dict):
            raise ValueError("stable compatibility raw frame must be a JSON object")
        feed = envelope.WhichOneof("payload")
        items = [StableProjectionItem(
            key=(
                f"{self.namespace}:latest:{feed}:{envelope.venue.lower()}:"
                f"{envelope.market.lower()}:{envelope.instrument_uid}"
            ),
            payload=stored.event.payload,
        )]
        publications: list[tuple[str, bytes]] = []
        self._legacy(binding, envelope, raw_json, items, publications)
        return StableProjectionRecord(
            partition_key=stored.cursor.partition_key,
            offset=stored.cursor.offset,
            event_id_hex=stored.event.event_id.hex(),
            shard_id=envelope.source_id,
            lease_epoch=envelope.lease_epoch,
            items=tuple(items),
            publications=tuple(publications),
        )

    def _legacy(self, binding, envelope, raw, items, publications) -> None:
        policy = binding.v1_compatibility
        if policy == "NONE":
            return
        symbol = envelope.native_symbol.upper()
        if policy.startswith("BINANCE_TRADE"):
            market = {
                "USDM": "binance_usdm",
                "SPOT": "binance_spot",
            }[envelope.market]
            trade = envelope.trade
            buyer_maker = trade.aggressor_side == common_pb2.AGGRESSOR_SIDE_SELL
            payload = json.dumps({
                "authoritative": True,
                "event_time": envelope.source_event_time_ns // 1_000_000,
                "is_live": True,
                "market": market,
                "price": float(trade.price.source_text),
                "provider": "binance",
                "quantity": float(trade.quantity.source_text),
                "raw": raw,
                "side": "sell" if buyer_maker else "buy",
                "source": f"{market}_trade",
                "symbol": symbol,
                "trade_id": int(trade.native_trade_id) if trade.native_trade_id.isdigit() else 0,
                "trade_time": envelope.source_event_time_ns // 1_000_000,
            }, sort_keys=True, separators=(",", ":")).encode()
            self._current_and_last(
                items, f"trade:price:{market}:{symbol}",
                f"trade:price:last:{market}:{symbol}", payload,
            )
            publications.append((f"stream:trade:{market}:{symbol}", payload))
            if policy == "BINANCE_TRADE_MARKET_AND_GENERIC":
                self._current_and_last(
                    items, f"trade:price:{symbol}", f"trade:price:last:{symbol}", payload,
                )
                publications.append((f"stream:trade:{symbol}", payload))
            return
        if policy == "BINANCE_BAR_GENERIC":
            bar = envelope.bar
            payload = json.dumps({
                "e": "kline",
                "E": envelope.source_event_time_ns // 1_000_000,
                "s": symbol,
                "k": {
                    "t": bar.open_time_ns // 1_000_000,
                    "T": bar.close_time_ns // 1_000_000,
                    "s": symbol,
                    "i": bar.interval,
                    "o": bar.open.source_text,
                    "h": bar.high.source_text,
                    "l": bar.low.source_text,
                    "c": bar.close.source_text,
                    "v": bar.volume.source_text,
                    "n": bar.trade_count,
                    "x": bool(bar.is_final),
                    "q": bar.quote_volume.source_text if bar.HasField("quote_volume") else None,
                },
            }, sort_keys=True, separators=(",", ":")).encode()
            self._current_and_last(
                items, f"kline:{bar.interval}:{symbol}",
                f"kline:last:{bar.interval}:{symbol}", payload,
            )
            publications.append((f"stream:kline:{bar.interval}:{symbol}", payload))
            return
        if policy == "VN_TRADE_GENERIC":
            if str(raw.get("symbol", "")).upper() != symbol:
                raise ValueError("VN raw symbol differs from canonical instrument")
            payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            self._current_and_last(
                items, f"vn:quote:{symbol}", f"vn:quote:last:{symbol}", payload,
            )
            publications.append((f"stream:vn:{symbol}", payload))
            return
        raise ValueError("unsupported stable V1 compatibility policy")

    def _current_and_last(self, items, current, last, payload) -> None:
        items.extend((
            StableProjectionItem(current, payload, self.latest_ttl_seconds),
            StableProjectionItem(last, payload),
        ))

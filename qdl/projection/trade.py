from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2
from qdl.transport.contracts import StoredEvent


def _decimal_text(value: common_pb2.DecimalValue) -> str:
    return value.source_text


@dataclass(frozen=True)
class ProjectionRecord:
    partition_key: str
    offset: int
    event_id_hex: str
    canonical_key: str
    canonical_payload: bytes
    legacy_items: tuple[tuple[str, bytes], ...]


class ProjectionTarget(Protocol):
    def apply(self, record: ProjectionRecord) -> bool: ...


class InMemoryProjectionTarget:
    def __init__(self):
        self.latest: dict[str, bytes] = {}
        self.checkpoints: dict[str, tuple[int, str]] = {}

    def apply(self, record: ProjectionRecord) -> bool:
        current = self.checkpoints.get(record.partition_key)
        if current is not None and record.offset <= current[0]:
            return False
        self.latest[record.canonical_key] = record.canonical_payload
        self.latest.update(dict(record.legacy_items))
        self.checkpoints[record.partition_key] = (record.offset, record.event_id_hex)
        return True

    def checksum(self, *, prefix: str = "") -> str:
        digest = hashlib.sha256()
        for key, value in sorted(self.latest.items()):
            if key.startswith(prefix):
                digest.update(len(key).to_bytes(4, "big"))
                digest.update(key.encode())
                digest.update(hashlib.sha256(value).digest())
        return digest.hexdigest()

    def clear(self) -> None:
        self.latest.clear()
        self.checkpoints.clear()


class TradeProjector:
    def __init__(
        self,
        target: ProjectionTarget,
        *,
        namespace: str = "shadow:qdl:v2",
        raw_resolver: Callable[[str, bytes], bytes | None] | None = None,
    ):
        self._target = target
        self._namespace = namespace.rstrip(":")
        self._raw_resolver = raw_resolver

    def project(self, stored: StoredEvent) -> bool:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        if envelope.WhichOneof("payload") != "trade":
            raise ValueError("trade projector received a non-trade canonical event")
        trade = envelope.trade
        market = _legacy_market(envelope.venue, envelope.market)
        source_event_ms = envelope.source_event_time_ns // 1_000_000
        buyer_maker = trade.aggressor_side == common_pb2.AGGRESSOR_SIDE_SELL
        native_id = int(trade.native_trade_id) if trade.native_trade_id.isdigit() else 0
        raw = self._resolve_raw(stored)
        legacy = {
            "authoritative": True,
            "event_time": source_event_ms,
            "is_live": True,
            "market": market,
            "price": float(_decimal_text(trade.price)),
            "provider": _legacy_provider(envelope.venue),
            "quantity": float(_decimal_text(trade.quantity)),
            "raw": raw,
            "side": "buy" if not buyer_maker else "sell",
            "source": _legacy_source(envelope.venue, envelope.market),
            "symbol": envelope.native_symbol,
            "trade_id": native_id,
            "trade_time": source_event_ms,
        }
        legacy_payload = json.dumps(
            legacy, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode()
        canonical_key = (
            f"{self._namespace}:latest:trade:{envelope.venue.lower()}:"
            f"{envelope.market.lower()}:{envelope.instrument_uid}"
        )
        legacy_items = (
            (f"{self._namespace}:legacy:trade:price:{market}:{envelope.native_symbol}", legacy_payload),
            (f"{self._namespace}:legacy:trade:price:{envelope.native_symbol}", legacy_payload),
        )
        return self._target.apply(
            ProjectionRecord(
                partition_key=stored.cursor.partition_key,
                offset=stored.cursor.offset,
                event_id_hex=stored.event.event_id.hex(),
                canonical_key=canonical_key,
                canonical_payload=stored.event.payload,
                legacy_items=legacy_items,
            )
        )

    def _resolve_raw(self, stored: StoredEvent) -> dict:
        raw_stream = stored.event.headers.get("raw_stream")
        raw_event_hex = stored.event.headers.get("raw_event_id")
        if not raw_stream or not raw_event_hex or self._raw_resolver is None:
            raise ValueError("V1 projection requires a durable raw-event reference")
        try:
            payload = self._raw_resolver(raw_stream, bytes.fromhex(raw_event_hex))
        except ValueError as exc:
            raise ValueError("invalid raw-event reference") from exc
        if payload is None:
            raise ValueError("referenced durable raw event is unavailable")
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise ValueError("referenced raw event is not an object")
        return decoded


def _legacy_market(venue: str, market: str) -> str:
    identity = (venue.upper(), market.upper())
    aliases = {
        ("BINANCE", "USDM"): "binance_usdm",
        ("BINANCE", "SPOT"): "binance_spot",
        ("OKX", "SWAP"): "okx_swap",
        ("OKX", "SPOT"): "okx_spot",
    }
    try:
        return aliases[identity]
    except KeyError as exc:
        raise ValueError(f"no frozen V1 trade projection for {identity}") from exc


def _legacy_provider(venue: str) -> str:
    providers = {"BINANCE": "binance", "OKX": "okx"}
    try:
        return providers[venue.upper()]
    except KeyError as exc:
        raise ValueError(f"no frozen V1 provider mapping for {venue}") from exc


def _legacy_source(venue: str, market: str) -> str:
    return _legacy_market(venue, market).replace("_swap", "_trade") + (
        "_trade" if venue.upper() == "BINANCE" else ""
    )

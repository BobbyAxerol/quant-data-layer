from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
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
    def __init__(self, target: ProjectionTarget, *, namespace: str = "shadow:qdl:v2"):
        self._target = target
        self._namespace = namespace.rstrip(":")

    def project(self, stored: StoredEvent) -> bool:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        if envelope.WhichOneof("payload") != "trade":
            raise ValueError("trade projector received a non-trade canonical event")
        trade = envelope.trade
        market = _legacy_market(envelope.venue, envelope.market)
        source_event_ms = envelope.source_event_time_ns // 1_000_000
        buyer_maker = trade.aggressor_side == common_pb2.AGGRESSOR_SIDE_SELL
        native_id = int(trade.native_trade_id) if trade.native_trade_id.isdigit() else 0
        raw = {
            "E": source_event_ms,
            "T": source_event_ms,
            "e": "aggTrade",
            "m": buyer_maker,
            "p": _decimal_text(trade.price),
            "q": _decimal_text(trade.quantity),
            "s": envelope.native_symbol,
            "t": native_id,
        }
        legacy = {
            "authoritative": True,
            "event_time": source_event_ms,
            "is_live": True,
            "market": market,
            "price": float(_decimal_text(trade.price)),
            "provider": envelope.provider.lower(),
            "quantity": float(_decimal_text(trade.quantity)),
            "raw": raw,
            "side": "buy" if not buyer_maker else "sell",
            "source": envelope.source_id,
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

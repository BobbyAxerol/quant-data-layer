from __future__ import annotations

import json
from collections.abc import Callable

from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.trade import ProjectionRecord, ProjectionTarget, _legacy_market
from qdl.transport.contracts import StoredEvent


class MarketProjector:
    """Canonical latest projection plus frozen V1-compatible bar projection."""

    def __init__(
        self,
        target: ProjectionTarget,
        *,
        namespace: str = "shadow:qdl:v2",
        raw_resolver: Callable[[str, bytes], bytes | None] | None = None,
    ) -> None:
        self._target = target
        self._namespace = namespace.rstrip(":")
        self._raw_resolver = raw_resolver

    def project(self, stored: StoredEvent) -> bool:
        envelope = market_data_pb2.EventEnvelope.FromString(stored.event.payload)
        feed = envelope.WhichOneof("payload")
        if feed not in {"quote", "bar", "book_snapshot", "book_delta"}:
            raise ValueError(f"market projector does not support payload: {feed}")
        canonical_key = (
            f"{self._namespace}:latest:{feed}:{envelope.venue.lower()}:"
            f"{envelope.market.lower()}:{envelope.instrument_uid}"
        )
        legacy_items: tuple[tuple[str, bytes], ...] = ()
        if feed == "bar":
            raw = self._raw(stored)
            interval = envelope.bar.interval
            market = _legacy_market(envelope.venue, envelope.market)
            payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            legacy_items = (
                (f"{self._namespace}:legacy:kline:{market}:{interval}:{envelope.native_symbol}", payload),
                (f"{self._namespace}:legacy:kline:{interval}:{envelope.native_symbol}", payload),
            )
        return self._target.apply(
            ProjectionRecord(
                feed_key=(
                    f"{envelope.venue.upper()}:{envelope.market.upper()}:"
                    f"{feed}:{envelope.native_symbol.upper()}"
                ),
                partition_key=stored.cursor.partition_key,
                offset=stored.cursor.offset,
                event_id_hex=stored.event.event_id.hex(),
                canonical_key=canonical_key,
                canonical_payload=stored.event.payload,
                legacy_items=legacy_items,
                shard_id=envelope.source_id,
                lease_epoch=envelope.lease_epoch,
            )
        )

    def _raw(self, stored: StoredEvent) -> dict:
        stream = stored.event.headers.get("raw_stream")
        event_hex = stored.event.headers.get("raw_event_id")
        if not stream or not event_hex or self._raw_resolver is None:
            raise ValueError("V1 projection requires a durable raw-event reference")
        payload = self._raw_resolver(stream, bytes.fromhex(event_hex))
        decoded = json.loads(payload) if payload is not None else None
        if not isinstance(decoded, dict):
            raise ValueError("referenced durable raw event is unavailable or invalid")
        return decoded

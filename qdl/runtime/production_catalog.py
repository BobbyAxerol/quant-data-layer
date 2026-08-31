from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import yaml

from qdl.adapters.binance_usdm import BinanceDiscovery, parse_exchange_info
from qdl.adapters.intervals import (
    BINANCE_SPOT_NATIVE_INTERVALS,
    BINANCE_USDM_NATIVE_INTERVALS,
    canonical_interval_ms,
    okx_candle_channel,
)
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.domain.instrument import InstrumentRecord, InstrumentStatus
from qdl.query import ConsumerGrade, FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    V2_REALTIME_RAW_TOPIC,
    StableAcquisitionPlan,
)


_DEMAND_SCHEMA = "qdl.v2.production-demand.v1"
_SOURCE_SCHEMA = "qdl.v2.stable-source-bindings.v1"
_ACQUISITION_SCHEMA = "qdl.v2.stable-acquisition-bindings.v1"
_SUPPORTED_MARKETS = {
    ("BINANCE", "USDM", "PERPETUAL"),
    ("BINANCE", "USDM", "FUTURE"),
    ("BINANCE", "SPOT", "SPOT"),
    ("OKX", "SWAP", "PERPETUAL"),
    # OKX dated legs share the documented public market-data socket with
    # swaps, but retain their own canonical market/product identity.
    ("OKX", "FUTURES", "FUTURE"),
    ("OKX", "SPOT", "SPOT"),
}
_BINANCE_STREAM_URL = {
    # One venue/market worker uses Binance's documented control endpoint and
    # subscribes the resolved demand dynamically. Symbols never become
    # containers or URL-specific combined-stream shards.
    "USDM": "wss://fstream.binance.com/ws",
    "SPOT": "wss://stream.binance.com:9443/ws",
}
_BOOK_FEEDS = frozenset({FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA})
_SUPPORTED_FEEDS = {FeedType.TRADE, FeedType.QUOTE, FeedType.BAR, *_BOOK_FEEDS}
_BINANCE_DEPTH_REST = {
    "USDM": "https://fapi.binance.com/fapi/v1/depth",
    "SPOT": "https://api.binance.com/api/v3/depth",
}
_OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"


@dataclass(frozen=True, slots=True, order=True)
class ProductionDemand:
    consumer_id: str
    consumer_grade: ConsumerGrade
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: FeedType
    interval: str | None
    source_policy_id: str
    # Book feeds are physical provider subscriptions represented by two public
    # logical products.  These values are carried through the catalog rather
    # than rediscovered by an adapter at runtime.
    depth_per_side: int = 0
    max_freshness_ms: int | None = None
    require_live: bool = False

    def __post_init__(self) -> None:
        if (self.venue, self.market, self.product_type) not in _SUPPORTED_MARKETS:
            raise ValueError("production demand market/product is not certified")
        if self.feed not in _SUPPORTED_FEEDS:
            raise ValueError("production demand feed is not certified")
        if self.feed is FeedType.BAR:
            _validate_native_bar_interval(
                venue=self.venue,
                market=self.market,
                interval=self.interval,
            )
        elif self.feed in _BOOK_FEEDS:
            if self.interval is not None:
                raise ValueError("interval is invalid for BOOK demand")
            if not 1 <= self.depth_per_side <= 10_000:
                raise ValueError("BOOK demand requires an explicit bounded depth")
            if self.max_freshness_ms is None or self.max_freshness_ms <= 0:
                raise ValueError("BOOK demand requires an explicit freshness bound")
            if not self.require_live:
                raise ValueError("BOOK demand must require a live provider feed")
        elif self.interval is not None:
            raise ValueError("interval is valid only for BAR demand")
        elif self.depth_per_side or self.max_freshness_ms is not None or self.require_live:
            raise ValueError("non-BOOK demand cannot carry BOOK acquisition fields")
        if not self.consumer_id.strip() or not self.native_symbol.strip() or not self.source_policy_id.strip():
            raise ValueError("production demand identity/source policy is incomplete")

    @property
    def requirement_key(self) -> tuple[str, str, str, str, FeedType, str | None]:
        return (
            self.venue,
            self.market,
            self.product_type,
            self.native_symbol,
            self.feed,
            self.interval,
        )


@dataclass(frozen=True, slots=True)
class ProductionDemandManifest:
    revision: int
    demands: tuple[ProductionDemand, ...]
    source_paths: tuple[str, ...]

    @classmethod
    def load_many(cls, paths: Iterable[str | Path]) -> "ProductionDemandManifest":
        demands: list[ProductionDemand] = []
        revisions: list[int] = []
        normalized_paths: list[str] = []
        for raw_path in paths:
            path = Path(raw_path).resolve()
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or set(payload) != {
                "schema", "revision", "consumers"
            }:
                raise ValueError("production demand manifest fields are invalid")
            if payload["schema"] != _DEMAND_SCHEMA or int(payload["revision"]) < 1:
                raise ValueError("production demand manifest schema/revision is invalid")
            consumers = payload["consumers"]
            if not isinstance(consumers, list) or not 1 <= len(consumers) <= 10_000:
                raise ValueError("production demand manifest needs bounded consumers")
            revisions.append(int(payload["revision"]))
            normalized_paths.append(str(path))
            for consumer in consumers:
                if not isinstance(consumer, dict) or set(consumer) != {
                    "consumer_id", "consumer_grade", "requirements"
                }:
                    raise ValueError("production demand consumer fields are invalid")
                consumer_id = str(consumer["consumer_id"]).strip()
                grade = ConsumerGrade(str(consumer["consumer_grade"]).upper())
                requirements = consumer["requirements"]
                if not consumer_id or not isinstance(requirements, list) or not requirements:
                    raise ValueError("production demand consumer is empty")
                for requirement in requirements:
                    demands.append(cls._requirement(consumer_id, grade, requirement))
        if not demands:
            raise ValueError("production demand set cannot be empty")
        deduped: dict[tuple[str, str, str, str, FeedType, str | None], ProductionDemand] = {}
        consumers_by_key: dict[tuple[str, str, str, str, FeedType, str | None], set[str]] = {}
        for item in sorted(demands):
            key = item.requirement_key
            current = deduped.get(key)
            if current is not None and current.source_policy_id != item.source_policy_id:
                raise ValueError("one feed requirement cannot use conflicting source policies")
            deduped.setdefault(key, item)
            consumers_by_key.setdefault(key, set()).add(item.consumer_id)
        # Consumer IDs are audit inputs but do not alter a canonical source binding.
        del consumers_by_key
        return cls(
            revision=max(revisions),
            demands=tuple(sorted(deduped.values())),
            source_paths=tuple(sorted(normalized_paths)),
        )

    @staticmethod
    def _requirement(
        consumer_id: str,
        grade: ConsumerGrade,
        raw: Any,
    ) -> ProductionDemand:
        required = {
            "venue", "market", "product_type", "native_symbol",
            "feed", "interval", "source_policy_id",
        }
        book_fields = {"depth_per_side", "max_freshness_ms", "require_live"}
        if not isinstance(raw, dict) or not required <= set(raw):
            raise ValueError("production demand requirement fields are invalid")
        venue = str(raw["venue"]).strip().upper()
        market = str(raw["market"]).strip().upper()
        product = str(raw["product_type"]).strip().upper()
        native_symbol = str(raw["native_symbol"]).strip().upper()
        source_policy = str(raw["source_policy_id"]).strip()
        feed = FeedType(str(raw["feed"]).strip().upper())
        interval = str(raw["interval"]).strip() if raw["interval"] is not None else None
        expected = required | book_fields if feed in _BOOK_FEEDS else required
        if set(raw) != expected:
            raise ValueError("production demand requirement fields are invalid")
        if feed in _BOOK_FEEDS:
            depth_per_side = raw["depth_per_side"]
            max_freshness_ms = raw["max_freshness_ms"]
            require_live = raw["require_live"]
            if (
                isinstance(depth_per_side, bool)
                or not isinstance(depth_per_side, int)
                or isinstance(max_freshness_ms, bool)
                or not isinstance(max_freshness_ms, int)
                or not isinstance(require_live, bool)
            ):
                raise ValueError("BOOK demand acquisition fields have invalid types")
        else:
            depth_per_side = 0
            max_freshness_ms = None
            require_live = False
        return ProductionDemand(
            consumer_id=consumer_id,
            consumer_grade=grade,
            venue=venue,
            market=market,
            product_type=product,
            native_symbol=native_symbol,
            feed=feed,
            interval=interval,
            source_policy_id=source_policy,
            depth_per_side=depth_per_side,
            max_freshness_ms=max_freshness_ms,
            require_live=require_live,
        )


def _validate_native_bar_interval(*, venue: str, market: str, interval: str | None) -> None:
    if interval is None:
        raise ValueError("BAR demand requires an interval")
    # Parsing the canonical interval first keeps case-sensitive `1M` versus
    # `1m` and calendar-month ambiguity out of every provider adapter.
    canonical_interval_ms(interval)
    if venue == "BINANCE":
        supported = (
            BINANCE_USDM_NATIVE_INTERVALS
            if market == "USDM"
            else BINANCE_SPOT_NATIVE_INTERVALS
        )
        if interval not in supported:
            raise ValueError(
                f"Binance {market} does not expose canonical BAR interval: {interval}"
            )
        return
    if venue == "OKX":
        # The helper encodes the documented UTC calendar spelling and fails
        # closed for unsupported native channels.
        okx_candle_channel(interval)
        return
    raise ValueError(f"production BAR venue is not certified: {venue}/{market}")


@dataclass(frozen=True, slots=True)
class ProductionCatalogBundle:
    source_catalog: dict[str, Any]
    acquisition_plan: dict[str, Any]
    provenance: dict[str, Any]

    def write(self, output_dir: str | Path) -> dict[str, str]:
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        source_path = target / "production-source-bindings.yaml"
        acquisition_path = target / "production-acquisition-bindings.yaml"
        provenance_path = target / "production-catalog-provenance.json"
        source_path.write_text(yaml.safe_dump(self.source_catalog, sort_keys=False))
        acquisition_path.write_text(yaml.safe_dump(self.acquisition_plan, sort_keys=False))
        provenance_path.write_text(
            json.dumps(self.provenance, indent=2, sort_keys=True) + "\n"
        )
        catalog = StableSourceCatalog.load(source_path)
        StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
        return {
            "source_catalog": str(source_path),
            "acquisition_plan": str(acquisition_path),
            "provenance": str(provenance_path),
        }


class ProductionCatalogBuilder:
    def __init__(
        self,
        *,
        catalog_revision: int,
        source_policy_revision: int,
        authority_revision: int,
        canonical_stream: str = "md.canonical.v2",
        raw_topic: str = V2_REALTIME_RAW_TOPIC,
        quarantine_topic: str = "md.quarantine.stable.v1",
    ) -> None:
        if min(catalog_revision, source_policy_revision, authority_revision) < 1:
            raise ValueError("production catalog revisions must be positive")
        topics = (canonical_stream, raw_topic, quarantine_topic)
        if any(not item.strip() for item in topics) or len(set(topics)) != 3:
            raise ValueError("production catalog topics must be unique")
        self.catalog_revision = catalog_revision
        self.source_policy_revision = source_policy_revision
        self.authority_revision = authority_revision
        self.canonical_stream = canonical_stream
        self.raw_topic = raw_topic
        self.quarantine_topic = quarantine_topic

    def build(
        self,
        *,
        demand: ProductionDemandManifest,
        binance_usdm: BinanceDiscovery | None,
        okx_rows: Iterable[Mapping[str, str]],
        binance_spot: BinanceDiscovery | None = None,
        previous_catalog: StableSourceCatalog | None = None,
        metadata_provenance: Mapping[str, str] | None = None,
    ) -> ProductionCatalogBundle:
        metadata = self._metadata(
            binance_usdm, okx_rows, demand.demands, binance_spot=binance_spot
        )
        return self.build_from_records(
            demand=demand,
            records=metadata.values(),
            previous_catalog=previous_catalog,
            metadata_provenance=metadata_provenance,
        )

    def build_from_records(
        self,
        *,
        demand: ProductionDemandManifest,
        records: Iterable[InstrumentRecord],
        previous_catalog: StableSourceCatalog | None = None,
        metadata_provenance: Mapping[str, str] | None = None,
    ) -> ProductionCatalogBundle:
        """Build a catalog from already-admitted authentic metadata records.

        Phase 11's demand compiler has already fetched and parsed one bounded
        public metadata capture per venue/market. Re-parsing or fabricating a
        second provider view would make the resulting runtime plan impossible
        to audit. This entry point reuses those admitted records verbatim.
        """
        metadata: dict[tuple[str, str, str], InstrumentRecord] = {}
        for record in records:
            key = (
                record.identity.venue,
                record.identity.market,
                record.native_symbol,
            )
            existing = metadata.get(key)
            if existing is not None and existing != record:
                raise ValueError(f"duplicate authoritative instrument metadata: {key}")
            metadata[key] = record
        previous = self._previous_records(previous_catalog)
        selected: dict[str, InstrumentRecord] = {}
        bindings: list[dict[str, Any]] = []
        acquisitions: list[dict[str, Any]] = []
        for item in demand.demands:
            key = (item.venue, item.market, item.native_symbol)
            try:
                discovered = metadata[key]
            except KeyError as error:
                raise ValueError(f"authoritative metadata missing demanded instrument: {key}") from error
            if discovered.identity.product_type.value != item.product_type:
                raise ValueError("demanded product type differs from authoritative metadata")
            if discovered.status is not InstrumentStatus.ACTIVE:
                raise ValueError("demanded instrument is not active")
            record = self._revisioned(discovered, previous.get(discovered.instrument_id))
            selected[record.instrument_id] = record
            binding_id = self._binding_id(item)
            bindings.append(self._source_binding(binding_id, item, record))
            acquisitions.append(self._acquisition(binding_id, item))
        source = {
            "schema": _SOURCE_SCHEMA,
            "canonical_stream": self.canonical_stream,
            "catalog_revision": self.catalog_revision,
            "source_policy_revision": self.source_policy_revision,
            "authority_revision": self.authority_revision,
            "instruments": [
                self._instrument(record) for record in sorted(selected.values(), key=lambda value: value.instrument_id)
            ],
            "bindings": sorted(bindings, key=lambda value: value["binding_id"]),
        }
        acquisition = {
            "schema": _ACQUISITION_SCHEMA,
            "revision": demand.revision,
            "topics": {
                "raw": self.raw_topic,
                "canonical": self.canonical_stream,
                "quarantine": self.quarantine_topic,
            },
            "bindings": sorted(acquisitions, key=lambda value: value["binding_id"]),
        }
        encoded_source = yaml.safe_dump(source, sort_keys=True).encode()
        encoded_acquisition = yaml.safe_dump(acquisition, sort_keys=True).encode()
        provenance = {
            "schema": "qdl.v2.production-catalog-provenance.v1",
            "catalog_revision": self.catalog_revision,
            "demand_revision": demand.revision,
            "demand_sources": list(demand.source_paths),
            "instrument_count": len(selected),
            "binding_count": len(bindings),
            "instrument_ids": sorted(selected),
            "source_catalog_sha256": hashlib.sha256(encoded_source).hexdigest(),
            "acquisition_plan_sha256": hashlib.sha256(encoded_acquisition).hexdigest(),
            "metadata": dict(sorted((metadata_provenance or {}).items())),
            "fabricated_metadata": False,
        }
        return ProductionCatalogBundle(source, acquisition, provenance)

    @classmethod
    def merge_authoritative_instruments(
        cls,
        *,
        records: Iterable[InstrumentRecord],
        previous_catalog: StableSourceCatalog,
    ) -> list[dict[str, Any]]:
        """Merge a bounded authoritative metadata view without dropping history.

        A catalog can intentionally retain an unbound expired dated instrument
        for replay/cursor lineage while a new provider-discovered contract is
        added for active acquisition.  This helper therefore overlays only the
        supplied authoritative records, carries unchanged prior records
        forward, and increments ``metadata_revision`` only when the canonical
        instrument payload truly changes.
        """
        previous = cls._previous_records(previous_catalog)
        incoming: dict[str, InstrumentRecord] = {}
        for record in records:
            existing = incoming.get(record.instrument_id)
            if existing is not None and existing != record:
                raise ValueError(
                    "conflicting authoritative metadata for instrument: "
                    + record.instrument_id
                )
            incoming[record.instrument_id] = record

        merged = dict(previous)
        for instrument_id, record in incoming.items():
            merged[instrument_id] = cls._revisioned(
                record,
                previous.get(instrument_id),
            )
        return [
            cls._instrument(record)
            for record in sorted(merged.values(), key=lambda item: item.instrument_id)
        ]

    @staticmethod
    def _metadata(
        binance_usdm: BinanceDiscovery | None,
        okx_rows: Iterable[Mapping[str, str]],
        demands: Iterable[ProductionDemand],
        *,
        binance_spot: BinanceDiscovery | None = None,
    ) -> dict[tuple[str, str, str], InstrumentRecord]:
        values: list[InstrumentRecord] = []
        if binance_usdm is not None:
            values.extend(binance_usdm.records)
        if binance_spot is not None:
            values.extend(binance_spot.records)
        demanded_okx = {
            item.native_symbol
            for item in demands
            if item.venue == "OKX"
        }
        for raw in okx_rows:
            if str(raw.get("instId") or "").upper() not in demanded_okx:
                continue
            record, _ = parse_public_instrument(
                raw, metadata_revision=1, valid_from_ns=0
            )
            values.append(record)
        result: dict[tuple[str, str, str], InstrumentRecord] = {}
        for record in values:
            key = (
                record.identity.venue,
                record.identity.market,
                record.native_symbol,
            )
            if key in result:
                raise ValueError(f"duplicate authoritative instrument metadata: {key}")
            result[key] = record
        return result

    @staticmethod
    def _previous_records(
        catalog: StableSourceCatalog | None,
    ) -> dict[str, InstrumentRecord]:
        if catalog is None:
            return {}
        return {
            binding.instrument.instrument_id: binding.instrument
            for binding in catalog.bindings
        }

    @classmethod
    def _revisioned(
        cls, current: InstrumentRecord, previous: InstrumentRecord | None
    ) -> InstrumentRecord:
        if previous is None:
            return replace(current, metadata_revision=1)
        previous_payload = cls._instrument(previous) | {"metadata_revision": 0}
        current_payload = cls._instrument(current) | {"metadata_revision": 0}
        revision = (
            previous.metadata_revision
            if previous_payload == current_payload
            else previous.metadata_revision + 1
        )
        return replace(current, metadata_revision=revision)

    @staticmethod
    def _instrument(record: InstrumentRecord) -> dict[str, Any]:
        identity = record.identity
        result = {
            "instrument_uid": identity.instrument_uid,
            "instrument_id": identity.instrument_id,
            "metadata_revision": record.metadata_revision,
            "venue": identity.venue,
            "market": identity.market,
            "product_type": identity.product_type.value,
            "canonical_symbol": identity.canonical_symbol,
            "native_symbol": record.native_symbol,
            "asset_class": record.asset_class.value,
            "base_asset": record.base_asset,
            "quote_asset": record.quote_asset,
            "settlement_asset": record.settlement_asset,
            "price_tick": record.price_tick.source_text,
            "quantity_step": record.quantity_step.source_text,
            "contract_multiplier": record.contract_multiplier.source_text,
            "session_calendar_id": record.session_calendar_id,
            "attributes": dict(sorted(record.attributes.items())),
        }
        # A dated contract is not reconstructible from the pair/symbol alone.
        # Preserve the provider-authoritative expiry through the generated
        # catalog so a restart cannot silently treat it like a perpetual.
        if record.expiry_time_ns is not None:
            result["expiry_time_ns"] = record.expiry_time_ns
        return result

    @staticmethod
    def _binding_id(item: ProductionDemand) -> str:
        symbol = re.sub(r"[^a-z0-9]+", "-", item.native_symbol.lower()).strip("-")
        suffix = f"-{item.interval}" if item.interval else ""
        return f"{item.venue.lower()}-{item.market.lower()}-{symbol}-{item.feed.value.lower()}{suffix}"

    def _source_binding(
        self,
        binding_id: str,
        item: ProductionDemand,
        record: InstrumentRecord,
    ) -> dict[str, Any]:
        if item.feed is FeedType.TRADE:
            stale_after_ms = 15_000
        elif item.feed is FeedType.QUOTE:
            stale_after_ms = 5_000
        elif item.feed in _BOOK_FEEDS:
            # Book freshness is an explicit demand property.  Do not quietly
            # reduce it to a generic trade/BBO threshold.
            stale_after_ms = int(item.max_freshness_ms or 0)
        else:
            # Final BAR freshness must scale with its canonical interval. The
            # former fixed three-minute limit mislabeled a valid 1h/1d BAR as
            # stale and made broad active demand impossible to certify.
            stale_after_ms = canonical_interval_ms(item.interval or "") * 3
        adapter = (
            f"binance-{item.market.lower()}/2.0.0"
            if item.venue == "BINANCE"
            else "okx-v5/2.0.0"
        )
        compatibility = "NONE"
        if item.venue == "BINANCE" and item.feed is FeedType.TRADE:
            compatibility = "BINANCE_TRADE_MARKET_AND_GENERIC"
        elif item.venue == "BINANCE" and item.feed is FeedType.BAR:
            compatibility = "BINANCE_BAR_GENERIC"
        return {
            "binding_id": binding_id,
            "instrument_uid": record.instrument_uid,
            "feed": item.feed.value,
            "interval": item.interval,
            "source": {
                "provider": f"{item.venue}_DIRECT",
                # Snapshot and delta are aliases of exactly one provider book
                # and must remain in one durable partition.  Their binding
                # IDs are intentionally distinct, their source identity is not.
                "source_id": (
                    f"{self._book_source_stem(item)}-primary-v2"
                    if item.feed in _BOOK_FEEDS
                    else f"{binding_id}-primary-v2"
                ),
                "source_role": "PRIMARY",
                "source_policy_id": item.source_policy_id,
                "authoritative": True,
                "adapter_version": adapter,
                "normalizer_version": "qdl-rust-core/2.0.0",
            },
            "quality": {
                "stale_after_ms": stale_after_ms,
                "require_final_bar": item.feed is FeedType.BAR,
                "continuous_calendar": True,
            },
            "v1_compatibility": compatibility,
        }

    @staticmethod
    def _book_source_stem(item: ProductionDemand) -> str:
        symbol = re.sub(r"[^a-z0-9]+", "-", item.native_symbol.lower()).strip("-")
        return f"{item.venue.lower()}-{item.market.lower()}-{symbol}-book"

    @staticmethod
    def _acquisition(binding_id: str, item: ProductionDemand) -> dict[str, Any]:
        if item.venue == "BINANCE":
            # The provider kind and the stream endpoint both follow the market:
            # a Spot demand generated with USD-M kinds would subscribe the wrong
            # venue endpoint while looking correct in the catalog.
            family = item.market.lower()
            if item.feed is FeedType.TRADE:
                mode, kind, channel, sequence = (
                    "RUST_NATIVE", f"binance_{family}_trade",
                    f"{item.native_symbol.lower()}@trade", "MONOTONIC",
                )
            elif item.feed is FeedType.QUOTE:
                mode, kind, channel, sequence = (
                    "RUST_NATIVE", f"binance_{family}_bbo",
                    f"{item.native_symbol.lower()}@bookTicker", "MONOTONIC",
                )
            elif item.feed in _BOOK_FEEDS:
                mode, kind, channel, sequence = (
                    "RUST_NATIVE", f"binance_{family}_book",
                    f"{item.native_symbol.lower()}@depth@100ms", "CONTIGUOUS",
                )
            else:
                # The current provider/host certification proves direct Binance
                # trade and BBO, but not final kline delivery after a valid WS
                # ACK.  Keep provider REST at the outer edge for every generated
                # Binance BAR demand; it writes the same V2 raw envelope and the
                # Rust core remains the only canonical/replay/query authority.
                # A reviewed manifest revision can re-enable a native BAR lane
                # after fresh final-bar admission evidence.
                mode, kind, channel, sequence = (
                    "PYTHON_REST", f"binance_{family}_rest_bar",
                    f"rest-klines/{item.interval}", "NONE",
                )
            websocket = (
                _BINANCE_STREAM_URL[item.market] if mode == "RUST_NATIVE" else None
            )
            business = None
        else:
            if item.feed is FeedType.TRADE:
                mode, kind, channel, sequence = "RUST_NATIVE", "okx_trade", "trades", "MONOTONIC"
            elif item.feed is FeedType.QUOTE:
                mode, kind, channel, sequence = "RUST_NATIVE", "okx_bbo", "bbo-tbt", "NONE"
            elif item.feed in _BOOK_FEEDS:
                mode, kind, channel, sequence = "RUST_NATIVE", "okx_book", "books", "CONTIGUOUS"
            elif item.feed is FeedType.BAR and item.market == "SWAP":
                # A bounded real-provider gate proved the shared OKX business
                # candle lane emits ``confirm=1`` final rows with canonical
                # OHLCV parity and lower first-final latency than REST. The
                # Rust core remains the only canonical/replay authority; the
                # shared Python edge still owns startup, reconnect and gap
                # repair through provider history. This is one multiplexed
                # venue lane, never a symbol-specific worker.
                mode, kind, channel, sequence = (
                    "RUST_NATIVE", "okx_bar",
                    okx_candle_channel(item.interval or "1m"), "NONE",
                )
            else:
                # Only OKX Swap passed the native final-bar provider gate.
                # Other OKX markets retain the shared REST final lane until
                # they carry equivalent real-provider evidence.
                mode, kind, channel, sequence = (
                    "PYTHON_REST", "okx_bar",
                    okx_candle_channel(item.interval or "1m"), "NONE",
                )
            websocket = _OKX_PUBLIC_WS if mode == "RUST_NATIVE" else None
            business = "wss://ws.okx.com:8443/ws/v5/business" if mode == "RUST_NATIVE" else None
        result = {
            "binding_id": binding_id,
            "mode": mode,
            "runtime": item.venue,
            "provider_kind": kind,
            "native_channel": channel,
            "sequence_policy": sequence,
            "websocket_url": websocket,
            "business_websocket_url": business,
        }
        if item.feed in _BOOK_FEEDS:
            result["l2"] = {
                "provider_protocol": (
                    "BINANCE_DIFF_DEPTH" if item.venue == "BINANCE" else "OKX_PUBLIC_BOOKS"
                ),
                "depth_per_side": item.depth_per_side,
                "rest_snapshot_url": (
                    _BINANCE_DEPTH_REST[item.market] if item.venue == "BINANCE" else None
                ),
                # Binance renews its documented REST anchor. OKX public books
                # renews its own websocket snapshot on the isolated BOOK lane;
                # it is never polled as if it were a Binance diff-depth book.
                "snapshot_refresh_seconds": 30,
            }
        return result


def load_binance_exchange_info(path: str | Path) -> BinanceDiscovery:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Binance exchangeInfo capture must be an object")
    return parse_exchange_info(payload, valid_from_ns=0)


def load_okx_instruments(path: str | Path) -> list[Mapping[str, str]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if str(payload.get("code")) != "0" or not isinstance(payload.get("data"), list):
            raise ValueError("OKX instruments capture is not a successful V5 response")
        payload = payload["data"]
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError("OKX instruments capture must contain a data list")
    return payload

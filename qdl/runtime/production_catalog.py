from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import yaml

from qdl.adapters.binance_usdm import BinanceDiscovery, parse_exchange_info
from qdl.adapters.intervals import okx_candle_channel
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.domain.instrument import InstrumentRecord, InstrumentStatus, ProductType
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
    ("BINANCE", "SPOT", "SPOT"),
    ("OKX", "SWAP", "PERPETUAL"),
    ("OKX", "SPOT", "SPOT"),
}
_BINANCE_STREAM_URL = {
    # One venue/market worker uses Binance's documented control endpoint and
    # subscribes the resolved demand dynamically. Symbols never become
    # containers or URL-specific combined-stream shards.
    "USDM": "wss://fstream.binance.com/ws",
    "SPOT": "wss://stream.binance.com:9443/ws",
}
_SUPPORTED_FEEDS = {FeedType.TRADE, FeedType.QUOTE, FeedType.BAR}


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
        if not isinstance(raw, dict) or set(raw) != {
            "venue", "market", "product_type", "native_symbol",
            "feed", "interval", "source_policy_id",
        }:
            raise ValueError("production demand requirement fields are invalid")
        venue = str(raw["venue"]).strip().upper()
        market = str(raw["market"]).strip().upper()
        product = str(raw["product_type"]).strip().upper()
        native_symbol = str(raw["native_symbol"]).strip().upper()
        source_policy = str(raw["source_policy_id"]).strip()
        feed = FeedType(str(raw["feed"]).strip().upper())
        interval = str(raw["interval"]).strip() if raw["interval"] is not None else None
        if (venue, market, product) not in _SUPPORTED_MARKETS:
            raise ValueError("production demand market/product is not certified")
        if feed not in _SUPPORTED_FEEDS:
            raise ValueError("production demand feed is not certified")
        if feed is FeedType.BAR and interval != "1m":
            raise ValueError("production V2 BAR acquisition is currently certified for 1m")
        if feed is not FeedType.BAR and interval is not None:
            raise ValueError("interval is valid only for BAR demand")
        if not native_symbol or not source_policy:
            raise ValueError("production demand identity/source policy is incomplete")
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
        )


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
        return {
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
        else:
            stale_after_ms = 180_000
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
                "source_id": f"{binding_id}-primary-v2",
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
            else:
                # Derived from the demanded interval; a literal candle1m here
                # would silently produce a one-minute channel for every interval.
                mode, kind, channel, sequence = (
                    "RUST_NATIVE", "okx_bar",
                    okx_candle_channel(item.interval or "1m"), "NONE",
                )
            websocket = "wss://ws.okx.com:8443/ws/v5/public" if mode == "RUST_NATIVE" else None
            business = "wss://ws.okx.com:8443/ws/v5/business" if mode == "RUST_NATIVE" else None
        return {
            "binding_id": binding_id,
            "mode": mode,
            "runtime": item.venue,
            "provider_kind": kind,
            "native_channel": channel,
            "sequence_policy": sequence,
            "websocket_url": websocket,
            "business_websocket_url": business,
        }


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

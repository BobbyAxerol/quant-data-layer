from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from qdl.marketdata.v2 import market_data_pb2
from qdl.common.v1 import common_pb2
from qdl.query import (
    AccessPurpose,
    ConsumerGrade,
    DataProduct,
    DataRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
)
from qdl.transport.contracts import partition_key


@dataclass(frozen=True, slots=True)
class StableSourceBinding:
    binding_id: str
    instrument: InstrumentRecord
    provider: str
    source_id: str
    source_role: str
    source_policy_id: str
    authoritative: bool
    adapter_version: str
    normalizer_version: str
    feed: FeedType
    interval: str | None
    stale_after_ms: int
    require_final_bar: bool
    continuous_calendar: bool
    v1_compatibility: str
    canonical_stream: str

    def __post_init__(self) -> None:
        required = (
            self.binding_id,
            self.provider,
            self.source_id,
            self.source_role,
            self.source_policy_id,
            self.adapter_version,
            self.normalizer_version,
            self.canonical_stream,
        )
        if any(not value.strip() for value in required):
            raise ValueError("stable source binding identity is incomplete")
        if self.source_role not in {"PRIMARY", "SECONDARY", "REFERENCE", "BACKFILL"}:
            raise ValueError("stable source role is invalid")
        if self.feed is FeedType.BAR and not self.interval:
            raise ValueError("stable BAR binding requires interval")
        if self.feed is not FeedType.BAR and self.interval is not None:
            raise ValueError("stable non-BAR binding cannot have interval")
        if self.require_final_bar and self.feed is not FeedType.BAR:
            raise ValueError("require_final_bar is valid only for BAR")
        if self.stale_after_ms <= 0:
            raise ValueError("stable source freshness bound must be positive")
        policies = {
            "NONE",
            "BINANCE_TRADE_MARKET_AND_GENERIC",
            "BINANCE_TRADE_MARKET_ONLY",
            "BINANCE_BAR_GENERIC",
            "VN_TRADE_GENERIC",
        }
        if self.v1_compatibility not in policies:
            raise ValueError("stable V1 compatibility policy is invalid")
        if self.v1_compatibility != "NONE" and (
            ("TRADE" in self.v1_compatibility and self.feed is not FeedType.TRADE)
            or ("BAR" in self.v1_compatibility and self.feed is not FeedType.BAR)
        ):
            raise ValueError("stable V1 compatibility policy differs from feed")

    @property
    def partition_key(self) -> str:
        return partition_key(
            instrument_uid=self.instrument.instrument_uid,
            feed_type=self.feed.value,
            source_id=self.source_id,
        )

    @property
    def requirement_key(self) -> tuple[str, FeedType, str | None]:
        return self.instrument.instrument_uid, self.feed, self.interval


class StableSourceCatalog:
    def __init__(
        self,
        *,
        canonical_stream: str,
        bindings: tuple[StableSourceBinding, ...],
        catalog_revision: int,
        source_policy_revision: int,
        authority_revision: int,
        instruments: tuple[InstrumentRecord, ...] | None = None,
    ) -> None:
        if not canonical_stream.strip() or not bindings:
            raise ValueError("stable catalog requires canonical stream and bindings")
        if min(catalog_revision, source_policy_revision, authority_revision) < 1:
            raise ValueError("stable catalog revisions must be positive")
        requirement_keys = [item.requirement_key for item in bindings]
        binding_ids = [item.binding_id for item in bindings]
        envelope_keys = [
            (item.instrument.instrument_uid, item.feed.value.lower(), item.interval or "", item.source_id)
            for item in bindings
        ]
        if (
            len(requirement_keys) != len(set(requirement_keys))
            or len(binding_ids) != len(set(binding_ids))
            or len(envelope_keys) != len(set(envelope_keys))
        ):
            raise ValueError("stable source bindings must be unique")
        if any(item.canonical_stream != canonical_stream for item in bindings):
            raise ValueError("stable bindings must share the catalog canonical stream")
        records: dict[str, InstrumentRecord] = {}
        for binding in bindings:
            current = records.get(binding.instrument.instrument_uid)
            if current is not None and current != binding.instrument:
                raise ValueError("stable bindings disagree on instrument metadata")
            records[binding.instrument.instrument_uid] = binding.instrument
        # A declared instrument does not need a materialised binding. Bound
        # feeds are acquired and stored; a pass-through history request only
        # needs the instrument's identity and metadata. Keeping the declared
        # set lets the query edge resolve the second case without inventing a
        # binding for it.
        declared = tuple(instruments) if instruments is not None else tuple(
            records[uid] for uid in sorted(records)
        )
        by_uid: dict[str, InstrumentRecord] = {}
        for item in declared:
            current = by_uid.get(item.identity.instrument_uid)
            if current is not None and current != item:
                raise ValueError("stable catalog declares conflicting instrument metadata")
            by_uid[item.identity.instrument_uid] = item
        missing = set(records) - set(by_uid)
        if missing:
            raise ValueError(
                "stable bindings reference undeclared instruments: "
                + ",".join(sorted(missing))
            )
        for uid, bound in records.items():
            if by_uid[uid] != bound:
                raise ValueError(
                    f"stable binding instrument disagrees with the declared record: {uid}"
                )
        self.instruments = declared
        self._by_uid = by_uid
        self.canonical_stream = canonical_stream
        self.bindings = bindings
        self.catalog_revision = catalog_revision
        self.source_policy_revision = source_policy_revision
        self.authority_revision = authority_revision
        self._by_requirement = {item.requirement_key: item for item in bindings}
        self._by_envelope = {key: item for key, item in zip(envelope_keys, bindings, strict=True)}

    def instrument_for(self, instrument_uid: str) -> InstrumentRecord:
        """Return a declared instrument whether or not it has a binding."""
        try:
            return self._by_uid[str(instrument_uid)]
        except KeyError as error:
            raise KeyError(
                f"instrument is not declared in the stable catalog: {instrument_uid}"
            ) from error

    @classmethod
    def load(cls, path: str | Path) -> "StableSourceCatalog":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("schema") != "qdl.v2.stable-source-bindings.v1":
            raise ValueError("unsupported stable source binding schema")
        expected = {
            "schema", "canonical_stream", "catalog_revision",
            "source_policy_revision", "authority_revision", "instruments", "bindings",
        }
        if set(raw) != expected:
            raise ValueError("stable source catalog fields are incomplete or unknown")
        instruments_raw = raw.get("instruments")
        if not isinstance(instruments_raw, list) or not 1 <= len(instruments_raw) <= 10_000:
            raise ValueError("stable source catalog requires 1..10000 instruments")
        instruments = tuple(cls._instrument(value) for value in instruments_raw)
        by_uid = {item.instrument_uid: item for item in instruments}
        if len(by_uid) != len(instruments):
            raise ValueError("stable source instrument UIDs must be unique")
        values = raw.get("bindings")
        if not isinstance(values, list) or not 1 <= len(values) <= 100_000:
            raise ValueError("stable source catalog requires 1..100000 bindings")
        canonical_stream = str(raw["canonical_stream"])
        return cls(
            canonical_stream=canonical_stream,
            bindings=tuple(
                cls._binding(value, canonical_stream, by_uid) for value in values
            ),
            catalog_revision=int(raw["catalog_revision"]),
            source_policy_revision=int(raw["source_policy_revision"]),
            authority_revision=int(raw["authority_revision"]),
            instruments=instruments,
        )

    @staticmethod
    def _instrument(raw: Any) -> InstrumentRecord:
        if not isinstance(raw, dict):
            raise ValueError("stable instrument must be a mapping")
        expected = {
            "instrument_uid", "instrument_id", "metadata_revision", "venue", "market",
            "product_type", "canonical_symbol", "native_symbol", "asset_class",
            "base_asset", "quote_asset", "settlement_asset", "price_tick",
            "quantity_step", "contract_multiplier", "session_calendar_id", "attributes",
        }
        if set(raw) != expected:
            raise ValueError("stable instrument fields are incomplete or unknown")
        identity = InstrumentIdentity.create(
            venue=str(raw["venue"]),
            market=str(raw["market"]),
            product_type=ProductType(str(raw["product_type"]).upper()),
            canonical_symbol=str(raw["canonical_symbol"]),
        )
        if (
            identity.instrument_uid != str(raw["instrument_uid"])
            or identity.instrument_id != str(raw["instrument_id"]).upper()
        ):
            raise ValueError("stable instrument UID/ID is not deterministic")
        attributes = raw["attributes"]
        if not isinstance(attributes, dict):
            raise ValueError("stable instrument attributes must be a mapping")
        return InstrumentRecord(
            identity=identity,
            metadata_revision=int(raw["metadata_revision"]),
            asset_class=AssetClass(str(raw["asset_class"]).upper()),
            native_symbol=str(raw["native_symbol"]).upper(),
            base_asset=str(raw["base_asset"]).upper(),
            quote_asset=str(raw["quote_asset"]).upper(),
            settlement_asset=str(raw["settlement_asset"]).upper(),
            price_tick=CanonicalDecimal.from_text(str(raw["price_tick"])),
            quantity_step=CanonicalDecimal.from_text(str(raw["quantity_step"])),
            contract_multiplier=CanonicalDecimal.from_text(str(raw["contract_multiplier"])),
            session_calendar_id=str(raw["session_calendar_id"]),
            attributes={str(key): str(value) for key, value in attributes.items()},
        )

    @staticmethod
    def _binding(
        raw: Any,
        canonical_stream: str,
        instruments: dict[str, InstrumentRecord],
    ) -> StableSourceBinding:
        if not isinstance(raw, dict) or set(raw) != {
            "binding_id", "instrument_uid", "feed", "interval", "source", "quality",
            "v1_compatibility",
        }:
            raise ValueError("stable source binding fields are incomplete or unknown")
        source = raw["source"]
        quality = raw["quality"]
        if not all(isinstance(value, dict) for value in (source, quality)):
            raise ValueError("stable binding sections must be mappings")
        try:
            instrument = instruments[str(raw["instrument_uid"])]
        except KeyError as error:
            raise ValueError("stable binding references an unknown instrument") from error
        expected_source = {
            "provider", "source_id", "source_role", "source_policy_id",
            "authoritative", "adapter_version", "normalizer_version",
        }
        if set(source) != expected_source:
            raise ValueError("stable source lineage fields are incomplete or unknown")
        if set(quality) != {"stale_after_ms", "require_final_bar", "continuous_calendar"}:
            raise ValueError("stable source quality fields are incomplete or unknown")
        interval = raw["interval"]
        return StableSourceBinding(
            binding_id=str(raw["binding_id"]),
            instrument=instrument,
            provider=str(source["provider"]),
            source_id=str(source["source_id"]),
            source_role=str(source["source_role"]).upper(),
            source_policy_id=str(source["source_policy_id"]),
            authoritative=bool(source["authoritative"]),
            adapter_version=str(source["adapter_version"]),
            normalizer_version=str(source["normalizer_version"]),
            feed=FeedType(str(raw["feed"]).upper()),
            interval=str(interval) if interval is not None else None,
            stale_after_ms=int(quality["stale_after_ms"]),
            require_final_bar=bool(quality["require_final_bar"]),
            continuous_calendar=bool(quality["continuous_calendar"]),
            v1_compatibility=str(raw["v1_compatibility"]).upper(),
            canonical_stream=canonical_stream,
        )

    def binding_for(self, requirement: DataRequirement) -> StableSourceBinding:
        try:
            binding = self._by_requirement[
                requirement.instrument_uid, requirement.feed, requirement.interval
            ]
        except KeyError as error:
            raise KeyError("requirement has no stable source binding") from error
        if binding.source_policy_id != requirement.source_policy_id:
            raise KeyError("requirement source policy does not match stable binding")
        return binding

    def binding_for_envelope(
        self, envelope: market_data_pb2.EventEnvelope
    ) -> StableSourceBinding:
        payload_name = envelope.WhichOneof("payload")
        interval = envelope.bar.interval if payload_name == "bar" else ""
        key = (envelope.instrument_uid, payload_name or "", interval, envelope.source_id)
        try:
            binding = self._by_envelope[key]
        except KeyError as error:
            raise ValueError("canonical event is outside the stable catalog") from error
        expected_role = getattr(common_pb2, f"SOURCE_ROLE_{binding.source_role}")
        if (
            envelope.instrument_id != binding.instrument.instrument_id
            or envelope.instrument_revision != binding.instrument.metadata_revision
            or envelope.venue != binding.instrument.identity.venue
            or envelope.market != binding.instrument.identity.market
            or envelope.product_type != binding.instrument.identity.product_type.value
            or envelope.native_symbol != binding.instrument.native_symbol
            or envelope.provider != binding.provider
            or envelope.source_role != expected_role
            or envelope.authority_revision != self.authority_revision
            or envelope.adapter_version != binding.adapter_version
            or envelope.normalizer_version != binding.normalizer_version
        ):
            raise ValueError("canonical event identity/lineage differs from stable binding")
        if (
            envelope.schema_major != 2
            or len(envelope.event_id) != 16
            or len(envelope.raw_capture_id) != 16
            or len(envelope.raw_payload_hash) != 32
            or not envelope.source_session_id
            or envelope.connection_generation < 1
            or envelope.lease_epoch < 1
            or envelope.partition_sequence < 1
            or not (
                0 < envelope.received_at_ns
                <= envelope.normalized_at_ns
                <= envelope.published_at_ns
            )
        ):
            raise ValueError("stable canonical event provenance is incomplete")
        self._validate_payload(envelope, binding)
        return binding

    @staticmethod
    def _validate_payload(
        envelope: market_data_pb2.EventEnvelope,
        binding: StableSourceBinding,
    ) -> None:
        payload_name = envelope.WhichOneof("payload")
        if payload_name != binding.feed.value.lower():
            raise ValueError("canonical payload feed differs from stable binding")
        if payload_name == "trade":
            if (
                envelope.trade.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED
                or envelope.trade.identity_kind
                == market_data_pb2.TRADE_IDENTITY_KIND_UNSPECIFIED
            ):
                raise ValueError("stable trade quantity/identity semantics are incomplete")
        elif payload_name == "quote":
            if envelope.quote.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable quote quantity unit is incomplete")
        elif payload_name == "bar":
            if envelope.bar.volume_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable BAR volume unit is incomplete")
            if binding.require_final_bar and (
                not envelope.bar.is_final
                or envelope.bar.lifecycle
                not in {
                    market_data_pb2.BAR_LIFECYCLE_FINAL,
                    market_data_pb2.BAR_LIFECYCLE_REVISED,
                }
            ):
                raise ValueError("stable binding requires a final/revised BAR")
        else:
            raise ValueError("stable baseline supports only TRADE/QUOTE/BAR")

    def instrument_registry(self) -> InstrumentRegistry:
        registry = InstrumentRegistry()
        registered: set[str] = set()
        for binding in self.bindings:
            record = binding.instrument
            if record.instrument_uid in registered:
                continue
            registry.register(record, [InstrumentAlias(
                provider=binding.provider,
                market=record.identity.market,
                native_symbol=record.native_symbol,
                instrument_uid=record.instrument_uid,
                instrument_revision=record.metadata_revision,
                valid_from_ns=0,
            )])
            registered.add(record.instrument_uid)
        return registry

    def entitlements(self) -> EntitlementPolicy:
        grants = []
        for binding in self.bindings:
            purposes = {
                AccessPurpose.INTERNAL_ALPHA,
                AccessPurpose.INTERNAL_RESEARCH,
            }
            if binding.authoritative:
                purposes.add(AccessPurpose.INTERNAL_EXECUTION)
            grants.append(EntitlementGrant(
                source_id=binding.source_id,
                license_revision="internal-stable-v2",
                purposes=frozenset(purposes),
                products=frozenset({
                    DataProduct.CANONICAL_SNAPSHOT,
                    DataProduct.CANONICAL_HISTORY,
                }),
                valid_from_ns=0,
            ))
        return EntitlementPolicy(tuple(grants))

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
    METRIC_INTERVAL_FEEDS,
    OPTIONAL_INTERVAL_FEEDS,
)
from qdl.transport.contracts import partition_key


def canonical_payload_interval(
    envelope: market_data_pb2.EventEnvelope,
) -> str | None:
    """Return the contract interval carried by a canonical V2 payload.

    BAR and sampled reference products carry different protobuf field names,
    but the stable binding identity intentionally has one interval coordinate.
    Point-in-time products return ``None``.  This helper is shared by catalog
    admission and the spool query edge so an event cannot be admitted under
    one key then queried under another.
    """

    payload_name = envelope.WhichOneof("payload")
    if payload_name == "bar":
        return envelope.bar.interval or None
    if payload_name == "long_short_ratio":
        return envelope.long_short_ratio.sampling_interval or None
    if payload_name == "taker_flow":
        return envelope.taker_flow.sampling_interval or None
    if payload_name == "basis":
        return envelope.basis.sampling_interval or None
    if payload_name == "open_interest":
        return envelope.open_interest.sampling_interval or None
    return None


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
        if self.feed in METRIC_INTERVAL_FEEDS and not self.interval:
            raise ValueError("stable metric-series binding requires interval")
        if (
            self.feed in OPTIONAL_INTERVAL_FEEDS
            and self.interval is not None
            and not self.interval.strip()
        ):
            raise ValueError("stable open-interest interval cannot be blank")
        if (
            self.feed is not FeedType.BAR
            and self.feed not in METRIC_INTERVAL_FEEDS
            and self.feed not in OPTIONAL_INTERVAL_FEEDS
            and self.interval is not None
        ):
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
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, raw: Any) -> "StableSourceCatalog":
        """Validate an already-decoded catalog without requiring file I/O.

        Runtime loaders still use :meth:`load`; this entry point is for an
        admitted, in-memory catalog projection such as a bounded provider
        acceptance run.  Both paths deliberately share the exact strict
        schema and identity checks.
        """

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
        required = {
            "instrument_uid", "instrument_id", "metadata_revision", "venue", "market",
            "product_type", "canonical_symbol", "native_symbol", "asset_class",
            "base_asset", "quote_asset", "settlement_asset", "price_tick",
            "quantity_step", "contract_multiplier", "session_calendar_id", "attributes",
        }
        if not required <= set(raw) or set(raw) - required - {"expiry_time_ns"}:
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
            expiry_time_ns=(
                int(raw["expiry_time_ns"])
                if raw.get("expiry_time_ns") is not None
                else None
            ),
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
        interval = canonical_payload_interval(envelope) or ""
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
            StableSourceCatalog._decimal(envelope.trade.price, "trade price", positive=True)
            StableSourceCatalog._decimal(
                envelope.trade.quantity, "trade quantity", positive=True
            )
        elif payload_name == "quote":
            if envelope.quote.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable quote quantity unit is incomplete")
            for field in (
                (envelope.quote.bid_price, "quote bid price"),
                (envelope.quote.ask_price, "quote ask price"),
                (envelope.quote.bid_quantity, "quote bid quantity"),
                (envelope.quote.ask_quantity, "quote ask quantity"),
            ):
                StableSourceCatalog._decimal(*field, positive=True)
        elif payload_name == "bar":
            if envelope.bar.volume_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable BAR volume unit is incomplete")
            for value, name in (
                (envelope.bar.open, "bar open"),
                (envelope.bar.high, "bar high"),
                (envelope.bar.low, "bar low"),
                (envelope.bar.close, "bar close"),
                (envelope.bar.volume, "bar volume"),
            ):
                StableSourceCatalog._decimal(value, name, positive=name != "bar volume")
            if binding.require_final_bar and (
                not envelope.bar.is_final
                or envelope.bar.lifecycle
                not in {
                    market_data_pb2.BAR_LIFECYCLE_FINAL,
                    market_data_pb2.BAR_LIFECYCLE_REVISED,
                }
            ):
                raise ValueError("stable binding requires a final/revised BAR")
        elif payload_name == "book_snapshot":
            if not envelope.book_snapshot.native_sequence or envelope.book_snapshot.depth < 1:
                raise ValueError("stable book snapshot sequence/depth is incomplete")
            if not envelope.book_snapshot.levels:
                raise ValueError("stable book snapshot has no readable levels")
            for level in envelope.book_snapshot.levels:
                StableSourceCatalog._book_level(level, positive_quantity=True)
            if (
                envelope.book_snapshot.sequence_verified
                and envelope.book_snapshot.book_generation < 1
            ):
                raise ValueError("verified book snapshot requires a positive generation")
        elif payload_name == "book_delta":
            if not all(
                (
                    envelope.book_delta.native_sequence_start,
                    envelope.book_delta.native_sequence_end,
                    envelope.book_delta.snapshot_sequence,
                )
            ):
                raise ValueError("stable book delta lacks continuity/snapshot sequence")
            for level in envelope.book_delta.updates:
                StableSourceCatalog._book_level(level, positive_quantity=False)
            if (
                envelope.book_delta.sequence_verified
                and envelope.book_delta.book_generation < 1
            ):
                raise ValueError("verified book delta requires a positive generation")
        elif payload_name == "funding_rate":
            StableSourceCatalog._decimal(envelope.funding_rate.rate, "funding rate")
            if envelope.funding_rate.funding_time_ns <= 0:
                raise ValueError("stable funding time is incomplete")
            if (
                envelope.funding_rate.HasField("next_funding_time_ns")
                and envelope.funding_rate.next_funding_time_ns <= 0
            ):
                raise ValueError("stable next funding time is invalid")
        elif payload_name == "open_interest":
            if envelope.open_interest.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable open-interest quantity unit is incomplete")
            StableSourceCatalog._decimal(
                envelope.open_interest.quantity,
                "open-interest quantity",
                nonnegative=True,
            )
            if envelope.open_interest.HasField("notional"):
                StableSourceCatalog._decimal(
                    envelope.open_interest.notional,
                    "open-interest notional",
                    nonnegative=True,
                )
            StableSourceCatalog._validate_interval(envelope, binding)
        elif payload_name == "mark_index_price":
            StableSourceCatalog._decimal(
                envelope.mark_index_price.mark_price, "mark price", positive=True
            )
            StableSourceCatalog._decimal(
                envelope.mark_index_price.index_price, "index price", positive=True
            )
        elif payload_name == "long_short_ratio":
            if (
                envelope.long_short_ratio.population
                == market_data_pb2.LONG_SHORT_RATIO_POPULATION_UNSPECIFIED
                or envelope.long_short_ratio.value_unit != market_data_pb2.METRIC_UNIT_RATIO
            ):
                raise ValueError("stable long-short ratio semantics are incomplete")
            for value, name in (
                (envelope.long_short_ratio.long_value, "long value"),
                (envelope.long_short_ratio.short_value, "short value"),
                (envelope.long_short_ratio.long_short_ratio, "long-short ratio"),
            ):
                StableSourceCatalog._decimal(value, name, nonnegative=True)
            StableSourceCatalog._validate_interval(envelope, binding)
        elif payload_name == "taker_flow":
            if envelope.taker_flow.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                raise ValueError("stable taker-flow quantity unit is incomplete")
            for value, name in (
                (envelope.taker_flow.buy_volume, "taker buy volume"),
                (envelope.taker_flow.sell_volume, "taker sell volume"),
                (envelope.taker_flow.buy_sell_ratio, "taker buy-sell ratio"),
            ):
                StableSourceCatalog._decimal(value, name, nonnegative=True)
            StableSourceCatalog._validate_interval(envelope, binding)
        elif payload_name == "basis":
            if (
                envelope.basis.kind == market_data_pb2.BASIS_KIND_UNSPECIFIED
                or envelope.basis.basis_unit == market_data_pb2.METRIC_UNIT_UNSPECIFIED
            ):
                raise ValueError("stable basis kind/unit is incomplete")
            StableSourceCatalog._decimal(envelope.basis.basis, "basis")
            if envelope.basis.HasField("annualized_basis"):
                StableSourceCatalog._decimal(
                    envelope.basis.annualized_basis, "annualized basis"
                )
            if envelope.basis.kind == market_data_pb2.BASIS_KIND_DERIVED and (
                not envelope.basis.formula_id
                or len(envelope.basis.input_instrument_uids) < 2
            ):
                raise ValueError("derived basis lacks formula/input lineage")
            StableSourceCatalog._validate_interval(envelope, binding)
        elif payload_name == "contract_metadata":
            metadata = envelope.contract_metadata
            if not metadata.contract_kind or not metadata.settlement_asset:
                raise ValueError("stable contract metadata identity is incomplete")
            for value, name in (
                (metadata.contract_multiplier, "contract multiplier"),
                (metadata.price_tick, "price tick"),
                (metadata.quantity_step, "quantity step"),
            ):
                StableSourceCatalog._decimal(value, name, positive=True)
            for field in ("expiry_time_ns", "funding_interval_ns"):
                if metadata.HasField(field) and getattr(metadata, field) <= 0:
                    raise ValueError(f"stable contract metadata {field} is invalid")
        elif payload_name == "ticker":
            StableSourceCatalog._decimal(envelope.ticker.last_price, "ticker last price", positive=True)
            for value_name, unit_name in (
                ("last_quantity", "last_quantity_unit"),
                ("volume_24h", "volume_24h_unit"),
            ):
                if envelope.ticker.HasField(value_name):
                    if getattr(envelope.ticker, unit_name) == common_pb2.QUANTITY_UNIT_UNSPECIFIED:
                        raise ValueError(f"stable ticker {unit_name} is incomplete")
                    StableSourceCatalog._decimal(
                        getattr(envelope.ticker, value_name), value_name, positive=True
                    )
        else:
            raise ValueError("stable canonical payload is unsupported")

    @staticmethod
    def _validate_interval(
        envelope: market_data_pb2.EventEnvelope,
        binding: StableSourceBinding,
    ) -> None:
        expected = binding.interval or ""
        observed = canonical_payload_interval(envelope) or ""
        if expected != observed:
            raise ValueError("stable canonical payload interval differs from binding")

    @staticmethod
    def _decimal(
        value,
        name: str,
        *,
        positive: bool = False,
        nonnegative: bool = False,
    ) -> CanonicalDecimal:
        if positive and nonnegative:
            raise ValueError("decimal validation cannot require both positive and nonnegative")
        source_text = str(value.source_text).strip()
        if not source_text:
            raise ValueError(f"stable {name} decimal source text is missing")
        parsed = CanonicalDecimal.from_text(source_text)
        coefficient = (
            value.mantissa_text
            if value.WhichOneof("coefficient") == "mantissa_text"
            else value.mantissa
            if value.WhichOneof("coefficient") == "mantissa"
            else None
        )
        if coefficient is None or str(parsed.coefficient) != str(coefficient) or parsed.scale != value.scale:
            raise ValueError(f"stable {name} decimal representation is inconsistent")
        if positive and parsed.as_decimal() <= 0:
            raise ValueError(f"stable {name} must be positive")
        if nonnegative and parsed.as_decimal() < 0:
            raise ValueError(f"stable {name} cannot be negative")
        return parsed

    @staticmethod
    def _book_level(level, *, positive_quantity: bool) -> None:
        if (
            level.side == common_pb2.BOOK_SIDE_UNSPECIFIED
            or level.quantity_unit == common_pb2.QUANTITY_UNIT_UNSPECIFIED
        ):
            raise ValueError("stable book level side/unit is incomplete")
        StableSourceCatalog._decimal(level.price, "book level price", positive=True)
        quantity = StableSourceCatalog._decimal(level.quantity, "book level quantity")
        if quantity.as_decimal() < 0 or (positive_quantity and quantity.as_decimal() <= 0):
            raise ValueError("stable book level quantity is invalid")

    def instrument_registry(
        self, *, include_unbound: bool = False
    ) -> InstrumentRegistry:
        """Registry of resolvable instruments.

        Unbound instruments are excluded by default: declaring metadata must not
        by itself make an instrument queryable. A deployment opts in when it
        enables the pass-through product.
        """
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
        if include_unbound:
            for record in self.instruments:
                if record.identity.instrument_uid in registered:
                    continue
                # No binding means no provider adapter, so the venue itself is
                # the only honest alias namespace for the pass-through.
                registry.register(record, [InstrumentAlias(
                    provider=record.identity.venue,
                    market=record.identity.market,
                    native_symbol=record.native_symbol,
                    instrument_uid=record.identity.instrument_uid,
                    instrument_revision=record.metadata_revision,
                    valid_from_ns=0,
                )])
                registered.add(record.identity.instrument_uid)
        return registry

    def entitlements(
        self, *, include_unbound: bool = False
    ) -> EntitlementPolicy:
        from qdl.runtime.provider_history import (
            PASS_THROUGH_LICENSE_REVISION,
            pass_through_source_id,
        )

        """Source licensing grants.

        A pass-through grant is strictly narrower than a bound one: it never
        carries `INTERNAL_EXECUTION`, because that product never passed the
        canonical core and is covered by no authority record. It is also opt-in,
        so adding catalog metadata cannot silently open a new data product.
        """
        grant_purposes: dict[str, set[AccessPurpose]] = {}
        for binding in self.bindings:
            purposes = {
                AccessPurpose.INTERNAL_ALPHA,
                AccessPurpose.INTERNAL_RESEARCH,
            }
            if binding.authoritative:
                purposes.add(AccessPurpose.INTERNAL_EXECUTION)
            # A provider session normally emits more than one feed and can
            # serve several instruments. Entitlements are source-scoped, not
            # binding-scoped, so merge the capabilities before constructing
            # the policy's unique ``(source_id, license_revision)`` grants.
            grant_purposes.setdefault(binding.source_id, set()).update(purposes)
        grants = [
            EntitlementGrant(
                source_id=source_id,
                license_revision="internal-stable-v2",
                purposes=frozenset(purposes),
                products=frozenset({
                    DataProduct.CANONICAL_SNAPSHOT,
                    DataProduct.CANONICAL_HISTORY,
                }),
                valid_from_ns=0,
            )
            for source_id, purposes in sorted(grant_purposes.items())
        ]
        if include_unbound:
            # Every declared instrument gets the pass-through grant, including
            # one that already has a binding. Routing decides per *requirement*
            # — uid, feed and interval together — so an instrument bound at 1m
            # still routes to the pass-through when asked for 15m. Granting
            # only unbound instruments would refuse exactly the case the product
            # exists for, and refuse it as unlicensed rather than as unbound,
            # which is the wrong reason as well as the wrong answer.
            #
            # This cannot downgrade anyone: `RoutedQueryBackend` returns the
            # spool whenever a binding covers the requirement, so the extra
            # grant is only ever reachable for a requirement no binding serves.
            for record in self.instruments:
                uid = record.identity.instrument_uid
                grants.append(EntitlementGrant(
                    source_id=pass_through_source_id(uid),
                    license_revision=PASS_THROUGH_LICENSE_REVISION,
                    purposes=frozenset({
                        AccessPurpose.INTERNAL_ALPHA,
                        AccessPurpose.INTERNAL_RESEARCH,
                    }),
                    products=frozenset({
                        DataProduct.CANONICAL_SNAPSHOT,
                        DataProduct.CANONICAL_HISTORY,
                    }),
                    valid_from_ns=0,
                ))
        return EntitlementPolicy(tuple(grants))

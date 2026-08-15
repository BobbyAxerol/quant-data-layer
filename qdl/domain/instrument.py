from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path

from qdl.domain.decimal import CanonicalDecimal


INSTRUMENT_NAMESPACE = uuid.UUID("9cb235b1-2ceb-5a3c-89ae-c4036ed36b90")


class AssetClass(str, Enum):
    CRYPTO = "CRYPTO"
    EQUITY = "EQUITY"
    DERIVATIVE = "DERIVATIVE"
    OPTION = "OPTION"
    INDEX = "INDEX"


class ProductType(str, Enum):
    SPOT = "SPOT"
    PERPETUAL = "PERPETUAL"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    COMMON_STOCK = "COMMON_STOCK"
    INDEX = "INDEX"
    EVENT_CONTRACT = "EVENT_CONTRACT"


class OptionType(str, Enum):
    CALL = "CALL"
    PUT = "PUT"


class InstrumentStatus(str, Enum):
    PRELISTED = "PRELISTED"
    ACTIVE = "ACTIVE"
    HALTED = "HALTED"
    EXPIRED = "EXPIRED"
    DELISTED = "DELISTED"


def _identity_component(value: str, name: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized or "." in normalized:
        raise ValueError(f"{name} must be non-empty and cannot contain '.'")
    return normalized


@dataclass(frozen=True)
class InstrumentIdentity:
    instrument_uid: str
    instrument_id: str
    venue: str
    market: str
    product_type: ProductType
    canonical_symbol: str

    @classmethod
    def create(
        cls,
        *,
        venue: str,
        market: str,
        product_type: ProductType | str,
        canonical_symbol: str,
    ) -> "InstrumentIdentity":
        product = product_type if isinstance(product_type, ProductType) else ProductType(str(product_type).upper())
        venue_value = _identity_component(venue, "venue")
        market_value = _identity_component(market, "market")
        symbol_value = _identity_component(canonical_symbol, "canonical_symbol")
        instrument_id = f"{venue_value}.{market_value}.{product.value}.{symbol_value}"
        return cls(
            instrument_uid=str(uuid.uuid5(INSTRUMENT_NAMESPACE, instrument_id)),
            instrument_id=instrument_id,
            venue=venue_value,
            market=market_value,
            product_type=product,
            canonical_symbol=symbol_value,
        )


@dataclass(frozen=True)
class InstrumentRecord:
    identity: InstrumentIdentity
    metadata_revision: int
    asset_class: AssetClass
    native_symbol: str
    base_asset: str
    quote_asset: str
    settlement_asset: str
    price_tick: CanonicalDecimal
    quantity_step: CanonicalDecimal
    contract_multiplier: CanonicalDecimal
    session_calendar_id: str
    status: InstrumentStatus = InstrumentStatus.ACTIVE
    expiry_time_ns: int | None = None
    strike_price: CanonicalDecimal | None = None
    option_type: OptionType | None = None
    underlying_instrument_uid: str | None = None
    valid_from_ns: int = 0
    valid_to_ns: int | None = None
    attributes: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.metadata_revision < 1:
            raise ValueError("metadata_revision must be positive")
        if self.valid_to_ns is not None and self.valid_to_ns <= self.valid_from_ns:
            raise ValueError("valid_to_ns must be after valid_from_ns")
        if self.identity.product_type is ProductType.OPTION:
            if self.expiry_time_ns is None or self.strike_price is None or self.option_type is None:
                raise ValueError("options require expiry, strike and option_type")
        if self.identity.product_type is ProductType.FUTURE and self.expiry_time_ns is None:
            raise ValueError("dated futures require expiry_time_ns")

    @property
    def instrument_uid(self) -> str:
        return self.identity.instrument_uid

    @property
    def instrument_id(self) -> str:
        return self.identity.instrument_id


@dataclass(frozen=True)
class InstrumentAlias:
    provider: str
    market: str
    native_symbol: str
    instrument_uid: str
    instrument_revision: int
    valid_from_ns: int
    valid_to_ns: int | None = None

    def normalized_key(self) -> tuple[str, str, str]:
        return (
            self.provider.strip().upper(),
            self.market.strip().upper(),
            self.native_symbol.strip().upper(),
        )

    def contains(self, event_time_ns: int) -> bool:
        return self.valid_from_ns <= event_time_ns and (
            self.valid_to_ns is None or event_time_ns < self.valid_to_ns
        )


class InstrumentRegistry:
    """Pure registry/resolver that can be snapshotted when control DB is down."""

    def __init__(self) -> None:
        self._records: dict[str, InstrumentRecord] = {}
        self._ids: dict[str, str] = {}
        self._aliases: dict[tuple[str, str, str], list[InstrumentAlias]] = {}

    def register(self, record: InstrumentRecord, aliases: list[InstrumentAlias]) -> None:
        existing_uid = self._ids.get(record.instrument_id)
        if existing_uid is not None and existing_uid != record.instrument_uid:
            raise ValueError(f"instrument_id collision: {record.instrument_id}")
        existing = self._records.get(record.instrument_uid)
        if existing is not None and existing.instrument_id != record.instrument_id:
            raise ValueError(f"instrument_uid collision: {record.instrument_uid}")
        pending: list[tuple[tuple[str, str, str], InstrumentAlias]] = []
        for alias in aliases:
            if alias.instrument_uid != record.instrument_uid:
                raise ValueError("alias instrument_uid does not match record")
            if alias.instrument_revision != record.metadata_revision:
                raise ValueError("alias instrument_revision does not match record")
            key = alias.normalized_key()
            periods = [*self._aliases.get(key, []), *(item for item_key, item in pending if item_key == key)]
            duplicate = False
            for current in periods:
                if current == alias:
                    duplicate = True
                    continue
                left_end = current.valid_to_ns if current.valid_to_ns is not None else 2**63 - 1
                right_end = alias.valid_to_ns if alias.valid_to_ns is not None else 2**63 - 1
                if max(current.valid_from_ns, alias.valid_from_ns) < min(left_end, right_end):
                    raise ValueError(f"overlapping alias ownership: {key}")
            if not duplicate:
                pending.append((key, alias))

        # Commit only after every identity and temporal constraint is valid.
        self._records[record.instrument_uid] = record
        self._ids[record.instrument_id] = record.instrument_uid
        for key, alias in pending:
            periods = self._aliases.setdefault(key, [])
            periods.append(alias)
            periods.sort(key=lambda item: item.valid_from_ns)

    def get(self, instrument_uid: str) -> InstrumentRecord:
        try:
            return self._records[instrument_uid]
        except KeyError as exc:
            raise KeyError(f"unknown instrument_uid: {instrument_uid}") from exc

    def get_by_id(self, instrument_id: str) -> InstrumentRecord:
        try:
            return self.get(self._ids[instrument_id.strip().upper()])
        except KeyError as exc:
            raise KeyError(f"unknown instrument_id: {instrument_id}") from exc

    def list_records(self) -> tuple[InstrumentRecord, ...]:
        return tuple(sorted(self._records.values(), key=lambda item: item.instrument_id))

    def resolve(
        self,
        *,
        provider: str,
        market: str,
        native_symbol: str,
        event_time_ns: int,
    ) -> InstrumentRecord:
        key = (
            provider.strip().upper(),
            market.strip().upper(),
            native_symbol.strip().upper(),
        )
        matches = [alias for alias in self._aliases.get(key, []) if alias.contains(event_time_ns)]
        if len(matches) != 1:
            raise KeyError(f"alias resolution requires exactly one temporal match: {key}")
        return self.get(matches[0].instrument_uid)

    def snapshot(self) -> dict[str, object]:
        records = []
        for record in sorted(self._records.values(), key=lambda item: item.instrument_id):
            payload = asdict(record)
            for decimal_name in (
                "price_tick",
                "quantity_step",
                "contract_multiplier",
                "strike_price",
            ):
                decimal_value = payload.get(decimal_name)
                if decimal_value is not None:
                    decimal_value["coefficient"] = str(decimal_value["coefficient"])
            records.append(payload)
        aliases = [
            asdict(alias)
            for values in self._aliases.values()
            for alias in values
        ]
        aliases.sort(key=lambda item: (item["provider"], item["market"], item["native_symbol"]))
        return {"schema": "qdl.instrument-registry.snapshot.v1", "records": records, "aliases": aliases}

    def export(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.snapshot(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

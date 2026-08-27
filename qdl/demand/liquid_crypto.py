"""Provider-neutral source policy for the liquid crypto feature planes.

The policy deliberately distinguishes a broad warmup universe from a small,
leased realtime/L2 set.  It only selects canonical records parsed from provider
metadata; in particular, dated contract symbols and expiries are never built
from date text in this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

from qdl.demand.contracts import (
    DataRequirement,
    DemandFeed,
    DemandPurpose,
    UniverseSelector,
    UniverseSelectorKind,
)
from qdl.domain.instrument import InstrumentRecord, InstrumentStatus, ProductType


_SCHEMA = "qdl.v2.liquid-crypto-feature-policy.v1"
_QUARTERLY_ALIASES = ("CURRENT_QUARTER", "NEXT_QUARTER")
_OKX_QUARTERLY_ALIASES = {"quarter": "CURRENT_QUARTER", "next_quarter": "NEXT_QUARTER"}


@dataclass(frozen=True, slots=True)
class LiquidCryptoFeaturePolicy:
    """A bounded source policy, not a runtime subscription manifest."""

    revision: int
    perpetual_base_assets: tuple[str, ...]
    perpetual_settlement_asset: str
    perpetual_venue_markets: tuple[tuple[str, str], ...]
    l2_base_assets: tuple[str, ...]
    l2_dated_venue_markets: tuple[tuple[str, str], ...]
    l2_depth_per_side: int
    l2_freshness_ms: int
    l2_ttl_seconds: int
    source_policy_id: str

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("liquid feature policy revision must be positive")
        for name, values in (
            ("perpetual_base_assets", self.perpetual_base_assets),
            ("l2_base_assets", self.l2_base_assets),
        ):
            normalized = tuple(str(value).strip().upper() for value in values)
            if not normalized or len(normalized) != len(set(normalized)) or any(not value.isalnum() for value in normalized):
                raise ValueError(f"{name} must contain unique nonblank asset codes")
            object.__setattr__(self, name, normalized)
        if not set(self.l2_base_assets).issubset(self.perpetual_base_assets):
            raise ValueError("l2_base_assets must be a subset of perpetual_base_assets")
        settlement = str(self.perpetual_settlement_asset).strip().upper()
        if not settlement.isalnum():
            raise ValueError("perpetual_settlement_asset must be an asset code")
        object.__setattr__(self, "perpetual_settlement_asset", settlement)
        for name, values in (
            ("perpetual_venue_markets", self.perpetual_venue_markets),
            ("l2_dated_venue_markets", self.l2_dated_venue_markets),
        ):
            normalized = tuple(
                (str(venue).strip().upper(), str(market).strip().upper())
                for venue, market in values
            )
            if not normalized or len(normalized) != len(set(normalized)) or any(not venue or not market for venue, market in normalized):
                raise ValueError(f"{name} must contain unique venue/market identities")
            object.__setattr__(self, name, normalized)
        if set(self.perpetual_venue_markets) != {("BINANCE", "USDM"), ("OKX", "SWAP")}:
            raise ValueError("liquid perpetual policy must declare Binance USD-M and OKX Swap separately")
        if set(self.l2_dated_venue_markets) != {("BINANCE", "USDM"), ("OKX", "FUTURES")}:
            raise ValueError("liquid L2 dated policy must declare Binance USD-M and OKX Futures")
        if not 1 <= self.l2_depth_per_side <= 1_000:
            raise ValueError("l2_depth_per_side is outside safe bounds")
        if not 1 <= self.l2_freshness_ms <= 86_400_000:
            raise ValueError("l2_freshness_ms is outside safe bounds")
        if not 30 <= self.l2_ttl_seconds <= 3_600:
            raise ValueError("l2_ttl_seconds is outside safe bounds")
        if not self.source_policy_id.strip():
            raise ValueError("source_policy_id is required")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "LiquidCryptoFeaturePolicy":
        allowed = {
            "schema",
            "revision",
            "perpetual_base_assets",
            "perpetual_settlement_asset",
            "perpetual_venue_markets",
            "l2_base_assets",
            "l2_dated_venue_markets",
            "l2_depth_per_side",
            "l2_freshness_ms",
            "l2_ttl_seconds",
            "source_policy_id",
        }
        if set(raw) != allowed:
            raise ValueError("liquid feature policy fields are invalid")
        if raw.get("schema") != _SCHEMA:
            raise ValueError("liquid feature policy schema is invalid")

        def pairs(name: str) -> tuple[tuple[str, str], ...]:
            values = raw.get(name)
            if not isinstance(values, list):
                raise ValueError(f"{name} must be a list")
            result = []
            for item in values:
                if not isinstance(item, Mapping) or set(item) != {"venue", "market"}:
                    raise ValueError(f"{name} item is invalid")
                result.append((str(item["venue"]), str(item["market"])))
            return tuple(result)

        def assets(name: str) -> tuple[str, ...]:
            values = raw.get(name)
            if not isinstance(values, list):
                raise ValueError(f"{name} must be a list")
            return tuple(str(value) for value in values)

        return cls(
            revision=int(raw["revision"]),
            perpetual_base_assets=assets("perpetual_base_assets"),
            perpetual_settlement_asset=str(raw["perpetual_settlement_asset"]),
            perpetual_venue_markets=pairs("perpetual_venue_markets"),
            l2_base_assets=assets("l2_base_assets"),
            l2_dated_venue_markets=pairs("l2_dated_venue_markets"),
            l2_depth_per_side=int(raw["l2_depth_per_side"]),
            l2_freshness_ms=int(raw["l2_freshness_ms"]),
            l2_ttl_seconds=int(raw["l2_ttl_seconds"]),
            source_policy_id=str(raw["source_policy_id"]),
        )

    @classmethod
    def load(cls, path: str | Path) -> "LiquidCryptoFeaturePolicy":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError("liquid feature policy must be a mapping")
        return cls.from_mapping(raw)

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "schema": _SCHEMA,
            "revision": self.revision,
            "perpetual_base_assets": list(self.perpetual_base_assets),
            "perpetual_settlement_asset": self.perpetual_settlement_asset,
            "perpetual_venue_markets": [
                {"venue": venue, "market": market}
                for venue, market in self.perpetual_venue_markets
            ],
            "l2_base_assets": list(self.l2_base_assets),
            "l2_dated_venue_markets": [
                {"venue": venue, "market": market}
                for venue, market in self.l2_dated_venue_markets
            ],
            "l2_depth_per_side": self.l2_depth_per_side,
            "l2_freshness_ms": self.l2_freshness_ms,
            "l2_ttl_seconds": self.l2_ttl_seconds,
            "source_policy_id": self.source_policy_id,
        }


@dataclass(frozen=True, slots=True)
class LiquidCryptoFeatureSet:
    """Metadata-discovered physical instruments for C3.6 feature planes."""

    perpetuals: tuple[InstrumentRecord, ...]
    l2_books: tuple[InstrumentRecord, ...]

    def __post_init__(self) -> None:
        if not self.perpetuals or not self.l2_books:
            raise ValueError("liquid crypto feature set cannot be empty")
        for name, values in (("perpetuals", self.perpetuals), ("l2_books", self.l2_books)):
            ids = [item.instrument_id for item in values]
            if len(ids) != len(set(ids)):
                raise ValueError(f"liquid crypto {name} contain duplicate identities")


def reference_feeds_for(record: InstrumentRecord) -> tuple[DemandFeed, ...]:
    """Return canonical reference surfaces without inventing provider values."""

    key = (record.identity.venue, record.identity.market)
    if key == ("BINANCE", "USDM"):
        return (
            DemandFeed.FUNDING_RATE,
            DemandFeed.OPEN_INTEREST,
            DemandFeed.LONG_SHORT_RATIO,
            DemandFeed.TAKER_FLOW,
            DemandFeed.MARK_PRICE,
            DemandFeed.INDEX_PRICE,
            DemandFeed.CONTRACT_METADATA,
            DemandFeed.BASIS,
        )
    if key == ("OKX", "SWAP"):
        return (
            DemandFeed.FUNDING_RATE,
            DemandFeed.OPEN_INTEREST,
            DemandFeed.MARK_PRICE,
            DemandFeed.INDEX_PRICE,
            DemandFeed.CONTRACT_METADATA,
            DemandFeed.BASIS,
        )
    if key == ("OKX", "FUTURES"):
        return (
            DemandFeed.OPEN_INTEREST,
            DemandFeed.MARK_PRICE,
            DemandFeed.INDEX_PRICE,
            DemandFeed.CONTRACT_METADATA,
            DemandFeed.BASIS,
        )
    raise ValueError("reference feed policy is not declared for this venue/market")


def select_liquid_crypto_feature_set(
    records: Iterable[InstrumentRecord],
    *,
    policy: LiquidCryptoFeaturePolicy,
) -> LiquidCryptoFeatureSet:
    """Select only exact active provider records for liquid and dated features."""

    active = tuple(
        record for record in records if record.status is InstrumentStatus.ACTIVE
    )
    perpetuals: list[InstrumentRecord] = []
    for venue, market in policy.perpetual_venue_markets:
        for base in policy.perpetual_base_assets:
            perpetuals.append(
                _exact_record(
                    active,
                    venue=venue,
                    market=market,
                    product_type=ProductType.PERPETUAL,
                    base_asset=base,
                    settlement_asset=policy.perpetual_settlement_asset,
                    label="liquid perpetual",
                )
            )

    l2_books = [
        record for record in perpetuals if record.base_asset in policy.l2_base_assets
    ]
    l2_books.extend(_binance_quarterlies(active, policy))
    l2_books.extend(_okx_quarterlies(active, policy))
    return LiquidCryptoFeatureSet(
        perpetuals=tuple(sorted(perpetuals, key=_record_sort_key)),
        l2_books=tuple(sorted(l2_books, key=_record_sort_key)),
    )


def build_l2_feature_requirements(
    feature_set: LiquidCryptoFeatureSet,
    *,
    policy: LiquidCryptoFeaturePolicy,
    consumer_id: str,
    purpose: DemandPurpose = DemandPurpose.ALPHA,
) -> tuple[DataRequirement, ...]:
    """Build declarative snapshot/delta leases; does not activate them."""

    owner = str(consumer_id).strip()
    if not owner:
        raise ValueError("consumer_id is required")
    result: list[DataRequirement] = []
    for record in feature_set.l2_books:
        for feed in (DemandFeed.BOOK_SNAPSHOT, DemandFeed.BOOK_DELTA):
            result.append(
                DataRequirement(
                    consumer_id=owner,
                    purpose=purpose,
                    universe=UniverseSelector(
                        selector_id=f"{owner}:{record.instrument_uid}:{feed.value}",
                        kind=UniverseSelectorKind.EXPLICIT,
                        venue=record.identity.venue,
                        market=record.identity.market,
                        product_type=record.identity.product_type.value,
                        native_symbols=(record.native_symbol,),
                    ),
                    feed=feed,
                    source_policy_id=policy.source_policy_id,
                    max_freshness_ms=policy.l2_freshness_ms,
                    ttl_seconds=policy.l2_ttl_seconds,
                    require_live=True,
                    depth_levels=policy.l2_depth_per_side,
                    configuration_revision=policy.revision,
                )
            )
    return tuple(sorted(result, key=lambda item: item.requirement_id))


def _exact_record(
    records: Iterable[InstrumentRecord],
    *,
    venue: str,
    market: str,
    product_type: ProductType,
    base_asset: str,
    settlement_asset: str,
    label: str,
) -> InstrumentRecord:
    matches = [
        record
        for record in records
        if record.identity.venue == venue
        and record.identity.market == market
        and record.identity.product_type is product_type
        and record.base_asset == base_asset
        and record.quote_asset == settlement_asset
        and record.settlement_asset == settlement_asset
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{label} identity is missing or ambiguous: {venue}/{market}/{base_asset}"
        )
    return matches[0]


def _binance_quarterlies(
    records: Iterable[InstrumentRecord], policy: LiquidCryptoFeaturePolicy
) -> tuple[InstrumentRecord, ...]:
    if ("BINANCE", "USDM") not in policy.l2_dated_venue_markets:
        return ()
    selected: list[InstrumentRecord] = []
    for base in policy.l2_base_assets:
        for contract_type in _QUARTERLY_ALIASES:
            matches = [
                record
                for record in records
                if record.identity.venue == "BINANCE"
                and record.identity.market == "USDM"
                and record.identity.product_type is ProductType.FUTURE
                and record.base_asset == base
                and str(record.attributes.get("contractType") or "").upper() == contract_type
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"Binance {base} {contract_type} quarterly is missing or ambiguous"
                )
            selected.append(matches[0])
    return tuple(selected)


def _okx_quarterlies(
    records: Iterable[InstrumentRecord], policy: LiquidCryptoFeaturePolicy
) -> tuple[InstrumentRecord, ...]:
    if ("OKX", "FUTURES") not in policy.l2_dated_venue_markets:
        return ()
    selected: list[InstrumentRecord] = []
    for base in policy.l2_base_assets:
        pairs: dict[tuple[str, str], dict[str, InstrumentRecord]] = {}
        for record in records:
            if (
                record.identity.venue != "OKX"
                or record.identity.market != "FUTURES"
                or record.identity.product_type is not ProductType.FUTURE
                or record.base_asset != base
            ):
                continue
            alias = _OKX_QUARTERLY_ALIASES.get(
                str(record.attributes.get("alias") or "").lower()
            )
            if alias is None:
                continue
            family = str(record.attributes.get("instFamily") or "").strip().upper()
            if not family:
                raise ValueError("OKX dated future is missing provider instFamily")
            key = (record.settlement_asset, family)
            current = pairs.setdefault(key, {})
            if alias in current:
                raise ValueError(f"OKX {base} {key} has duplicate {alias} contracts")
            current[alias] = record
        complete = []
        for key, values in sorted(pairs.items()):
            if set(values) == set(_QUARTERLY_ALIASES):
                complete.extend(values[alias] for alias in _QUARTERLY_ALIASES)
            elif values:
                raise ValueError(f"OKX {base} {key} quarterly pair is incomplete")
        if not complete:
            raise ValueError(f"OKX {base} has no live current/next quarterly pair")
        selected.extend(complete)
    return tuple(selected)


def _record_sort_key(record: InstrumentRecord) -> tuple[str, str, str, str]:
    return (
        record.identity.venue,
        record.identity.market,
        record.identity.product_type.value,
        record.native_symbol,
    )

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from qdl.adapters.intervals import (
    BINANCE_SPOT_NATIVE_INTERVALS,
    BINANCE_USDM_NATIVE_INTERVALS,
    OKX_NATIVE_INTERVALS,
    VN_NATIVE_INTERVALS,
)


class CapabilityAvailability(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    TIER_GATED = "TIER_GATED"
    REGION_GATED = "REGION_GATED"
    UNVERIFIED = "UNVERIFIED"


@dataclass(frozen=True)
class FeedCapability:
    availability: CapabilityAvailability
    rest_history: bool = False
    live: bool = False
    snapshot: bool = False
    delta: bool = False
    sequence: bool = False
    checksum: bool = False
    resubscribe: bool = False
    resnapshot_on_gap: bool = False
    native_intervals: tuple[str, ...] = ()
    constraint: str | None = None

    @property
    def enabled(self) -> bool:
        return self.availability is CapabilityAvailability.AVAILABLE


@dataclass(frozen=True)
class VenueCapabilityProfile:
    provider: str
    venue: str
    market: str
    region_profile: str
    legal_entity: str
    account_tier: str
    timestamp_precision: str
    rate_limit_model: str
    source_authority: str
    feeds: dict[str, FeedCapability] = field(default_factory=dict)

    def capability(self, feed: str) -> FeedCapability:
        try:
            return self.feeds[feed.lower()]
        except KeyError as exc:
            raise KeyError(f"capability not declared for feed: {feed}") from exc

    def require(self, feed: str) -> FeedCapability:
        capability = self.capability(feed)
        if not capability.enabled:
            raise RuntimeError(
                f"{self.provider}/{self.market}/{feed} is {capability.availability.value}: "
                f"{capability.constraint or 'no approved capability'}"
            )
        return capability


def okx_global_capabilities(market: str, *, account_tier: str = "PUBLIC") -> VenueCapabilityProfile:
    market_value = market.upper()
    if market_value not in {"SPOT", "SWAP", "FUTURES", "OPTION", "EVENTS"}:
        raise ValueError(f"unsupported OKX market profile: {market}")
    deep_book = CapabilityAvailability.TIER_GATED
    return VenueCapabilityProfile(
        provider="OKX_DIRECT",
        venue="OKX",
        market=market_value,
        region_profile="GLOBAL",
        legal_entity="OKX_GLOBAL",
        account_tier=account_tier,
        timestamp_precision="MILLISECOND",
        rate_limit_model="ENDPOINT_BUCKET_PLUS_IP_OR_USER",
        source_authority="SHADOW",
        feeds={
            "instrument": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True, live=True, resubscribe=True),
            "trade": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True, live=True, resubscribe=True),
            "bbo": FeedCapability(CapabilityAvailability.AVAILABLE, live=True, resubscribe=True),
            "bar": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                live=True,
                resubscribe=True,
                native_intervals=OKX_NATIVE_INTERVALS,
            ),
            "l2": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                live=True,
                snapshot=True,
                delta=True,
                sequence=True,
                checksum=False,
                resubscribe=True,
                resnapshot_on_gap=True,
            ),
            "l2_deep": FeedCapability(
                deep_book,
                live=True,
                snapshot=True,
                delta=True,
                sequence=True,
                checksum=False,
                resubscribe=True,
                resnapshot_on_gap=True,
                constraint="requires approved OKX VIP/channel entitlement",
            ),
            "funding_rate": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="public funding-history; cadence is provider supplied",
            ),
            "open_interest": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                snapshot=True,
                constraint="public current snapshot; no certified historical series in this profile",
            ),
            "mark_index_price": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                snapshot=True,
                constraint="mark/index history and current public reference reads",
            ),
            "long_short_ratio": FeedCapability(
                CapabilityAvailability.UNAVAILABLE,
                constraint="no provider-equivalent public OKX global/top account ratio",
            ),
            "taker_flow": FeedCapability(
                CapabilityAvailability.UNAVAILABLE,
                constraint="no provider-equivalent public OKX taker long/short ratio",
            ),
            "basis": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="derived-only from explicit mark/index or contract inputs; no provider-native basis claim",
            ),
            "contract_metadata": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                snapshot=True,
                constraint="versioned public instrument metadata",
            ),
            "sbe_trade": FeedCapability(
                CapabilityAvailability.TIER_GATED,
                live=True,
                sequence=True,
                resubscribe=True,
                constraint="requires pinned OKX SBE schema, login/tier entitlement and JSON parity",
            ),
            "sbe_bbo": FeedCapability(
                CapabilityAvailability.TIER_GATED,
                live=True,
                resubscribe=True,
                constraint="requires authenticated SBE service and tested JSON rollback",
            ),
            "sbe_l2": FeedCapability(
                CapabilityAvailability.TIER_GATED,
                live=True,
                snapshot=True,
                delta=True,
                sequence=True,
                resubscribe=True,
                resnapshot_on_gap=True,
                constraint="requires VIP deep-book entitlement and unknown-schema fail-closed decoder",
            ),
            "option_summary": FeedCapability(
                CapabilityAvailability.REGION_GATED,
                rest_history=True,
                live=True,
                constraint="requires approved OKX legal-entity/profile option endpoint",
            ),
        },
    )


def binance_usdm_capabilities() -> VenueCapabilityProfile:
    return VenueCapabilityProfile(
        provider="BINANCE_DIRECT",
        venue="BINANCE",
        market="USDM",
        region_profile="GLOBAL",
        legal_entity="BINANCE_GLOBAL",
        account_tier="PUBLIC",
        timestamp_precision="MILLISECOND",
        rate_limit_model="REQUEST_WEIGHT_PLUS_IP",
        source_authority="PRIMARY",
        feeds={
            "instrument": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True),
            "trade": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True, live=True, resubscribe=True),
            "bbo": FeedCapability(CapabilityAvailability.AVAILABLE, live=True, resubscribe=True),
            "bar": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                live=True,
                resubscribe=True,
                native_intervals=BINANCE_USDM_NATIVE_INTERVALS,
            ),
            "l2": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                live=True,
                snapshot=True,
                delta=True,
                sequence=True,
                resubscribe=True,
                resnapshot_on_gap=True,
            ),
            "funding_rate": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="public USD-M funding history",
            ),
            "open_interest": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                snapshot=True,
                constraint="public current and bounded USD-M open-interest history",
            ),
            "mark_index_price": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                snapshot=True,
                constraint="public USD-M mark/index reference reads",
            ),
            "long_short_ratio": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="bounded public global/top account and top-position ratio history",
            ),
            "taker_flow": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="bounded public taker buy/sell ratio history",
            ),
            "basis": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                constraint="bounded provider-native USD-M basis history by contract type",
            ),
            "contract_metadata": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                snapshot=True,
                constraint="versioned USD-M exchange-info metadata",
            ),
        },
    )


def binance_spot_capabilities() -> VenueCapabilityProfile:
    """Spot uses the same public market-data contract, not the USD-M profile."""
    return VenueCapabilityProfile(
        provider="BINANCE_DIRECT",
        venue="BINANCE",
        market="SPOT",
        region_profile="GLOBAL",
        legal_entity="BINANCE_GLOBAL",
        account_tier="PUBLIC",
        timestamp_precision="MILLISECOND",
        rate_limit_model="REQUEST_WEIGHT_PLUS_IP",
        source_authority="PRIMARY",
        feeds={
            "instrument": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True),
            "trade": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True, live=True, resubscribe=True),
            "bbo": FeedCapability(CapabilityAvailability.AVAILABLE, live=True, resubscribe=True),
            "bar": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                live=True,
                resubscribe=True,
                native_intervals=BINANCE_SPOT_NATIVE_INTERVALS,
            ),
            "l2": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                live=True,
                snapshot=True,
                delta=True,
                sequence=True,
                resubscribe=True,
                resnapshot_on_gap=True,
            ),
        },
    )


def dnse_capabilities() -> VenueCapabilityProfile:
    return VenueCapabilityProfile(
        provider="DNSE",
        venue="VN_MARKETS",
        market="EQUITY_AND_DERIVATIVES",
        region_profile="VN",
        legal_entity="DNSE_VN",
        account_tier="CONFIGURED_ACCOUNT",
        timestamp_precision="MILLISECOND",
        rate_limit_model="PROVIDER_SESSION_AND_ENDPOINT",
        source_authority="PRIMARY",
        feeds={
            "instrument": FeedCapability(CapabilityAvailability.UNVERIFIED, constraint="instrument master reconciles controlled VN allowlist"),
            "trade": FeedCapability(CapabilityAvailability.UNVERIFIED, live=True, resubscribe=True),
            "bbo": FeedCapability(CapabilityAvailability.AVAILABLE, live=True, resubscribe=True),
            "bar": FeedCapability(
                CapabilityAvailability.AVAILABLE,
                rest_history=True,
                native_intervals=VN_NATIVE_INTERVALS,
            ),
            "l2": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="not certified in current provider contract"),
            "funding_rate": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="crypto derivative metric not applicable to DNSE profile"),
            "open_interest": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="no certified public VN derivative OI product in current adapter"),
            "mark_index_price": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="crypto mark/index semantics are not applicable to DNSE profile"),
            "long_short_ratio": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="crypto participant-ratio metric is not applicable to DNSE profile"),
            "taker_flow": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="crypto taker-flow metric is not applicable to DNSE profile"),
            "basis": FeedCapability(CapabilityAvailability.UNAVAILABLE, constraint="crypto futures-basis product is not applicable to DNSE profile"),
            "contract_metadata": FeedCapability(CapabilityAvailability.AVAILABLE, rest_history=True, snapshot=True, constraint="versioned VN instrument metadata only"),
        },
    )

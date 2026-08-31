"""Canonical BAR interval semantics shared by every venue adapter.

The instrument catalog and the public V2 contract carry a canonical lowercase
``<count><unit>`` interval. Venue-native spellings are derived here so that no
adapter keeps a private table which can silently drift from another venue's.

Canonical intervals are fixed-duration only. Month and quarter bars have no
constant millisecond length, so they are rejected rather than approximated;
a calendar-month product needs its own capability and gap contract.
"""

from __future__ import annotations

_UNIT_MS = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}

_MONDAY_AFTER_UNIX_EPOCH_MS = 4 * 86_400_000

# OKX spells intraday bars natively, but its calendar bars are aligned to a
# UTC+8 trading day by default; only the ``utc`` suffix selects the UTC+0
# calendar. Canonical ``1d`` means exactly one UTC day on every venue, so
# calendar bars map to the ``utc`` variants and never to the UTC+8 default.
# Source: upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md, bar size table.
_OKX_INTRADAY = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h")
# Calendar bars resolve to the utc variants so a canonical day means the same
# UTC day on every venue. The fetcher no longer computes a boundary from the
# epoch, so multi-day and weekly bars, whose anchor the epoch does not share,
# are served by asking the venue for a range and reading the boundaries it
# returns.
_OKX_CALENDAR_UTC = ("6h", "12h", "1d", "2d", "3d", "1w")


def normalise_interval(interval: str) -> str:
    """Validate and return the canonical interval token.

    Case is never folded. Both Binance and OKX spell a calendar month ``1M``
    and a minute ``1m``, so lowercasing the input would silently turn a month
    request into a minute of data. Anything that is not already canonical
    lowercase fails closed instead.
    """
    value = str(interval or "").strip()
    if not value:
        raise ValueError("canonical interval is required")
    if value.endswith("M"):
        raise ValueError(
            "calendar-month bars have no fixed duration and are not canonical "
            f"intervals; 'M' is never folded into minutes: {interval!r}"
        )
    if value != value.lower():
        raise ValueError(
            f"canonical interval must be lowercase, venue spelling is derived: {interval!r}"
        )
    return value


def canonical_interval_ms(interval: str) -> int:
    """Return the fixed millisecond duration of a canonical interval."""
    value = normalise_interval(interval)
    unit = value[-1]
    if unit not in _UNIT_MS:
        raise ValueError(
            f"canonical interval must use a fixed s/m/h/d/w duration: {interval!r}"
        )
    try:
        count = int(value[:-1])
    except ValueError as error:
        raise ValueError(
            f"canonical interval count must be an integer: {interval!r}"
        ) from error
    if count <= 0:
        raise ValueError(f"canonical interval must be positive: {interval!r}")
    return count * _UNIT_MS[unit]


def provider_bar_calendar_anchor_ms(interval: str, *, provider: str | None = None) -> int:
    """Return the documented open-time anchor for one provider BAR interval.

    Canonical duration and provider calendar anchor are separate concerns.
    Binance ``3d`` and weekly klines start on the provider's Monday UTC grid;
    OKX ``3Dutc`` starts on the Unix UTC grid while ``1Wutc`` is Monday
    anchored.  Keeping this mapping here lets history, scheduling and durable
    checkpoint validation agree without making a venue-specific exception in
    one caller.
    """
    value = normalise_interval(interval)
    venue = (provider or "").strip().upper()
    if value == "1w":
        return _MONDAY_AFTER_UNIX_EPOCH_MS
    if value == "3d" and venue == "BINANCE":
        return _MONDAY_AFTER_UNIX_EPOCH_MS
    return 0


def is_valid_bar_open_ms(
    interval: str,
    open_ms: int,
    *,
    provider: str | None = None,
) -> bool:
    """Return whether an observed BAR open lies on its provider calendar grid."""
    if isinstance(open_ms, bool) or not isinstance(open_ms, int) or open_ms <= 0:
        return False
    duration_ms = canonical_interval_ms(interval)
    anchor_ms = provider_bar_calendar_anchor_ms(interval, provider=provider)
    return (open_ms - anchor_ms) % duration_ms == 0


def latest_closed_boundary_ms(
    interval: str,
    observed_ms: int,
    *,
    provider: str | None = None,
) -> int:
    """Return the latest exclusive closed-bar boundary in UTC.

    Intraday and daily fixed-duration bars are Unix aligned.  Provider-aware
    callers also get the documented calendar anchor for Binance/OKX multi-day
    bars, so a durable checkpoint and a provider history cutoff cannot disagree
    about a valid final BAR.
    """
    if observed_ms <= 0:
        raise ValueError("bar observation time must be positive")
    value = normalise_interval(interval)
    duration_ms = canonical_interval_ms(value)
    anchor_ms = provider_bar_calendar_anchor_ms(value, provider=provider)
    return (observed_ms - anchor_ms) // duration_ms * duration_ms + anchor_ms


def okx_bar_size(interval: str) -> str:
    """Return the OKX ``bar`` token for a canonical interval.

    Raises for any interval OKX does not expose as a fixed-duration bar, so an
    unsupported request fails closed instead of reaching the venue.
    """
    value = normalise_interval(interval)
    canonical_interval_ms(value)
    if value in _OKX_INTRADAY:
        return value[:-1] + value[-1].upper() if value[-1] == "h" else value
    if value in _OKX_CALENDAR_UTC:
        return f"{value[:-1]}{value[-1].upper()}utc"
    raise ValueError(f"OKX does not expose a fixed-duration bar for {interval!r}")


def okx_candle_channel(interval: str) -> str:
    """Return the OKX candle channel name for a canonical interval."""
    return f"candle{okx_bar_size(interval)}"


_OKX_SUPPORTED = _OKX_INTRADAY + _OKX_CALENDAR_UTC

# Exact provider-native fixed-duration bars exposed to the demand/warmup
# planner. Calendar months are intentionally absent because V2 canonical
# intervals are fixed-duration only.
BINANCE_USDM_NATIVE_INTERVALS = (
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h",
    "12h", "1d", "3d", "1w",
)
BINANCE_SPOT_NATIVE_INTERVALS = (
    "1s", *BINANCE_USDM_NATIVE_INTERVALS,
)
OKX_NATIVE_INTERVALS = _OKX_SUPPORTED
VN_NATIVE_INTERVALS = ("1m",)


def okx_interval_from_bar_size(bar: str) -> str:
    """Return the canonical interval for an OKX ``bar`` token.

    The inverse of :func:`okx_bar_size`. Resolution is exact: an unknown or
    ambiguously cased token fails closed rather than being coerced.
    """
    value = str(bar or "").strip()
    if not value:
        raise ValueError("OKX bar size is required")
    for interval in _OKX_SUPPORTED:
        if okx_bar_size(interval) == value:
            return interval
    raise ValueError(f"unsupported OKX bar size: {bar!r}")


def okx_interval_from_channel(channel: str) -> str:
    """Return the canonical interval for an OKX candle channel name."""
    value = str(channel or "").strip()
    prefix = "candle"
    if not value.startswith(prefix) or len(value) == len(prefix):
        raise ValueError(f"not an OKX candle channel: {channel!r}")
    return okx_interval_from_bar_size(value[len(prefix):])

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

# OKX spells intraday bars natively, but its calendar bars are aligned to a
# UTC+8 trading day by default; only the ``utc`` suffix selects the UTC+0
# calendar. Canonical ``1d`` means exactly one UTC day on every venue, so
# calendar bars map to the ``utc`` variants and never to the UTC+8 default.
# Source: upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md, bar size table.
_OKX_INTRADAY = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h")
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

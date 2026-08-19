"""Binance V2 acquisition edges."""

from qdl.adapters.binance.bar_edge import (
    BinanceBarRawBinding,
    fetch_latest_closed_bar_raw_envelope,
)

__all__ = ["BinanceBarRawBinding", "fetch_latest_closed_bar_raw_envelope"]

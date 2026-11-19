"""DL v2.4.2: a provider says an instrument does not exist (delisted, closed, renamed): one typed, per-instrument failure.

Purpose: before v2.4.2 such an answer surfaced as a generic error (OKX: `ValueError` -> `RuntimeError("OKX V5 request
exhausted retries")` after three attempts; Binance: `BinanceProviderError`). The bar edge could not tell it from a
transient fault, so one delisted instrument (OKX `RVN-USDT-SWAP`, 2026-11-19) failed every cycle and blocked the whole
history repair behind it. Adapters now raise `ProviderInstrumentUnavailable` for the documented "no such instrument"
answers below, without retrying the same request; callers quarantine that one binding and continue with the others.

Codes (each with its source):
- OKX `51001`, message "Instrument ID, Instrument ID code, or Spread ID doesn't exist." (OKX V5 REST error codes,
  https://www.okx.com/docs-v5/en/#error-code-rest-api-public; the message is the venue's verbatim response captured
  2026-11-19 13:04Z for `RVN-USDT-SWAP` on `/api/v5/market/history-candles`).
- Binance USD-M `-1121` BAD_SYMBOL "Invalid symbol.", `-1122` INVALID_SYMBOL_STATUS "Invalid symbol status.",
  `-4141` SYMBOL_ALREADY_CLOSED "Symbol is closed" (https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code).

The classification is about the instrument, not the request: it is never a rate limit (those stay
`ProviderRateLimited`) and never retried immediately. Venue truth for removal is the live instruments list, applied
through the declaration gate (`scripts/catalog_venue_listing_gate.py`), not this error.
"""
from __future__ import annotations

OKX_INSTRUMENT_UNAVAILABLE_CODES = {"51001": "Instrument ID, Instrument ID code, or Spread ID doesn't exist."}
BINANCE_INSTRUMENT_UNAVAILABLE_CODES = {
    -1121: "BAD_SYMBOL Invalid symbol.",
    -1122: "INVALID_SYMBOL_STATUS Invalid symbol status.",
    -4141: "SYMBOL_ALREADY_CLOSED Symbol is closed",
}


class ProviderInstrumentUnavailable(RuntimeError):
    """The provider answered that this instrument does not exist or is no longer tradable/listed."""

    def __init__(self, provider: str, symbol: str, code: str | int, message: str | None = None,
                 endpoint: str | None = None) -> None:
        self.provider = provider
        self.symbol = symbol
        self.code = str(code)
        self.provider_message = message
        self.endpoint = endpoint
        super().__init__(f"{provider} instrument unavailable symbol={symbol} code={code} endpoint={endpoint} "
                         f"message={message}")


def find_instrument_unavailable(error: BaseException | None) -> ProviderInstrumentUnavailable | None:
    """The typed failure anywhere in the exception chain (adapters may wrap it), else None."""
    seen = set()
    while error is not None and id(error) not in seen:
        if isinstance(error, ProviderInstrumentUnavailable):
            return error
        seen.add(id(error))
        error = error.__cause__ or error.__context__
    return None

"""DL v2.4.2: a provider says an instrument does not exist (delisted, closed, renamed): one typed, per-instrument failure.

Purpose: before v2.4.2 such an answer surfaced as a generic error (OKX: `ValueError` -> `RuntimeError("OKX V5 request
exhausted retries")` after three attempts; Binance: `BinanceProviderError`). The bar edge could not tell it from a
transient fault, so one delisted instrument (OKX `RVN-USDT-SWAP`, 2026-11-19) failed every cycle and blocked the whole
history repair behind it. Adapters now raise `ProviderInstrumentUnavailable` for the documented "no such instrument"
answers below, without retrying the same request; callers quarantine that one binding and continue with the others.

Codes (each with its source; only documented codes are listed):
- OKX V5 REST, section "Public / Trade class" (https://www.okx.com/docs-v5/en/#error-code-rest-api-public-trade-class;
  page sha256 853869c8c075520f2baa2f77d2f348486319c14e218fef2b330a196946814bbd captured 2026-11-19T13:47:45Z, excerpt in
  tests/fixtures/v242_okx_docs/okx-error-codes-excerpt.json):
  `51001` "Instrument ID, Instrument ID code or Spread ID doesn't exist." (also the venue's verbatim answer for
  `RVN-USDT-SWAP` on `/api/v5/market/history-candles`, 2026-11-19 13:04Z), `51021` "The pair or contract is not yet
  listed", `51022` "Contract suspended.", `51027` "Contract expired.".
  Deliberately NOT here: `51028` "Contract under delivery.", `51029` "Contract is being settled.", `51030` "Funding fee
  is being settled." are documented transient states of a listed instrument and keep the ordinary retry path.
  `51155` was checked and is not an instrument-existence code on that page.
- OKX V5 WebSocket `60018` "Wrong URL or {0} doesn't exist." (#error-code-websocket-public-general-class) is the
  live-path equivalent; it is handled per subscription argument by the Rust ingestor
  (`qdl_core::okx::parse_subscription_ack_or_rejection`), not here.
- Binance USD-M `-1121` BAD_SYMBOL "Invalid symbol.", `-1122` INVALID_SYMBOL_STATUS "Invalid symbol status.",
  `-4141` SYMBOL_ALREADY_CLOSED "Symbol is closed" (https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code).

The classification is about the instrument, not the request: it is never a rate limit (those stay
`ProviderRateLimited`) and never retried immediately. Venue truth for removal is the live instruments list, applied
through the declaration gate (`scripts/catalog_venue_listing_gate.py`), not this error.
"""
from __future__ import annotations

OKX_DOCS_ERROR_CODES_URL = "https://www.okx.com/docs-v5/en/#error-code-rest-api-public-trade-class"
OKX_INSTRUMENT_UNAVAILABLE_CODES = {
    "51001": "Instrument ID, Instrument ID code or Spread ID doesn't exist.",
    "51021": "The pair or contract is not yet listed",
    "51022": "Contract suspended.",
    "51027": "Contract expired.",
}
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

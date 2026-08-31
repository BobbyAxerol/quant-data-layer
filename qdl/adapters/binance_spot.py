"""Discover Binance Spot instruments from an authentic exchangeInfo capture.

The USD-M parser cannot serve Spot: it pins `market="USDM"` and requires a
`contractType`, which Spot symbols do not carry, so every Spot symbol is
silently skipped. Sharing it would therefore produce an empty discovery rather
than an error, which is the worst possible failure for a catalog generator.

This parser is deliberately separate and states Spot's own identity rules:
a Spot symbol has no contract type, no expiry, no margin asset and a
multiplier of one, and its settlement asset is the quote asset.
"""

from __future__ import annotations

from typing import Any, Mapping

from qdl.adapters.binance_usdm import BinanceDiscovery, _filter_value
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentStatus,
    ProductType,
)

BINANCE_SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"


def parse_spot_exchange_info(
    payload: Mapping[str, Any], *, valid_from_ns: int
) -> BinanceDiscovery:
    records: list[InstrumentRecord] = []
    aliases: list[InstrumentAlias] = []
    for item in payload.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType"):
            raise ValueError(
                "Binance Spot exchangeInfo must not contain a contract type; "
                "this looks like a derivatives capture"
            )
        native_symbol = str(item.get("symbol") or "").upper()
        if not native_symbol:
            raise ValueError("Binance exchangeInfo contains an empty symbol")
        base_asset = str(item.get("baseAsset") or "").upper()
        quote_asset = str(item.get("quoteAsset") or "").upper()
        if not base_asset or not quote_asset:
            raise ValueError("Binance exchangeInfo is missing base/quote asset identity")
        identity = InstrumentIdentity.create(
            venue="BINANCE",
            market="SPOT",
            product_type=ProductType.SPOT,
            canonical_symbol=f"{base_asset}-{quote_asset}",
        )
        record = InstrumentRecord(
            identity=identity,
            metadata_revision=1,
            asset_class=AssetClass.CRYPTO,
            native_symbol=native_symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            # Spot settles in the quote asset; there is no separate margin asset.
            settlement_asset=quote_asset,
            price_tick=CanonicalDecimal.from_text(
                _filter_value(item, "PRICE_FILTER", "tickSize")
            ),
            quantity_step=CanonicalDecimal.from_text(
                _filter_value(item, "LOT_SIZE", "stepSize")
            ),
            contract_multiplier=CanonicalDecimal.from_text("1"),
            session_calendar_id="CRYPTO_24X7",
            status=InstrumentStatus.ACTIVE,
            expiry_time_ns=None,
            valid_from_ns=valid_from_ns,
            attributes={},
        )
        records.append(record)
        aliases.append(InstrumentAlias(
            provider="BINANCE_DIRECT",
            market="SPOT",
            native_symbol=native_symbol,
            instrument_uid=record.instrument_uid,
            instrument_revision=record.metadata_revision,
            valid_from_ns=valid_from_ns,
        ))
    if not records:
        raise ValueError("Binance exchangeInfo returned no active Spot instruments")
    return BinanceDiscovery(
        tuple(records), tuple(aliases), int(payload.get("serverTime") or 0)
    )

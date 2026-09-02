from __future__ import annotations

from qdl._compat import StrEnum

from qdl.common.v1 import common_pb2


class QuantityUnit(StrEnum):
    BASE_ASSET = "BASE_ASSET"
    QUOTE_ASSET = "QUOTE_ASSET"
    CONTRACT = "CONTRACT"
    SHARE = "SHARE"

    @property
    def proto(self) -> int:
        return getattr(common_pb2, f"QUANTITY_UNIT_{self.value}")


def resolve_quantity_unit(
    *, venue: str, market: str, product_type: str
) -> QuantityUnit:
    """Resolve the venue-native order/volume unit without silent defaults."""

    venue_name = venue.strip().upper()
    market_name = market.strip().upper()
    product = product_type.strip().upper()
    if not venue_name or not market_name or not product:
        raise ValueError("quantity-unit identity is incomplete")

    if product == "COMMON_STOCK":
        if venue_name in {"DNSE", "HOSE", "HNX", "UPCOM", "VN_MARKETS"}:
            return QuantityUnit.SHARE
        raise ValueError("COMMON_STOCK quantity unit requires a VN venue identity")

    if product == "FUTURE" and (
        venue_name in {"DNSE", "HOSE", "HNX", "UPCOM", "VN_MARKETS"}
        or market_name in {"VN_DERIVATIVES", "DERIVATIVES"}
    ):
        return QuantityUnit.CONTRACT

    if venue_name == "OKX":
        if market_name == "SPOT" and product == "SPOT":
            return QuantityUnit.BASE_ASSET
        if market_name in {"SWAP", "FUTURES", "OPTIONS"} and product in {
            "PERPETUAL", "FUTURE", "OPTION",
        }:
            return QuantityUnit.CONTRACT
        raise ValueError("unsupported OKX quantity-unit identity")

    if venue_name == "DERIBIT" and product == "OPTION":
        return QuantityUnit.CONTRACT

    if venue_name == "BINANCE":
        if market_name == "SPOT" and product == "SPOT":
            return QuantityUnit.BASE_ASSET
        if market_name == "USDM" and product in {"PERPETUAL", "FUTURE"}:
            # Binance USD-M trade/book/kline quantity is expressed in base-asset
            # units for both perpetual and dated delivery futures. The dated
            # instrument keeps its expiry in identity/lineage; it does not
            # change the venue-native quantity unit into contract count.
            return QuantityUnit.BASE_ASSET
        raise ValueError("unsupported Binance quantity-unit identity")

    raise ValueError(
        f"quantity unit is undefined for {venue_name}/{market_name}/{product}"
    )


def quantity_unit_proto(*, venue: str, market: str, product_type: str) -> int:
    return resolve_quantity_unit(
        venue=venue, market=market, product_type=product_type
    ).proto


def quantity_unit_name(value: int) -> str:
    name = common_pb2.QuantityUnit.Name(value)
    if name == "QUANTITY_UNIT_UNSPECIFIED":
        raise ValueError("canonical quantity unit cannot be UNSPECIFIED")
    return name.removeprefix("QUANTITY_UNIT_")

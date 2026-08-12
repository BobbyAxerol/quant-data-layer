from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.context import DataLayerContext, get_context


router = APIRouter(prefix="/v1", tags=["latest"])


async def _touch_demand(ctx: DataLayerContext, **kwargs) -> None:
    registry = getattr(ctx, "demand_registry", None)
    if registry is not None:
        await registry.touch_request(**kwargs)


@router.get("/binance/price/{symbol}")
async def get_binance_price(symbol: str, market: str = "auto", ctx: DataLayerContext = Depends(get_context)):
    await _touch_demand(ctx,
        owner_id=f"api:binance-price:{market}:{symbol.upper()}",
        source=f"binance_{market}",
        feed="trade",
        symbol=symbol,
    )
    data = await ctx.redis_cache.get_binance_price(symbol.upper(), market=market)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached trade price for {symbol} market={market}")
    return data


@router.get("/binance/price-last/{symbol}")
async def get_binance_price_last(symbol: str, market: str = "auto", ctx: DataLayerContext = Depends(get_context)):
    await _touch_demand(ctx,
        owner_id=f"api:binance-price-last:{market}:{symbol.upper()}",
        source=f"binance_{market}",
        feed="trade",
        symbol=symbol,
    )
    data = await ctx.redis_cache.get_binance_price_last(symbol.upper(), market=market)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No last trade price snapshot for {symbol} market={market}")
    live = await ctx.redis_cache.get_binance_price(symbol.upper(), market=market)
    return {
        "symbol": symbol.upper(),
        "market": market.lower().strip() or "auto",
        "is_live": live is not None,
        "snapshot": data,
    }


@router.get("/binance/kline/{symbol}")
async def get_binance_kline(
    symbol: str,
    interval: str = "1m",
    ctx: DataLayerContext = Depends(get_context),
):
    await _touch_demand(ctx,
        owner_id=f"api:binance-kline:{interval}:{symbol.upper()}",
        source="binance",
        feed="kline",
        symbol=symbol,
        interval=interval,
    )
    data = await ctx.redis_cache.get_binance_kline(symbol.upper(), interval)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached kline for {symbol} @ {interval}")
    return data


@router.get("/vn/quote/{symbol}")
async def get_vn_quote(symbol: str, ctx: DataLayerContext = Depends(get_context)):
    await _touch_demand(ctx,
        owner_id=f"api:vn-quote:{symbol.upper()}",
        source="dnse",
        feed="vn_quote",
        symbol=symbol,
    )
    data = await ctx.redis_cache.get_vn_quote(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached quote for {symbol}")
    return data


@router.get("/vn/quote-last/{symbol}")
async def get_vn_quote_last(symbol: str, ctx: DataLayerContext = Depends(get_context)):
    await _touch_demand(ctx,
        owner_id=f"api:vn-quote-last:{symbol.upper()}",
        source="dnse",
        feed="vn_quote",
        symbol=symbol,
    )
    data = await ctx.redis_cache.get_vn_quote_last(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No last quote snapshot for {symbol}")
    live = await ctx.redis_cache.get_vn_quote(symbol.upper())
    return {
        "symbol": symbol.upper(),
        "is_live": live is not None,
        "snapshot": data,
    }


@router.get("/vn/board")
async def get_vn_board(ctx: DataLayerContext = Depends(get_context)):
    data = await ctx.redis_cache.get_vn_board()
    if data is None:
        raise HTTPException(status_code=404, detail="No cached VN board data")
    return data

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.context import DataLayerContext, get_context


router = APIRouter(prefix="/v1", tags=["latest"])


@router.get("/binance/price/{symbol}")
async def get_binance_price(symbol: str, ctx: DataLayerContext = Depends(get_context)):
    data = await ctx.redis_cache.get_binance_price(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached trade price for {symbol}")
    return data


@router.get("/binance/kline/{symbol}")
async def get_binance_kline(
    symbol: str,
    interval: str = "1m",
    ctx: DataLayerContext = Depends(get_context),
):
    data = await ctx.redis_cache.get_binance_kline(symbol.upper(), interval)
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached kline for {symbol} @ {interval}")
    return data


@router.get("/vn/quote/{symbol}")
async def get_vn_quote(symbol: str, ctx: DataLayerContext = Depends(get_context)):
    data = await ctx.redis_cache.get_vn_quote(symbol.upper())
    if data is None:
        raise HTTPException(status_code=404, detail=f"No cached quote for {symbol}")
    return data


@router.get("/vn/quote-last/{symbol}")
async def get_vn_quote_last(symbol: str, ctx: DataLayerContext = Depends(get_context)):
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


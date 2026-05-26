from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.context import DataLayerContext, get_context
from app.fallback.crypto import fallback_decision, okx_reference_payload
from app.providers.okx import rest as okx_rest


router = APIRouter(prefix="/v1/fallback", tags=["fallback"])


@router.get("/crypto/reference/{symbol}")
async def get_crypto_fallback_reference(
    symbol: str,
    feed: str = Query("kline", description="kline or trade. Trade fallback uses latest OKX candle close as reference."),
    interval: str = Query("1m", description="OKX candle interval used for fallback reference"),
    limit: int = Query(1, ge=1, le=300),
    force: bool = Query(False, description="Operator-forced fallback reference lookup"),
    include_data: bool = Query(True, description="Fetch OKX reference data when fallback is activated"),
    ctx: DataLayerContext = Depends(get_context),
):
    symbol = symbol.upper().strip()
    feed = feed.lower().strip()
    if feed not in {"kline", "trade"}:
        raise HTTPException(status_code=400, detail={"error": "unsupported_fallback_feed", "supported": ["kline", "trade"]})

    if feed == "trade":
        binance_payload = await ctx.redis_cache.get_binance_price(symbol)
    else:
        binance_payload = await ctx.redis_cache.get_binance_kline(symbol, interval)

    decision = fallback_decision(symbol, binance_payload, force=force)
    okx_data = None
    if decision["activated"] and include_data:
        try:
            okx_data = okx_rest.fetch_candles(symbol, interval=interval, limit=limit)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": str(exc), "supported": sorted(okx_rest.OKX_INTERVAL_ALIASES)},
            )
        except okx_rest.OkxProviderError as exc:
            raise HTTPException(status_code=502, detail=exc.detail or str(exc))

    return okx_reference_payload(
        symbol=symbol,
        interval=interval,
        decision=decision,
        okx_data=okx_data,
        feed=feed,
    )


@router.get("/crypto/status/{symbol}")
async def get_crypto_fallback_status(
    symbol: str,
    interval: str = Query("1m", description="Binance kline interval to inspect"),
    ctx: DataLayerContext = Depends(get_context),
):
    symbol = symbol.upper().strip()
    trade_payload = await ctx.redis_cache.get_binance_price(symbol)
    kline_payload = await ctx.redis_cache.get_binance_kline(symbol, interval)
    return {
        "symbol": symbol,
        "reference_provider": "okx",
        "reference_for": "BINANCE",
        "authoritative": False,
        "trade": fallback_decision(symbol, trade_payload),
        "kline": fallback_decision(symbol, kline_payload),
    }


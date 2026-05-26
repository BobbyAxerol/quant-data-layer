from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.providers.binance import rest as binance_rest
from app.providers.okx import rest as okx_rest


router = APIRouter(prefix="/v1", tags=["history"])


def _value_error_400(error: ValueError, *, supported=None):
    detail = {"error": str(error)}
    if supported is not None:
        detail["supported"] = sorted(supported)
    raise HTTPException(status_code=400, detail=detail)


@router.get("/binance/klines/{symbol}")
async def get_binance_klines(
    symbol: str,
    interval: str = "1m",
    limit: int = Query(500, ge=1, le=1500),
    start_time: int | None = None,
    end_time: int | None = None,
    market: str = Query("auto", description="auto, spot, usdm/futures"),
):
    try:
        return binance_rest.fetch_klines(symbol, interval, limit, start_time, end_time, market)
    except ValueError as exc:
        if "interval" in str(exc).lower():
            _value_error_400(exc, supported=binance_rest.BINANCE_KLINE_INTERVALS)
        _value_error_400(exc, supported=binance_rest.BINANCE_KLINE_URLS.keys())
    except binance_rest.BinanceProviderError as exc:
        raise HTTPException(status_code=502, detail={"error": str(exc), "attempts": exc.attempts})


@router.get("/crypto/ohlcv/{provider}/{symbol}")
@router.get("/crypto/ohlcv/{provider}/{symbol}/{interval}")
async def get_crypto_ohlcv(
    provider: str,
    symbol: str,
    interval: str = "1m",
    limit: int = Query(500, ge=1, le=1500),
    start_time: int | None = None,
    end_time: int | None = None,
    market: str = Query("auto", description="Binance only: auto, spot, usdm/futures"),
):
    provider = provider.lower().strip()
    symbol = symbol.upper().strip()

    if provider == "binance":
        return await get_binance_klines(symbol, interval, limit, start_time, end_time, market)

    if provider == "okx":
        try:
            return okx_rest.fetch_candles(symbol, interval, limit, start_time, end_time)
        except ValueError as exc:
            _value_error_400(exc, supported=okx_rest.OKX_INTERVAL_ALIASES)
        except okx_rest.OkxProviderError as exc:
            raise HTTPException(status_code=502, detail=exc.detail or str(exc))

    raise HTTPException(status_code=400, detail=f"Unsupported crypto history provider: {provider}")


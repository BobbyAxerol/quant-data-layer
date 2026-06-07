from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

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


def _symbols_from_batch_body(body: dict[str, Any]) -> list[str]:
    raw_symbols = body.get("symbols") or body.get("symbol") or []
    if isinstance(raw_symbols, str):
        raw_symbols = [item.strip() for item in raw_symbols.split(",")]
    symbols = []
    seen = set()
    for item in raw_symbols:
        symbol = str(item).upper().strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _int_from_body(body: dict[str, Any], key: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(body.get(key, default))
    except Exception:
        raise HTTPException(status_code=400, detail={key: "must_be_integer"})
    if value < minimum or value > maximum:
        raise HTTPException(status_code=400, detail={key: f"must_be_between_{minimum}_and_{maximum}"})
    return value


@router.post("/crypto/ohlcv/{provider}/batch")
async def post_crypto_ohlcv_batch(provider: str, body: dict[str, Any] = Body(...)):
    provider = provider.lower().strip()
    symbols = _symbols_from_batch_body(body)
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols_required")
    if len(symbols) > 100:
        raise HTTPException(status_code=400, detail="too_many_symbols_max_100_use_client_side_chunks")

    interval = str(body.get("interval") or "1m").strip()
    limit = _int_from_body(body, "limit", 500, minimum=1, maximum=1500)
    concurrency = _int_from_body(body, "concurrency", 8, minimum=1, maximum=30)
    start_time = body.get("start_time")
    end_time = body.get("end_time")
    market = str(body.get("market") or "auto").strip()

    if provider == "binance":
        try:
            binance_rest.normalize_interval(interval)
            binance_rest.kline_urls(market)
        except ValueError as exc:
            if "interval" in str(exc).lower():
                _value_error_400(exc, supported=binance_rest.BINANCE_KLINE_INTERVALS)
            _value_error_400(exc, supported=binance_rest.BINANCE_KLINE_URLS.keys())
    elif provider == "okx":
        try:
            okx_rest.normalize_interval(interval)
        except ValueError as exc:
            _value_error_400(exc, supported=okx_rest.OKX_INTERVAL_ALIASES)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported crypto history provider: {provider}")

    semaphore = asyncio.Semaphore(concurrency)
    results: dict[str, dict[str, Any]] = {}
    errors: dict[str, Any] = {}

    async def fetch_one(symbol: str):
        async with semaphore:
            try:
                if provider == "binance":
                    payload = await asyncio.to_thread(
                        binance_rest.fetch_klines,
                        symbol,
                        interval,
                        limit,
                        start_time,
                        end_time,
                        market,
                    )
                else:
                    payload = await asyncio.to_thread(
                        okx_rest.fetch_candles,
                        symbol,
                        interval,
                        limit,
                        start_time,
                        end_time,
                    )
                results[symbol] = payload
            except binance_rest.BinanceProviderError as exc:
                errors[symbol] = {"error": str(exc), "attempts": exc.attempts}
            except okx_rest.OkxProviderError as exc:
                errors[symbol] = exc.detail or {"error": str(exc)}
            except Exception as exc:
                errors[symbol] = {"error": str(exc)}

    await asyncio.gather(*(fetch_one(symbol) for symbol in symbols))
    return {
        "provider": provider,
        "market": "crypto",
        "requested_interval": interval,
        "limit": limit,
        "requested_count": len(symbols),
        "success_count": len(results),
        "error_count": len(errors),
        "partial": bool(errors),
        "results": results,
        "errors": errors,
    }

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.providers.binance import derivatives as binance_derivatives
from app.providers.binance.rest import BINANCE_KLINE_INTERVALS, BinanceProviderError


router = APIRouter(prefix="/v1/binance/futures", tags=["binance-futures"])


def _bad_request(exc: ValueError, *, supported: Any | None = None):
    detail: dict[str, Any] = {"error": str(exc)}
    if supported is not None:
        detail["supported"] = sorted(supported)
    raise HTTPException(status_code=400, detail=detail)


def _provider_error(exc: BinanceProviderError):
    raise HTTPException(status_code=502, detail={"error": str(exc), "attempts": exc.attempts})


@router.get("/exchange-info")
async def get_exchange_info(symbol: str | None = None):
    try:
        return binance_derivatives.fetch_exchange_info(symbol)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/klines/{symbol}")
async def get_derivative_klines(
    symbol: str,
    interval: str = Query("1d", description="Binance kline interval"),
    limit: int = Query(30, ge=1, le=1500),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_klines(symbol, interval, limit, start_time, end_time)
    except ValueError as exc:
        _bad_request(exc, supported=BINANCE_KLINE_INTERVALS)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/depth/{symbol}")
async def get_derivative_depth(symbol: str, limit: int = Query(5)):
    try:
        return binance_derivatives.fetch_depth(symbol, limit)
    except ValueError as exc:
        _bad_request(exc)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/open-interest/{symbol}")
async def get_open_interest(symbol: str):
    try:
        return binance_derivatives.fetch_open_interest(symbol)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/open-interest-history/{symbol}")
async def get_open_interest_history(
    symbol: str,
    period: str = Query("1d"),
    limit: int = Query(30, ge=1, le=500),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_metric_history(
            "open_interest_hist",
            symbol,
            period,
            limit,
            start_time,
            end_time,
        )
    except ValueError as exc:
        _bad_request(exc, supported=binance_derivatives.BINANCE_DERIVATIVE_PERIODS)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/long-short/{kind}/{symbol}")
async def get_long_short_ratio(
    kind: str,
    symbol: str,
    period: str = Query("1d"),
    limit: int = Query(30, ge=1, le=500),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_long_short_ratio(
            kind,
            symbol,
            period,
            limit,
            start_time,
            end_time,
        )
    except ValueError as exc:
        _bad_request(exc, supported=binance_derivatives.LONG_SHORT_KIND_TO_ENDPOINT)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/taker-long-short/{symbol}")
async def get_taker_long_short_ratio(
    symbol: str,
    period: str = Query("1d"),
    limit: int = Query(30, ge=1, le=500),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_taker_long_short_ratio(
            symbol,
            period,
            limit,
            start_time,
            end_time,
        )
    except ValueError as exc:
        _bad_request(exc, supported=binance_derivatives.BINANCE_DERIVATIVE_PERIODS)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/funding-rate/{symbol}")
async def get_funding_rate(
    symbol: str,
    limit: int = Query(100, ge=1, le=1000),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_funding_rate(symbol, limit, start_time, end_time)
    except ValueError as exc:
        _bad_request(exc)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.get("/basis/{pair}")
async def get_basis(
    pair: str,
    contract_type: str = Query("CURRENT_QUARTER"),
    period: str = Query("1d"),
    limit: int = Query(30, ge=1, le=500),
    start_time: int | None = None,
    end_time: int | None = None,
):
    try:
        return binance_derivatives.fetch_basis(
            pair,
            contract_type,
            period,
            limit,
            start_time,
            end_time,
        )
    except ValueError as exc:
        _bad_request(exc)
    except BinanceProviderError as exc:
        _provider_error(exc)


@router.post("/basis-bundle")
async def post_basis_bundle(body: dict[str, Any] = Body(...)):
    try:
        perp_symbol = str(body["perp_symbol"])
        delivery_symbol = str(body["delivery_symbol"])
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"missing_required_field:{exc.args[0]}")

    try:
        return binance_derivatives.fetch_basis_bundle(
            perp_symbol=perp_symbol,
            delivery_symbol=delivery_symbol,
            pair=body.get("pair"),
            interval=str(body.get("interval") or "1d"),
            period=str(body.get("period") or "1d"),
            limit=int(body.get("limit") or 30),
            include_depth=bool(body.get("include_depth", True)),
            depth_limit=int(body.get("depth_limit") or 5),
            contract_type=str(body.get("contract_type") or "CURRENT_QUARTER"),
            start_time=body.get("start_time"),
            end_time=body.get("end_time"),
        )
    except ValueError as exc:
        _bad_request(exc)

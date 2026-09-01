#!/usr/bin/env python3
"""Bounded, read-only provider admission for every declared crypto demand slice."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Callable, Iterable, Mapping

import requests
import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEMAND_PATH = ROOT / "config/v2/stable-crypto-demand.yaml"
# This is an operator read-only admission ceiling, not a legacy sample size.
# Keep it aligned with the public V2 bounded warmup/query contract so a sealed
# multi-venue demand manifest is never rejected solely because it grew beyond
# the Phase 10 six-slice fixture.
MAX_SLICES = 10_000


class ProviderAdmissionError(RuntimeError):
    """A provider response is absent, malformed, stale, or semantically unsafe."""


@dataclass(frozen=True, slots=True)
class DemandSlice:
    consumer_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    feed: str
    interval: str | None
    source_policy_id: str

    @property
    def key(self) -> str:
        return ":".join(
            (
                self.venue,
                self.market,
                self.product_type,
                self.native_symbol,
                self.feed,
                self.interval or "",
                self.source_policy_id,
            )
        )


def _positive_decimal(value: Any, field: str) -> Decimal:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ProviderAdmissionError(f"{field} is not a decimal") from error
    if not decimal.is_finite() or decimal <= 0:
        raise ProviderAdmissionError(f"{field} must be positive")
    return decimal


def _timestamp_ms(value: Any, field: str) -> int:
    try:
        result = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderAdmissionError(f"{field} is not an integer timestamp") from error
    if result <= 0:
        raise ProviderAdmissionError(f"{field} must be positive")
    return result


def _load_slices(path: Path) -> tuple[DemandSlice, ...]:
    raw = yaml.safe_load(path.read_bytes())
    if not isinstance(raw, Mapping) or raw.get("schema") != "qdl.v2.production-demand.v1":
        raise ProviderAdmissionError("only qdl.v2.production-demand.v1 demand is supported")
    consumers = raw.get("consumers")
    if not isinstance(consumers, list) or not consumers:
        raise ProviderAdmissionError("demand has no consumers")
    slices: dict[str, DemandSlice] = {}
    for consumer in consumers:
        if not isinstance(consumer, Mapping):
            raise ProviderAdmissionError("consumer is not a mapping")
        consumer_id = str(consumer.get("consumer_id") or "").strip()
        requirements = consumer.get("requirements")
        if not consumer_id or not isinstance(requirements, list):
            raise ProviderAdmissionError("consumer identity or requirements are missing")
        for requirement in requirements:
            if not isinstance(requirement, Mapping):
                raise ProviderAdmissionError("requirement is not a mapping")
            try:
                item = DemandSlice(
                    consumer_id=consumer_id,
                    venue=str(requirement["venue"]).upper(),
                    market=str(requirement["market"]).upper(),
                    product_type=str(requirement["product_type"]).upper(),
                    native_symbol=str(requirement["native_symbol"]).upper(),
                    feed=str(requirement["feed"]).upper(),
                    interval=(str(requirement["interval"]) if requirement.get("interval") else None),
                    source_policy_id=str(requirement["source_policy_id"]),
                )
            except KeyError as error:
                raise ProviderAdmissionError(f"requirement is missing {error.args[0]}") from error
            if item.venue not in {"BINANCE", "OKX"}:
                continue
            if item.feed not in {
                "TRADE", "QUOTE", "BAR", "BOOK_SNAPSHOT", "BOOK_DELTA",
            }:
                raise ProviderAdmissionError(f"unsupported Phase 10.1 feed: {item.feed}")
            slices[item.key] = item
    values = tuple(sorted(slices.values(), key=lambda item: item.key))
    if not values or len(values) > MAX_SLICES:
        raise ProviderAdmissionError("bounded crypto demand slice count is invalid")
    return values


def _endpoint(slice_: DemandSlice) -> tuple[str, dict[str, str]]:
    symbol = slice_.native_symbol
    if slice_.venue == "BINANCE":
        base = "https://fapi.binance.com/fapi/v1" if slice_.market == "USDM" else "https://api.binance.com/api/v3"
        if slice_.market not in {"USDM", "SPOT"}:
            raise ProviderAdmissionError(f"unsupported Binance market: {slice_.market}")
        if slice_.feed == "TRADE":
            return f"{base}/trades", {"symbol": symbol, "limit": "1"}
        if slice_.feed == "QUOTE":
            return f"{base}/ticker/bookTicker", {"symbol": symbol}
        if slice_.feed in {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
            # REST can only attest the current L2 snapshot. Delta continuity
            # remains a Rust WebSocket/core responsibility, but both public
            # products must prove the same venue-owned depth source exists.
            return f"{base}/depth", {"symbol": symbol, "limit": "100"}
        return f"{base}/klines", {"symbol": symbol, "interval": slice_.interval or "", "limit": "3"}
    if slice_.venue == "OKX":
        if slice_.market not in {"SWAP", "SPOT"}:
            raise ProviderAdmissionError(f"unsupported OKX market: {slice_.market}")
        base = "https://www.okx.com/api/v5/market"
        if slice_.feed == "TRADE":
            return f"{base}/trades", {"instId": symbol, "limit": "1"}
        if slice_.feed == "QUOTE":
            return f"{base}/books", {"instId": symbol, "sz": "1"}
        if slice_.feed in {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
            return f"{base}/books", {"instId": symbol, "sz": "100"}
        return f"{base}/candles", {"instId": symbol, "bar": slice_.interval or "", "limit": "3"}
    raise ProviderAdmissionError(f"unsupported venue: {slice_.venue}")


def _first_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, list) or not value or not isinstance(value[0], Mapping):
        raise ProviderAdmissionError(f"{field} must contain one object")
    return value[0]


def _validate_binance(slice_: DemandSlice, payload: Any, received_ms: int) -> int:
    if slice_.feed == "TRADE":
        row = _first_mapping(payload, "binance trades")
        _positive_decimal(row.get("price"), "binance trade price")
        _positive_decimal(row.get("qty"), "binance trade quantity")
        if row.get("id") is None:
            raise ProviderAdmissionError("binance trade id is missing")
        return _timestamp_ms(row.get("time"), "binance trade time")
    if slice_.feed == "QUOTE":
        if not isinstance(payload, Mapping):
            raise ProviderAdmissionError("binance quote must be an object")
        _positive_decimal(payload.get("bidPrice"), "binance bid price")
        _positive_decimal(payload.get("askPrice"), "binance ask price")
        return received_ms
    if slice_.feed in {"BOOK_SNAPSHOT", "BOOK_DELTA"}:
        if not isinstance(payload, Mapping):
            raise ProviderAdmissionError("binance depth must be an object")
        try:
            update_id = int(str(payload.get("lastUpdateId")))
        except (TypeError, ValueError) as error:
            raise ProviderAdmissionError("binance depth update id is invalid") from error
        if update_id <= 0:
            raise ProviderAdmissionError("binance depth update id must be positive")
        for side in ("bids", "asks"):
            levels = payload.get(side)
            if not isinstance(levels, list) or not levels or not isinstance(levels[0], list):
                raise ProviderAdmissionError(f"binance depth has no {side}")
            if len(levels[0]) < 2:
                raise ProviderAdmissionError(f"binance depth {side} level is incomplete")
            _positive_decimal(levels[0][0], f"binance depth {side} price")
            _positive_decimal(levels[0][1], f"binance depth {side} quantity")
        return received_ms
    if not isinstance(payload, list) or not payload:
        raise ProviderAdmissionError("binance klines must contain rows")
    closed = [row for row in payload if isinstance(row, list) and len(row) >= 7 and _timestamp_ms(row[6], "binance kline close time") < received_ms]
    if not closed:
        raise ProviderAdmissionError("binance returned no closed requested bar")
    row = closed[-1]
    for index, name in ((1, "open"), (2, "high"), (3, "low"), (4, "close"), (5, "volume")):
        _positive_decimal(row[index], f"binance kline {name}")
    return _timestamp_ms(row[6], "binance kline close time")


def _okx_data(payload: Any, field: str) -> list[Any]:
    if not isinstance(payload, Mapping) or str(payload.get("code")) != "0":
        raise ProviderAdmissionError(f"{field} response code is not 0")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise ProviderAdmissionError(f"{field} has no data")
    return data


def _validate_okx(slice_: DemandSlice, payload: Any, received_ms: int) -> int:
    data = _okx_data(payload, "okx")
    if slice_.feed == "TRADE":
        row = _first_mapping(data, "okx trades")
        _positive_decimal(row.get("px"), "okx trade price")
        _positive_decimal(row.get("sz"), "okx trade size")
        if not str(row.get("tradeId") or "").strip():
            raise ProviderAdmissionError("okx trade id is missing")
        return _timestamp_ms(row.get("ts"), "okx trade time")
    if slice_.feed in {"QUOTE", "BOOK_SNAPSHOT", "BOOK_DELTA"}:
        row = _first_mapping(data, "okx books")
        bids, asks = row.get("bids"), row.get("asks")
        if not isinstance(bids, list) or not bids or not isinstance(asks, list) or not asks:
            raise ProviderAdmissionError("okx quote has no bid/ask")
        _positive_decimal(bids[0][0], "okx bid price")
        _positive_decimal(asks[0][0], "okx ask price")
        return _timestamp_ms(row.get("ts"), "okx book time")
    closed = [row for row in data if isinstance(row, list) and len(row) >= 9 and str(row[-1]) == "1"]
    if not closed:
        raise ProviderAdmissionError("okx returned no closed requested bar")
    row = closed[-1]
    for index, name in ((1, "open"), (2, "high"), (3, "low"), (4, "close"), (5, "volume")):
        _positive_decimal(row[index], f"okx kline {name}")
    timestamp = _timestamp_ms(row[0], "okx kline time")
    if timestamp >= received_ms:
        raise ProviderAdmissionError("okx closed kline time is not before receipt")
    return timestamp


def _validate(slice_: DemandSlice, payload: Any, received_ms: int) -> int:
    if slice_.venue == "BINANCE":
        return _validate_binance(slice_, payload, received_ms)
    if slice_.venue == "OKX":
        return _validate_okx(slice_, payload, received_ms)
    raise ProviderAdmissionError(f"unsupported venue: {slice_.venue}")


def run(
    demand_path: Path,
    *,
    timeout_seconds: float,
    get: Callable[..., requests.Response] = requests.get,
) -> dict[str, Any]:
    slices = _load_slices(demand_path)
    results: list[dict[str, Any]] = []
    for slice_ in slices:
        url, params = _endpoint(slice_)
        received_ms = int(time.time() * 1_000)
        response = get(
            url,
            params=params,
            timeout=timeout_seconds,
            headers={"User-Agent": "qdl-phase10-read-only-admission/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        provider_time_ms = _validate(slice_, payload, received_ms)
        results.append(
            {
                "slice": slice_.key,
                "endpoint_class": f"{slice_.venue}:{slice_.market}:{slice_.feed}",
                "provider_time_ms": provider_time_ms,
                "received_at_ms": received_ms,
                "payload_sha256": hashlib.sha256(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            }
        )
    return {
        "schema": "qdl.phase10.provider-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "demand_path": str(demand_path.resolve()),
        "slice_count": len(results),
        "production_writes": 0,
        "slices": results,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand-file", type=Path, default=DEFAULT_DEMAND_PATH)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not 1 <= args.timeout_seconds <= 30:
        raise SystemExit("--timeout-seconds must be between 1 and 30")
    try:
        report = run(args.demand_file, timeout_seconds=args.timeout_seconds)
    except (ProviderAdmissionError, requests.RequestException) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    rendered = json.dumps(report, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

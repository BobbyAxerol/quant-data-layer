#!/usr/bin/env python3
"""Bounded, read-only provider admission for active Phase 10.3 feeds.

This verifier intentionally talks only to public provider edges. It exercises
the direct-control WebSocket protocol used by the Rust native ingestor and the
approved Binance/OKX REST closed-BAR recovery edges without starting a
producer, a projector, a consumer group, or any Data Layer service. Its output
contains bounded metadata and frame hashes, not raw market data.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import resource
import time
from typing import Any, Iterable, Mapping

from websockets.asyncio.client import connect

from qdl.adapters.binance import (
    BinanceBarRawBinding,
    fetch_latest_closed_bar_raw_envelope,
)
from qdl.adapters.intervals import canonical_interval_ms, okx_candle_channel
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_latest_closed_bar_raw_envelope as fetch_okx_latest_closed_bar_raw_envelope,
)
from qdl.provider.v1 import raw_provider_pb2
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
_MAX_PRE_ACK_FRAMES = 128
_HEARTBEAT_SECONDS = 10.0

# Phase 10.3 is retained as a reproducible, historical provider-protocol
# baseline. Universal five-symbol/L2 admission belongs to the later Phase
# 11.x verifiers; silently widening this script would change the meaning of
# its recorded evidence and ask its BTC/ETH-only websocket parser to certify
# products it was never designed to own.
_PHASE10_BASELINE_BINDING_IDS = frozenset(
    {
        "binance-usdm-btcusdt-trade",
        "binance-usdm-btcusdt-quote",
        "binance-usdm-btcusdt-bar-1m",
        "binance-usdm-ethusdt-trade",
        "binance-usdm-ethusdt-quote",
        "binance-usdm-ethusdt-bar-1m",
        "okx-swap-btcusdt-trade",
        "okx-swap-btcusdt-quote",
        "okx-swap-btcusdt-bar-1m",
        "okx-swap-eth-usdt-swap-trade",
        "okx-swap-eth-usdt-swap-quote",
        "okx-swap-eth-usdt-swap-bar-1m",
    }
)


class ProviderAdmissionError(RuntimeError):
    """A public provider did not meet the strict realtime admission contract."""


@dataclass(frozen=True, slots=True)
class NativeBinding:
    binding_id: str
    venue: str
    market: str
    product_type: str
    native_symbol: str
    native_channel: str
    feed: str
    interval: str | None
    stale_after_ms: int
    mode: str
    websocket_url: str
    business_websocket_url: str | None

    @property
    def key(self) -> tuple[str, str]:
        return self.native_channel, self.native_symbol


@dataclass(frozen=True, slots=True)
class FrameObservation:
    binding_id: str
    source_time_ms: int
    frame_sha256: str
    final_bar: bool
    observed_at_ms: int = 0
    source_time_missing: bool = False


@dataclass(frozen=True, slots=True)
class SessionEvidence:
    role: str
    generation: int
    ack_count: int
    pre_ack_frames: int
    event_count: int
    observations: tuple[FrameObservation, ...]
    filtered_status_frames: int = 0
    transport: str = "WEBSOCKET"
    final_bar_binding_ids: tuple[str, ...] = ()


class _SessionAccumulator:
    def __init__(self, bindings: tuple[NativeBinding, ...]) -> None:
        self._expected = {item.binding_id for item in bindings}
        self._bar_ids = {item.binding_id for item in bindings if item.feed == "BAR"}
        self._observations: dict[str, FrameObservation] = {}
        self._final_bar_ids: set[str] = set()
        self.event_count = 0

    def add(self, observation: FrameObservation) -> None:
        if observation.binding_id not in self._expected:
            raise ProviderAdmissionError("provider frame resolved outside requested bindings")
        self.event_count += 1
        self._observations[observation.binding_id] = observation
        if observation.final_bar:
            self._final_bar_ids.add(observation.binding_id)

    def complete(self, *, require_final_bars: bool) -> bool:
        if set(self._observations) != self._expected:
            return False
        return not require_final_bars or self._bar_ids <= self._final_bar_ids

    def missing_binding_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._expected - set(self._observations)))

    def missing_final_bar_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._bar_ids - self._final_bar_ids))

    def evidence(
        self,
        *,
        role: str,
        generation: int,
        ack_count: int,
        pre_ack_frames: int,
        filtered_status_frames: int = 0,
    ) -> SessionEvidence:
        return SessionEvidence(
            role=role,
            generation=generation,
            ack_count=ack_count,
            pre_ack_frames=pre_ack_frames,
            event_count=self.event_count,
            observations=tuple(
                self._observations[binding_id]
                for binding_id in sorted(self._observations)
            ),
            filtered_status_frames=filtered_status_frames,
            final_bar_binding_ids=tuple(sorted(self._final_bar_ids)),
        )


def _positive_decimal(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ProviderAdmissionError(f"{field} is not a decimal") from error
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise ProviderAdmissionError(f"{field} is outside the positive decimal domain")
    return result


def _timestamp_ms(value: Any, field: str) -> int:
    try:
        result = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderAdmissionError(f"{field} is not an integer timestamp") from error
    if result <= 0:
        raise ProviderAdmissionError(f"{field} must be positive")
    return result


def _payload(raw: str | bytes) -> tuple[str, Mapping[str, Any]]:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ProviderAdmissionError("provider WebSocket frame is not UTF-8") from error
    if not isinstance(raw, str):
        raise ProviderAdmissionError("provider WebSocket frame is not text")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProviderAdmissionError("provider WebSocket frame is not JSON") from error
    if not isinstance(payload, Mapping):
        raise ProviderAdmissionError("provider WebSocket frame is not an object")
    return raw, payload


def load_active_provider_bindings(
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    acquisition_path: Path = DEFAULT_ACQUISITION_PATH,
) -> tuple[NativeBinding, ...]:
    """Resolve the recorded twelve-binding Phase 10.3 provider baseline.

    `RUST_NATIVE` bindings are read from direct provider WebSockets. A
    `PYTHON_REST` binding is allowed only for a Binance/OKX final BAR and
    remains a provider edge: its captured raw envelope is still normalized by
    the Rust canonical core in the real deployment.
    """
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    sources = {item.binding_id: item for item in catalog.bindings}
    values: list[NativeBinding] = []
    for item in acquisition.bindings:
        if item.binding_id not in _PHASE10_BASELINE_BINDING_IDS:
            continue
        if not item.enabled or item.mode not in {"RUST_NATIVE", "PYTHON_REST"}:
            continue
        source = sources[item.binding_id]
        identity = source.instrument.identity
        if item.runtime not in {"BINANCE", "OKX"}:
            raise ProviderAdmissionError("unexpected non-crypto provider runtime")
        if identity.venue != item.runtime:
            raise ProviderAdmissionError("provider runtime and catalog venue differ")
        if item.mode == "PYTHON_REST":
            expected_provider_kinds = {
                "BINANCE": {"binance_usdm_rest_bar", "binance_spot_rest_bar"},
                "OKX": {"okx_bar"},
            }
            if (
                source.feed.value != "BAR"
                or item.provider_kind not in expected_provider_kinds[item.runtime]
            ):
                raise ProviderAdmissionError(
                    "Phase 10.3 REST acquisition is only venue-owned final BAR"
                )
        values.append(
            NativeBinding(
                binding_id=item.binding_id,
                venue=identity.venue,
                market=identity.market,
                product_type=identity.product_type.value,
                native_symbol=source.instrument.native_symbol,
                native_channel=item.native_channel,
                feed=source.feed.value,
                interval=source.interval,
                stale_after_ms=source.stale_after_ms,
                mode=item.mode,
                websocket_url=item.websocket_url or "",
                business_websocket_url=item.business_websocket_url,
            )
        )
    result = tuple(sorted(values, key=lambda item: item.binding_id))
    result_ids = {item.binding_id for item in result}
    if result_ids != _PHASE10_BASELINE_BINDING_IDS:
        missing = sorted(_PHASE10_BASELINE_BINDING_IDS - result_ids)
        unexpected = sorted(result_ids - _PHASE10_BASELINE_BINDING_IDS)
        raise ProviderAdmissionError(
            "Phase 10.3 baseline bindings are missing or changed "
            f"missing={','.join(missing) or 'none'} "
            f"unexpected={','.join(unexpected) or 'none'}"
        )
    if {item.venue for item in result} != {"BINANCE", "OKX"}:
        raise ProviderAdmissionError("active provider demand must include Binance and OKX")
    if any(item.market not in {"USDM", "SWAP"} for item in result):
        raise ProviderAdmissionError("disabled Spot capability leaked into active provider demand")
    return result


def load_active_native_bindings(
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    acquisition_path: Path = DEFAULT_ACQUISITION_PATH,
) -> tuple[NativeBinding, ...]:
    """Compatibility helper for checks concerned only with native sockets."""
    return tuple(
        item
        for item in load_active_provider_bindings(
            catalog_path=catalog_path,
            acquisition_path=acquisition_path,
        )
        if item.mode == "RUST_NATIVE"
    )


def _binding_lookup(bindings: Iterable[NativeBinding]) -> dict[tuple[str, str], NativeBinding]:
    values = tuple(bindings)
    result = {item.key: item for item in values}
    if len(result) != len(values):
        raise ProviderAdmissionError("native binding keys are not unique")
    return result


def okx_request_id(*, role: str, generation: int) -> str:
    """Return a provider-safe, bounded request id for one public/business role."""
    suffixes = {
        "OKX:SWAP:PUBLIC": "p",
        "OKX:SWAP:BUSINESS": "b",
    }
    if generation <= 0 or role not in suffixes:
        raise ProviderAdmissionError("OKX request identity is invalid")
    # OKX rejected punctuation in a real public admission (60033). Keep the
    # ID alphanumeric and short; it remains correlated to role/generation.
    return f"qdl103{generation}{suffixes[role]}"


def binance_request_id(*, role: str, generation: int) -> int:
    """Return a per-lane direct-control request id for Binance."""
    suffixes = {
        "BINANCE:USDM:BAR": 1,
        "BINANCE:USDM:TRADE": 2,
        "BINANCE:USDM:QUOTE": 3,
    }
    if generation <= 0 or role not in suffixes:
        raise ProviderAdmissionError("Binance request identity is invalid")
    return 10_000 + generation * 10 + suffixes[role]


def binance_admission_lanes(
    bindings: tuple[NativeBinding, ...],
) -> dict[str, tuple[NativeBinding, ...]]:
    """Split one Binance worker into deterministic feed lanes, never symbols."""
    if any(
        item.venue != "BINANCE"
        or item.market != "USDM"
        or item.mode != "RUST_NATIVE"
        for item in bindings
    ):
        raise ProviderAdmissionError("Binance admission lane identity is invalid")
    lanes = {
        feed: tuple(item for item in bindings if item.feed == feed)
        for feed in ("BAR", "TRADE", "QUOTE")
    }
    if sum(len(items) for items in lanes.values()) != len(bindings):
        raise ProviderAdmissionError("Binance admission lanes are incomplete or ambiguous")
    return lanes


def is_binance_trade_status_frame(payload: Mapping[str, Any]) -> bool:
    """Mirror the Rust core's narrow non-canonical Binance status-frame rule.

    Binance may send this exact zero-price/quantity `trade` status frame.  It
    is not a fill and must not make provider admission fail.  Every other zero
    or malformed trade remains invalid, including string-typed `st`, so this
    verifier stays aligned with the Rust core's JSON type checks.
    """
    status = payload.get("st")
    return (
        payload.get("e") == "trade"
        and isinstance(payload.get("p"), str)
        and payload.get("p") == "0"
        and isinstance(payload.get("q"), str)
        and payload.get("q") == "0"
        and isinstance(payload.get("X"), str)
        and payload.get("X") == "NA"
        and isinstance(status, int)
        and not isinstance(status, bool)
        and status == 1
    )


def is_binance_direct_book_ticker_frame(payload: Mapping[str, Any]) -> bool:
    """Recognise the documented direct Binance BBO shape without `e`.

    The inference is deliberately structural and complete. It aligns the
    older provider verifier with the Rust direct-session decoder without
    treating arbitrary control objects as market data.
    """
    return (
        isinstance(payload.get("u"), int)
        and not isinstance(payload.get("u"), bool)
        and payload["u"] >= 0
        and all(
            isinstance(payload.get(field), str) and bool(payload[field])
            for field in ("b", "B", "a", "A")
        )
    )


def parse_binance_data(
    raw: str | bytes,
    *,
    bindings: Mapping[tuple[str, str], NativeBinding],
) -> FrameObservation:
    text, payload = _payload(raw)
    observed_at_ms = time.time_ns() // 1_000_000
    event = str(payload.get("e") or "")
    if not event and is_binance_direct_book_ticker_frame(payload):
        event = "bookTicker"
    symbol = str(payload.get("s") or "").upper()
    if not symbol:
        raise ProviderAdmissionError("Binance frame has no symbol")
    if event == "trade":
        channel = f"{symbol.lower()}@trade"
        try:
            _positive_decimal(payload.get("p"), "Binance trade price")
            _positive_decimal(payload.get("q"), "Binance trade quantity")
        except ProviderAdmissionError as error:
            raise ProviderAdmissionError(
                f"{error} symbol={symbol} price={payload.get('p')!r} quantity={payload.get('q')!r}"
            ) from error
        source_time_ms = _timestamp_ms(payload.get("T"), "Binance trade time")
        final_bar = False
        source_time_missing = False
    elif event == "bookTicker":
        channel = f"{symbol.lower()}@bookTicker"
        _positive_decimal(payload.get("b"), "Binance bid price")
        _positive_decimal(payload.get("B"), "Binance bid quantity")
        _positive_decimal(payload.get("a"), "Binance ask price")
        _positive_decimal(payload.get("A"), "Binance ask quantity")
        provider_time = payload.get("T")
        if provider_time is None:
            provider_time = payload.get("E")
        source_time_missing = provider_time is None
        source_time_ms = (
            observed_at_ms
            if source_time_missing
            else _timestamp_ms(provider_time, "Binance quote event time")
        )
        final_bar = False
    elif event == "kline":
        kline = payload.get("k")
        if not isinstance(kline, Mapping):
            raise ProviderAdmissionError("Binance kline payload is missing")
        interval = str(kline.get("i") or "")
        if not interval:
            raise ProviderAdmissionError("Binance kline interval is missing")
        channel = f"{symbol.lower()}@kline_{interval}"
        for key, label in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")):
            _positive_decimal(kline.get(key), f"Binance kline {label}")
        _positive_decimal(kline.get("v"), "Binance kline volume", allow_zero=True)
        source_time_ms = _timestamp_ms(kline.get("T"), "Binance kline close time")
        final_bar = bool(kline.get("x"))
        source_time_missing = False
    else:
        raise ProviderAdmissionError(f"unsupported Binance native event: {event or 'missing'}")
    binding = bindings.get((channel, symbol))
    if binding is None or binding.feed != {"trade": "TRADE", "bookTicker": "QUOTE", "kline": "BAR"}[event]:
        raise ProviderAdmissionError("Binance frame has no matching active binding")
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=source_time_ms,
        frame_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        final_bar=final_bar,
        observed_at_ms=observed_at_ms,
        source_time_missing=source_time_missing,
    )


def parse_binance_rest_bar(
    payload: Mapping[str, Any],
    *,
    binding: NativeBinding,
    raw_frame_bytes: bytes,
    observed_ms: int,
) -> FrameObservation:
    """Validate one provider-authentic, fully closed Binance REST BAR.

    The caller has already verified the signed raw capture envelope.  This
    parser deliberately checks the native row again because this evidence is
    the admission boundary that permits the Python provider edge to hand raw
    bytes to the Rust canonical core.
    """
    if (
        binding.mode != "PYTHON_REST"
        or binding.venue != "BINANCE"
        or binding.feed != "BAR"
        or binding.interval is None
        or binding.native_channel != f"rest-klines/{binding.interval}"
    ):
        raise ProviderAdmissionError("Binance REST BAR binding identity is invalid")
    if str(payload.get("symbol") or "").upper() != binding.native_symbol:
        raise ProviderAdmissionError("Binance REST BAR symbol differs from binding")
    if str(payload.get("interval") or "") != binding.interval:
        raise ProviderAdmissionError("Binance REST BAR interval differs from binding")
    if str(payload.get("bar_origin") or "").upper() != "VENUE_NATIVE":
        raise ProviderAdmissionError("Binance REST BAR must be venue-native")
    row = payload.get("row")
    if not isinstance(row, list) or len(row) < 11:
        raise ProviderAdmissionError("Binance REST BAR row is incomplete")
    open_time_ms = _timestamp_ms(row[0], "Binance REST BAR open time")
    close_time_ms = _timestamp_ms(row[6], "Binance REST BAR close time")
    if close_time_ms >= observed_ms:
        raise ProviderAdmissionError("Binance REST BAR is not closed at observation time")
    interval_ms = canonical_interval_ms(binding.interval)
    if close_time_ms != open_time_ms + interval_ms - 1:
        raise ProviderAdmissionError("Binance REST BAR boundary differs from interval")
    for index, label in ((1, "open"), (2, "high"), (3, "low"), (4, "close")):
        _positive_decimal(row[index], f"Binance REST BAR {label}")
    _positive_decimal(row[5], "Binance REST BAR base volume", allow_zero=True)
    _positive_decimal(row[7], "Binance REST BAR quote volume", allow_zero=True)
    try:
        trade_count = int(str(row[8]))
    except (TypeError, ValueError) as error:
        raise ProviderAdmissionError("Binance REST BAR trade count is invalid") from error
    if trade_count < 0:
        raise ProviderAdmissionError("Binance REST BAR trade count is negative")
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=close_time_ms,
        frame_sha256=hashlib.sha256(raw_frame_bytes).hexdigest(),
        final_bar=True,
        observed_at_ms=observed_ms,
    )


def parse_okx_data(
    raw: str | bytes,
    *,
    bindings: Mapping[tuple[str, str], NativeBinding],
) -> FrameObservation:
    text, payload = _payload(raw)
    if payload.get("event") is not None:
        raise ProviderAdmissionError("OKX control frame cannot be admitted as data")
    argument = payload.get("arg")
    if not isinstance(argument, Mapping):
        raise ProviderAdmissionError("OKX data frame has no arg")
    channel = str(argument.get("channel") or "")
    instrument = str(argument.get("instId") or "").upper()
    binding = bindings.get((channel, instrument))
    if binding is None:
        raise ProviderAdmissionError("OKX frame has no matching active binding")
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise ProviderAdmissionError("OKX data frame has no rows")
    first = rows[0]
    final_bar = False
    if binding.feed == "TRADE":
        if not isinstance(first, Mapping):
            raise ProviderAdmissionError("OKX trade row is not an object")
        if str(first.get("instId") or "").upper() != instrument:
            raise ProviderAdmissionError("OKX trade row instrument differs from subscription")
        if not str(first.get("tradeId") or "").strip():
            raise ProviderAdmissionError("OKX trade id is missing")
        _positive_decimal(first.get("px"), "OKX trade price")
        _positive_decimal(first.get("sz"), "OKX trade quantity")
        source_time_ms = _timestamp_ms(first.get("ts"), "OKX trade time")
    elif binding.feed == "QUOTE":
        if not isinstance(first, Mapping):
            raise ProviderAdmissionError("OKX BBO row is not an object")
        for side in ("bids", "asks"):
            levels = first.get(side)
            if (
                not isinstance(levels, list)
                or len(levels) != 1
                or not isinstance(levels[0], list)
                or len(levels[0]) < 2
            ):
                raise ProviderAdmissionError(f"OKX BBO requires one {side} level")
            _positive_decimal(levels[0][0], f"OKX {side} price")
            _positive_decimal(levels[0][1], f"OKX {side} quantity")
        if not str(first.get("seqId") or "").strip():
            raise ProviderAdmissionError("OKX BBO sequence is missing")
        source_time_ms = _timestamp_ms(first.get("ts"), "OKX quote time")
    elif binding.feed == "BAR":
        if not isinstance(first, list) or len(first) < 9:
            raise ProviderAdmissionError("OKX candle row is incomplete")
        for index, label in ((1, "open"), (2, "high"), (3, "low"), (4, "close")):
            _positive_decimal(first[index], f"OKX candle {label}")
        _positive_decimal(first[5], "OKX candle volume", allow_zero=True)
        open_time_ms = _timestamp_ms(first[0], "OKX candle open time")
        if binding.interval != "1m":
            raise ProviderAdmissionError("Phase 10.3 OKX BAR interval is not 1m")
        source_time_ms = open_time_ms + 60_000
        final_bar = str(first[8]) == "1"
    else:
        raise ProviderAdmissionError("OKX active binding has unsupported feed")
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=source_time_ms,
        frame_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        final_bar=final_bar,
        observed_at_ms=time.time_ns() // 1_000_000,
    )


async def _recv_with_heartbeat(socket) -> str | bytes | None:
    try:
        return await asyncio.wait_for(socket.recv(), timeout=_HEARTBEAT_SECONDS)
    except TimeoutError:
        pong_waiter = await socket.ping()
        await asyncio.wait_for(pong_waiter, timeout=_HEARTBEAT_SECONDS)
        return None


async def _binance_session(
    bindings: tuple[NativeBinding, ...],
    *,
    role: str,
    generation: int,
    timeout_seconds: float,
    require_final_bars: bool,
) -> SessionEvidence:
    if not bindings:
        raise ProviderAdmissionError("Binance native session has no bindings")
    urls = {item.websocket_url for item in bindings}
    if len(urls) != 1:
        raise ProviderAdmissionError("Binance bindings disagree on control WebSocket URL")
    request_id = binance_request_id(role=role, generation=generation)
    command = {
        "method": "SUBSCRIBE",
        "params": [item.native_channel for item in bindings],
        "id": request_id,
    }
    lookup = _binding_lookup(bindings)
    accumulator = _SessionAccumulator(bindings)
    acknowledged = False
    pre_ack: list[FrameObservation] = []
    pre_ack_count = 0
    filtered_status_frames = 0
    started = time.monotonic()
    async with connect(next(iter(urls)), ping_interval=None, ping_timeout=None, open_timeout=10) as socket:
        await socket.send(json.dumps(command, separators=(",", ":")))
        while time.monotonic() - started < timeout_seconds:
            raw = await _recv_with_heartbeat(socket)
            if raw is None:
                continue
            text, payload = _payload(raw)
            if "id" in payload:
                if payload.get("id") != request_id:
                    raise ProviderAdmissionError("Binance subscription ACK id mismatch")
                if payload.get("result") is not None or payload.get("code") is not None:
                    raise ProviderAdmissionError("Binance subscription was rejected")
                if acknowledged:
                    raise ProviderAdmissionError("Binance subscription ACK was duplicated")
                acknowledged = True
                pre_ack_count = len(pre_ack)
                for item in pre_ack:
                    accumulator.add(item)
                pre_ack.clear()
            else:
                if is_binance_trade_status_frame(payload):
                    # This is intentionally not a canonical trade.  The exact
                    # same narrow rule is tested in qdl-realtime-core.
                    filtered_status_frames += 1
                    continue
                observation = parse_binance_data(text, bindings=lookup)
                if acknowledged:
                    accumulator.add(observation)
                else:
                    if len(pre_ack) >= _MAX_PRE_ACK_FRAMES:
                        raise ProviderAdmissionError("Binance pre-ACK data buffer exceeded bound")
                    pre_ack.append(observation)
            if acknowledged and accumulator.complete(require_final_bars=require_final_bars):
                return accumulator.evidence(
                    role=role,
                    generation=generation,
                    ack_count=1,
                    pre_ack_frames=pre_ack_count,
                    filtered_status_frames=filtered_status_frames,
                )
    raise ProviderAdmissionError(
        "Binance session did not satisfy all demanded bindings before deadline "
        f"acknowledged={acknowledged} events={accumulator.event_count} "
        f"pre_ack_frames={pre_ack_count} filtered_status_frames={filtered_status_frames} "
        f"missing_bindings={','.join(accumulator.missing_binding_ids()) or 'none'} "
        f"missing_final_bars={','.join(accumulator.missing_final_bar_ids()) or 'none'}"
    )


def _binance_rest_bar_binding(binding: NativeBinding) -> BinanceBarRawBinding:
    if binding.interval is None:
        raise ProviderAdmissionError("Binance REST BAR interval is missing")
    return BinanceBarRawBinding(
        market=binding.market,
        product_type=binding.product_type,
        native_symbol=binding.native_symbol,
        interval=binding.interval,
        subscription_id=f"phase10-admission-{binding.binding_id}",
        source_session_id=f"phase10-admission-{binding.binding_id}",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="qdl-phase10-provider-admission/1.0.0",
        config_revision=1,
        instrument_catalog_revision=1,
    )


def parse_okx_rest_bar(
    raw: str | bytes,
    *,
    binding: NativeBinding,
    observed_ms: int,
) -> FrameObservation:
    """Validate one provider-authentic, fully closed OKX REST BAR."""
    if (
        binding.mode != "PYTHON_REST"
        or binding.venue != "OKX"
        or binding.market not in {"SWAP", "SPOT"}
        or binding.feed != "BAR"
        or binding.interval is None
        or binding.native_channel != okx_candle_channel(binding.interval)
    ):
        raise ProviderAdmissionError("OKX REST BAR binding identity is invalid")
    observation = parse_okx_data(raw, bindings={binding.key: binding})
    if not observation.final_bar:
        raise ProviderAdmissionError("OKX REST BAR is not final")
    if observation.source_time_ms > observed_ms:
        raise ProviderAdmissionError("OKX REST BAR is not closed at observation time")
    return observation


def _okx_rest_bar_binding(binding: NativeBinding) -> OkxBarRawBinding:
    if binding.interval is None:
        raise ProviderAdmissionError("OKX REST BAR interval is missing")
    return OkxBarRawBinding(
        market=binding.market,
        product_type=binding.product_type,
        native_symbol=binding.native_symbol,
        interval=binding.interval,
        subscription_id=f"phase10-admission-{binding.binding_id}",
        source_session_id=f"phase10-admission-{binding.binding_id}",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="qdl-phase10-provider-admission/1.0.0",
        config_revision=1,
        instrument_catalog_revision=1,
    )


async def _binance_rest_bar_session(
    bindings: tuple[NativeBinding, ...],
) -> tuple[SessionEvidence, ...]:
    """Read each approved REST BAR once; no producer or runtime is started."""
    if not bindings:
        raise ProviderAdmissionError("Binance REST BAR session has no bindings")
    if any(
        item.mode != "PYTHON_REST"
        or item.venue != "BINANCE"
        or item.market != "USDM"
        or item.feed != "BAR"
        for item in bindings
    ):
        raise ProviderAdmissionError("Binance REST BAR session scope is invalid")

    async def fetch(binding: NativeBinding) -> FrameObservation:
        envelope = await asyncio.to_thread(
            fetch_latest_closed_bar_raw_envelope,
            _binance_rest_bar_binding(binding),
            attempts=4,
            test_provenance=False,
        )
        validate_raw_envelope(envelope)
        if (
            envelope.transport_protocol != raw_provider_pb2.TRANSPORT_PROTOCOL_HTTP
            or envelope.venue != "BINANCE"
            or envelope.market != binding.market
            or envelope.product_type != binding.product_type
            or envelope.native_symbol != binding.native_symbol
            or envelope.native_channel != binding.native_channel
            or envelope.test_provenance
        ):
            raise ProviderAdmissionError("Binance REST BAR raw capture provenance differs")
        try:
            payload = json.loads(bytes(envelope.raw_frame_bytes))
        except (TypeError, json.JSONDecodeError) as error:
            raise ProviderAdmissionError("Binance REST BAR capture is not JSON") from error
        if not isinstance(payload, Mapping):
            raise ProviderAdmissionError("Binance REST BAR capture is not an object")
        observed_ms = time.time_ns() // 1_000_000
        return parse_binance_rest_bar(
            payload,
            binding=binding,
            raw_frame_bytes=bytes(envelope.raw_frame_bytes),
            observed_ms=observed_ms,
        )

    observations = tuple(await asyncio.gather(*(fetch(item) for item in bindings)))
    return (
        SessionEvidence(
            role="BINANCE:USDM:REST_BAR",
            generation=1,
            ack_count=0,
            pre_ack_frames=0,
            event_count=len(observations),
            observations=observations,
            transport="HTTP",
        ),
    )


async def _okx_rest_bar_session(
    bindings: tuple[NativeBinding, ...],
) -> tuple[SessionEvidence, ...]:
    """Read each approved OKX REST BAR once; no producer or runtime is started."""
    if not bindings:
        raise ProviderAdmissionError("OKX REST BAR session has no bindings")
    if any(
        item.mode != "PYTHON_REST"
        or item.venue != "OKX"
        or item.market not in {"SWAP", "SPOT"}
        or item.feed != "BAR"
        for item in bindings
    ):
        raise ProviderAdmissionError("OKX REST BAR session scope is invalid")

    async def fetch(binding: NativeBinding) -> FrameObservation:
        envelope = await fetch_okx_latest_closed_bar_raw_envelope(
            _okx_rest_bar_binding(binding),
            attempts=4,
            test_provenance=False,
        )
        validate_raw_envelope(envelope)
        if (
            envelope.transport_protocol != raw_provider_pb2.TRANSPORT_PROTOCOL_HTTP
            or envelope.venue != "OKX"
            or envelope.market != binding.market
            or envelope.product_type != binding.product_type
            or envelope.native_symbol != binding.native_symbol
            or envelope.native_channel != binding.native_channel
            or envelope.test_provenance
        ):
            raise ProviderAdmissionError("OKX REST BAR raw capture provenance differs")
        return parse_okx_rest_bar(
            bytes(envelope.raw_frame_bytes),
            binding=binding,
            observed_ms=time.time_ns() // 1_000_000,
        )

    observations = tuple(await asyncio.gather(*(fetch(item) for item in bindings)))
    return (
        SessionEvidence(
            role="OKX:REST_BAR",
            generation=1,
            ack_count=0,
            pre_ack_frames=0,
            event_count=len(observations),
            observations=observations,
            transport="HTTP",
        ),
    )


async def _okx_session(
    bindings: tuple[NativeBinding, ...],
    *,
    role: str,
    websocket_url: str,
    generation: int,
    timeout_seconds: float,
    require_final_bars: bool,
) -> SessionEvidence:
    if not bindings:
        raise ProviderAdmissionError("OKX native session has no bindings")
    request_id = okx_request_id(role=role, generation=generation)
    expected_acks = {(item.native_channel, item.native_symbol) for item in bindings}
    command = {
        "id": request_id,
        "op": "subscribe",
        "args": [
            {"channel": item.native_channel, "instId": item.native_symbol}
            for item in bindings
        ],
    }
    lookup = _binding_lookup(bindings)
    accumulator = _SessionAccumulator(bindings)
    pre_ack: list[FrameObservation] = []
    pre_ack_count = 0
    started = time.monotonic()
    async with connect(websocket_url, ping_interval=None, ping_timeout=None, open_timeout=10) as socket:
        await socket.send(json.dumps(command, separators=(",", ":")))
        while time.monotonic() - started < timeout_seconds:
            raw = await _recv_with_heartbeat(socket)
            if raw is None:
                continue
            text, payload = _payload(raw)
            event = payload.get("event")
            if event is not None:
                if event == "error":
                    raise ProviderAdmissionError(
                        "OKX subscription was rejected "
                        f"role={role} code={payload.get('code')} msg={payload.get('msg')}"
                    )
                if event != "subscribe" or payload.get("id") != request_id:
                    raise ProviderAdmissionError("OKX control event is unexpected or mismatched")
                argument = payload.get("arg")
                if not isinstance(argument, Mapping):
                    raise ProviderAdmissionError("OKX subscription ACK has no arg")
                key = (
                    str(argument.get("channel") or ""),
                    str(argument.get("instId") or "").upper(),
                )
                if key not in expected_acks:
                    raise ProviderAdmissionError("OKX subscription ACK is duplicate or undeclared")
                expected_acks.remove(key)
                if not expected_acks:
                    pre_ack_count = len(pre_ack)
                    for item in pre_ack:
                        accumulator.add(item)
                    pre_ack.clear()
            else:
                observation = parse_okx_data(text, bindings=lookup)
                if expected_acks:
                    if len(pre_ack) >= _MAX_PRE_ACK_FRAMES:
                        raise ProviderAdmissionError("OKX pre-ACK data buffer exceeded bound")
                    pre_ack.append(observation)
                else:
                    accumulator.add(observation)
            if not expected_acks and accumulator.complete(require_final_bars=require_final_bars):
                return accumulator.evidence(
                    role=role,
                    generation=generation,
                    ack_count=len(bindings),
                    pre_ack_frames=pre_ack_count,
                )
    raise ProviderAdmissionError(f"{role} session did not satisfy all demanded bindings before deadline")


async def _two_session_probe(
    collector,
    *,
    first_requires_final_bars: bool,
) -> tuple[SessionEvidence, SessionEvidence]:
    first = await collector(generation=1, require_final_bars=first_requires_final_bars)
    second = await collector(generation=2, require_final_bars=False)
    return first, second


def _render_report(
    *,
    bindings: tuple[NativeBinding, ...],
    sessions: tuple[SessionEvidence, ...],
    elapsed_seconds: float,
    cpu_seconds: float,
    max_rss_kib: int,
) -> dict[str, Any]:
    by_binding: dict[str, list[FrameObservation]] = {item.binding_id: [] for item in bindings}
    session_seen: dict[str, int] = {item.binding_id: 0 for item in bindings}
    transports: dict[str, set[str]] = {item.binding_id: set() for item in bindings}
    final_bar_ids: set[str] = set()
    for session in sessions:
        seen_here = {item.binding_id for item in session.observations}
        for binding_id in seen_here:
            session_seen[binding_id] += 1
            transports[binding_id].add(session.transport)
        for observation in session.observations:
            by_binding[observation.binding_id].append(observation)
        final_bar_ids.update(session.final_bar_binding_ids)
    declared_bar_ids = {item.binding_id for item in bindings if item.feed == "BAR"}
    if not final_bar_ids <= declared_bar_ids:
        raise ProviderAdmissionError("session finality references a non-BAR binding")
    values: list[dict[str, Any]] = []
    for binding in bindings:
        observations = by_binding[binding.binding_id]
        expected_sessions = 1 if binding.mode == "PYTHON_REST" else 2
        expected_transport = "HTTP" if binding.mode == "PYTHON_REST" else "WEBSOCKET"
        if (
            len(observations) < expected_sessions
            or session_seen[binding.binding_id] != expected_sessions
        ):
            raise ProviderAdmissionError(
                "provider recovery/reconnect missed a demanded binding"
            )
        if transports[binding.binding_id] != {expected_transport}:
            raise ProviderAdmissionError("provider transport differs from declared acquisition mode")
        final_bar_seen = (
            binding.binding_id in final_bar_ids
            or any(item.final_bar for item in observations)
        )
        if binding.feed == "BAR" and not final_bar_seen:
            raise ProviderAdmissionError(
                "demanded BAR never arrived as final "
                f"binding={binding.binding_id} transport={expected_transport}"
            )
        latest = observations[-1]
        capture_ms = latest.observed_at_ms or (time.time_ns() // 1_000_000)
        age_ms = max(0, capture_ms - latest.source_time_ms)
        if age_ms > binding.stale_after_ms:
            raise ProviderAdmissionError(
                f"demanded binding is stale binding={binding.binding_id} age_ms={age_ms}"
            )
        values.append(
            {
                "binding_id": binding.binding_id,
                "venue": binding.venue,
                "market": binding.market,
                "native_symbol": binding.native_symbol,
                "feed": binding.feed,
                "interval": binding.interval,
                "acquisition_mode": binding.mode,
                "transport": expected_transport,
                "source_to_capture_age_ms": age_ms,
                "capture_time_ms": capture_ms,
                "session_observations": session_seen[binding.binding_id],
                "final_bar_observed": final_bar_seen,
                "last_frame_sha256": latest.frame_sha256,
            }
        )
    return {
        "schema": "qdl.phase10.realtime-provider-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_DIRECT_READ_ONLY",
        "binding_count": len(values),
        "bindings": values,
        "session_count": len(sessions),
        "websocket_binding_count": sum(
            item.mode == "RUST_NATIVE" for item in bindings
        ),
        "rest_closed_bar_recovery_count": sum(
            item.mode == "PYTHON_REST" for item in bindings
        ),
        "intentional_reconnect_count": len(
            {
                item.role
                for item in sessions
                if item.transport == "WEBSOCKET"
            }
        ),
        "ack_count": sum(item.ack_count for item in sessions),
        "pre_ack_frame_count": sum(item.pre_ack_frames for item in sessions),
        "accepted_provider_frame_count": sum(item.event_count for item in sessions),
        "filtered_status_frame_count": sum(
            item.filtered_status_frames for item in sessions
        ),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "process_cpu_seconds": round(cpu_seconds, 6),
        "process_max_rss_kib": max_rss_kib,
        "fallback_count": 0,
        "production_writes": 0,
        "runtime_mutations": 0,
        "scope": "provider-protocol admission only; no Kafka/projector/consumer authority",
    }


async def run(
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    acquisition_path: Path = DEFAULT_ACQUISITION_PATH,
    timeout_seconds: float = 90.0,
) -> dict[str, Any]:
    if not 65.0 <= timeout_seconds <= 120.0:
        raise ProviderAdmissionError("timeout_seconds must be between 65 and 120")
    bindings = load_active_provider_bindings(
        catalog_path=catalog_path,
        acquisition_path=acquisition_path,
    )
    binance_native = tuple(
        item
        for item in bindings
        if item.venue == "BINANCE" and item.mode == "RUST_NATIVE"
    )
    binance_rest = tuple(
        item
        for item in bindings
        if item.venue == "BINANCE" and item.mode == "PYTHON_REST"
    )
    okx_native = tuple(
        item for item in bindings if item.venue == "OKX" and item.mode == "RUST_NATIVE"
    )
    okx_rest = tuple(
        item for item in bindings if item.venue == "OKX" and item.mode == "PYTHON_REST"
    )
    if any(item.feed not in {"TRADE", "QUOTE"} for item in okx_native):
        raise ProviderAdmissionError("OKX native scope must contain only TRADE/QUOTE")
    if any(item.feed != "BAR" for item in okx_rest):
        raise ProviderAdmissionError("OKX REST scope must contain only final BAR")
    binance_lanes = binance_admission_lanes(binance_native)
    okx_public = okx_native
    binance_trade_symbols = {
        item.native_symbol for item in binance_lanes["TRADE"]
    }
    binance_quote_symbols = {
        item.native_symbol for item in binance_lanes["QUOTE"]
    }
    binance_bar_symbols = {item.native_symbol for item in binance_rest}
    okx_public_symbols = {item.native_symbol for item in okx_public}
    okx_bar_symbols = {item.native_symbol for item in okx_rest}
    if (
        binance_lanes["BAR"]
        or not binance_trade_symbols
        or binance_trade_symbols != binance_quote_symbols
        or binance_trade_symbols != binance_bar_symbols
        or any(item.feed != "BAR" for item in binance_rest)
        or not okx_public_symbols
        or okx_public_symbols != okx_bar_symbols
    ):
        raise ProviderAdmissionError("active provider role composition differs from Phase 10.3 demand")
    public_urls = {item.websocket_url for item in okx_public}
    if len(public_urls) != 1 or "" in public_urls:
        raise ProviderAdmissionError("OKX native bindings disagree on public WebSocket URL")
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    binance_trade_task = _two_session_probe(
        lambda **kwargs: _binance_session(
            binance_lanes["TRADE"],
            role="BINANCE:USDM:TRADE",
            timeout_seconds=timeout_seconds,
            **kwargs,
        ),
        first_requires_final_bars=False,
    )
    binance_quote_task = _two_session_probe(
        lambda **kwargs: _binance_session(
            binance_lanes["QUOTE"],
            role="BINANCE:USDM:QUOTE",
            timeout_seconds=timeout_seconds,
            **kwargs,
        ),
        first_requires_final_bars=False,
    )
    public_task = _two_session_probe(
        lambda **kwargs: _okx_session(
            okx_public,
            role="OKX:SWAP:PUBLIC",
            websocket_url=next(iter(public_urls)),
            timeout_seconds=timeout_seconds,
            **kwargs,
        ),
        first_requires_final_bars=False,
    )
    binance_trade_sessions, binance_quote_sessions, binance_rest_sessions, okx_rest_sessions, public_sessions = await asyncio.gather(
        binance_trade_task,
        binance_quote_task,
        _binance_rest_bar_session(binance_rest),
        _okx_rest_bar_session(okx_rest),
        public_task,
    )
    return _render_report(
        bindings=bindings,
        sessions=(
            binance_trade_sessions
            + binance_quote_sessions
            + binance_rest_sessions
            + okx_rest_sessions
            + public_sessions
        ),
        elapsed_seconds=time.monotonic() - started_wall,
        cpu_seconds=time.process_time() - started_cpu,
        max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--acquisition", type=Path, default=DEFAULT_ACQUISITION_PATH)
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = asyncio.run(
            run(
                catalog_path=args.catalog,
                acquisition_path=args.acquisition,
                timeout_seconds=args.timeout_seconds,
            )
        )
    except (OSError, ProviderAdmissionError, TimeoutError, ValueError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

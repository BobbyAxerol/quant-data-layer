#!/usr/bin/env python3
"""Bounded, read-only native WebSocket admission for active Phase 10.3 feeds.

This verifier intentionally talks only to the public Binance/OKX WebSocket
edges. It exercises the exact direct-control protocol used by the Rust native
ingestor without starting a producer, a projector, a consumer group, or any
Data Layer service. Its output contains bounded metadata and frame hashes, not
raw market data.
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

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_ACQUISITION_PATH = ROOT / "config/v2/stable-acquisition-bindings.yaml"
_MAX_BINDINGS = 128
_MAX_PRE_ACK_FRAMES = 128
_HEARTBEAT_SECONDS = 10.0


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


@dataclass(frozen=True, slots=True)
class SessionEvidence:
    role: str
    generation: int
    ack_count: int
    pre_ack_frames: int
    event_count: int
    observations: tuple[FrameObservation, ...]
    filtered_status_frames: int = 0


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


def load_active_native_bindings(
    *,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    acquisition_path: Path = DEFAULT_ACQUISITION_PATH,
) -> tuple[NativeBinding, ...]:
    """Resolve only enabled native bindings from the governed stable manifest."""
    catalog = StableSourceCatalog.load(catalog_path)
    acquisition = StableAcquisitionPlan.load(acquisition_path, catalog=catalog)
    sources = {item.binding_id: item for item in catalog.bindings}
    values: list[NativeBinding] = []
    for item in acquisition.bindings:
        if not item.enabled or item.mode != "RUST_NATIVE":
            continue
        source = sources[item.binding_id]
        identity = source.instrument.identity
        if item.runtime not in {"BINANCE", "OKX"}:
            raise ProviderAdmissionError("unexpected non-crypto Rust-native runtime")
        if identity.venue != item.runtime:
            raise ProviderAdmissionError("native runtime and catalog venue differ")
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
                websocket_url=item.websocket_url or "",
                business_websocket_url=item.business_websocket_url,
            )
        )
    result = tuple(sorted(values, key=lambda item: item.binding_id))
    if not result or len(result) > _MAX_BINDINGS:
        raise ProviderAdmissionError("active Rust-native binding count is outside admission bound")
    if {item.venue for item in result} != {"BINANCE", "OKX"}:
        raise ProviderAdmissionError("active native demand must include Binance and OKX")
    if any(item.market not in {"USDM", "SWAP"} for item in result):
        raise ProviderAdmissionError("disabled Spot capability leaked into active native demand")
    return result


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


def parse_binance_data(
    raw: str | bytes,
    *,
    bindings: Mapping[tuple[str, str], NativeBinding],
) -> FrameObservation:
    text, payload = _payload(raw)
    event = str(payload.get("e") or "")
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
    elif event == "bookTicker":
        channel = f"{symbol.lower()}@bookTicker"
        _positive_decimal(payload.get("b"), "Binance bid price")
        _positive_decimal(payload.get("B"), "Binance bid quantity")
        _positive_decimal(payload.get("a"), "Binance ask price")
        _positive_decimal(payload.get("A"), "Binance ask quantity")
        source_time_ms = _timestamp_ms(
            payload.get("E") or payload.get("T"), "Binance quote event time"
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
            raise ProviderAdmissionError("Phase 10.3 native OKX BAR interval is not 1m")
        source_time_ms = open_time_ms + 60_000
        final_bar = str(first[8]) == "1"
    else:
        raise ProviderAdmissionError("OKX active binding has unsupported feed")
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=source_time_ms,
        frame_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        final_bar=final_bar,
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
    generation: int,
    timeout_seconds: float,
    require_final_bars: bool,
) -> SessionEvidence:
    if not bindings:
        raise ProviderAdmissionError("Binance native session has no bindings")
    urls = {item.websocket_url for item in bindings}
    if len(urls) != 1:
        raise ProviderAdmissionError("Binance bindings disagree on control WebSocket URL")
    request_id = 10_000 + generation
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
                    role="BINANCE:USDM",
                    generation=generation,
                    ack_count=1,
                    pre_ack_frames=pre_ack_count,
                    filtered_status_frames=filtered_status_frames,
                )
    raise ProviderAdmissionError("Binance session did not satisfy all demanded bindings before deadline")


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
    for session in sessions:
        seen_here = {item.binding_id for item in session.observations}
        for binding_id in seen_here:
            session_seen[binding_id] += 1
        for observation in session.observations:
            by_binding[observation.binding_id].append(observation)
    now_ms = time.time_ns() // 1_000_000
    values: list[dict[str, Any]] = []
    for binding in bindings:
        observations = by_binding[binding.binding_id]
        if len(observations) < 2 or session_seen[binding.binding_id] != 2:
            raise ProviderAdmissionError("reconnect/resubscribe missed a demanded binding")
        final_bar_seen = any(item.final_bar for item in observations)
        if binding.feed == "BAR" and not final_bar_seen:
            raise ProviderAdmissionError("demanded BAR never arrived as final")
        latest = observations[-1]
        age_ms = max(0, now_ms - latest.source_time_ms)
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
                "source_age_ms": age_ms,
                "session_observations": session_seen[binding.binding_id],
                "final_bar_observed": final_bar_seen,
                "last_frame_sha256": latest.frame_sha256,
            }
        )
    return {
        "schema": "qdl.phase10.realtime-provider-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_WEBSOCKET_READ_ONLY",
        "binding_count": len(values),
        "bindings": values,
        "session_count": len(sessions),
        "intentional_reconnect_count": len({item.role for item in sessions}),
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
    bindings = load_active_native_bindings(
        catalog_path=catalog_path,
        acquisition_path=acquisition_path,
    )
    binance = tuple(item for item in bindings if item.venue == "BINANCE")
    okx = tuple(item for item in bindings if item.venue == "OKX")
    okx_public = tuple(item for item in okx if item.feed in {"TRADE", "QUOTE"})
    okx_business = tuple(item for item in okx if item.feed == "BAR")
    if len(binance) != 6 or len(okx_public) != 4 or len(okx_business) != 2:
        raise ProviderAdmissionError("active native role composition differs from Phase 10.3 demand")
    public_urls = {item.websocket_url for item in okx_public}
    business_urls = {item.business_websocket_url for item in okx_business}
    if len(public_urls) != 1 or len(business_urls) != 1 or None in business_urls:
        raise ProviderAdmissionError("OKX active bindings disagree on public/business WebSocket URLs")
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    binance_task = _two_session_probe(
        lambda **kwargs: _binance_session(binance, timeout_seconds=timeout_seconds, **kwargs),
        first_requires_final_bars=True,
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
    business_task = _two_session_probe(
        lambda **kwargs: _okx_session(
            okx_business,
            role="OKX:SWAP:BUSINESS",
            websocket_url=next(iter(business_urls)),
            timeout_seconds=timeout_seconds,
            **kwargs,
        ),
        first_requires_final_bars=True,
    )
    binance_sessions, public_sessions, business_sessions = await asyncio.gather(
        binance_task, public_task, business_task
    )
    return _render_report(
        bindings=bindings,
        sessions=binance_sessions + public_sessions + business_sessions,
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

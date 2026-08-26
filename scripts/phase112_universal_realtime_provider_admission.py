#!/usr/bin/env python3
"""Bounded, read-only admission for every active Phase 11.2 realtime binding.

The verifier compiles the current alpha/Trading System declarations, admits
them against fresh public Binance/OKX instrument metadata, then validates each
admitted final BAR through its provider REST close-confirmation edge and each
admitted TRADE/QUOTE through a shared, multiplexed provider WebSocket session.
It never starts a Data Layer role, writes Kafka/Redis/SQLite, changes a route,
or persists raw provider bytes.  The optional report contains only identifiers,
frame digests and bounded operational measurements.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import resource
import time
from typing import Any, Iterable, Mapping, Sequence

from websockets.asyncio.client import connect

from qdl.adapters.binance import (
    BinanceBarRawBinding,
    fetch_latest_closed_bar_raw_envelope as fetch_binance_latest_closed_bar,
)
from qdl.adapters.intervals import canonical_interval_ms, okx_candle_channel
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_latest_closed_bar_raw_envelope as fetch_okx_latest_closed_bar,
)
from qdl.demand import (
    ActiveDemandInventory,
    ActiveDemandSourceRegistry,
    InventoryError,
    ProviderAdmission,
    admit_provider_metadata,
    converge_active_demand,
)
from qdl.raw.envelope import validate_raw_envelope
from qdl.runtime.production_catalog import ProductionCatalogBuilder
from qdl.runtime.universal_realtime import (
    ProviderRealtimeBinding,
    UniversalRealtimePlan,
    build_universal_realtime_plan,
    provider_realtime_bindings,
)
from scripts.phase111_active_demand_inventory import (
    ROOT as QDL_ROOT,
    compile_inventory,
    fetch_provider_metadata,
)


DEFAULT_SOURCE_REGISTRY = QDL_ROOT / "config/v2/active-demand-source-registry.yaml"
DEFAULT_OUTPUT = QDL_ROOT / "upgrade/evidence/phase112-universal-realtime-provider-admission.json"
_MAX_NATIVE_BINDINGS_PER_SESSION = 200
_MAX_REST_CONCURRENCY = 32
_HEARTBEAT_SECONDS = 10.0
_MAX_PRE_ACK_FRAMES = 512


class ProviderAdmissionError(RuntimeError):
    """A declared provider edge did not satisfy the Phase 11.2 contract."""


def _related_workspace_root(name: str) -> Path:
    """Resolve sibling repositories on host or an explicitly mounted runner.

    The defaults are convenience only: production-like invocation still passes
    roots explicitly.  This guard keeps an isolated `/app` mount importable and
    never guesses a source document from an unrelated directory.
    """
    for parent in (QDL_ROOT.parent, *QDL_ROOT.parents):
        candidate = parent / name
        if candidate.is_dir():
            return candidate
    mounted = Path("/") / name
    if mounted.is_dir():
        return mounted
    return mounted


DEFAULT_EXECUTION_ALPHA_ROOT = _related_workspace_root("execution_alpha")
DEFAULT_TRADING_SYSTEM_ROOT = _related_workspace_root("trading_system")


@dataclass(frozen=True, slots=True)
class FrameObservation:
    binding_id: str
    source_time_ms: int
    observed_at_ms: int
    frame_sha256: str
    final_bar: bool
    transport: str
    generation: int
    source_time_missing: bool = False


@dataclass(frozen=True, slots=True)
class SessionEvidence:
    role: tuple[str, str, str]
    generation: int
    binding_ids: tuple[str, ...]
    ack_count: int
    event_count: int
    observations: tuple[FrameObservation, ...]
    transport: str = "WEBSOCKET"


@dataclass(frozen=True, slots=True)
class ProviderAdmissionPlan:
    plan: UniversalRealtimePlan
    bindings: tuple[ProviderRealtimeBinding, ...]
    accepted_missing: tuple[dict[str, str | None], ...]
    deferred_requirement_ids: tuple[str, ...]
    # Keep the authenticated metadata admission that built this plan available
    # to later read-only acceptance phases.  Defaults preserve the existing
    # test/report constructor and no runtime role reads these fields.
    inventory: ActiveDemandInventory | None = None
    admission: ProviderAdmission | None = None


def _text(value: object, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ProviderAdmissionError(f"{field} is missing")
    return result


def _positive_decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ProviderAdmissionError(f"{field} is not a decimal") from error
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise ProviderAdmissionError(f"{field} is outside the allowed decimal domain")
    return result


def _timestamp_ms(value: object, field: str) -> int:
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
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProviderAdmissionError("provider WebSocket frame is not JSON") from error
    if not isinstance(value, Mapping):
        raise ProviderAdmissionError("provider WebSocket frame is not an object")
    return raw, value


def _chunks(values: Sequence[ProviderRealtimeBinding], size: int) -> tuple[tuple[ProviderRealtimeBinding, ...], ...]:
    if not 1 <= size <= _MAX_NATIVE_BINDINGS_PER_SESSION:
        raise ValueError("native session size is outside the approved bound")
    return tuple(tuple(values[offset:offset + size]) for offset in range(0, len(values), size))


def _native_groups(
    bindings: Iterable[ProviderRealtimeBinding],
    *,
    max_bindings_per_session: int,
) -> tuple[tuple[tuple[str, str, str], tuple[ProviderRealtimeBinding, ...]], ...]:
    """Group native demand by shared provider role, never by consumer/symbol."""
    grouped: dict[tuple[str, str, str], list[ProviderRealtimeBinding]] = defaultdict(list)
    for item in bindings:
        if item.mode != "RUST_NATIVE":
            continue
        grouped[(item.venue, item.market, item.feed.value)].append(item)
    result = []
    for role, values in sorted(grouped.items()):
        ordered = sorted(values, key=lambda item: (item.native_symbol, item.native_channel, item.binding_id))
        for batch in _chunks(ordered, max_bindings_per_session):
            result.append((role, batch))
    return tuple(result)


def _build_plan(
    *,
    source_registry: Path,
    repository_root: Path,
    execution_alpha_root: Path,
    trading_system_root: Path,
    metadata_timeout_seconds: float,
    metadata_attempts: int,
) -> ProviderAdmissionPlan:
    registry = ActiveDemandSourceRegistry.load(source_registry)
    inventory = compile_inventory(
        source_registry=source_registry,
        repository_root=repository_root,
        execution_alpha_root=execution_alpha_root,
        trading_system_root=trading_system_root,
    )
    admission = admit_provider_metadata(
        inventory,
        fetch_provider_metadata(
            inventory,
            timeout_seconds=metadata_timeout_seconds,
            attempts=metadata_attempts,
        ),
    )
    accepted_missing = tuple(
        {
            "requirement_id": row.requirement_id,
            "consumer_id": row.consumer_id,
            "venue": row.venue,
            "market": row.market,
            "product_type": row.product_type,
            "native_symbol": row.native_symbol,
            "feed": row.feed,
            "interval": row.interval,
            "state": row.state,
            "reason": row.reason,
        }
        for row in admission.rows
        if row.state == "MISSING_INSTRUMENT"
    )
    unexpected = tuple(
        row for row in admission.rows
        if row.state not in {"ADMITTED", "MISSING_INSTRUMENT"}
    )
    if unexpected:
        first = unexpected[0]
        raise ProviderAdmissionError(
            "active-demand provider admission has an unapproved failure "
            f"state={first.state} requirement={first.requirement_id}"
        )
    convergence = converge_active_demand(
        inventory,
        admission,
        registry.admission_policy,
    )
    plan = build_universal_realtime_plan(
        inventory=inventory,
        admission=admission,
        convergence=convergence,
        builder=ProductionCatalogBuilder(
            catalog_revision=inventory.revision,
            source_policy_revision=registry.revision,
            authority_revision=1,
        ),
    )
    bindings = provider_realtime_bindings(plan)
    if not bindings:
        raise ProviderAdmissionError("active demand has no admitted realtime binding")
    if {item.binding_id for item in bindings} != set(plan.owners_by_binding):
        raise ProviderAdmissionError("provider projection does not cover every admitted binding")
    return ProviderAdmissionPlan(
        plan=plan,
        bindings=bindings,
        accepted_missing=accepted_missing,
        deferred_requirement_ids=plan.deferred_requirement_ids,
        inventory=inventory,
        admission=admission,
    )


async def _recv_with_heartbeat(socket) -> str | bytes | None:
    try:
        return await asyncio.wait_for(socket.recv(), timeout=_HEARTBEAT_SECONDS)
    except TimeoutError:
        pong_waiter = await socket.ping()
        await asyncio.wait_for(pong_waiter, timeout=_HEARTBEAT_SECONDS)
        return None


def _binance_lookup(
    bindings: Iterable[ProviderRealtimeBinding],
) -> dict[tuple[str, str], ProviderRealtimeBinding]:
    values = tuple(bindings)
    result = {
        (item.native_channel, item.native_symbol.upper()): item
        for item in values
    }
    if len(result) != len(values):
        raise ProviderAdmissionError("Binance provider bindings are not unique")
    return result


def _okx_lookup(
    bindings: Iterable[ProviderRealtimeBinding],
) -> dict[tuple[str, str], ProviderRealtimeBinding]:
    values = tuple(bindings)
    result = {
        (item.native_channel, item.native_symbol.upper()): item
        for item in values
    }
    if len(result) != len(values):
        raise ProviderAdmissionError("OKX provider bindings are not unique")
    return result


def _binance_observation(
    raw: str | bytes,
    *,
    bindings: Mapping[tuple[str, str], ProviderRealtimeBinding],
    generation: int,
) -> FrameObservation | None:
    text, payload = _payload(raw)
    observed_at_ms = time.time_ns() // 1_000_000
    event = str(payload.get("e") or "")
    symbol = str(payload.get("s") or "").upper()
    if not event and _is_binance_direct_book_ticker(payload):
        event = "bookTicker"
    if not event:
        if "code" in payload or "msg" in payload:
            raise ProviderAdmissionError(
                "Binance provider control frame rejected subscription "
                f"code={payload.get('code')} id={payload.get('id')}"
            )
        raise ProviderAdmissionError(
            "Binance provider frame has no event type "
            f"keys={','.join(sorted(str(key) for key in payload)[:8]) or 'none'}"
        )
    if event == "trade":
        # Binance can emit this exact status payload; it is deliberately not a
        # trade and must not satisfy an active trade binding.
        if (
            payload.get("p") == "0"
            and payload.get("q") == "0"
            and payload.get("X") == "NA"
            and isinstance(payload.get("st"), int)
            and not isinstance(payload.get("st"), bool)
            and payload["st"] == 1
        ):
            return None
        _positive_decimal(payload.get("p"), "Binance trade price")
        _positive_decimal(payload.get("q"), "Binance trade quantity")
        source_time_ms = _timestamp_ms(payload.get("T"), "Binance trade time")
        source_time_missing = False
        channel = f"{symbol.lower()}@trade"
        expected_feed = "TRADE"
    elif event == "bookTicker":
        _positive_decimal(payload.get("b"), "Binance bid price")
        _positive_decimal(payload.get("B"), "Binance bid quantity")
        _positive_decimal(payload.get("a"), "Binance ask price")
        _positive_decimal(payload.get("A"), "Binance ask quantity")
        # Binance Spot bookTicker deliberately has no provider event time.
        # Canonicalization preserves that fact through SOURCE_TIME_MISSING and
        # uses capture time only as the event envelope's bounded fallback.
        # Keep this verifier byte-for-byte semantically aligned: T takes
        # precedence over E where both are present, matching the Rust/Python
        # canonicalizers.
        provider_time = payload.get("T")
        if provider_time is None:
            provider_time = payload.get("E")
        source_time_missing = provider_time is None
        source_time_ms = (
            observed_at_ms
            if source_time_missing
            else _timestamp_ms(provider_time, "Binance quote event time")
        )
        channel = f"{symbol.lower()}@bookTicker"
        expected_feed = "QUOTE"
    else:
        raise ProviderAdmissionError(f"unsupported Binance native event: {event}")
    binding = bindings.get((channel, symbol))
    if binding is None or binding.feed.value != expected_feed:
        raise ProviderAdmissionError("Binance provider frame cross-mixed an undeclared binding")
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=source_time_ms,
        observed_at_ms=observed_at_ms,
        frame_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        final_bar=False,
        transport="WEBSOCKET",
        generation=generation,
        source_time_missing=source_time_missing,
    )


def _is_binance_direct_book_ticker(payload: Mapping[str, Any]) -> bool:
    sequence = payload.get("u")
    return (
        isinstance(sequence, int)
        and not isinstance(sequence, bool)
        and sequence >= 0
        and all(isinstance(payload.get(field), str) and payload[field] for field in ("b", "B", "a", "A"))
    )


def _okx_observation(
    raw: str | bytes,
    *,
    bindings: Mapping[tuple[str, str], ProviderRealtimeBinding],
    generation: int,
) -> FrameObservation:
    text, payload = _payload(raw)
    if payload.get("event") is not None:
        raise ProviderAdmissionError("OKX control frame cannot satisfy a realtime binding")
    argument = payload.get("arg")
    if not isinstance(argument, Mapping):
        raise ProviderAdmissionError("OKX data frame has no arg")
    channel = str(argument.get("channel") or "")
    symbol = str(argument.get("instId") or "").upper()
    binding = bindings.get((channel, symbol))
    if binding is None:
        raise ProviderAdmissionError("OKX provider frame cross-mixed an undeclared binding")
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], Mapping):
        raise ProviderAdmissionError("OKX realtime data row is invalid")
    row = rows[0]
    if binding.feed.value == "TRADE":
        if str(row.get("instId") or "").upper() != symbol or not str(row.get("tradeId") or "").strip():
            raise ProviderAdmissionError("OKX trade identity is invalid")
        _positive_decimal(row.get("px"), "OKX trade price")
        _positive_decimal(row.get("sz"), "OKX trade quantity")
        source_time_ms = _timestamp_ms(row.get("ts"), "OKX trade time")
    elif binding.feed.value == "QUOTE":
        for side in ("bids", "asks"):
            levels = row.get(side)
            if not isinstance(levels, list) or len(levels) != 1 or not isinstance(levels[0], list) or len(levels[0]) < 2:
                raise ProviderAdmissionError(f"OKX BBO {side} is invalid")
            _positive_decimal(levels[0][0], f"OKX {side} price")
            _positive_decimal(levels[0][1], f"OKX {side} quantity")
        _text(row.get("seqId"), "OKX BBO sequence")
        source_time_ms = _timestamp_ms(row.get("ts"), "OKX BBO time")
    else:
        raise ProviderAdmissionError("OKX native provider binding is not TRADE/QUOTE")
    observed_at_ms = time.time_ns() // 1_000_000
    return FrameObservation(
        binding_id=binding.binding_id,
        source_time_ms=source_time_ms,
        observed_at_ms=observed_at_ms,
        frame_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        final_bar=False,
        transport="WEBSOCKET",
        generation=generation,
    )


async def _binance_session(
    role: tuple[str, str, str],
    bindings: tuple[ProviderRealtimeBinding, ...],
    *,
    generation: int,
    timeout_seconds: float,
    request_id: int,
) -> SessionEvidence:
    urls = {item.websocket_url for item in bindings}
    if len(urls) != 1 or None in urls:
        raise ProviderAdmissionError("Binance native bindings disagree on WebSocket endpoint")
    lookup = _binance_lookup(bindings)
    remaining = {item.binding_id for item in bindings}
    observations: dict[str, FrameObservation] = {}
    pre_ack: list[FrameObservation] = []
    acknowledged = False
    started = time.monotonic()
    command = {
        "method": "SUBSCRIBE",
        "params": [item.native_channel for item in bindings],
        "id": request_id,
    }
    async with connect(next(iter(urls)), ping_interval=None, ping_timeout=None, open_timeout=10, max_queue=1024) as socket:
        await socket.send(json.dumps(command, separators=(",", ":")))
        while time.monotonic() - started < timeout_seconds:
            raw = await _recv_with_heartbeat(socket)
            if raw is None:
                continue
            _text, payload = _payload(raw)
            if "id" in payload:
                if payload.get("id") != request_id or payload.get("result") is not None or payload.get("code") is not None or acknowledged:
                    raise ProviderAdmissionError("Binance subscription ACK is invalid")
                acknowledged = True
                for observation in pre_ack:
                    observations[observation.binding_id] = observation
                    remaining.discard(observation.binding_id)
                pre_ack.clear()
            else:
                observation = _binance_observation(raw, bindings=lookup, generation=generation)
                if observation is not None:
                    if acknowledged:
                        observations[observation.binding_id] = observation
                        remaining.discard(observation.binding_id)
                    else:
                        if len(pre_ack) >= _MAX_PRE_ACK_FRAMES:
                            raise ProviderAdmissionError("Binance pre-ACK frame buffer exceeded bound")
                        pre_ack.append(observation)
            if acknowledged and not remaining:
                return SessionEvidence(
                    role=role,
                    generation=generation,
                    binding_ids=tuple(item.binding_id for item in bindings),
                    ack_count=1,
                    event_count=len(observations),
                    observations=tuple(observations[key] for key in sorted(observations)),
                )
    raise ProviderAdmissionError(
        "Binance shared session did not observe every demanded binding "
        f"role={'/'.join(role)} generation={generation} acknowledged={acknowledged} missing={','.join(sorted(remaining)) or 'none'}"
    )


async def _okx_session(
    role: tuple[str, str, str],
    bindings: tuple[ProviderRealtimeBinding, ...],
    *,
    generation: int,
    timeout_seconds: float,
    request_id: str,
) -> SessionEvidence:
    urls = {item.websocket_url for item in bindings}
    if len(urls) != 1 or None in urls:
        raise ProviderAdmissionError("OKX native bindings disagree on public WebSocket endpoint")
    lookup = _okx_lookup(bindings)
    pending_acks = {(item.native_channel, item.native_symbol.upper()) for item in bindings}
    remaining = {item.binding_id for item in bindings}
    observations: dict[str, FrameObservation] = {}
    pre_ack: list[FrameObservation] = []
    command = {
        "id": request_id,
        "op": "subscribe",
        "args": [
            {"channel": item.native_channel, "instId": item.native_symbol}
            for item in bindings
        ],
    }
    started = time.monotonic()
    async with connect(next(iter(urls)), ping_interval=None, ping_timeout=None, open_timeout=10, max_queue=1024) as socket:
        await socket.send(json.dumps(command, separators=(",", ":")))
        while time.monotonic() - started < timeout_seconds:
            raw = await _recv_with_heartbeat(socket)
            if raw is None:
                continue
            _text, payload = _payload(raw)
            event = payload.get("event")
            if event is not None:
                if event == "error":
                    raise ProviderAdmissionError(
                        f"OKX subscription was rejected role={'/'.join(role)} code={payload.get('code')}"
                    )
                if event != "subscribe" or payload.get("id") != request_id:
                    raise ProviderAdmissionError("OKX subscription control event is invalid")
                argument = payload.get("arg")
                if not isinstance(argument, Mapping):
                    raise ProviderAdmissionError("OKX subscription ACK has no argument")
                key = (str(argument.get("channel") or ""), str(argument.get("instId") or "").upper())
                if key not in pending_acks:
                    raise ProviderAdmissionError("OKX subscription ACK is duplicate or undeclared")
                pending_acks.remove(key)
                if not pending_acks:
                    for observation in pre_ack:
                        observations[observation.binding_id] = observation
                        remaining.discard(observation.binding_id)
                    pre_ack.clear()
            else:
                observation = _okx_observation(raw, bindings=lookup, generation=generation)
                if pending_acks:
                    if len(pre_ack) >= _MAX_PRE_ACK_FRAMES:
                        raise ProviderAdmissionError("OKX pre-ACK frame buffer exceeded bound")
                    pre_ack.append(observation)
                else:
                    observations[observation.binding_id] = observation
                    remaining.discard(observation.binding_id)
            if not remaining:
                return SessionEvidence(
                    role=role,
                    generation=generation,
                    binding_ids=tuple(item.binding_id for item in bindings),
                    ack_count=len(bindings),
                    event_count=len(observations),
                    observations=tuple(observations[key] for key in sorted(observations)),
                )
    raise ProviderAdmissionError(
        "OKX shared session did not observe every demanded binding "
        f"role={'/'.join(role)} generation={generation} pending_acks={len(pending_acks)} missing={','.join(sorted(remaining)) or 'none'}"
    )


def _binance_bar_binding(item: ProviderRealtimeBinding) -> BinanceBarRawBinding:
    if item.interval is None:
        raise ProviderAdmissionError("Binance final BAR interval is missing")
    return BinanceBarRawBinding(
        market=item.market,
        product_type=item.product_type,
        native_symbol=item.native_symbol,
        interval=item.interval,
        subscription_id=f"phase112-{item.binding_id}",
        source_session_id=f"phase112-{item.binding_id}",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="qdl-phase112-provider-admission/1.0.0",
        config_revision=item.demand_revision,
        instrument_catalog_revision=item.catalog_revision,
    )


def _okx_bar_binding(item: ProviderRealtimeBinding) -> OkxBarRawBinding:
    if item.interval is None:
        raise ProviderAdmissionError("OKX final BAR interval is missing")
    return OkxBarRawBinding(
        market=item.market,
        product_type=item.product_type,
        native_symbol=item.native_symbol,
        interval=item.interval,
        subscription_id=f"phase112-{item.binding_id}",
        source_session_id=f"phase112-{item.binding_id}",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="qdl-phase112-provider-admission/1.0.0",
        config_revision=item.demand_revision,
        instrument_catalog_revision=item.catalog_revision,
    )


def _binance_bar_observation(item: ProviderRealtimeBinding, envelope) -> FrameObservation:
    validate_raw_envelope(envelope)
    if envelope.test_provenance:
        raise ProviderAdmissionError("Binance final BAR provider evidence is synthetic")
    observed_at_ms = time.time_ns() // 1_000_000
    try:
        payload = json.loads(bytes(envelope.raw_frame_bytes))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderAdmissionError("Binance final BAR envelope is not JSON") from error
    if not isinstance(payload, Mapping) or str(payload.get("symbol") or "").upper() != item.native_symbol:
        raise ProviderAdmissionError("Binance final BAR symbol differs from binding")
    if str(payload.get("interval") or "") != item.interval or str(payload.get("bar_origin") or "").upper() != "VENUE_NATIVE":
        raise ProviderAdmissionError("Binance final BAR identity/origin differs from binding")
    row = payload.get("row")
    if not isinstance(row, list) or len(row) < 9:
        raise ProviderAdmissionError("Binance final BAR row is incomplete")
    open_time_ms = _timestamp_ms(row[0], "Binance final BAR open time")
    close_time_ms = _timestamp_ms(row[6], "Binance final BAR close time")
    interval_ms = canonical_interval_ms(item.interval or "")
    if close_time_ms != open_time_ms + interval_ms - 1 or close_time_ms >= observed_at_ms:
        raise ProviderAdmissionError("Binance final BAR is early or has an invalid boundary")
    for index, field in ((1, "open"), (2, "high"), (3, "low"), (4, "close")):
        _positive_decimal(row[index], f"Binance final BAR {field}")
    _positive_decimal(row[5], "Binance final BAR base volume", allow_zero=True)
    return FrameObservation(
        binding_id=item.binding_id,
        source_time_ms=close_time_ms,
        observed_at_ms=observed_at_ms,
        frame_sha256=hashlib.sha256(bytes(envelope.raw_frame_bytes)).hexdigest(),
        final_bar=True,
        transport="HTTP",
        generation=1,
    )


def _okx_bar_observation(item: ProviderRealtimeBinding, envelope) -> FrameObservation:
    validate_raw_envelope(envelope)
    if envelope.test_provenance:
        raise ProviderAdmissionError("OKX final BAR provider evidence is synthetic")
    observed_at_ms = time.time_ns() // 1_000_000
    try:
        payload = json.loads(bytes(envelope.raw_frame_bytes))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderAdmissionError("OKX final BAR envelope is not JSON") from error
    if not isinstance(payload, Mapping):
        raise ProviderAdmissionError("OKX final BAR envelope is not an object")
    argument = payload.get("arg")
    rows = payload.get("data")
    if not isinstance(argument, Mapping) or not isinstance(rows, list) or not rows or not isinstance(rows[0], list):
        raise ProviderAdmissionError("OKX final BAR envelope is incomplete")
    if str(argument.get("instId") or "").upper() != item.native_symbol or str(argument.get("channel") or "") != okx_candle_channel(item.interval or ""):
        raise ProviderAdmissionError("OKX final BAR identity differs from binding")
    row = rows[0]
    if len(row) < 9 or str(row[8]) != "1":
        raise ProviderAdmissionError("OKX final BAR is not confirmed")
    open_time_ms = _timestamp_ms(row[0], "OKX final BAR open time")
    interval_ms = canonical_interval_ms(item.interval or "")
    close_time_ms = open_time_ms + interval_ms - 1
    if close_time_ms >= observed_at_ms:
        raise ProviderAdmissionError("OKX final BAR is early")
    for index, field in ((1, "open"), (2, "high"), (3, "low"), (4, "close")):
        _positive_decimal(row[index], f"OKX final BAR {field}")
    _positive_decimal(row[5], "OKX final BAR volume", allow_zero=True)
    return FrameObservation(
        binding_id=item.binding_id,
        source_time_ms=close_time_ms,
        observed_at_ms=observed_at_ms,
        frame_sha256=hashlib.sha256(bytes(envelope.raw_frame_bytes)).hexdigest(),
        final_bar=True,
        transport="HTTP",
        generation=1,
    )


async def _rest_bar_observations(
    bindings: Iterable[ProviderRealtimeBinding],
    *,
    max_concurrency: int,
) -> tuple[SessionEvidence, ...]:
    if not 1 <= max_concurrency <= _MAX_REST_CONCURRENCY:
        raise ValueError("REST final BAR concurrency is outside the approved bound")
    semaphore = asyncio.Semaphore(max_concurrency)

    async def fetch(item: ProviderRealtimeBinding) -> FrameObservation:
        async with semaphore:
            if item.venue == "BINANCE":
                envelope = await asyncio.to_thread(
                    fetch_binance_latest_closed_bar,
                    _binance_bar_binding(item),
                    attempts=4,
                )
                return _binance_bar_observation(item, envelope)
            if item.venue == "OKX":
                envelope = await fetch_okx_latest_closed_bar(
                    _okx_bar_binding(item),
                    attempts=4,
                )
                return _okx_bar_observation(item, envelope)
            raise ProviderAdmissionError("final BAR venue is outside Phase 11.2")

    values = tuple(sorted(bindings, key=lambda item: item.binding_id))
    observations = await asyncio.gather(*(fetch(item) for item in values))
    by_role: dict[tuple[str, str, str], list[FrameObservation]] = defaultdict(list)
    by_role_ids: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for item, observation in zip(values, observations):
        role = (item.venue, item.market, "BAR")
        by_role[role].append(observation)
        by_role_ids[role].append(item.binding_id)
    return tuple(
        SessionEvidence(
            role=role,
            generation=1,
            binding_ids=tuple(sorted(by_role_ids[role])),
            ack_count=0,
            event_count=len(by_role[role]),
            observations=tuple(sorted(by_role[role], key=lambda item: item.binding_id)),
            transport="HTTP",
        )
        for role in sorted(by_role)
    )


async def _native_observations(
    bindings: Iterable[ProviderRealtimeBinding],
    *,
    max_bindings_per_session: int,
    timeout_seconds: float,
) -> tuple[SessionEvidence, ...]:
    groups = _native_groups(bindings, max_bindings_per_session=max_bindings_per_session)
    primary_tasks = []
    for index, (role, batch) in enumerate(groups, start=1):
        if role[0] == "BINANCE":
            primary_tasks.append(_binance_session(
                role, batch, generation=1, timeout_seconds=timeout_seconds, request_id=112_000 + index,
            ))
        elif role[0] == "OKX":
            primary_tasks.append(_okx_session(
                role, batch, generation=1, timeout_seconds=timeout_seconds, request_id=f"qdl112{index}g1",
            ))
        else:
            raise ProviderAdmissionError("native realtime venue is outside Phase 11.2")
    primary = tuple(await asyncio.gather(*primary_tasks))

    # One isolated reconnect probe per logical provider role proves lifecycle
    # behavior without multiplying sessions by every active symbol.
    probes: list[tuple[tuple[str, str, str], tuple[ProviderRealtimeBinding, ...]]] = []
    seen_roles: set[tuple[str, str, str]] = set()
    for role, batch in groups:
        if role not in seen_roles:
            probes.append((role, (batch[0],)))
            seen_roles.add(role)
    reconnect_tasks = []
    for index, (role, batch) in enumerate(probes, start=1):
        if role[0] == "BINANCE":
            reconnect_tasks.append(_binance_session(
                role, batch, generation=2, timeout_seconds=timeout_seconds, request_id=113_000 + index,
            ))
        else:
            reconnect_tasks.append(_okx_session(
                role, batch, generation=2, timeout_seconds=timeout_seconds, request_id=f"qdl112{index}g2",
            ))
    return primary + tuple(await asyncio.gather(*reconnect_tasks))


def _report(
    admission_plan: ProviderAdmissionPlan,
    sessions: Iterable[SessionEvidence],
    *,
    elapsed_seconds: float,
    cpu_seconds: float,
    max_rss_kib: int,
    max_bindings_per_session: int,
    rest_concurrency: int,
) -> dict[str, Any]:
    sessions = tuple(sessions)
    by_binding: dict[str, list[FrameObservation]] = defaultdict(list)
    roles_by_binding: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    for session in sessions:
        for observation in session.observations:
            by_binding[observation.binding_id].append(observation)
            roles_by_binding[observation.binding_id].add(session.role)
    reconnect_roles = {
        session.role for session in sessions
        if session.transport == "WEBSOCKET" and session.generation == 2
    }
    expected_native_roles = {
        (item.venue, item.market, item.feed.value)
        for item in admission_plan.bindings
        if item.mode == "RUST_NATIVE"
    }
    if reconnect_roles != expected_native_roles:
        raise ProviderAdmissionError(
            "provider reconnect evidence is incomplete "
            f"expected={len(expected_native_roles)} observed={len(reconnect_roles)}"
        )
    rows = []
    for item in admission_plan.bindings:
        observations = by_binding.get(item.binding_id, [])
        if not observations:
            raise ProviderAdmissionError(f"provider evidence missing binding={item.binding_id}")
        if item.feed.value == "BAR":
            if len(observations) != 1 or not observations[0].final_bar or observations[0].transport != "HTTP":
                raise ProviderAdmissionError(f"final BAR evidence is incomplete binding={item.binding_id}")
        else:
            matching_roles = {(item.venue, item.market, item.feed.value)}
            if not matching_roles <= roles_by_binding[item.binding_id]:
                raise ProviderAdmissionError(f"native role evidence is incomplete binding={item.binding_id}")
            if not any(
                observation.generation == 1 and observation.transport == "WEBSOCKET"
                for observation in observations
            ):
                raise ProviderAdmissionError(
                    f"native primary-generation evidence is missing binding={item.binding_id}"
                )
        latest = max(observations, key=lambda value: value.observed_at_ms)
        age_ms = max(0, latest.observed_at_ms - latest.source_time_ms)
        if age_ms > item.stale_after_ms:
            raise ProviderAdmissionError(
                f"provider evidence is stale binding={item.binding_id} age_ms={age_ms} limit_ms={item.stale_after_ms}"
            )
        rows.append(
            {
                "binding_id": item.binding_id,
                "instrument_uid": item.instrument_uid,
                "instrument_id": item.instrument_id,
                "venue": item.venue,
                "market": item.market,
                "product_type": item.product_type,
                "native_symbol": item.native_symbol,
                "feed": item.feed.value,
                "interval": item.interval,
                "acquisition_mode": item.mode,
                "provider_kind": item.provider_kind,
                "native_channel": item.native_channel,
                "source_to_capture_age_ms": age_ms,
                "source_time_missing": latest.source_time_missing,
                "transport": latest.transport,
                "final_bar_observed": latest.final_bar,
                "reconnect_probed": (item.venue, item.market, item.feed.value) in reconnect_roles,
                "last_frame_sha256": latest.frame_sha256,
            }
        )
    native_sessions = [item for item in sessions if item.transport == "WEBSOCKET"]
    return {
        "schema": "qdl.phase112.universal-realtime-provider-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_DIRECT_READ_ONLY",
        "inventory_sha256": admission_plan.plan.inventory_sha256,
        "plan": admission_plan.plan.report_payload(),
        "binding_count": len(rows),
        "websocket_binding_count": sum(item["acquisition_mode"] == "RUST_NATIVE" for item in rows),
        "rest_final_bar_binding_count": sum(item["acquisition_mode"] == "PYTHON_REST" for item in rows),
        "accepted_missing_instrument_count": len(admission_plan.accepted_missing),
        "accepted_missing_instruments": list(admission_plan.accepted_missing),
        "deferred_requirement_count": len(admission_plan.deferred_requirement_ids),
        "native_session_count": len(native_sessions),
        "native_shared_role_count": len({item.role for item in native_sessions if item.generation == 1}),
        "intentional_reconnect_role_count": len(reconnect_roles),
        "subscription_ack_count": sum(item.ack_count for item in native_sessions),
        "accepted_provider_frame_count": sum(item.event_count for item in sessions),
        "source_time_missing_binding_count": sum(item["source_time_missing"] for item in rows),
        "max_bindings_per_native_session": max_bindings_per_session,
        "max_rest_concurrency": rest_concurrency,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "process_cpu_seconds": round(cpu_seconds, 6),
        "process_max_rss_kib": max_rss_kib,
        "bindings": rows,
        "raw_provider_frames_persisted": 0,
        "runtime_mutations": 0,
        "production_writes": 0,
        "fallback_count": 0,
        "scope": "isolated provider protocol admission; no Data Layer runtime role was started",
    }


async def run(
    *,
    source_registry: Path = DEFAULT_SOURCE_REGISTRY,
    repository_root: Path = QDL_ROOT,
    execution_alpha_root: Path = DEFAULT_EXECUTION_ALPHA_ROOT,
    trading_system_root: Path = DEFAULT_TRADING_SYSTEM_ROOT,
    metadata_timeout_seconds: float = 15.0,
    metadata_attempts: int = 4,
    native_timeout_seconds: float = 90.0,
    max_bindings_per_session: int = _MAX_NATIVE_BINDINGS_PER_SESSION,
    rest_concurrency: int = 16,
) -> dict[str, Any]:
    if not 5.0 <= metadata_timeout_seconds <= 60.0:
        raise ValueError("metadata timeout is outside bounds")
    if not 1 <= metadata_attempts <= 5:
        raise ValueError("metadata attempts are outside bounds")
    if not 30.0 <= native_timeout_seconds <= 120.0:
        raise ValueError("native timeout is outside bounds")
    started_wall = time.monotonic()
    started_cpu = time.process_time()
    try:
        admission_plan = _build_plan(
            source_registry=source_registry,
            repository_root=repository_root,
            execution_alpha_root=execution_alpha_root,
            trading_system_root=trading_system_root,
            metadata_timeout_seconds=metadata_timeout_seconds,
            metadata_attempts=metadata_attempts,
        )
    except InventoryError as error:
        raise ProviderAdmissionError(f"active-demand compile/admission failed: {error}") from error
    final_bars = tuple(item for item in admission_plan.bindings if item.feed.value == "BAR")
    native = tuple(item for item in admission_plan.bindings if item.feed.value in {"TRADE", "QUOTE"})
    if len(final_bars) + len(native) != len(admission_plan.bindings):
        raise ProviderAdmissionError("universal provider projection contains an unsupported feed")
    rest_sessions, native_sessions = await asyncio.gather(
        _rest_bar_observations(final_bars, max_concurrency=rest_concurrency),
        _native_observations(
            native,
            max_bindings_per_session=max_bindings_per_session,
            timeout_seconds=native_timeout_seconds,
        ),
    )
    return _report(
        admission_plan,
        rest_sessions + native_sessions,
        elapsed_seconds=time.monotonic() - started_wall,
        cpu_seconds=time.process_time() - started_cpu,
        max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        max_bindings_per_session=max_bindings_per_session,
        rest_concurrency=rest_concurrency,
    )


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-registry", type=Path, default=DEFAULT_SOURCE_REGISTRY)
    parser.add_argument("--repository-root", type=Path, default=QDL_ROOT)
    parser.add_argument("--execution-alpha-root", type=Path, default=DEFAULT_EXECUTION_ALPHA_ROOT)
    parser.add_argument("--trading-system-root", type=Path, default=DEFAULT_TRADING_SYSTEM_ROOT)
    parser.add_argument("--metadata-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--metadata-attempts", type=int, default=4)
    parser.add_argument("--native-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--max-bindings-per-session", type=int, default=_MAX_NATIVE_BINDINGS_PER_SESSION)
    parser.add_argument("--rest-concurrency", type=int, default=16)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = asyncio.run(run(
            source_registry=args.source_registry,
            repository_root=args.repository_root,
            execution_alpha_root=args.execution_alpha_root,
            trading_system_root=args.trading_system_root,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
            metadata_attempts=args.metadata_attempts,
            native_timeout_seconds=args.native_timeout_seconds,
            max_bindings_per_session=args.max_bindings_per_session,
            rest_concurrency=args.rest_concurrency,
        ))
    except (ProviderAdmissionError, ValueError) as error:
        print(f"phase112 provider admission: FAIL: {error}")
        return 1
    _write_report(args.output, report)
    print(json.dumps({
        "status": report["status"],
        "binding_count": report["binding_count"],
        "websocket_binding_count": report["websocket_binding_count"],
        "rest_final_bar_binding_count": report["rest_final_bar_binding_count"],
        "native_session_count": report["native_session_count"],
        "elapsed_seconds": report["elapsed_seconds"],
        "output": str(args.output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

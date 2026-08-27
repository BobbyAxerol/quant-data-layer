#!/usr/bin/env python3
"""Bounded read-only Phase 11.4 L2 provider capture.

This verifier proves documented public-provider wire semantics only. It never
starts Data Layer roles, writes Kafka/Redis/SQLite, changes routes or stores raw
provider frames. The committed report keeps identifiers, timestamps, counts
and SHA-256 digests only.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import resource
import time
from typing import Any, Mapping, Sequence

import httpx
from websockets.asyncio.client import connect


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ADMISSION = ROOT / "upgrade/evidence/phase111-active-demand-provider-admission.json"
DEFAULT_OUTPUT = ROOT / "upgrade/evidence/phase114-l2-real-provider-capture.json"
BINANCE_WS = "wss://fstream.binance.com/ws"
BINANCE_DEPTH = "https://fapi.binance.com/fapi/v1/depth"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"


class ProviderCaptureError(RuntimeError):
    """A provider frame cannot prove the required execution-grade contract."""


@dataclass(frozen=True, slots=True)
class BinanceReplay:
    symbol: str
    snapshot_sequence: int
    bridge_start: int
    bridge_end: int
    final_sequence: int
    frame_count: int
    frame_sha256: str


@dataclass(frozen=True, slots=True)
class OkxReplay:
    symbol: str
    snapshot_sequence: int
    final_sequence: int
    update_count: int
    reset_count: int
    frame_count: int
    frame_sha256: str


def _json_object(raw: str | bytes) -> tuple[str, Mapping[str, Any]]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    value = json.loads(raw)
    if not isinstance(value, Mapping):
        raise ProviderCaptureError("provider frame is not a JSON object")
    return raw, value


def _int(value: object, field: str) -> int:
    try:
        result = int(str(value))
    except (TypeError, ValueError) as error:
        raise ProviderCaptureError(f"provider field is not an integer: {field}") from error
    if result < 0:
        raise ProviderCaptureError(f"provider field is negative: {field}")
    return result


def _digest(frames: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for frame in frames:
        digest.update(hashlib.sha256(frame.encode("utf-8")).digest())
    return digest.hexdigest()


def active_binance_book_symbols(document: Mapping[str, Any]) -> tuple[str, ...]:
    """Return all currently admitted active Binance USD-M L2 requirements."""

    rows = document.get("rows")
    if not isinstance(rows, list):
        raise ProviderCaptureError("active-demand admission has no rows list")
    symbols = {
        str(row.get("native_symbol", "")).strip().upper()
        for row in rows
        if isinstance(row, Mapping)
        and row.get("state") == "ADMITTED"
        and row.get("venue") == "BINANCE"
        and row.get("market") == "USDM"
        and row.get("feed") in {"BOOK_SNAPSHOT", "BOOK_DELTA"}
    }
    if not symbols or "" in symbols:
        raise ProviderCaptureError("active-demand admission has no valid Binance L2 symbol")
    return tuple(sorted(symbols))


def validate_binance_replay(
    *,
    symbol: str,
    snapshot_sequence: int,
    frames: Sequence[Mapping[str, Any]],
    raw_frames: Sequence[str],
    min_chain_frames: int = 2,
) -> BinanceReplay | None:
    """Validate documented snapshot/range/`pu` continuity in arrival order.

    `None` means more bounded frames are required; a true discontinuity raises
    rather than being hidden by a later provider frame.
    """

    if min_chain_frames < 1:
        raise ValueError("min_chain_frames must be positive")
    expected = snapshot_sequence + 1
    final_sequence: int | None = None
    bridge_start: int | None = None
    bridge_end: int | None = None
    accepted = 0
    for frame in frames:
        if frame.get("e") != "depthUpdate" or str(frame.get("s", "")).upper() != symbol:
            continue
        start = _int(frame.get("U"), "U")
        end = _int(frame.get("u"), "u")
        if start > end:
            raise ProviderCaptureError(f"Binance {symbol} has an invalid U/u range")
        if final_sequence is None:
            if end < expected:
                continue
            if start <= expected <= end:
                bridge_start, bridge_end, final_sequence = start, end, end
                accepted = 1
            continue
        if end <= final_sequence:
            # Provider duplicate is idempotent and cannot become a replay event.
            continue
        previous = _int(frame.get("pu"), "pu")
        if previous != final_sequence:
            raise ProviderCaptureError(
                f"Binance {symbol} depth continuity failed: pu={previous} "
                f"last={final_sequence}"
            )
        final_sequence = end
        accepted += 1
    if (
        final_sequence is None
        or bridge_start is None
        or bridge_end is None
        or accepted < min_chain_frames
    ):
        return None
    return BinanceReplay(
        symbol=symbol,
        snapshot_sequence=snapshot_sequence,
        bridge_start=bridge_start,
        bridge_end=bridge_end,
        final_sequence=final_sequence,
        frame_count=accepted,
        frame_sha256=_digest(raw_frames),
    )


def validate_okx_replay(
    *,
    symbol: str,
    frames: Sequence[Mapping[str, Any]],
    raw_frames: Sequence[str],
) -> OkxReplay | None:
    """Validate OKX `books` snapshot/update continuity without invented CRC."""

    snapshot_sequence: int | None = None
    final_sequence: int | None = None
    updates = 0
    resets = 0
    for frame in frames:
        argument = frame.get("arg")
        rows = frame.get("data")
        if not isinstance(argument, Mapping) or argument.get("channel") != "books":
            continue
        if str(argument.get("instId", "")).upper() != symbol:
            continue
        action = frame.get("action")
        # Subscription ACKs carry the same `arg` but are not book state.
        if action not in {"snapshot", "update"}:
            continue
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], Mapping):
            raise ProviderCaptureError(f"OKX {symbol} books frame has invalid data cardinality")
        row = rows[0]
        sequence = _int(row.get("seqId"), "seqId")
        if action == "snapshot":
            snapshot_sequence = sequence
            final_sequence = sequence
            updates = 0
            continue
        if action != "update" or final_sequence is None:
            continue
        if str(row.get("prevSeqId")) == "-1":
            # OKX documented maintenance reset invalidates the active snapshot;
            # wait for a new snapshot and do not manufacture continuity.
            snapshot_sequence = None
            final_sequence = None
            updates = 0
            resets += 1
            continue
        previous = _int(row.get("prevSeqId"), "prevSeqId")
        if previous != final_sequence:
            raise ProviderCaptureError(
                f"OKX {symbol} books continuity failed: prevSeqId={previous} "
                f"last={final_sequence}"
            )
        final_sequence = sequence
        updates += 1
        if snapshot_sequence is not None and updates >= 1:
            return OkxReplay(
                symbol=symbol,
                snapshot_sequence=snapshot_sequence,
                final_sequence=final_sequence,
                update_count=updates,
                reset_count=resets,
                frame_count=updates + 1,
                frame_sha256=_digest(raw_frames),
            )
    return None


async def _recv_json(socket, deadline: float) -> tuple[str, Mapping[str, Any]]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ProviderCaptureError("provider capture timed out")
    return _json_object(await asyncio.wait_for(socket.recv(), timeout=remaining))


async def _binance_snapshots(symbols: Sequence[str]) -> dict[str, Mapping[str, Any]]:
    async with httpx.AsyncClient(timeout=8.0) as client:
        responses = await asyncio.gather(
            *(client.get(BINANCE_DEPTH, params={"symbol": symbol, "limit": 100}) for symbol in symbols)
        )
    snapshots: dict[str, Mapping[str, Any]] = {}
    for symbol, response in zip(symbols, responses, strict=True):
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, Mapping) or not isinstance(value.get("bids"), list) or not isinstance(value.get("asks"), list):
            raise ProviderCaptureError(f"Binance {symbol} REST depth response is malformed")
        _int(value.get("lastUpdateId"), "lastUpdateId")
        snapshots[symbol] = value
    return snapshots


async def capture_binance(symbols: Sequence[str], deadline: float, max_frames: int) -> tuple[BinanceReplay, ...]:
    frames: dict[str, list[Mapping[str, Any]]] = {symbol: [] for symbol in symbols}
    raw_frames: dict[str, list[str]] = {symbol: [] for symbol in symbols}
    async with connect(BINANCE_WS, open_timeout=8, close_timeout=4, ping_interval=10) as socket:
        await socket.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@depth@100ms" for symbol in symbols],
            "id": 114,
        }))
        while any(not values for values in frames.values()):
            raw, frame = await _recv_json(socket, deadline)
            symbol = str(frame.get("s", "")).upper()
            if frame.get("e") == "depthUpdate" and symbol in frames:
                frames[symbol].append(frame)
                raw_frames[symbol].append(raw)
        # Keep draining the diff stream while REST establishes its sequence
        # anchor. Awaiting REST directly here creates exactly the race that the
        # documented Binance bootstrap algorithm is intended to prevent.
        snapshots_task = asyncio.create_task(_binance_snapshots(symbols))
        while not snapshots_task.done():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProviderCaptureError("Binance REST/bootstrap capture timed out")
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=min(0.25, remaining))
            except TimeoutError:
                continue
            raw, frame = _json_object(raw)
            symbol = str(frame.get("s", "")).upper()
            if frame.get("e") == "depthUpdate" and symbol in frames:
                if len(frames[symbol]) >= max_frames:
                    raise ProviderCaptureError(f"Binance {symbol} exceeded bounded capture frames")
                frames[symbol].append(frame)
                raw_frames[symbol].append(raw)
        snapshots = await snapshots_task
        while True:
            accepted = []
            for symbol in symbols:
                replay = validate_binance_replay(
                    symbol=symbol,
                    snapshot_sequence=_int(snapshots[symbol].get("lastUpdateId"), "lastUpdateId"),
                    frames=frames[symbol],
                    raw_frames=raw_frames[symbol],
                )
                if replay is not None:
                    accepted.append(replay)
            if len(accepted) == len(symbols):
                return tuple(accepted)
            raw, frame = await _recv_json(socket, deadline)
            symbol = str(frame.get("s", "")).upper()
            if frame.get("e") == "depthUpdate" and symbol in frames:
                if len(frames[symbol]) >= max_frames:
                    raise ProviderCaptureError(f"Binance {symbol} exceeded bounded capture frames")
                frames[symbol].append(frame)
                raw_frames[symbol].append(raw)


async def capture_okx(symbol: str, deadline: float, max_frames: int) -> OkxReplay:
    frames: list[Mapping[str, Any]] = []
    raw_frames: list[str] = []
    async with connect(OKX_WS, open_timeout=8, close_timeout=4, ping_interval=10) as socket:
        await socket.send(json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}))
        while True:
            raw, frame = await _recv_json(socket, deadline)
            argument = frame.get("arg")
            if not isinstance(argument, Mapping) or argument.get("channel") != "books":
                continue
            if str(argument.get("instId", "")).upper() != symbol:
                continue
            if frame.get("action") not in {"snapshot", "update"}:
                continue
            if len(frames) >= max_frames:
                raise ProviderCaptureError(f"OKX {symbol} exceeded bounded capture frames")
            frames.append(frame)
            raw_frames.append(raw)
            replay = validate_okx_replay(symbol=symbol, frames=frames, raw_frames=raw_frames)
            if replay is not None:
                return replay


async def run(
    *,
    admission_path: Path,
    okx_symbol: str,
    timeout_seconds: float,
    max_frames: int,
) -> dict[str, Any]:
    document = json.loads(admission_path.read_text())
    if not isinstance(document, Mapping):
        raise ProviderCaptureError("active-demand admission document is invalid")
    symbols = active_binance_book_symbols(document)
    started_ns = time.time_ns()
    deadline = time.monotonic() + timeout_seconds
    binance, okx = await asyncio.gather(
        capture_binance(symbols, deadline, max_frames),
        capture_okx(okx_symbol, deadline, max_frames),
    )
    elapsed_ms = (time.time_ns() - started_ns) // 1_000_000
    return {
        "schema": "qdl.phase114.l2-real-provider-capture.v1",
        "status": "PASS",
        "provenance": ["REAL_BINANCE_USDM_PUBLIC_WS_REST", "REAL_OKX_V5_PUBLIC_WS"],
        "runtime_mutations": 0,
        "production_writes": 0,
        "raw_provider_bytes_persisted": 0,
        "admission_sha256": hashlib.sha256(admission_path.read_bytes()).hexdigest(),
        "started_at_ns": started_ns,
        "elapsed_ms": int(elapsed_ms),
        "limits": {"timeout_seconds": timeout_seconds, "max_frames_per_symbol": max_frames},
        "resource": {"max_rss_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)},
        "binance_usdm": [asdict(item) for item in binance],
        "okx_swap": asdict(okx),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--admission", type=Path, default=DEFAULT_ADMISSION)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--okx-symbol", default="BTC-USDT-SWAP")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--max-frames", type=int, default=64)
    args = parser.parse_args()
    if not 10.0 <= args.timeout_seconds <= 60.0:
        parser.error("--timeout-seconds must be within [10, 60]")
    if not 4 <= args.max_frames <= 128:
        parser.error("--max-frames must be within [4, 128]")
    report = asyncio.run(
        run(
            admission_path=args.admission,
            okx_symbol=str(args.okx_symbol).upper(),
            timeout_seconds=args.timeout_seconds,
            max_frames=args.max_frames,
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "status": report["status"],
        "binance_symbols": len(report["binance_usdm"]),
        "okx_updates": report["okx_swap"]["update_count"],
        "elapsed_ms": report["elapsed_ms"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()

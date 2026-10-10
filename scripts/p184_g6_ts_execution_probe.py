#!/usr/bin/env python3
"""P18.4 gate G6: read-only probe of the TS writer's own EXECUTION reads, classified against the frozen D3 policy.

Purpose: the frozen G6 gate (TS upgrade/p184-phase-d-gates.md) is the per-binding D3 policy of the TS EXECUTION
consumer (TRADE 3000, QUOTE/MARK 2000 ms, ...; "effective bound = applicable manifest/source policy plus stricter
consumer intent policy, at use time"). The stage-100 load reads as ALPHA consumers and cannot measure it. This probe runs
INSIDE the TS writer container, so it uses the TS image's qdl_sdk, the writer's settings, consumer identity, sealed
binding and the exact EXECUTION requirement the writer builds (DataLayerV2Facade.requirement; the MARK reference
requirement is the one latest_execution_feed builds), and classifies every returned view BEFORE client validation with
the same pure functions as the stage-100 driver (g6_policy, g6_classify, g6_reference_*; copied verbatim below because
the DL package is not in the TS image; tests/test_p184_g6_view_quality.py pins parity with
scripts/phase3_consumer_load_acceptance.py).

Usage (from the DL repo on the host):
  docker exec -i market_data_service_p184w python -B - --duration 1800 < scripts/p184_g6_ts_execution_probe.py

Reads: GetSnapshot QUOTE and TRADE, and the MARK/INDEX reference batch (one item), for every EXECUTION route of the
writer's sealed binding on BINANCE and OKX, round-robin, one read per binding every --period seconds at most and never
above --max-rps in total (default 10). Bounded: --duration (default 1800 s) plus a hard deadline; SIGTERM/SIGINT stop it
early with a final summary. Output: JSON lines on stdout only, first {"kind": "start", "pid": <in-container pid>}, then
optional cumulative {"kind": "partial"} lines every --emit-seconds, last {"kind": "final"}; rows use the campaign's G6
quality-row shape with policy_basis.

Boundary: Query reads only, as the writer's identity. No Redis, no cursor commit, no file write in the container: the
client is built with its cursor and audit directories redirected to /sys (read-only sysfs, already present), so any
accidental write fails instead of touching the writer's state. Nothing is restarted or recreated.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import signal
import statistics
import sys
import time

# ---------------------------------------------------------------- pure: verbatim copies (parity-tested)

_G6_POLICY_MS = {"TRADE": 3000, "QUOTE": 2000, "MARK_INDEX_PRICE": 2000, "BOOK_DELTA": 2000,
                 "BOOK_SNAPSHOT": 60000, "BAR|1m": 180000}
G6_D3_BASIS = "D3_EXECUTION"


def g6_policy(feed: str, interval: str | None, *, grade: object,
              requirement_max_ms: int | None) -> tuple[int | None, str]:
    """(policy_ms, policy_basis) of one read. Pure: unit-tested and copied into the TS execution probe."""
    grade_name = str(getattr(grade, "value", grade) or "UNKNOWN").upper()
    if grade_name != "EXECUTION":
        return requirement_max_ms, f"REQUIREMENT_{grade_name}"
    d3 = _G6_POLICY_MS.get(f"BAR|{interval}" if feed == "BAR" else feed)
    bounds = [bound for bound in (d3, requirement_max_ms) if bound is not None]
    return (min(bounds) if bounds else None), G6_D3_BASIS


def g6_classify(*, feed: str, freshness_ms: int | None, eligible: bool, state: str | None, gap_open: bool,
                complete: bool, event_recency_state: str | None, provider_session_state: str | None,
                session_liveness_ms: int | None, max_session_liveness_ms: int | None, observe: bool,
                on_change: bool, policy_ms: int | None) -> tuple[str, list[str]]:
    """ELIGIBLE / ELIGIBLE_QUIET (beyond the event threshold with a proven live session under OBSERVE) /
    NOT_ELIGIBLE / UNSAFE (eligible although the policy rejects it). Pure: unit-tested."""
    beyond = freshness_ms is not None and policy_ms is not None and freshness_ms > policy_ms
    session_ok = (provider_session_state == "LIVE"
                  and (max_session_liveness_ms is None
                       or (session_liveness_ms is not None and session_liveness_ms <= max_session_liveness_ms)))
    quiet_proven = (observe and (feed in {"TRADE", "BOOK_DELTA"} or (feed == "QUOTE" and on_change))
                    and event_recency_state == "STALE" and session_ok)
    if not eligible:
        return "NOT_ELIGIBLE", []
    reasons = []
    if gap_open:
        reasons.append("GAP_OPEN")
    if not complete:
        reasons.append("INCOMPLETE")
    if state not in (None, "LIVE"):
        reasons.append(f"STATE_{state}")
    if freshness_ms is None:
        reasons.append("AGE_UNKNOWN")
    elif beyond and not quiet_proven:
        reasons.append("AGE_BEYOND_POLICY")
    if (max_session_liveness_ms is not None and provider_session_state not in (None, "NOT_APPLICABLE")
            and not session_ok):
        reasons.append("SESSION_BEYOND_POLICY")
    if reasons:
        return "UNSAFE", reasons
    return ("ELIGIBLE_QUIET" if beyond else "ELIGIBLE"), []


def g6_reference_anchor_ns(observations) -> int | None:
    """The server's freshness anchor of a current MARK/INDEX result (qdl/query/service.py
    _reference_freshness_timestamp): min provider_confirmation_ns when every observation is PROVIDER_CONFIRMATION
    based, else min observed_at_ns when every one is SOURCE_EVENT (the default without a label); None otherwise.
    observations: [(observed_at_ns, labels mapping)]. Pure."""
    items = list(observations or [])
    if not items:
        return None
    bases = {str((labels or {}).get("freshness_basis", "SOURCE_EVENT")) for _, labels in items}
    try:
        if bases == {"PROVIDER_CONFIRMATION"}:
            values = [int(labels["provider_confirmation_ns"]) for _, labels in items]
        elif bases == {"SOURCE_EVENT"}:
            values = [int(observed) for observed, _ in items]
        else:
            return None
    except (KeyError, TypeError, ValueError):
        return None
    return min(values) if all(value > 0 for value in values) else None


def g6_reference_age(*, grade: object, observations, sent_ns: int, receipt_ns: int) -> dict[str, object]:
    """Age of a MARK reference result. A reference result carries no served-at time, only the server's freshness
    anchor (g6_reference_anchor_ns). EXECUTION keeps the unchanged use-time basis (client receipt minus the newest
    source event), so what counts as unsafe for EXECUTION reads does not move. Any other grade is aged from the
    request-sent time against the server's anchor: the server served at or after sent_ns, so an age beyond the policy
    on this basis is beyond it at serve time too, and the network round trip can no longer manufacture UNSAFE.
    client_age_ms (receipt minus the server anchor, includes the round trip) is kept for every grade. Pure."""
    items = list(observations or [])
    anchor = g6_reference_anchor_ns(items)
    client_age = None if anchor is None else max(0, (receipt_ns - anchor) // 1_000_000)
    if str(getattr(grade, "value", grade) or "").upper() == "EXECUTION":
        newest = max((int(observed) for observed, _ in items), default=0)
        age = max(0, (receipt_ns - newest) // 1_000_000) if newest > 0 else None
        basis = "CLIENT_RECEIPT_MINUS_NEWEST_SOURCE_EVENT"
    else:
        age = None if anchor is None else max(0, (sent_ns - anchor) // 1_000_000)
        basis = "REQUEST_SENT_MINUS_SERVER_FRESHNESS_ANCHOR"
    return {"age_ms": age, "client_age_ms": client_age, "age_basis": basis}


def g6_reference_classify(*, age_ms: int | None, policy_ms: int | None, quiet_proven: bool) -> tuple[str, list[str]]:
    """An OK MARK reference result is a served (eligible) view: UNSAFE when beyond its policy without a quiet proof,
    ELIGIBLE_QUIET with one. Pure."""
    if age_ms is None:
        return "UNSAFE", ["AGE_UNKNOWN"]
    if policy_ms is not None and age_ms > policy_ms:
        return ("ELIGIBLE_QUIET", []) if quiet_proven else ("UNSAFE", ["STALE_SERVED_WITHOUT_QUIET_PROOF"])
    return "ELIGIBLE", []


# ---------------------------------------------------------------- pure: aggregation in the campaign row shape

_RESERVOIR = 20000
_EXECUTION_MARK_INDEX_LIVE_ENDPOINT = "qdl://stable-stream/internal/v2/execution/mark-index/latest"
_EXECUTION_MARK_INDEX_DELIVERY_STAGES = frozenset({"CANONICAL_READ_COMMITTED", "SPOOL_CONFIRMED"})


def percentiles(values: list[int]) -> dict[str, float | int | None]:
    if not values:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None, "max_ms": None}
    ordered = sorted(values)
    return {"n": len(ordered), "p50_ms": round(statistics.median(ordered), 3),
            "p95_ms": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
            "p99_ms": round(ordered[math.ceil(len(ordered) * 0.99) - 1], 3) if len(ordered) >= 100 else None,
            "max_ms": round(ordered[-1], 3)}


class Rows:
    """Per-key exact counts and max; ages kept up to _RESERVOIR per key (first-come; the probe is rate-bounded)."""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def _row(self, key: str) -> dict:
        return self.rows.setdefault(key, {"n": 0, "ages": [], "max_age_ms": None, "max_client_age_ms": None,
                                          "policy_ms": None, "policy_basis": None, "age_basis": None,
                                          "classes": {}, "reasons": {}, "refusals": {}, "requirement": None,
                                          "unsafe_examples": []})

    def view(self, key: str, observation: dict) -> None:
        row = self._row(key)
        row["n"] += 1
        for name in ("policy_ms", "policy_basis", "requirement"):
            row[name] = observation.get(name, row[name])
        row["age_basis"] = observation.get("age_basis") or row["age_basis"]
        row["classes"][observation["class"]] = row["classes"].get(observation["class"], 0) + 1
        for reason in observation.get("reasons") or []:
            row["reasons"][reason] = row["reasons"].get(reason, 0) + 1
        for field, top in (("age_ms", "max_age_ms"), ("client_age_ms", "max_client_age_ms")):
            value = observation.get(field)
            if isinstance(value, int):
                row[top] = value if row[top] is None else max(row[top], value)
        age = observation.get("age_ms")
        if isinstance(age, int) and len(row["ages"]) < _RESERVOIR:
            row["ages"].append(age)
        if observation["class"] == "UNSAFE" and len(row["unsafe_examples"]) < 16:
            row["unsafe_examples"].append({k: observation.get(k) for k in ("symbol", "age_ms", "client_age_ms",
                                                                          "reasons", "state", "recency")}
                                          | {"at_ns": time.time_ns()})

    def refusal(self, key: str, code: str) -> None:
        row = self._row(key)
        row["refusals"][code] = row["refusals"].get(code, 0) + 1

    def summary(self) -> dict:
        out = {}
        for key, row in sorted(self.rows.items()):
            out[key] = {**{k: v for k, v in row.items() if k != "ages"},
                        "age_ms": {**percentiles(row["ages"]), "basis": f"first {_RESERVOIR} ages per key "
                                                                        "(exact n and max above)"},
                        "unsafe": row["classes"].get("UNSAFE", 0)}
        return out


# ---------------------------------------------------------------- runtime (TS image only)

class _ReadOnlyStateSettings:
    """The writer's settings with the cursor/audit directories redirected to /sys: the client constructor only
    mkdir(exist_ok)s an existing directory there, and any write would fail on sysfs instead of reaching the
    writer's cursor or audit files."""

    def __init__(self, settings) -> None:
        self._settings = settings

    def __getattr__(self, name):
        if name in ("DATA_LAYER_V2_CURSOR_DIR", "DATA_LAYER_V2_AUDIT_DIR"):
            return "/sys"
        return getattr(self._settings, name)


def _value(item) -> str | None:
    return None if item is None else str(getattr(item, "value", item))


def _requirement_summary(requirement) -> dict:
    return {"grade": _value(getattr(requirement, "consumer_grade", None)),
            "max_freshness_ms": getattr(requirement, "max_freshness_ms", None),
            "event_recency_policy": _value(getattr(requirement, "event_recency_policy", None)),
            "max_session_liveness_ms": getattr(requirement, "max_session_liveness_ms", None)}


def _mark_quiet_proven(request, data, *, at_ns: int) -> bool:
    """Same acceptance as qdl/certification/reference_l2_acceptance.py _quiet_execution_mark_index_evidence for an
    EXECUTION OBSERVE request (lineage, provenance labels, then the SDK's quiet-evidence validator)."""
    from qdl_sdk.execution_quality import validate_quiet_execution_mark_index_evidence

    if (_value(getattr(request, "consumer_grade", None)) != "EXECUTION"
            or _value(getattr(request, "event_recency_policy", None)) != "OBSERVE"
            or not getattr(request, "max_session_liveness_ms", None)):
        return False
    try:
        if (data is None or len(data.observations) != 1 or len(data.lineage) != 1
                or data.lineage[0].provider_endpoint != _EXECUTION_MARK_INDEX_LIVE_ENDPOINT
                or data.lineage[0].source_role != "REFERENCE"
                or data.coverage.terminal_reason != "LIVE_EXECUTION_VIEW"):
            return False
        observation = data.observations[0]
        labels = dict(observation.labels or {})
        source_ns, confirmation_ns = int(labels["source_event_time_ns"]), int(labels["provider_confirmation_ns"])
        watermark = labels["spool_watermark_offset"]
        if (labels.get("execution_view") != "STABLE_STREAM_GATEWAY"
                or labels.get("freshness_basis") not in {"SOURCE_EVENT", "PROVIDER_CONFIRMATION"}
                or labels.get("delivery_stage") not in _EXECUTION_MARK_INDEX_DELIVERY_STAGES
                or source_ns != observation.observed_at_ns or confirmation_ns != data.received_at_ns
                or not 0 < source_ns <= confirmation_ns <= at_ns
                or int(labels["connection_generation"]) < 1 or int(labels["gateway_lease_epoch"]) < 1
                or (watermark != "PENDING" and int(watermark) < 0)):
            return False
        validate_quiet_execution_mark_index_evidence(labels, at_ns=at_ns,
                                                     max_session_liveness_ms=request.max_session_liveness_ms)
        return True
    except Exception:
        return False


def _targets(client) -> list[dict]:
    """EXECUTION routes of the writer's sealed binding for the probed feeds on BINANCE and OKX."""
    binding = getattr(client, "binding", None)
    if binding is None:
        raise RuntimeError("the writer client has no sealed consumer binding")
    out = []
    for route in binding.routes:
        identity = route.identity
        if (identity.venue in ("BINANCE", "OKX") and identity.feed in ("QUOTE", "TRADE", "MARK_INDEX_PRICE")
                and route.execution_grade):
            market = "spot" if identity.market == "SPOT" else "auto"
            out.append({"provider": identity.venue.lower(), "symbol": identity.native_symbol, "market": market,
                        "feed": identity.feed, "venue": identity.venue})
    return sorted(out, key=lambda t: (t["feed"], t["venue"], t["symbol"]))


async def _read(client, target: dict, rows: Rows, *, timeout: float) -> None:
    from qdl_sdk import Feed, Grade, ReferenceProduct, ReferenceRequirement, StalePolicy

    facade = client.v2
    feed = Feed(target["feed"])
    route = client.v2_route_requirement(target["provider"], target["symbol"], feed=feed, market=target["market"])
    instrument = await asyncio.wait_for(facade.resolve(target["provider"], target["symbol"], market=target["market"]),
                                        timeout)
    requirement = facade.requirement(instrument, feed, route_requirement=route)
    if feed is Feed.MARK_INDEX_PRICE:
        request = ReferenceRequirement(
            instrument_uid=instrument.instrument_uid, product=ReferenceProduct.MARK_INDEX_PRICE,
            consumer_grade=Grade.EXECUTION, source_policy_id=requirement.source_policy_id, limit=1, page_size=1,
            max_pages=1, max_freshness_ms=requirement.max_freshness_ms,
            event_recency_policy=requirement.event_recency_policy,
            max_session_liveness_ms=requirement.max_session_liveness_ms, require_full_coverage=True)
        policy_ms, basis = g6_policy(target["feed"], None, grade=_value(request.consumer_grade),
                                     requirement_max_ms=request.max_freshness_ms)
        key = f"{target['feed']}|{target['venue']}|" + ("" if basis == G6_D3_BASIS else f"@{basis}")
        sent_ns = time.time_ns()
        response = await asyncio.wait_for(client.v2.client.reference_batch((request,), require_all=False), timeout)
        receipt_ns = time.time_ns()
        item = response.results[0] if response.results else None
        data = getattr(item, "data", None)
        observations = list(getattr(data, "observations", None) or [])
        base = {"policy_ms": policy_ms, "policy_basis": basis, "requirement": _requirement_summary(request),
                "symbol": target["symbol"]}
        if getattr(item, "status", None) != "OK" or not observations:
            code = getattr(getattr(item, "problem", None), "code", None) or "NO_OBSERVATION"
            rows.view(key, {**base, "age_ms": None, "class": "NOT_ELIGIBLE", "reasons": [str(code)[:64]]})
            return
        ages = g6_reference_age(grade=_value(request.consumer_grade),
                                observations=[(o.observed_at_ns, dict(o.labels or {})) for o in observations],
                                sent_ns=sent_ns, receipt_ns=receipt_ns)
        verdict, reasons = g6_reference_classify(age_ms=ages["age_ms"], policy_ms=policy_ms,
                                                 quiet_proven=_mark_quiet_proven(request, data, at_ns=receipt_ns))
        rows.view(key, {**base, **ages, "class": verdict, "reasons": reasons})
        return
    policy_ms, basis = g6_policy(target["feed"], None, grade=_value(requirement.consumer_grade),
                                 requirement_max_ms=requirement.max_freshness_ms)
    key = f"{target['feed']}|{target['venue']}|" + ("" if basis == G6_D3_BASIS else f"@{basis}")
    response = await asyncio.wait_for(facade.client.snapshot(requirement), timeout)
    quality = response.data.quality
    verdict, reasons = g6_classify(
        feed=target["feed"], freshness_ms=quality.freshness_ms, eligible=bool(quality.execution_eligible),
        state=_value(quality.state), gap_open=bool(quality.gap_open), complete=bool(quality.complete),
        event_recency_state=_value(getattr(quality, "event_recency_state", None)),
        provider_session_state=_value(getattr(quality, "provider_session_state", None)),
        session_liveness_ms=getattr(quality, "provider_session_liveness_ms", None),
        max_session_liveness_ms=requirement.max_session_liveness_ms,
        observe=requirement.event_recency_policy is StalePolicy.OBSERVE
        or _value(requirement.event_recency_policy) == "OBSERVE",
        on_change="DELIVERY_ON_CHANGE" in [_value(f) for f in (getattr(quality, "flags", None) or [])],
        policy_ms=policy_ms)
    rows.view(key, {"age_ms": quality.freshness_ms, "policy_ms": policy_ms, "policy_basis": basis,
                    "requirement": _requirement_summary(requirement), "symbol": target["symbol"],
                    "class": verdict, "reasons": reasons, "state": _value(quality.state),
                    "recency": _value(getattr(quality, "event_recency_state", None))})


def _emit(kind: str, payload: dict) -> None:
    try:
        sys.stdout.write(json.dumps({"kind": kind, **payload}, sort_keys=True, default=str) + "\n")
        sys.stdout.flush()
    except (BrokenPipeError, OSError):
        os._exit(0)  # the reader is gone; nothing else to do and nothing to clean up


async def run(args) -> int:
    sys.path.insert(0, "/app")
    from adapters.market_data.data_layer_v2 import build_versioned_data_layer_client
    from shared.config import settings

    started = time.time()
    deadline = time.monotonic() + args.duration
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    loop.call_later(args.duration + args.hard_deadline_grace, lambda: os._exit(3))  # hard deadline
    client = build_versioned_data_layer_client(
        _ReadOnlyStateSettings(settings), consumer_id=settings.MARKET_DATA_DATA_LAYER_V2_CONSUMER_ID,
        mode_override=settings.MARKET_DATA_DATA_LAYER_CONSUMER_MODE)
    targets = _targets(client)
    period = max(args.period, len(targets) / args.max_rps) if targets else args.period
    _emit("start", {"pid": os.getpid(), "consumer_id": settings.MARKET_DATA_DATA_LAYER_V2_CONSUMER_ID,
                    "targets": len(targets), "per_binding_period_s": round(period, 3),
                    "rate_rps": round(len(targets) / period, 3) if targets else 0, "duration_s": args.duration})
    rows, errors, reads = Rows(), {}, 0
    step = period / max(1, len(targets))
    next_emit = time.monotonic() + args.emit_seconds if args.emit_seconds > 0 else math.inf
    try:
        index, tick = 0, time.monotonic()
        while targets and not stop.is_set() and time.monotonic() < deadline:
            target = targets[index % len(targets)]
            index += 1
            try:
                await _read(client, target, rows, timeout=args.read_timeout)
                reads += 1
            except asyncio.CancelledError:
                raise
            except Exception as error:  # a read with no view is a typed refusal of that binding
                code = str(getattr(error, "code", None) or type(error).__name__)[:64]
                rows.refusal(f"{target['feed']}|{target['venue']}|", code)
                errors[code] = errors.get(code, 0) + 1
            if time.monotonic() >= next_emit:
                _emit("partial", {"window_unix_s": [started, time.time()], "reads": reads, "errors": errors,
                                  "rows": rows.summary()})
                next_emit += args.emit_seconds
            tick += step
            delay = tick - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=min(delay, max(0.0, deadline - time.monotonic())))
                except asyncio.TimeoutError:
                    pass
            else:
                tick = time.monotonic()  # behind schedule: do not burst to catch up
    finally:
        _emit("final", {"schema": "qdl.p184.g6-ts-execution-probe.v1", "window_unix_s": [started, time.time()],
                        "reads": reads, "errors": errors, "targets": len(targets),
                        "per_binding_period_s": round(period, 3), "stopped_by": "signal" if stop.is_set() else
                        "duration", "rows": rows.summary()})
        try:
            await asyncio.wait_for(client.v2.close(), 5)
        except Exception:
            pass
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--duration", type=float, default=1800.0)
    parser.add_argument("--period", type=float, default=2.0, help="minimum seconds between reads of one binding")
    parser.add_argument("--max-rps", type=float, default=10.0, help="total read-rate ceiling")
    parser.add_argument("--read-timeout", type=float, default=5.0)
    parser.add_argument("--emit-seconds", type=float, default=0.0, help="cumulative partial line period (0: off)")
    parser.add_argument("--hard-deadline-grace", type=float, default=30.0)
    args = parser.parse_args(argv)
    if not (0 < args.duration <= 7200 and args.period > 0 and 0 < args.max_rps <= 20):
        parser.error("duration must be in (0, 7200], period > 0, max-rps in (0, 20]")
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())

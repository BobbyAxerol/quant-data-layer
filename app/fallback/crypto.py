from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.config import OKX_FALLBACK_ENABLED, OKX_FALLBACK_PRIORITY_SYMBOLS, OKX_FALLBACK_STALE_SECONDS


def _event_timestamp_seconds(payload: dict | None) -> float | None:
    if not isinstance(payload, dict):
        return None

    candidates = [
        payload.get("event_time"),
        payload.get("trade_time"),
        payload.get("T"),
        payload.get("E"),
        payload.get("time"),
        payload.get("timestamp"),
    ]
    raw = payload.get("raw")
    if isinstance(raw, dict):
        candidates.extend([raw.get("E"), raw.get("T"), raw.get("t")])

    for value in candidates:
        if value is None:
            continue
        try:
            if isinstance(value, str):
                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return dt.timestamp()
                except ValueError:
                    value = float(value)
            value = float(value)
            if value > 10_000_000_000:
                value = value / 1000.0
            return value
        except Exception:
            continue
    return None


def freshness_status(
    payload: dict | None,
    *,
    now: datetime | None = None,
    stale_seconds: float = OKX_FALLBACK_STALE_SECONDS,
) -> dict:
    now = now or datetime.now(tz=timezone.utc)
    event_ts = _event_timestamp_seconds(payload)
    if payload is None:
        return {
            "available": False,
            "stale": True,
            "age_seconds": None,
            "event_time": None,
            "reason": "BINANCE_MISSING",
        }
    if event_ts is None:
        return {
            "available": True,
            "stale": True,
            "age_seconds": None,
            "event_time": None,
            "reason": "BINANCE_TIMESTAMP_MISSING",
        }
    age = max(0.0, now.timestamp() - event_ts)
    stale = age > stale_seconds
    return {
        "available": True,
        "stale": stale,
        "age_seconds": round(age, 3),
        "event_time": datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat(),
        "reason": "BINANCE_STALE" if stale else "BINANCE_FRESH",
    }


def fallback_decision(
    symbol: str,
    binance_payload: dict | None,
    *,
    force: bool = False,
    enabled: bool = OKX_FALLBACK_ENABLED,
    stale_seconds: float = OKX_FALLBACK_STALE_SECONDS,
    priority_symbols: list[str] | tuple[str, ...] = tuple(OKX_FALLBACK_PRIORITY_SYMBOLS),
    now: datetime | None = None,
) -> dict:
    symbol = symbol.upper().strip()
    freshness = freshness_status(binance_payload, now=now, stale_seconds=stale_seconds)
    is_priority = symbol in {item.upper() for item in priority_symbols}

    if not enabled:
        return {
            "enabled": False,
            "activated": False,
            "reason": "FALLBACK_DISABLED",
            "symbol": symbol,
            "is_priority": is_priority,
            "binance": freshness,
        }

    if force:
        reason = "OPERATOR_FORCED"
        activated = True
    elif freshness["reason"] in {"BINANCE_MISSING", "BINANCE_TIMESTAMP_MISSING"}:
        reason = freshness["reason"]
        activated = True
    elif freshness["stale"]:
        reason = "BINANCE_STALE"
        activated = True
    else:
        reason = "BINANCE_FRESH"
        activated = False

    return {
        "enabled": enabled,
        "activated": activated,
        "reason": reason,
        "symbol": symbol,
        "is_priority": is_priority,
        "binance": freshness,
    }


def okx_reference_payload(
    *,
    symbol: str,
    interval: str,
    decision: dict,
    okx_data: dict | None,
    reference_for: str = "BINANCE",
    feed: str = "ohlcv",
) -> dict[str, Any]:
    return {
        "symbol": symbol.upper().strip(),
        "feed": feed,
        "provider": "okx",
        "venue": "OKX",
        "reference_for": reference_for,
        "authoritative": False,
        "fallback_reason": decision["reason"],
        "fallback_activated": decision["activated"],
        "priority_symbol": decision["is_priority"],
        "requested_interval": interval,
        "activation": decision,
        "data": okx_data,
    }


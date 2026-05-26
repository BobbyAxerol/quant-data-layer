from __future__ import annotations

import json
from pathlib import Path

import yaml

from app.config import BINANCE_SYMBOLS_FILE


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VN_SYMBOLS_FILE = PROJECT_ROOT / "symbols_vn.yaml"

DEFAULT_PRIORITY_CRYPTO = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT")
DEFAULT_PRIORITY_VN = ("VN30F1M", "FPT", "HPG", "VCB", "BID")


def _dedupe_upper(symbols: list[str]) -> list[str]:
    seen = set()
    result = []
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result


def load_binance_symbols(path: str | Path | None = None) -> list[str]:
    file_path = Path(path or BINANCE_SYMBOLS_FILE)
    if not file_path.exists():
        local_fallback = PROJECT_ROOT / "symbols.json"
        file_path = local_fallback if local_fallback.exists() else file_path
    if not file_path.exists():
        return []
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        payload = payload.get("symbols", [])
    if not isinstance(payload, list):
        return []
    return _dedupe_upper(payload)


def load_vn_symbols(path: str | Path | None = None) -> list[str]:
    file_path = Path(path or VN_SYMBOLS_FILE)
    if not file_path.exists():
        return []
    try:
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    symbols = payload.get("symbols", []) if isinstance(payload, dict) else payload
    if not isinstance(symbols, list):
        return []
    return _dedupe_upper(symbols)


def configured_universe() -> dict:
    binance = load_binance_symbols()
    vn = load_vn_symbols()
    return {
        "binance": {
            "provider": "binance",
            "market": "crypto",
            "symbols": binance,
            "count": len(binance),
        },
        "dnse": {
            "provider": "dnse",
            "market": "vn_stock",
            "symbols": vn,
            "count": len(vn),
        },
    }


def priority_universe() -> dict:
    configured = configured_universe()
    binance_symbols = set(configured["binance"]["symbols"])
    vn_symbols = set(configured["dnse"]["symbols"])
    return {
        "binance": [symbol for symbol in DEFAULT_PRIORITY_CRYPTO if symbol in binance_symbols],
        "dnse": [symbol for symbol in DEFAULT_PRIORITY_VN if symbol in vn_symbols],
    }


def active_universe() -> dict:
    # Phase 3 exposes the contract. Persistent active-universe registration is
    # intentionally left for the control-plane store in a later phase.
    return {
        "mode": "configured_equals_active",
        "providers": configured_universe(),
        "priority": priority_universe(),
    }


def provider_priority() -> dict:
    return {
        "crypto": {
            "trade": ["binance"],
            "kline": ["binance"],
            "history": ["binance", "okx"],
            "fallback_reference": ["okx"],
            "fallback_policy": "explicit_only",
            "authoritative_provider": "binance",
        },
        "vn_stock": {
            "quote": ["dnse", "vnstock"],
            "history": ["vnstock", "dnse_rest"],
            "fallback_policy": "source_must_be_explicit",
        },
    }

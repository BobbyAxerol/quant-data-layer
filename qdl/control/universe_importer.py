from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True, order=True)
class DiscoveryRequirement:
    provider: str
    venue: str
    market: str
    native_symbol: str
    requested_feeds: tuple[str, ...]
    resolution_state: str = "PENDING_AUTHORITATIVE_DISCOVERY"


def _dedupe_symbols(values: object) -> list[str]:
    if isinstance(values, dict):
        values = values.get("symbols", [])
    if not isinstance(values, list):
        raise ValueError("universe file must contain a symbol list")
    return sorted({str(value).strip().upper() for value in values if str(value).strip()})


def import_legacy_universe(
    *,
    binance_path: str | Path,
    vn_path: str | Path,
) -> list[DiscoveryRequirement]:
    """Import allowlists as discovery requirements, never fabricated instruments."""

    binance_payload = json.loads(Path(binance_path).read_text(encoding="utf-8"))
    vn_payload = yaml.safe_load(Path(vn_path).read_text(encoding="utf-8")) or {}
    requirements = [
        DiscoveryRequirement(
            provider="BINANCE_DIRECT",
            venue="BINANCE",
            market="USDM",
            native_symbol=symbol,
            requested_feeds=("TRADE", "BAR"),
        )
        for symbol in _dedupe_symbols(binance_payload)
    ]
    requirements.extend(
        DiscoveryRequirement(
            provider="DNSE",
            venue="VN_MARKETS",
            market="CONTROLLED_ALLOWLIST",
            native_symbol=symbol,
            requested_feeds=("BBO", "BAR"),
        )
        for symbol in _dedupe_symbols(vn_payload)
    )
    return sorted(requirements)


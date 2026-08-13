from __future__ import annotations

from dataclasses import dataclass
from os import environ
from typing import Mapping


SUPPORTED_BINANCE_SOURCES = frozenset(
    {
        "binance_spot_trade",
        "binance_futures_trade",
        "binance_spot_kline",
        "binance_futures_kline",
    }
)
DEFAULT_BINANCE_SOURCES = (
    "binance_futures_trade",
    "binance_futures_kline",
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _parse_bool(value: str | None, *, default: bool, name: str) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be one of true/false, 1/0, yes/no, on/off")


def _parse_sources(value: str | None) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_BINANCE_SOURCES
    sources = tuple(item.strip().lower() for item in value.split(",") if item.strip())
    if len(sources) != len(set(sources)):
        raise ValueError("DATA_LAYER_BINANCE_SOURCES contains duplicate sources")
    unsupported = sorted(set(sources) - SUPPORTED_BINANCE_SOURCES)
    if unsupported:
        raise ValueError(f"Unsupported DATA_LAYER_BINANCE_SOURCES: {','.join(unsupported)}")
    return sources


@dataclass(frozen=True)
class RuntimeSourceConfig:
    binance_sources: tuple[str, ...]
    dnse_stream_enabled: bool
    vnstock_poller_enabled: bool
    preload_watchdog_enabled: bool

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "RuntimeSourceConfig":
        env = environ if values is None else values
        return cls(
            binance_sources=_parse_sources(env.get("DATA_LAYER_BINANCE_SOURCES")),
            dnse_stream_enabled=_parse_bool(
                env.get("DATA_LAYER_DNSE_STREAM_ENABLED"),
                default=True,
                name="DATA_LAYER_DNSE_STREAM_ENABLED",
            ),
            vnstock_poller_enabled=_parse_bool(
                env.get("DATA_LAYER_VNSTOCK_POLLER_ENABLED"),
                default=True,
                name="DATA_LAYER_VNSTOCK_POLLER_ENABLED",
            ),
            preload_watchdog_enabled=_parse_bool(
                env.get("DATA_LAYER_PRELOAD_WATCHDOG_ENABLED"),
                default=True,
                name="DATA_LAYER_PRELOAD_WATCHDOG_ENABLED",
            ),
        )

    @property
    def spot_enabled(self) -> bool:
        return any(source.startswith("binance_spot") for source in self.binance_sources)

    @property
    def usdm_enabled(self) -> bool:
        return any(source.startswith("binance_futures") for source in self.binance_sources)

    def public_summary(self) -> dict[str, object]:
        return {
            "binance_sources": list(self.binance_sources),
            "spot_enabled": self.spot_enabled,
            "usdm_enabled": self.usdm_enabled,
            "dnse_stream_enabled": self.dnse_stream_enabled,
            "vnstock_poller_enabled": self.vnstock_poller_enabled,
            "preload_watchdog_enabled": self.preload_watchdog_enabled,
        }

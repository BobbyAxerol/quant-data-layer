from __future__ import annotations

from typing import Any, Protocol


class LegacyClient(Protocol):
    def latest_trade(self, provider: str, symbol: str, **kwargs) -> dict: ...
    def warmup_ohlcv(self, provider: str, symbol: str, **kwargs) -> dict: ...


class V1CompatibilityFacade:
    """Delegates frozen V1 methods without changing their default semantics."""

    def __init__(self, legacy_client: LegacyClient, telemetry: Any | None = None):
        self._legacy = legacy_client
        self._telemetry = telemetry

    def latest_trade(self, provider: str, symbol: str, **kwargs) -> dict:
        self._record("latest_trade")
        return self._legacy.latest_trade(provider, symbol, **kwargs)

    def warmup_ohlcv(self, provider: str, symbol: str, **kwargs) -> dict:
        self._record("warmup_ohlcv")
        return self._legacy.warmup_ohlcv(provider, symbol, **kwargs)

    def _record(self, contract: str) -> None:
        if self._telemetry is not None:
            self._telemetry.record(
                consumer_id="legacy-facade",
                sdk_major=1,
                contract=contract,
                cursor_offset=0,
            )

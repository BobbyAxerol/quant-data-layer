from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

import orjson
import redis
import requests


class DataLayerClientError(RuntimeError):
    pass


class DataLayerClient:
    """
    Official sync client for services inside bobby_network.

    REST is used for warmup/recovery/diagnostics. Redis Pub/Sub is used for
    live streaming. Services should use this client instead of opening direct
    Binance/DNSE/OKX/vnstock market-data connections.
    """

    def __init__(
        self,
        base_url: str = "http://data_layer:8100",
        redis_host: str = "redis_service",
        redis_port: int = 6379,
        redis_db: int = 2,
        timeout: float = 15.0,
        session: requests.Session | None = None,
        redis_client: redis.Redis | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.redis_client = redis_client or redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=False,
        )

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        try:
            response.raise_for_status()
        except Exception as exc:
            raise DataLayerClientError(f"GET {path} failed: {response.status_code} {response.text[:300]}") from exc
        payload = response.json()
        if not isinstance(payload, dict):
            raise DataLayerClientError(f"GET {path} returned non-object payload")
        return payload

    @staticmethod
    def _decode(raw: Any) -> dict | None:
        if not raw:
            return None
        if isinstance(raw, str):
            raw = raw.encode()
        return orjson.loads(raw)

    @staticmethod
    def _symbols(symbols: str | Iterable[str]) -> list[str]:
        if isinstance(symbols, str):
            return [symbols.upper().strip()]
        return [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()]

    def health(self) -> dict:
        return self._get("/v1/health")

    def stream_health(self) -> dict:
        return self._get("/v1/health/streams")

    def latest_trade(self, provider: str, symbol: str) -> dict:
        provider = provider.lower().strip()
        if provider != "binance":
            raise ValueError("latest_trade currently supports provider='binance'")
        return self._get(f"/v1/binance/price/{symbol.upper()}")

    def latest_kline(self, provider: str, symbol: str, interval: str = "1m") -> dict:
        provider = provider.lower().strip()
        if provider != "binance":
            raise ValueError("latest_kline currently supports provider='binance'")
        return self._get(f"/v1/binance/kline/{symbol.upper()}", params={"interval": interval})

    def latest_vn_quote(self, symbol: str, *, allow_last_snapshot: bool = True) -> dict:
        symbol = symbol.upper()
        try:
            return self._get(f"/v1/vn/quote/{symbol}")
        except DataLayerClientError:
            if not allow_last_snapshot:
                raise
            return self._get(f"/v1/vn/quote-last/{symbol}")

    def warmup_ohlcv(
        self,
        market: str,
        symbol: str,
        interval: str = "1m",
        limit: int = 1000,
        provider: str | None = None,
        **kwargs,
    ) -> dict:
        market = market.lower().strip()
        symbol = symbol.upper().strip()
        if market in {"vn", "vn_stock", "hose", "dnse"}:
            return self._get(
                f"/v1/preload/{symbol}",
                params={
                    "interval": interval,
                    "limit": limit,
                    "fresh": kwargs.pop("fresh", True),
                },
            )
        if market in {"crypto", "binance", "okx"}:
            resolved_provider = (provider or ("okx" if market == "okx" else "binance")).lower().strip()
            params = {"interval": interval, "limit": limit}
            params.update(kwargs)
            return self._get(f"/v1/crypto/ohlcv/{resolved_provider}/{symbol}", params=params)
        raise ValueError(f"Unsupported warmup market: {market}")

    def fallback_status(self, symbol: str, interval: str = "1m") -> dict:
        return self._get(f"/v1/fallback/crypto/status/{symbol.upper()}", params={"interval": interval})

    def fallback_reference(
        self,
        symbol: str,
        interval: str = "1m",
        *,
        feed: str = "kline",
        force: bool = False,
        include_data: bool = True,
        limit: int = 1,
    ) -> dict:
        return self._get(
            f"/v1/fallback/crypto/reference/{symbol.upper()}",
            params={
                "interval": interval,
                "feed": feed,
                "force": force,
                "include_data": include_data,
                "limit": limit,
            },
        )

    def control_contracts(self) -> dict:
        return {
            "redis_channels": {
                "trade": "stream:trade:{symbol}",
                "kline": "stream:kline:{interval}:{symbol}",
                "vn_quote": "stream:vn:{symbol}",
            },
            "rest_recovery": {
                "trade": "/v1/binance/price/{symbol}",
                "kline": "/v1/binance/kline/{symbol}?interval=1m",
                "vn_preload": "/v1/preload/{symbol}?interval=1m&limit=1000",
                "vn_last_quote": "/v1/vn/quote-last/{symbol}",
            },
            "provider_policy": {
                "binance": "authoritative for Binance crypto live trade/kline data",
                "dnse": "authoritative primary for VN live quote data when market is open",
                "vnstock": "VN fallback/preload provider; source must remain explicit",
                "okx": "crypto fallback reference only; authoritative=false",
            },
        }

    def redis_get(self, key: str) -> dict | None:
        return self._decode(self.redis_client.get(key))

    def subscribe(self, channels: str | Iterable[str]):
        pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(*([channels] if isinstance(channels, str) else list(channels)))
        return pubsub

    def stream_trades(self, symbols: str | Iterable[str]):
        channels = [f"stream:trade:{symbol}" for symbol in self._symbols(symbols)]
        return self.subscribe(channels)

    def stream_klines(self, symbols: str | Iterable[str], interval: str = "1m"):
        channels = [f"stream:kline:{interval}:{symbol}" for symbol in self._symbols(symbols)]
        return self.subscribe(channels)

    def stream_vn_quotes(self, symbols: str | Iterable[str]):
        channels = [f"stream:vn:{symbol}" for symbol in self._symbols(symbols)]
        return self.subscribe(channels)

    @staticmethod
    def validate_source(payload: dict, allowed_sources: set[str] | list[str] | tuple[str, ...]) -> bool:
        source = str(payload.get("source") or payload.get("provider") or payload.get("venue") or "").lower()
        return source in {str(item).lower() for item in allowed_sources}

    @staticmethod
    def validate_freshness(
        payload: dict,
        max_age_seconds: float,
        *,
        now: datetime | None = None,
    ) -> dict:
        now = now or datetime.now(timezone.utc)
        candidates = [
            payload.get("event_time"),
            payload.get("trade_time"),
            payload.get("timestamp"),
            payload.get("time"),
        ]
        raw = payload.get("raw")
        if isinstance(raw, dict):
            candidates.extend([raw.get("E"), raw.get("T"), raw.get("t")])

        event_seconds = None
        for value in candidates:
            if value is None:
                continue
            try:
                if isinstance(value, str):
                    try:
                        event_seconds = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        event_seconds = float(value)
                else:
                    event_seconds = float(value)
                if event_seconds > 10_000_000_000:
                    event_seconds /= 1000.0
                break
            except Exception:
                continue

        if event_seconds is None:
            return {"fresh": False, "age_seconds": None, "reason": "timestamp_missing"}

        age = max(0.0, now.timestamp() - event_seconds)
        return {
            "fresh": age <= max_age_seconds,
            "age_seconds": round(age, 3),
            "reason": "fresh" if age <= max_age_seconds else "stale",
        }

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Mapping

import requests

from app.openapi_sdk.python.dnse.common import build_signature


logger = logging.getLogger(__name__)

DEFAULT_DNSE_API_VERSION = "2026-07-23"
_ALLOWED_RESOLUTIONS = frozenset({"1", "3", "5", "15", "30", "1H", "1D", "1W"})
_DERIVATIVE_SYMBOLS = frozenset({"VN30F1M", "VN30F2M", "VN30F1Q", "VN30F2Q"})


class DnseHistoryError(RuntimeError):
    """A bounded, redacted DNSE history acquisition failure."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class DnseHistoryConfig:
    api_key: str
    api_secret: str
    base_url: str = "https://openapi.dnse.com.vn"
    api_version: str = DEFAULT_DNSE_API_VERSION
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0
    attempts: int = 4
    max_backoff_seconds: float = 30.0
    max_pages: int = 256
    max_rows: int = 100_000
    max_response_bytes: int = 8 * 1024 * 1024
    use_environment_proxy: bool = False

    def __post_init__(self) -> None:
        if not self.api_key or not self.api_secret:
            raise ValueError("DNSE history credentials are required")
        if not self.base_url.startswith("https://"):
            raise ValueError("DNSE history requires HTTPS")
        if not self.api_version.strip():
            raise ValueError("DNSE API version is required")
        if min(self.connect_timeout_seconds, self.read_timeout_seconds) <= 0:
            raise ValueError("DNSE history timeouts must be positive")
        if not 1 <= self.attempts <= 8:
            raise ValueError("DNSE history attempts must be between 1 and 8")
        if not 0.1 <= self.max_backoff_seconds <= 300:
            raise ValueError("DNSE history max backoff is invalid")
        if not 1 <= self.max_pages <= 1024 or not 1 <= self.max_rows <= 1_000_000:
            raise ValueError("DNSE history page/row bound is invalid")
        if not 1024 <= self.max_response_bytes <= 64 * 1024 * 1024:
            raise ValueError("DNSE history response bound is invalid")

    @classmethod
    def from_environment(cls) -> "DnseHistoryConfig":
        return cls(
            api_key=os.getenv("DNSE_API_KEY", ""),
            api_secret=os.getenv("DNSE_API_SECRET_KEY", ""),
            base_url=os.getenv("DNSE_REST_BASE", "https://openapi.dnse.com.vn"),
            api_version=os.getenv("DNSE_API_VERSION", DEFAULT_DNSE_API_VERSION),
            connect_timeout_seconds=float(
                os.getenv("DNSE_REST_CONNECT_TIMEOUT_SECONDS", "5")
            ),
            read_timeout_seconds=float(
                os.getenv("DNSE_REST_READ_TIMEOUT_SECONDS", "30")
            ),
            attempts=int(os.getenv("DNSE_REST_ATTEMPTS", "4")),
            max_backoff_seconds=float(
                os.getenv("DNSE_REST_MAX_BACKOFF_SECONDS", "30")
            ),
            use_environment_proxy=os.getenv(
                "DNSE_REST_USE_ENV_PROXY", "false"
            ).lower() in {"1", "true", "yes", "on"},
        )


class DnseQuotaLimiter:
    """Process-local thread-safe safety margin around documented DNSE quotas."""

    def __init__(
        self,
        *,
        hourly_limit: int = 900,
        daily_limit: int = 9000,
        min_interval_seconds: float = 4.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min(hourly_limit, daily_limit) <= 0 or min_interval_seconds < 0:
            raise ValueError("DNSE quota limiter bounds are invalid")
        self.hourly_limit = hourly_limit
        self.daily_limit = daily_limit
        self.min_interval_seconds = min_interval_seconds
        self.clock = clock
        self.sleep = sleep
        self._hourly: deque[float] = deque()
        self._daily: deque[float] = deque()
        self._last_request = float("-inf")
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self.clock()
                while self._hourly and self._hourly[0] <= now - 3600:
                    self._hourly.popleft()
                while self._daily and self._daily[0] <= now - 86400:
                    self._daily.popleft()
                delays = [max(0.0, self._last_request + self.min_interval_seconds - now)]
                if len(self._hourly) >= self.hourly_limit:
                    delays.append(max(0.0, self._hourly[0] + 3600 - now))
                if len(self._daily) >= self.daily_limit:
                    delays.append(max(0.0, self._daily[0] + 86400 - now))
                delay = max(delays)
                if delay <= 0:
                    self._hourly.append(now)
                    self._daily.append(now)
                    self._last_request = now
                    return
            self.sleep(max(delay, 0.001))


class DnseHistoryClient:
    """Strict DNSE `/price/ohlc` transport for bootstrap and bounded repair."""

    def __init__(
        self,
        config: DnseHistoryConfig,
        *,
        session: requests.Session | None = None,
        limiter: DnseQuotaLimiter | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.trust_env = config.use_environment_proxy
        self.limiter = limiter or DnseQuotaLimiter()
        self.sleep = sleep
        self.random_uniform = random_uniform

    @staticmethod
    def _bar_type(symbol: str) -> str:
        return "DERIVATIVE" if symbol in _DERIVATIVE_SYMBOLS else "STOCK"

    def _headers(self, path: str) -> dict[str, str]:
        date_value = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
        nonce = uuid.uuid4().hex
        signed_headers, signature = build_signature(
            self.config.api_secret,
            "GET",
            path,
            date_value,
            algorithm="hmac-sha256",
            nonce=nonce,
            header_name="X-Aux-Date",
        )
        return {
            "Accept": "application/json",
            "X-API-Key": self.config.api_key,
            "X-Aux-Date": date_value,
            "X-Signature": (
                f'Signature keyId="{self.config.api_key}",algorithm="hmac-sha256",'
                f'headers="{signed_headers}",signature="{signature}",nonce="{nonce}"'
            ),
            "version": self.config.api_version,
        }

    @staticmethod
    def _retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                current = now or datetime.now(timezone.utc)
                return max(0.0, (parsed - current).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        provider_delay = self._retry_after_seconds(retry_after)
        exponential = min(2 ** attempt, self.config.max_backoff_seconds)
        delay = provider_delay if provider_delay is not None else exponential
        return min(
            self.config.max_backoff_seconds,
            max(0.0, delay) + self.random_uniform(0.0, min(0.25, delay / 10)),
        )

    def _request_page(self, params: Mapping[str, str]) -> Mapping[str, Any]:
        path = "/price/ohlc"
        last_error: Exception | None = None
        for attempt in range(self.config.attempts):
            self.limiter.acquire()
            try:
                response = self.session.get(
                    f"{self.config.base_url.rstrip('/')}{path}",
                    params=dict(params),
                    headers=self._headers(path),
                    timeout=(
                        self.config.connect_timeout_seconds,
                        self.config.read_timeout_seconds,
                    ),
                    verify=True,
                )
            except requests.RequestException as error:
                last_error = error
                if attempt + 1 == self.config.attempts:
                    break
                self.sleep(self._backoff(attempt, None))
                continue

            if len(response.content) > self.config.max_response_bytes:
                raise DnseHistoryError("DNSE history response exceeds byte bound")
            if response.status_code == 200:
                try:
                    payload = response.json()
                except (ValueError, json.JSONDecodeError) as error:
                    raise DnseHistoryError("DNSE history response is not valid JSON") from error
                if not isinstance(payload, Mapping):
                    raise DnseHistoryError("DNSE history response root is invalid")
                return payload

            last_error = DnseHistoryError(
                f"DNSE history HTTP status={response.status_code}",
                status_code=response.status_code,
            )
            retryable = response.status_code in {408, 425, 429} or response.status_code >= 500
            if not retryable or attempt + 1 == self.config.attempts:
                raise last_error
            self.sleep(self._backoff(attempt, response.headers.get("Retry-After")))

        raise DnseHistoryError(
            f"DNSE history transport exhausted attempts={self.config.attempts}"
        ) from last_error

    @staticmethod
    def _decimal(value: Any, field: str, *, allow_zero: bool) -> Decimal:
        if value is None or isinstance(value, bool):
            raise DnseHistoryError(f"DNSE OHLC {field} is missing")
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError) as error:
            raise DnseHistoryError(f"DNSE OHLC {field} is invalid") from error
        if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
            raise DnseHistoryError(f"DNSE OHLC {field} is outside domain")
        return parsed

    def _rows_from_page(
        self,
        payload: Mapping[str, Any],
        *,
        from_ts: int,
        to_ts: int,
    ) -> tuple[list[dict[str, Any]], int]:
        fields = ("t", "o", "h", "l", "c", "v")
        arrays = {field: payload.get(field) for field in fields}
        if any(not isinstance(value, list) for value in arrays.values()):
            raise DnseHistoryError("DNSE OHLC parallel arrays are missing")
        lengths = {len(value) for value in arrays.values()}
        if len(lengths) != 1:
            raise DnseHistoryError("DNSE OHLC parallel array lengths differ")

        rows: list[dict[str, Any]] = []
        previous_time = -1
        for index in range(len(arrays["t"])):
            timestamp = arrays["t"][index]
            if isinstance(timestamp, bool):
                raise DnseHistoryError("DNSE OHLC timestamp is invalid")
            try:
                timestamp = int(timestamp)
            except (TypeError, ValueError) as error:
                raise DnseHistoryError("DNSE OHLC timestamp is invalid") from error
            if timestamp < from_ts or timestamp > to_ts or timestamp < previous_time:
                raise DnseHistoryError("DNSE OHLC timestamp ordering/bound is invalid")
            previous_time = timestamp
            row = {field: arrays[field][index] for field in fields}
            row["t"] = timestamp
            prices = {
                field: self._decimal(row[field], field, allow_zero=False)
                for field in ("o", "h", "l", "c")
            }
            self._decimal(row["v"], "v", allow_zero=True)
            if (
                prices["h"] < max(prices["o"], prices["c"], prices["l"])
                or prices["l"] > min(prices["o"], prices["c"], prices["h"])
            ):
                raise DnseHistoryError("DNSE OHLC price invariants failed")
            rows.append(row)

        next_time = payload.get("nextTime", 0)
        if next_time in (None, ""):
            next_time = 0
        if isinstance(next_time, bool):
            raise DnseHistoryError("DNSE OHLC nextTime is invalid")
        try:
            next_time = int(next_time)
        except (TypeError, ValueError) as error:
            raise DnseHistoryError("DNSE OHLC nextTime is invalid") from error
        return rows, next_time

    def fetch_ohlc(
        self,
        symbol: str,
        resolution: str,
        from_ts: int,
        to_ts: int,
    ) -> list[dict[str, Any]]:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol or not normalized_symbol.replace("_", "").isalnum():
            raise ValueError("DNSE symbol is invalid")
        if resolution not in _ALLOWED_RESOLUTIONS:
            raise ValueError("DNSE resolution is unsupported")
        if isinstance(from_ts, bool) or isinstance(to_ts, bool) or from_ts <= 0 or to_ts <= from_ts:
            raise ValueError("DNSE history range is invalid")

        current_from = int(from_ts)
        by_time: dict[int, tuple[str, dict[str, Any]]] = {}
        for _page in range(self.config.max_pages):
            payload = self._request_page({
                "symbol": normalized_symbol,
                "type": self._bar_type(normalized_symbol),
                "resolution": resolution,
                "from": str(current_from),
                "to": str(int(to_ts)),
            })
            rows, next_time = self._rows_from_page(
                payload, from_ts=current_from, to_ts=int(to_ts)
            )
            for row in rows:
                digest = json.dumps(
                    row, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                )
                previous = by_time.get(row["t"])
                if previous is not None and previous[0] != digest:
                    raise DnseHistoryError(
                        f"DNSE OHLC conflicting timestamp={row['t']}"
                    )
                by_time[row["t"]] = digest, row
                if len(by_time) > self.config.max_rows:
                    raise DnseHistoryError("DNSE history exceeds row bound")

            if next_time == 0 or next_time >= to_ts:
                return [by_time[key][1] for key in sorted(by_time)]
            if next_time <= current_from:
                raise DnseHistoryError("DNSE OHLC pagination did not advance")
            current_from = next_time

        raise DnseHistoryError("DNSE OHLC pagination exceeds page bound")


_default_client: DnseHistoryClient | None = None
_default_client_lock = threading.Lock()


def default_dnse_history_client() -> DnseHistoryClient:
    global _default_client
    with _default_client_lock:
        if _default_client is None:
            _default_client = DnseHistoryClient(DnseHistoryConfig.from_environment())
        return _default_client


def fetch_dnse_ohlc_raw(
    symbol: str, resolution: str, from_ts: int, to_ts: int
) -> list[dict[str, Any]]:
    return default_dnse_history_client().fetch_ohlc(
        symbol, resolution, from_ts, to_ts
    )

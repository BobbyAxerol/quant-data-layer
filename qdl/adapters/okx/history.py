from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from qdl.adapters.okx.client import OkxRestClient


class PaginationStalled(RuntimeError):
    """OKX returned a page that cannot advance the documented cursor."""


@dataclass(frozen=True)
class HistoryCoverage:
    requested_start_ms: int
    requested_end_ms: int
    observed_min_ts_ms: int | None
    observed_max_ts_ms: int | None
    complete_left: bool
    complete_right: bool
    truncated: bool
    terminal_reason: str
    provider_endpoint: str
    source_role: str = "PRIMARY"

    @property
    def status(self) -> str:
        if self.observed_min_ts_ms is None:
            return "EMPTY"
        return "FULL" if self.complete_left and self.complete_right and not self.truncated else "PARTIAL"


@dataclass(frozen=True)
class OkxCandle:
    inst_id: str
    bar: str
    price_type: str
    open_ts_ms: int
    open: str
    high: str
    low: str
    close: str
    volume_raw: str | None
    volume_ccy_raw: str | None
    volume_quote_raw: str | None
    confirmed: bool
    source_id: str = "OKX_DIRECT"

    def __post_init__(self) -> None:
        try:
            open_, high, low, close = map(
                Decimal, (self.open, self.high, self.low, self.close)
            )
        except InvalidOperation as error:
            raise ValueError("OKX candle contains invalid decimal text") from error
        if high < max(open_, close) or low > min(open_, close) or high < low:
            raise ValueError("OKX candle OHLC invariants failed")
        for volume in (self.volume_raw, self.volume_ccy_raw, self.volume_quote_raw):
            if volume is not None and Decimal(volume) < 0:
                raise ValueError("OKX candle volume cannot be negative")


@dataclass(frozen=True)
class OkxCandleHistory:
    records: tuple[OkxCandle, ...]
    coverage: HistoryCoverage


@dataclass(frozen=True)
class OkxFundingRate:
    inst_id: str
    funding_time_ms: int
    funding_rate: str
    realized_rate: str | None
    formula_type: str | None
    method: str | None
    raw_json: str
    source_id: str = "OKX_DIRECT"


@dataclass(frozen=True)
class OkxFundingHistory:
    records: tuple[OkxFundingRate, ...]
    coverage: HistoryCoverage


@dataclass(frozen=True)
class OkxOpenInterestSnapshot:
    inst_id: str
    inst_type: str
    open_interest_contracts: str
    open_interest_ccy: str | None
    open_interest_usd: str | None
    observed_ts_ms: int
    source_id: str = "OKX_DIRECT"
    coverage: str = "SNAPSHOT_ONLY"


class OkxHistoricalClient:
    """OKX V5 history/reference adapter; it does not project into V1 authority."""

    _CANDLE_ENDPOINTS = {
        "TRADE": ("/api/v5/market/history-candles", 300),
        "MARK": ("/api/v5/market/history-mark-price-candles", 100),
        "INDEX": ("/api/v5/market/history-index-candles", 100),
    }

    def __init__(self, client: OkxRestClient):
        self._client = client

    async def candles(
        self,
        *,
        inst_id: str,
        bar: str,
        start_ms: int,
        end_ms: int,
        price_type: str = "TRADE",
        max_records: int = 10_000,
        max_pages: int = 100,
    ) -> OkxCandleHistory:
        price_type = price_type.upper()
        if price_type not in self._CANDLE_ENDPOINTS:
            raise ValueError("price_type must be TRADE, MARK or INDEX")
        self._validate_window(inst_id, bar, start_ms, end_ms, max_records, max_pages)
        endpoint, page_limit = self._CANDLE_ENDPOINTS[price_type]

        def parse(row: list[Any]) -> OkxCandle:
            expected = 9 if price_type == "TRADE" else 6
            if not isinstance(row, list) or len(row) < expected:
                raise ValueError(f"OKX {price_type} candle row has invalid shape")
            if price_type == "TRADE":
                volumes = tuple(str(row[index]) for index in (5, 6, 7))
                confirm = str(row[8]) == "1"
            else:
                volumes = (None, None, None)
                confirm = str(row[5]) == "1"
            return OkxCandle(
                inst_id=inst_id, bar=bar, price_type=price_type,
                open_ts_ms=int(row[0]), open=str(row[1]), high=str(row[2]),
                low=str(row[3]), close=str(row[4]),
                volume_raw=volumes[0], volume_ccy_raw=volumes[1],
                volume_quote_raw=volumes[2], confirmed=confirm,
            )

        records, coverage = await self._paginate_time_window(
            endpoint=endpoint,
            base_params={"instId": inst_id, "bar": bar},
            bucket="market",
            start_ms=start_ms,
            end_ms=end_ms,
            page_limit=page_limit,
            max_records=max_records,
            max_pages=max_pages,
            parser=parse,
            timestamp=lambda item: item.open_ts_ms,
            merge=self._merge_candle_revision,
        )
        return OkxCandleHistory(tuple(records), coverage)

    async def funding_history(
        self,
        *,
        inst_id: str,
        start_ms: int,
        end_ms: int,
        max_records: int = 10_000,
        max_pages: int = 100,
    ) -> OkxFundingHistory:
        self._validate_window(inst_id, "funding", start_ms, end_ms, max_records, max_pages)

        def parse(row: dict[str, Any]) -> OkxFundingRate:
            if not isinstance(row, dict) or not row.get("fundingTime"):
                raise ValueError("OKX funding history row is malformed")
            rate = str(row.get("fundingRate") or "")
            Decimal(rate)
            realized = str(row.get("realizedRate") or "").strip() or None
            if realized is not None:
                Decimal(realized)
            return OkxFundingRate(
                inst_id=str(row.get("instId") or inst_id),
                funding_time_ms=int(row["fundingTime"]),
                funding_rate=rate,
                realized_rate=realized,
                formula_type=str(row.get("formulaType") or "").strip() or None,
                method=str(row.get("method") or "").strip() or None,
                raw_json=json.dumps(row, sort_keys=True, separators=(",", ":")),
            )

        records, coverage = await self._paginate_time_window(
            endpoint="/api/v5/public/funding-rate-history",
            base_params={"instId": inst_id},
            bucket="public",
            start_ms=start_ms,
            end_ms=end_ms,
            page_limit=400,
            max_records=max_records,
            max_pages=max_pages,
            parser=parse,
            timestamp=lambda item: item.funding_time_ms,
            merge=self._merge_identical,
        )
        return OkxFundingHistory(tuple(records), coverage)

    async def open_interest_snapshot(
        self, *, inst_type: str, inst_id: str | None = None
    ) -> tuple[OkxOpenInterestSnapshot, ...]:
        params = {"instType": inst_type.upper()}
        if inst_id:
            params["instId"] = inst_id
        rows = await self._client.get(
            "/api/v5/public/open-interest", params=params, bucket="public"
        )
        result = []
        for row in rows:
            if not row.get("instId") or not row.get("ts"):
                raise ValueError("OKX open-interest snapshot row is malformed")
            oi = str(row.get("oi") or "")
            Decimal(oi)
            oi_ccy = str(row.get("oiCcy") or "").strip() or None
            if oi_ccy is not None:
                Decimal(oi_ccy)
            oi_usd = str(row.get("oiUsd") or "").strip() or None
            if oi_usd is not None:
                Decimal(oi_usd)
            result.append(OkxOpenInterestSnapshot(
                inst_id=str(row["instId"]), inst_type=str(row.get("instType") or inst_type),
                open_interest_contracts=oi, open_interest_ccy=oi_ccy,
                open_interest_usd=oi_usd,
                observed_ts_ms=int(row["ts"]),
            ))
        return tuple(sorted(result, key=lambda item: item.inst_id))

    async def _paginate_time_window(
        self,
        *,
        endpoint: str,
        base_params: dict[str, str],
        bucket: str,
        start_ms: int,
        end_ms: int,
        page_limit: int,
        max_records: int,
        max_pages: int,
        parser: Callable[[Any], Any],
        timestamp: Callable[[Any], int],
        merge: Callable[[Any, Any], Any],
    ) -> tuple[list[Any], HistoryCoverage]:
        # OKX `after` means strictly older. end+1 preserves an inclusive end.
        cursor = end_ms + 1
        selected: dict[int, Any] = {}
        complete_left = False
        terminal_reason = "MAX_PAGES"
        truncated = False
        pages = 0
        while pages < max_pages:
            rows = await self._client.get(
                endpoint,
                params={**base_params, "after": str(cursor), "limit": str(page_limit)},
                bucket=bucket,
            )
            pages += 1
            if not rows:
                terminal_reason = "PROVIDER_EXHAUSTED"
                break
            parsed = [parser(row) for row in rows]
            oldest = min(timestamp(item) for item in parsed)
            if oldest >= cursor:
                raise PaginationStalled(
                    f"OKX pagination made no progress endpoint={endpoint} cursor={cursor} oldest={oldest}"
                )
            for item in parsed:
                ts = timestamp(item)
                if start_ms <= ts <= end_ms:
                    selected[ts] = merge(selected.get(ts), item)
            if oldest <= start_ms:
                complete_left = True
                terminal_reason = "REACHED_REQUEST_START"
                break
            if len(selected) >= max_records:
                truncated = True
                terminal_reason = "MAX_RECORDS"
                break
            cursor = oldest
        else:
            truncated = True

        ordered = [selected[key] for key in sorted(selected)]
        if len(ordered) > max_records:
            ordered = ordered[-max_records:]
            truncated = True
            terminal_reason = "MAX_RECORDS"
        observed = [timestamp(item) for item in ordered]
        coverage = HistoryCoverage(
            requested_start_ms=start_ms,
            requested_end_ms=end_ms,
            observed_min_ts_ms=min(observed) if observed else None,
            observed_max_ts_ms=max(observed) if observed else None,
            complete_left=complete_left,
            complete_right=True,
            truncated=truncated,
            terminal_reason=terminal_reason,
            provider_endpoint=endpoint,
        )
        return ordered, coverage

    @staticmethod
    def _merge_candle_revision(current: OkxCandle | None, candidate: OkxCandle) -> OkxCandle:
        if current is None or current == candidate:
            return candidate
        if candidate.confirmed and not current.confirmed:
            return candidate
        if current.confirmed and not candidate.confirmed:
            return current
        raise ValueError("OKX returned conflicting candle content at one confirmation state")

    @staticmethod
    def _merge_identical(current: Any | None, candidate: Any) -> Any:
        if current is None or current == candidate:
            return candidate
        raise ValueError("OKX returned conflicting immutable history rows")

    @staticmethod
    def _validate_window(
        inst_id: str, bar: str, start_ms: int, end_ms: int,
        max_records: int, max_pages: int,
    ) -> None:
        if not inst_id.strip() or not bar.strip():
            raise ValueError("inst_id and data interval/type are required")
        if start_ms < 0 or end_ms <= start_ms:
            raise ValueError("history window must satisfy 0 <= start_ms < end_ms")
        if max_records <= 0 or max_records > 100_000:
            raise ValueError("max_records must be between 1 and 100000")
        if max_pages <= 0 or max_pages > 1000:
            raise ValueError("max_pages must be between 1 and 1000")

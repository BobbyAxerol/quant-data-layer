from __future__ import annotations

import asyncio
import copy
import dataclasses
import json
import unittest
from pathlib import Path

from qdl.adapters.intervals import (
    canonical_interval_ms,
    okx_bar_size,
    okx_candle_channel,
    okx_interval_from_bar_size,
    okx_interval_from_channel,
)
from qdl.canonical.market import canonicalize_okx_bar
from qdl.canonical.trade import TradeContext
from qdl.adapters.okx.bar_edge import (
    OkxBarRawBinding,
    fetch_closed_bar_history_raw_envelopes as fetch_okx_history,
)
from qdl.adapters.okx.history import (
    HistoryCoverage,
    OkxCandle,
    OkxCandleHistory,
)


ROOT = Path(__file__).resolve().parents[1]


def _binding(interval: str) -> OkxBarRawBinding:
    return OkxBarRawBinding(
        market="SWAP",
        product_type="PERPETUAL",
        native_symbol="BTC-USDT-SWAP",
        interval=interval,
        subscription_id="okx-swap-bar",
        source_session_id="history-session",
        connection_generation=1,
        lease_epoch=1,
        authority_revision=1,
        partition_plan_epoch=1,
        adapter_version="okx-v5/2.0.0",
        config_revision=1,
        instrument_catalog_revision=1,
    )


class CanonicalIntervalTests(unittest.TestCase):
    def test_fixed_durations_are_exact(self):
        for interval, expected in (
            ("1m", 60_000),
            ("5m", 300_000),
            ("15m", 900_000),
            ("30m", 1_800_000),
            ("1h", 3_600_000),
            ("4h", 14_400_000),
            ("1d", 86_400_000),
            ("1w", 604_800_000),
        ):
            with self.subTest(interval=interval):
                self.assertEqual(canonical_interval_ms(interval), expected)

    def test_variable_length_and_malformed_intervals_fail_closed(self):
        for interval in ("1M", "", "  ", "0m", "-5m", "m", "5", "1y", "abc"):
            with self.subTest(interval=interval):
                with self.assertRaises(ValueError):
                    canonical_interval_ms(interval)


class OkxBarSizeTests(unittest.TestCase):
    def test_intraday_bars_use_the_native_okx_spelling(self):
        for interval, expected in (
            ("1m", "1m"),
            ("3m", "3m"),
            ("5m", "5m"),
            ("15m", "15m"),
            ("30m", "30m"),
            ("1h", "1H"),
            ("2h", "2H"),
            ("4h", "4H"),
        ):
            with self.subTest(interval=interval):
                self.assertEqual(okx_bar_size(interval), expected)

    def test_calendar_bars_bind_to_the_utc_calendar(self):
        """OKX aligns 6H and larger to a UTC+8 day unless the utc variant is used.

        Canonical `1d` must mean the same UTC day on Binance and OKX, so the
        mapping must never resolve to the UTC+8 default.
        """
        for interval, expected in (
            ("6h", "6Hutc"),
            ("12h", "12Hutc"),
            ("1d", "1Dutc"),
            ("2d", "2Dutc"),
            ("3d", "3Dutc"),
            ("1w", "1Wutc"),
        ):
            with self.subTest(interval=interval):
                resolved = okx_bar_size(interval)
                self.assertEqual(resolved, expected)
                self.assertTrue(resolved.endswith("utc"))

    def test_unsupported_bars_fail_closed(self):
        for interval in ("2m", "45m", "8h", "5d", "1M", "1s"):
            with self.subTest(interval=interval):
                with self.assertRaises(ValueError):
                    okx_bar_size(interval)

    def test_channel_name_derives_from_the_bar_size(self):
        self.assertEqual(okx_candle_channel("1m"), "candle1m")
        self.assertEqual(okx_candle_channel("1h"), "candle1H")
        self.assertEqual(okx_candle_channel("1d"), "candle1Dutc")


class OkxIntervalGenericHistoryTests(unittest.TestCase):
    """The OKX history edge previously certified 1m only and hard-coded 60s."""

    def _run(self, interval: str, *, limit: int, now_ms: int):
        interval_ms = canonical_interval_ms(interval)
        boundary = now_ms // interval_ms * interval_ms
        start = boundary - limit * interval_ms
        records = tuple(
            OkxCandle(
                inst_id="BTC-USDT-SWAP",
                bar=okx_bar_size(interval),
                price_type="TRADE",
                open_ts_ms=start + index * interval_ms,
                open="100",
                high="101",
                low="99",
                close="100.5",
                volume_raw="10",
                volume_ccy_raw="0.1",
                volume_quote_raw="1005",
                confirmed=True,
            )
            for index in range(limit)
        )
        coverage = HistoryCoverage(
            requested_start_ms=start,
            requested_end_ms=boundary - 1,
            observed_min_ts_ms=start,
            observed_max_ts_ms=records[-1].open_ts_ms,
            complete_left=True,
            complete_right=True,
            truncated=False,
            terminal_reason="REACHED_REQUEST_START",
            provider_endpoint="/api/v5/market/history-candles",
        )
        seen: dict[str, object] = {}

        class Client:
            async def candles(self, **kwargs):
                seen.update(kwargs)
                return OkxCandleHistory(records, coverage)

        envelopes = asyncio.run(fetch_okx_history(
            _binding(interval), limit=limit, now_ms=now_ms, history_client=Client()
        ))
        return envelopes, seen, start, interval_ms

    def test_hourly_history_requests_the_native_bar_and_hour_window(self):
        envelopes, seen, start, interval_ms = self._run(
            "1h", limit=3, now_ms=4 * 3_600_000 + 137
        )
        self.assertEqual(seen["bar"], "1H")
        self.assertEqual(seen["start_ms"], start)
        self.assertEqual(seen["end_ms"], 4 * 3_600_000 - 1)
        payloads = [json.loads(item.raw_frame_bytes) for item in envelopes]
        self.assertEqual({item["arg"]["channel"] for item in payloads}, {"candle1H"})
        opens = [int(item["data"][0][0]) for item in payloads]
        self.assertEqual(
            opens, [start + index * interval_ms for index in range(3)]
        )

    def test_daily_history_uses_the_utc_calendar_bar(self):
        _, seen, start, _ = self._run("1d", limit=2, now_ms=5 * 86_400_000 + 999)
        self.assertEqual(seen["bar"], "1Dutc")
        self.assertEqual(start, 3 * 86_400_000)

    def test_minute_history_behaviour_is_unchanged(self):
        envelopes, seen, _, _ = self._run("1m", limit=3, now_ms=240_000)
        self.assertEqual(seen["bar"], "1m")
        payloads = [json.loads(item.raw_frame_bytes) for item in envelopes]
        self.assertEqual({item["arg"]["channel"] for item in payloads}, {"candle1m"})
        self.assertEqual(
            [int(item["data"][0][0]) for item in payloads],
            [60_000, 120_000, 180_000],
        )

    def test_gap_detection_uses_the_declared_interval(self):
        interval_ms = canonical_interval_ms("15m")
        boundary = 10 * interval_ms
        start = boundary - 3 * interval_ms
        opens = [start, start + interval_ms, start + 3 * interval_ms]
        records = tuple(
            OkxCandle(
                inst_id="BTC-USDT-SWAP", bar="15m", price_type="TRADE",
                open_ts_ms=value, open="1", high="1", low="1", close="1",
                volume_raw="1", volume_ccy_raw="1", volume_quote_raw="1",
                confirmed=True,
            )
            for value in opens
        )
        coverage = HistoryCoverage(
            requested_start_ms=start, requested_end_ms=boundary - 1,
            observed_min_ts_ms=start, observed_max_ts_ms=opens[-1],
            complete_left=True, complete_right=True, truncated=False,
            terminal_reason="REACHED_REQUEST_START",
            provider_endpoint="/api/v5/market/history-candles",
        )

        class Client:
            async def candles(self, **_kwargs):
                return OkxCandleHistory(records, coverage)

        with self.assertRaises(RuntimeError) as caught:
            asyncio.run(fetch_okx_history(
                _binding("15m"), limit=3, now_ms=boundary + 5,
                history_client=Client(),
            ))
        self.assertIn("time gap", str(caught.exception))

    def test_binding_rejects_an_interval_okx_does_not_expose(self):
        with self.assertRaises(ValueError):
            dataclasses.replace(_binding("1m"), interval="45m")


class OkxBarCanonicalisationTests(unittest.TestCase):
    """The canonicaliser previously pinned channel, interval and close time to 1m."""

    def setUp(self) -> None:
        self.fixture = json.loads(
            (ROOT / "tests/fixtures/phase2/okx_bar.json").read_text(encoding="utf-8")
        )
        self.context = TradeContext(**self.fixture["context"])

    def _canonicalise(self, channel: str):
        raw = copy.deepcopy(self.fixture["raw"])
        raw["arg"]["channel"] = channel
        return canonicalize_okx_bar(raw, self.context)

    def test_round_trip_between_canonical_and_native_bar_size(self):
        for interval in ("1m", "5m", "30m", "1h", "4h", "6h", "1d", "1w"):
            with self.subTest(interval=interval):
                native = okx_bar_size(interval)
                self.assertEqual(okx_interval_from_bar_size(native), interval)
                self.assertEqual(
                    okx_interval_from_channel(f"candle{native}"), interval
                )

    def test_minute_bar_behaviour_is_unchanged(self):
        envelope = self._canonicalise("candle1m")
        open_ns = envelope.bar.open_time_ns
        self.assertEqual(envelope.bar.interval, "1m")
        self.assertEqual(
            envelope.bar.close_time_ns, open_ns + 60_000 * 1_000_000 - 1_000_000
        )

    def test_interval_is_read_from_the_frame_not_assumed(self):
        envelope = self._canonicalise("candle1H")
        open_ns = envelope.bar.open_time_ns
        self.assertEqual(envelope.bar.interval, "1h")
        self.assertEqual(
            envelope.bar.close_time_ns,
            open_ns + 3_600_000 * 1_000_000 - 1_000_000,
        )

    def test_utc_daily_channel_maps_to_the_canonical_day(self):
        envelope = self._canonicalise("candle1Dutc")
        self.assertEqual(envelope.bar.interval, "1d")

    def test_unsupported_or_foreign_channels_fail_closed(self):
        for channel in ("candle45m", "candle1D", "trades", "candle", ""):
            with self.subTest(channel=channel):
                with self.assertRaises(ValueError):
                    self._canonicalise(channel)


if __name__ == "__main__":
    unittest.main()

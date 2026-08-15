from __future__ import annotations

import unittest

from qdl.adapters.okx import OkxHistoricalClient, PaginationStalled


def trade_candle(ts: int, close: str = "2", confirm: str = "1") -> list[str]:
    return [str(ts), "1", "3", "0.5", close, "10", "20", "20", confirm]


def reference_candle(ts: int, close: str = "2", confirm: str = "1") -> list[str]:
    return [str(ts), "1", "3", "0.5", close, confirm]


class FakeRest:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    async def get(self, path, *, params, bucket, attempts=3):
        self.calls.append((path, dict(params), bucket))
        return self.pages.get((path, params.get("after")), [])


class OkxCandlePaginationTests(unittest.IsolatedAsyncioTestCase):
    async def test_after_walk_exact_inclusive_window_dedup_and_confirmed_revision(self):
        endpoint = "/api/v5/market/history-candles"
        rest = FakeRest({
            (endpoint, "401"): [trade_candle(400), trade_candle(300), trade_candle(200, "2", "0")],
            (endpoint, "200"): [trade_candle(200, "2.5", "1"), trade_candle(100), trade_candle(99)],
        })
        result = await OkxHistoricalClient(rest).candles(
            inst_id="BTC-USDT-SWAP", bar="1m", start_ms=100, end_ms=400,
        )
        self.assertEqual([row.open_ts_ms for row in result.records], [100, 200, 300, 400])
        self.assertEqual(result.records[1].close, "2.5")
        self.assertTrue(result.records[1].confirmed)
        self.assertEqual([call[1]["after"] for call in rest.calls], ["401", "200"])
        self.assertEqual(result.coverage.status, "FULL")
        self.assertEqual(result.coverage.terminal_reason, "REACHED_REQUEST_START")

    async def test_mark_and_index_have_distinct_price_type_and_no_fake_volume(self):
        mark_path = "/api/v5/market/history-mark-price-candles"
        index_path = "/api/v5/market/history-index-candles"
        rest = FakeRest({
            (mark_path, "201"): [reference_candle(200), reference_candle(100)],
            (index_path, "201"): [reference_candle(200), reference_candle(100)],
        })
        client = OkxHistoricalClient(rest)
        mark = await client.candles(
            inst_id="BTC-USDT-SWAP", bar="1m", start_ms=100, end_ms=200,
            price_type="MARK",
        )
        index = await client.candles(
            inst_id="BTC-USDT", bar="1m", start_ms=100, end_ms=200,
            price_type="INDEX",
        )
        self.assertEqual(mark.records[0].price_type, "MARK")
        self.assertEqual(index.records[0].price_type, "INDEX")
        self.assertIsNone(mark.records[0].volume_raw)
        self.assertEqual(rest.calls[0][1]["limit"], "100")

    async def test_no_progress_and_same_confirmation_conflict_fail_closed(self):
        endpoint = "/api/v5/market/history-candles"
        stalled = FakeRest({(endpoint, "401"): [trade_candle(401)]})
        with self.assertRaises(PaginationStalled):
            await OkxHistoricalClient(stalled).candles(
                inst_id="BTC-USDT-SWAP", bar="1m", start_ms=100, end_ms=400,
            )
        conflict = FakeRest({
            (endpoint, "401"): [trade_candle(200, "2", "1")],
            (endpoint, "200"): [trade_candle(200, "2.5", "1"), trade_candle(100)],
        })
        with self.assertRaisesRegex(ValueError, "conflicting candle"):
            await OkxHistoricalClient(conflict).candles(
                inst_id="BTC-USDT-SWAP", bar="1m", start_ms=100, end_ms=400,
            )

    async def test_page_budget_is_explicit_partial_coverage(self):
        endpoint = "/api/v5/market/history-candles"
        rest = FakeRest({(endpoint, "401"): [trade_candle(400), trade_candle(300)]})
        result = await OkxHistoricalClient(rest).candles(
            inst_id="BTC-USDT-SWAP", bar="1m", start_ms=100, end_ms=400,
            max_pages=1,
        )
        self.assertEqual(result.coverage.status, "PARTIAL")
        self.assertTrue(result.coverage.truncated)
        self.assertEqual(result.coverage.terminal_reason, "MAX_PAGES")


class OkxReferenceCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_funding_history_paginates_by_funding_time_with_provenance(self):
        endpoint = "/api/v5/public/funding-rate-history"
        row = lambda ts: {
            "instId": "BTC-USDT-SWAP", "fundingTime": str(ts),
            "fundingRate": "0.0001", "realizedRate": "0.00009",
            "formulaType": "withRate", "method": "current_period",
        }
        rest = FakeRest({
            (endpoint, "401"): [row(400), row(300), row(200)],
            (endpoint, "200"): [row(100)],
        })
        result = await OkxHistoricalClient(rest).funding_history(
            inst_id="BTC-USDT-SWAP", start_ms=100, end_ms=400,
        )
        self.assertEqual([item.funding_time_ms for item in result.records], [100, 200, 300, 400])
        self.assertEqual(result.records[0].formula_type, "withRate")
        self.assertIn('"realizedRate":"0.00009"', result.records[0].raw_json)
        self.assertEqual(rest.calls[0][2], "public")

    async def test_open_interest_never_claims_historical_coverage(self):
        class OpenInterestRest(FakeRest):
            async def get(self, path, *, params, bucket, attempts=3):
                self.calls.append((path, dict(params), bucket))
                return [{
                    "instId": "BTC-USDT-SWAP", "instType": "SWAP",
                    "oi": "123.4", "oiCcy": "12.34", "ts": "400",
                }]

        rest = OpenInterestRest({})
        result = await OkxHistoricalClient(rest).open_interest_snapshot(
            inst_type="SWAP", inst_id="BTC-USDT-SWAP"
        )
        self.assertEqual(result[0].coverage, "SNAPSHOT_ONLY")
        self.assertEqual(result[0].open_interest_contracts, "123.4")
        self.assertEqual(rest.calls[0][0], "/api/v5/public/open-interest")


if __name__ == "__main__":
    unittest.main()

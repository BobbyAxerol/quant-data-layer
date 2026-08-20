from __future__ import annotations

import threading
import unittest
from unittest.mock import patch

from app.database import dnse_fallback
from app.openapi_sdk.python.websocket_marketdata.trading_websocket.client import TradingClient

import requests

from app.providers.dnse.history import (
    DnseHistoryClient,
    DnseHistoryConfig,
    DnseHistoryError,
    DnseQuotaLimiter,
)


class _Response:
    def __init__(self, status=200, payload=None, headers=None, content=None):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.content = content if content is not None else b"{}"

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Session:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self.trust_env = None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _Limiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1


def _page(times=(100, 160), *, next_time=0, close=None):
    close = close or ["100.5"] * len(times)
    return {
        "t": list(times),
        "o": ["100"] * len(times),
        "h": ["101"] * len(times),
        "l": ["99"] * len(times),
        "c": close,
        "v": ["0"] * len(times),
        "nextTime": next_time,
    }


class DnseHistoryClientTests(unittest.TestCase):
    def config(self, **overrides):
        values = {
            "api_key": "key-not-secret",
            "api_secret": "secret-not-logged",
            "attempts": 3,
            "max_backoff_seconds": 5,
        }
        values.update(overrides)
        return DnseHistoryConfig(**values)

    def client(self, outcomes, **config):
        session = _Session(outcomes)
        limiter = _Limiter()
        sleeps = []
        client = DnseHistoryClient(
            self.config(**config),
            session=session,
            limiter=limiter,
            sleep=sleeps.append,
            random_uniform=lambda _low, _high: 0,
        )
        return client, session, limiter, sleeps

    def test_version_signature_tls_and_explicit_proxy_policy(self):
        client, session, limiter, _sleeps = self.client([_Response(payload=_page())])
        rows = client.fetch_ohlc("vn30f1m", "1", 100, 220)
        self.assertEqual([row["t"] for row in rows], [100, 160])
        self.assertFalse(session.trust_env)
        self.assertEqual(limiter.calls, 1)
        url, request = session.calls[0]
        self.assertEqual(url, "https://openapi.dnse.com.vn/price/ohlc")
        self.assertTrue(request["verify"])
        self.assertEqual(request["headers"]["version"], "2026-07-23")
        self.assertEqual(request["headers"]["X-API-Key"], "key-not-secret")
        self.assertIn('nonce="', request["headers"]["X-Signature"])
        self.assertEqual(request["params"]["type"], "DERIVATIVE")
        self.assertEqual(request["timeout"], (5.0, 30.0))

    def test_timeout_and_rate_limit_retry_are_bounded(self):
        client, _session, limiter, sleeps = self.client([
            requests.Timeout("injected"),
            _Response(status=429, payload={}, headers={"Retry-After": "2"}),
            _Response(payload=_page()),
        ])
        self.assertEqual(len(client.fetch_ohlc("FPT", "1", 100, 220)), 2)
        self.assertEqual(limiter.calls, 3)
        self.assertEqual(sleeps, [1, 2.0])

    def test_non_retryable_status_is_redacted_and_fails_immediately(self):
        client, _session, limiter, sleeps = self.client([
            _Response(status=401, payload={}, content=b"secret-provider-body"),
        ])
        with self.assertRaisesRegex(DnseHistoryError, "status=401") as raised:
            client.fetch_ohlc("FPT", "1", 100, 220)
        self.assertNotIn("secret-provider-body", str(raised.exception))
        self.assertEqual(limiter.calls, 1)
        self.assertEqual(sleeps, [])

    def test_malformed_parallel_arrays_and_price_domain_fail_closed(self):
        malformed = _page()
        malformed["v"] = ["1"]
        client, *_ = self.client([_Response(payload=malformed)])
        with self.assertRaisesRegex(DnseHistoryError, "array lengths"):
            client.fetch_ohlc("FPT", "1", 100, 220)

        invalid = _page(close=["102", "100"])
        client, *_ = self.client([_Response(payload=invalid)])
        with self.assertRaisesRegex(DnseHistoryError, "price invariants"):
            client.fetch_ohlc("FPT", "1", 100, 220)

    def test_pagination_deduplicates_equal_overlap_and_rejects_conflict(self):
        first = _page((100, 160), next_time=160)
        equal = _page((160, 220), next_time=0)
        client, *_ = self.client([_Response(payload=first), _Response(payload=equal)])
        rows = client.fetch_ohlc("FPT", "1", 100, 280)
        self.assertEqual([row["t"] for row in rows], [100, 160, 220])

        conflict = _page((160, 220), next_time=0, close=["100.6", "100.5"])
        client, *_ = self.client([_Response(payload=first), _Response(payload=conflict)])
        with self.assertRaisesRegex(DnseHistoryError, "conflicting timestamp"):
            client.fetch_ohlc("FPT", "1", 100, 280)

    def test_non_advancing_pagination_and_response_bounds_fail_closed(self):
        client, *_ = self.client([_Response(payload=_page((100,), next_time=100))])
        with self.assertRaisesRegex(DnseHistoryError, "did not advance"):
            client.fetch_ohlc("FPT", "1", 100, 220)

        client, *_ = self.client(
            [_Response(payload=_page(), content=b"x" * 2048)],
            max_response_bytes=1024,
        )
        with self.assertRaisesRegex(DnseHistoryError, "byte bound"):
            client.fetch_ohlc("FPT", "1", 100, 220)

    def test_quota_limiter_serializes_concurrent_reservations(self):
        limiter = DnseQuotaLimiter(
            hourly_limit=100,
            daily_limit=100,
            min_interval_seconds=0,
        )
        workers = [threading.Thread(target=limiter.acquire) for _ in range(20)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=2)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(len(limiter._hourly), 20)
        self.assertEqual(len(limiter._daily), 20)

    def test_legacy_chunk_boundaries_are_contiguous_and_failure_is_not_partial(self):
        calls = []

        def fetcher(symbol, resolution, start, end):
            calls.append((symbol, resolution, start, end))
            return []

        with patch.object(dnse_fallback, "DNSE_API_KEY", "k"), patch.object(
            dnse_fallback, "DNSE_API_SECRET_KEY", "s"
        ), patch.object(dnse_fallback, "_fetch_ohlc_raw", side_effect=fetcher):
            frame = dnse_fallback.fetch_dnse_ohlcv_direct(
                "FPT", "2026-01-01", "2026-01-10", chunk_days=7, max_retries=1
            )
        self.assertTrue(frame.empty)
        self.assertEqual(calls[0][3], calls[1][2])

        with patch.object(dnse_fallback, "DNSE_API_KEY", "k"), patch.object(
            dnse_fallback, "DNSE_API_SECRET_KEY", "s"
        ), patch.object(
            dnse_fallback, "_fetch_ohlc_raw", side_effect=TimeoutError("injected")
        ):
            with self.assertRaises(TimeoutError):
                dnse_fallback.fetch_dnse_ohlcv_direct(
                    "FPT", "2026-01-01", "2026-01-01", max_retries=1
                )

    def test_vendor_dispatch_queue_is_bounded_only_when_opted_in(self):
        bounded = TradingClient("k", "s", dispatch_queue_capacity=77)
        legacy = TradingClient("k", "s")
        self.assertEqual(bounded.dispatch_queue_capacity, 77)
        self.assertEqual(legacy.dispatch_queue_capacity, 0)
        with self.assertRaisesRegex(ValueError, "dispatch_queue_capacity"):
            TradingClient("k", "s", dispatch_queue_capacity=-1)

    def test_environment_version_and_proxy_are_explicit(self):
        with patch.dict(
            "os.environ",
            {
                "DNSE_API_KEY": "k",
                "DNSE_API_SECRET_KEY": "s",
                "DNSE_API_VERSION": "2026-08-01",
                "DNSE_REST_USE_ENV_PROXY": "true",
            },
            clear=True,
        ):
            config = DnseHistoryConfig.from_environment()
        self.assertEqual(config.api_version, "2026-08-01")
        self.assertTrue(config.use_environment_proxy)


if __name__ == "__main__":
    unittest.main()

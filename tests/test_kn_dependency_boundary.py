"""KN packaging must not silently invent a VN provider or start a dead worker."""
import sys
import unittest
from unittest import mock

from app.database.preload import fetch_ohlcv_chunked
from app.stream.vnstock_poller import VnstockPoller


class KnDependencyBoundaryTests(unittest.TestCase):
    def test_missing_sdk_fails_before_thread_or_cache_mutation(self):
        cache = mock.Mock()
        poller = VnstockPoller(cache, ["FPT"])
        with mock.patch.dict(sys.modules, {"vnstock": None}), mock.patch(
            "app.stream.vnstock_poller.threading.Thread"
        ) as thread:
            with self.assertRaisesRegex(RuntimeError, "VNSTOCK_UNAVAILABLE"):
                poller.start()
        thread.assert_not_called()
        self.assertFalse(poller._running)
        self.assertIsNone(poller._thread)
        self.assertEqual(cache.mock_calls, [])

    def test_missing_history_sdk_is_not_an_empty_success(self):
        with mock.patch.dict(sys.modules, {"vnstock": None}):
            with self.assertRaisesRegex(RuntimeError, "VNSTOCK_UNAVAILABLE"):
                fetch_ohlcv_chunked("FPT", "2026-09-01", "2026-09-02")

    def test_missing_transitive_dependency_is_not_mislabeled(self):
        error = ModuleNotFoundError("missing dependency", name="not_vnstock")
        with mock.patch("builtins.__import__", side_effect=error):
            with self.assertRaises(ModuleNotFoundError) as caught:
                VnstockPoller(mock.Mock(), ["FPT"]).start()
        self.assertIs(caught.exception, error)

import unittest
from datetime import datetime, timedelta, timezone

from app.fallback import crypto


class TestPhase4CryptoFallback(unittest.TestCase):
    def test_missing_binance_payload_activates_okx_reference(self):
        decision = crypto.fallback_decision("BTCUSDT", None)

        self.assertTrue(decision["activated"])
        self.assertEqual(decision["reason"], "BINANCE_MISSING")

    def test_fresh_binance_payload_does_not_activate_fallback(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        payload = {"event_time": int((now - timedelta(seconds=10)).timestamp() * 1000)}

        decision = crypto.fallback_decision(
            "BTCUSDT",
            payload,
            now=now,
            stale_seconds=180,
        )

        self.assertFalse(decision["activated"])
        self.assertEqual(decision["reason"], "BINANCE_FRESH")

    def test_stale_binance_payload_activates_fallback(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        payload = {"event_time": int((now - timedelta(seconds=300)).timestamp() * 1000)}

        decision = crypto.fallback_decision(
            "BTCUSDT",
            payload,
            now=now,
            stale_seconds=180,
        )

        self.assertTrue(decision["activated"])
        self.assertEqual(decision["reason"], "BINANCE_STALE")

    def test_force_activates_even_when_binance_is_fresh(self):
        now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        payload = {"event_time": int(now.timestamp() * 1000)}

        decision = crypto.fallback_decision(
            "ETHUSDT",
            payload,
            force=True,
            now=now,
        )

        self.assertTrue(decision["activated"])
        self.assertEqual(decision["reason"], "OPERATOR_FORCED")

    def test_reference_payload_cannot_be_mistaken_for_authoritative_binance(self):
        decision = crypto.fallback_decision("BTCUSDT", None)
        payload = crypto.okx_reference_payload(
            symbol="BTCUSDT",
            interval="1m",
            decision=decision,
            okx_data={"provider": "okx", "data": []},
        )

        self.assertEqual(payload["provider"], "okx")
        self.assertEqual(payload["venue"], "OKX")
        self.assertEqual(payload["reference_for"], "BINANCE")
        self.assertFalse(payload["authoritative"])
        self.assertTrue(payload["fallback_activated"])


if __name__ == "__main__":
    unittest.main()


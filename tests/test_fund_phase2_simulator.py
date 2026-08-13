from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

from qdl.canonical.trade import TradeContext, canonicalize_binance_usdm_trade, canonicalize_okx_trade
from qdl.simulator import OkxBookSimulator


ROOT = Path(__file__).resolve().parents[1]


class CrossLanguageGoldenTests(unittest.TestCase):
    def test_python_provider_canonicalizers_match_checked_in_golden(self):
        manifest = json.loads(
            (ROOT / "contracts/golden/phase2/manifest.json").read_text()
        )
        canonicalizers = {
            "binance_usdm_trade.json": canonicalize_binance_usdm_trade,
            "okx_trade.json": canonicalize_okx_trade,
        }
        for case in manifest["cases"]:
            fixture = json.loads(
                (ROOT / "tests/fixtures/phase2" / case["fixture"]).read_text()
            )
            envelope = canonicalizers[case["fixture"]](
                fixture["raw"], TradeContext(**fixture["context"])
            )
            payload = envelope.SerializeToString(deterministic=True)
            self.assertEqual(
                payload,
                (ROOT / "contracts/golden/phase2" / case["golden"]).read_bytes(),
            )
            self.assertEqual(hashlib.sha256(payload).hexdigest(), case["sha256"])


class OkxProtocolSimulatorTests(unittest.TestCase):
    def test_fixture_covers_recovery_gap_keepalive_and_connection_generation(self):
        frames = json.loads(
            (ROOT / "tests/fixtures/phase2/okx_protocol_frames.json").read_text()
        )
        simulator = OkxBookSimulator()
        observed = set()
        for frame in frames:
            result = simulator.apply(frame)
            self.assertEqual(result.state.value, frame["expected_state"], frame)
            self.assertEqual(result.accepted, frame["expected_accepted"], frame)
            observed.add(frame["kind"])
            if frame["kind"] == "keepalive_ping":
                self.assertEqual(result.response, "pong")
        self.assertTrue(
            {"rest_envelope", "connect", "subscribe_ack", "keepalive_ping", "book", "maintenance"}
            <= observed
        )


if __name__ == "__main__":
    unittest.main()

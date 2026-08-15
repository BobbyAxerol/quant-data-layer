from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from scripts.phase2_shadow_v1_smoke import run


class ShadowV1SmokeTests(unittest.TestCase):
    @patch("scripts.phase2_shadow_v1_smoke.requests.get")
    def test_reads_v1_but_writes_only_local_shadow_state(self, get: Mock):
        def response(url, **_kwargs):
            symbol = url.rsplit("/", 1)[-1]
            result = Mock()
            result.raise_for_status.return_value = None
            result.json.return_value = {
                "snapshot": {
                    "raw": {
                        "e": "trade",
                        "E": 1_786_610_747_772,
                        "T": 1_786_610_747_772,
                        "s": symbol,
                        "t": 7_969_064_626,
                        "p": "63744.50" if symbol == "BTCUSDT" else "1842.10",
                        "q": "0.055",
                        "X": "MARKET",
                        "m": False,
                        "st": 1,
                    }
                }
            }
            return result

        get.side_effect = response
        result = run("http://data-layer", ["BTCUSDT", "ETHUSDT"], 1.0)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["canonical_events"], 2)
        self.assertEqual(result["production_writes"], 0)
        self.assertEqual(get.call_count, 2)


if __name__ == "__main__":
    unittest.main()

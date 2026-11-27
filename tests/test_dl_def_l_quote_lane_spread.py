"""DL-DEF-2026-11-26-L lane spread: OKX execution QUOTE subscriptions spread over several sessions.

Fixture ``tests/fixtures/dl_def_l/okx-swap-lanes-live.json`` is the lane plan of the live ``ingestor_okx_swap``
config (sha ``1ca07ba6...``, read-only 2026-11-26): 465 bindings, all 97 OKX SWAP bbo-tbt QUOTE bindings pinned to
shard 3 (okx-public-003), public shards 1-5 in use, 100 subscriptions per connection.
"""

from __future__ import annotations

import json
import random
import unittest
from collections import Counter
from pathlib import Path

from qdl.runtime.ingestor_lanes import (
    binding_key,
    lane_plan_report,
    spread_lane_pins,
    sticky_lane_pins,
    validate_lane_plan,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads((ROOT / "tests/fixtures/dl_def_l/okx-swap-lanes-live.json").read_text(encoding="utf-8"))
TIER_A = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"]


def _live():
    rows = [dict(b) for b in FIXTURE["bindings"]]
    pins = {binding_key(b): int(b["lane_shard"]) for b in rows}
    return rows, pins, int(FIXTURE["max_subscriptions_per_connection"])


def _spread(rows, pins, cap, lanes=3, first=TIER_A):
    return spread_lane_pins("OKX", rows, pins, scope="public", feed="QUOTE", lanes=lanes,
                            max_subscriptions=cap, first=first)


class LiveTopologyTests(unittest.TestCase):
    def test_the_live_plan_has_every_execution_quote_on_one_session(self):
        rows, pins, cap = _live()
        report = lane_plan_report("OKX", rows, pins)
        quote_lanes = {name: len(v["members"]) for name, v in report.items() if v["feed"] == "QUOTE"}
        self.assertEqual(quote_lanes, {"okx-public-003": 97})
        self.assertEqual(cap, 100)


class SpreadTests(unittest.TestCase):
    def test_spread_is_balanced_under_the_limit_on_new_shards_only_for_quotes(self):
        rows, pins, cap = _live()
        spread, moves = _spread(rows, pins, cap)
        report = lane_plan_report("OKX", rows, spread)
        quote_lanes = {name: len(v["members"]) for name, v in report.items() if v["feed"] == "QUOTE"}
        self.assertEqual(sorted(quote_lanes), ["okx-public-003", "okx-public-006", "okx-public-007"])
        self.assertEqual(sorted(quote_lanes.values()), [32, 32, 33])
        self.assertTrue(all(n <= cap for n in quote_lanes.values()))
        validate_lane_plan("OKX", rows, spread, cap)
        # Only QUOTE bindings move; every other binding keeps its lane.
        quote_keys = {binding_key(b) for b in rows if b["feed"] == "QUOTE"}
        self.assertTrue({m["binding"] for m in moves} <= quote_keys)
        self.assertEqual({k: v for k, v in spread.items() if k not in quote_keys},
                         {k: v for k, v in pins.items() if k not in quote_keys})
        self.assertEqual(len(moves), 64, "about two thirds of the quotes leave shard 3")
        self.assertTrue(all(m["from"] == 3 and m["to"] in (6, 7) for m in moves))

    def test_tier_a_names_are_dealt_across_sessions(self):
        rows, pins, cap = _live()
        spread, _ = _spread(rows, pins, cap)
        lane_of = {b["native_symbol"]: spread[binding_key(b)] for b in rows if b["feed"] == "QUOTE"}
        self.assertEqual(len({lane_of[s] for s in TIER_A[:3]}), 3, "BTC, ETH and SOL never share a session")
        self.assertEqual([lane_of[s] for s in TIER_A], [3, 6, 7, 3, 6, 7])

    def test_spread_is_deterministic_whatever_the_input_order(self):
        rows, pins, cap = _live()
        expected, _ = _spread(rows, pins, cap)
        for seed in range(5):
            shuffled = list(rows)
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(_spread(shuffled, pins, cap)[0], expected)

    def test_spreading_an_already_spread_plan_moves_nothing(self):
        rows, pins, cap = _live()
        spread, _ = _spread(rows, pins, cap)
        again, moves = _spread(rows, spread, cap)
        self.assertEqual(moves, [])
        self.assertEqual(again, spread)

    def test_growth_after_the_spread_is_sticky_and_balanced(self):
        rows, pins, cap = _live()
        spread, _ = _spread(rows, pins, cap)
        previous = [dict(b, lane_shard=spread[binding_key(b)]) for b in rows]
        added = [{"feed": "QUOTE", "native_channel": "bbo-tbt", "native_symbol": f"NEW{i}-USDT-SWAP"} for i in range(7)]
        grown = sticky_lane_pins("OKX", previous, rows + added, cap)
        # No retained binding moves.
        self.assertEqual({k: grown[k] for k in spread}, spread)
        counts = Counter(grown[binding_key(b)] for b in rows + added if b["feed"] == "QUOTE")
        self.assertLessEqual(max(counts.values()) - min(counts.values()), 1, counts)

    def test_a_plan_over_the_subscription_limit_is_refused(self):
        rows = [{"feed": "QUOTE", "native_channel": "bbo-tbt", "native_symbol": f"S{i}-USDT-SWAP"} for i in range(250)]
        pins = {binding_key(b): 1 + i // 100 for i, b in enumerate(rows)}
        with self.assertRaises(ValueError):
            spread_lane_pins("OKX", rows, pins, scope="public", feed="QUOTE", lanes=2, max_subscriptions=100)
        spread, _ = spread_lane_pins("OKX", rows, pins, scope="public", feed="QUOTE", lanes=3, max_subscriptions=100)
        self.assertEqual(sorted(Counter(spread.values()).values()), [83, 83, 84])


if __name__ == "__main__":
    unittest.main()

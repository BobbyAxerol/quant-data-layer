"""DL v2.3.0 sticky ingestor lanes: the renderer side (qdl.runtime.ingestor_lanes).

The first pinned render must reproduce today's lanes exactly (names and
members), and growth must never move a retained binding or rename a lane.
The ingestor-side grouping is tested in qdl-native-raw-ingestor.rs.
"""

from __future__ import annotations

import time
import unittest
from pathlib import Path

from qdl.runtime.ingestor_lanes import (
    apply_lane_pins,
    binding_key,
    lane_plan_report,
    legacy_lane_assignment,
    sticky_lane_pins,
    validate_lane_plan,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan, stable_authority_record

ROOT = Path(__file__).resolve().parents[1]


def _okx(symbol: str, channel: str, feed: str) -> dict:
    return {"native_symbol": symbol, "native_channel": channel, "feed": feed}


class LegacyAssignmentTests(unittest.TestCase):
    def test_okx_numbers_lanes_across_feeds_inside_a_service(self):
        rows = [_okx(f"S{i}-USDT-SWAP", "books", "BOOK") for i in range(3)] + [
            _okx(f"S{i}-USDT-SWAP", "bbo-tbt", "QUOTE") for i in range(5)
        ] + [_okx("S0-USDT-SWAP", "candle1m", "BAR")]
        pins = legacy_lane_assignment("OKX", rows, 2)
        report = lane_plan_report("OKX", rows, pins)
        self.assertEqual({name: entry["feed"] for name, entry in report.items()}, {
            "okx-public-001": "BOOK", "okx-public-002": "BOOK",
            "okx-public-003": "QUOTE", "okx-public-004": "QUOTE", "okx-public-005": "QUOTE",
            "okx-business-001": "BAR",
        })

    def test_binance_numbers_lanes_inside_route_and_feed(self):
        rows = [
            {"native_symbol": f"S{i}USDT", "native_channel": f"s{i}usdt@bookTicker", "feed": "QUOTE"}
            for i in range(3)
        ] + [{"native_symbol": "S0USDT", "native_channel": "s0usdt@markPrice@1s", "feed": "MARK_INDEX"}]
        report = lane_plan_report("BINANCE", rows, legacy_lane_assignment("BINANCE", rows, 2))
        self.assertEqual(sorted(report), [
            "binance-market-markindex-001", "binance-public-quote-001", "binance-public-quote-002",
        ])


class StickyPinTests(unittest.TestCase):
    def test_growth_keeps_retained_bindings_and_names(self):
        before = [_okx(f"S{i:02d}-USDT-SWAP", "bbo-tbt", "QUOTE") for i in range(0, 10, 2)]
        before += [_okx(f"S{i:02d}-USDT-SWAP", "mark-price", "MARK_INDEX") for i in range(0, 10, 2)]
        old = legacy_lane_assignment("OKX", before, 3)
        old_report = lane_plan_report("OKX", before, old)
        # Interleaved symbols and a new BOOK feed: positional numbering would rename every lane.
        after = [_okx(f"S{i:02d}-USDT-SWAP", "bbo-tbt", "QUOTE") for i in range(10)]
        after += [_okx(f"S{i:02d}-USDT-SWAP", "mark-price", "MARK_INDEX") for i in range(0, 10, 2)]
        after += [_okx("S01-USDT-SWAP", "books", "BOOK")]
        after.sort(key=binding_key)
        pins = sticky_lane_pins("OKX", before, after, 3)
        for row in before:
            self.assertEqual(pins[binding_key(row)], old[binding_key(row)])
        new_report = lane_plan_report("OKX", after, pins)
        for name, entry in old_report.items():
            self.assertEqual(new_report[name]["feed"], entry["feed"], name)
        self.assertEqual(new_report["okx-public-007"]["feed"], "BOOK", "a new feed opens a new number")
        self.assertEqual(new_report["okx-public-005"]["feed"], "QUOTE", "growth fills, then opens, same-feed lanes")
        legacy_after = lane_plan_report("OKX", after, legacy_lane_assignment("OKX", after, 3))
        self.assertNotEqual(legacy_after["okx-public-001"]["feed"], old_report["okx-public-001"]["feed"],
                            "control: the positional rule would have renamed okx-public-001")

    def test_departures_free_capacity_for_the_same_feed_only(self):
        before = [_okx(f"S{i}-USDT-SWAP", "bbo-tbt", "QUOTE") for i in range(4)]
        after = before[1:] + [_okx("T-USDT-SWAP", "bbo-tbt", "QUOTE")]
        pins = sticky_lane_pins("OKX", before, after, 2)
        self.assertEqual(pins[binding_key(after[-1])], 1, "fills the lane S0 left")

    def test_invalid_plans_are_refused(self):
        rows = [_okx("A-USDT-SWAP", "bbo-tbt", "QUOTE"), _okx("A-USDT-SWAP", "mark-price", "MARK_INDEX")]
        with self.assertRaisesRegex(ValueError, "mixes feed classes"):
            validate_lane_plan("OKX", rows, {binding_key(row): 1 for row in rows}, 10)
        with self.assertRaisesRegex(ValueError, "subscriptions"):
            validate_lane_plan("OKX", rows[:1] * 3, {binding_key(rows[0]): 1}, 2)
        partial = [{**rows[0], "lane_shard": 1}, rows[1]]
        with self.assertRaisesRegex(ValueError, "but not all"):
            sticky_lane_pins("OKX", partial, rows, 10)


class RepositoryIngestorConfigTests(unittest.TestCase):
    def test_pinning_the_rendered_configs_reproduces_their_lanes(self):
        catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
        acquisition = StableAcquisitionPlan.load(
            ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog
        )
        authority = stable_authority_record(
            rust_image_digest="d" * 64,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=(ROOT / "config/v2/stable-acquisition-bindings.yaml").read_bytes(),
            effective_at_ns=time.time_ns(),
        )
        configs = acquisition.native_ingestor_configs(catalog=catalog, authority=authority)
        self.assertTrue(configs)
        for key, config in configs.items():
            runtime = config["runtime"]
            limit = config["max_subscriptions_per_connection"]
            with self.subTest(config=key):
                legacy = legacy_lane_assignment(runtime, config["bindings"], limit)
                pinned = apply_lane_pins(config, sticky_lane_pins(runtime, config["bindings"], config["bindings"], limit))
                self.assertEqual(
                    {binding_key(row): row["lane_shard"] for row in pinned["bindings"]}, legacy
                )


if __name__ == "__main__":
    unittest.main()

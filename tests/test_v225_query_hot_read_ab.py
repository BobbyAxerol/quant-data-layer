"""scripts/v225_query_hot_read_ab.py: the pure analysis rules (path-exact masking, percentiles, prereg shape).

The session itself needs Docker and production read access (run once, receipts in
upgrade/evidence/releases/v2.2.5/query-ab-20261020/); here the parts that decide the verdict are proven.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("v225_query_hot_read_ab", ROOT / "scripts/v225_query_hot_read_ab.py")
tool = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tool)


class RawMaskTest(unittest.TestCase):
    def test_only_the_named_paths_are_masked(self):
        a = b'{"request_id":"x1","data":[{"q":{"f":2240,"g":5},"c":"c1"},{"q":{"f":7,"g":5}}],"n":2240}'
        b = b'{"request_id":"y22","data":[{"q":{"f":2242,"g":5},"c":"c2"},{"q":{"f":8,"g":5}}],"n":2240}'
        paths = {("request_id",), ("data", 0, "q", "f"), ("data", 0, "c"), ("data", 1, "q", "f")}
        self.assertEqual(tool.raw_mask(a, paths),
                         b'{"request_id":"<V>","data":[{"q":{"f":"<V>","g":5},"c":"<V>"},{"q":{"f":"<V>","g":5}}],"n":2240}')
        self.assertEqual(tool.raw_mask(a, paths), tool.raw_mask(b, paths))

    def test_a_difference_outside_the_paths_stays_visible(self):
        a = b'{"request_id":"x1","data":[{"p":"1.0"}]}'
        b = b'{"request_id":"y2","data":[{"p":"1.1"}]}'
        self.assertNotEqual(tool.raw_mask(a, {("request_id",)}), tool.raw_mask(b, {("request_id",)}))

    def test_escaped_strings_and_whitespace(self):
        self.assertEqual(tool.raw_mask(b'{"a": "s\\"x", "b": [1, 2]}', {("b", 1)}), b'{"a": "s\\"x", "b": [1, "<V>"]}')


class RulesTest(unittest.TestCase):
    def test_nearest_rank_percentile(self):
        self.assertEqual(tool.pct(list(range(1, 101)), 0.99), 99)
        self.assertEqual(tool.pct(list(range(1, 11)), 0.95), 10)
        self.assertIsNone(tool.pct([], 0.5))

    def test_prereg_names_both_images_and_every_rule(self):
        self.assertEqual(set(tool.PREREG["images"].values()), {tool.OLD_IMAGE, tool.NEW_IMAGE})
        self.assertTrue({"HR1", "HR2", "HR3", "HR4", "HR5", "BC", "verdict"} <= set(tool.PREREG["rules"]))
        self.assertIn("INCONCLUSIVE", tool.PREREG["rules"]["verdict"])


if __name__ == "__main__":
    unittest.main()

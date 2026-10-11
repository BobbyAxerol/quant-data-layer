import unittest
from types import SimpleNamespace as NS
from unittest.mock import patch
from scripts.diagnose_kn_bar_window import diagnose


class BarWindowDiagnosticTests(unittest.TestCase):
    def run_case(self, spans, flags=(), topic="canonical"):
        reader = NS(environment="paper", bar_diagnostic_ranges=lambda *a, **k:
                    (NS(topic_id=topic, partition=0, offset=12), spans, flags))
        binding = NS(feed=NS(value="BAR"), interval="15m", binding_id="test")
        with patch("scripts.diagnose_kn_bar_window.binding_product_key", return_value="test"):
            return diagnose(reader, [binding], "canonical")

    def test_contiguous_retained_window(self):
        result = self.run_case([(0, 1800000)])
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["bindings"][0]["retained_rows"], 3)

    def test_gap_is_not_pass(self):
        result = self.run_case([(0, 0), (1800000, 1800000)])
        self.assertEqual(result["status"], "FINDINGS")
        self.assertEqual(result["bindings"][0]["missing_opens"], 1)

    def test_no_generation_and_wrong_topic_fail_closed(self):
        self.assertEqual(self.run_case([])["status"], "FINDINGS")
        self.assertEqual(self.run_case([(0, 0)], topic="old")["status"], "FINDINGS")

    def test_flags_and_empty_inventory_not_pass(self):
        self.assertEqual(self.run_case([(0, 0)], [(0, 1)])["status"], "FINDINGS")
        self.assertEqual(diagnose(None, [], "canonical")["status"], "FINDINGS")

"""KN-1 review F3: the native slice probe passes only on an explicit predicate.

``scripts/kn_native_slice_probe.py run`` used to exit 0 whatever it observed.
These tests pin ``slice_verdict`` (one failing condition at a time) and the
process exit code.
"""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_native_slice_probe", ROOT / "scripts/kn_native_slice_probe.py")
assert _SPEC is not None and _SPEC.loader is not None
PROBE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = PROBE
_SPEC.loader.exec_module(PROBE)


def _product(name: str) -> dict:
    return {"product": name, "records": 10, "decode_errors": 0, "token_errors": 0,
            "offsets_strictly_increasing": True, "resume_exactly_next_record": True,
            "digest_python_equals_proto_path": True, "controls": ["REPLAYING", "LIVE"]}


PRODUCTS = ["okx", "binance"]


def _passing() -> dict:
    negatives = [{"case": name, "expected": status, "observed": status, "pass": True}
                 for name, status in PROBE.NEGATIVE_CASES.items()]
    return {"products": [_product(name) for name in PRODUCTS], "negatives": negatives,
            "negatives_pass": len(negatives), "negatives_total": len(negatives)}


class SliceVerdictTests(unittest.TestCase):
    def test_a_complete_result_passes(self):
        self.assertEqual(PROBE.slice_verdict(_passing(), expected_products=PRODUCTS), [])

    def test_each_failing_condition_is_reported(self):
        product_mutations = {
            "no records": ("records", 0),
            "decode_errors": ("decode_errors", 1),
            "token_errors": ("token_errors", 2),
            "not strictly increasing": ("offsets_strictly_increasing", False),
            "exactly the next record": ("resume_exactly_next_record", False),
            "digest differs": ("digest_python_equals_proto_path", False),
            "REPLAYING then LIVE": ("controls", ["LIVE", "REPLAYING"]),
        }
        for expected, (field, value) in product_mutations.items():
            with self.subTest(field=field):
                result = _passing()
                result["products"][1][field] = value
                failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
                self.assertEqual(len(failures), 1, failures)
                self.assertIn(expected, failures[0])
                self.assertTrue(failures[0].startswith("binance:"))

    def test_missing_controls_or_fields_fail_closed(self):
        result = _passing()
        result["products"][0]["controls"] = ["REPLAYING"]
        del result["products"][1]["token_errors"]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertEqual(len(failures), 2, failures)

    def test_a_missing_product_fails(self):
        result = _passing()
        result["products"].pop()
        self.assertIn("products: missing ['binance']", PROBE.slice_verdict(result, expected_products=PRODUCTS))
        self.assertTrue(PROBE.slice_verdict({"products": [], "negatives": _passing()["negatives"]},
                                            expected_products=[]))

    def test_duplicate_or_unexpected_products_fail(self):
        # Astra R2 counterexample: two copies of one product for two expected.
        result = _passing()
        result["products"] = [_product("okx"), _product("okx")]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertIn("products: duplicated ['okx']", failures)
        self.assertIn("products: missing ['binance']", failures)
        result = _passing()
        result["products"].append(_product("other"))
        self.assertIn("products: unexpected ['other']", PROBE.slice_verdict(result, expected_products=PRODUCTS))

    def test_a_wrong_or_missing_negative_fails(self):
        result = _passing()
        result["negatives"][3]["observed"] = "OK"
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        name = result["negatives"][3]["case"]
        self.assertEqual(failures, [f"negative {name}: expected UNAUTHENTICATED, observed OK"])
        result = _passing()
        dropped = result["negatives"].pop()["case"]
        self.assertEqual(PROBE.slice_verdict(result, expected_products=PRODUCTS),
                         [f"negatives: missing ['{dropped}']"])

    def test_empty_negative_objects_fail(self):
        # Astra R2 counterexample: 24 empty objects compared None == None.
        result = _passing()
        result["negatives"] = [{} for _ in range(PROBE.EXPECTED_NEGATIVES)]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertTrue(any(f.startswith("negatives: missing") for f in failures), failures)
        # An entry without observed/expected fields fails too.
        result = _passing()
        del result["negatives"][0]["observed"]
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)

    def test_repeated_negative_case_fails(self):
        # Astra R2 counterexample: the same case 24 times.
        result = _passing()
        result["negatives"] = [dict(result["negatives"][0]) for _ in range(PROBE.EXPECTED_NEGATIVES)]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertIn(f"negatives: duplicated ['{result['negatives'][0]['case']}']", failures)
        self.assertTrue(any(f.startswith("negatives: missing") for f in failures))

    def test_an_expected_status_not_in_the_authoritative_table_fails(self):
        result = _passing()
        result["negatives"][0]["expected"] = result["negatives"][0]["observed"] = "OK"
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)

    def test_malformed_sections_fail_closed(self):
        self.assertTrue(PROBE.slice_verdict({}, expected_products=PRODUCTS))
        result = _passing()
        result["negatives"] = None
        self.assertIn("negatives: missing or not a list", PROBE.slice_verdict(result, expected_products=PRODUCTS))
        result = _passing()
        result["products"][0]["records"] = True
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)


class SliceExitCodeTests(unittest.TestCase):
    def _run(self, result: dict) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as directory:
            probes = Path(directory) / "probes.json"
            probes.write_text(json.dumps([{"physical_key": name} for name in PRODUCTS]), encoding="utf-8")
            out = Path(directory) / "result.json"

            async def fake_main_async(args):
                return copy.deepcopy(result)

            argv = ["run", "--target", "t", "--profile", "p", "--bundle", "b", "--cursor-keys", "k",
                    "--probes", str(probes), "--topic-id", "x", "--route-generation", "r",
                    "--quota-redis-url", "redis://unused", "--quota-prefix", "q", "--source-mode", "capture",
                    "--out", str(out)]
            with mock.patch.object(PROBE, "main_async", fake_main_async), \
                    contextlib.redirect_stderr(io.StringIO()):
                code = PROBE.main(argv)
            return code, json.loads(out.read_text(encoding="utf-8"))

    def test_pass_exits_zero_and_records_the_verdict(self):
        code, written = self._run(_passing())
        self.assertEqual(code, 0)
        self.assertEqual(written["verdict"], {"pass": True, "failures": []})

    def test_astra_counterexamples_exit_non_zero(self):
        empty = _passing()
        empty["negatives"] = [{} for _ in range(PROBE.EXPECTED_NEGATIVES)]
        repeated = _passing()
        repeated["negatives"] = [dict(repeated["negatives"][0]) for _ in range(PROBE.EXPECTED_NEGATIVES)]
        duplicate_product = _passing()
        duplicate_product["products"] = [_product("okx"), _product("okx")]
        for name, result in (("empty", empty), ("repeated", repeated), ("duplicate_product", duplicate_product)):
            with self.subTest(name):
                code, written = self._run(result)
                self.assertEqual(code, 1)
                self.assertFalse(written["verdict"]["pass"])

    def test_any_failure_exits_non_zero(self):
        result = _passing()
        result["products"][0]["token_errors"] = 1
        code, written = self._run(result)
        self.assertEqual(code, 1)
        self.assertFalse(written["verdict"]["pass"])
        self.assertEqual(len(written["verdict"]["failures"]), 1)


if __name__ == "__main__":
    unittest.main()

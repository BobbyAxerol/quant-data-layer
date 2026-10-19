"""Offline tests for scripts/ci_test_shard.py: partition properties, determinism, and that a module that fails to
import still fails its shard. Uses a temporary test tree; no Docker or network."""

import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import ci_test_shard as shard


class PartitionTests(unittest.TestCase):
    WEIGHTS = {f"test_m{i:02d}": (i * 7) % 11 + 1 for i in range(23)}

    def test_union_is_complete_disjoint_and_non_empty(self):
        for shards in (1, 2, 3, 4, 7):
            with self.subTest(shards=shards):
                buckets = shard.partition(self.WEIGHTS, shards)
                self.assertEqual(len(buckets), shards)
                self.assertEqual(shard.verify_partition(buckets, self.WEIGHTS), [])
                flat = [name for bucket in buckets for name in bucket]
                self.assertEqual(sorted(flat), sorted(self.WEIGHTS))

    def test_deterministic_and_order_independent(self):
        reordered = dict(reversed(list(self.WEIGHTS.items())))
        self.assertEqual(shard.partition(self.WEIGHTS, 4), shard.partition(reordered, 4))
        self.assertEqual(shard.partition(self.WEIGHTS, 4), shard.partition(dict(self.WEIGHTS), 4))

    def test_greedy_balance_bound(self):
        buckets = shard.partition(self.WEIGHTS, 4)
        loads = [sum(self.WEIGHTS[n] for n in b) for b in buckets]
        # Greedy longest-first: max load <= mean + largest single weight.
        self.assertLessEqual(max(loads), sum(loads) / 4 + max(self.WEIGHTS.values()))

    def test_verify_detects_missing_overlap_extra_and_empty(self):
        modules = {"a": 1, "b": 1, "c": 1}
        self.assertIn("modules in no shard: ['c']", shard.verify_partition([["a"], ["b"]], modules))
        self.assertTrue(any("in shards" in p for p in shard.verify_partition([["a", "b"], ["b", "c"]], modules)))
        self.assertTrue(any("not discovered" in p for p in shard.verify_partition([["a", "b"], ["c", "z"]], modules)))
        self.assertIn("shard 2 is empty", shard.verify_partition([["a", "b", "c"], []], modules))

    def test_more_shards_than_modules_is_reported(self):
        buckets = shard.partition({"a": 1}, 2)
        self.assertIn("shard 2 is empty", shard.verify_partition(buckets, {"a": 1}))

    def test_recorded_seconds_override_test_counts(self):
        groups = {"test_a": [unittest.TestSuite([unittest.FunctionTestCase(lambda: None)] * 3)],
                  "test_b": [unittest.TestSuite([unittest.FunctionTestCase(lambda: None)])]}
        self.assertEqual(shard.module_weights(groups), {"test_a": 3, "test_b": 1})
        self.assertEqual(shard.module_weights(groups, {"test_b": 90.0}), {"test_a": 3, "test_b": 90.0})


class DiscoveryShardTests(unittest.TestCase):
    """Builds a throw-away test tree and runs the real discover/partition/run path."""

    MODULES = {
        "test_alpha.py": "import unittest\nclass A(unittest.TestCase):\n"
                         "    def test_1(self): pass\n    def test_2(self): pass\n",
        "test_beta.py": "import unittest\nclass B(unittest.TestCase):\n    def test_1(self): pass\n",
        "test_gamma.py": "import unittest\nclass C(unittest.TestCase):\n"
                         "    @classmethod\n    def setUpClass(cls): cls.ready = True\n"
                         "    def test_1(self): self.assertTrue(self.ready)\n",
        "test_broken.py": "import unittest\nimport module_that_does_not_exist_qdl\n",
        "test_failing.py": "import unittest\nclass F(unittest.TestCase):\n"
                           "    def test_1(self): self.fail('expected')\n",
        "helper_support.py": "VALUE = 1\n",
    }

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        start = Path(self.tmp.name) / "tests_shard_fixture"
        start.mkdir()
        (start / "__init__.py").write_text("")
        for name, text in self.MODULES.items():
            (start / name).write_text(text)
        self.start = str(start)
        saved_path, saved_modules = list(sys.path), set(sys.modules)

        def restore():
            sys.path[:] = saved_path
            for name in set(sys.modules) - saved_modules:
                if name.startswith(("test_alpha", "test_beta", "test_gamma", "test_broken", "test_failing",
                                    "tests_shard_fixture")):
                    del sys.modules[name]
        self.addCleanup(restore)

    def discover(self):
        return shard.group_by_module(unittest.TestLoader().discover(self.start))

    def run_main(self, *args):
        err, out = io.StringIO(), io.StringIO()
        with patch("sys.stderr", err), patch("sys.stdout", out):
            code = shard.main(["--start-dir", self.start, *args])
        return code, out.getvalue(), err.getvalue()

    def test_modules_match_discover_including_import_failures(self):
        groups = self.discover()
        self.assertEqual(sorted(groups), ["test_alpha", "test_beta", "test_broken", "test_failing", "test_gamma"])
        total = unittest.TestLoader().discover(self.start).countTestCases()
        self.assertEqual(sum(i.countTestCases() for items in groups.values() for i in items), total)

    def test_shards_cover_the_suite_and_list_is_stable(self):
        listed = []
        for index in (1, 2, 3):
            code, out, _err = self.run_main("--shard", str(index), "--of", "3", "--list", "--verify-partition")
            self.assertEqual(code, 0)
            listed.append(out.split())
        self.assertEqual(sorted(sum(listed, [])), sorted(self.discover()))
        again = [self.run_main("--shard", str(i), "--of", "3", "--list")[1].split() for i in (1, 2, 3)]
        self.assertEqual(listed, again)

    def test_import_failure_and_test_failure_fail_their_shard(self):
        weights = shard.module_weights(self.discover())
        buckets = shard.partition(weights, 2)
        results = {}
        for index in (1, 2):
            code, _out, err = self.run_main("--shard", str(index), "--of", "2")
            results[index] = code
            self.assertIn("shard-timing", err)
        for index, bucket in enumerate(buckets, start=1):
            bad = {"test_broken", "test_failing"} & set(bucket)
            self.assertEqual(results[index], 1 if bad else 0, (bucket, results))
        self.assertIn(1, results.values())

    def test_shard_index_is_validated(self):
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            shard.main(["--start-dir", self.start, "--shard", "3", "--of", "2"])

    def test_verify_partition_fails_when_shards_outnumber_modules(self):
        code, _out, err = self.run_main("--shard", "1", "--of", "9", "--list")
        self.assertEqual(code, 2)
        self.assertIn("partition check FAILED", err)


class RepositorySuiteTests(unittest.TestCase):
    def test_checked_in_weights_file_is_optional_and_valid(self):
        recorded = shard.load_recorded()
        self.assertTrue(all(isinstance(v, float) and v >= 0 for v in recorded.values()))


if __name__ == "__main__":
    unittest.main()

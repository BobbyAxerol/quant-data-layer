"""Unit tests for scripts/build_qdl_release_images.py: arguments, lock/disk guards and post-check verdicts.

No docker call is made: only the pure helpers and git reads of this repository are exercised.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import build_qdl_release_images as b


REPO = Path(__file__).resolve().parents[1]
SHA = "a" * 64


def shas(value: str = SHA) -> dict[str, str]:
    return {name: value for name in b.RUST_BINARIES}


class ArgumentTests(unittest.TestCase):
    def test_default_is_dry_run_all_components(self):
        args = b.parse_args([])
        self.assertFalse(args.build)
        self.assertEqual(args.components, ["rust", "python"])

    def test_build_requires_lock_owner(self):
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            b.parse_args(["--build"])

    def test_component_selection_and_disk_floor(self):
        self.assertEqual(b.parse_args(["--component", "rust"]).components, ["rust"])
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            b.parse_args(["--min-free-gib", "0"])

    def test_identity_names_release_tags_and_receipt_dir(self):
        rev = "0123456789abcdef0123456789abcdef01234567"
        ident = b.identity(rev, Path("/state"))
        self.assertEqual(ident["rust_tag"], "qdl-v2-rust:2.2.4-0123456")
        self.assertEqual(ident["python_tag"], "qdl-v2-python:2.2.4-0123456")
        self.assertEqual(ident["build_dir"], "/state/build-v2.2.4-0123456")

    def test_resolve_rev_rejects_non_commit(self):
        with self.assertRaises(b.BuildError):
            b.resolve_rev(REPO, "no-such-ref-p184")


class GuardTests(unittest.TestCase):
    def test_lock_must_be_held_by_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "lock"
            with self.assertRaisesRegex(b.BuildError, "no host lock"):
                b.check_lock(lock, "me")
            lock.write_text(json.dumps({"owner": "other"}))
            with self.assertRaisesRegex(b.BuildError, "held by 'other'"):
                b.check_lock(lock, "me")
            lock.write_text(json.dumps({"owner": "me"}))
            self.assertEqual(b.check_lock(lock, "me")["owner"], "me")
            with self.assertRaises(b.BuildError):
                b.check_lock(lock, None)

    def test_disk_floor(self):
        usage = mock.Mock(free=10 * 2**30)
        with mock.patch.object(b.shutil, "disk_usage", return_value=usage):
            with self.assertRaises(b.BuildError):
                b.check_disk(Path("/"), 40)
            self.assertEqual(b.check_disk(Path("/"), 5), 10.0)


class CommandTests(unittest.TestCase):
    def test_rust_compile_is_offline_pinned_and_read_only_source(self):
        argv = b.rust_compile_argv(Path("/s/build-v2.2.4-abc1234"), Path("/w"))
        self.assertIn("--network", argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")
        self.assertIn("--pull=never", argv)
        self.assertIn(b.BUILDER_IMAGE, argv)
        self.assertIn("/s/build-v2.2.4-abc1234/src:/src:ro", argv)
        self.assertIn("CARGO_TARGET_DIR=/work/dl-target", argv)
        script = argv[-1]
        self.assertEqual(script.count("cargo build --offline --locked --release"), 4)
        self.assertNotIn("--features", script)

    def test_cargo_bins_match_binary_set(self):
        bins = sorted(word for cmd in b.CARGO_COMMANDS for prev, word in zip(cmd.split(), cmd.split()[1:])
                      if prev == "--bin")
        self.assertEqual(tuple(bins), tuple(sorted(b.RUST_BINARIES)))

    def test_rust_dockerfile_is_release_labelled_without_test_label(self):
        text = b.rust_dockerfile("f" * 40)
        self.assertTrue(text.startswith(f"FROM {b.RUST_BASE_TAG}\n"))
        self.assertIn('org.opencontainers.image.version="2.2.4"', text)
        self.assertIn('qdl.build.method="cargo-cache-551428d9+runtime-overlay-f41ecb66"', text)
        self.assertNotIn("qdl.test.only", text)
        self.assertIn("USER 10001:10001", text)

    def test_rust_image_build_has_no_network_and_no_pull(self):
        argv = b.rust_image_argv(Path("/s/d"), "t")
        self.assertIn("--pull=false", argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")

    def test_probes_are_isolated(self):
        argv = b.probe_argv("img", "true")
        for flag in ("--rm", "--read-only", "--pull=never"):
            self.assertIn(flag, argv)
        self.assertEqual(argv[argv.index("--network") + 1], "none")

    def test_python_build_carries_revision_and_release(self):
        argv = b.python_image_argv(Path("/s/d"), "e" * 40, "t")
        self.assertIn("QDL_GIT_SHA=" + "e" * 40, argv)
        self.assertIn("QDL_RELEASE=2.2.4", argv)
        self.assertIn("/s/d/src/Dockerfile", argv)


class ParseTests(unittest.TestCase):
    def test_parse_sha256sum(self):
        text = f"{SHA}  qdl-projector\n{'b' * 64}  ./qdl/x.py\n\n"
        self.assertEqual(b.parse_sha256sum(text), {"qdl-projector": SHA, "qdl/x.py": "b" * 64})
        with self.assertRaises(b.BuildError):
            b.parse_sha256sum("nothex  file\n")

    def test_runtime_probe(self):
        good = ("ldd_not_found qdl-realtime-core 0\nldd_not_found qdl-projector 0\n"
                "ldd_not_found qdl-stream-gateway 0\nldd_not_found qdl-native-raw-ingestor 0\n"
                "env_test_clock 0\nuid 10001\n")
        self.assertTrue(b.parse_runtime_probe(good)["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("uid 10001", "uid 0"))["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("env_test_clock 0", "env_test_clock 1"))["ok"])
        self.assertFalse(b.parse_runtime_probe(good.replace("qdl-projector 0", "qdl-projector 2"))["ok"])

    def test_feature_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            clean = Path(tmp) / "clean"
            clean.write_bytes(b"xx QDL_TEST_DATA_CLOCK_NAMESPACE TEST_DATA_CLOCK_INVALID xx")
            self.assertEqual(b.forbidden_markers(clean), [])
            dirty = Path(tmp) / "dirty"
            dirty.write_bytes(b"..p184.test-data-clock.v1../work/TEST_ONLY..")
            self.assertEqual(b.forbidden_markers(dirty), ["p184.test-data-clock.v1", "/work/TEST_ONLY"])


class VerdictTests(unittest.TestCase):
    def references(self, gateway_equal=True):
        rc22 = shas("1" * 64)
        rc22["qdl-stream-gateway"] = "2" * 64
        stream = {"qdl-stream-gateway": "3" * 64 if gateway_equal else "4" * 64}
        return {"rc22_core": rc22, "stream_baa72c96": stream,
                "rc18_ingestor": {"qdl-native-raw-ingestor": "5" * 64},
                "rc14_projector": {"qdl-projector": "6" * 64}}

    def release(self):
        rel = shas("1" * 64)
        rel["qdl-stream-gateway"] = "3" * 64
        return rel

    def test_expected_mix_passes(self):
        verdict = b.binary_verdicts(self.release(), self.references())
        self.assertTrue(verdict["ok"], verdict["unexpected"])
        self.assertTrue(verdict["core_equal_to_measured"])

    def test_core_difference_is_unexpected_and_flagged(self):
        rel = self.release()
        rel["qdl-realtime-core"] = "9" * 64
        verdict = b.binary_verdicts(rel, self.references())
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["core_equal_to_measured"])
        self.assertEqual({r["binary"] for r in verdict["unexpected"]}, {"qdl-realtime-core"})

    def test_gateway_must_equal_stream_overlay(self):
        verdict = b.binary_verdicts(self.release(), self.references(gateway_equal=False))
        self.assertEqual([(r["binary"], r["reference"]) for r in verdict["unexpected"]],
                         [("qdl-stream-gateway", "stream_baa72c96")])

    def test_missing_binary_or_unread_reference_fails(self):
        rel = self.release()
        del rel["qdl-kafka-smoke"]
        self.assertEqual(b.binary_verdicts(rel, self.references())["missing"], ["qdl-kafka-smoke"])
        refs = self.references()
        del refs["rc14_projector"]
        unexpected = b.binary_verdicts(self.release(), refs)["unexpected"]
        self.assertEqual([r["observed"] for r in unexpected], ["NOT_READ"])

    def test_every_binary_has_an_expectation_and_production_refs_are_declared(self):
        self.assertEqual(set(b.BINARY_EXPECTATIONS), set(b.RUST_BINARIES))
        for rows in b.BINARY_EXPECTATIONS.values():
            for ref, expected, _ in rows:
                self.assertIn(ref, b.REFERENCES)
                self.assertIn(expected, ("EQUAL", "DIFFERENT"))

    def test_python_file_verdict(self):
        ref = {"qdl/a.py": "1", "qdl/b.py": "2", "app/c.py": "3"}
        rel = {"qdl/a.py": "1", "qdl/b.py": "9", "app/c.py": "3", "qdl/new.py": "4"}
        ok = b.python_file_verdict(rel, ref, ["qdl/b.py", "qdl/new.py"])
        self.assertTrue(ok["ok"])
        bad = b.python_file_verdict(rel, ref, ["qdl/b.py"])
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["unexpected_different"], ["qdl/new.py"])
        missing = b.python_file_verdict(rel, ref, ["qdl/b.py", "qdl/new.py", "app/c.py"])
        self.assertEqual(missing["expected_but_equal"], ["app/c.py"])

    def test_freeze_verdict_lists_differences(self):
        verdict = b.freeze_verdict("a==1\nb==2\n", "a==1\nb==3\n")
        self.assertFalse(verdict["equal"])
        self.assertEqual(verdict["only_release"], ["b==2"])
        self.assertEqual(verdict["only_reference"], ["b==3"])


class SourceTests(unittest.TestCase):
    """git reads of this repository at HEAD (the release source carries the SDK bump)."""

    def head(self):
        return subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"]).decode().strip()

    def test_head_meets_source_preconditions(self):
        result = b.source_preconditions(REPO, self.head())
        self.assertTrue(result["ok"], result["problems"])

    def test_pre_bump_source_is_refused(self):
        result = b.source_preconditions(REPO, "87db2159f411e7c4d7d49033e7573b42a75fc91d")
        self.assertFalse(result["ok"])
        self.assertTrue(any("2.4.1" in p for p in result["problems"]))

    def test_expected_python_diff_uses_the_rc19_tree_model(self):
        diff = b.expected_python_diff(REPO, self.head())
        self.assertIn("qdl_sdk/__init__.py", diff)
        self.assertIn("generated/python/qdl/query/v2/query_pb2.py", diff)
        # rc19 carries qdl/ at 4d3476c, so the bcb0283..4d3476c Query files are not expected to differ.
        for name in subprocess.check_output(["git", "-C", str(REPO), "diff", "--name-only",
                                             b.PYTHON_REFERENCE_TREE[1][1], b.PYTHON_REFERENCE_TREE[0][1],
                                             "--", "qdl"]).decode().split():
            if name not in subprocess.check_output(["git", "-C", str(REPO), "diff", "--name-only",
                                                    b.PYTHON_REFERENCE_TREE[0][1], self.head(),
                                                    "--", "qdl"]).decode().split():
                self.assertNotIn(name, diff)

    def test_plan_is_dry(self):
        with mock.patch.object(b.subprocess, "run", wraps=subprocess.run) as run:
            result = b.plan(REPO, self.head(), ["rust", "python"], Path("/state"))
        self.assertFalse(any(call.args and call.args[0][:1] == ["docker"] for call in run.call_args_list))
        self.assertEqual(result["identity"]["build_dir"], f"/state/build-v2.2.4-{self.head()[:7]}")
        self.assertTrue(result["source_preconditions"]["ok"])


if __name__ == "__main__":
    unittest.main()

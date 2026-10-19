"""Offline recipe tests. No Docker or network calls."""

import hashlib
import io
import json
import tempfile
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from scripts import p184_consolidated_build as build

ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 40
READY = dict(ts_status="pass", ts_commit="b" * 40, ts_receipt="c" * 64,
             dl_ci_status="pass", dl_ci_receipt="d" * 64, build_approval="approved")
JOBS = ["consolidated-source-preflight", "unit-tests[shard=1]", "unit-tests[shard=2]", "release-gates"]
RECEIPT = {"schema": build.LOCAL_CI_RECEIPT_SCHEMA, "sha": SHA, "status": "PASS", "scope": "full",
           "workflow_jobs": JOBS, "jobs": {job: "PASS" for job in JOBS}}
_TMP = None


def write_receipt(receipt, name="summary.json"):
    path = Path(_TMP.name) / name
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def setUpModule():
    global _TMP
    _TMP = tempfile.TemporaryDirectory()
    path, digest = write_receipt(RECEIPT)
    READY.update(dl_ci_receipt=digest, dl_ci_receipt_file=path)


def tearDownModule():
    _TMP.cleanup()


class NoProcessTests(unittest.TestCase):
    def setUp(self):
        self.process_guard = patch.object(
            subprocess, "Popen", side_effect=AssertionError("unit tests must not launch processes")
        )
        self.process_guard.start()
        self.addCleanup(self.process_guard.stop)


class RecipeTests(NoProcessTests):
    def prepare(self, **kwargs):
        with patch.object(build, "source_contract", return_value={"Dockerfile": SHA}):
            return build.prepare(ROOT, SHA, "v2.2.3-rc.1", **kwargs)

    def test_default_hold_emits_no_build_script(self):
        recipe = self.prepare()
        self.assertEqual(recipe["status"], "HOLD")
        self.assertEqual(len(recipe["holds"]), 3)
        self.assertIsNone(recipe["build_script"])

    def test_each_gate_is_independently_required(self):
        for key in ("ts_status", "dl_ci_status", "build_approval"):
            with self.subTest(key=key):
                recipe = self.prepare(**{**READY, key: "pending"})
                self.assertEqual(recipe["status"], "HOLD")
                self.assertIsNone(recipe["build_script"])

    def test_pass_requires_full_evidence_digests_and_ts_commit(self):
        for key in ("ts_commit", "ts_receipt", "dl_ci_receipt"):
            for invalid in (None, "", "abc", "G" * 64):
                with self.subTest(key=key, invalid=invalid), self.assertRaises(ValueError):
                    self.prepare(**{**READY, key: invalid})

    def test_statuses_are_not_truthy_strings(self):
        for key in ("ts_status", "dl_ci_status", "build_approval"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.prepare(**{**READY, key: "true"})

    def test_release_rejects_shell_and_mutable_tags(self):
        for release in ("latest", "v2.2.3;true", "v2.2.3\n", "v2.2.3/overlay"):
            with self.subTest(release=release), self.assertRaises(ValueError):
                build.prepare(ROOT, SHA, release)

    def test_exactly_two_images_share_commit_and_archive(self):
        recipe = self.prepare(**READY)
        self.assertEqual(recipe["status"], "PREPARED_NOT_EXECUTED")
        self.assertEqual(len(recipe["images"]), 2)
        script = recipe["build_script"]
        self.assertEqual(script.count(" archive --format=tar "), 1)
        self.assertEqual(script.count("docker build "), 2)
        self.assertEqual(script.count(f"QDL_GIT_SHA={SHA}"), 2)
        self.assertEqual(script.count('"$build_dir/source"\n'), 4)
        self.assertEqual(script.count("org.opencontainers.image.revision"), 2)
        for image in recipe["images"]:
            self.assertEqual(image["source_commit"], SHA)
            self.assertTrue(image["tag"].endswith(SHA))
        for forbidden in ("docker push", "docker run", "docker compose", "--build-context", "git checkout"):
            self.assertNotIn(forbidden, script)

    def test_recipe_generation_executes_no_commands_with_mocked_source(self):
        with patch.object(build.subprocess, "check_output", side_effect=AssertionError("execution")):
            self.prepare(**READY)

    def test_dependency_image_must_be_pinned(self):
        for invalid in ("qdl:latest", "sha256:" + "a" * 64, "x;true@sha256:" + "a" * 64):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.prepare(**READY, dependency_image=invalid)

    def test_dependency_reuse_is_python_only_not_source_overlay(self):
        dependency = "registry.example/qdl-deps@sha256:" + "e" * 64
        script = self.prepare(**READY, dependency_image=dependency)["build_script"]
        self.assertEqual(script.count("QDL_DEPENDENCY_IMAGE=" + dependency), 1)
        self.assertNotIn("--build-context", script)
        self.assertEqual(script.count(" archive --format=tar "), 1)

    def test_prerequisite_roster(self):
        self.assertEqual(set(build.PREREQUISITES),
                         {"IDR040fix1", "bindingV3", "quota_log", "b6a0119", "IDR075", "DR05_book_fence", "DR08_terminal_status",
                          "IDR084_hot_mark"})


class IntegrationScopeTests(NoProcessTests):
    def prepare(self, **kwargs):
        with patch.object(build, "source_contract", return_value={"Dockerfile": SHA}):
            return build.prepare(ROOT, SHA, "v2.2.3-rc.1", **kwargs)

    def test_scope_is_market_data_not_paper_feature_completion(self):
        result = self.prepare(**READY)
        self.assertEqual(result["gates"]["ts_tests_scope"], "MARKET_DATA_INTEGRATION_ONLY")
        self.assertEqual(result["status"], "PREPARED_NOT_EXECUTED")

    def test_missing_new_recovery_prerequisite_refuses_old_candidate(self):
        for missing in ("DR05_book_fence", "DR08_terminal_status"):
            def fake(repo, *args):
                if args[0] == "rev-parse":
                    return SHA
                if args[0] == "merge-base" and args[2] == build.PREREQUISITES[missing]:
                    raise subprocess.CalledProcessError(1, "git")
                return ""
            with self.subTest(missing=missing), patch.object(build, "git", side_effect=fake):
                with self.assertRaisesRegex(ValueError, missing):
                    build.source_contract(ROOT, SHA)


class SourceTests(NoProcessTests):
    def test_reject_mutable_refs_and_abbreviations_before_git(self):
        for value in ("HEAD", "fix/p184-dl-closure", "b6a0119", "a" * 39, "A" * 40):
            with self.subTest(value=value), patch.object(build, "git") as git:
                with self.assertRaises(ValueError):
                    build.source_contract(ROOT, value)
                git.assert_not_called()

    def test_missing_ancestor_fails_closed(self):
        with patch.object(build, "git", side_effect=[SHA, subprocess.CalledProcessError(1, "git")]):
            with self.assertRaisesRegex(ValueError, "prerequisite ancestry"):
                build.source_contract(ROOT, SHA)

    def test_missing_marker_fails_closed(self):
        with patch.object(build, "git", side_effect=[SHA] + [""] * (len(build.PREREQUISITES) + 1)):
            with self.assertRaisesRegex(ValueError, "source contract changed"):
                build.source_contract(ROOT, SHA)

    def test_source_contract_uses_only_requested_commit(self):
        def fake_git(repo, *args):
            if args == ("rev-parse", SHA + "^{commit}"):
                return SHA
            if args[0] == "merge-base":
                self.assertEqual(args[1], "--is-ancestor")
                self.assertIn(args[2], build.PREREQUISITES.values())
                self.assertEqual(args[3], SHA)
                return ""
            self.assertTrue(args[1].startswith(SHA + ":"))
            path = args[1].split(":", 1)[1]
            self.assertIn(path, build.CONTRACTS)
            return "\n".join(build.CONTRACTS[path]) if args[0] == "show" else "f" * 40

        with patch.object(build, "git", side_effect=fake_git):
            blobs = build.source_contract(ROOT, SHA)
        self.assertEqual(blobs, dict.fromkeys(build.CONTRACTS, "f" * 40))

    def test_git_wrapper_is_mocked(self):
        with patch.object(subprocess, "check_output", return_value=(SHA + "\n").encode()) as call:
            self.assertEqual(build.git(ROOT, "rev-parse", "HEAD"), SHA)
        call.assert_called_once_with(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], stderr=subprocess.PIPE
        )

    def test_cli_hold_is_machine_readable_and_nonzero(self):
        argv = ["recipe", "--source-commit", SHA, "--release", "v2.2.3"]
        with patch.object(sys, "argv", argv), patch.object(
            build, "source_contract", return_value={"Dockerfile": "f" * 40}
        ), patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(build.main(), 2)
        recipe = json.loads(output.getvalue())
        self.assertEqual(recipe["status"], "HOLD")
        self.assertEqual(recipe["source_commit"], SHA)
        self.assertIsNone(recipe["build_script"])

    def test_cli_source_error_is_not_hold_json(self):
        argv = ["recipe", "--source-commit", SHA, "--release", "v2.2.3"]
        with patch.object(sys, "argv", argv), patch.object(
            build, "source_contract", side_effect=ValueError("missing prerequisite")
        ), patch("sys.stdout", new_callable=io.StringIO) as output, patch(
            "sys.stderr", new_callable=io.StringIO
        ) as error:
            with self.assertRaises(SystemExit) as raised:
                build.main()
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("missing prerequisite", error.getvalue())

    def test_prepare_checks_sha_even_when_source_checker_is_mocked(self):
        for invalid in ("HEAD", "a" * 39, "A" * 40, SHA + "\n"):
            with self.subTest(invalid=invalid), patch.object(build, "source_contract") as source:
                with self.assertRaisesRegex(ValueError, "source commit"):
                    build.prepare(ROOT, invalid, "v2.2.3", **READY)
                source.assert_not_called()


if __name__ == "__main__":
    unittest.main()


class DlCiReceiptScopeTests(NoProcessTests):
    def prepare(self, receipt, **kwargs):
        path, digest = write_receipt(receipt, "candidate.json")
        args = {**READY, "dl_ci_receipt": digest, "dl_ci_receipt_file": path, **kwargs}
        with patch.object(build, "source_contract", return_value={"Dockerfile": SHA}):
            return build.prepare(ROOT, SHA, "v2.2.3-rc.1", **args)

    def test_full_passing_receipt_for_the_commit_is_accepted(self):
        self.assertEqual(self.prepare(RECEIPT)["status"], "PREPARED_NOT_EXECUTED")

    def test_scoped_or_unscoped_receipts_are_refused(self):
        for scope in ("affected", None, "FULL"):
            receipt = {**RECEIPT, "scope": scope} if scope else {k: v for k, v in RECEIPT.items() if k != "scope"}
            with self.subTest(scope=scope), self.assertRaisesRegex(ValueError, "scope"):
                self.prepare(receipt)

    def test_status_commit_schema_and_job_coverage_are_checked(self):
        cases = {
            "status": {**RECEIPT, "status": "INCOMPLETE"},
            "different commit": {**RECEIPT, "sha": "f" * 40},
            "schema": {**RECEIPT, "schema": "other"},
            "every workflow job": {**RECEIPT, "jobs": {JOBS[0]: "PASS"}},
        }
        for message, receipt in cases.items():
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.prepare(receipt)

    def test_digest_only_or_mismatched_digest_is_refused(self):
        with self.assertRaisesRegex(ValueError, "receipt file"):
            self.prepare(RECEIPT, dl_ci_receipt_file=None)
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.prepare(RECEIPT, dl_ci_receipt="0" * 64)

    def test_pending_dl_ci_needs_no_receipt(self):
        recipe = self.prepare(RECEIPT, dl_ci_status="pending", dl_ci_receipt=None, dl_ci_receipt_file=None)
        self.assertEqual(recipe["status"], "HOLD")

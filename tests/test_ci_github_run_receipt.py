"""Offline tests for scripts/ci_github_run_receipt.py: changes-log parsing, GitHub job naming, receipt building and
the CLI with a fake API client. No network, no token."""

import json
from pathlib import Path
import tempfile
import unittest
import urllib.request
from unittest.mock import patch

try:
    import yaml
except ImportError:
    yaml = None

from scripts import ci_github_run_receipt as gh
from scripts import p184_consolidated_build as build

ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 40
LOG = "\n".join([
    "2026-10-19T06:47:31.7301473Z   \"base\": \"" + "b" * 40 + "\",",
    "2026-10-19T06:47:31.7301717Z   \"base_ref\": \"dev\",",
    "2026-10-19T06:47:31.7302518Z   \"event\": \"push\",",
    "2026-10-19T06:47:31.7303008Z   \"head\": \"" + SHA + "\",",
    "2026-10-19T06:47:31.7000000Z \x1b[36;1m  python3 -B scripts/ci_change_scope.py --github-output x\x1b[0m",
    "2026-10-19T06:47:31.7315298Z full=true",
    "2026-10-19T06:47:31.7315436Z python=true",
    "2026-10-19T06:47:31.7315577Z rust=true",
    "2026-10-19T06:47:31.7315716Z contracts=true",
    "2026-10-19T06:47:31.7316153Z reason=full: event push is not pull_request",
    "2026-10-19T06:47:32.0000000Z   \"event\": \"push\",",  # the step-summary copy repeats the same values
])
WORKFLOW = """
jobs:
  changes: {runs-on: x, steps: []}
  unit-tests:
    strategy: {matrix: {shard: [1, 2]}}
    steps: []
  named:
    name: Pretty name
    steps: []
  ci-result: {steps: []}
"""
RUN = {"id": 7, "run_attempt": 2, "html_url": "https://example/runs/7", "path": ".github/workflows/ci.yml",
       "event": "push", "head_branch": "dev", "head_sha": SHA, "status": "completed", "conclusion": "success"}


def jobs(names, conclusion="success"):
    return [{"id": i, "name": n, "conclusion": conclusion, "status": "completed", "started_at": "s",
             "completed_at": "c"} for i, n in enumerate(names)]


class LogParsingTests(unittest.TestCase):
    def test_outputs_and_fields(self):
        outputs, fields = gh.parse_changes_log(LOG)
        self.assertEqual(outputs, dict.fromkeys(gh.FLAGS, "true"))
        self.assertEqual(fields, {"base": "b" * 40, "base_ref": "dev", "event": "push", "head": SHA})

    def test_missing_or_conflicting_values_fail(self):
        with self.assertRaisesRegex(ValueError, "lacks"):
            gh.parse_changes_log(LOG.replace("rust=true", "rust is true"))
        with self.assertRaisesRegex(ValueError, "conflicting"):
            gh.parse_changes_log(LOG + "\n2026-10-19T06:47:33.0Z full=false")

    def test_echoed_command_text_is_not_an_output(self):
        outputs, _ = gh.parse_changes_log(LOG + "\n2026-10-19T06:47:33.0Z \x1b[36;1mfull=false\x1b[0m")
        self.assertEqual(outputs["full"], "true")


@unittest.skipIf(yaml is None, "PyYAML not installed")
class ReceiptTests(unittest.TestCase):
    NAMES = ["changes", "unit-tests (1)", "unit-tests (2)", "Pretty name", "ci-result"]

    def test_github_job_names_expand_matrix_and_use_name(self):
        self.assertEqual(gh.expected_job_names(WORKFLOW), self.NAMES)

    def test_real_workflow_names_match_github(self):
        names = gh.expected_job_names((ROOT / ".github/workflows/ci.yml").read_text())
        self.assertIn("unit-tests (4)", names)
        self.assertIn("ci-result", names)
        self.assertEqual(len(names), len(set(names)))

    def test_full_receipt_is_accepted_by_the_consolidated_build(self):
        receipt = gh.build_receipt(RUN, jobs(self.NAMES), WORKFLOW, LOG, "owner/repo")
        self.assertEqual((receipt["schema"], receipt["scope"], receipt["run_attempt"]),
                         (build.GITHUB_RUN_RECEIPT_SCHEMA, "full", 2))
        self.assertIs(build.verify_github_run_receipt(receipt, SHA, ROOT), receipt)

    def test_skipped_job_or_scoped_classifier_is_partial(self):
        skipped = jobs(self.NAMES)
        skipped[1]["conclusion"] = "skipped"
        self.assertEqual(gh.build_receipt(RUN, skipped, WORKFLOW, LOG, "o/r")["scope"], "partial")
        scoped = LOG.replace("full=true", "full=false").replace("rust=true", "rust=false")
        receipt = gh.build_receipt(RUN, jobs(self.NAMES), WORKFLOW, scoped, "o/r")
        self.assertEqual(receipt["scope"], "partial")
        with self.assertRaisesRegex(ValueError, "not full"):
            build.verify_github_run_receipt(receipt, SHA, ROOT)

    def test_cli_with_fake_client(self):
        class Fake:
            repo = "owner/repo"

            def run(self, run_id):
                return dict(RUN, id=run_id)

            def jobs(self, run_id):
                return jobs(ReceiptTests.NAMES)

            def workflow(self, sha):
                assert sha == SHA
                return WORKFLOW

            def job_log(self, job_id):
                assert job_id == 0  # the `changes` job
                return LOG

        with tempfile.TemporaryDirectory() as tmp, patch("sys.stdout"):
            out = Path(tmp) / "receipt.json"
            self.assertEqual(gh.main(["--run-id", "7", "--out", str(out)], client=Fake()), 0)
            receipt = json.loads(out.read_text())
        self.assertEqual((receipt["run_id"], receipt["head_sha"], receipt["scope"]), (7, SHA, "full"))

    def test_incomplete_run_is_refused(self):
        class Running:
            repo = "o/r"

            def run(self, run_id):
                return dict(RUN, status="in_progress")

        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(SystemExit):
            gh.main(["--run-id", "1", "--out", str(Path(tmp) / "r.json")], client=Running())


class RedirectTests(unittest.TestCase):
    def test_authorization_is_dropped_on_cross_host_redirect(self):
        handler = gh._DropAuthOnRedirect()
        req = urllib.request.Request("https://api.github.com/x", headers={"Authorization": "Bearer t"})
        new = handler.redirect_request(req, None, 302, "Found", {}, "https://blob.example/log?sig=1")
        self.assertFalse(new.has_header("Authorization"))
        same = handler.redirect_request(req, None, 302, "Found", {}, "https://api.github.com/y")
        self.assertEqual(same.get_header("Authorization"), "Bearer t")


if __name__ == "__main__":
    unittest.main()

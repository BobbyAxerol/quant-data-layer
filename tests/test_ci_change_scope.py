"""Offline tests for scripts/ci_change_scope.py and its wiring into .github/workflows/ci.yml.

Classification uses temporary git repositories (git only, no Docker or network); the workflow checks parse the
real ci.yml with yaml and assert that every `needs` / `needs.<job>.outputs.<name>` reference exists and that the
job-level `if:` scope of each job matches JOB_SCOPE, which `ci-result` enforces.

The CI unit image has no git and the bare runner python may have no yaml, so the git tests skip in the image and
the workflow tests skip without yaml; the `changes` job runs this module on the runner before classifying.
"""

import json
import os
import shutil
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

try:
    import yaml
except ImportError:  # bare runner python in the `changes` job
    yaml = None

from scripts import ci_change_scope as scope

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text()) if yaml else {"jobs": {}}
JOBS = WORKFLOW["jobs"]


# Built, not literal: a test file that names a journal path would make every journal commit run the suite.
PLAN = "DATA_LAYER_UNIFIED_" + "IMPLEMENTATION_PLAN.md"
JOURNALS = (PLAN, "CERTIFICATION_" + "LEDGER.md", "READ" + "ME.md")


def flags(paths, referenced=frozenset()):
    return scope.finalize(scope.classify_paths(paths, referenced)[0])


class PathRuleTests(unittest.TestCase):
    def test_docs_only_sets_no_suite(self):
        result = flags([PLAN, "docs/runbooks/x.md", "docs/img.png",
                        "upgrade/evidence/releases/v0.0.0/NOTES.md", "LICENSE", "rust/README.md"])
        self.assertEqual(result, dict.fromkeys(scope.FLAGS, False))

    def test_docs_read_by_tests_or_shipped_run_python(self):
        self.assertTrue(flags(["docs/runbooks/phase93-hold-close-expand.md"],
                              frozenset({"docs/runbooks/phase93-hold-close-expand.md"}))["python"])
        for path in ("qdl_sdk/README.md", "qdl_sdk/CHANGELOG.md", "tests/fixtures/notes.md"):
            with self.subTest(path=path):
                self.assertTrue(flags([path])["python"])

    def test_whole_path_matching(self):
        self.assertTrue(scope.names_path('ROOT / "docs/runbooks/a.md"', "docs/runbooks/a.md"))
        self.assertFalse(scope.names_path('"qdl_sdk/' + JOURNALS[2] + '"', JOURNALS[2]))
        self.assertFalse(scope.names_path('"docs/runbooks/a.md.bak"', "docs/runbooks/a.md"))
        self.assertTrue(scope.names_path(JOURNALS[2] + " 34a", JOURNALS[2]))

    def test_no_test_names_a_journal_file(self):
        """Journal commits must stay docs-only: no tests/ file may name the plan, the ledger or the root README."""
        for path in sorted((ROOT / "tests").rglob("*")):
            if path.is_file() and path.suffix in (".py", ".json", ".yaml", ".yml", ".txt"):
                text = path.read_text(errors="replace")
                for journal in JOURNALS:
                    with self.subTest(path=str(path.relative_to(ROOT)), journal=journal):
                        self.assertFalse(scope.names_path(text, journal))

    def test_ci_defining_paths_force_full(self):
        for path in (".github/workflows/ci.yml", ".github/CODEOWNERS", "Dockerfile", "Dockerfile.qdl-core",
                     "docker-compose.ci.yml", "docker-compose.v2-stable.yml", "pyproject.toml", "poetry.lock",
                     "requirements-dev.txt", ".dockerignore", ".trivyignore", "scripts/ci_change_scope.py",
                     "scripts/ci_test_shard.py", "scripts/prepare_nonroot_runtime.sh", ".github/README.md"):
            with self.subTest(path=path):
                self.assertEqual(flags([path]), dict.fromkeys(scope.FLAGS, True))

    def test_root_only_names_do_not_match_nested_files(self):
        self.assertEqual(flags(["deploy/Dockerfile.notes"]), {**dict.fromkeys(scope.FLAGS, False), "python": True})
        self.assertFalse(flags(["rust/qdl-core/Cargo.toml"])["full"])

    def test_rust_paths(self):
        for path in ("rust/qdl-core/src/lib.rs", "Cargo.toml", "Cargo.lock", "deny.toml", "rust-toolchain.toml",
                     ".cargo/config.toml"):
            with self.subTest(path=path):
                self.assertEqual(flags([path]), {"full": False, "python": False, "rust": True, "contracts": False})

    def test_every_cargo_workspace_member_is_a_rust_path(self):
        members = re.search(r"members\s*=\s*\[(.*?)\]", (ROOT / "Cargo.toml").read_text(), re.S).group(1)
        for member in re.findall(r'"([^"]+)"', members):
            with self.subTest(member=member):
                self.assertTrue(scope.matches(member + "/src/lib.rs", scope.RUST_PATTERNS))

    def test_contracts_generated_and_fixtures_fan_out(self):
        for path in ("contracts/proto/qdl/x.proto", "contracts/golden/kn_v220/state_codec.json",
                     "generated/rust/qdl.query.v2.tonic.rs", "generated/python/qdl/x_pb2.py"):
            with self.subTest(path=path):
                self.assertEqual(flags([path]), {"full": False, "python": True, "rust": True, "contracts": True})
        self.assertEqual(flags(["tests/fixtures/phase2/okx_bar.json"]),
                         {"full": False, "python": True, "rust": True, "contracts": False})

    def test_everything_else_and_unknown_paths_run_python(self):
        for path in ("qdl/runtime/x.py", "app/main.py", "scripts/p184_local_ci.py", "tests/test_x.py",
                     "config/v2/a.json", "consumers/stable/a.yaml", "migrations/001.sql", "Makefile",
                     "upgrade/evidence/x.json", "symbols.json", "brand-new-dir/file.bin"):
            with self.subTest(path=path):
                self.assertEqual(flags([path]), {"full": False, "python": True, "rust": False, "contracts": False})

    def test_rust_fixture_references_are_covered(self):
        """Rust tests read tests/fixtures, contracts/golden and generated/rust; each must set rust."""
        referenced = set()
        for path in (ROOT / "rust").rglob("*.rs"):
            for match in re.findall(r"\.\./\.\./(?:\.\./)?((?:tests|contracts|generated|config)/[\w./{}-]+)",
                                    path.read_text()):
                referenced.add(match)
        self.assertTrue(referenced)
        for path in sorted(referenced):
            with self.subTest(path=path):
                self.assertTrue(flags([path])["rust"], path)


@unittest.skipUnless(shutil.which("git"), "git binary required (CI images carry none)")
class GitDecisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name)
        self.git("init", "-q", "-b", "dev")
        (self.repo / "tests").mkdir()
        (self.repo / "tests/test_runbook.py").write_text('PATH = "docs/runbooks/read.md"\n')
        (self.repo / "NOTES.md").write_text("x\n")
        self.base = self.commit("base")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=t", "-c", "user.email=t@invalid",
                               *args], check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, message, files=None):
        for name, text in (files or {}).items():
            path = self.repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)
        return self.git("rev-parse", "HEAD")

    def decide(self, head, **kw):
        args = dict(repo=self.repo, base=self.base, head=head, event="pull_request", base_ref="dev", labels=set())
        args.update(kw)
        return scope.decide(**args)

    def test_docs_only_pull_request_into_dev(self):
        head = self.commit("docs", {"NOTES.md": "y\n", "docs/a.md": "a\n"})
        result = self.decide(head)
        self.assertEqual({f: result[f] for f in scope.FLAGS}, dict.fromkeys(scope.FLAGS, False))
        self.assertEqual(result["changed_files"], 2)
        self.assertIn("docs only", result["reason"])

    def test_doc_named_by_a_test_runs_python(self):
        head = self.commit("runbook", {"docs/runbooks/read.md": "r\n", "docs/runbooks/other.md": "o\n"})
        result = self.decide(head)
        self.assertTrue(result["python"])
        self.assertFalse(result["full"])
        self.assertIn("docs/runbooks/read.md", result["reason"])

    def test_forced_full_cases(self):
        head = self.commit("code", {"qdl/x.py": "x = 1\n"})
        cases = {"push": dict(event="push"), "dispatch": dict(event="workflow_dispatch"),
                 "main": dict(base_ref="main"), "label": dict(labels={"ci:full", "other"}),
                 "zero base": dict(base="0" * 40), "empty base": dict(base=""),
                 "unknown base": dict(base="1" * 40)}
        for name, kw in cases.items():
            with self.subTest(name=name):
                result = self.decide(head, **kw)
                self.assertEqual({f: result[f] for f in scope.FLAGS}, dict.fromkeys(scope.FLAGS, True))
                self.assertTrue(result["reason"].startswith("full: "))

    def test_merge_base_diff_ignores_later_base_commits(self):
        self.git("checkout", "-q", "-b", "feature")
        head = self.commit("feature", {"docs/f.md": "f\n"})
        self.git("checkout", "-q", "dev")
        moved_base = self.commit("dev moves", {"qdl/y.py": "y = 1\n"})
        result = self.decide(head, base=moved_base)
        self.assertFalse(any(result[f] for f in scope.FLAGS))

    def test_cli_writes_github_output_and_summary(self):
        head = self.commit("rust", {"rust/qdl-core/src/lib.rs": "\n"})
        out, summary = self.repo / "out.txt", self.repo / "summary.json"
        with patch("sys.stdout"), patch("sys.stderr"):
            code = scope.main(["--repo", str(self.repo), "--base", self.base, "--head", head, "--event",
                               "pull_request", "--base-ref", "dev", "--github-output", str(out),
                               "--summary-file", str(summary)])
        self.assertEqual(code, 0)
        lines = dict(line.split("=", 1) for line in out.read_text().splitlines())
        self.assertEqual({k: lines[k] for k in scope.FLAGS},
                         {"full": "false", "python": "false", "rust": "true", "contracts": "false"})
        self.assertEqual(lines["changed_files"], "1")
        self.assertTrue(json.loads(summary.read_text())["rust"])


def needs(outputs, **results):
    data = {"changes": {"result": "success", "outputs": outputs}}
    for job in (*scope.ALWAYS_JOBS, *scope.JOB_SCOPE):
        data.setdefault(job, {"result": "skipped", "outputs": {}})
    for job in scope.ALWAYS_JOBS:
        data[job]["result"] = "success"
    for job, result in results.items():
        data[job.replace("_", "-")] = {"result": result, "outputs": {}}
    return data


OUT = {"full": "false", "python": "true", "rust": "false", "contracts": "false", "reason": "scoped"}


class VerifyNeedsTests(unittest.TestCase):
    def test_scoped_success_with_skips_passes(self):
        self.assertEqual(scope.verify_needs(needs(OUT, sdk_python310="success", unit_tests="success",
                                                  release_gates="success")), [])

    def test_docs_only_passes_with_only_always_jobs(self):
        out = dict(OUT, python="false")
        self.assertEqual(scope.verify_needs(needs(out)), [])

    def test_failed_cancelled_or_wrongly_skipped_jobs_fail(self):
        base = dict(sdk_python310="success", unit_tests="success", release_gates="success")
        for job, result in (("unit_tests", "failure"), ("unit_tests", "cancelled"), ("release_gates", "skipped"),
                            ("repository_scan", "failure"), ("consolidated_source_preflight", "skipped"),
                            ("kn_native_integration", "failure")):
            with self.subTest(job=job, result=result):
                self.assertTrue(scope.verify_needs(needs(OUT, **{**base, job: result})))

    def test_changes_failure_or_missing_outputs_fail_closed(self):
        data = needs(OUT)
        data["changes"]["result"] = "failure"
        self.assertTrue(scope.verify_needs(data))
        data = needs({"reason": "x"})
        self.assertTrue(scope.verify_needs(data))
        data = needs(OUT, sdk_python310="success", unit_tests="success", release_gates="success")
        del data["release-gates"]
        self.assertTrue(scope.verify_needs(data))

    def test_cli_reads_needs_json(self):
        env = {"NEEDS_JSON": json.dumps(needs(dict(OUT, python="false")))}
        with patch.dict(os.environ, env), patch("sys.stdout"):
            self.assertEqual(scope.main(["--verify-needs"]), 0)
        env = {"NEEDS_JSON": json.dumps(needs(OUT))}
        with patch.dict(os.environ, env), patch("sys.stdout"):
            self.assertEqual(scope.main(["--verify-needs"]), 1)


@unittest.skipIf(yaml is None, "PyYAML not installed")
class WorkflowWiringTests(unittest.TestCase):
    def test_triggers_concurrency_and_dispatch(self):
        triggers = WORKFLOW.get("on", WORKFLOW.get(True))
        self.assertEqual(triggers["pull_request"]["branches"], ["dev", "main"])
        self.assertEqual(triggers["push"]["branches"], ["dev"])
        self.assertIn("workflow_dispatch", triggers)
        group = WORKFLOW["concurrency"]["group"]
        self.assertIn("format('pr-{0}', github.event.pull_request.number)", group)
        self.assertIn("format('run-{0}', github.run_id)", group)  # push/dispatch runs are never queued away
        self.assertEqual(WORKFLOW["concurrency"]["cancel-in-progress"], "${{ github.event_name == 'pull_request' }}")

    def test_every_needs_and_output_reference_exists(self):
        for name, job in JOBS.items():
            listed = job.get("needs") or []
            listed = [listed] if isinstance(listed, str) else listed
            for dep in listed:
                self.assertIn(dep, JOBS, f"{name} needs unknown job {dep}")
            text = yaml.safe_dump(job)
            for dep, output in re.findall(r"needs\.([\w-]+)\.outputs\.(\w+)", text):
                self.assertIn(dep, listed, f"{name} reads {dep} outputs without needing it")
                self.assertIn(output, JOBS[dep].get("outputs") or {}, f"{name}: {dep} has no output {output}")
            for step_ref in re.findall(r"steps\.([\w-]+)\.outputs", text):
                self.assertIn(step_ref, [s.get("id") for s in job["steps"]], f"{name}: unknown step id {step_ref}")

    def test_changes_outputs_are_the_classifier_flags(self):
        self.assertEqual(set(JOBS["changes"]["outputs"]), {*scope.FLAGS, "reason"})
        self.assertEqual(JOBS["changes"]["steps"][0]["with"]["fetch-depth"], 0)
        classify = next(s for s in JOBS["changes"]["steps"] if s.get("id") == "scope")
        self.assertEqual(classify["shell"], "bash")  # -eo pipefail: a classifier crash fails the job
        self.assertIn('--github-output "$GITHUB_OUTPUT"', classify["run"])
        self.assertIn("tests.test_ci_change_scope", JOBS["changes"]["steps"][1]["run"])

    def test_job_level_if_matches_job_scope(self):
        for name, job in JOBS.items():
            condition = job.get("if")
            if name in scope.JOB_SCOPE:
                terms = re.findall(r"needs\.changes\.outputs\.(\w+) == 'true'", condition or "")
                self.assertEqual(tuple(terms), scope.JOB_SCOPE[name], name)
                self.assertEqual(condition, " || ".join(f"needs.changes.outputs.{t} == 'true'" for t in terms))
                self.assertEqual(job.get("needs"), "changes", name)
            elif name == "ci-result":
                self.assertEqual(condition, "always()")
            else:
                self.assertIn(name, scope.ALWAYS_JOBS)
                self.assertIsNone(condition, name)

    def test_ci_result_needs_every_job(self):
        result = JOBS["ci-result"]
        self.assertEqual(set(result["needs"]), set(JOBS) - {"ci-result"})
        step = result["steps"][-1]
        self.assertEqual(step["env"]["NEEDS_JSON"], "${{ toJSON(needs) }}")
        self.assertIn("--verify-needs", step["run"])

    def test_unit_shards_match_the_shard_count(self):
        unit = JOBS["unit-tests"]
        self.assertFalse(unit["strategy"]["fail-fast"])
        shards = unit["strategy"]["matrix"]["shard"]
        self.assertEqual(shards, list(range(1, int(unit["env"]["QDL_TEST_SHARDS"]) + 1)))
        run = next(s["run"] for s in unit["steps"] if s.get("name") == "Run unit test shard")
        self.assertIn("scripts.ci_test_shard --shard ${{ matrix.shard }}", run)

    def test_release_gates_keep_every_former_unit_step(self):
        names = [s.get("name") for s in JOBS["release-gates"]["steps"]]
        for expected in ("Build data_layer image", "Audit final runtime dependencies",
                         "Run KN Query and BAR readback against isolated Redis",
                         "Run isolated Phase 7.1 beta topology and rollback gate", "Build immutable Rust replay tool",
                         "Scan final runtime image for critical and high vulnerabilities",
                         "Rehearse immutable SBOM and signed release manifest", "Clean CI Compose resources"):
            self.assertIn(expected, names)
        self.assertNotIn("Run unit tests", names)
        repo_scan = [s.get("name") for s in JOBS["repository-scan"]["steps"]]
        self.assertIn("Scan repository for leaked secrets and critical misconfiguration", repo_scan)

    def test_rust_cache_is_pinned_and_keyed_on_lockfile(self):
        cache = next(s for s in JOBS["rust-workspace"]["steps"] if str(s.get("uses", "")).startswith("actions/cache"))
        self.assertEqual(cache["uses"], "actions/cache@5a3ec84eff668545956fd18022155c47e93e2684")
        self.assertIn("hashFiles('Cargo.lock'", cache["with"]["key"])
        self.assertIn("rust-1.82-slim", cache["with"]["key"])
        self.assertNotIn("restore-keys", cache["with"])


if __name__ == "__main__":
    unittest.main()

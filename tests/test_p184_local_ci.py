"""Offline tests for scripts/p184_local_ci.py plan parsing and step classification. No Docker, git or network."""

import contextlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from scripts import p184_local_ci as ci

ROOT = Path(__file__).resolve().parents[1]
SHA = "e" * 40
WORKFLOW = (ROOT / ".github/workflows/ci.yml").read_text()


def fake_reader(files):
    return lambda path: files.get(path)


def decide(step, *, present=(), host_python="3.12.3", python_version=None, tools=None, files=None, **kw):
    return ci.classify(step, sha=SHA, read_file=fake_reader(files or {}), image_present=lambda ref: ref in present,
                       host_python=host_python, python_version=python_version, tools=tools or {}, **kw)


def run_step(script, **overrides):
    values = dict(job="j", index=1, name="n", uses=None, script=script, env={}, with_={},
                  working_directory=None, condition=None, timeout_s=60)
    values.update(overrides)
    return ci.Step(**values)


class NoProcessTests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(subprocess, "Popen", side_effect=AssertionError("unit tests must not launch processes"))
        guard.start()
        self.addCleanup(guard.stop)


class PlanParsingTests(NoProcessTests):
    def setUp(self):
        super().setUp()
        self.workflow = ci.load_workflow(WORKFLOW)
        self.steps = ci.plan_steps(self.workflow, SHA)

    def test_every_job_and_step_is_planned_in_order(self):
        raw = yaml.safe_load(WORKFLOW)["jobs"]
        expanded = ci.expand_jobs(self.workflow)
        self.assertEqual(list(dict.fromkeys(s.job for s in self.steps)), list(expanded))
        self.assertEqual({job_id for job_id, _j, _m in expanded.values()}, set(raw) - set(ci.ORCHESTRATION_JOBS))
        for job, (job_id, body, _matrix) in expanded.items():
            planned = [s for s in self.steps if s.job == job]
            self.assertEqual([s.index for s in planned], list(range(1, len(body["steps"]) + 1)))
            for step, raw_step in zip(planned, body["steps"]):
                self.assertEqual(step.uses, raw_step.get("uses"))
                if "run" in raw_step and "${{" not in raw_step["run"]:
                    self.assertEqual(step.script, raw_step["run"])  # same command text, byte for byte

    def test_expressions_are_substituted_with_the_exact_sha(self):
        unit = [s for s in self.steps if s.job == "release-gates"]
        build = next(s for s in unit if s.name == "Build immutable Rust replay tool")
        self.assertIn(f"qdl-core:{SHA}", build.script)
        self.assertNotIn("${{", build.script)
        env = ci.job_environment(self.workflow["jobs"]["release-gates"], SHA)
        self.assertEqual(env["QDL_CI_IMAGE"], f"data-layer-ci:{SHA}")
        scan = next(s for s in unit if s.name.startswith("Scan final runtime image"))
        self.assertEqual(scan.with_["image-ref"], f"data-layer-ci:{SHA}")

    def test_job_selection_and_step_timeouts(self):
        steps = ci.plan_steps(self.workflow, SHA, {"kn-native-integration"})
        self.assertEqual({s.job for s in steps}, {"kn-native-integration"})
        self.assertIn(30 * 60, [s.timeout_s for s in steps])
        self.assertTrue(all(s.timeout_s > 0 for s in steps))

    def test_conditions_follow_push_event_semantics(self):
        self.assertEqual(ci.evaluate_condition("github.event_name == 'pull_request'", False)[0], False)
        self.assertEqual(ci.evaluate_condition("always()", True), (True, ""))
        self.assertFalse(ci.evaluate_condition(None, True)[0])
        self.assertTrue(ci.evaluate_condition(None, False)[0])
        self.assertFalse(ci.evaluate_condition("contains(github.ref, 'x')", False)[0])

    def test_real_workflow_classification_offline(self):
        present = {"bufbuild/buf:1.50.0", "rust:1.82-slim", "redis:7.2-alpine", "alpine:3.20"}
        files = {"docker-compose.yml": "services:\n  data_layer:\n    build:\n      dockerfile: Dockerfile\n",
                 "Dockerfile": "FROM python:3.12-slim@sha256:" + "1" * 64 + " AS builder\nFROM builder\n",
                 "scripts/phase5_migration_smoke.sh": 'image="${QDL_PHASE5_POSTGRES_IMAGE:-postgres:16-alpine}"\n'}
        decisions, python_version, tools = {}, None, {"buf": False}
        for step in self.steps:
            decision = decide(step, present=present, python_version=python_version, tools=tools, files=files)
            if decision.action == "setup" and step.uses.startswith("actions/setup-python"):
                python_version = {"version": step.with_["python-version"], "host": decision.executor == "host"}
            if decision.action == "setup" and step.uses.startswith("bufbuild/buf-setup-action"):
                tools["buf"] = True
            decisions[(step.job, step.index)] = decision
        by_name = {(s.job, s.name): decisions[(s.job, s.index)] for s in self.steps}
        self.assertEqual(by_name[("consolidated-source-preflight",
                                  "Checkout exact CI revision with prerequisite history")].reason, "clone")
        self.assertEqual(by_name[("contract-tests", "Checkout")].reason, "archive")
        self.assertIn("requires_sudo", by_name[("release-gates", "Prepare non-root runtime bind mounts")].reason)
        build = by_name[("release-gates", "Build data_layer image")]
        self.assertEqual((build.action, build.blocking), ("skip", True))
        self.assertIn("python:3.12-slim@sha256:" + "1" * 64, build.reason)
        shard_build = by_name[("unit-tests[shard=1]", "Build data_layer image")]
        self.assertEqual((shard_build.action, shard_build.blocking), ("skip", True))
        self.assertIn("postgres:16-alpine",
                      by_name[("release-gates", "Run additive PostgreSQL migration gate")].reason)
        self.assertEqual(by_name[("contract-tests", "Test Python generated contracts")].executor, "container")
        self.assertIn("--allow-download",
                      by_name[("rust-workspace", "Check Rust dependency, license and advisory policy")].reason)
        self.assertEqual(by_name[("rust-workspace", "Cache Cargo registry and target for this lockfile and "
                                  "toolchain")].action, "noop")
        sdk = by_name[("sdk-python310", "Set up declared minimum Python")]
        self.assertEqual((sdk.action, sdk.blocking), ("skip", True))
        predicted = ci.predict(self.steps, decisions)
        gates = [p for s, p in zip(self.steps, predicted) if s.job == "release-gates"]
        self.assertEqual(gates[-1][0], "RUN")  # `if: always()` compose cleanup still runs after a blocking skip
        self.assertEqual(gates[6][0], "SKIPPED")


class ClassificationTests(NoProcessTests):
    def test_sudo_publication_and_unknown_actions_are_never_run(self):
        self.assertIn("requires_sudo", decide(run_step("sudo scripts/prepare_nonroot_runtime.sh")).reason)
        self.assertEqual(decide(run_step('gh release create "$TAG"')).action, "skip")
        self.assertEqual(decide(run_step(None, uses="someone/else@v1")).action, "skip")

    def test_buf_uses_local_image_or_skips_without_download(self):
        setup = run_step(None, uses="bufbuild/buf-setup-action@x", with_={"version": "1.50.0"})
        self.assertEqual(decide(setup).action, "skip")
        ok = decide(setup, present={"bufbuild/buf:1.50.0"})
        self.assertEqual((ok.action, ok.images), ("setup", ("bufbuild/buf:1.50.0",)))
        self.assertEqual(decide(run_step("buf lint\n")).action, "skip")
        self.assertEqual(decide(run_step("buf lint\n"), tools={"buf": True}).action, "run")

    def test_missing_images_skip_unless_pull_allowed(self):
        step = run_step("docker run --rm redis@sha256:" + "a" * 64 + " true\n")
        self.assertEqual(decide(step).action, "skip")
        self.assertEqual(decide(step, allow_pull=True).action, "run")
        self.assertEqual(decide(step, present={"redis@sha256:" + "a" * 64}).action, "run")

    def test_own_build_outputs_are_not_required_images(self):
        step = run_step(f"docker run --rm qdl-core:{SHA} x\nQDL_TEST_IMAGE=data-layer:v0.1.0 scripts/s.sh\n",
                        env={})
        self.assertEqual(ci.required_images(step, fake_reader({}), SHA), ())

    def test_skipped_docker_build_skips_its_dependents_only(self):
        unavailable = set()
        build = run_step(f"docker build -f Dockerfile.qdl-core\n-t qdl-core:{SHA} .\n")
        skipped = ci.propagate_unavailable(build, ci.Decision("skip", reason="missing base"), unavailable)
        self.assertEqual(skipped.action, "skip")
        self.assertEqual(unavailable, {f"qdl-core:{SHA}"})
        user = ci.propagate_unavailable(run_step(f"docker run qdl-core:{SHA}"), ci.Decision("run"), unavailable)
        self.assertEqual(user.action, "skip")
        other = ci.propagate_unavailable(run_step("docker run data-layer:v0.1.0"), ci.Decision("run"), unavailable)
        self.assertEqual(other.action, "run")

    def test_dockerfile_bases_ignore_stages_and_args(self):
        text = "ARG X=builder\nFROM rust@sha256:" + "b" * 64 + " AS builder\nFROM ${X} AS dep\nFROM builder\n" \
               "FROM debian@sha256:" + "c" * 64 + "\n"
        self.assertEqual(ci.dockerfile_bases(text), ["rust@sha256:" + "b" * 64, "debian@sha256:" + "c" * 64])

    def test_python_steps_use_container_when_pip_or_interpreter_differs(self):
        pip = run_step("python -m pip install x\npython -m unittest t\n")
        host = {"version": "3.12", "host": True}
        self.assertEqual(decide(pip, python_version=host).executor, "container")
        self.assertEqual(decide(run_step("python -B -m unittest t\n"), python_version=host).executor, "host")
        other = {"version": "3.10", "host": False}
        self.assertEqual(decide(run_step("python x.py\n"), python_version=other).executor, "container")
        self.assertTrue(decide(run_step("docker run x\npython y.py\n"), python_version=other).blocking)


class SafetyTests(NoProcessTests):
    def test_protected_compose_projects_are_never_owned(self):
        for project in ci.PROTECTED_PROJECTS:
            data = {"Name": "/kn-kafka", "Config": {"Labels": {"com.docker.compose.project": project}}}
            self.assertFalse(ci.owned("container", data, {project}))
        mine = {"Name": "/x", "Config": {"Labels": {"com.docker.compose.project": "qdl-localci-abc"}}}
        self.assertTrue(ci.owned("container", mine, {"qdl-localci-abc"}))
        self.assertFalse(ci.owned("container", {"Name": "/redis_marketdata", "Config": {"Labels": {}}}, set()))
        self.assertFalse(ci.owned("volume", {"Name": "kn-x", "Labels": {}}, set()))

    def test_overall_status_distinguishes_incomplete_from_pass(self):
        ok = {"status": "PASS"}
        pr = {"status": "SKIPPED", "reason": "condition false for local push event: x"}
        self.assertEqual(ci.overall_status([ok, pr]), "PASS")
        self.assertEqual(ci.overall_status([ok, {"status": "SKIPPED", "reason": "requires_sudo"}]), "INCOMPLETE")
        self.assertEqual(ci.overall_status([ok, {"status": "FAIL"}]), "FAIL")

    def test_disk_floor_is_forty_gigabytes(self):
        self.assertEqual(ci.MIN_FREE_BYTES, 40 * 1024 ** 3)


class TrivyCommandFlagsTests(unittest.TestCase):
    def test_boolean_ignore_unfixed_and_repo_ignorefile_are_passed(self):
        import tempfile
        from types import SimpleNamespace
        from pathlib import Path as _P
        import scripts.p184_local_ci as ci
        with tempfile.TemporaryDirectory() as src:
            (_P(src) / ".trivyignore").write_text("CVE-0000-0000\n")
            for value in (True, "true", "True"):
                step = SimpleNamespace(with_={"image-ref": "img", "ignore-unfixed": value, "exit-code": "1"})
                cmd = ci.trivy_command(step, "aquasec/trivy:x", src)
                self.assertIn("--ignore-unfixed", cmd)
                self.assertIn("--ignorefile", cmd)
            step = SimpleNamespace(with_={"image-ref": "img", "ignore-unfixed": False})
            self.assertNotIn("--ignore-unfixed", ci.trivy_command(step, "aquasec/trivy:x", src))


class MatrixAndScopeTests(NoProcessTests):
    def setUp(self):
        super().setUp()
        self.workflow = ci.load_workflow(WORKFLOW)

    def test_matrix_jobs_expand_with_substituted_shard(self):
        expanded = ci.expand_jobs(self.workflow)
        shards = [name for name in expanded if name.startswith("unit-tests")]
        self.assertEqual(shards, [f"unit-tests[shard={i}]" for i in (1, 2, 3, 4)])
        steps = ci.plan_steps(self.workflow, SHA, {"unit-tests[shard=3]"})
        run = next(s for s in steps if s.name == "Run unit test shard")
        self.assertIn("--shard 3 --of", run.script)
        self.assertNotIn("${{", run.script)
        self.assertEqual(ci.job_environment(expanded["unit-tests[shard=3]"][1], SHA)["QDL_TEST_SHARDS"], "4")

    def test_matrix_combinations_and_unsupported_forms(self):
        self.assertEqual(ci.matrix_combinations({}), [{}])
        self.assertEqual(ci.matrix_combinations({"strategy": {"matrix": {"a": [1, 2], "b": ["x"]}}}),
                         [{"a": 1, "b": "x"}, {"a": 2, "b": "x"}])
        self.assertEqual(ci.expanded_name("j", {"a": 1, "b": "x"}), "j[a=1,b=x]")
        for matrix in ({"include": [{"a": 1}]}, {"a": []}, {"a": 1}, "${{ fromJSON(x) }}"):
            with self.subTest(matrix=matrix), self.assertRaises(ValueError):
                ci.matrix_combinations({"strategy": {"matrix": matrix}})

    def test_full_selection_ignores_job_level_if_and_skips_orchestration(self):
        selected = ci.select_jobs(self.workflow)
        self.assertEqual(selected, list(ci.expand_jobs(self.workflow)))
        for job in ci.ORCHESTRATION_JOBS:
            self.assertIn(job, self.workflow["jobs"])
            self.assertFalse(any(name.startswith(job) for name in selected))
        for job in ("contract-tests", "rust-workspace", "release-gates", "kn-native-integration",
                    "consolidated-source-preflight", "repository-scan", "sdk-python310"):
            self.assertIn(job, selected)

    def test_changed_since_selection_follows_job_level_if(self):
        none = dict.fromkeys(("full", "python", "rust", "contracts"), False)
        docs = ci.select_jobs(self.workflow, outputs=none)
        self.assertEqual(docs, ["consolidated-source-preflight", "repository-scan"])
        python = ci.select_jobs(self.workflow, outputs={**none, "python": True})
        self.assertEqual(python, ["consolidated-source-preflight", "repository-scan", "sdk-python310",
                                  "unit-tests[shard=1]", "unit-tests[shard=2]", "unit-tests[shard=3]",
                                  "unit-tests[shard=4]", "release-gates"])
        rust = ci.select_jobs(self.workflow, outputs={**none, "rust": True})
        self.assertEqual(rust, ["consolidated-source-preflight", "repository-scan", "rust-workspace",
                                "release-gates", "kn-native-integration"])
        self.assertEqual(ci.select_jobs(self.workflow, {"unit-tests"}),
                         [f"unit-tests[shard={i}]" for i in (1, 2, 3, 4)])
        self.assertEqual(ci.select_jobs(self.workflow, {"unit-tests[shard=2]"}), ["unit-tests[shard=2]"])

    def test_unsupported_job_condition_fails_closed(self):
        for condition in ("github.ref == 'refs/heads/dev'", "needs.changes.outputs.docs == 'true'",
                          "needs.changes.outputs.python != 'true'"):
            with self.subTest(condition=condition), self.assertRaises(ValueError):
                ci.job_in_scope(condition, {"python": True})
        self.assertTrue(ci.job_in_scope(None, {}))


@unittest.skipUnless(shutil.which("git"), "git binary required (CI images carry none)")
class DryRunScopeTests(unittest.TestCase):
    """main() --dry-run against a throw-away git repository holding the working-tree ci.yml; job execution is
    patched to fail, so only read-only git calls run."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name) / "repo"
        (self.repo / ".github/workflows").mkdir(parents=True)
        (self.repo / ".github/workflows/ci.yml").write_text(WORKFLOW)
        (self.repo / "NOTES.md").write_text("x\n")
        self.git("init", "-q", "-b", "dev")
        self.base = self.commit("base")
        guard = patch.object(ci, "execute", side_effect=AssertionError("dry run must not execute jobs"))
        guard.start()
        self.addCleanup(guard.stop)

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=t", "-c", "user.email=t@invalid",
                               *args], check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", message)
        return self.git("rev-parse", "HEAD")

    def plan(self, sha, *extra):
        out = io.StringIO()
        with patch.object(ci, "image_present", return_value=False), patch.object(ci, "HOST_PYTHON", sys.executable), \
                contextlib.redirect_stdout(out):
            code = ci.main(["--repo", str(self.repo), "--sha", sha, "--out", str(Path(self.tmp.name) / "out"),
                            "--dry-run", *extra])
        self.assertEqual(code, 0)
        return json.loads(out.getvalue())

    def test_default_is_full_with_every_expanded_job(self):
        plan = self.plan(self.base)
        self.assertEqual(plan["scope"], "full")
        self.assertEqual(list(plan["jobs"]), list(ci.expand_jobs(ci.load_workflow(WORKFLOW))))
        self.assertNotIn("changes", plan["jobs"])

    def test_jobs_subset_and_changed_since_are_affected(self):
        self.assertEqual(self.plan(self.base, "--jobs", "repository-scan")["scope"], "affected")
        (self.repo / "NOTES.md").write_text("y\n")
        head = self.commit("docs")
        plan = self.plan(head, "--changed-since", self.base)
        self.assertEqual(plan["scope"], "affected")
        self.assertEqual(plan["scope_basis"]["changed_since"], self.base)
        self.assertEqual(plan["scope_basis"]["classifier"]["changed_files"], 1)
        self.assertEqual(sorted(plan["jobs"]), ["consolidated-source-preflight", "repository-scan"])
        (self.repo / "qdl").mkdir()
        (self.repo / "qdl/x.py").write_text("x = 1\n")
        head = self.commit("code")
        plan = self.plan(head, "--changed-since", self.base)
        self.assertIn("unit-tests[shard=4]", plan["jobs"])
        self.assertNotIn("rust-workspace", plan["jobs"])
        self.assertEqual(plan["scope"], "affected")

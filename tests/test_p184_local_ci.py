"""Offline tests for scripts/p184_local_ci.py plan parsing and step classification. No Docker, git or network."""

from pathlib import Path
import subprocess
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
        self.assertEqual(list(dict.fromkeys(s.job for s in self.steps)), list(raw))
        for job, body in raw.items():
            planned = [s for s in self.steps if s.job == job]
            self.assertEqual([s.index for s in planned], list(range(1, len(body["steps"]) + 1)))
            for step, raw_step in zip(planned, body["steps"]):
                self.assertEqual(step.uses, raw_step.get("uses"))
                if "run" in raw_step and "${{" not in raw_step["run"]:
                    self.assertEqual(step.script, raw_step["run"])  # same command text, byte for byte

    def test_expressions_are_substituted_with_the_exact_sha(self):
        unit = [s for s in self.steps if s.job == "unit-tests"]
        build = next(s for s in unit if s.name == "Build immutable Rust replay tool")
        self.assertIn(f"qdl-core:{SHA}", build.script)
        self.assertNotIn("${{", build.script)
        env = ci.job_environment(self.workflow["jobs"]["unit-tests"], SHA)
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
        self.assertIn("requires_sudo", by_name[("unit-tests", "Prepare non-root runtime bind mounts")].reason)
        build = by_name[("unit-tests", "Build data_layer image")]
        self.assertEqual((build.action, build.blocking), ("skip", True))
        self.assertIn("python:3.12-slim@sha256:" + "1" * 64, build.reason)
        self.assertIn("postgres:16-alpine", by_name[("unit-tests", "Run additive PostgreSQL migration gate")].reason)
        self.assertEqual(by_name[("contract-tests", "Test Python generated contracts")].executor, "container")
        self.assertIn("--allow-download",
                      by_name[("contract-tests", "Check Rust dependency, license and advisory policy")].reason)
        sdk = by_name[("sdk-python310", "Set up declared minimum Python")]
        self.assertEqual((sdk.action, sdk.blocking), ("skip", True))
        predicted = ci.predict(self.steps, decisions)
        unit = [p for s, p in zip(self.steps, predicted) if s.job == "unit-tests"]
        self.assertEqual(unit[-1][0], "RUN")  # `if: always()` compose cleanup still runs after a blocking skip
        self.assertEqual(unit[6][0], "SKIPPED")


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


if __name__ == "__main__":
    unittest.main()


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

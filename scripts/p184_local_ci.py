"""P18.4 D5: run the Data Layer GitHub Actions CI locally against one exact commit.

Purpose: the Data Layer repository is not pushed (owner rule), so `.github/workflows/ci.yml` never runs on
GitHub. This runner parses the workflow with yaml and replays every job's steps, in order and with the same
`run:` text, against a private copy of `<sha>`: `git archive` extracted per job (a throw-away git index is
added so `git diff` steps work), or a private clone when the checkout asks for full history
(`fetch-depth: 0`). Each step records name, command, exit code, duration and log path; `summary.json`
(schema qdl.p184.local-ci-receipt.v1) carries the sha and PASS/FAIL/SKIPPED per step; its SHA-256 is the
`--dl-ci-receipt-sha256` input of scripts/p184_consolidated_build.py only when the overall status is PASS and
`scope` is `full` (that script reads the receipt with `--dl-ci-receipt` and refuses anything else).

Scope: by default every job runs (push-event semantics; job-level `needs`/`if` are ignored), a `strategy.matrix`
job runs once per combination (`unit-tests[shard=1]`, ...), and the receipt says `scope: full`. The orchestration
jobs `changes` and `ci-result` (ORCHESTRATION_JOBS) only route GitHub jobs and are not replayed. `--changed-since
BASE` is a quick local preflight: scripts/ci_change_scope.py classifies `BASE...SHA` as a pull request into dev and
only the jobs whose job-level `if:` holds run; such a receipt says `scope: affected`, as does any `--jobs` subset, and
is never a release receipt.

Boundary (what it will not do):
* never pushes, publishes or changes the source repository; `--out` must be outside it;
* never runs sudo: steps that call sudo are reported SKIPPED (requires_sudo);
* never downloads tools on the host or pulls images unless `--allow-pull` / `--allow-download` is given:
  steps needing a missing image, the buf binary (a local bufbuild/buf:<version> image is used instead),
  trivy or a host-side curl are reported SKIPPED with the reason. Package fetches that a step performs
  inside its own container (apt, cargo, pip) are part of that step and are not intercepted;
* never touches running roles: it refuses to start while an image tag or container name it would create
  already exists, and every compose call uses an isolated COMPOSE_PROJECT_NAME;
* cleans what it created after each job: leftover containers/networks/volumes matching its ownership
  predicate, images it built (removed by ID, only when every tag on the ID is one it created), BuildKit
  cache records that appeared during the job (pruned by record ID), and the job's source copy. Counts go
  into summary.json. The root disk is shared with the runtime (Kafka/Postgres, 2026-10-08 incident), so it
  aborts when less than 40 GB is free on the output or Docker filesystem.

usage: p184_local_ci.py --sha SHA --out DIR [--jobs a,b] [--changed-since BASE] [--dry-run] [--allow-pull]
                        [--allow-download]
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import itertools
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
try:  # imported as scripts.p184_local_ci (tests) or run as scripts/p184_local_ci.py
    from scripts import ci_change_scope
except ImportError:
    import ci_change_scope

SCHEMA = "qdl.p184.local-ci-receipt.v1"
WORKFLOW = ".github/workflows/ci.yml"
EVENT = "push"
MIN_FREE_BYTES = 40 * 1024 ** 3
DEFAULT_STEP_TIMEOUT_S = 3600
HOST_PYTHON = "/usr/bin/python3"
# Images whose tags appear in run scripts; anything else is matched only by @sha256 digest.
KNOWN_REPOSITORIES = ("rust", "redis", "postgres", "alpine", "python", "debian", "apache/kafka",
                      "bufbuild/buf", "aquasec/trivy")
TAG_RE = re.compile(r"(?<![\w./-])((?:%s):[A-Za-z0-9][\w.-]*)(?![\w/@])" % "|".join(
    re.escape(r) for r in KNOWN_REPOSITORIES))
DIGEST_RE = re.compile(r"(?<![\w./-])([a-z0-9][\w./-]*@sha256:[0-9a-f]{64})")
SCRIPT_RE = re.compile(r"(?<![\w/.-])(scripts/[\w./-]+\.sh)\b")
DEFAULT_IMAGE_RE = re.compile(r"\$\{(\w*IMAGE):-([^}]+)\}")
EXPR_RE = re.compile(r"\$\{\{\s*(.+?)\s*\}\}")
# Names the CI steps give to their own disposable resources (ownership predicate for leftovers).
OWNED_CONTAINER_PREFIXES = ("kn-", "qdl-phase5-postgres-", "qdl_phase2_")
OWNED_NETWORK_PREFIXES = ("qdl_phase2_",)
EXTRA_OWNED_PROJECTS = ("qdl_phase71_beta",)
FIXED_NAMES = ("kn-python-redis", "kn-kafka", "kn-redis-6379", "kn-redis-6380")
PROTECTED_PROJECTS = frozenset({"data_layer", "qdl_v2_stable_candidate"})
# GitHub-only routing jobs: `changes` classifies the PR diff, `ci-result` aggregates job results.
ORCHESTRATION_JOBS = ("changes", "ci-result")
SCOPE_TERM_RE = re.compile(r"needs\.changes\.outputs\.(\w+)\s*==\s*'true'")


def owned_image_tags(sha):
    return ("data-layer:v0.1.0", f"data-layer-ci:{sha}", f"qdl-core:{sha}")


@dataclasses.dataclass
class Step:
    job: str
    index: int
    name: str
    uses: str | None
    script: str | None
    env: dict
    with_: dict
    working_directory: str | None
    condition: str | None
    timeout_s: int


@dataclasses.dataclass
class Decision:
    action: str            # run | skip | noop | checkout | setup
    executor: str = "host"  # host | container
    reason: str = ""
    blocking: bool = False
    images: tuple = ()


# ----------------------------------------------------------------------------------------------- parsing

def load_workflow(text):
    data = yaml.safe_load(text)
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), dict):
        raise ValueError("workflow has no jobs mapping")
    return data


def substitute(text, context):
    """Replace ${{ expr }} with values from context; unknown expressions stay verbatim."""
    if not isinstance(text, str):
        return text

    def repl(match):
        key = match.group(1)
        return str(context[key]) if key in context else match.group(0)
    return EXPR_RE.sub(repl, text)


def expression_context(sha, job_env, matrix=None):
    context = {"github.sha": sha, "github.event_name": EVENT, "github.token": "",
               "github.ref_name": "dev", "github.base_ref": ""}
    for key, value in job_env.items():
        context[f"env.{key}"] = value
    for key, value in (matrix or {}).items():
        context[f"matrix.{key}"] = value
    return context


def job_environment(job, sha, matrix=None):
    base = expression_context(sha, {}, matrix)
    return {key: substitute(str(value), base) for key, value in (job.get("env") or {}).items()}


def matrix_combinations(job):
    """[{}] for a plain job, else one dict per combination of a list-valued `strategy.matrix` (in YAML order)."""
    matrix = (job.get("strategy") or {}).get("matrix")
    if not matrix:
        return [{}]
    if not isinstance(matrix, dict) or any(key in matrix for key in ("include", "exclude")) or not all(
            isinstance(values, list) and values for values in matrix.values()):
        raise ValueError(f"unsupported strategy.matrix: {matrix!r}")
    keys = list(matrix)
    return [dict(zip(keys, values)) for values in itertools.product(*(matrix[key] for key in keys))]


def expanded_name(job_id, matrix):
    return job_id + ("[" + ",".join(f"{k}={v}" for k, v in matrix.items()) + "]" if matrix else "")


def expand_jobs(workflow):
    """Ordered {expanded job name: (job id, job dict, matrix values)}; orchestration jobs are left out."""
    expanded = {}
    for job_id, job in workflow["jobs"].items():
        if job_id in ORCHESTRATION_JOBS:
            continue
        for matrix in matrix_combinations(job):
            expanded[expanded_name(job_id, matrix)] = (job_id, job, matrix)
    return expanded


def job_in_scope(condition, outputs):
    """Evaluate a job-level `if:` made of `needs.changes.outputs.X == 'true'` terms joined by `||`."""
    if condition is None:
        return True
    expr = EXPR_RE.sub(lambda m: m.group(1), str(condition)).strip()
    terms = [term.strip() for term in expr.split("||")]
    values = []
    for term in terms:
        match = SCOPE_TERM_RE.fullmatch(term)
        if not match or match.group(1) not in ci_change_scope.OUTPUT_FLAGS:
            raise ValueError(f"unsupported job-level condition: {expr}")
        values.append(outputs.get(match.group(1)) is True)
    return any(values)


def select_jobs(workflow, jobs_filter=frozenset(), outputs=None):
    """Expanded job names to run. `jobs_filter` takes job ids or expanded names; `outputs` (classifier flags)
    keeps only jobs whose job-level `if:` holds."""
    selected = []
    for name, (job_id, job, _matrix) in expand_jobs(workflow).items():
        if jobs_filter and job_id not in jobs_filter and name not in jobs_filter:
            continue
        if outputs is not None and not job_in_scope(job.get("if"), outputs):
            continue
        selected.append(name)
    return selected


def plan_steps(workflow, sha, jobs=None):
    """Flatten the workflow into ordered Steps with expressions substituted (no execution). `jobs` holds expanded
    job names (see select_jobs); None plans every non-orchestration job."""
    steps = []
    for job_name, (job_id, job, matrix) in expand_jobs(workflow).items():
        if jobs is not None and job_name not in jobs:
            continue
        env = job_environment(job, sha, matrix)
        context = expression_context(sha, env, matrix)
        for index, raw in enumerate(job.get("steps") or [], start=1):
            minutes = raw.get("timeout-minutes")
            steps.append(Step(
                job=job_name, index=index, name=raw.get("name") or raw.get("uses") or f"step-{index}",
                uses=raw.get("uses"), script=substitute(raw.get("run"), context),
                env={k: substitute(str(v), context) for k, v in (raw.get("env") or {}).items()},
                with_={k: substitute(str(v), context) for k, v in (raw.get("with") or {}).items()},
                working_directory=raw.get("working-directory"), condition=raw.get("if"),
                timeout_s=int(minutes) * 60 if minutes else DEFAULT_STEP_TIMEOUT_S))
    return steps


def evaluate_condition(condition, failed):
    """Return (run, reason). GitHub semantics: no condition means success()."""
    if condition is None:
        return (not failed, "an earlier step failed or a blocking prerequisite was skipped" if failed else "")
    expr = EXPR_RE.sub(lambda m: m.group(1), str(condition)).strip()
    if expr == "always()":
        return True, ""
    if expr in ("success()",):
        return (not failed, "an earlier step failed or a blocking prerequisite was skipped" if failed else "")
    if expr == "failure()":
        return (failed, "" if failed else "no previous failure")
    match = re.fullmatch(r"github\.event_name\s*(==|!=)\s*'(\w+)'", expr)
    if match:
        equal = EVENT == match.group(2)
        ok = equal if match.group(1) == "==" else not equal
        if not ok:
            return False, f"condition false for local {EVENT} event: {expr}"
        return (not failed, "an earlier step failed or a blocking prerequisite was skipped" if failed else "")
    return False, f"unsupported condition: {expr}"


# ------------------------------------------------------------------------------------------ requirements

def dockerfile_bases(text):
    stages, bases = set(), []
    for line in text.splitlines():
        match = re.match(r"\s*FROM\s+(\S+)(?:\s+AS\s+(\S+))?", line, re.IGNORECASE)
        if not match:
            continue
        ref = match.group(1)
        if "$" not in ref and ref not in stages:
            bases.append(ref)
        if match.group(2):
            stages.add(match.group(2))
    return bases


def compose_dockerfile(compose_text, service):
    service_def = ((yaml.safe_load(compose_text) or {}).get("services") or {}).get(service) or {}
    build = service_def.get("build")
    if isinstance(build, dict):
        return build.get("dockerfile", "Dockerfile")
    return "Dockerfile" if build else None


def required_images(step, read_file, sha):
    """Images a run step needs that it does not build itself (pinned digests, known tags, script defaults,
    Dockerfile bases of the builds it starts)."""
    script = step.script or ""
    found = set(DIGEST_RE.findall(script)) | set(TAG_RE.findall(script))
    for path in sorted(set(SCRIPT_RE.findall(script))):
        text = read_file(path) or ""
        for var, default in DEFAULT_IMAGE_RE.findall(text):
            if var not in step.env:
                found.add(default)
    for match in re.finditer(r"docker build\b[^\n]*?-f\s+(\S+)", script.replace("\\\n", " ")):
        found.update(dockerfile_bases(read_file(match.group(1)) or ""))
    match = re.search(r"docker compose\b(.*?)\bbuild\s+(\w+)", script.replace("\\\n", " "))
    if match:
        files = re.findall(r"-f\s+(\S+)", match.group(1)) or ["docker-compose.yml"]
        dockerfile = compose_dockerfile(read_file(files[0]) or "", match.group(2))
        if dockerfile:
            found.update(dockerfile_bases(read_file(dockerfile) or ""))
    own = set(owned_image_tags(sha))
    return tuple(sorted(image for image in found if image not in own and "${" not in image))


def is_build_step(script):
    return bool(re.search(r"docker (compose\b[^\n]*\bbuild\b|build\b)", script or ""))


def classify(step, *, sha, read_file, image_present, host_python, python_version, tools,
             allow_pull=False, allow_download=False):
    """Decide how a step is replayed locally. Pure: every host fact comes in through arguments."""
    uses = (step.uses or "").split("@")[0]
    if uses == "actions/checkout":
        if step.with_.get("path"):
            return Decision("skip", reason="secondary checkout is pull-request only")
        return Decision("checkout", reason="clone" if str(step.with_.get("fetch-depth")) == "0" else "archive")
    if uses == "actions/setup-python":
        version = step.with_.get("python-version", "")
        if host_python.startswith(version + ".") or host_python == version:
            return Decision("setup", reason=f"host python {host_python}")
        image = f"python:{version}-slim"
        if image_present(image) or allow_pull:
            return Decision("setup", executor="container", reason=f"container {image}", images=(image,))
        return Decision("skip", reason=f"python {version} not on host and {image} not present locally "
                        "(not pulled; pass --allow-pull)", blocking=True)
    if uses == "docker/setup-buildx-action":
        return Decision("noop", reason="local docker buildx")
    if uses == "actions/cache":
        return Decision("noop", reason="no remote cache locally: cold build")
    if uses == "bufbuild/buf-setup-action":
        image = f"bufbuild/buf:{step.with_.get('version')}"
        if image_present(image):
            return Decision("setup", reason=f"buf shim over local image {image}", images=(image,))
        return Decision("skip", reason=f"{image} not present locally; buf is not downloaded")
    if uses == "aquasecurity/trivy-action":
        if tools.get("trivy_image"):
            return Decision("run", reason=f"trivy image {tools['trivy_image']}")
        return Decision("skip", reason="no local trivy image (pass --trivy-image); trivy and its vulnerability "
                        "database are not downloaded")
    if step.uses:
        return Decision("skip", reason=f"unsupported action {step.uses}")
    script = step.script or ""
    if re.search(r"(^|[\s;&|(])sudo\s", script):
        return Decision("skip", reason="requires_sudo: not run (host preparation is reported, never escalated)")
    if re.search(r"\bgh\s+release\b|\bgit\s+push\b", script):
        return Decision("skip", reason="external publication is never run locally")
    if re.search(r"^\s*(curl|wget)\b", script, re.MULTILINE) and not allow_download:
        return Decision("skip", reason="downloads a tool on the host (pass --allow-download)")
    if re.search(r"(^|\s)buf\s", script) and not tools.get("buf"):
        return Decision("skip", reason="buf unavailable (no local bufbuild/buf image)")
    images = required_images(step, read_file, sha)
    missing = [image for image in images if not image_present(image)]
    if missing and not allow_pull:
        return Decision("skip", reason="images not present locally (not pulled; pass --allow-pull): "
                        + ", ".join(missing), blocking="docker compose" in script and is_build_step(script),
                        images=images)
    uses_docker = bool(re.search(r"\bdocker\b", script))
    container = python_version and (bool(re.search(r"pip install", script)) or not python_version["host"])
    if container and not uses_docker:
        return Decision("run", executor="container", images=images,
                        reason=f"python:{python_version['version']}-slim (pip/interpreter isolation)")
    if container and uses_docker and not python_version["host"]:
        return Decision("skip", reason="needs both docker and a non-host python", blocking=True)
    return Decision("run", images=images)


def built_tags(script):
    return set(re.findall(r"-t\s+(\S+)", (script or "").replace("\\\n", " ")))


def propagate_unavailable(step, decision, unavailable):
    """A skipped `docker build -t X` makes X unavailable; later steps of the job that reference X are skipped
    instead of failing on a missing image (the compose build of the CI image is blocking and stops the job)."""
    script = step.script or ""
    if decision.action == "skip" and is_build_step(script):
        unavailable.update(built_tags(script))
        return decision
    missing = sorted(tag for tag in unavailable if tag in script)
    if missing and decision.action == "run":
        return Decision("skip", reason="depends on skipped build: " + ", ".join(missing))
    return decision


# ------------------------------------------------------------------------------------------- execution

def sh(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, **kwargs)


def git_show(repo, sha):
    def read(path):
        result = sh(["git", "-C", str(repo), "show", f"{sha}:{path}"])
        return result.stdout if result.returncode == 0 else None
    return read


def image_present(ref):
    return sh(["docker", "image", "inspect", ref]).returncode == 0


def free_bytes(path):
    return shutil.disk_usage(path).free


def docker_root():
    result = sh(["docker", "info", "--format", "{{.DockerRootDir}}"])
    return result.stdout.strip() or "/var/lib/docker"


def build_cache_ids():
    result = sh(["docker", "buildx", "du", "--verbose"])
    return re.findall(r"^ID:\s+(\S+)", result.stdout, re.MULTILINE)


def docker_ids(kind):
    args = {"container": ["docker", "ps", "-aq", "--no-trunc"], "network": ["docker", "network", "ls", "-q",
            "--no-trunc"], "volume": ["docker", "volume", "ls", "-q"]}[kind]
    return set(sh(args).stdout.split())


def inspect(kind, ident):
    result = sh(["docker", kind, "inspect", ident])
    try:
        return json.loads(result.stdout)[0]
    except (ValueError, IndexError):
        return {}


def owned(kind, data, projects):
    labels = (data.get("Config", {}).get("Labels") if kind == "container" else data.get("Labels")) or {}
    project = labels.get("com.docker.compose.project")
    if project in PROTECTED_PROJECTS:
        return False
    if project in projects or "qdl.test.namespace" in labels:
        return True
    name = (data.get("Name") or "").lstrip("/")
    prefixes = OWNED_CONTAINER_PREFIXES if kind == "container" else OWNED_NETWORK_PREFIXES
    return kind != "volume" and name.startswith(prefixes)


def write_shims(bindir, python_mode, buf_image, uid_gid):
    bindir.mkdir(parents=True, exist_ok=True)
    if python_mode == "host":
        (bindir / "python").write_text(f'#!/bin/sh\nexec {HOST_PYTHON} "$@"\n')
        (bindir / "python").chmod(0o755)
    if buf_image:
        (bindir / "buf").write_text(
            "#!/bin/sh\n# local buf: same version as buf-setup-action, run from the local image\n"
            'net=none; [ "$1" = generate ] && net=host\n'
            f'exec docker run --rm --network "$net" --user {uid_gid} -e HOME=/tmp --tmpfs /tmp:size=256m '
            '-e HTTPS_PROXY -e HTTP_PROXY -e NO_PROXY -e https_proxy -e http_proxy -e no_proxy '
            f'-v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" -w "$PWD" {buf_image} "$@"\n')
        (bindir / "buf").chmod(0o755)


def run_step(command, cwd, env, log_path, timeout_s):
    started = time.monotonic()
    with open(log_path, "w") as log:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            log.write(f"\n[p184_local_ci] TIMEOUT after {timeout_s}s\n")
            code = 124
    return code, round(time.monotonic() - started, 3)


def prepare_source(repo, sha, target, mode):
    if mode == "clone":
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout", str(repo), str(target)],
                       check=True)
        subprocess.run(["git", "-C", str(target), "checkout", "--quiet", "--detach", sha], check=True)
        return
    target.mkdir(parents=True)
    archive = subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", sha], check=True,
                             capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", str(target)], input=archive, check=True)
    # Private index so `git diff --exit-code` steps compare against the archived tree.
    for args in (["init", "--quiet"], ["add", "-A"],
                 ["-c", "user.name=p184-local-ci", "-c", "user.email=local-ci@invalid", "commit", "--quiet",
                  "--no-verify", "-m", f"archive {sha}"]):
        subprocess.run(["git", "-C", str(target), *args], check=True, capture_output=True)


def remove_tree(path, counts):
    if not path.exists():
        return
    try:
        shutil.rmtree(path)
    except PermissionError:
        # Root-owned output from steps that bind-mount the workspace (cargo target/). Removed inside a --rm
        # container from the image the host already has; the directory is this runner's private copy.
        sh(["docker", "run", "--rm", "--network", "none", "-v", f"{path}:/w", "alpine:3.20",
            "sh", "-c", "rm -rf /w/* /w/.[!.]* /w/..?*"])
        counts["root_cleanup_containers"] = counts.get("root_cleanup_containers", 0) + 1
        shutil.rmtree(path)


def cleanup_job(sha, before, projects, since, counts):
    for kind in ("container", "network", "volume"):
        events = sh(["docker", "events", "--since", since, "--until", str(int(time.time())), "--filter",
                     f"type={kind}", "--filter", "event=create", "--format", "{{.Actor.ID}}"])
        counts[f"{kind}s_created"] = counts.get(f"{kind}s_created", 0) + len(events.stdout.split())
        removed = foreign = 0
        for ident in sorted(docker_ids(kind) - before[kind]):
            data = inspect(kind, ident)
            if data and owned(kind, data, projects):
                args = {"container": ["docker", "rm", "-f", ident], "network": ["docker", "network", "rm", ident],
                        "volume": ["docker", "volume", "rm", ident]}[kind]
                removed += sh(args).returncode == 0
            else:
                foreign += 1
        counts[f"{kind}s_leftover_removed"] = counts.get(f"{kind}s_leftover_removed", 0) + removed
        counts[f"{kind}s_new_not_owned_untouched"] = counts.get(f"{kind}s_new_not_owned_untouched", 0) + foreign
    tags = set(owned_image_tags(sha))
    for tag in sorted(tags):
        data = inspect("image", tag)
        if not data or data["Id"] in before["images"]:
            continue
        if not set(data.get("RepoTags") or []) <= tags:
            sh(["docker", "image", "rm", tag])  # foreign tag shares the ID: untag ours only
            counts["image_tags_removed"] = counts.get("image_tags_removed", 0) + 1
            continue
        if sh(["docker", "ps", "-aq", "--filter", f"ancestor={data['Id']}"]).stdout.strip():
            counts["images_kept_in_use"] = counts.get("images_kept_in_use", 0) + 1
            continue
        if sh(["docker", "image", "rm", "--force", data["Id"]]).returncode == 0:
            counts["images_removed_by_id"] = counts.get("images_removed_by_id", 0) + 1
            counts.setdefault("image_ids_removed", []).append(data["Id"])
    new_cache = [ident for ident in reversed(build_cache_ids()) if ident not in before["cache"]]
    pruned = 0
    for ident in new_cache:
        pruned += sh(["docker", "builder", "prune", "--force", "--filter", f"id={ident}"]).returncode == 0
    counts["build_cache_records_new"] = counts.get("build_cache_records_new", 0) + len(new_cache)
    counts["build_cache_prune_calls_ok"] = counts.get("build_cache_prune_calls_ok", 0) + pruned


def preflight_host(sha, out):
    problems = []
    for path in (out, docker_root()):
        try:
            free = free_bytes(path)
        except OSError:
            free = free_bytes(out)
        if free < MIN_FREE_BYTES:
            problems.append(f"{path}: {free / 1024 ** 3:.1f} GB free < 40 GB")
    for tag in owned_image_tags(sha):
        if image_present(tag):
            problems.append(f"image tag {tag} already exists (another run or a foreign image); refusing")
    for name in FIXED_NAMES:
        if sh(["docker", "container", "inspect", name]).returncode == 0:
            problems.append(f"container {name} already exists; refusing")
    for project in EXTRA_OWNED_PROJECTS:
        if sh(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"]).stdout.strip():
            problems.append(f"compose project {project} has containers; refusing")
    return problems


def execute(args, steps, decisions, sha, out):
    summary_steps, counts, job_status = [], {}, {}
    uid_gid = f"{os.getuid()}:{os.getgid()}"
    project = f"qdl-localci-{sha[:12]}"
    assert project not in PROTECTED_PROJECTS
    for job in dict.fromkeys(step.job for step in steps):
        problems = [p for p in preflight_host(sha, out) if "GB free" in p]
        if problems:
            raise SystemExit("disk precheck failed: " + "; ".join(problems))
        job_dir = out / "jobs" / job
        source, bindir, tmp = job_dir / "src", job_dir / "bin", job_dir / "tmp"
        for path in (bindir, tmp, job_dir / "runner-temp", job_dir / "logs"):
            path.mkdir(parents=True, exist_ok=True)
        before = {kind: docker_ids(kind) for kind in ("container", "network", "volume")}
        before["cache"] = set(build_cache_ids())
        before["images"] = {inspect("image", tag).get("Id") for tag in owned_image_tags(sha)} - {None}
        wanted = {image for s in steps if s.job == job for image in decisions[(job, s.index)].images}
        absent = sorted(image for image in wanted if not image_present(image))
        since = str(int(time.time()) - 1)
        failed, python_version, buf_image = False, None, None
        job_steps = [s for s in steps if s.job == job]
        env_base = dict(os.environ)
        env_base.update({"CI": "true", "GITHUB_ACTIONS": "true", "GITHUB_SHA": sha, "GITHUB_EVENT_NAME": EVENT,
                         "GITHUB_WORKSPACE": str(source), "RUNNER_TEMP": str(job_dir / "runner-temp"),
                         "TMPDIR": str(tmp), "PYTHONDONTWRITEBYTECODE": "1", "COMPOSE_PROJECT_NAME": project,
                         "PATH": f"{bindir}:{os.environ.get('PATH', '')}"})
        _job_id, job_def, matrix = args.expanded_jobs[job]
        env_base.update(job_environment(job_def, sha, matrix))
        try:
            for step in job_steps:
                decision = decisions[(job, step.index)]
                run_it, why = evaluate_condition(step.condition, failed)
                record = {"job": job, "index": step.index, "name": step.name, "uses": step.uses,
                          "command": step.script, "executor": decision.executor, "exit_code": None,
                          "duration_s": 0.0, "log": None}
                if not run_it:
                    record.update(status="SKIPPED", reason=why)
                elif decision.action == "skip":
                    record.update(status="SKIPPED", reason=decision.reason)
                    failed = failed or decision.blocking
                elif decision.action == "checkout":
                    started = time.monotonic()
                    try:
                        prepare_source(args.repo, sha, source, decision.reason)
                        record.update(status="PASS", exit_code=0, reason=f"{decision.reason} of {sha}")
                    except subprocess.CalledProcessError as exc:
                        record.update(status="FAIL", exit_code=exc.returncode, reason=str(exc))
                        failed = True
                    record["duration_s"] = round(time.monotonic() - started, 3)
                elif decision.action in ("setup", "noop"):
                    if step.uses and step.uses.startswith("actions/setup-python"):
                        python_version = {"version": step.with_["python-version"],
                                          "host": decision.executor == "host"}
                    if step.uses and step.uses.startswith("bufbuild/buf-setup-action"):
                        buf_image = decision.images[0]
                    write_shims(bindir, "host" if python_version and python_version["host"] else None,
                                buf_image, uid_gid)
                    record.update(status="PASS", exit_code=0, reason=decision.reason)
                else:
                    log = job_dir / "logs" / f"{step.index:02d}-{re.sub(r'[^a-z0-9]+', '-', step.name.lower())[:60]}.log"
                    script_file = job_dir / "logs" / f"{step.index:02d}.sh"
                    cwd = source / (step.working_directory or ".")
                    env = dict(env_base, **step.env)
                    if step.uses:  # trivy
                        command = trivy_command(step, args.trivy_image, source)
                    elif decision.executor == "container":
                        script_file.write_text(step.script)
                        image = f"python:{python_version['version']}-slim"
                        command = ["docker", "run", "--rm", "--network", "host", "--user", uid_gid,
                                   "-e", "HOME=/tmp/home", "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", f"GITHUB_SHA={sha}",
                                   *sum((["-e", k] for k in ("HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY", "https_proxy",
                                                              "http_proxy", "no_proxy") if k in os.environ), []),
                                   "--tmpfs", "/tmp:size=1g,exec", "-v", f"{job_dir}:{job_dir}", "-w", str(cwd),
                                   image, "bash", "--noprofile", "--norc", "-eo", "pipefail", str(script_file)]
                    else:
                        script_file.write_text(step.script)
                        command = ["bash", "--noprofile", "--norc", "-eo", "pipefail", str(script_file)]
                    code, duration = run_step(command, cwd, env, log, step.timeout_s)
                    record.update(status="PASS" if code == 0 else "FAIL", exit_code=code, duration_s=duration,
                                  log=str(log))
                    failed = failed or code != 0
                summary_steps.append(record)
                print(f"[{record['status']:7}] {job} #{step.index} {step.name}", flush=True)
        finally:
            job_counts = {"images_pulled_kept": [image for image in absent if image_present(image)]}
            cleanup_job(sha, before, {project, *EXTRA_OWNED_PROJECTS}, since, job_counts)
            remove_tree(source, job_counts)
            for path in (tmp, job_dir / "runner-temp", bindir):
                remove_tree(path, job_counts)
            counts[job] = job_counts
        job_status[job] = overall_status([r for r in summary_steps if r["job"] == job])
    return summary_steps, counts, job_status


def trivy_command(step, image, source):
    with_ = step.with_
    common = ["--exit-code", with_.get("exit-code", "1"), "--severity", with_.get("severity", "CRITICAL,HIGH"),
              "--format", with_.get("format", "table")]
    if with_.get("scan-type") == "fs":
        return ["docker", "run", "--rm", "-v", f"{source}:/scan:ro", image, "fs", *common,
                "--scanners", with_.get("scanners", "vuln"), "/scan"]
    # YAML parses `ignore-unfixed: true` as a boolean; accept both spellings, as trivy-action does (a string-only
    # comparison silently dropped the flag and scanned 44 unfixed Debian HIGHs, 2026-10-10).
    extra = ["--ignore-unfixed"] if str(with_.get("ignore-unfixed", "")).lower() == "true" else []
    mounts = ["-v", "/var/run/docker.sock:/var/run/docker.sock:ro"]
    if (Path(source) / ".trivyignore").is_file():
        # trivy-action reads the repository's .trivyignore from the workspace; mount it the same way.
        mounts += ["-v", f"{Path(source) / '.trivyignore'}:/tmp/.trivyignore:ro"]
        extra += ["--ignorefile", "/tmp/.trivyignore"]
    return ["docker", "run", "--rm", *mounts, image, "image", *common, *extra, with_["image-ref"]]


def predict(steps, decisions):
    """Dry-run view of execute(): per step RUN or SKIPPED with the reason, honouring `if:` and blocking skips
    (a step that would actually run is assumed to pass)."""
    predicted, failed, job = [], False, None
    for step in steps:
        if step.job != job:
            job, failed = step.job, False
        decision = decisions[(step.job, step.index)]
        run_it, why = evaluate_condition(step.condition, failed)
        if not run_it:
            predicted.append(("SKIPPED", why))
        elif decision.action == "skip":
            predicted.append(("SKIPPED", decision.reason))
            failed = failed or decision.blocking
        else:
            predicted.append(("RUN", decision.reason))
    return predicted


def overall_status(records):
    statuses = [r["status"] for r in records]
    if "FAIL" in statuses:
        return "FAIL"
    real_skips = [r for r in records if r["status"] == "SKIPPED"
                  and not r.get("reason", "").startswith(("condition false", "secondary checkout"))]
    return "INCOMPLETE" if real_skips else "PASS"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--workflow", default=WORKFLOW)
    parser.add_argument("--jobs", default="", help="comma-separated job ids or expanded names (default: all); "
                        "a subset makes the receipt scope=affected")
    parser.add_argument("--changed-since", default="", help="quick preflight: run only the jobs scripts/"
                        "ci_change_scope.py selects for BASE...SHA (receipt scope=affected, never a release input)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-pull", action="store_true")
    parser.add_argument("--allow-download", action="store_true")
    parser.add_argument("--trivy-image", default="")
    args = parser.parse_args(argv)
    args.repo = args.repo.resolve()
    sha = sh(["git", "-C", str(args.repo), "rev-parse", "--verify", f"{args.sha}^{{commit}}"]).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise SystemExit(f"cannot resolve commit {args.sha}")
    out = args.out.resolve()
    if out == args.repo or args.repo in out.parents:
        raise SystemExit("--out must be outside the repository")
    read_file = git_show(args.repo, sha)
    workflow_text = read_file(args.workflow)
    if workflow_text is None:
        raise SystemExit(f"{args.workflow} not present at {sha}")
    args.workflow_data = load_workflow(workflow_text)
    args.expanded_jobs = expand_jobs(args.workflow_data)
    jobs_filter = {j for j in args.jobs.split(",") if j}
    known = set(args.expanded_jobs) | {job_id for job_id, _job, _m in args.expanded_jobs.values()}
    unknown = jobs_filter - known
    if unknown:
        raise SystemExit(f"unknown jobs: {sorted(unknown)}")
    scope_basis = {"jobs_filter": sorted(jobs_filter), "changed_since": None, "classifier": None}
    outputs = None
    if args.changed_since:
        base = sh(["git", "-C", str(args.repo), "rev-parse", "--verify",
                   f"{args.changed_since}^{{commit}}"]).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}", base):
            raise SystemExit(f"cannot resolve --changed-since {args.changed_since}")
        decision = ci_change_scope.decide(repo=args.repo, base=base, head=sha, event="pull_request",
                                          base_ref="dev", labels=set())
        outputs = {flag: decision[flag] for flag in ci_change_scope.OUTPUT_FLAGS}
        scope_basis.update(changed_since=base, classifier=decision)
    selected = select_jobs(args.workflow_data, jobs_filter, outputs)
    scope = "affected" if (jobs_filter or args.changed_since) else "full"
    if scope == "full" and selected != list(args.expanded_jobs):
        raise SystemExit("internal: full scope must select every job")
    if not selected:
        print(json.dumps({"schema": SCHEMA + ".plan", "sha": sha, "scope": scope, "scope_basis": scope_basis,
                          "jobs": {}, "note": "no job in scope"}, indent=2))
        return 0
    steps = plan_steps(args.workflow_data, sha, set(selected))
    host_python = sh([HOST_PYTHON, "-c", "import platform; print(platform.python_version())"]).stdout.strip()
    decisions, python_version, tools = {}, None, {"trivy_image": args.trivy_image}
    current_job, unavailable = None, set()
    for step in steps:
        if step.job != current_job:
            current_job, python_version, tools["buf"], unavailable = step.job, None, False, set()
        decision = classify(step, sha=sha, read_file=read_file, image_present=image_present,
                            host_python=host_python, python_version=python_version, tools=tools,
                            allow_pull=args.allow_pull, allow_download=args.allow_download)
        if decision.action == "setup" and (step.uses or "").startswith("actions/setup-python"):
            python_version = {"version": step.with_.get("python-version"), "host": decision.executor == "host"}
        if decision.action == "setup" and (step.uses or "").startswith("bufbuild/buf-setup-action"):
            tools["buf"] = True
        decision = propagate_unavailable(step, decision, unavailable)
        decisions[(step.job, step.index)] = decision
    predicted = predict(steps, decisions)
    plan = [{"job": s.job, "index": s.index, "name": s.name, "action": decisions[(s.job, s.index)].action,
             "predicted": predicted[i][0], "predicted_reason": predicted[i][1],
             "executor": decisions[(s.job, s.index)].executor, "reason": decisions[(s.job, s.index)].reason,
             "blocking": decisions[(s.job, s.index)].blocking, "images": list(decisions[(s.job, s.index)].images),
             "condition": s.condition, "timeout_s": s.timeout_s} for i, s in enumerate(steps)]
    if args.dry_run:
        jobs_view = {}
        for entry in plan:
            view = jobs_view.setdefault(entry["job"], {"steps": 0, "run": 0, "skipped": []})
            view["steps"] += 1
            if entry["predicted"] == "RUN":
                view["run"] += 1
            else:
                view["skipped"].append(f'#{entry["index"]} {entry["name"]}: {entry["predicted_reason"]}')
        print(json.dumps({"schema": SCHEMA + ".plan", "sha": sha, "workflow": args.workflow, "scope": scope,
                          "scope_basis": scope_basis, "host_python": host_python, "jobs": jobs_view,
                          "steps": plan}, indent=2))
        return 0
    out.mkdir(parents=True, exist_ok=True)
    problems = preflight_host(sha, out)
    if problems:
        raise SystemExit("precheck failed: " + "; ".join(problems))
    started = dt.datetime.now(dt.timezone.utc).isoformat()
    records, counts, job_status = execute(args, steps, decisions, sha, out)
    summary = {"schema": SCHEMA, "sha": sha, "workflow": args.workflow, "event": EVENT, "started_at": started,
               "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(), "jobs": job_status,
               "scope": scope, "scope_basis": scope_basis, "workflow_jobs": list(args.expanded_jobs),
               "orchestration_jobs_not_replayed": [j for j in ORCHESTRATION_JOBS if j in args.workflow_data["jobs"]],
               "status": overall_status(records), "steps": records, "cleanup": counts,
               "flags": {"allow_pull": args.allow_pull, "allow_download": args.allow_download,
                         "trivy_image": args.trivy_image or None}}
    path = out / "summary.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": summary["status"], "summary": str(path),
                      "summary_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}))
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

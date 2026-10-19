#!/usr/bin/env python3
"""Decide which CI suites a change needs, and enforce that decision in the final CI status.

Purpose: a docs-only journal commit used to cost a full 30-minute CI run (GitHub run 39201550345). This classifier
maps the changed paths of a pull request into dev onto four flags (`full`, `python`, `rust`, `contracts`) that
gate the jobs of `.github/workflows/ci.yml`, and `--verify-needs` turns the results of those jobs into one
pass/fail status (`ci-result`) that is green only when every in-scope job succeeded.

Rules (fail safe toward running):
* full (every flag true) when the event is not `pull_request` (push to dev, workflow_dispatch), the PR targets
  `main` (release path), the PR carries the `ci:full` label, the base is unknown / all-zero / the diff fails, or a
  changed path defines CI itself (FULL_PATTERNS);
* docs (`*.md` outside qdl_sdk/ and tests/, `docs/**`, `LICENSE`) set no suite, unless the doc's exact path is
  named by a tracked file under `tests/` (the suite reads runbooks such as docs/runbooks/phase93-hold-close-expand.md);
* Rust (RUST_PATTERNS) sets rust; `tests/fixtures/**` sets python + rust (Rust tests read those fixtures);
  `contracts/**` and `generated/**` set contracts + python + rust (Rust tests read contracts/golden and
  generated/rust);
* anything else, including unknown paths, sets python.

Tree-identity reuse (`--reuse-lookup`, CI only): identical git tree => identical bytes, `.github` included, so a
full green CI run of that tree is the evidence and running it again adds nothing (a re-run is not flake
detection). The `changes` job records `tree=` (the tested checkout: the merge ref for a pull_request, the pushed
commit for a push) and looks at the last REUSE_CANDIDATES successful runs of ci.yml. The API reports a
pull_request run's head_sha as the PR head, not the merge it tested, so a candidate's tree is read from its own
`changes` log (`tree=`); runs without that line are never reused. A candidate is reused only if it is attempt 1,
every job concluded success (none skipped / cancelled / failed), its classifier said full, it was not itself
inherited, and its tree is byte-identical. Then every suite flag is false, `reuse_run=<id>`, and on a pull_request
`base_check=true` keeps contract-tests (the PR-base `buf breaking` depends on the base; it is set on every
pull_request run). workflow_dispatch and the `ci:full` label always run fresh. Any API error, missing token or
deadline means no reuse.

Incremental base (CI2, pull_request only): the scope diff starts at the nearest strict ancestor of the PR head that
has a green full or inherited-full run of ci.yml (its head_sha), instead of the PR base; none found or any lookup
failure falls back to the PR base. A CI-defining file changed since that ancestor still forces full. A scoped
incremental run is never a release receipt (CI6): only a full run, or a run that inherited one on the same tree.

Owner rules CI1-CI8 (/home/bobby/CLAUDE.md section 7b, 2026-10-19) govern this file.

Boundary: read-only on the repository (git diff / git grep / rev-parse) and on the GitHub API; stdlib only so it runs
on the bare runner python; it never selects tests inside a suite. The release receipt still requires a full run (scripts/p184_local_ci.py
marks scoped receipts `scope=affected`; scripts/p184_consolidated_build.py refuses them).

usage:
  ci_change_scope.py --base SHA --head SHA --event NAME [--base-ref REF] [--labels a,b] [--repo DIR]
                     [--github-output FILE] [--summary-file FILE]
  ci_change_scope.py --verify-needs   (reads the job results JSON from $NEEDS_JSON, i.e. ${{ toJSON(needs) }})
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

FLAGS = ("full", "python", "rust", "contracts")
# base_check: run contract-tests (PR-base `buf breaking` depends on the base, not only the tree) on an inherited PR run.
OUTPUT_FLAGS = FLAGS + ("base_check",)
WORKFLOW_PATH = ".github/workflows/ci.yml"
REUSE_CANDIDATES = 20
REUSE_DEADLINE_S = 90
OUTPUT_LINE = re.compile(r"^\S+Z (full|python|rust|contracts|base_check|tree|reuse_run)=(\S*)\s*$", re.M)
SUMMARY_FIELD = re.compile(r'^\S+Z\s+"(event|base|head|base_ref)": "([^"]*)",?\s*$', re.M)
FULL_LABEL = "ci:full"
# Paths that define how CI itself builds or runs: any change re-runs every suite.
FULL_PATTERNS = (
    ".github/**", "Dockerfile*", "docker-compose*.yml", ".dockerignore", ".trivyignore", "pyproject.toml",
    "poetry.lock", "requirements*", "scripts/ci_change_scope.py", "scripts/ci_test_shard.py",
    "scripts/prepare_nonroot_runtime.sh",
)
DOC_PATTERNS = ("*.md", "docs/**", "LICENSE")
DOC_EXCEPTIONS = ("qdl_sdk/**", "tests/**")  # SDK README/CHANGELOG ship in the wheel; tests/** are test inputs
RUST_PATTERNS = ("rust/**", "Cargo.toml", "Cargo.lock", "deny.toml", "rust-toolchain*", ".cargo/**")
CONTRACT_PATTERNS = ("contracts/**", "generated/**")
RUST_FIXTURE_PATTERNS = ("tests/fixtures/**",)
# Job -> flags that put it in scope (mirrors the job-level `if:` of ci.yml; tests/test_ci_change_scope.py
# asserts the two agree). Jobs absent from this map are always in scope.
JOB_SCOPE = {
    "contract-tests": ("contracts", "base_check"),
    "rust-workspace": ("rust",),
    "sdk-python310": ("python",),
    "unit-tests": ("python",),
    "release-gates": ("python", "rust"),
    "kn-native-integration": ("rust",),
}
ALWAYS_JOBS = ("changes", "consolidated-source-preflight", "repository-scan")


def matches(path, patterns):
    """Glob match where `**` spans directories and a bare pattern without `/` matches the basename anywhere."""
    for pattern in patterns:
        if pattern.endswith("/**"):
            if path.startswith(pattern[:-2]):
                return True
        elif "/" in pattern:
            if fnmatch.fnmatchcase(path, pattern):
                return True
        elif fnmatch.fnmatchcase(path.rsplit("/", 1)[-1], pattern) and (
                pattern.startswith("*") or "/" not in path):
            # `*.md` matches anywhere; a literal root name (`LICENSE`, `Cargo.toml`, `Dockerfile*`) only at the root.
            return True
    return False


def is_doc(path):
    return matches(path, DOC_PATTERNS) and not matches(path, DOC_EXCEPTIONS)


def classify_paths(paths, test_referenced=frozenset()):
    """Return (flags, reasons) for a list of changed repo-relative paths. Pure."""
    flags = dict.fromkeys(FLAGS, False)
    reasons = []
    for path in paths:
        if matches(path, FULL_PATTERNS):
            flags["full"] = True
            reasons.append(f"ci-defining path {path}")
        elif is_doc(path):
            if path in test_referenced:
                flags["python"] = True
                reasons.append(f"doc read by tests {path}")
        elif matches(path, CONTRACT_PATTERNS):
            flags.update(contracts=True, python=True, rust=True)
        elif matches(path, RUST_PATTERNS):
            flags["rust"] = True
        elif matches(path, RUST_FIXTURE_PATTERNS):
            flags.update(python=True, rust=True)
        else:
            flags["python"] = True
    return flags, reasons


def finalize(flags):
    if flags["full"]:
        return dict.fromkeys(FLAGS, True)
    return dict(flags)


def git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], text=True, capture_output=True)


def changed_paths(repo, base, head):
    """Paths changed by `head` relative to its merge base with `base`; None when that cannot be computed."""
    if not re.fullmatch(r"[0-9a-f]{40}", base or "") or set(base) == {"0"}:
        return None
    if not re.fullmatch(r"[0-9a-f]{40}", head or ""):
        return None
    result = git(repo, "diff", "--name-only", "--no-renames", "-z", f"{base}...{head}")
    if result.returncode != 0:
        return None
    return sorted(p for p in result.stdout.split("\0") if p)


def docs_read_by_tests(repo, head, docs):
    """Subset of `docs` whose exact path appears in a tracked file under tests/ at `head`."""
    if not docs:
        return frozenset()
    args = ["grep", "-l", "-F"]
    for doc in docs:
        args += ["-e", doc]
    result = git(repo, *args, head, "--", "tests")
    if result.returncode not in (0, 1):
        return frozenset(docs)  # cannot tell: treat every doc as a test input
    referenced = set()
    for line in result.stdout.splitlines():
        name = line.split(":", 1)[1] if ":" in line else line
        text = git(repo, "show", f"{head}:{name}").stdout
        referenced.update(doc for doc in docs if names_path(text, doc))
    return frozenset(referenced)


def names_path(text, path):
    """True when `path` occurs in `text` as a whole path (`qdl_sdk/README.md` does not name `README.md`)."""
    return re.search(r"(?<![\w./-])" + re.escape(path) + r"(?![\w/-]|\.\w)", text) is not None


class _DropAuthOnRedirect(urllib.request.HTTPRedirectHandler):
    """Job logs redirect to a signed blob URL that rejects a forwarded Authorization header."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc:
            new.remove_header("Authorization")
        return new


class GitHubAPI:
    """Minimal read-only GitHub REST client (stdlib). The token is sent as a header and never printed."""

    def __init__(self, token, repo, timeout=20):
        if not token or not re.fullmatch(r"[\w.-]+/[\w.-]+", repo or ""):
            raise ValueError("GitHub token and OWNER/NAME repository required")
        self.token, self.repo, self.timeout = token, repo, timeout
        self.opener = urllib.request.build_opener(_DropAuthOnRedirect)

    def request(self, path, accept="application/vnd.github+json"):
        req = urllib.request.Request(f"https://api.github.com/repos/{self.repo}{path}", headers={
            "Authorization": f"Bearer {self.token}", "Accept": accept, "X-GitHub-Api-Version": "2022-11-28"})
        with self.opener.open(req, timeout=self.timeout) as response:
            return response.read()

    def json(self, path):
        return json.loads(self.request(path))

    def run(self, run_id):
        return self.json(f"/actions/runs/{int(run_id)}")

    def successful_runs(self, limit=REUSE_CANDIDATES):
        workflow = WORKFLOW_PATH.rsplit("/", 1)[-1]
        return self.json(f"/actions/workflows/{workflow}/runs?status=success&per_page={int(limit)}")["workflow_runs"]

    def jobs(self, run_id):
        jobs, page = [], 1
        while True:
            data = self.json(f"/actions/runs/{int(run_id)}/jobs?filter=latest&per_page=100&page={page}")
            jobs += data["jobs"]
            if len(jobs) >= data["total_count"] or not data["jobs"]:
                return jobs
            page += 1

    def job_log(self, job_id):
        return self.request(f"/actions/jobs/{int(job_id)}/logs").decode(errors="replace")


def parse_changes_log(text):
    """Classifier outputs (`key=value` stdout lines) and summary fields from a `changes` job log. A key that
    appears twice must carry the same value; the four scope flags are mandatory."""
    outputs, fields = {}, {}
    for target, pattern in ((outputs, OUTPUT_LINE), (fields, SUMMARY_FIELD)):
        for key, value in pattern.findall(text):
            if target.setdefault(key, value) != value:
                raise ValueError(f"changes log has conflicting {key} values")
    missing = [flag for flag in FLAGS if outputs.get(flag) not in ("true", "false")]
    if missing:
        raise ValueError(f"changes log lacks classifier outputs {missing}")
    return outputs, fields


def full_run_problems(jobs, outputs):
    """Why a run cannot serve as full CI evidence (empty list = it can): classifier full, not itself inherited,
    every workflow job present and success, none skipped / failed / cancelled."""
    problems = []
    if any(outputs.get(flag) != "true" for flag in FLAGS):
        problems.append("classifier scope not full")
    if outputs.get("reuse_run"):
        problems.append(f"itself inherited from run {outputs['reuse_run']}")
    names = {job["name"]: job.get("conclusion") for job in jobs}
    for job_id in (*ALWAYS_JOBS, *JOB_SCOPE, "ci-result"):
        if not any(name == job_id or name.startswith(job_id + " (") for name in names):
            problems.append(f"job {job_id} missing")
    bad = sorted(name for name, conclusion in names.items() if conclusion != "success")
    if bad:
        problems.append(f"jobs not success: {bad}")
    return problems


def classify_candidate(run, jobs, outputs):
    """'full' (ran every suite green), 'inherited' (green, inherited a full run on its tree) or None."""
    if run.get("path") != WORKFLOW_PATH or run.get("conclusion") != "success" or run.get("run_attempt") != 1:
        return None
    if not full_run_problems(jobs, outputs):
        return "full"
    names = {job["name"]: job.get("conclusion") for job in jobs}
    if (outputs.get("reuse_run", "").isdigit() and names.get("ci-result") == "success"
            and all(conclusion in ("success", "skipped") for conclusion in names.values())):
        return "inherited"
    return None


def evaluate_candidates(api, current_run_id=None, deadline_s=REUSE_DEADLINE_S, clock=time.monotonic):
    """Recent successful CI runs as [{run_id, event, head_sha, tree, kind}], newest first, plus notes. A run whose
    jobs or `changes` log cannot be read is left out; a listing failure returns no candidates (fail toward running).
    The API reports a pull_request run's head_sha as the PR head, not the merge it tested: the tested tree is read
    from the run's own `changes` log (`tree=`), and runs without that line have no tree."""
    notes, evaluated = [], []
    end = clock() + deadline_s
    try:
        candidates = api.successful_runs()
    except Exception as exc:  # noqa: BLE001 - any failure means run fresh
        return [], [f"candidate listing failed: {type(exc).__name__}"]
    for run in candidates:
        if clock() > end:
            notes.append("lookup deadline reached")
            break
        run_id = run.get("id")
        if str(run_id) == str(current_run_id or "") or run.get("path") != WORKFLOW_PATH:
            continue
        if run.get("run_attempt") != 1:
            notes.append(f"{run_id}: attempt {run.get('run_attempt')} (re-run), not used")
            continue
        try:
            jobs = api.jobs(run_id)
            changes = [job for job in jobs if job.get("name") == "changes"]
            if len(changes) != 1:
                notes.append(f"{run_id}: no changes job")
                continue
            outputs, _fields = parse_changes_log(api.job_log(changes[0]["id"]))
        except Exception as exc:  # noqa: BLE001
            notes.append(f"{run_id}: lookup failed ({type(exc).__name__})")
            continue
        tree = outputs.get("tree", "")
        kind = classify_candidate(run, jobs, outputs)
        evaluated.append({"run_id": run_id, "event": run.get("event"), "head_sha": run.get("head_sha"),
                          "tree": tree if re.fullmatch(r"[0-9a-f]{40}", tree) else "", "kind": kind})
    return evaluated, notes


def reusable_run(evaluated, tree):
    """CI1: the newest run that ran every suite green on the byte-identical tree (never an inherited run)."""
    for candidate in evaluated:
        if candidate["kind"] == "full" and tree and candidate["tree"] == tree:
            return candidate["run_id"]
    return None


def incremental_base(evaluated, repo, head):
    """CI2: the nearest strict ancestor of `head` that has a green full or inherited-full run (its head_sha).
    Returns (sha, run_id) or (None, None)."""
    best = None
    for candidate in evaluated:
        sha = candidate["head_sha"] or ""
        if candidate["kind"] not in ("full", "inherited") or not re.fullmatch(r"[0-9a-f]{40}", sha) or sha == head:
            continue
        if git(repo, "merge-base", "--is-ancestor", sha, head).returncode != 0:
            continue
        count = git(repo, "rev-list", "--count", f"{sha}..{head}")
        if count.returncode != 0 or not count.stdout.strip().isdigit():
            continue
        distance = int(count.stdout.strip())
        if best is None or distance < best[0]:
            best = (distance, sha, candidate["run_id"])
    return (best[1], best[2]) if best else (None, None)


def tested_tree(repo, rev="HEAD"):
    result = git(repo, "rev-parse", "--verify", f"{rev}^{{tree}}")
    value = result.stdout.strip()
    return value if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", value) else ""


def decide(*, repo, base, head, event, base_ref, labels, tree="", reuse_api=None, current_run_id=None):
    """Scope of one CI run. With `reuse_api` (GitHub, CI only): CI1 tree-identity reuse first, then for a pull
    request CI2 incremental base (nearest green full ancestor on the head line instead of the PR base)."""
    summary = {"event": event, "base": base, "head": head, "base_ref": base_ref, "labels": sorted(labels),
               "tree": tree, "reuse_run": "", "reuse_notes": [], "scope_base": base, "scope_base_run": None,
               "base_check": event == "pull_request"}
    forced_fresh = event == "workflow_dispatch" or FULL_LABEL in labels
    evaluated = []
    if reuse_api is not None and tree and not forced_fresh and event in ("pull_request", "push"):
        evaluated, notes = evaluate_candidates(reuse_api, current_run_id)
        summary["reuse_notes"] = notes[:20]
        run_id = reusable_run(evaluated, tree)
        if run_id:
            summary.update(dict.fromkeys(FLAGS, False), reuse_run=str(run_id), changed_files=0, forced=[],
                           paths_sample=[], reason=f"inherited full CI run {run_id} on identical tree {tree}")
            return summary
    elif forced_fresh:
        summary["reuse_notes"] = ["fresh run forced (workflow_dispatch or ci:full)"]
    scope_base = base
    if event == "pull_request" and evaluated:
        sha, run_id = incremental_base(evaluated, repo, head)
        if sha:
            scope_base = sha
            summary.update(scope_base=sha, scope_base_run=run_id)
    forced = []
    if event != "pull_request":
        forced.append(f"event {event or '<none>'} is not pull_request")
    if base_ref == "main":
        forced.append("pull request into main (release path)")
    if FULL_LABEL in labels:
        forced.append(f"label {FULL_LABEL}")
    paths = changed_paths(repo, scope_base, head)
    if paths is None:
        forced.append("base unknown, all-zero or diff failed")
        paths = []
    docs = [p for p in paths if is_doc(p) and not matches(p, FULL_PATTERNS)]
    flags, reasons = classify_paths(paths, docs_read_by_tests(repo, head, docs) if paths else frozenset())
    if forced:
        flags["full"] = True
    flags = finalize(flags)
    if forced:
        reason = "full: " + "; ".join(forced)
    elif flags["full"]:
        reason = "full: " + "; ".join(reasons[:5])
    elif not any(flags.values()):
        reason = "docs only: no suite needed"
    else:
        reason = "scoped: " + ",".join(f for f in FLAGS[1:] if flags[f])
        if reasons:
            reason += " (" + "; ".join(reasons[:5]) + ")"
    if summary["scope_base_run"]:
        reason += f" [since {scope_base[:12]}, green full run {summary['scope_base_run']}]"
    summary.update(flags, reason=reason, changed_files=len(paths), forced=forced, paths_sample=paths[:50])
    return summary


def required_jobs(outputs):
    """Jobs that must succeed for these classifier outputs (string 'true'/'false' values, as in needs.*.outputs)."""
    required = set(ALWAYS_JOBS)
    for job, flags in JOB_SCOPE.items():
        if any(outputs.get(flag) == "true" for flag in flags):
            required.add(job)
    return required


def verify_needs(needs):
    """Return a list of problems for the `needs` context of ci-result; empty means the CI status is PASS."""
    problems = []
    changes = needs.get("changes") or {}
    if changes.get("result") != "success":
        return [f"changes job result is {changes.get('result')!r}; scope unknown"]
    outputs = changes.get("outputs") or {}
    for flag in OUTPUT_FLAGS:
        if outputs.get(flag) not in ("true", "false"):
            problems.append(f"changes output {flag!r} missing or invalid: {outputs.get(flag)!r}")
    if not re.fullmatch(r"[0-9a-f]{40}", outputs.get("tree") or ""):
        problems.append(f"changes output 'tree' missing or invalid: {outputs.get('tree')!r}")
    # GitHub omits a job output whose value is empty, so an absent reuse_run means "no reuse" (run 39216183450).
    reuse = outputs.get("reuse_run") or ""
    if reuse and not reuse.isdigit():
        problems.append(f"changes output 'reuse_run' invalid: {reuse!r}")
    elif reuse and any(outputs.get(flag) == "true" for flag in FLAGS):
        problems.append("changes inherited a run but still scoped suites in")
    if problems:
        return problems
    required = required_jobs(outputs)
    for job in sorted(set(JOB_SCOPE) | set(ALWAYS_JOBS)):
        if job not in needs:
            problems.append(f"{job}: not listed in ci-result needs")
    for job, value in sorted(needs.items()):
        result = value.get("result")
        if job in required:
            if result != "success":
                problems.append(f"{job}: in scope, result {result}")
        elif result not in ("skipped", "success"):
            problems.append(f"{job}: out of scope, result {result}")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--event", default="")
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--labels", default="")
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--summary-file", type=Path)
    parser.add_argument("--verify-needs", action="store_true")
    parser.add_argument("--reuse-lookup", action="store_true", help="look up a full CI run on the identical tree "
                        "(GitHub API; token from $GH_TOKEN, repository from $GITHUB_REPOSITORY)")
    args = parser.parse_args(argv)
    if args.verify_needs:
        needs = json.loads(os.environ.get("NEEDS_JSON") or "{}")
        problems = verify_needs(needs)
        view = {job: value.get("result") for job, value in sorted(needs.items())}
        outputs = (needs.get("changes") or {}).get("outputs") or {}
        print(json.dumps({"results": view, "scope": outputs,
                          "status": "FAIL" if problems else "PASS", "problems": problems}, indent=2))
        if not problems and outputs.get("reuse_run") and os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as handle:
                handle.write(f"inherited full CI run {outputs['reuse_run']} on identical tree {outputs['tree']}\n")
        return 1 if problems else 0
    labels = {label.strip() for label in args.labels.split(",") if label.strip()}
    tree = tested_tree(args.repo)
    api = None
    if args.reuse_lookup:
        try:
            api = GitHubAPI(os.environ.get("GH_TOKEN", ""), os.environ.get("GITHUB_REPOSITORY", ""))
        except ValueError as exc:
            print(f"reuse lookup disabled: {exc}", file=sys.stderr)
    summary = decide(repo=args.repo, base=args.base.strip(), head=args.head.strip(), event=args.event.strip(),
                     base_ref=args.base_ref.strip(), labels=labels, tree=tree, reuse_api=api,
                     current_run_id=os.environ.get("GITHUB_RUN_ID"))
    lines = [f"{flag}={'true' if summary[flag] else 'false'}" for flag in OUTPUT_FLAGS]
    lines += [f"tree={summary['tree']}", f"reuse_run={summary['reuse_run']}", f"reason={summary['reason']}",
              f"changed_files={summary['changed_files']}"]
    text = "\n".join(lines) + "\n"
    sys.stdout.write(text)
    if args.github_output:
        with open(args.github_output, "a") as handle:
            handle.write(text)
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(rendered, file=sys.stderr)
    if args.summary_file:
        args.summary_file.write_text(rendered + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

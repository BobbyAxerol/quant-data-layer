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

Boundary: read-only on the repository (git diff / git grep only); stdlib only so it runs on the bare runner python;
it never selects tests inside a suite. The release receipt still requires a full run (scripts/p184_local_ci.py
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
from pathlib import Path

FLAGS = ("full", "python", "rust", "contracts")
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
    "contract-tests": ("contracts",),
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


def decide(*, repo, base, head, event, base_ref, labels):
    summary = {"event": event, "base": base, "head": head, "base_ref": base_ref, "labels": sorted(labels)}
    forced = []
    if event != "pull_request":
        forced.append(f"event {event or '<none>'} is not pull_request")
    if base_ref == "main":
        forced.append("pull request into main (release path)")
    if FULL_LABEL in labels:
        forced.append(f"label {FULL_LABEL}")
    paths = changed_paths(repo, base, head)
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
    summary.update(flags, reason=reason, changed_files=len(paths), forced=forced,
                   paths_sample=paths[:50])
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
    for flag in FLAGS:
        if outputs.get(flag) not in ("true", "false"):
            problems.append(f"changes output {flag!r} missing or invalid: {outputs.get(flag)!r}")
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
    args = parser.parse_args(argv)
    if args.verify_needs:
        needs = json.loads(os.environ.get("NEEDS_JSON") or "{}")
        problems = verify_needs(needs)
        view = {job: value.get("result") for job, value in sorted(needs.items())}
        print(json.dumps({"results": view, "scope": (needs.get("changes") or {}).get("outputs"),
                          "status": "FAIL" if problems else "PASS", "problems": problems}, indent=2))
        return 1 if problems else 0
    labels = {label.strip() for label in args.labels.split(",") if label.strip()}
    summary = decide(repo=args.repo, base=args.base.strip(), head=args.head.strip(), event=args.event.strip(),
                     base_ref=args.base_ref.strip(), labels=labels)
    lines = [f"{flag}={'true' if summary[flag] else 'false'}" for flag in FLAGS]
    lines += [f"reason={summary['reason']}", f"changed_files={summary['changed_files']}"]
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

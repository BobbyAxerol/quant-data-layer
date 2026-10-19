#!/usr/bin/env python3
"""Write a release-input receipt for one GitHub Actions run of `.github/workflows/ci.yml`.

Purpose: GitHub CI is the authoritative full run of the Data Layer, while a local replay (scripts/p184_local_ci.py)
is always INCOMPLETE on this host (its sudo step is skipped). This script turns one GitHub run into a receipt
(schema qdl.ci.github-run-receipt.v1) that scripts/p184_consolidated_build.py accepts as the DL CI gate when the
receipt shows a full, successful run of exactly the source commit.

Sources (all read-only GitHub REST calls; the token from ~/.config/data-layer/github/token.env is never printed):
* the run (`/actions/runs/{id}`): attempt, workflow path, event, head_sha, status, conclusion;
* the jobs of the latest attempt (`/actions/runs/{id}/jobs`, filter=latest): every job name and conclusion;
* the workflow file at head_sha (`/contents/.github/workflows/ci.yml?ref=head_sha`): the expected job names, matrix
  jobs named as GitHub names them (`unit-tests (1)`);
* the scope: the log of the `changes` job, i.e. the `full=` / `python=` / `rust=` / `contracts=` lines that
  scripts/ci_change_scope.py prints to stdout, and the `"event"` / `"base"` / `"head"` fields of its JSON summary.
  The step summary is not exposed by the REST API; the job outputs are not either. The log lines are corroborated
  by the job conclusions: in a full run every expected job ran and none was skipped.

`scope` is `full` only when the classifier said full=true (every flag true) AND every expected job is present with
conclusion success. The consolidated build re-derives this from the raw fields instead of trusting `scope`.

Boundary: read-only; no rerun, no comment, no status write. The receipt holds identifiers and conclusions only.

usage: ci_github_run_receipt.py --run-id N [--repo OWNER/NAME] --out FILE
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import itertools
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

SCHEMA = "qdl.ci.github-run-receipt.v1"
WORKFLOW = ".github/workflows/ci.yml"
TOKEN_FILE = Path.home() / ".config/data-layer/github/token.env"
FLAGS = ("full", "python", "rust", "contracts")
LOG_LINE = re.compile(r"^\S+Z (full|python|rust|contracts)=(true|false)\s*$", re.M)
LOG_FIELD = re.compile(r'^\S+Z\s+"(event|base|head|base_ref)": "([^"]*)",?\s*$', re.M)


def load_token(path=TOKEN_FILE):
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.removeprefix("export ").strip()] = value.strip().strip("'\"")
    return values


class _DropAuthOnRedirect(urllib.request.HTTPRedirectHandler):
    """Job logs redirect to a signed blob URL that rejects a forwarded Authorization header."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc:
            new.remove_header("Authorization")
        return new


class GitHub:
    def __init__(self, token, repo):
        self.token, self.repo = token, repo
        self.opener = urllib.request.build_opener(_DropAuthOnRedirect)

    def request(self, path, accept="application/vnd.github+json"):
        url = f"https://api.github.com/repos/{self.repo}{path}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}", "Accept": accept,
                                                   "X-GitHub-Api-Version": "2022-11-28"})
        with self.opener.open(req, timeout=60) as response:
            return response.read()

    def json(self, path):
        return json.loads(self.request(path))

    def run(self, run_id):
        return self.json(f"/actions/runs/{run_id}")

    def jobs(self, run_id):
        jobs, page = [], 1
        while True:
            data = self.json(f"/actions/runs/{run_id}/jobs?filter=latest&per_page=100&page={page}")
            jobs += data["jobs"]
            if len(jobs) >= data["total_count"] or not data["jobs"]:
                return jobs
            page += 1

    def workflow(self, sha, path=WORKFLOW):
        data = self.json(f"/contents/{path}?ref={sha}")
        return base64.b64decode(data["content"]).decode()

    def job_log(self, job_id):
        return self.request(f"/actions/jobs/{job_id}/logs").decode(errors="replace")


def expected_job_names(workflow_text):
    """Job names as GitHub shows them: `name:` or the id; a matrix job once per combination, `id (v1, v2)`."""
    import yaml  # host-side tool; the consolidated build itself stays stdlib-only
    jobs = yaml.safe_load(workflow_text)["jobs"]
    names = []
    for job_id, job in jobs.items():
        base = job.get("name") or job_id
        matrix = (job.get("strategy") or {}).get("matrix")
        if not matrix:
            names.append(base)
            continue
        if not isinstance(matrix, dict) or "include" in matrix or "exclude" in matrix:
            raise ValueError(f"unsupported matrix in {job_id}")
        for values in itertools.product(*matrix.values()):
            names.append(f"{base} ({', '.join(str(v) for v in values)})")
    return names


def parse_changes_log(text):
    """Classifier outputs and identity fields from the changes job log; a value must be consistent if repeated."""
    outputs, fields = {}, {}
    for target, pattern in ((outputs, LOG_LINE), (fields, LOG_FIELD)):
        for key, value in pattern.findall(text):
            if target.setdefault(key, value) != value:
                raise ValueError(f"changes log has conflicting {key} values")
    missing = [flag for flag in FLAGS if flag not in outputs]
    if missing:
        raise ValueError(f"changes log lacks classifier outputs {missing}")
    return outputs, fields


def build_receipt(run, jobs, workflow_text, changes_log, repo):
    expected = expected_job_names(workflow_text)
    outputs, fields = parse_changes_log(changes_log)
    job_rows = sorted(({"name": j["name"], "conclusion": j["conclusion"], "status": j["status"],
                        "started_at": j.get("started_at"), "completed_at": j.get("completed_at")} for j in jobs),
                      key=lambda row: row["name"])
    by_name = {row["name"]: row["conclusion"] for row in job_rows}
    all_success = bool(expected) and all(by_name.get(name) == "success" for name in expected)
    full = all(outputs[flag] == "true" for flag in FLAGS)
    return {
        "schema": SCHEMA,
        "repo": repo,
        "run_id": run["id"],
        "run_attempt": run["run_attempt"],
        "html_url": run["html_url"],
        "workflow_path": run["path"],
        "event": run["event"],
        "head_branch": run["head_branch"],
        "head_sha": run["head_sha"],
        "status": run["status"],
        "conclusion": run["conclusion"],
        "jobs": job_rows,
        "expected_jobs": expected,
        "changes_outputs": outputs,
        "changes_fields": fields,
        "scope": "full" if full and all_success else "partial",
        "scope_source": "changes job log (scripts/ci_change_scope.py stdout) + job conclusions",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def main(argv=None, client=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--repo", default="")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if client is None:
        env = load_token()
        repo = args.repo or env.get("GITHUB_REPO") or os.environ.get("GITHUB_REPO", "")
        client = GitHub(env.get("GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN", ""), repo)
    repo = args.repo or client.repo
    run = client.run(args.run_id)
    if run.get("status") != "completed":
        raise SystemExit(f"run {args.run_id} is {run.get('status')}, not completed")
    jobs = client.jobs(args.run_id)
    changes = [j for j in jobs if j["name"] == "changes"]
    if len(changes) != 1:
        raise SystemExit("run has no single `changes` job (workflow predates change scoping)")
    receipt = build_receipt(run, jobs, client.workflow(run["head_sha"]), client.job_log(changes[0]["id"]), repo)
    data = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    args.out.write_text(data)
    print(json.dumps({"receipt": str(args.out), "sha256": hashlib.sha256(data.encode()).hexdigest(),
                      "head_sha": receipt["head_sha"], "event": receipt["event"],
                      "conclusion": receipt["conclusion"], "scope": receipt["scope"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

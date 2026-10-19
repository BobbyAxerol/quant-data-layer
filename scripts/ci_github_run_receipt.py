#!/usr/bin/env python3
"""Write a release-input receipt for one GitHub Actions run of `.github/workflows/ci.yml`.

Purpose: GitHub CI is the authoritative full run of the Data Layer, while a local replay (scripts/p184_local_ci.py)
is always INCOMPLETE on this host (its sudo step is skipped). This script turns one GitHub run into a receipt
(schema qdl.ci.github-run-receipt.v1) that scripts/p184_consolidated_build.py accepts as the DL CI gate when it
shows a full green run on exactly the source tree (owner rule CI6, /home/bobby/CLAUDE.md section 7b).

Sources (read-only GitHub REST calls through scripts/ci_change_scope.GitHubAPI; the token from
~/.config/data-layer/github/token.env is never printed):
* the run (`/actions/runs/{id}`): attempt, workflow path, event, head_sha, status, conclusion;
* the jobs of the latest attempt: every job name and conclusion;
* the workflow file at head_sha: the expected job names, matrix jobs named as GitHub names them (`unit-tests (1)`);
* the `changes` job log: the stdout lines of scripts/ci_change_scope.py (`full=`, `python=`, `rust=`,
  `contracts=`, `base_check=`, `tree=`, `reuse_run=`) and the `"event"` / `"base"` / `"head"` fields of its JSON
  summary. The REST API exposes neither the step summary nor job outputs; the log is corroborated by the job
  conclusions.

Inheritance (CI1): when the run inherited a full run on the identical tree (`reuse_run=<id>`), the receipt records
`inherited_from_run` and embeds that original run's own receipt as `inherited_receipt` (one level only; an
inherited run is never an origin). `scope` is `full`, `inherited` or `partial`; the consolidated build re-derives it
from the raw fields instead of trusting it.

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
import sys
from pathlib import Path

try:  # imported as scripts.ci_github_run_receipt (tests) or run as scripts/ci_github_run_receipt.py
    from scripts import ci_change_scope as scope
except ImportError:
    import ci_change_scope as scope

SCHEMA = "qdl.ci.github-run-receipt.v1"
WORKFLOW = scope.WORKFLOW_PATH
TOKEN_FILE = Path.home() / ".config/data-layer/github/token.env"
FLAGS = scope.FLAGS
parse_changes_log = scope.parse_changes_log
_DropAuthOnRedirect = scope._DropAuthOnRedirect


def load_token(path=TOKEN_FILE):
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.removeprefix("export ").strip()] = value.strip().strip("'\"")
    return values


class GitHub(scope.GitHubAPI):
    def workflow(self, sha, path=WORKFLOW):
        data = self.json(f"/contents/{path}?ref={sha}")
        return base64.b64decode(data["content"]).decode()


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


def build_receipt(run, jobs, workflow_text, changes_log, repo):
    expected = expected_job_names(workflow_text)
    outputs, fields = parse_changes_log(changes_log)
    job_rows = sorted(({"name": j["name"], "conclusion": j["conclusion"], "status": j["status"],
                        "started_at": j.get("started_at"), "completed_at": j.get("completed_at")} for j in jobs),
                      key=lambda row: row["name"])
    by_name = {row["name"]: row["conclusion"] for row in job_rows}
    all_success = bool(expected) and all(by_name.get(name) == "success" for name in expected)
    if all(outputs[flag] == "true" for flag in FLAGS) and all_success and not outputs.get("reuse_run"):
        kind = "full"
    elif outputs.get("reuse_run") and by_name.get("ci-result") == "success":
        kind = "inherited"
    else:
        kind = "partial"
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
        "tested_tree": outputs.get("tree") or None,
        "scope": kind,
        "scope_source": "changes job log (scripts/ci_change_scope.py stdout) + job conclusions",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def receipt_for(client, run_id, repo, allow_inherited=True):
    run = client.run(run_id)
    if run.get("status") != "completed":
        raise SystemExit(f"run {run_id} is {run.get('status')}, not completed")
    jobs = client.jobs(run_id)
    changes = [j for j in jobs if j["name"] == "changes"]
    if len(changes) != 1:
        raise SystemExit(f"run {run_id} has no single `changes` job (workflow predates change scoping)")
    receipt = build_receipt(run, jobs, client.workflow(run["head_sha"]), client.job_log(changes[0]["id"]), repo)
    origin = receipt["changes_outputs"].get("reuse_run")
    if origin:
        if not allow_inherited:
            raise SystemExit(f"run {run_id} inherited run {origin}; an inherited run is never an origin")
        receipt["inherited_from_run"] = int(origin)
        receipt["inherited_receipt"] = receipt_for(client, int(origin), repo, allow_inherited=False)
    return receipt


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
    receipt = receipt_for(client, args.run_id, repo)
    data = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    args.out.write_text(data)
    print(json.dumps({"receipt": str(args.out), "sha256": hashlib.sha256(data.encode()).hexdigest(),
                      "head_sha": receipt["head_sha"], "event": receipt["event"],
                      "conclusion": receipt["conclusion"], "scope": receipt["scope"],
                      "inherited_from_run": receipt.get("inherited_from_run")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

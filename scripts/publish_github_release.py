#!/usr/bin/env python3
"""Create the GitHub release for an annotated tag that is already pushed.

This repository's release manifest *is* the annotated tag body - a CERTIFICATE
listing the running images with digests read from the live stack. So this tool
publishes what the tag already says rather than composing anything new: the
release title and notes come out of `git tag -l --format`, and a tag that does
not exist locally and on the remote is refused before any network call.

Why a script rather than `gh`. `gh` is not installed on this host and there is
no ambient GITHUB_TOKEN. The token lives in a 0600 file outside the repository,
on the same convention as ~/.config/trading-system/binance, and is read here,
used once and never printed - not in an error, not in a traceback, not in the
curl-equivalent request echoed on failure.

**It refuses to replace an existing release.** A published release cannot be
un-published cleanly; the standing rule in this workspace is to cut a new
version and record the correction rather than delete one. If the release is
already there, this stops and says so.

    python3 -B scripts/publish_github_release.py --tag v2.0.20
    python3 -B scripts/publish_github_release.py --tag v2.0.20 --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

TOKEN_FILE = Path(
    os.environ.get("QDL_GITHUB_TOKEN_FILE",
                   str(Path.home() / ".config/data-layer/github/token.env"))
)
API = "https://api.github.com"


def read_env_file(path: Path) -> dict[str, str]:
    """Parse KEY=VALUE lines. Values are never logged, only returned."""
    if not path.exists():
        raise SystemExit(f"token file not found: {path}\n"
                         f"Create it and put GITHUB_TOKEN=... in it.")
    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        raise SystemExit(f"{path} is mode {mode:o}; it holds a token and must "
                         f"be 600. Run: chmod 600 {path}")
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out


def git(*args: str) -> str:
    result = subprocess.run(("git", *args), capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def call(url: str, token: str, method: str = "GET", payload: dict | None = None):
    """One API call. On failure the body is shown, the token never is."""
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Accept", "application/vnd.github+json")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30,
                                    context=ssl.create_default_context()) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="show what would be published and make no write call")
    args = parser.parse_args()

    env = read_env_file(TOKEN_FILE)
    token = env.get("GITHUB_TOKEN", "")
    repo = env.get("GITHUB_REPO", "")
    if not token:
        raise SystemExit(f"GITHUB_TOKEN is empty in {TOKEN_FILE}")
    if not repo:
        raise SystemExit(f"GITHUB_REPO is empty in {TOKEN_FILE}")

    # The tag has to exist here and on the remote: a release pointing at a tag
    # GitHub cannot see is a broken release, and it is created silently.
    if not git("tag", "-l", args.tag):
        raise SystemExit(f"tag {args.tag} does not exist locally")
    if args.tag not in git("ls-remote", "--tags", "origin"):
        raise SystemExit(f"tag {args.tag} is not on origin yet; push it first")

    body = git("tag", "-l", args.tag, "--format=%(contents)")
    subject = body.splitlines()[0] if body else args.tag

    status, existing = call(f"{API}/repos/{repo}/releases/tags/{args.tag}", token)
    if status == 200:
        raise SystemExit(
            f"a release for {args.tag} already exists: {existing.get('html_url')}\n"
            f"This tool will not replace it. Cut a new version instead.")
    if status == 401:
        raise SystemExit("GitHub rejected the token (401). Check it has not "
                         "expired and carries Contents: Read and write.")
    if status not in (404,):
        raise SystemExit(f"unexpected status {status} checking for an existing "
                         f"release: {existing.get('message')}")

    payload = {"tag_name": args.tag, "name": subject,
               "body": body, "draft": False, "prerelease": False}
    print(f"repo    : {repo}")
    print(f"tag     : {args.tag}")
    print(f"title   : {subject}")
    print(f"notes   : {len(body.splitlines())} lines from the annotated tag")
    if args.dry_run:
        print("\ndry run: no write call made")
        return 0

    status, created = call(f"{API}/repos/{repo}/releases", token, "POST", payload)
    if status != 201:
        raise SystemExit(f"create failed with {status}: {created.get('message')}")
    print(f"\npublished: {created.get('html_url')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

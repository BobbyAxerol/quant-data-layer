"""Offline IDR039 recipe preparation. Never execute builds or activate runtime.

A passing DL CI gate needs the receipt file itself (`--dl-ci-receipt`), not only its digest; it must hash to
`--dl-ci-receipt-sha256`. Two receipt forms are accepted:
* scripts/p184_local_ci.py (qdl.p184.local-ci-receipt.v1): names the source commit, status PASS, scope `full`, every
  workflow job present. A scoped preflight (`scope: affected`, from `--changed-since` or `--jobs`) or a receipt
  without a scope is refused, so a quick affected-only run can never stand in for the release CI;
* scripts/ci_github_run_receipt.py (qdl.ci.github-run-receipt.v1), GitHub CI being the authoritative full run:
  head_sha = source commit, workflow .github/workflows/ci.yml, run completed / success, and the tested tree (the
  `changes` job's `tree=`; a pull_request run tests GitHub's merge ref) equals the commit's tree. Either the run is
  full itself (classifier full, every expected job success, none skipped), or it inherited (CI1) a full run on the
  identical tree: then `inherited_from_run` = its `reuse_run`, its own jobs did not fail, and the embedded
  `inherited_receipt` passes the full-run rules with the same tree. Legacy receipts without a tree fall back to the
  ancestor rule for pull_request runs. The scope is re-derived from these fields, not read from `scope`
  (owner rules CI1 / CI6, /home/bobby/CLAUDE.md section 7b).
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

LOCAL_CI_RECEIPT_SCHEMA = "qdl.p184.local-ci-receipt.v1"
GITHUB_RUN_RECEIPT_SCHEMA = "qdl.ci.github-run-receipt.v1"
CI_WORKFLOW = ".github/workflows/ci.yml"


PREREQUISITES = {
    "IDR040fix1": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "bindingV3": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "quota_log": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "b6a0119": "b6a01190587e8771c62b5b4988782b06c5e2578a",
    "IDR075": "704552d8736905cdb73feec86f3f01d339d02cbb",
    "DR05_book_fence": "d68fb2ff47622db8224dc4b9e004dca4e20f643e",
    "DR08_terminal_status": "0b374bc918af02476cdfb1502d5df132426f901b",
    "IDR084_hot_mark": "922a6ec38a8da94d30c634f025fd23c03838c24d",
}
CONTRACTS = {
    "rust/qdl-realtime-core/src/lib.rs": ["REST_AHEAD_OF_STREAM"],
    "rust/qdl-core/src/canonical.rs": [],
    "rust/qdl-kafka/src/bin/qdl-production-core.rs": [],
    "rust/qdl-projector/src/products.rs": [],
    "qdl/runtime/kn_hot_view.py": [],
    "qdl/runtime/kn_query_backend.py": ["binding.feed is FeedType.MARK_INDEX_PRICE"],
    "rust/qdl-stream-gateway/src/service.rs": ["terminal.get_or_insert(status)"],
    "rust/qdl-stream-gateway/tests/native_stream.rs": [
        "t05_prolonged_trade_stall_preserves_terminal_status_and_peer_progress",
        "t05_prolonged_book_delta_stall_preserves_terminal_status_and_peer_progress"],
    "tests/test_kn_hot_book_invalidation.py": [],
    "qdl/consumer/universal_release.py": ["qdl.v2.consumer-route-binding.v3"],
    "qdl/security/data_plane.py": ["qdl_consumer_quota_exhausted"],
    "rust/qdl-stream-gateway/src/subscription.rs": ["pub offered:", "pub dequeued:"],
    "scripts/prepare_paper_read_extension.py": ["def prepare_paper_extension("],
    "Dockerfile": ["ARG QDL_GIT_SHA=", "ARG QDL_RELEASE=", "COPY --chown=qdl:qdl . /app"],
    "Dockerfile.qdl-rust-runtime": [
        "ARG QDL_GIT_SHA=", "ARG QDL_RELEASE=", "COPY rust ./rust",
        "COPY generated/rust ./generated/rust", "cargo build --release --locked",
        "--bin qdl-projector", "--bin qdl-stream-gateway",
    ],
    ".dockerignore": [".git"],
    "scripts/verify_runtime_dependencies.py": ["dependency image must be digest-pinned"],
    "poetry.lock": [],
    "Cargo.lock": [],
    "rust-toolchain.toml": [],
    "generated/rust/qdl.query.v2.tonic.rs": [],
    "tests/test_paper_read_extension.py": [],
    "tests/test_phase115_universal_release.py": [],
    "tests/test_fund_phase71_beta_runtime.py": [],
}


def git(repo, *args):
    return subprocess.check_output(
        ["git", "-C", str(repo), *args], stderr=subprocess.PIPE
    ).decode().strip()


def full_sha(value, field, length=40):
    if not re.fullmatch(r"[0-9a-f]{%d}" % length, value or ""):
        raise ValueError(f"{field} must be a full lowercase {length}-hex digest")
    return value


def source_contract(repo, commit):
    full_sha(commit, "source commit")
    if git(repo, "rev-parse", commit + "^{commit}") != commit:
        raise ValueError("source must identify a commit, not a tag object")
    for name, ancestor in PREREQUISITES.items():
        try:
            git(repo, "merge-base", "--is-ancestor", ancestor, commit)
        except subprocess.CalledProcessError as exc:
            raise ValueError(f"missing prerequisite ancestry: {name}") from exc
    hashes = {}
    for path, markers in CONTRACTS.items():
        content = git(repo, "show", f"{commit}:{path}")
        if any(marker not in content for marker in markers):
            raise ValueError(f"source contract changed: {path}")
        hashes[path] = git(repo, "rev-parse", f"{commit}:{path}")
    return hashes


def _verify_full_github_run(receipt, label):
    """The run itself ran every suite green: classifier full, not inherited, every expected job success."""
    if receipt.get("schema") != GITHUB_RUN_RECEIPT_SCHEMA:
        raise ValueError(f"{label} schema must be {GITHUB_RUN_RECEIPT_SCHEMA}")
    if receipt.get("workflow_path") != CI_WORKFLOW:
        raise ValueError(f"{label} must be of {CI_WORKFLOW}")
    if (receipt.get("status"), receipt.get("conclusion")) != ("completed", "success"):
        raise ValueError(f"{label} conclusion must be success")
    outputs = receipt.get("changes_outputs") or {}
    if any(outputs.get(flag) != "true" for flag in ("full", "python", "rust", "contracts")):
        raise ValueError(f"{label} scope is not full: changes outputs {outputs}")
    if outputs.get("reuse_run"):
        raise ValueError(f"{label} itself inherited run {outputs['reuse_run']}")
    jobs = {row.get("name"): row.get("conclusion") for row in receipt.get("jobs") or []}
    expected = receipt.get("expected_jobs") or []
    if not {"changes", "ci-result"} <= set(expected):
        raise ValueError(f"{label} expected jobs must include changes and ci-result")
    missing = [name for name in expected if name not in jobs]
    if missing:
        raise ValueError(f"{label} does not cover every workflow job: missing {missing}")
    not_success = sorted(name for name, conclusion in jobs.items() if conclusion != "success")
    if not_success:
        raise ValueError(f"{label} has jobs not success (skipped or failed): {not_success}")


def verify_github_run_receipt(receipt, commit, repo):
    """Refuse a GitHub run receipt unless it is a full green run on exactly `commit`'s tree, directly or by
    inheriting (CI1) a full green run on the byte-identical tree (owner rule CI6)."""
    if receipt.get("head_sha") != commit:
        raise ValueError("DL CI GitHub run is for a different commit")
    if receipt.get("workflow_path") != CI_WORKFLOW:
        raise ValueError(f"DL CI GitHub run must be of {CI_WORKFLOW}")
    if (receipt.get("status"), receipt.get("conclusion")) != ("completed", "success"):
        raise ValueError("DL CI GitHub run conclusion must be success")
    outputs = receipt.get("changes_outputs") or {}
    fields = receipt.get("changes_fields") or {}
    if fields.get("head") != commit:
        raise ValueError("DL CI GitHub run changes job classified a different head")
    event = receipt.get("event")
    if fields.get("event") != event:
        raise ValueError("DL CI GitHub run event differs from its changes job")
    if event not in ("push", "workflow_dispatch", "pull_request"):
        raise ValueError(f"DL CI GitHub run event {event!r} not accepted")
    tested_tree = outputs.get("tree") or ""
    if tested_tree:
        # The tested checkout (a pull_request merge ref included) must hold exactly the commit's bytes.
        if git(repo, "rev-parse", commit + "^{tree}") != tested_tree:
            raise ValueError("DL CI GitHub run tested a tree that differs from the commit's tree")
    elif event == "pull_request":
        base = fields.get("base") or ""
        full_sha(base, "pull-request base of the DL CI GitHub run")
        try:
            git(repo, "merge-base", "--is-ancestor", base, commit)
        except subprocess.CalledProcessError as exc:
            raise ValueError("DL CI GitHub pull_request run tested a merge that differs from the commit "
                             "(base is not an ancestor); use a push or workflow_dispatch run") from exc
    origin_id = receipt.get("inherited_from_run")
    if not origin_id and not outputs.get("reuse_run"):
        _verify_full_github_run(receipt, "DL CI GitHub run")
        return receipt
    if str(origin_id) != str(outputs.get("reuse_run")):
        raise ValueError("DL CI GitHub run inheritance does not match its changes job (reuse_run)")
    if event == "workflow_dispatch" or not tested_tree:
        raise ValueError("DL CI GitHub inherited run needs a recorded tree and is never a workflow_dispatch run")
    jobs = {row.get("name"): row.get("conclusion") for row in receipt.get("jobs") or []}
    if jobs.get("changes") != "success" or jobs.get("ci-result") != "success":
        raise ValueError("DL CI GitHub inherited run needs changes and ci-result success")
    bad = sorted(name for name, conclusion in jobs.items() if conclusion not in ("success", "skipped"))
    if bad:
        raise ValueError(f"DL CI GitHub inherited run has failed or cancelled jobs: {bad}")
    origin = receipt.get("inherited_receipt") or {}
    if str(origin.get("run_id")) != str(origin_id):
        raise ValueError("DL CI GitHub inherited run does not embed the receipt of its original run")
    _verify_full_github_run(origin, f"DL CI GitHub original run {origin_id}")
    if (origin.get("changes_outputs") or {}).get("tree") != tested_tree:
        raise ValueError(f"DL CI GitHub original run {origin_id} tested a different tree")
    return receipt


def verify_dl_ci_receipt(path, commit, expected_sha256, repo=None):
    """Refuse a DL CI receipt that is not a full, passing run of exactly `commit` with the attested digest."""
    if path is None:
        raise ValueError("DL CI pass requires the receipt file (--dl-ci-receipt), not only its digest")
    data = Path(path).read_bytes()
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ValueError("DL CI receipt file does not match --dl-ci-receipt-sha256")
    try:
        receipt = json.loads(data)
    except ValueError as exc:
        raise ValueError("DL CI receipt is not JSON") from exc
    if receipt.get("schema") == GITHUB_RUN_RECEIPT_SCHEMA:
        return verify_github_run_receipt(receipt, commit, repo)
    if receipt.get("schema") != LOCAL_CI_RECEIPT_SCHEMA:
        raise ValueError(f"DL CI receipt schema must be {LOCAL_CI_RECEIPT_SCHEMA} or {GITHUB_RUN_RECEIPT_SCHEMA}")
    if receipt.get("sha") != commit:
        raise ValueError("DL CI receipt is for a different commit")
    if receipt.get("scope") != "full":
        raise ValueError(f"DL CI receipt scope is {receipt.get('scope')!r}; a release needs scope 'full'")
    if receipt.get("status") != "PASS":
        raise ValueError(f"DL CI receipt status is {receipt.get('status')!r}; a release needs PASS")
    expected_jobs = receipt.get("workflow_jobs") or []
    if not expected_jobs or set(receipt.get("jobs") or {}) != set(expected_jobs):
        raise ValueError("DL CI receipt does not cover every workflow job")
    return receipt


def prepare(repo, commit, release, *, ts_status="pending", ts_commit=None,
            ts_receipt=None, dl_ci_status="pending", dl_ci_receipt=None,
            build_approval="pending", dependency_image="builder", dl_ci_receipt_file=None):
    full_sha(commit, "source commit")
    if not re.fullmatch(r"v2\.\d+\.\d+(?:-rc\.\d+)?", release):
        raise ValueError("release must be v2.N.N or v2.N.N-rc.N")
    if ts_status not in {"pending", "pass"} or dl_ci_status not in {"pending", "pass"}:
        raise ValueError("test statuses must be pending or pass")
    if build_approval not in {"pending", "approved"}:
        raise ValueError("build approval must be pending or approved")
    if ts_status == "pass":
        full_sha(ts_commit, "TS tested source commit")
        full_sha(ts_receipt, "TS completed-test receipt SHA256", 64)
    if dl_ci_status == "pass":
        full_sha(dl_ci_receipt, "DL CI receipt SHA256 for source commit", 64)
        verify_dl_ci_receipt(dl_ci_receipt_file, commit, dl_ci_receipt, repo)
    if dependency_image != "builder" and not re.fullmatch(
        r"[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}", dependency_image
    ):
        raise ValueError("dependency image must be builder or a digest-pinned image")
    blobs = source_contract(repo, commit)
    holds = []
    if ts_status != "pass":
        holds.append("TS market-data integration regressions must pass; Paper/OR feature completion is not required")
    if dl_ci_status != "pass":
        holds.append("DL CI must pass for the exact source commit")
    if build_approval != "approved":
        holds.append("explicit build approval required")
    images = [
        {"name": "qdl-v2-python", "dockerfile": "Dockerfile"},
        {"name": "qdl-v2-rust", "dockerfile": "Dockerfile.qdl-rust-runtime"},
    ]
    for image in images:
        image["tag"] = f"{image['name']}:{release}-{commit}"
        image["source_commit"] = commit
    script = None
    if not holds:
        # A private archive context excludes all staged/untracked source overlays.
        lines = [
            "set -eu",
            'build_dir=$(mktemp -d)',
            "trap 'rm -rf -- \"$build_dir\"' EXIT",
            'mkdir "$build_dir/source"',
            f"git -C {shlex.quote(str(Path(repo).resolve()))} archive --format=tar {commit} > \"$build_dir/source.tar\"",
            'sha256sum "$build_dir/source.tar"',
            'tar -xf "$build_dir/source.tar" -C "$build_dir/source"',
        ]
        for image in images:
            iid = f"$build_dir/{image['name']}.iid"
            dependency_arg = (
                f"--build-arg QDL_DEPENDENCY_IMAGE={dependency_image} "
                if image["dockerfile"] == "Dockerfile" else ""
            )
            lines.extend([
                f"docker build --file \"$build_dir/source/{image['dockerfile']}\" "
                f"--build-arg QDL_GIT_SHA={commit} --build-arg QDL_RELEASE={release} "
                f"{dependency_arg}--iidfile \"{iid}\" --tag {image['tag']} \"$build_dir/source\"",
                f'test "$(docker image inspect --format \'{{{{index .Config.Labels "org.opencontainers.image.revision"}}}}\' "$(cat "{iid}")")" = {commit}',
                f'docker image inspect --format \'{{{{.Id}}}} {{{{json .Config.Labels}}}}\' "$(cat "{iid}")"',
            ])
        script = "\n".join(lines) + "\n"
    return {
        "schema": "qdl.p184.consolidated-build-recipe.v1",
        "status": "HOLD" if holds else "PREPARED_NOT_EXECUTED",
        "source_commit": commit, "release": release,
        "dependency_image": dependency_image,
        "prerequisites": PREREQUISITES, "source_git_blobs": blobs,
        "gates": {"ts_tests": ts_status, "ts_tests_scope": "MARKET_DATA_INTEGRATION_ONLY", "ts_source_commit": ts_commit,
                  "ts_receipt_sha256": ts_receipt, "dl_ci": dl_ci_status,
                  "dl_ci_source_commit": commit, "dl_ci_receipt_sha256": dl_ci_receipt,
                  "build_approval": build_approval},
        "holds": holds, "images": images, "build_script": script,
        "build_script_sha256": hashlib.sha256(script.encode()).hexdigest() if script else None,
        "limitations": [
            "TS gate inputs are operator attestations; the DL CI receipt (local replay or GitHub run) is checked "
            "for commit, success and full scope, not re-executed.",
            "Ancestry and source markers do not replace semantic regression tests.",
            "One application commit does not make upstream package resolution reproducible.",
            "No image built, pushed, deployed or certified by this generator.",
            "Final DR/broker delta and consumer capacity acceptance remain required after packaging; no dependency on Paper/OR feature completion.",
            "IDR075 remains prepared, not activated; IDR040fix2 remains deferred.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--ts-tests-status", choices=["pending", "pass"], default="pending")
    parser.add_argument("--ts-source-commit")
    parser.add_argument("--ts-tests-receipt-sha256")
    parser.add_argument("--dl-ci-status", choices=["pending", "pass"], default="pending")
    parser.add_argument("--dl-ci-receipt-sha256")
    parser.add_argument("--dl-ci-receipt", type=Path, help="summary.json of scripts/p184_local_ci.py (scope full) or the "
                        "receipt of scripts/ci_github_run_receipt.py")
    parser.add_argument("--build-approval", choices=["pending", "approved"], default="pending")
    parser.add_argument("--dependency-image", default="builder")
    args = parser.parse_args()
    try:
        recipe = prepare(
            args.repo, args.source_commit, args.release,
            ts_status=args.ts_tests_status, ts_commit=args.ts_source_commit,
            ts_receipt=args.ts_tests_receipt_sha256, dl_ci_status=args.dl_ci_status,
            dl_ci_receipt=args.dl_ci_receipt_sha256, build_approval=args.build_approval,
            dependency_image=args.dependency_image, dl_ci_receipt_file=args.dl_ci_receipt,
        )
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        parser.error(str(exc))
    print(json.dumps(recipe, indent=2, sort_keys=True))
    return 2 if recipe["holds"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

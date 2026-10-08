"""Offline IDR039 recipe preparation. Never execute builds or activate runtime."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess


PREREQUISITES = {
    "IDR040fix1": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "bindingV3": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "quota_log": "4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf",
    "b6a0119": "b6a01190587e8771c62b5b4988782b06c5e2578a",
    "IDR075": "704552d8736905cdb73feec86f3f01d339d02cbb",
    "DR05_book_fence": "d68fb2ff47622db8224dc4b9e004dca4e20f643e",
    "DR08_terminal_status": "0b374bc918af02476cdfb1502d5df132426f901b",
}
CONTRACTS = {
    "rust/qdl-realtime-core/src/lib.rs": ["REST_AHEAD_OF_STREAM"],
    "rust/qdl-core/src/canonical.rs": [],
    "rust/qdl-kafka/src/bin/qdl-production-core.rs": [],
    "rust/qdl-projector/src/products.rs": [],
    "qdl/runtime/kn_hot_view.py": [],
    "qdl/runtime/kn_query_backend.py": [],
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


def prepare(repo, commit, release, *, ts_status="pending", ts_commit=None,
            ts_receipt=None, dl_ci_status="pending", dl_ci_receipt=None,
            build_approval="pending", dependency_image="builder"):
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
            "Gate inputs are operator attestations, not independently verified CI results.",
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
    parser.add_argument("--build-approval", choices=["pending", "approved"], default="pending")
    parser.add_argument("--dependency-image", default="builder")
    args = parser.parse_args()
    try:
        recipe = prepare(
            args.repo, args.source_commit, args.release,
            ts_status=args.ts_tests_status, ts_commit=args.ts_source_commit,
            ts_receipt=args.ts_tests_receipt_sha256, dl_ci_status=args.dl_ci_status,
            dl_ci_receipt=args.dl_ci_receipt_sha256, build_approval=args.build_approval,
            dependency_image=args.dependency_image,
        )
    except (ValueError, subprocess.CalledProcessError) as exc:
        parser.error(str(exc))
    print(json.dumps(recipe, indent=2, sort_keys=True))
    return 2 if recipe["holds"] else 0


if __name__ == "__main__":
    raise SystemExit(main())

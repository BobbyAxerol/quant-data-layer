from __future__ import annotations

import argparse
from pathlib import Path

from qdl.certification import verify_release_bundle, write_release_bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--image-ref", required=True)
    parser.add_argument("--authority", choices=("SHADOW", "CANARY", "PRIMARY"), default="SHADOW")
    parser.add_argument("--signing-key", type=Path)
    parser.add_argument("--verification-key", type=Path)
    args = parser.parse_args()
    write_release_bundle(
        args.repo.resolve(), args.output_dir.resolve(),
        release=args.release, git_sha=args.git_sha, image_ref=args.image_ref,
        authority=args.authority, signing_key=args.signing_key,
    )
    verify_release_bundle(
        args.repo.resolve(), args.output_dir.resolve(),
        verification_key=args.verification_key,
    )
    print(f"PASS release_bundle={args.output_dir.resolve()}")


if __name__ == "__main__":
    main()

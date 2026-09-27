#!/usr/bin/env python3
"""Verify a digest-pinned dependency image before reusing its installed venv."""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path
import re
import tomllib

from packaging.markers import Marker


def verify(lock, image, distribution=metadata.distribution):
    if image != "builder" and not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("dependency image must be digest-pinned")
    packages, digest, count = [], hashlib.sha256(), 0
    for spec in sorted(lock["package"], key=lambda p: (p["name"], p["version"])):
        if "main" not in spec.get("groups", ()):
            continue
        marker = spec.get("markers")
        if marker and not Marker(marker).evaluate():
            continue
        dist = distribution(spec["name"])
        if dist.version != spec["version"]:
            raise ValueError(f"locked version mismatch: {spec['name']}")
        files = dist.files
        if not files:
            raise ValueError(f"missing RECORD: {spec['name']}")
        verified = 0
        for item in sorted(files, key=str):
            if not item.hash:
                continue  # Installed bytecode and RECORD itself have no wheel hash.
            actual = hashlib.new(item.hash.mode)
            with Path(dist.locate_file(item)).open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    actual.update(chunk)
            encoded = base64.urlsafe_b64encode(actual.digest()).rstrip(b"=").decode()
            if encoded != item.hash.value:
                raise ValueError(f"installed file hash mismatch: {spec['name']}/{item}")
            digest.update(json.dumps([spec["name"], str(item), encoded], separators=(",", ":")).encode())
            verified += 1
        if not verified:
            raise ValueError(f"empty RECORD hashes: {spec['name']}")
        count += verified
        packages.append({"name": spec["name"], "version": dist.version, "verified_files": verified})
    if not packages:
        raise ValueError("empty main dependency lock")
    return {"schema": "qdl.dependency-image.v1", "status": "PASS", "image": image,
            "packages": packages, "verified_files": count, "installed_files_sha256": digest.hexdigest(),
            "scope": "main lock versions and installed RECORD hashes; not original wheel reconstruction"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.lock.read_bytes()
    result = verify(tomllib.loads(raw.decode()), args.image)
    result["lock_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "packages"}))


if __name__ == "__main__":
    main()

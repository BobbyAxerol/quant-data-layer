#!/usr/bin/env python3
"""Build the Data Layer v2.2.4 release images reproducibly (dry-run by default).

Purpose
    One recipe for the two v2.2.4 release images, built from ``git archive <REV>``:

    * ``qdl-v2-rust:2.2.4-<sha7>``: the 12 Dockerfile.qdl-rust-runtime binaries compiled at
      <REV> in the pinned builder image 551428d9 (offline cargo cache, ``--network none``)
      and copied onto the pinned rc16 runtime base f41ecb66. This is the owner-approved thin
      method of build-rc22-a4e7b3b/build.sh (no BuildKit Rust compile on the runtime disk).
    * ``qdl-v2-python:2.2.4-<sha7>``: recipe build of the repository ``Dockerfile`` at <REV>
      (python base pinned by digest inside the Dockerfile). This step needs registry and
      package network access; everything else runs with ``--network none``.

    Post-checks write receipts to ``~/.local/state/qdl-v2/build-v2.2.4-<sha7>/``: in-image
    sha256 of every Rust binary against the build context and against the images that run
    in production today (expected EQUAL/DIFFERENT per binary, see BINARY_EXPECTATIONS),
    ldd/uid/env probes, a feature-only TEST clock marker probe, and the sha256 of every
    python file under /app against the rc19 Query image with the expected-different set
    derived from git.

Boundary
    Dry-run (default) only reads git and prints the plan; it runs no docker command.
    ``--build`` builds images and runs ``--rm`` probe containers. It never creates,
    recreates or stops a runtime container, never touches Kafka/Redis/Postgres, never
    pushes an image or a git ref, and never moves an existing tag. ``--build`` requires
    the host measurement lock to be held by ``--lock-owner`` (AGENTS 34a).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.host_measurement_lock import MAX_MINUTES as MAX_LOCK_MINUTES, lock_state  # noqa: E402


RELEASE = "2.2.4"
SDK_VERSION = "2.4.1"
REPO = Path(__file__).resolve().parents[1]
HOME = Path.home()
STATE_ROOT = HOME / ".local/state/qdl-v2"
LOCK_PATH = HOME / ".local/state/host-measurement-window.lock"
CARGO_WORK = HOME / ".local/state/trading-system/p184-n1-final-20261012"
MIN_FREE_GIB = 40

BUILDER_IMAGE = "sha256:551428d98db6b6523b3515fe040d7f9ed53abf625a78e0a1413f4e2056883817"
RUST_BASE_TAG = "qdl-v2-rust:2.2.3-rc16-bcb0283"
RUST_BASE_ID = "sha256:f41ecb66ecb0dca109300a69cf3f8cd46ce12c4c54ba1338391fc069b5e682a2"
PYTHON_BASE = (
    "python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a"
)
BUILD_METHOD = "cargo-cache-551428d9+runtime-overlay-f41ecb66"

# Commits the release must contain: the stream gateway Codex SR1/SR4 change and the Query G5 change.
REQUIRED_ANCESTORS = {
    "stream_gateway_sr1_sr4": "c934c1e72a46e75123d770dea07441b01cfeb111",
    "query_g5_rc19": "4d3476c5b6944eba557d00ee71f351f150a5b752",
}

# Identical to the four cargo invocations of Dockerfile.qdl-rust-runtime, offline.
CARGO_COMMANDS = (
    "cargo build --offline --locked --release -p qdl-kafka --bin qdl-realtime-core "
    "--bin qdl-native-raw-ingestor --bin qdl-production-core --bin qdl-kafka-smoke "
    "--bin qdl-authority-rehearsal --bin qdl-phase91-canary-rehearsal "
    "--bin qdl-phase92-primary-rehearsal",
    "cargo build --offline --locked --release -p qdl-core --bin qdl-parity-replay --bin qdl-binance-shadow",
    "cargo build --offline --locked --release -p qdl-venue-core --bin qdl-venue-core-certify",
    "cargo build --offline --locked --release -p qdl-projector --bin qdl-projector "
    "-p qdl-stream-gateway --bin qdl-stream-gateway",
)
RUST_BINARIES = (
    "qdl-authority-rehearsal", "qdl-binance-shadow", "qdl-kafka-smoke", "qdl-native-raw-ingestor",
    "qdl-parity-replay", "qdl-phase91-canary-rehearsal", "qdl-phase92-primary-rehearsal",
    "qdl-production-core", "qdl-projector", "qdl-realtime-core", "qdl-stream-gateway",
    "qdl-venue-core-certify",
)
KEY_BINARIES = ("qdl-realtime-core", "qdl-stream-gateway", "qdl-native-raw-ingestor", "qdl-projector")
ADMISSION_POLICY = "config/v2/provider-admission-policy-v1.json"

# Reference images (production today). Ids are asserted before they are read.
REFERENCES = {
    "rc22_core": ("qdl-v2-rust:2.2.3-rc22-a4e7b3b",
                  "sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb"),
    "stream_baa72c96": ("local/qdl-stream:isolation-sr1-c934c1e",
                        "sha256:baa72c96d5255cbfc0e332f893c470ff293d88fde4e20ea4c7c94ba5ea09a38b"),
    "rc18_ingestor": ("qdl-v2-rust:2.2.3-rc18-cdbf28b",
                      "sha256:7fba25779f72a3adffc88f195935a316ac94acbfcb362996fafbff89f62c9103"),
    "rc14_projector": ("qdl-v2-rust:2.2.3-rc14-a388537",
                       "sha256:ce629061a8c180ae7e6cf88e32ebaddcf965ffe3924c88a3f1b942fc1623d4f9"),
}
PYTHON_REFERENCE = ("qdl-v2-python:2.2.3-rc19-4d3476c",
                    "sha256:b9ca6c36fb0ad003b77e47bb3a777c3e565944d59cb3a2e9bc5dfb68e3448481")
# rc19 is a thin overlay: /app/qdl at 4d3476c, every other /app path at bcb0283 (build-query-rc19-4d3476c).
PYTHON_REFERENCE_TREE = (("qdl/", "4d3476c5b6944eba557d00ee71f351f150a5b752"),
                         ("", "bcb02833f4193ed116bde3f730c8299b34a0e87f"))
PYTHON_HASH_ROOTS = ("qdl", "app", "generated", "config", "consumers", "qdl_sdk")

# Per binary: (reference, expectation, reason). The production reference of each role is listed
# first. A result other than the expectation is UNEXPECTED and fails the build verdict.
_RC22_SAME = "no source delta since a4e7b3b in its crates, Cargo.lock or generated/rust"
BINARY_EXPECTATIONS = {
    "qdl-realtime-core": [("rc22_core", "EQUAL", "rust_core x3 run rc22; " + _RC22_SAME +
                           "; a difference means the release image is not the measured core: stop")],
    "qdl-stream-gateway": [
        ("stream_baa72c96", "EQUAL", "stream_kn_1/2 run the c934c1e gateway; c934c1e..REV touches "
         "only a cfg(test) hunk of qdl-projector"),
        ("rc22_core", "DIFFERENT", "rc22 gateway is a4e7b3b, before e3b5552/c934c1e"),
    ],
    "qdl-native-raw-ingestor": [
        ("rc18_ingestor", "DIFFERENT", "ingestors run rc18 cdbf28b; qdl-kafka DL-R1/RB1 changed after it"),
        ("rc22_core", "EQUAL", _RC22_SAME),
    ],
    "qdl-projector": [
        ("rc14_projector", "DIFFERENT", "projectors run rc14 a388537 without IDR-107/CF3/CF4"),
        ("rc22_core", "EQUAL", "only the cfg(test) hunk of products.rs changed since a4e7b3b "
         "(assumption: a test-module edit leaves the release binary unchanged; a DIFFERENT is recorded)"),
    ],
}
for _name in RUST_BINARIES:
    BINARY_EXPECTATIONS.setdefault(_name, [("rc22_core", "EQUAL", _RC22_SAME)])

# Byte markers that only the isolated-replay-clock feature compiles in (qdl-test-data-clock replay
# module). The env names QDL_TEST_DATA_CLOCK_* are present in every build (the guard that refuses
# them), so they are not a feature marker.
FORBIDDEN_FEATURE_MARKERS = (b"p184.test-data-clock.v1", b"/work/TEST_ONLY")


class BuildError(RuntimeError):
    pass


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE).decode().strip()


def resolve_rev(repo: Path, rev: str) -> str:
    try:
        full = git(repo, "rev-parse", "--verify", rev + "^{commit}")
    except subprocess.CalledProcessError as exc:
        raise BuildError(f"revision {rev!r} is not a commit") from exc
    if not re.fullmatch(r"[0-9a-f]{40}", full):
        raise BuildError("revision did not resolve to a full sha")
    return full


def identity(rev: str, state_root: Path = STATE_ROOT) -> dict:
    sha7 = rev[:7]
    return {
        "rev": rev,
        "sha7": sha7,
        "rust_tag": f"qdl-v2-rust:{RELEASE}-{sha7}",
        "python_tag": f"qdl-v2-python:{RELEASE}-{sha7}",
        "build_dir": str(state_root / f"build-v{RELEASE}-{sha7}"),
    }


def check_lock(lock_path: Path, owner: str | None, at: dt.datetime | None = None) -> dict:
    """--build needs the measurement lock held by the named owner AND inside its window (the build is its own
    window). Expiry follows ``scripts/host_measurement_lock.lock_state``: an absent, malformed, naive or past
    ``until_utc`` is refused, as is a window longer than the 34a maximum (Codex B1). Called again at every expensive
    stage boundary (``build``), so a window that ends mid-build stops the next stage; renewing is the owner's
    ``host_measurement_lock.py`` release + acquire, never done here."""
    if not owner:
        raise BuildError("--build requires --lock-owner (take the host measurement lock first)")
    at = at or dt.datetime.now(dt.timezone.utc)
    state, lock = lock_state(lock_path, at)
    if state == "ABSENT":
        raise BuildError(f"no host lock at {lock_path}: take it for the build first")
    if not isinstance(lock, dict):
        raise BuildError(f"unreadable host lock {lock_path}")
    if lock.get("owner") != owner:
        raise BuildError(f"host lock held by {lock.get('owner')!r}, not {owner!r}: do not build")
    if state == "UNREADABLE":
        raise BuildError(f"host lock {lock_path} has no valid timezone-aware until_utc: do not build")
    if state == "EXPIRED":
        raise BuildError(f"host lock of {owner!r} expired at {lock.get('until_utc')}: re-acquire before building")
    until = dt.datetime.fromisoformat(lock["until_utc"].replace("Z", "+00:00"))
    if until - at > dt.timedelta(minutes=MAX_LOCK_MINUTES):
        raise BuildError(f"host lock window ends {lock['until_utc']}, beyond the {MAX_LOCK_MINUTES} min maximum")
    return lock


def check_disk(path: Path, min_gib: int) -> float:
    free = shutil.disk_usage(path).free / 2**30
    if free < min_gib:
        raise BuildError(f"{free:.1f} GiB free on the runtime disk < {min_gib} GiB")
    return round(free, 1)


def source_preconditions(repo: Path, rev: str) -> dict:
    """Read-only checks of the release source at REV (git show only)."""
    def show(path: str) -> str:
        return git(repo, "show", f"{rev}:{path}")

    problems = []
    if f'__version__ = "{SDK_VERSION}"' not in show("qdl_sdk/__init__.py"):
        problems.append(f"qdl_sdk/__init__.py at REV is not {SDK_VERSION} (apply the SDK bump first)")
    if f'VERSION = "{SDK_VERSION}"' not in show("scripts/build_qdl_sdk_release.py"):
        problems.append(f"scripts/build_qdl_sdk_release.py VERSION at REV is not {SDK_VERSION}")
    dockerfile = show("Dockerfile")
    if dockerfile.count(f"FROM {PYTHON_BASE}") != 2:
        problems.append("Dockerfile python base is not the pinned digest in both stages")
    copied = sorted(re.findall(r"/src/target/release/(\S+) /usr/local/bin/", show("Dockerfile.qdl-rust-runtime")))
    if tuple(copied) != tuple(sorted(RUST_BINARIES)):
        problems.append(f"Dockerfile.qdl-rust-runtime binary set changed: {copied}")
    gateway = show("rust/qdl-stream-gateway/Cargo.toml")
    if not re.search(r"^default = \[\]$", gateway, re.M):
        problems.append("qdl-stream-gateway default features are not empty")
    for name, commit in REQUIRED_ANCESTORS.items():
        if subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, rev]).returncode:
            problems.append(f"{name} {commit[:7]} is not an ancestor of REV")
    return {"ok": not problems, "problems": problems}


def expected_python_diff(repo: Path, rev: str) -> list[str]:
    """Paths under PYTHON_HASH_ROOTS whose bytes differ between the rc19 tree model and REV."""
    changed = set()
    for prefix, base in PYTHON_REFERENCE_TREE:
        names = git(repo, "diff", "--name-only", "--no-renames", base, rev, "--", *PYTHON_HASH_ROOTS)
        for name in filter(None, names.splitlines()):
            owner = next(p for p, _ in PYTHON_REFERENCE_TREE if name.startswith(p))
            if owner == prefix:
                changed.add(name)
    return sorted(changed)


def rust_compile_argv(build_dir: Path, cargo_work: Path = CARGO_WORK) -> list[str]:
    return [
        "docker", "run", "--rm", "--pull=never", "--network", "none", "--name", f"qdl-release-{build_dir.name}",
        "--cpus", "2", "--memory", "3g", "--pids-limit", "256", "--user", "1001:1001",
        "-e", "HOME=/work", "-e", "CARGO_HOME=/work/cargo", "-e", "CARGO_TARGET_DIR=/work/dl-target",
        "-e", "CARGO_BUILD_JOBS=2", "-v", f"{cargo_work}:/work", "-v", f"{build_dir / 'src'}:/src:ro",
        "-w", "/src", BUILDER_IMAGE, "sh", "-c", "set -eu\n" + "\n".join(CARGO_COMMANDS),
    ]


def rust_dockerfile(rev: str) -> str:
    return (
        f"FROM {RUST_BASE_TAG}\n"
        "COPY --chmod=755 bin/ /usr/local/bin/\n"
        f"COPY {ADMISSION_POLICY} /opt/qdl/{ADMISSION_POLICY}\n"
        f'LABEL org.opencontainers.image.revision="{rev}" org.opencontainers.image.version="{RELEASE}" '
        'org.opencontainers.image.title="Quant Data Layer Rust runtime" '
        f'qdl.build.method="{BUILD_METHOD}"\n'
        "USER 10001:10001\n"
        'ENTRYPOINT ["/usr/local/bin/qdl-kafka-smoke"]\n'
    )


def rust_image_argv(build_dir: Path, tag: str) -> list[str]:
    return ["docker", "build", "--pull=false", "--network", "none", "--iidfile", str(build_dir / "rust.iid"),
            "-t", tag, str(build_dir / "ctx")]


def python_image_argv(build_dir: Path, rev: str, tag: str) -> list[str]:
    return ["docker", "build", "-f", str(build_dir / "src/Dockerfile"), "--build-arg", f"QDL_GIT_SHA={rev}",
            "--build-arg", f"QDL_RELEASE={RELEASE}", "--iidfile", str(build_dir / "python.iid"),
            "-t", tag, str(build_dir / "src")]


def probe_argv(image: str, script: str) -> list[str]:
    return ["docker", "run", "--rm", "--pull=never", "--network", "none", "--read-only",
            "--entrypoint", "sh", image, "-c", script]


RUST_SHA_PROBE = "cd /usr/local/bin && sha256sum *"
RUST_RUNTIME_PROBE = (
    "for b in qdl-realtime-core qdl-projector qdl-stream-gateway qdl-native-raw-ingestor; do "
    'printf "ldd_not_found %s " "$b"; ldd /usr/local/bin/$b | grep -c "not found" || true; done; '
    'printf "env_test_clock "; env | grep -c QDL_TEST_DATA_CLOCK || true; printf "uid "; id -u'
)
PYTHON_FILE_PROBE = (
    "cd /app && find " + " ".join(PYTHON_HASH_ROOTS) +
    " -type f ! -name '*.pyc' ! -path '*/__pycache__/*' -print0 2>/dev/null | sort -z | xargs -0 sha256sum"
)
PIP_FREEZE_PROBE = "PIP_DISABLE_PIP_VERSION_CHECK=1 /opt/venv/bin/python -m pip freeze --all"


def parse_sha256sum(text: str) -> dict[str, str]:
    out = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        digest, _, name = line.partition("  ")
        if not re.fullmatch(r"[0-9a-f]{64}", digest) or not name:
            raise BuildError(f"bad sha256sum line: {line!r}")
        out[name.lstrip("*").removeprefix("./")] = digest
    return out


def parse_runtime_probe(text: str) -> dict:
    ldd, env_hits, uid = {}, None, None
    for line in text.splitlines():
        parts = line.split()
        if parts[:1] == ["ldd_not_found"] and len(parts) == 3:
            ldd[parts[1]] = int(parts[2])
        elif parts[:1] == ["env_test_clock"] and len(parts) == 2:
            env_hits = int(parts[1])
        elif parts[:1] == ["uid"] and len(parts) == 2:
            uid = parts[1]
    ok = len(ldd) == 4 and not any(ldd.values()) and env_hits == 0 and uid == "10001"
    return {"ldd_not_found": ldd, "env_test_clock": env_hits, "uid": uid, "ok": ok}


def forbidden_markers(path: Path) -> list[str]:
    data = path.read_bytes()
    return [m.decode() for m in FORBIDDEN_FEATURE_MARKERS if m in data]


def binary_verdicts(release: dict[str, str], references: dict[str, dict[str, str]]) -> dict:
    rows, unexpected = [], []
    missing = sorted(set(RUST_BINARIES) - set(release))
    for name in RUST_BINARIES:
        for ref, expected, reason in BINARY_EXPECTATIONS[name]:
            ref_sha = references.get(ref, {}).get(name)
            got = release.get(name)
            if got is None or ref_sha is None:
                observed = "NOT_READ"
            else:
                observed = "EQUAL" if got == ref_sha else "DIFFERENT"
            row = {"binary": name, "reference": ref, "release_sha256": got, "reference_sha256": ref_sha,
                   "expected": expected, "observed": observed, "reason": reason}
            rows.append(row)
            if observed != expected:
                unexpected.append(row)
    core_ok = all(r["observed"] == "EQUAL" for r in rows
                  if r["binary"] == "qdl-realtime-core" and r["reference"] == "rc22_core")
    return {"rows": rows, "unexpected": unexpected, "missing": missing,
            "core_equal_to_measured": core_ok, "ok": not unexpected and not missing}


def python_file_verdict(release: dict[str, str], reference: dict[str, str], expected_diff: list[str]) -> dict:
    observed = sorted(name for name in set(release) | set(reference) if release.get(name) != reference.get(name))
    expected = set(expected_diff)
    return {
        "files_release": len(release),
        "files_reference": len(reference),
        "observed_different": observed,
        "unexpected_different": sorted(set(observed) - expected),
        "expected_but_equal": sorted(expected - set(observed)),
        "ok": set(observed) == expected,
    }


def freeze_verdict(release: str, reference: str) -> dict:
    a, b = set(release.split()), set(reference.split())
    return {"only_release": sorted(a - b), "only_reference": sorted(b - a), "equal": a == b}


def plan(repo: Path, rev: str, components: list[str], state_root: Path = STATE_ROOT) -> dict:
    ident = identity(rev, state_root)
    build_dir = Path(ident["build_dir"])
    steps = [["git", "-C", str(repo), "archive", "--format=tar", rev, "|", "tar", "-x", "-C", str(build_dir / "src")]]
    if "rust" in components:
        steps += [rust_compile_argv(build_dir), ["copy 12 binaries + policy -> ctx, SHA256SUMS, marker probe"],
                  rust_image_argv(build_dir, ident["rust_tag"]), probe_argv(ident["rust_tag"], RUST_SHA_PROBE),
                  probe_argv(ident["rust_tag"], RUST_RUNTIME_PROBE)]
        steps += [probe_argv(tag, RUST_SHA_PROBE) for tag, _ in REFERENCES.values()]
    if "python" in components:
        steps += [python_image_argv(build_dir, rev, ident["python_tag"]),
                  probe_argv(ident["python_tag"], PYTHON_FILE_PROBE), probe_argv(PYTHON_REFERENCE[0], PYTHON_FILE_PROBE),
                  probe_argv(ident["python_tag"], PIP_FREEZE_PROBE), probe_argv(PYTHON_REFERENCE[0], PIP_FREEZE_PROBE)]
    return {
        "schema": "qdl.release-image-build-plan.v1",
        "release": RELEASE,
        "identity": ident,
        "components": components,
        "pinned": {"builder": BUILDER_IMAGE, "rust_base": [RUST_BASE_TAG, RUST_BASE_ID], "python_base": PYTHON_BASE,
                   "cargo_work": str(CARGO_WORK)},
        "network": {"rust_compile": "none", "rust_image": "none", "probes": "none",
                    "python_image": "REQUIRED (pinned base pull, poetry install, apt-get upgrade)"},
        "source_preconditions": source_preconditions(repo, rev),
        "binary_expectations": {k: [list(x) for x in v] for k, v in BINARY_EXPECTATIONS.items()},
        "expected_python_diff_vs_rc19": expected_python_diff(repo, rev) if "python" in components else None,
        "steps": steps,
    }


class Runner:
    def __init__(self, build_dir: Path):
        self.build_dir = build_dir
        self.log = []

    def run(self, argv: list[str], name: str, capture: bool = False) -> str:
        started = time.time()
        out_path = self.build_dir / f"{name}.out"
        with open(out_path, "wb") as out:
            proc = subprocess.run(argv, stdout=out if not capture else subprocess.PIPE, stderr=subprocess.STDOUT)
            if capture:
                out.write(proc.stdout)
        self.log.append({"step": name, "argv": argv, "rc": proc.returncode, "seconds": round(time.time() - started, 1),
                         "output": str(out_path)})
        if proc.returncode:
            raise BuildError(f"step {name} failed rc={proc.returncode}; see {out_path}")
        return proc.stdout.decode() if capture else ""


def image_id(ref: str) -> str:
    proc = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", ref], capture_output=True, text=True)
    return proc.stdout.strip() if proc.returncode == 0 else ""


def image_labels(ref: str) -> dict:
    out = subprocess.check_output(["docker", "image", "inspect", "--format", "{{json .Config.Labels}}", ref], text=True)
    return json.loads(out) or {}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(repo: Path, rev: str, components: list[str], args) -> dict:
    ident = identity(rev)
    build_dir = Path(ident["build_dir"])
    receipt = {"schema": "qdl.release-image-build-receipt.v1", "release": RELEASE, "identity": ident,
               "components": components, "script_sha256": sha256_file(Path(__file__)),
               "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    receipt["lock"] = check_lock(LOCK_PATH, args.lock_owner)
    receipt["lock_checks"] = [{"stage": "start", "until_utc": receipt["lock"].get("until_utc")}]

    def guard(stage: str) -> None:
        """Re-validate the owner's window before an expensive stage (raises BuildError -> status FAIL)."""
        lock = check_lock(LOCK_PATH, args.lock_owner)
        receipt["lock_checks"].append({"stage": stage, "until_utc": lock.get("until_utc"),
                                       "at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    receipt["free_gib_before"] = check_disk(STATE_ROOT, args.min_free_gib)
    pre = source_preconditions(repo, rev)
    if not pre["ok"]:
        raise BuildError("source preconditions failed: " + "; ".join(pre["problems"]))
    pins = {BUILDER_IMAGE: BUILDER_IMAGE, RUST_BASE_TAG: RUST_BASE_ID, **dict(REFERENCES.values()),
            PYTHON_REFERENCE[0]: PYTHON_REFERENCE[1]}
    seen = {ref: image_id(ref) for ref in pins}
    moved = {ref: got for ref, got in seen.items() if got != pins[ref]}
    if moved:
        raise BuildError(f"pinned/reference images missing or moved: {moved}")
    for tag in (ident["rust_tag"], ident["python_tag"]):
        if image_id(tag):
            raise BuildError(f"{tag} already exists; never move a release tag")
    if build_dir.exists():
        raise BuildError(f"{build_dir} exists; a release build dir is never reused")
    build_dir.mkdir(mode=0o750, parents=True)
    (build_dir / "src").mkdir(mode=0o750)
    runner = Runner(build_dir)
    receipt["steps"] = runner.log
    status = "FAIL"
    try:
        archive = subprocess.run(["git", "-C", str(repo), "archive", "--format=tar", rev], check=True,
                                 capture_output=True).stdout
        subprocess.run(["tar", "-x", "-C", str(build_dir / "src")], input=archive, check=True)
        receipt["source_archive_sha256"] = hashlib.sha256(archive).hexdigest()
        if "rust" in components:
            receipt["rust"] = build_rust(runner, build_dir, rev, ident["rust_tag"], guard)
        if "python" in components:
            guard("python-image")
            receipt["python"] = build_python(runner, build_dir, repo, rev, ident["python_tag"])
        ok = all(receipt[c]["ok"] for c in components)
        status = "PASS" if ok else "FAIL"
    except BuildError as exc:
        receipt["error"] = str(exc)
    finally:
        receipt["status"] = status
        receipt["free_gib_after"] = round(shutil.disk_usage(STATE_ROOT).free / 2**30, 1)
        receipt["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (build_dir / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def build_rust(runner: Runner, build_dir: Path, rev: str, tag: str, guard=lambda stage: None) -> dict:
    guard("rust-compile")
    runner.run(rust_compile_argv(build_dir), "rust-compile")
    ctx = build_dir / "ctx"
    (ctx / "bin").mkdir(parents=True)
    (ctx / "config/v2").mkdir(parents=True)
    target = CARGO_WORK / "dl-target/release"
    for name in RUST_BINARIES:
        shutil.copy2(target / name, ctx / "bin" / name)
    shutil.copy2(build_dir / "src" / ADMISSION_POLICY, ctx / ADMISSION_POLICY)
    ctx_sums = {f"bin/{n}": sha256_file(ctx / "bin" / n) for n in RUST_BINARIES}
    ctx_sums[ADMISSION_POLICY] = sha256_file(ctx / ADMISSION_POLICY)
    (ctx / "SHA256SUMS").write_text("".join(f"{v}  {k}\n" for k, v in sorted(ctx_sums.items())))
    markers = {n: forbidden_markers(ctx / "bin" / n) for n in RUST_BINARIES}
    (ctx / "Dockerfile").write_text(rust_dockerfile(rev))
    guard("rust-image")
    runner.run(rust_image_argv(build_dir, tag), "rust-image")
    image = (build_dir / "rust.iid").read_text().strip()
    release = parse_sha256sum(runner.run(probe_argv(image, RUST_SHA_PROBE), "rust-postcheck-sha", capture=True))
    runtime = parse_runtime_probe(runner.run(probe_argv(image, RUST_RUNTIME_PROBE), "rust-postcheck-runtime",
                                             capture=True))
    refs = {}
    for ref, (ref_tag, ref_id) in REFERENCES.items():
        refs[ref] = parse_sha256sum(runner.run(probe_argv(ref_id, RUST_SHA_PROBE), f"ref-{ref}-sha", capture=True))
    verdicts = binary_verdicts(release, refs)
    ctx_equal = all(release.get(n) == ctx_sums[f"bin/{n}"] for n in RUST_BINARIES)
    labels = image_labels(image)
    label_ok = (labels.get("org.opencontainers.image.revision") == rev
                and labels.get("org.opencontainers.image.version") == RELEASE
                and "qdl.test.only" not in labels)
    marker_ok = not any(markers.values())
    result = {"image": image, "tag": tag, "labels": labels, "label_ok": label_ok, "ctx_sha256": ctx_sums,
              "image_equals_ctx": ctx_equal, "runtime_probe": runtime, "feature_markers": markers,
              "feature_markers_ok": marker_ok, "key_binaries": {n: release.get(n) for n in KEY_BINARIES},
              "verdicts": verdicts}
    result["ok"] = label_ok and ctx_equal and runtime["ok"] and marker_ok and verdicts["ok"]
    return result


def build_python(runner: Runner, build_dir: Path, repo: Path, rev: str, tag: str) -> dict:
    runner.run(python_image_argv(build_dir, rev, tag), "python-image")
    image = (build_dir / "python.iid").read_text().strip()
    release = parse_sha256sum(runner.run(probe_argv(image, PYTHON_FILE_PROBE), "python-postcheck-files", capture=True))
    reference = parse_sha256sum(runner.run(probe_argv(PYTHON_REFERENCE[1], PYTHON_FILE_PROBE), "ref-rc19-files",
                                           capture=True))
    files = python_file_verdict(release, reference, expected_python_diff(repo, rev))
    freeze = freeze_verdict(runner.run(probe_argv(image, PIP_FREEZE_PROBE), "python-pip-freeze", capture=True),
                            runner.run(probe_argv(PYTHON_REFERENCE[1], PIP_FREEZE_PROBE), "ref-rc19-pip-freeze",
                                       capture=True))
    labels = image_labels(image)
    label_ok = (labels.get("org.opencontainers.image.revision") == rev
                and labels.get("org.opencontainers.image.version") == RELEASE)
    # A pip freeze difference is listed, never waived; it does not fail the build by itself.
    return {"image": image, "tag": tag, "labels": labels, "label_ok": label_ok, "files": files,
            "pip_freeze": freeze, "ok": label_ok and files["ok"]}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rev", default="HEAD", help="release source commit (default HEAD)")
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument("--component", choices=("all", "rust", "python"), default="all")
    parser.add_argument("--build", action="store_true", help="execute (default: dry-run plan only)")
    parser.add_argument("--lock-owner", help="owner of the host measurement lock held for this build")
    parser.add_argument("--min-free-gib", type=int, default=MIN_FREE_GIB)
    args = parser.parse_args(argv)
    if args.build and not args.lock_owner:
        parser.error("--build requires --lock-owner")
    if args.min_free_gib < 1:
        parser.error("--min-free-gib must be positive")
    args.components = ["rust", "python"] if args.component == "all" else [args.component]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        rev = resolve_rev(args.repo, args.rev)
        if not args.build:
            print(json.dumps(plan(args.repo, rev, args.components), indent=2))
            return 0
        receipt = build(args.repo, rev, args.components, args)
    except BuildError as exc:
        print(json.dumps({"status": "REFUSED", "error": str(exc)}))
        return 2
    print(json.dumps({"status": receipt["status"], "receipt": str(Path(receipt["identity"]["build_dir"]) /
                                                                      "receipt.json"),
                      "error": receipt.get("error")}))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())

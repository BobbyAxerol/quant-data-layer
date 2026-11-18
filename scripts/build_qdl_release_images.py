#!/usr/bin/env python3
"""Build the Data Layer release images reproducibly (dry-run by default; v2.3.2: rust and python).

Purpose
    One recipe for the two release images, built from ``git archive <REV>``:

    * ``qdl-v2-rust:<RELEASE>-<sha7>``: the 12 Dockerfile.qdl-rust-runtime binaries compiled at <REV> in the pinned
      builder image (BUILDER_RECIPE: the builder stage of Dockerfile.qdl-rust-runtime without the source, plus the
      rust-toolchain.toml components; offline cargo cache, ``--network none``) and copied onto the pinned runtime base
      (the runtime stage of Dockerfile.qdl-rust-runtime). This is the owner-approved thin method of
      build-rc22-a4e7b3b/build.sh (no BuildKit Rust compile on the runtime disk).
    * ``qdl-v2-python:<RELEASE>-<sha7>``: recipe build of the repository ``Dockerfile`` at <REV>
      (python base pinned by digest inside the Dockerfile). This step needs registry and
      package network access; everything else runs with ``--network none``.

    Post-checks write receipts to ``~/.local/state/qdl-v2/build-v<RELEASE>-<sha7>/``: in-image
    sha256 of every Rust binary against the build context and against the image that runs every Rust role in
    production today (expected EQUAL/DIFFERENT per binary, see BINARY_EXPECTATIONS), ldd/uid/env probes, a
    feature-only TEST clock marker probe, and the sha256 of every python file under /app against each production
    python image (Query, bar_edge) with the expected-different set derived from git.

    The builder is not pulled: when it is absent, build it once from BUILDER_RECIPE (network for apt and rustup only)
    and pin its id in BUILDER_IMAGE, e.g. ``printf %s "$RECIPE" | docker build --iidfile builder.iid -t
    <BUILDER_TAG> -``.

Boundary
    Dry-run (default) only reads git and prints the plan; it runs no docker command.
    ``--build`` builds images and runs ``--rm`` probe containers. It never creates,
    recreates or stops a runtime container, never touches Kafka/Redis/Postgres, never
    pushes an image or a git ref, and never moves an existing tag.

Host coordination (AGENTS 34b, owner rule): a build is ``correctness`` work. It never takes or waits for the
exclusive measurement lock; it writes a non-exclusive activity record
``~/.local/state/host-activity/<owner>.json`` (owner, purpose, class, started/until, CPU cap), runs under a load gate
(1-min load < ``--max-load``, re-checked before every expensive stage) and removes the record when it ends. The
compile and probe containers carry a CPU cap. ``--class measurement`` keeps the old rule (the measurement lock held
by ``--lock-owner``, AGENTS 34a) for a build that is itself part of a measurement window.
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


# v2.3.2 (Stream SessionAlive frames, opt-in DELIVERY_ON_CHANGE) changes rust and python: both images are built. It
# also carries the v2.3.1 dependency refresh into the Rust roles (ROLLOUT_PLAN v2.3.1 section 8). The v2.3.1 note:
# python-only (Query), the Rust roles kept the v2.3.0 rc2 image.
# v2.4.0 (S1-S9: shared reads, quota on admission, session-fair lanes, advisory token revision, role claims ignored)
# changes rust (stream gateway, qdl-contracts accept rule) and python (Query): both images are built.
RELEASE = "2.4.0"
SDK_VERSION = "2.8.0"
REPO = Path(__file__).resolve().parents[1]
HOME = Path.home()
STATE_ROOT = HOME / ".local/state/qdl-v2"
LOCK_PATH = HOME / ".local/state/host-measurement-window.lock"
ACTIVITY_DIR = HOME / ".local/state/host-activity"
MAX_LOAD = 20.0
LOAD_WAIT_S = 1800
COMPILE_CPUS = "2"
PROBE_CPUS = "1"
CPU_CAP = {"rust_compile": f"--cpus {COMPILE_CPUS} --memory 3g", "probes": f"--cpus {PROBE_CPUS}",
           "image_builds": "docker build (BuildKit) has no per-build CPU flag; rust image is COPY only, python image "
                           "is the repository Dockerfile"}
CARGO_WORK = HOME / ".local/state/trading-system/p184-n1-final-20261012"
MIN_FREE_GIB = 40

# The v2.2.4 builder 551428d9 (qdl-rust-builder:r134-test, no recipe in the repository) and the rc16 runtime base
# f41ecb66 were removed from the host after v2.2.4. v2.3.2 pins their documented equivalents:
# * builder: BUILDER_RECIPE, the builder stage of Dockerfile.qdl-rust-runtime (same rust 1.82.0 base digest, same apt
#   packages) without its COPY/cargo steps, plus the rust-toolchain.toml components so rustup never fetches offline;
# * runtime base: c46ae0fa, the runtime stage of Dockerfile.qdl-rust-runtime (debian bookworm abd67ffc + ca-certificates
#   libssl3 libzstd1 zlib1g + user qdl 10001) as built on 2026-11-13 for qdl-v2-rust:2.3.1-funding-50be2b92 (its
#   parent layer, untagged; tagged RUST_BASE_TAG for the overlay). The revision/version labels it inherits are
#   overwritten by rust_dockerfile.
RUST_BASE_IMAGE = "rust@sha256:1111c28d995d06a7863ba6cea3b3dcb87bebe65af8ec5517caaf2c8c26f38010"
BUILDER_APT_PACKAGES = ("clang", "cmake", "libclang-dev", "libcurl4-openssl-dev", "libssl-dev", "libzstd-dev", "make",
                        "pkg-config", "zlib1g-dev")
BUILDER_RECIPE = (
    f"FROM {RUST_BASE_IMAGE}\n"
    "RUN apt-get update -qq \\\n"
    "    && apt-get install -y --no-install-recommends " + " ".join(BUILDER_APT_PACKAGES) + " \\\n"
    "    && rm -rf /var/lib/apt/lists/*\n"
    "RUN rustup component add --toolchain 1.82.0 clippy rustfmt\n"
)
BUILDER_TAG = "qdl-rust-builder:1.82.0-v2.3.2"
BUILDER_IMAGE = "sha256:2cb66ad6bb4033a5d9d6814ab40d260619472568ab7e1826ebbc460f740f58d3"
RUST_BASE_TAG = "qdl-v2-rust-base:bookworm-50be2b92"
RUST_BASE_ID = "sha256:c46ae0fa4abf540f594da7d7f4011511d2f3ae6d9d6dffad97fecab153e2e19f"
PYTHON_BASE = (
    "python:3.12-slim@sha256:2c941e860699f878900b0edc2403613c234d4b32eda3cc9fa7036991a2a63c4a"
)
BUILD_METHOD = f"cargo-cache-{BUILDER_IMAGE[7:15]}+runtime-overlay-{RUST_BASE_ID[7:15]}"

# Commits the release must contain: the stream gateway Codex SR1/SR4 change, the Query G5 change, the v2.3.1
# dependency refresh and the v2.3.2 Stream SessionAlive / DELIVERY_ON_CHANGE change.
REQUIRED_ANCESTORS = {
    "stream_gateway_sr1_sr4": "c934c1e72a46e75123d770dea07441b01cfeb111",
    "query_g5_rc19": "4d3476c5b6944eba557d00ee71f351f150a5b752",
    "v231_dependency_refresh": "ad703172e3142e3d05868136f73a5a7095bdff06",
    "v232_session_alive": "2358766f66d9c08a89d2b40858144c62e866f8a0",
    "v240_python_s1_s8": "67e4199c45a4642eb704729cce9ec6a93189f9a7",
    "v240_rust_stream": "a65e8f8b9d1a21ea0251c4badb6dcaf461dad661",
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

# Reference images (production today, 2026-11-18). Ids are asserted before they are read.
REFERENCES = {
    # Every Rust role runs it: rust_core x3, market_projector x2, stream_kn x2, both ingestors (v2.3.0 rc2, built
    # with Dockerfile.qdl-rust-runtime under BuildKit, CARGO_HOME /usr/local/cargo).
    "v230_rc2": ("qdl-v2-rust:2.3.0-rc2-832c02c4",
                 "sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512"),
}
PRODUCTION_RUST_REFERENCE = "v230_rc2"
PRODUCTION_RUST_REVISION = "832c02c450e219cb64cb818c976c3d0084b8f7b7"
# The python images in production, each with the tree its /app files were built from: Query x2 runs v2.3.1, bar_edge
# runs v2.3.0 rc3 (built on the pinned dependency image 753604f8, so its pip freeze differs by design).
PYTHON_REFERENCES = {
    "query_v231": ("qdl-v2-python:2.3.1-aa75218",
                   "sha256:26c3b051ed1378aa83928230866cffcd6abcea87d248c06f850ee056994b3b47",
                   "aa752182d057e160c95770a4a247c2839a0300ab"),
    "bar_edge_v230_rc3": ("qdl-v2-python:2.3.0-rc3-7cd4fc36",
                          "sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b",
                          "7cd4fc36097ec0ad8cd5bf5ccd002838416ae485"),
}
PYTHON_HASH_ROOTS = ("qdl", "app", "generated", "config", "consumers", "qdl_sdk")

# Per binary: (reference, expectation, reason). The production reference of each role is listed
# first. A result other than the expectation is UNEXPECTED and fails the build verdict.
# Expectations:
#   EQUAL                  whole-file sha256 equal.
#   DIFFERENT              whole-file sha256 differs.
#   EQUAL_LOADED_SECTIONS  every SHF_ALLOC ELF section (code, rodata, relocations, init arrays, notes) is
#                          byte-equal except the linker build-id note, and otherwise only symbol tables or debug
#                          sections differ (LOADED_SECTION_ALLOWED_DIFF); a whole-file
#                          EQUAL also satisfies it. Used where a cfg(test) edit changes the crate's symbol hashes only
#                          (build v2.2.4-c7ce7d3: projector .strtab 0x812d2 -> 0x812c1, .text/.rodata equal).
#   DIFFERENT_BUILD_PATH   the reference was built with another CARGO_HOME: the files differ, the release uses
#                          the method's cargo path, the reference uses a different known one, the number of
#                          embedded registry paths is equal and the section layout (names, types, .text size)
#                          is equal. This is a classification, not an equivalence proof: the behaviour is
#                          proved separately (DR08 live-budget isolation A/B against the reference). A whole-file
#                          EQUAL also satisfies it (byte identity is the stronger result).
#
# v2.3.2 against the production rc2 (832c02c4..REV, `git diff --name-only` of Cargo.toml Cargo.lock rust generated/rust
# and the Cargo.lock dependency closure of each binary's package): every binary has a source or locked-dependency
# delta, so every one is DIFFERENT, never DIFFERENT_BUILD_PATH (which needs an unchanged crate graph). A binary EQUAL
# to rc2 would mean the release image does not carry the v2.3.1 dependency refresh: stop. The deltas per package:
_V231_REFRESH = ("v2.3.1 dependency refresh in its crate graph (ad70317: futures-util 0.3.31->0.3.34, "
                 "tokio 1.48.0->1.51.6, rustls-webpki 0.103.14->0.103.17, webpki-roots 1.0.9->1.0.12, "
                 "zstd-sys 2.0.16->2.1.3, libz-sys 1.1.29->1.1.33; bindgen/regex dropped)")
_CONTRACTS = ("qdl-contracts gateway_bundle.rs and generated/rust common/query v1/v2 changed (DL v2.3.2 SessionAlive; "
              "DL v2.4.0 advisory token revision in Manifest::accepts_token_revision)")
_BINARY_DELTA = {
    "qdl-kafka": f"{_V231_REFRESH}; {_CONTRACTS}",
    "qdl-core": f"futures-util 0.3.34, tokio 1.51.6, rustls-webpki 0.103.17, webpki-roots 1.0.12; {_CONTRACTS}",
    "qdl-venue-core": f"futures-util 0.3.31->0.3.34; {_CONTRACTS}",
    "qdl-projector": f"futures-util 0.3.34, tokio 1.51.6, zstd-sys 2.1.3, libz-sys 1.1.33; {_CONTRACTS}",
    "qdl-stream-gateway": (f"DL v2.3.2 SessionAlive frames and opt-in DELIVERY_ON_CHANGE (hub.rs, service.rs, "
                           f"session_liveness.rs, main.rs); DL v2.4.0 quota after purpose + refund guard, local "
                           f"token bucket, role claims ignored (auth.rs, service.rs, main.rs); {_V231_REFRESH}; "
                           f"{_CONTRACTS}"),
}
BINARY_PACKAGE = {
    "qdl-authority-rehearsal": "qdl-kafka", "qdl-kafka-smoke": "qdl-kafka", "qdl-native-raw-ingestor": "qdl-kafka",
    "qdl-phase91-canary-rehearsal": "qdl-kafka", "qdl-phase92-primary-rehearsal": "qdl-kafka",
    "qdl-production-core": "qdl-kafka", "qdl-realtime-core": "qdl-kafka",
    "qdl-binance-shadow": "qdl-core", "qdl-parity-replay": "qdl-core",
    "qdl-venue-core-certify": "qdl-venue-core", "qdl-projector": "qdl-projector",
    "qdl-stream-gateway": "qdl-stream-gateway",
}
BINARY_EXPECTATIONS = {
    name: [(PRODUCTION_RUST_REFERENCE, "DIFFERENT", _BINARY_DELTA[BINARY_PACKAGE[name]])] for name in RUST_BINARIES
}
EXPECTATIONS = ("EQUAL", "DIFFERENT", "EQUAL_LOADED_SECTIONS", "DIFFERENT_BUILD_PATH")
SECTION_EXPECTATIONS = ("EQUAL_LOADED_SECTIONS", "DIFFERENT_BUILD_PATH")
# Sections that may differ under EQUAL_LOADED_SECTIONS: the symbol tables (not loaded), debug sections, and the
# linker build-id note, which is loaded but is only the linker's hash over the output file (symbol tables
# included), never executed or read by the binary.
LOADED_SECTION_ALLOWED_DIFF = (".symtab", ".strtab", ".note.gnu.build-id")
LOADED_SECTION_ALLOWED_PREFIX = ".debug"
# Embedded registry path prefixes of the known build methods (CARGO_HOME of the builder).
CARGO_REGISTRY_PATHS = {"release": b"/work/cargo/registry/", "dockerfile": b"/usr/local/cargo/registry/"}

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


def _owner_ok(owner: str | None) -> str:
    if not owner or not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", owner):
        raise BuildError("--build requires a plain --lock-owner name (the activity/lock owner)")
    return owner


def record_activity(activity_dir: Path, owner: str | None, purpose: str, minutes: int,
                    at: dt.datetime | None = None) -> dict:
    """AGENTS 34b: write the non-exclusive ``correctness`` record for this build; never waits for any lock."""
    owner = _owner_ok(owner)
    if not 1 <= minutes <= MAX_LOCK_MINUTES:
        raise BuildError(f"activity window must be 1..{MAX_LOCK_MINUTES} minutes")
    at = at or dt.datetime.now(dt.timezone.utc)
    record = {"owner": owner, "purpose": purpose, "class": "correctness",
              "started_utc": at.strftime("%Y-%m-%dT%H:%M:%SZ"),
              "until_utc": (at + dt.timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "cpu_cap": CPU_CAP}
    activity_dir.mkdir(parents=True, exist_ok=True)
    (activity_dir / f"{owner}.json").write_text(json.dumps(record, sort_keys=True) + "\n")
    return record


def check_activity(activity_dir: Path, owner: str, at: dt.datetime | None = None) -> dict:
    """The record still exists, is ours and is inside its window (re-checked at each expensive stage)."""
    path = activity_dir / f"{_owner_ok(owner)}.json"
    try:
        record = json.loads(path.read_text())
        until = dt.datetime.fromisoformat(str(record["until_utc"]).replace("Z", "+00:00"))
    except (OSError, ValueError, KeyError) as error:
        raise BuildError(f"activity record {path} is missing or unreadable") from error
    if record.get("owner") != owner or record.get("class") != "correctness":
        raise BuildError(f"activity record {path} is not this build's correctness record")
    if until <= (at or dt.datetime.now(dt.timezone.utc)):
        raise BuildError(f"activity window of {owner!r} ended at {record['until_utc']}")
    return record


def clear_activity(activity_dir: Path, owner: str) -> None:
    path = activity_dir / f"{owner}.json"
    try:
        if json.loads(path.read_text()).get("owner") == owner:
            path.unlink()
    except (OSError, ValueError):
        pass


def check_load(max_load: float = MAX_LOAD, wait_s: float = LOAD_WAIT_S, read=os.getloadavg,
               sleep=time.sleep, clock=time.monotonic) -> float:
    """Wait (bounded) until the 1-min load is below ``max_load``; the value seen, or BuildError."""
    deadline = clock() + wait_s
    while True:
        load = read()[0]
        if load < max_load:
            return round(load, 2)
        if clock() >= deadline:
            raise BuildError(f"1-min load {load:.1f} stayed >= {max_load} for {wait_s:.0f} s; not building")
        sleep(15)


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


def expected_python_diff(repo: Path, rev: str, base: str) -> list[str]:
    """Paths under PYTHON_HASH_ROOTS whose bytes differ between the reference image's source tree and REV."""
    names = git(repo, "diff", "--name-only", "--no-renames", base, rev, "--", *PYTHON_HASH_ROOTS)
    return sorted(filter(None, names.splitlines()))


def rust_compile_argv(build_dir: Path, cargo_work: Path = CARGO_WORK) -> list[str]:
    return [
        "docker", "run", "--rm", "--pull=never", "--network", "none", "--name", f"qdl-release-{build_dir.name}",
        "--cpus", COMPILE_CPUS, "--memory", "3g", "--pids-limit", "256", "--user", "1001:1001",
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
    return ["docker", "run", "--rm", "--pull=never", "--network", "none", "--read-only", "--cpus", PROBE_CPUS,
            "--entrypoint", "sh", image, "-c", script]


def extract_argv(image: str, binary: str) -> list[str]:
    """Copy one reference binary out of an image to stdout (isolated, read-only, no shell)."""
    if binary not in RUST_BINARIES:
        raise BuildError(f"unknown binary {binary!r}")
    return ["docker", "run", "--rm", "--pull=never", "--network", "none", "--read-only", "--cpus", PROBE_CPUS,
            "--entrypoint", "cat", image, f"/usr/local/bin/{binary}"]


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


SHT_NOBITS = 8
SHF_ALLOC = 0x2


def elf_sections(data: bytes) -> list[dict]:
    """Section table of an ELF64 little-endian file: name, type, flags, size, loaded, sha256 of the file bytes
    (None for NOBITS). Pure parsing, no tool dependency."""
    import struct
    if data[:4] != b"\x7fELF" or data[4] != 2 or data[5] != 1:
        raise BuildError("not an ELF64 little-endian file")
    shoff, = struct.unpack_from("<Q", data, 0x28)
    shentsize, shnum, shstrndx = struct.unpack_from("<HHH", data, 0x3A)
    if shentsize != 64 or not shnum or shoff + shnum * 64 > len(data):
        raise BuildError("bad ELF section header table")
    raw = [struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * 64) for i in range(shnum)]
    str_off, str_size = raw[shstrndx][4], raw[shstrndx][5]
    names = data[str_off:str_off + str_size]
    out = []
    for name_off, kind, flags, _addr, offset, size, *_rest in raw:
        name = names[name_off:names.index(b"\0", name_off)].decode()
        body = None if kind == SHT_NOBITS else hashlib.sha256(data[offset:offset + size]).hexdigest()
        out.append({"name": name, "type": kind, "flags": flags, "size": size,
                    "loaded": bool(flags & SHF_ALLOC), "sha256": body})
    return out


def loaded_section_verdict(release: bytes, reference: bytes) -> dict:
    """EQUAL_LOADED_SECTIONS when the section lists match by name/type/flags and every differing section is a
    symbol table (LOADED_SECTION_ALLOWED_DIFF), never a loaded one; otherwise DIFFERENT."""
    rel, ref = elf_sections(release), elf_sections(reference)
    layout_equal = [(s["name"], s["type"], s["flags"]) for s in rel] == [(s["name"], s["type"], s["flags"]) for s in ref]
    differing = [] if not layout_equal else [
        a["name"] for a, b in zip(rel, ref) if (a["size"], a["sha256"]) != (b["size"], b["sha256"])]
    compared = [{"name": a["name"], "loaded": a["loaded"], "size": a["size"], "release_sha256": a["sha256"],
                 "reference_sha256": b["sha256"], "reference_size": b["size"]}
                for a, b in zip(rel, ref)] if layout_equal else []
    ok = layout_equal and all(name in LOADED_SECTION_ALLOWED_DIFF or name.startswith(LOADED_SECTION_ALLOWED_PREFIX)
                              for name in differing)
    return {"observed": "EQUAL_LOADED_SECTIONS" if ok else "DIFFERENT", "layout_equal": layout_equal,
            "differing_sections": differing,
            "allowed_to_differ": list(LOADED_SECTION_ALLOWED_DIFF) + [LOADED_SECTION_ALLOWED_PREFIX + "*"],
            "sections": compared}


def build_path_verdict(release: bytes, reference: bytes) -> dict:
    """DIFFERENT_BUILD_PATH when the files differ only in a way consistent with another CARGO_HOME: the
    release embeds only the release registry path, the reference only another known one, the counts are
    equal, and the section layout and .text size are equal. Otherwise DIFFERENT."""
    counts = {side: {k: data.count(p) for k, p in CARGO_REGISTRY_PATHS.items()}
              for side, data in (("release", release), ("reference", reference))}
    rel_methods = [k for k, n in counts["release"].items() if n]
    ref_methods = [k for k, n in counts["reference"].items() if n]
    rel, ref = elf_sections(release), elf_sections(reference)
    layout_equal = [(s["name"], s["type"]) for s in rel] == [(s["name"], s["type"]) for s in ref]
    text = {side: next((s["size"] for s in secs if s["name"] == ".text"), None)
            for side, secs in (("release", rel), ("reference", ref))}
    ok = (release != reference and rel_methods == ["release"] and len(ref_methods) == 1
          and ref_methods != ["release"]
          and counts["release"]["release"] == counts["reference"][ref_methods[0]]
          and layout_equal and text["release"] is not None and text["release"] == text["reference"])
    differing = [a["name"] for a, b in zip(rel, ref) if (a["size"], a["sha256"]) != (b["size"], b["sha256"])] \
        if layout_equal else None
    return {"observed": "DIFFERENT_BUILD_PATH" if ok else "DIFFERENT", "registry_path_counts": counts,
            "release_method": rel_methods, "reference_method": ref_methods, "layout_equal": layout_equal,
            "text_size": text, "differing_sections": differing}


def section_analysis(expected: str, release: bytes, reference: bytes) -> dict:
    if expected == "EQUAL_LOADED_SECTIONS":
        return loaded_section_verdict(release, reference)
    if expected == "DIFFERENT_BUILD_PATH":
        return build_path_verdict(release, reference)
    raise ValueError(f"no section analysis for {expected}")


def binary_verdicts(release: dict[str, str], references: dict[str, dict[str, str]],
                    analyses: dict[tuple[str, str], dict] | None = None) -> dict:
    """Whole-file verdicts; for a SECTION_EXPECTATIONS row whose files differ, ``analyses[(binary, ref)]``
    (from section_analysis on the two files) decides, and a missing analysis is NOT_ANALYSED."""
    analyses = analyses or {}
    rows, unexpected = [], []
    missing = sorted(set(RUST_BINARIES) - set(release))
    for name in RUST_BINARIES:
        for ref, expected, reason in BINARY_EXPECTATIONS[name]:
            ref_sha = references.get(ref, {}).get(name)
            got = release.get(name)
            analysis = None
            if got is None or ref_sha is None:
                observed = "NOT_READ"
            elif got == ref_sha:
                observed = "EQUAL"
            elif expected in SECTION_EXPECTATIONS:
                analysis = analyses.get((name, ref))
                observed = analysis["observed"] if analysis else "NOT_ANALYSED"
            else:
                observed = "DIFFERENT"
            row = {"binary": name, "reference": ref, "release_sha256": got, "reference_sha256": ref_sha,
                   "expected": expected, "observed": observed, "reason": reason}
            if analysis is not None:
                row["analysis"] = analysis
            rows.append(row)
            satisfied = observed == expected or (expected in SECTION_EXPECTATIONS and observed == "EQUAL")
            if not satisfied:
                unexpected.append(row)
    core_ok = all(r["observed"] == "EQUAL" for r in rows
                  if r["binary"] == "qdl-realtime-core" and r["reference"] == PRODUCTION_RUST_REFERENCE)
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
        steps += [rust_compile_argv(build_dir), [f"copy {len(RUST_BINARIES)} binaries + policy -> ctx, SHA256SUMS, marker probe"],
                  rust_image_argv(build_dir, ident["rust_tag"]), probe_argv(ident["rust_tag"], RUST_SHA_PROBE),
                  probe_argv(ident["rust_tag"], RUST_RUNTIME_PROBE)]
        steps += [probe_argv(tag, RUST_SHA_PROBE) for tag, _ in REFERENCES.values()]
    if "python" in components:
        steps += [python_image_argv(build_dir, rev, ident["python_tag"]),
                  probe_argv(ident["python_tag"], PYTHON_FILE_PROBE), probe_argv(ident["python_tag"], PIP_FREEZE_PROBE)]
        for ref_tag, _, _ in PYTHON_REFERENCES.values():
            steps += [probe_argv(ref_tag, PYTHON_FILE_PROBE), probe_argv(ref_tag, PIP_FREEZE_PROBE)]
    return {
        "schema": "qdl.release-image-build-plan.v1",
        "release": RELEASE,
        "identity": ident,
        "components": components,
        "pinned": {"builder": BUILDER_IMAGE, "builder_tag": BUILDER_TAG, "builder_recipe": BUILDER_RECIPE, "rust_base": [RUST_BASE_TAG, RUST_BASE_ID], "python_base": PYTHON_BASE,
                   "cargo_work": str(CARGO_WORK)},
        "network": {"rust_compile": "none", "rust_image": "none", "probes": "none",
                    "python_image": "REQUIRED (pinned base pull, poetry install, apt-get upgrade)"},
        "source_preconditions": source_preconditions(repo, rev),
        "binary_expectations": {k: [list(x) for x in v] for k, v in BINARY_EXPECTATIONS.items()},
        "expected_python_diff": {ref: expected_python_diff(repo, rev, tree) for ref, (_, _, tree)
                                 in PYTHON_REFERENCES.items()} if "python" in components else None,
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
    measurement = getattr(args, "klass", "correctness") == "measurement"
    if measurement:
        receipt["lock"] = check_lock(LOCK_PATH, args.lock_owner)
        receipt["lock_checks"] = [{"stage": "start", "until_utc": receipt["lock"].get("until_utc")}]
    else:
        receipt["activity"] = record_activity(ACTIVITY_DIR, args.lock_owner,
                                              f"DL v{RELEASE} release image build", args.activity_minutes)
        receipt["lock_checks"] = [{"stage": "start", "class": "correctness",
                                   "until_utc": receipt["activity"]["until_utc"],
                                   "load1": check_load(args.max_load)}]

    def guard(stage: str) -> None:
        """Re-validate the window (and, for correctness work, the load gate) before an expensive stage."""
        if measurement:
            lock = check_lock(LOCK_PATH, args.lock_owner)
            entry = {"stage": stage, "until_utc": lock.get("until_utc")}
        else:
            record = check_activity(ACTIVITY_DIR, args.lock_owner)
            entry = {"stage": stage, "class": "correctness", "until_utc": record["until_utc"],
                     "load1": check_load(args.max_load)}
        entry["at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        receipt["lock_checks"].append(entry)
    receipt["free_gib_before"] = check_disk(STATE_ROOT, args.min_free_gib)
    pre = source_preconditions(repo, rev)
    if not pre["ok"]:
        raise BuildError("source preconditions failed: " + "; ".join(pre["problems"]))
    pins = {BUILDER_IMAGE: BUILDER_IMAGE, RUST_BASE_TAG: RUST_BASE_ID, **dict(REFERENCES.values()),
            **{tag: ref_id for tag, ref_id, _ in PYTHON_REFERENCES.values()}}
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
        if not measurement:
            clear_activity(ACTIVITY_DIR, args.lock_owner)
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
    analyses = {}
    refbin = build_dir / "refbin"
    for name in RUST_BINARIES:
        for ref, expected, _ in BINARY_EXPECTATIONS[name]:
            ref_sha = refs.get(ref, {}).get(name)
            if expected not in SECTION_EXPECTATIONS or ref_sha is None or release.get(name) in (None, ref_sha):
                continue
            refbin.mkdir(exist_ok=True)
            out = refbin / f"{ref}-{name}"
            runner.run(extract_argv(REFERENCES[ref][1], name), f"ref-{ref}-{name}-extract")
            out.write_bytes((build_dir / f"ref-{ref}-{name}-extract.out").read_bytes())
            if sha256_file(out) != ref_sha:
                raise BuildError(f"extracted {ref}/{name} does not match its in-image sha256")
            analyses[(name, ref)] = section_analysis(expected, (ctx / "bin" / name).read_bytes(), out.read_bytes())
    verdicts = binary_verdicts(release, refs, analyses)
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
    release_freeze = runner.run(probe_argv(image, PIP_FREEZE_PROBE), "python-pip-freeze", capture=True)
    files, freeze = {}, {}
    for ref, (_, ref_id, tree) in PYTHON_REFERENCES.items():
        reference = parse_sha256sum(runner.run(probe_argv(ref_id, PYTHON_FILE_PROBE), f"ref-{ref}-files",
                                               capture=True))
        files[ref] = python_file_verdict(release, reference, expected_python_diff(repo, rev, tree))
        freeze[ref] = freeze_verdict(release_freeze, runner.run(probe_argv(ref_id, PIP_FREEZE_PROBE),
                                                                f"ref-{ref}-pip-freeze", capture=True))
    labels = image_labels(image)
    label_ok = (labels.get("org.opencontainers.image.revision") == rev
                and labels.get("org.opencontainers.image.version") == RELEASE)
    # A pip freeze difference is listed, never waived; it does not fail the build by itself.
    return {"image": image, "tag": tag, "labels": labels, "label_ok": label_ok, "files": files,
            "pip_freeze": freeze, "ok": label_ok and all(v["ok"] for v in files.values())}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--rev", default="HEAD", help="release source commit (default HEAD)")
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument("--component", choices=("all", "rust", "python"), default="all")
    parser.add_argument("--build", action="store_true", help="execute (default: dry-run plan only)")
    parser.add_argument("--lock-owner", help="owner name: of the correctness activity record (default class), or of "
                                             "the measurement lock held for this build (--class measurement)")
    parser.add_argument("--class", dest="klass", choices=("correctness", "measurement"), default="correctness",
                        help="AGENTS 34b class; a build is correctness work unless it is part of a measurement")
    parser.add_argument("--activity-minutes", type=int, default=90)
    parser.add_argument("--max-load", type=float, default=MAX_LOAD)
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

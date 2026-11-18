# DL v2.4.0 R0 build record (images built and post-checked; security scans NOT run)

Built once on 2026-11-18 13:19:20-13:40:02Z with `scripts/build_qdl_release_images.py --rev c95476c --build
--lock-owner dl-v240-r0-build` (recipe commit c95476c, the d37e88c recipe with the v2.4.0 constants and the AGENTS 34b
correctness class). Receipt (outside Git): `~/.local/state/qdl-v2/build-v2.4.0-c95476c/receipt.json`
(sha256 `b8e948f48b1e0e6f91a885570ae1586f2f9a2cf95dd7aca61f2014cfd4f5011c`), status `PASS`.

- Host class: `correctness` activity record `~/.local/state/host-activity/dl-v240-r0-build.json` (removed at the
  end); no measurement lock taken. Load gate (1-min load < 20) at each stage: start 18.67, rust-compile 18.67,
  rust-image 19.30, python-image 18.91.
- CPU caps: compile `--cpus 2 --memory 3g --network none`, probes/extracts `--cpus 1 --network none --read-only`;
  the two `docker build` steps have no BuildKit CPU flag.
- Source: `git archive c95476cae46a98113fb6bbebaa1e54fac256f363` (archive sha256 `4715cdaa02ad5824...6d457e72`).
  Preconditions PASS (SDK 2.8.0, pinned python base, binary set, required ancestors incl. 67e4199 and a65e8f8).

## Rust: `qdl-v2-rust:2.4.0-c95476c`, **`sha256:6970231dfe984c5b01ee62758b9e26b3b1a9a1b810668417b8fc65ce04937a75`**

- Labels: revision `c95476ca...`, version `2.4.0`, `qdl.build.method=cargo-cache-2cb66ad6+runtime-overlay-c46ae0fa`,
  no `qdl.test.only`. Builder `qdl-rust-builder:1.82.0-v2.3.2` `2cb66ad6...`, runtime base `c46ae0fa...`.
- Compile: 12 binaries, offline, cargo "Finished release in 2m 48s".
- Post-checks: image binaries equal the build context (12/12); runtime probe (ldd/uid/env) ok; no TEST clock feature
  markers; binary verdicts vs production rc2 `58bdd282`: 12/12 DIFFERENT as expected, 0 unexpected.
- `qdl-stream-gateway` sha256 `9ebd1e4fa4ff29dd33ffba4a1c06014c05b03f9490d945854fd4186e1f08f8ed`.

## Python: `qdl-v2-python:2.4.0-c95476c`, **`sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`**

- Labels: revision and version `2.4.0` ok.
- Files under /app vs Query v2.3.1 `26c3b051`: 457 vs 454 files, 30 different, all expected from git, 0 unexpected;
  vs bar_edge v2.3.0 rc3 `a235c0a1`: 31 different, all expected, 0 unexpected.
- pip freeze: equal to Query v2.3.1; vs bar_edge rc3 differs as by design (rc3 was built on the pinned dependency
  image): certifi, msgpack, setuptools versions, pillow and soupsieve only in rc3 (listed, not waived).

## OPEN (scheduled by the coordinator, once)

cargo-deny (ignore = []), trivy, pip-audit on the two image ids above. No gate may be marked passed before they run.

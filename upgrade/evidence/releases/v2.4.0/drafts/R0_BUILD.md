# DL v2.4.0 R0 build record (images built and post-checked; security gates PASS, see R0_SECURITY.md)

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

## Rust image rebuilt on an upgraded runtime base (2026-11-18 ~13:55-14:03Z, coordinator-approved R0 fix)

The first rust image `6970231d` failed trivy on fixable Debian base packages (`R0_SECURITY.md`). Fix:
- runtime base rebuilt from the pinned `RUST_BASE_RECIPE` (runtime stage of `Dockerfile.qdl-rust-runtime` +
  `apt-get upgrade`, network for apt only; recipe sha256 `86e535de682cfb511f0b24a5f771bac3444cfcf62e279d671f8f4c7b40ca9e0f`):
  `qdl-v2-rust-base:bookworm-v240-20261118` = `sha256:b33ec0cb161e80f3300684f96fa956eedabc49325e401a170fb25c02d4b2eef7`
  (debian 12.15, perl-base 5.36.0-7+deb12u4, apt/libapt-pkg 2.6.2, libpcre2-8-0 10.42-1+deb12u2); pinned in `3d4d84b`.
- **`qdl-v2-rust:2.4.0-3d4d84b` = `sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e`**, built
  with `--rev 3d4d84b --component rust`, receipt `~/.local/state/qdl-v2/build-v2.4.0-3d4d84b/receipt.json` PASS;
  `qdl.build.method=cargo-cache-2cb66ad6+runtime-overlay-b33ec0cb`. The build context (12 binaries + admission
  policy) is byte-identical to the c95476c build (13/13 sha256 equal): only the base layer changed. Post-checks as
  above (image = context, runtime probe, no TEST markers, 12/12 DIFFERENT vs rc2 as expected).
- The superseded `6970231d` was removed by digest (no container referenced it). The old base `c46ae0fa` is kept: it
  is the parent of the 2.3.1-funding and 2.3.2 rust images.

## A-FUND rebuild from c17472c (2026-11-18 20:31-20:36Z): both images, base b33ec0cb

Query code changed (historical MARK_INDEX_PRICE candles, SDK 2.9.0), so both images were rebuilt with the recipe
(`--rev c17472c`, compile `--cpus 2 --cpu-shares 256`, probes `--cpus 1 --cpu-shares 256`, inside a host slot,
`--max-load 30`; load 17.71 / 17.71 / 21.31 / 21.29). Receipt `~/.local/state/qdl-v2/build-v2.4.0-c17472c/receipt.json`
PASS.
- **rust `qdl-v2-rust:2.4.0-c17472c` = `sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98`**,
  `runtime-overlay-b33ec0cb`; context 13/13 byte-identical to 3d4d84b (only the revision label differs).
- **python `qdl-v2-python:2.4.0-c17472c` = `sha256:9cffe89b2a44b29c6a4a8ef707512141464836ad1570319bb9462c9ec92a837f`**;
  files vs Query v2.3.1 and bar_edge rc3: 0 unexpected differences; pip freeze as before.
These supersede `aef35edf` / `75717b3c` as the v2.4.0 release candidates.

## Hot-read fallback rebuild from acf42d7 (2026-11-19 05:32:58-05:34:42Z): python only

Window 1 RCA (ROLLOUT_PLAN section 8): `qdl/runtime/kn_hot_view.py` changed, so only the python image was rebuilt with
the recipe (`--rev acf42d7 --component python --build`, host slot, `--max-load 30`, correctness activity record, load
19.52/19.88). Receipt `~/.local/state/qdl-v2/build-v2.4.0-acf42d7/receipt.json` PASS.
- **python `qdl-v2-python:2.4.0-acf42d7` = `sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18`**
  (python base `python:3.12-slim@sha256:2c941e86…`, unchanged); files vs Query v2.3.1 / bar_edge rc3: 0 unexpected
  differences; pip freeze equal to v2.3.1. Against `9cffe89b` exactly 3 files differ under `/app`: `kn_hot_view.py`,
  `tests/test_kn_hot_view.py`, `DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`; pip freeze equal.
- rust stays `abafab2c` (no Rust change; the rust base `b33ec0cb` is not used by the python image).
- `9cffe89b` is kept until the window closes (rollback reference for no live role; remove by digest afterwards).

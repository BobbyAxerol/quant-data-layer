# DL v2.3.2 R0: Rust and Python release images, build proof and security scans

Status: **images built and post-checked (PASS); security scans NOT RUN.** The cargo-deny, trivy and
pip-audit gates are open. The v2.3.1 section 8 advisory follow-up is not closed until they pass on these exact
image ids.

## Build

Built once on 2026-11-18 08:32:58-08:44:31Z with `scripts/build_qdl_release_images.py --rev 2401393 --build`
(recipe commit d37e88c), under the host lock `dl-v232-r0-build`. The lock was taken 08:32:53Z, released
08:44:31Z right after the build, and was valid until 11:02:53Z. Receipt (outside Git):
`~/.local/state/qdl-v2/build-v2.3.2-2401393/receipt.json`, status `PASS`.

- Source: `git archive 240139391f3f3ec85c4051d9ace13bb2b3130331` (archive sha256 `792dacbaf18c5d36...6cff6457f`).
  The source preconditions passed: SDK 2.7.0, pinned python base, a binary set equal to
  Dockerfile.qdl-rust-runtime, empty gateway default features, and the required ancestors c934c1e, 4d3476c,
  ad70317 and 2358766.
- Docker 29.7.2 with the containerd snapshotter: an image id is the manifest digest.

### Rust: `qdl-v2-rust:2.3.2-2401393`, **`sha256:646378c6dcfbbf9b01e1b58ba41dd57ef98a847e3236ad531eafa8afb4fc2901`**

- Labels: revision `240139391f3f...`, version `2.3.2`, `qdl.build.method=cargo-cache-2cb66ad6+runtime-overlay-c46ae0fa`,
  no `qdl.test.only`.
- Compile: 12 binaries, `--network none`, `--cpus 2 --memory 3g`, offline cargo cache
  `~/.local/state/trading-system/p184-n1-final-20261012/cargo`, `cargo build --offline --locked --release` (4
  invocations, as in Dockerfile.qdl-rust-runtime). The step took 540 s, and cargo reported "Finished release in
  2m 37s".
- Builder `qdl-rust-builder:1.82.0-v2.3.2` `sha256:2cb66ad6bb4033a5d9d6814ab40d260619472568ab7e1826ebbc460f740f58d3`.
  - Built 2026-11-18 08:28Z from `BUILDER_RECIPE` in the script, with network for apt and rustup only.
  - The recipe is the builder stage of Dockerfile.qdl-rust-runtime: rust 1.82.0 `rust@sha256:1111c28d...` and the
    same apt packages, without COPY/cargo. It adds `rustup component add clippy rustfmt` from
    rust-toolchain.toml. rustc and cargo are 1.82.0.
  - It replaces 551428d9, which is gone from the host and had no recipe.
  - The builder was built twice by mistake: a duplicate driver ran, serialized by the container flock, about 80 s. The
    first id was replaced by `2cb66ad6`.
- Runtime base `qdl-v2-rust-base:bookworm-50be2b92` `sha256:c46ae0fa4abf540f594da7d7f4011511d2f3ae6d9d6dffad97fecab153e2e19f`.
  - It is the runtime stage of Dockerfile.qdl-rust-runtime: debian bookworm `abd67ffc...` plus ca-certificates,
    libssl3, libzstd1, zlib1g and the user qdl 10001.
  - It was built on 2026-11-13, as the untagged parent layer of `qdl-v2-rust:2.3.1-funding-50be2b92`. The tag was
    added today.
  - It replaces f41ecb66, which is gone.
  - Its apt packages are from 2026-11-13. trivy decides whether that is acceptable.
- Offline cache: before the build, 6 crates were missing from the cache. They were copied from the local
  preflight cache, which was verified by the Cargo.lock checksums, and its newer index entries were copied with
  them. The 6 crates were `libz-sys-1.1.33`, `rustls-webpki-0.103.17`, `tokio-1.51.6`, `tokio-macros-2.7.2`,
  `webpki-roots-1.0.12` and `zstd-sys-2.1.3+zstd.1.5.7`. No network fetch happened.
- Still absent from the cache, and not needed for `x86_64-unknown-linux-gnu`: `bumpalo-3.20.3`,
  `js-sys-0.3.104`, `wasip2-1.0.4`, `wasm-bindgen-0.2.127`, `wasm-bindgen-macro-support-0.2.127`, `web-sys-0.3.104`,
  `windows-sys-0.52.0`, `windows-sys-0.61.2` and `zerocopy-derive-0.8.56`. The offline compile succeeded without
  them. The build target dir `dl-target` was deleted after the build.
- Post-checks:
  - the in-image sha256 equals the build context for all 12 binaries;
  - `ldd` "not found" is 0 for realtime-core, projector, stream-gateway and native-raw-ingestor;
  - no `QDL_TEST_DATA_CLOCK` env, and uid 10001;
  - no feature-only TEST clock markers.

| Binary | sha256 in R0 | rc2 `58bdd282` (production) | Expected | Observed |
|---|---|---|---|---|
| qdl-authority-rehearsal | `dfc0e1d80b2d127c7d0160d4ba1912547e3ef33a2b471244fa05ffbaabe17cc6` | `01e930c7d7c7...` | DIFFERENT | DIFFERENT |
| qdl-binance-shadow | `c5adad4dab34b545bb5e1b4156d753c7a9b06a88991c253fabe8afaefdfb1632` | `8c6c19d55f27...` | DIFFERENT | DIFFERENT |
| qdl-kafka-smoke | `0b443ce8d0a62d1e87045968dbfb55d27260b5badf5219f41c47b0b5ceca9609` | `9532e8463378...` | DIFFERENT | DIFFERENT |
| qdl-native-raw-ingestor | `959173ab6794c88fe4f0bd47c864f2a1050373312741ecde0743c5d459e98c58` | `25fb62b6e2bc...` | DIFFERENT | DIFFERENT |
| qdl-parity-replay | `f7645ee6b4d95b3683fc3665dca01a1ac90cf2a739a70fea4b9dcb43a96b7035` | `37640815a92b...` | DIFFERENT | DIFFERENT |
| qdl-phase91-canary-rehearsal | `065b10fb5718e1832bcf1a693d44c726cada6954dc8ab0df50ded3f770634d0c` | `c9a3495dbaf8...` | DIFFERENT | DIFFERENT |
| qdl-phase92-primary-rehearsal | `7c82a934f6f14c402c5b5e99ffe29bc613ef64119f43f5262643de33d7ca15ce` | `ebe0931370e3...` | DIFFERENT | DIFFERENT |
| qdl-production-core | `6c2dee8d6b85ac9f352551753ce907c4aaa0f4fd9924498992569775d0c16958` | `f55804da004e...` | DIFFERENT | DIFFERENT |
| qdl-projector | `3b2b32a9108bf70235038b5f7590dc30e9d195f2f12de96d03cf887ca84e97d0` | `c3f514ac959b...` | DIFFERENT | DIFFERENT |
| qdl-realtime-core | `a0a7db0de08975dab84cf8f22888ef2ef1b3eb39182a0ff245c669d965e37042` | `c8b77ffc275d...` | DIFFERENT | DIFFERENT |
| qdl-stream-gateway | `9838cf544bb8bc2c34f5e198653e8fa95ba8459bcddb019ec2842d808c335d48` | `7064936865a8...` | DIFFERENT | DIFFERENT |
| qdl-venue-core-certify | `f81d552a2c4b54c7abc44da03e75267150cf96ab663c863d959eef0adb5896de` | `9fcba38b4453...` | DIFFERENT | DIFFERENT |

Why every binary is expected to be DIFFERENT (`BINARY_EXPECTATIONS`):
- 832c02c4..2401393 changes qdl-contracts and generated/rust, which are in every crate graph.
- The Cargo.lock closure of every package contains crates refreshed in v2.3.1: futures-util 0.3.34, tokio 1.51.6,
  rustls-webpki 0.103.17, webpki-roots 1.0.12, zstd-sys 2.1.3 and libz-sys 1.1.33, depending on the package.
- The stream gateway also carries the v2.3.2 SessionAlive and DELIVERY_ON_CHANGE code.
- A binary EQUAL to rc2 would have meant that the release image lacks the refresh.
- The test `test_every_binary_has_a_rust_delta_since_the_production_image` proves the delta from git. Unexpected
  results: 0. The binary-expectation gate is **PASS**.

### Python: `qdl-v2-python:2.3.2-2401393`, **`sha256:7068ad1101525d20aa33475af78eeff8eccba0bb404ef64d34b1f0ad50d249a8`**

- Recipe: the repository `Dockerfile` with `QDL_GIT_SHA=2401393...` and `QDL_RELEASE=2.3.2`, dependency image
  `builder` (full poetry install). It needed the network. The step took 117 s.
- Labels: revision and version are correct.
- `verify_runtime_dependencies.py` ran inside the build and passed (the build step's rc is 0).
- The image serves Query and bar_edge.
- File sha256 under `/app` (qdl, app, generated, config, consumers, qdl_sdk; 454 files each):

| Reference | Image | Tree | Files that differ | Of them expected by git | Unexpected | Pip freeze |
|---|---|---|---|---|---|---|
| Query x2 | `26c3b051...3b47` 2.3.1 | aa752182 | 11 | 11 | 0 | equal |
| bar_edge | `a235c0a1...d0b` 2.3.0-rc3 | 7cd4fc36 | 15 | 15 | 0 | R0 has certifi 2026.10.23, msgpack 1.2.4, setuptools 84.2.1, and no pillow or soupsieve (rc3: certifi 2026.4.22, msgpack 1.2.1, setuptools 84.0.0, pillow 12.3.0, soupsieve 2.9.2) |

The bar_edge freeze difference is the v2.3.1 dependency refresh. It is listed, not waived.

## Security gates: NOT RUN

| Gate | Tool and source | Result |
|---|---|---|
| cargo-deny `check`, `deny.toml` with `ignore = []` | cargo-deny 0.20.2 (x86_64-unknown-linux-musl, local copy from the v2.3.0 tooling); advisory-db refreshed on the host 2026-11-18 to `233155b4025b` (2026-11-17T11:14:35+01:00) | **NOT RUN** |
| trivy, rust and python images | `aquasec/trivy:0.70.0` `sha256:be1190af...`, `--input` docker save, `--ignore-unfixed --severity CRITICAL,HIGH --exit-code 1`, repository `.trivyignore` | **NOT RUN** |
| pip-audit on the python image | pip-audit `>=2.9,<3` in `python:3.12-slim`, `pip freeze --local` of the image, `--no-deps --disable-pip`, no ignore list | **NOT RUN** |

Why the gates did not run:
- Each scan waits for the agent resource gate: 1-min load < 20 and no other `claude-*`, `dl-v23*` or `qq-*`
  container running.
- From 08:45Z to about 10:30Z the host always had other agents' `claude-*` test containers or a load above 20.
- The scan driver was stopped without having started a container.

The script that runs the gates is `<scratchpad>/qq/dls2/scan.sh`. It runs one capped `--rm` container at a time
under the shared container flock, and gives up after `GATE_WAIT_S`, 7200 s by default. Its outputs go to
`qq/dls2/out/`. No gate may be marked passed until it has run on the two image ids above.

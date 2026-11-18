# DL v2.4.0 R0 security gates (2026-11-18): **PASS** on the release ids (rust re-scanned after the base fix)

Images (receipt `~/.local/state/qdl-v2/build-v2.4.0-c95476c/receipt.json`):
- rust `qdl-v2-rust:2.4.0-c95476c` `sha256:6970231dfe984c5b01ee62758b9e26b3b1a9a1b810668417b8fc65ce04937a75`;
- python `qdl-v2-python:2.4.0-c95476c` `sha256:75717b3c44364fe708769083b6b8268b913bb6ed270b20b162e1c0eaeab7f2d9`.

Driver: `<scratchpad>/v240/scan/scan240.sh` (successor of `qq/dls2/scan.sh`), one capped `--rm` container at a time
(`--cpus 1`), gate 1-min load < 20 (seen 19.96, 19.87, 17.02) and no other `claude-v240-*` container. Outputs (not
durable) under `<scratchpad>/v240/scan/out/`. Two invocation defects of the old driver were fixed and the two gates
re-run once each: cargo-deny 0.20.2 has no `--disable-fetch` (now `cargo-deny --offline check`), and the pip freeze
captured a pip cache WARNING line into the requirements file (stderr now separate).

| Gate | Tool / data | Result |
|---|---|---|
| cargo-deny, `ignore = []` (`deny.toml`) | cargo-deny 0.20.2, offline, source = `git archive c95476c`; RustSec advisory-db `472a4c9a` (2026-11-18 10:14 +01:00, refreshed by a public git fetch) | **PASS**: `advisories ok, bans ok, licenses ok, sources ok`; 6 duplicate-version warnings (hashbrown, indexmap, socket2, syn, tower, webpki-roots), no errors |
| pip-audit | pip-audit 2.10.1 on `pip freeze --local` of the python image (47 packages; only public package names sent) | **PASS**: `No known vulnerabilities found` |
| trivy, python image | trivy 0.70.0, vuln scanner, DB 2026-11-18T07:15Z (downloaded; the image was scanned from a local `docker save`), `.trivyignore` (2 entries, unchanged) | **PASS** (no fixable CRITICAL/HIGH). debian 13.7: 47 HIGH, all unfixed (no Debian fix); MEDIUM 11 fixable (pip 25.0.1 -> 25.3/26.x, the image's own pip; jsonwebtoken 9.3.1 in `app/Cargo.lock`, CVE-2026-25537, fixed 10.3.0); LOW 2 fixable |
| trivy, rust image | same | **FAIL: 3 CRITICAL + 10 HIGH with a fix available**, all in the debian 12.15 runtime base `c46ae0fa` (built 2026-11-13), none in DL code |

## Blocking findings (rust image, runtime base packages; fix available, not waived)

| Package | Installed | Fixed | Findings |
|---|---|---|---|
| perl-base | 5.36.0-7+deb12u3 | 5.36.0-7+deb12u4 | CRITICAL CVE-2026-13221, CVE-2026-42496, CVE-2026-8376; HIGH CVE-2026-42497, CVE-2026-48962, CVE-2026-57432, CVE-2026-57433 |
| apt, libapt-pkg6.0 | 2.6.1 | 2.6.2 | HIGH CVE-2026-109518 |
| libpcre2-8-0 | 10.42-1 | 10.42-1+deb12u1 / deb12u2 | HIGH CVE-2026-86145, CVE-2026-89157, CVE-2026-89161, CVE-2026-103111 |

Also in the rust image: 1 CRITICAL and 37 HIGH without a Debian fix, MEDIUM 9 fixable / 108 unfixed, LOW 87 (base
system packages: bsdutils/util-linux family, coreutils, bash, ...). The Rust binaries themselves carry no package
metadata for trivy; their dependency closure is covered by cargo-deny above (PASS).

**Proposed fix (not done, needs the coordinator):** rebuild the pinned runtime base `RUST_BASE` (the runtime stage of
`Dockerfile.qdl-rust-runtime`) with `apt-get update && apt-get upgrade` on the current bookworm point release, pin its
new id in the recipe, rebuild `qdl-v2-rust:2.4.0-<sha>` (binaries unchanged: same source, the overlay only), and
re-run trivy-rust once. No waiver is proposed for fixable CRITICAL/HIGH.

## Non-blocking, recorded

- jsonwebtoken 9.3.1 (MEDIUM CVE-2026-25537, fixed in 10.x) is a pinned dependency of the Rust gateway
  (`Cargo.toml` `=9.3.1`, `default-features = false`); RustSec/cargo-deny reports no advisory for it. A 10.x
  upgrade is a code change for a later release.
- pip 25.0.1 inside the python image (MEDIUM, fixable): the image's own installer, not used at runtime.

## Re-scan after the approved base fix (2026-11-18 14:07:44-14:07:54Z): **PASS**

- Image: `qdl-v2-rust:2.4.0-3d4d84b` `sha256:aef35edf6cfa63f7ea9d8de69bce0a8e4354cb1008250be885344d6a534dfe5e`
  (`R0_BUILD.md`: same binaries as `6970231d`, runtime base `b33ec0cb`).
- trivy 0.70.0 once, same DB (2026-11-18T07:15Z) and `.trivyignore`, `--cpus 1 --memory 3g`, gate load 19.68.
- Result: **0 fixable CRITICAL/HIGH** (all 13 blocking findings above are fixed). Remaining, all without a Debian fix:
  CRITICAL 1, HIGH 37, MEDIUM 108, LOW 87. No fixable MEDIUM. No waiver.
- cargo-deny and pip-audit results above are unchanged (the Rust source and the python image are the same).

Release ids: rust `aef35edf`, python `75717b3c`. Follow-up for the next release (MEDIUM, no waiver needed):
jsonwebtoken 10.x, the python image's pip 25.0.1.

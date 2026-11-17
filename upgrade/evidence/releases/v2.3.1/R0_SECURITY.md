# DL v2.3.1 R0: candidate Python image and security scans

Built once on 2026-11-17 06:06-06:08Z, under the host lock `dl-v231-r0-build` (06:03:15-06:08Z), which was released
right after the build.

**Image:** `qdl-v2-python:2.3.1-aa75218`, **`sha256:26c3b051ed1378aa83928230866cffcd6abcea87d248c06f850ee056994b3b47`**.
- Labels: revision `aa752182d057e160c95770a4a247c2839a0300ab`, version `2.3.1`. Size 186,931,658 bytes.
- Source: `git archive aa752182`, archive sha256 in `~/.local/state/qdl-v2/build-v2.3.1-aa75218/source.tar.sha256`.
- Recipe: the repository `Dockerfile` with `--build-arg QDL_GIT_SHA=<rev> --build-arg QDL_RELEASE=2.3.1`. This is
  exactly `python_image_argv` in `scripts/build_qdl_release_images.py`.
- Why the recipe ran directly: the script refused to start. Its pinned Rust reference images (builder `551428d9`,
  rc14/rc16/rc18, isolation-sr1) are no longer on the host, and it checks them even for `--component python`. This is
  a follow-up for the section-7 Rust window.
- In-build dependency verification (`verify_runtime_dependencies.py`): PASS, 5,804 files, lock sha256
  `7f925e6e55c01b30...`.

**CI release-gates runtime checks**, the same assertions as `.github/workflows/ci.yml`:
- `/opt/venv/bin/poetry` is absent;
- the uvicorn shebang is `/opt/venv/bin/python`; uvicorn 0.46.0, CPython 3.12.14;
- installed versions: msgpack 1.2.4, setuptools 84.2.1, certifi 2026.10.23, anyio 4.15.1, httpx 0.28.1,
  httpcore 1.0.9, pyjwt 2.15.1;
- pillow and soupsieve are not installed.

**pip-audit** (>=2.9,<3) on `pip freeze --local` (47 packages), with **no ignore list**:
- result: `No known vulnerabilities found`, exit 0.

**trivy 0.70.0** (CI version):
- target: `docker save` of the image (`--input`), `--ignore-unfixed --severity CRITICAL,HIGH --exit-code 1`, with
  the repository `.trivyignore` (2 BuildKit entries only);
- debian 13.7 (87 packages): 0;
- every python-pkg: 0;
- `app/Cargo.lock`: 0;
- exit 0.
- Note: this scan started at host load 23.0, above the agent's guard of 20. It is a correctness scan, not a
  measurement.

Receipts, outside Git: `~/.local/state/qdl-v2/build-v2.3.1-aa75218/{python-build.log,python.iid,pip-audit.txt,trivy.txt}`.
The image tar was deleted after the scan.

**Rust side:** cargo-deny 0.20.2 with an empty ignore list reports advisories ok (CERTIFICATE_DRAFT section 2). No
Rust image is built for v2.3.1 (option A, ROLLOUT_PLAN section 7).

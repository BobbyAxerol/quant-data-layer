# QDL SDK 2.1.0 Consumer Artifact

Status: LOCAL_ARTIFACT_PUBLISHED / TS_INSTALL_PASS /
ALPHA_REQUIRES_PYJWT_UPDATE. No shared runtime or consumer was changed.
Public GitHub publication remains part of the certified DL release workflow;
this handoff does not publish the held v2.2.3 tag or certify that DL runtime.

## Artifact

Package source: `b2c8c5696a7e6b9bdcf2d1f97e94f837f5ba0fe8`.
Builder: `scripts/build_qdl_sdk_release.py` (existing deterministic builder).
Immutable handoff directory, outside Git:
`/home/bobby/.local/state/qdl-v2/sdk-2.1.0-b2c8c56/`.

- Wheel: `artifacts/qdl_sdk-2.1.0-py3-none-any.whl`.
- SHA256: `6923b46f46bd07c850b18a6074439bbcb4085d84428a8e72c476437dd51524e7`.
- Manifest: `artifacts/qdl_sdk-2.1.0.release.json`; SBOM: `artifacts/qdl_sdk-2.1.0.cdx.json`.
- SDK source digest: `f17c883fa78d0b77bd29830d74971ccc2b53c79c90a8d98e7fbcda040432be9d`.
- Contract digest: `2a25a601ae80b2ee4b36e6d1cb8ca44a13b8a037bf69a96f45659d1782a3a624`.
- Install receipt: `consumer-install-receipt.json` in that directory.

TS and alpha must consume the SAME wheel hash. Do not copy selected SDK files,
install a different artifact under version 2.1.0, or change live site-packages.
If the eventual one-SHA DL build changes package bytes, fail artifact parity and
review that change before promotion; do not overwrite this artifact/evidence.

## Verification

SDK source suite: `python -B -m unittest discover -s tests -p '*sdk*.py' -q`:
**89 PASS, zero failures/skips**, 11.467 seconds in existing Python image
`sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f`.
The wheel test builds twice and checks deterministic bytes plus every RECORD
digest/size, including generated protobuf files and absence of service internals.

Clean target installs used disposable containers, `--network none`, no credentials,
wheel mounted read-only, no source mount, `--no-index --no-deps`, and `/tmp/sdk-only`.
Python import caches were invalidated after installation; version AND module path
were asserted. Both generated query and market-data protobuf modules imported.
This is package compatibility, not a live consumer acceptance window.

| Consumer image | Result |
| --- | --- |
| TS `sha256:a6548ee36aeb0242c1734c071678c525c1506d05c10a70b533eb7110e7eaf16a` | PASS with existing dependencies |
| Alpha `sha256:a03c62f01504e024205c535b9a7fae0afcde1d504e1386c636d9a1ea5ea71089` | Existing PyJWT 2.13.0 is BELOW declared minimum; not dependency-compatible as-is |
| Same alpha image, disposable target with PyJWT 2.15.1 | PASS, including exact target module paths |

Passing dependency versions: grpcio 1.83.0, httpx 0.28.1, protobuf 6.33.6,
pydantic 2.13.4, PyJWT 2.15.1, cryptography 50.0.0. SDK declared version
specifiers were checked using `packaging.requirements.Requirement`.

The alpha correction is a build dependency input for Claude, NOT a change to the
running alpha or an excuse to lower the SDK requirement. Offline dependency:
`dependencies/pyjwt-2.15.1-py3-none-any.whl`, downloaded from PyPI without running
provider code, SHA256 `42d59d631f7768a1028a64c7ff581a9bf7519804daf91fc5b6c56e30eec5e193`.
Keep all other locked dependencies unchanged; do not broadly upgrade packages.

Initial smoke harness failures are retained in the receipt: alpha venv has no pip,
TS has no standalone packaging library, and importing immediately after target
installation initially selected the existing SDK due to import-cache state.
These were corrected in the isolated test method, not application code. No
success was claimed until imports came from the installed wheel's target path.

## Consumer Build Handoff

1. Verify both wheel hashes before adding artifacts to an isolated build context.
2. Pin qdl-sdk 2.1.0 by the exact artifact hash in TS and alpha build inputs.
3. In alpha only, update the locked PyJWT dependency to the tested 2.15.1 artifact;
   keep crypto extras and the other approved dependency locks consistent.
4. Run caller-specific tests and packaging probes on the resulting immutable
   image. Do not treat this install smoke as DR01, DR05 or DR08 acceptance.
5. Wait for Claude's stage-3 window. No deployed DR01 or final DR08 has run here.

All disposable install/test containers were `--rm`; no application images were
built, no shared Redis/DB/network/credentials were mounted, and no alpha was
started. The small artifact directory is the retained consumer deliverable, not
disposable build cache. BAR rollback `f7351c3b` remains retained.

# P18.4 Consolidated Build Preparation (IDR039)

Status: IMPLEMENTED / TESTED LOCALLY / RELEASE_HOLD, 2026-10-08.
This is recipe preparation, not an image build or CI/release certificate.
Main journal: [approved parent-owned scope](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md),
"Pre-stage3 consolidated build, SDK and DR05". Parent handles journal and commit;
neither was edited by this slice.

## Governing Update: Independent Closure, 2026-10-08

The TS build gate means completed market-data integration regressions, NOT
completion of Paper/OR features. Final DR/broker acceptance and exact-source DL CI
remain mandatory; existing receipts do not certify later source automatically.
The recipe now also requires DR05 fence d68fb2ff47622db8224dc4b9e004dca4e20f643e
and DR08 terminal-status 0b374bc918af02476cdfb1502d5df132426f901b, including the
changed core/projector/read-view paths and prolonged-stall regressions. Eight
prerequisite entries (2026-10-09: + IDR084_hot_mark 922a6ec, the only served-runtime change since 0b374bc, with its
kn_query_backend contract marker) and 24 source paths are inventoried. Older candidate d68fb2f
is refused because terminal-status ancestry is absent.

Affected portable suite: 20 PASS, 0 FAIL, 0 SKIP. The real Git preflight against
0b374bc validates ancestry/paths and remains HOLD (no CI or build attestation).
CI's expected gate/hold fields are updated together with the generator.
100-alpha capacity is a candidate workload, not a measured certification; see
owner amendment in the main plan. Fixed 5-vCPU acceptance is superseded by
measured adequate resources plus headroom, without relaxing correctness/latency.

## Authority and Scope

- [Phase C packet](p184-dl-phase-c-packet.md), governing owner amendment first.
- [Architecture](../DATA_LAYER_V2_KAFKA_NATIVE_ARCHITECTURE_REVIEW.md), section 18.13.
- IDR source read: /home/bobby/.worktrees/trading-system-next-upgrade/INTEGRATION_DEFECT_REGISTER.md,
  IDR-002, IDR-029, IDR-039, IDR-040 and IDR-075. IDR039 remains OPEN until
  the consolidated image set is built and certified; preparation does not close it.
- Own files: scripts/p184_consolidated_build.py, tests/test_p184_consolidated_build.py,
  this evidence, and only the new consolidated-source-preflight job in
  .github/workflows/ci.yml (parent-review authorization). No existing CI job,
  Dockerfile, manifest or runtime change.

## Recipe Contract

The generator reads committed Git objects, never working-tree application files.
It requires a full lowercase 40-character commit SHA, prerequisite ancestry and
source/path markers. It records source Git blob IDs. No abbreviated SHA, branch
or mutable tag is accepted.

| Required inclusion | Immutable ancestor | Meaning preserved |
| --- | --- | --- |
| IDR040fix1 | 4eb08dc15113cc4f73b65b239dfeb4f8ec0e23cf | REST ahead of stream is refused; no fresh timestamp manufacture |
| bindingV3 | same 4eb08dc ancestor | Sealed per-product request policy |
| quota log | same 4eb08dc ancestor | Bounded exhaustion logging, no quota increase |
| b6a0119 | b6a01190587e8771c62b5b4988782b06c5e2578a | Offered/dequeued counters, not client-apply proof |
| IDR075 | 704552d8736905cdb73feec86f3f01d339d02cbb | Preparation only, no entitlement activation |

Future build commands use ONE private git archive context for BOTH images:
qdl-v2-python via Dockerfile and qdl-v2-rust via Dockerfile.qdl-rust-runtime.
Both use verified existing QDL_GIT_SHA and QDL_RELEASE arguments and tags with
the full application SHA. No overlay, source bind mount, named build context,
per-role image or mixed application source is supplied. The Rust Dockerfile
includes core, native ingestor, projector and stream gateway binaries.
Commands capture image IDs through --iidfile, assert OCI revision labels against
the source SHA, print archive SHA256/image IDs/labels, and remove only their own
temporary archive directory. Future build logs must be retained externally.
A partial build failure does not yield a certified image set.

Python optionally uses --dependency-image REPOSITORY@sha256:DIGEST through the
existing QDL_DEPENDENCY_IMAGE argument. Only /opt/venv is reused; application files
remain from the archive. The existing verifier checks locked versions and RECORD
hashes. Default builder performs a clean dependency build. The main plan records
historically withdrawn vnstock4.0.2/vnai2.4.8: no availability was tested here.
An approved dependency image must be accessible to the future builder; local-only
images may need an independently approved distribution path. No dependency image
was selected, exported, retagged or pulled here.

## Explicit Gates and CI Handoff

Standard-library-only generator and unittest suite work offline. Full Git history
containing all prerequisite objects is required in CI; shallow history fails closed.
Parent review identified that the original real-Git unit tests were incompatible
with the Docker unittest image (.git excluded; git absent) and shallow checkout.
The revised unit suite mocks Git and invokes the CLI in-process with mocked argv
and source checks. A suite-wide Popen guard rejects any attempted process launch;
no Git binary, checkout, Bash or third-party test dependency is required.
prepare() validates the full source SHA before calling even a mocked source checker.

The sole ci.yml addition is consolidated-source-preflight: pinned checkout action,
ref github.sha, fetch-depth: 0, pinned Python setup, portable stdlib tests, then
real committed-source CLI preflight against GITHUB_SHA. It checks checkout HEAD
matches that SHA and requires exit 2 PLUS empty stderr and valid JSON containing
the exact HOLD status, pending gates, three hold reasons, prerequisite roster,
source blob inventory, both same-SHA images, and null build script/hash.
Argument/source failures also exit 2, so exit status alone is never accepted.
No continue-on-error or silent error catch. No build, push, deploy or runtime
operation in the added job. Existing jobs and release.yml remain unchanged.
A successful source preflight is not an exact-source full DL CI certificate.

Preparation with gates pending (JSON HOLD, exit 2, no executable build script):

```sh
python3 -B scripts/p184_consolidated_build.py \
  --source-commit FULL_DL_COMMIT --release v2.2.3
```

Only after TS market-data integration tests pass, exact-source DL CI passes, and the owner
approves the future build, provide all explicit inputs:

```sh
python3 -B scripts/p184_consolidated_build.py \
  --source-commit FULL_DL_COMMIT --release v2.2.3 \
  --ts-tests-status pass --ts-source-commit FULL_TESTED_TS_COMMIT \
  --ts-tests-receipt-sha256 TS_COMPLETED_RECEIPT_SHA256 \
  --dl-ci-status pass --dl-ci-receipt-sha256 EXACT_DL_COMMIT_CI_RECEIPT_SHA256 \
  --build-approval approved
```

This still ONLY prints JSON, with PREPARED_NOT_EXECUTED and a future build_script.
Gate receipts are explicit operator attestations, not remote CI verification.
Do not use placeholder digests as acceptance evidence. The parent must review the
receipt contents and their commit association; this tool checks digest syntax only.
Never pipe preparation output into execution as part of this source-only task.

## Same-Source SDK Promotion Gate

Documentation-only gate; no SDK rebuild or image build performed in this slice.
The parent-owned [SDK 2.1.0 consumer handoff](qdl-sdk-2.1.0-consumer-handoff.md)
is authoritative for the consumer artifact and compatibility limitations:

- Package source: b2c8c5696a7e6b9bdcf2d1f97e94f837f5ba0fe8.
- Wheel: qdl_sdk-2.1.0-py3-none-any.whl.
- Wheel SHA256: 6923b46f46bd07c850b18a6074439bbcb4085d84428a8e72c476437dd51524e7.

Before promotion, rebuild the SDK using scripts/build_qdl_sdk_release.py from
the SAME archived final full SHA used for BOTH consolidated application images.
Run that archived script with --output-dir pointing to a NEW isolated directory
outside the archive and outside the immutable consumer handoff directory.
Do not invoke a builder from the current working tree or overlay SDK sources.
Record the final source SHA, archive hash, new wheel SHA256, release manifest and
SBOM identifiers, and compare the new wheel bytes/hash to the exact consumer
artifact above. Require exact wheel parity before promotion; matching version
2.1.0 alone is insufficient. Preserve the existing TS/alpha handoff unchanged.

Any wheel drift is a promotion HOLD: preserve both artifacts and receipts,
investigate the source/build difference, and obtain parent review plus a new
explicit consumer handoff/compatibility decision before proceeding. Never
overwrite, relabel or silently replace the old wheel or its evidence. SDK parity
does not waive the documented alpha PyJWT dependency gate or DR/broker gates.
The recipe generator does not automate or attest this SDK promotion gate.

## Verification and Limits

Executed: python3 -B -m unittest tests.test_p184_consolidated_build -v.
Revised result: 18 PASS, 0 FAIL, 0 SKIP. All Git and CLI source checks mocked;
includes independent pending gates, malformed digests/statuses, mutable refs,
full-SHA validation even with mocked source checker, two-image single-context
contract, dependency pin validation, and CLI source error vs HOLD output.
All tests block subprocess creation. Earlier 14/16-test passes were local
checkout evidence only, not evidence of Docker/CI portability.

Portability rehearsal: copied only the recipe and test into a TemporaryDirectory,
set PATH='' and PYTHONPATH='', then invoked absolute sys.executable with
-B -m unittest discover -s tests -p test_p184_consolidated_build.py -v:
18 PASS, 0 FAIL, 0 SKIP with no .git, Git or Bash. Temporary copy auto-removed.

Executed the exact inline Python preflight from the new job locally against
52642e52e01981590bc433bf95ac12d4e941cf10: PASS with build HOLD.
Negative harness checks: source error exit2, malformed JSON object exit2 and
unexpected exit0 all rejected (3/3). This is local rehearsal, NOT hosted CI.
Real committed-source checks now belong to the dedicated full-history job, not
Docker unittest discovery. No Docker command was executed.
git diff --check passed; no-index --check on new code/test files had no whitespace
diagnostics (exit 1 means files differ from /dev/null).

Not run: CI, Docker packaging/linkage, Rust compilation, provider/DR/broker tests,
image scans, registry operations or runtime certification. Marker/ancestry checks
do not establish semantic correctness or exclude later reverts; exact-source CI
and packaged-image verification remain required. Base package resolution can vary
despite one application SHA. Image IDs are local content IDs, not registry manifest
digests. IDR040fix2 is deferred and IDR075 remains prepared, not activated.
No release readiness or CI-green claim is made.

## Runtime, Cleanup and Handoff

Canonical source: /home/bobby/data_layer, main,
5bf5cd540498b82959342c7cfb2ebeaac3007426; stable release v2.2.2 per main journal.
Active feature: /home/bobby/.worktrees/data-layer-p184-closure,
fix/p184-dl-closure. Observed HEAD advanced during concurrent parent work from
351cc72c300d7a3174627f322eb5accc6447947c to
b2c8c5696a7e6b9bdcf2d1f97e94f837f5ba0fe8; this slice made no commit.
Parent advanced the feature to 52642e52e01981590bc433bf95ac12d4e941cf10
during the review; local CI-block rehearsal checked this revision.
Uncommitted recipe/test/CI edits are not claimed to be in any built image.

No runtime/Docker access or mutation, network request, push, merge or workflow
dispatch. Active service set, image digests and config revision were NOT
re-inspected; inherit only the packet's historical inventory, not a fresh claim.
Named BAR rollback f7351c3b remains protected per parent scope; other active and
rollback artifacts untouched. No test images/containers/caches created. The no-Git rehearsal used only a
TemporaryDirectory, auto-removed on completion; no retained test files. No Docker
cleanup or disk-capacity measurements were performed. Docker/BuildKit inventory and cleanup intentionally
deferred because this is source-only preparation, not image-phase closure.
Concurrent DR05 and parent work were left untouched.

Next permitted step: parent review, journal update and eventual commit; obtain
final TS market-data integration and exact-source DL CI receipts before separately authorizing
a consolidated rebuild. Building does not authorize deployment. DR/broker gates
and paired IDR075 activation still need the reviewed runtime packet and approval.

# R0-R2 Rust Authority Convergence Runbook

## Scope

This runbook implements the approved `R0`, `R1`, and `R2` closure recorded in
[`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md).
It is a narrow twelve-slice promotion for Binance USD-M and OKX Swap. It does
not change V1 endpoints or schemas, does not reset Kafka offsets, and does not
promote DNSE.

V1 remains the rollback route throughout. The current C40 attempt is retained
as immutable evidence; it is terminalized rather than deleted.

## Invariants

1. One canonical writer per selected slice: generic `rust_core` excludes all
   twelve promoted bindings before a `production_core_*` canary starts.
2. A new image requires a new candidate digest. No direct rewrite of
   `qdl_authority_slices` provenance is permitted.
3. The R1 candidate rollover is legal only from `BLOCKED`, is a full CAS over
   state/revision/owner/lease/plan/candidate/image, retains append-only
   old/new snapshots, and publishes one durable control event.
4. The production core uses a new consumer group and a signed tail cursor.
   It uses `auto.offset.reset=error`; it can seed only currently uncommitted
   assigned partitions to the signed tail. It never uses `earliest`, `latest`,
   `seek`, group reset, or manual offset mutation.
5. A block packet may honestly carry `UNKNOWN` provenance so an unsafe canary
   can always be fenced. `REVALIDATE`, `CANARY`, and `PRIMARY` require `REAL`
   provenance and zero semantic mismatch, open gap, duplicate external effect,
   and consumer error.
6. The R2 handoff stores terminal watermark `W` first, then only allows the
   primary CAS when every selected slice begins at `W + 1` with the same
   candidate/plan/owner lineage.

## R0 - Source And Artifact Gate

Run from the reviewed feature worktree. Do not build from an unreviewed or
dirty checkout.

```bash
cd /home/bobby/.worktrees/quant-data-layer-v2-authority
git diff --check
docker run --rm --network none --user 0:0   -e PYTHONDONTWRITEBYTECODE=1 -v "$PWD:/app:ro" -w /app   qdl-v2-python:2.0.0-31c8ca5   python -m unittest -v     tests.test_phase92_bootstrap_cursor     tests.test_phase_r1_candidate_rollover     tests.test_phasec3_cutover_packet     tests.test_stable_runtime_refresh     tests.test_phaseb_stable_deployment     tests.test_phaseb_stable_edge
```

The Rust gate must run in a disposable container with a temporary target
folder, followed by the isolated PostgreSQL migration tests for
`0006/0007/0009/0011` and the `BLOCKED`-only rollover function:

```bash
scripts/phasec3_authority_db_smoke.sh
scripts/phase_r1_candidate_rollover_smoke.sh
```

Both tests must pass before an immutable image is built.
No V1, Kafka, Redis, authority row, or runtime bundle changes occur in R0.

## R1.1 - Build And Prepare A New Candidate

1. Commit the reviewed R0 source on its feature branch.
2. Build a new immutable Rust image from that commit and retain the current
   active generic-core image as rollback.
3. Clone the active bundle to a new private release directory with the R1
   bundle tool. Do not use `phaseb_prepare_stable_candidate.py`: it rotates
   authority DB credentials and workload identities. This tool preserves those
   values, creates only a new signed-bootstrap key/group, and writes new
   generic/production-core runtime JSON.

```bash
python scripts/phase_r1_prepare_release_bundle.py \
  --source-bundle "$QDL_CURRENT_RELEASE_ROOT" \
  --source-env "$QDL_ACTIVE_ENV_WITH_ROTATED_IDENTITIES" \
  --output-bundle "$QDL_R1_RELEASE_ROOT" \
  --rust-image-id "$QDL_R1_RUST_IMAGE" \
  --rollback-rust-image-id "$QDL_ACTIVE_RUST_IMAGE" \
  --source-commit "$QDL_R1_SOURCE_COMMIT" \
  --apply --confirm PREPARE_QDL_R1_RELEASE_BUNDLE
```

   `--source-env` is mandatory and must name the actual active env inside the
   source bundle. The clone carries every referenced `identities*` and
   `cert-material*` lineage directory, rewrites those bundle-local paths, and
   removes historical Compose overrides so an old C39/C40 image cannot be
   silently reintroduced. The generic-core bundle must exclude the twelve
   promoted bindings and each production-core config must mount
   `production-bootstrap.json` read-only.
4. Collect fresh read-only twelve-slice reference parity from the currently
   running fenced Rust image, then create a typed R1 pre-canary admission bound
   to that reference, the new image inspect/revision, and the new R1 release
   artifact. This evidence is deliberately `PENDING_R1_CANARY`: it proves
   current provider/contract health and candidate provenance, not candidate
   runtime output. It is valid for the bounded canary only and never for R2.

```bash
python scripts/phase_r1_prepare_precanary_admission.py \
  --release-artifact "$QDL_R1_RELEASE_ROOT/release/artifact-manifest.json" \
  --reference-parity "$QDL_R1_RELEASE_ROOT/review/reference-live-parity.json" \
  --candidate-image-id "$QDL_R1_RUST_IMAGE" \
  --output "$QDL_R1_RELEASE_ROOT/review/precanary-admission.json"
```

5. Build a fresh C40-compatible bootstrap candidate packet from the R1
   runtime production-core manifest, new SBOM/rollback artifacts and that typed
   admission. This writes a packet only. The admission expires quickly and
   fails closed on a stale reference, wrong image/commit, non-root image,
   candidate/rollback image reuse, incomplete 12-slice parity, or any dirty
   count.
6. Apply migration `0011_authority_candidate_rollover.sql` to the isolated
   authority database only after its dry-run/checksum matches the reviewed
   commit. It is additive and has no V1 table/data mutation.

## R1.2 - Terminalize C40, Rollover, Revalidate And Canary

All packets are first generated/read-only, inspected, and only then applied
with their printed confirmation token. These paths never reset Kafka.

```bash
# 1. C40 canary -> BLOCKED. The bootstrap file identifies exactly 12 slices.
python scripts/phase_r1_prepare_transition.py   --stage BLOCK_CANARY --bootstrap /secure/qdl/c40/bootstrap.json   --actor BobbyAxerol --change-ticket QDL-R1-001   --output /secure/qdl/r1/block-c40.json
python scripts/phasec3_authority_cutover.py --packet /secure/qdl/r1/block-c40.json
# Apply only after reviewing APPLY_C3_<digest>.

# 2. New candidate rollover. It inserts/validates the new prerequisite bundle
# and invokes the blocked-only DB CAS. It does not change Kafka offsets.
python scripts/phase_r1_candidate_rollover.py prepare   --bootstrap /secure/qdl/r1/new-bootstrap.json   --actor BobbyAxerol --change-ticket QDL-R1-001   --output /secure/qdl/r1/rollover.json
python scripts/phase_r1_candidate_rollover.py apply   --packet /secure/qdl/r1/rollover.json
# Apply only after reviewing APPLY_R1_ROLLOVER_<digest>.

# 3. BLOCKED -> VALIDATING, then VALIDATING -> RUST_CANARY.
python scripts/phase_r1_prepare_transition.py   --stage REVALIDATE --rollover-packet /secure/qdl/r1/rollover.json   --actor BobbyAxerol --change-ticket QDL-R1-001   --output /secure/qdl/r1/revalidate.json
python scripts/phase_r1_prepare_transition.py   --stage CANARY --rollover-packet /secure/qdl/r1/rollover.json   --actor BobbyAxerol --change-ticket QDL-R1-001   --output /secure/qdl/r1/canary.json
```

After the canary control event is durably published, issue the signed bootstrap
cursor using the new production group. The issuer reads Kafka metadata and
committed offsets only; it refuses a nonempty group and does not call an offset
reset API.

```bash
python scripts/phase92_issue_bootstrap_cursor.py   --env-file "$QDL_RELEASE_ROOT/stable.env"   --candidate-digest <new-candidate-digest> --generation 1
# Review the dry-run. Apply only with ISSUE_QDL_PHASE92_BOOTSTRAP_CURSOR.
```

Start only `production_core_1..3` from the new immutable image. The generic
cores, V1, Kafka, Redis, V1 container and Trading System are not restarted by
this step. Record a bounded 5-10 minute real-provider canary from all twelve
slices with the existing live parity and handoff collectors.

R1 acceptance requires all of:

- signed cursor starts with `SEEDED` or `RESUME_STORED`, never a fallback reset;
- no historical replay before the signed high-watermark tails;
- 12/12 authentic Binance/OKX slices with zero semantic/provenance/gap/duplicate
  mismatch and contiguous target watermarks;
- canary-only target writes; public/V1 compatibility writes remain zero;
- bounded Kafka lag, CPU, memory and disk; V1 and Trading System health remain
  unchanged.

Rollback at R1: stop only `production_core_1..3`, apply `BLOCK_CANARY` if
needed, retain cursor/checkpoint/control evidence, and continue on V1.

## R2 - Terminal Handoff And Rust Primary

Use live R1 canary evidence plus the current authority rows to create both the
append-only terminal/handoff evidence and the C3 `PRIMARY` packet. The generator
binds checkpoint `binding_id` and `shard_id` to the exact rollover
`partition_id`, not a native-symbol heuristic.

```bash
python scripts/phase_r2_prepare_primary.py   --rollover-packet /secure/qdl/r1/rollover.json   --live-evidence /secure/qdl/r1/live-handoff.json   --actor BobbyAxerol --change-ticket QDL-R2-001   --output-dir /secure/qdl/r2

python scripts/phasec40_prepare_cutover.py apply-evidence   --packet /secure/qdl/r2/terminal-handoff.json
# Apply only after reviewing APPLY_C40_HANDOFF_<digest>.

python scripts/phasec3_authority_cutover.py   --packet /secure/qdl/r2/primary-cutover.json
# Apply only after reviewing APPLY_C3_<digest>.
```

R2 acceptance requires exactly one `RUST_PRIMARY` authority per selected slice,
first public/canonical compatibility watermark `W + 1`, zero duplicate/gap,
Python projection preserves V1 bytes, demanded Trading System slices remain
ready, and the V1 fallback route remains callable and fresh.

Rollback at R2: fence/block Rust first, retain final watermark and all append-
only evidence, use the existing `ROLLBACK_PENDING -> PYTHON_PRIMARY` handoff
only with a new accepted exact packet. Never restart Python as an uncoordinated
writer.

## Cleanup

Retain active V1, the active R1/R2 image, and one verified rollback image. Only
remove reference-audited disposable test containers, temporary test databases,
failed-build tags and BuildKit cache. Do not remove Kafka, Redis, SQLite,
certificate, runtime bundle, V1, authority or terminal-evidence volumes.

# V2 Production And Rust Authority Cutover Runbook

## Purpose

This runbook moves Data Layer consumers from the current V1 service to stable
V2 and promotes Rust as canonical realtime authority without creating two
writers. It implements Phase C in
`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`.

This is a gated procedure, not one shell script. Stop at every approval point.
DNSE is excluded until its separate provider gate passes.

## Current Baseline

- V1: `data-layer:v0.1.0`, loopback port `8100`, production authority.
- Stable V2 query ports: `18201`, `18202`.
- Stable V2 stream HTTP ports: `18210`, `18211`.
- Stable V2 stream gRPC ports: `18220`, `18221`.
- Stable project: `qdl_v2_stable_candidate`.
- Initial V2 authority: `RUST_SHADOW`.
- Initial venues: Binance and OKX only.
- V1, current Redis and current provider processes are not restarted by the
  merge or isolated-deploy steps.

## Gate 0 - Merge The Certified Feature Branch Into Dev

The assistant prepares and tests the feature branch. The operator reviews and
merges it; the assistant never merges `dev` or `main` without explicit
approval.

```bash
cd /home/bobby/data_layer
git status --short
git diff --check
git log --oneline dev..feat/v2-stable-rust-binance-okx
git push -u origin feat/v2-stable-rust-binance-okx
```

Open and verify the pull request:

```bash
gh pr create \
  --base dev \
  --head feat/v2-stable-rust-binance-okx \
  --title "feat(data-layer): release stable V2 Rust multivenue core" \
  --body-file docs/runbooks/v2-production-rust-authority-cutover.md
gh pr checks --watch
```

Review gates:

- all required CI checks green;
- no secret, runtime state, provider SDK snapshot or generated cache tracked;
- the PR contains Phase B evidence and this Phase C plan;
- V1 compatibility tests remain green;
- no merge targets `main`.

After the operator merges the PR in GitHub:

```bash
cd /home/bobby/data_layer
git switch dev
git fetch origin
git pull --ff-only origin dev
git status --short
git switch -c feat/v2-production-authority-cutover
```

Stop and record the new branch SHA. Do not build production artifacts from the
old unmerged feature worktree.

## Gate 1 - Production Authority Wiring

The stable runtime currently accepts only `RUST_SHADOW`. Before any production
route changes, the new feature branch must implement and test:

1. PostgreSQL authority CAS plus immutable handoff/audit records.
2. A transactional authority outbox written in the same database transaction.
3. An idempotent dispatcher to the compacted Kafka authority topic.
4. Rust startup/restart reconstruction from authority plus target watermarks.
5. Phase 9.2 sink fencing on every canonical/public/compatibility durable write.
6. Operator CLI commands for status, preflight, canary, promote, block and
   rollback.
7. Fail-closed handling for stale owner, revision, lease, partition plan,
   missing handoff and incomplete target recovery.

Mandatory tests include migration idempotency, outbox retry/crash recovery,
compacted-topic rebuild, stale-writer races, broker loss, process restart,
W/W+1 handoff, rollback, exact market-data parity and V1 compatibility.

Do not continue while the stable binary still rejects `RUST_CANARY` or
`RUST_PRIMARY`, or while authority can be changed by environment variable
alone.

## Gate 2 - Build Immutable Artifacts

After Gate 1 is committed and CI-green:

```bash
cd /home/bobby/data_layer
export RELEASE_SHA="$(git rev-parse --short=12 HEAD)"
export PYTHON_IMAGE="qdl-v2-python:2.0.0-$RELEASE_SHA"
export RUST_IMAGE="qdl-v2-rust:2.0.0-$RELEASE_SHA"

docker build --pull \
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  -t "$PYTHON_IMAGE" .
docker build --pull -f Dockerfile.phase8-rust \
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  -t "$RUST_IMAGE" .

docker image inspect "$PYTHON_IMAGE" "$RUST_IMAGE" \
  --format '{{.RepoTags}} {{.Id}}'
```

Run the full Python/Rust/contract/security suites and generate SBOM, provenance
and image-digest evidence. Retain V1 and one tested rollback image pair. Remove
only exact failed-build tags and project-scoped test resources.

## Gate 3 - Prepare The Isolated Stable Bundle

Use a fresh private runtime directory and existing approved Kafka client CA.
Never commit `stable.env`, private keys or the generated bundle.

```bash
export QDL_RELEASE_ROOT="/home/bobby/.local/state/qdl-v2/$RELEASE_SHA"
install -d -m 0700 "$QDL_RELEASE_ROOT"

python scripts/phaseb_prepare_stable_candidate.py \
  --python-image "$PYTHON_IMAGE" \
  --rust-image "$RUST_IMAGE" \
  --cert-dir /path/to/approved/phase8-certificates \
  --output-dir "$QDL_RELEASE_ROOT"
```

The manifest must report contract `2.0.0`, authority `RUST_SHADOW`,
`cutover_authorized=false`, immutable image IDs, five consumer manifests and
no recorded secret values.

## Gate 4 - Start Isolated V2

Start only dedicated infrastructure first:

```bash
docker compose \
  --env-file "$QDL_RELEASE_ROOT/stable.env" \
  -f docker-compose.v2-stable.yml \
  up -d kafka1 kafka2 kafka3 stable_redis stable_state_init stable_tls_init

python scripts/phaseb_bootstrap_stable_broker.py \
  --env-file "$QDL_RELEASE_ROOT/stable.env"
```

Then start Binance/OKX acquisition, Rust core and Python projection/API roles.
Do not enable profile `stable-vn`.

```bash
docker compose \
  --env-file "$QDL_RELEASE_ROOT/stable.env" \
  -f docker-compose.v2-stable.yml \
  up -d \
  rust_core rust_core_2 rust_core_3 \
  ingestor_binance_usdm ingestor_binance_spot \
  ingestor_okx_swap ingestor_okx_spot \
  binance_bar_edge \
  stream_v2_active stream_v2_passive projector_v2 \
  query_v2_1 query_v2_2
```

Acceptance:

```bash
curl --fail --silent http://127.0.0.1:18201/health/ready
curl --fail --silent http://127.0.0.1:18202/health/ready
curl --fail --silent http://127.0.0.1:18210/health/ready
curl --fail --silent http://127.0.0.1:18211/health/ready
curl --fail --silent http://127.0.0.1:8100/v1/health
```

Require authentic Binance/OKX data, zero unexplained gap/duplicate/quarantine,
bounded broker/projector lag, exact replica results and V1 unchanged.

Rollback before consumer migration:

```bash
docker compose \
  --env-file "$QDL_RELEASE_ROOT/stable.env" \
  -f docker-compose.v2-stable.yml \
  down
```

Do not add `-v`; preserve evidence until the failed gate is understood.

## Gate 5 - Paper Consumer Canary

Move only explicitly named manifests, in this order:

1. monitoring;
2. one paper alpha;
3. Trading System paper market-data adapter;
4. remaining approved paper consumers.

For each consumer, record V1 cursor/watermark, V2 warmup result, first live
cursor, restart cursor, freshness, gaps, duplicates, source/session status and
rollback route. No sandbox/live order path is included.

Rollback changes only that consumer endpoint/SDK config back to V1.

## Gate 6 - Exact-Slice Authority Approval

Before any `RUST_CANARY` or `RUST_PRIMARY` transition, produce this packet:

```text
release_sha:
python_image_id:
rust_image_id:
slice_id:
binding_ids:
old_owner:
new_owner:
authority_revision:
lease_epoch:
partition_plan_epoch:
terminal_watermark_W:
authority_topic:
raw/canonical/public/compatibility topics:
consumer_groups:
V2_ports:
volumes:
secret_references:
affected_consumers:
canary_hold:
primary_hold:
rollback_command:
operator:
change_ticket:
```

The operator must explicitly approve this exact packet.

The transition follows only:

```text
PYTHON_PRIMARY
  -> RUST_SHADOW
  -> RUST_CANARY
  -> RUST_PRIMARY
```

The old writer is fenced at `W`; Rust reconstructs every required target
through `W` and first publishes at `W+1`. Any failed gate enters `BLOCKED`
and rolls back using a newer authority revision and accepted reverse handoff.

## Gate 7 - Expand And Release

Hold the first primary slice for the approved window. Expand one venue/feed
slice at a time; no inherited certification. Keep V1 available until every
registered consumer has migrated and rollback has been exercised.

Only then may the operator approve stable V2 public routing, a V1 sunset date,
a release PR `dev -> main`, and tag/release publication. DNSE remains disabled
until its provider-specific external gates pass.

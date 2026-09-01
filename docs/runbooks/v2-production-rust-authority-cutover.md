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
- Operator-declared active consumer: Trading System only; all alphas are down.
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

Gate 1 is implemented on the feature branch, but no production CAS has
executed. The deployable topology reuses migrations 0006/0007/0009 and adds
migration 0010, a dedicated non-public PostgreSQL authority database,
function-scoped dispatcher role, transactional outbox dispatcher, compacted
authority/checkpoint topics, per-principal ACLs, and three bounded
qdl-production-core workers behind stable-authority and
stable-authority-primary profiles.

The stable binary reconstructs authority and every target checkpoint before
reading raw input. Missing, partial, stale or wrong-owner state fails closed.
Only RUST_CANARY writes the canary topic; only RUST_PRIMARY writes canonical,
public V2 and legacy compatibility topics.

Verification completed before immutable build:

- focused topology/outbox/operator tests: 23/23 passed;
- disposable network-none PostgreSQL bootstrap and least-privilege smoke: pass;
- full Python suite: 546 passed, 6 environment skips;
- Rust workspace: 70 passed; fmt and strict Clippy passed;
- isolated real-provider SDK acceptance: Binance and OKX query/stream/cursor
  parity passed while V1 remained HTTP 200.


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
  --output-dir "$QDL_RELEASE_ROOT" \
  --consumer-network executor_network
```

The manifest must report contract `2.0.0`, authority `RUST_SHADOW`,
`cutover_authorized=false`, immutable image IDs, five consumer manifests and
no recorded secret values. `--consumer-network` must name an already-created
external network shared with the sole approved consumer. Only V2 query/stream
ingress joins it; Kafka, Redis, projector and Rust cores remain private.

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
TLS_CA="$QDL_RELEASE_ROOT/identities/trading-system/ca.crt"
TLS_CERT="$QDL_RELEASE_ROOT/identities/trading-system/client.crt"
TLS_KEY="$QDL_RELEASE_ROOT/identities/trading-system/client.key"

curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
  https://localhost:18201/health/ready
curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
  https://localhost:18202/health/ready
curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
  https://localhost:18210/health/live
curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
  https://localhost:18211/health/live
curl --fail --silent http://127.0.0.1:8100/v1/health

if curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
    https://localhost:18210/health/ready >/dev/null; then
  GRPC_TARGET=localhost:18220
else
  curl --fail --silent --cacert "$TLS_CA" --cert "$TLS_CERT" --key "$TLS_KEY" \
    https://localhost:18211/health/ready >/dev/null
  GRPC_TARGET=localhost:18221
fi

docker run --rm --network host \
  -e QDL_STABLE_JWT_PRIVATE_KEY_FILE=/bundle/identities/trading-system-jwt/private.key \
  -e QDL_STABLE_JWT_KEY_ID=stable-trading-system-rs256-v1 \
  -v "$PWD:/workspace:ro" -v "$QDL_RELEASE_ROOT:/bundle:ro" \
  "$PYTHON_IMAGE" python /workspace/scripts/phasec1_isolated_consumer_acceptance.py \
  --grpc-target "$GRPC_TARGET" \
  --tls-ca-file /bundle/identities/trading-system/ca.crt \
  --tls-certificate-file /bundle/identities/trading-system/client.crt \
  --tls-private-key-file /bundle/identities/trading-system/client.key
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

## Gate 5 - Trading System Dual-Read And Route Switch

Do not start or migrate an alpha. Keep Trading System on V1 while its adapter
performs a bounded read-only comparison against V2 for the same Binance/OKX
instruments and closed-bar/event boundaries.

Require exact identity, timestamp, decimal, unit, finality and session semantics;
zero unexplained gap/duplicate; bounded freshness/lag; and successful signed
cursor replay/restart. Built-in health and authority telemetry must be green.

Then perform one controlled Trading System adapter restart with venue-aware
routing:

```text
Binance/OKX: V2 primary -> governed V1 fallback
DNSE:        V1 only
```

The exact adapter config keys and restart command are populated from the current
Trading System deployment during the cutover preflight; do not invent or
hardcode them in advance. Every fallback/return transition records source,
reason, watermark and operator identity. V1 fallback is accepted only when its
freshness/session/contract checks pass; otherwise execution remains blocked.

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

The operator must explicitly approve this exact packet. One packet may enumerate
all approved Binance/OKX slices for one maintenance window, but the controller
executes and audits each CAS independently.

Every slice follows only:

```text
PYTHON_PRIMARY
  -> RUST_SHADOW
  -> RUST_CANARY
  -> RUST_PRIMARY
```

The canary gate is bounded by accepted real events and continuity evidence, not
an arbitrary multi-day wait. The old writer is fenced at `W`; Rust reconstructs
all required targets through `W` and first publishes at `W+1`. A failed slice
enters `BLOCKED` and rolls back under a newer revision without undoing an
unrelated healthy slice.

The source-owned command is plan-only by default. Every packet contains exactly
one state step for 1..32 unique slices, expires, binds image/contract/partition/
route digests, requires clean real-provider evidence and includes an executable
Trading System V1 rollback command.

    python scripts/phasec3_authority_cutover.py \
      --packet /secure/qdl-v2/change/canary-packet.json

Review the printed APPLY_C3_<digest> token. Apply only the same packet bytes:

    QDL_CONTROL_ADMIN_DSN='postgresql://...' \
    python scripts/phasec3_authority_cutover.py \
      --packet /secure/qdl-v2/change/canary-packet.json \
      --apply --confirm APPLY_C3_<digest>

Start control services before canary. Start production workers only after the
RUST_CANARY authority event is durable:

    docker compose --env-file "$QDL_RELEASE_ROOT/stable.env" \
      -f docker-compose.v2-stable.yml --profile stable-authority \
      up -d stable_authority_db authority_outbox_v2

    docker compose --env-file "$QDL_RELEASE_ROOT/stable.env" \
      -f docker-compose.v2-stable.yml \
      --profile stable-authority --profile stable-authority-primary \
      up -d production_core_1 production_core_2 production_core_3

The command checks the current DB row under lock and executes one transaction
per slice, stopping on the first stale CAS. Primary and Python restore use the
accepted qdl_transition_authority_v2 handoff; no environment label can promote
authority.


## Gate 7 - Close With V1 Hot Fallback

After approved Binance/OKX slices are `RUST_PRIMARY`, Trading System uses V2
normally and V1 remains live at port `8100` as the tested fallback. Exercise
one V2 -> V1 -> V2 route drill with durable source-switch audit and no market
semantic mismatch.

There is no alpha migration and no V1 sunset in this cutover. Publish the V2
release only after the Trading System cycle, authority audit, cursor/replay
continuity and fallback drill pass. DNSE remains V1-only until its provider
gate passes.

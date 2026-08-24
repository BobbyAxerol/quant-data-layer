# Phase 10.3 Shared Rust-Primary Handoff

## Purpose

This runbook is the only operator procedure for the Phase 10.3 shared
Rust-primary realtime handoff. It replaces the obsolete
`production_core_*`/Phase-9 ceremony for this path.

It is deliberately narrow:

- one topic: `md.raw.realtime.v2`;
- one fixed shared core group: `qdl-v2-realtime-core-v2`;
- two native acquisition edges plus the Binance closed-bar edge;
- three core replicas, three projectors, two query replicas and two stream
  replicas;
- V1 remains running and is the rollback route.

It does **not** start `production_core_*`, create a per-symbol image,
container or topic, reset/seek Kafka offsets, delete a topic, flush Redis or
SQLite, restart V1, rewrite alpha configuration, change order execution, or
activate VN. VN needs a separate verified in-session provider admission.

## Preconditions

The operator must have a newly generated, unexpired review packet from
`scripts/phase103_prepare_shared_primary_packet.py`. The packet must name:

1. the exact Rust and Python image SHA256 IDs;
2. the host runtime directory ending in `/runtime`;
3. the `RUST_PRIMARY` authority revision and configuration revision;
4. all 13 V2 service names listed below;
5. a 300-second acceptance window; and
6. the packet SHA256 and confirmation token.

The separate approval must quote those values and explicitly allow only the
following blast radius:

```text
Kafka: CREATE_OR_VERIFY_ONLY md.raw.realtime.v2 and the sealed phase8-producer/
       phase8-core ACLs.
V2 services: ingestor_binance_usdm, ingestor_okx_swap, binance_bar_edge,
             rust_core, rust_core_2, rust_core_3,
             projector_v2, projector_v2_2, projector_v2_3,
             query_v2_1, query_v2_2, stream_v2_active, stream_v2_passive.
V1, Kafka offsets, Redis, SQLite, PostgreSQL, alpha configuration, order paths,
VN edge, all volumes and all other services: unchanged.
Rollback: V1 manifest route plus stop only the same 13 V2 services.
```

No approval means review-only commands only.

## Review-Only Preparation

Run these commands from the canonical Data Layer checkout. They do not contact
Docker, Kafka, Redis, PostgreSQL, providers or consumers.

```bash
cd /home/bobby/data_layer

export QDL_PACKET_DIR=/home/bobby/.local/state/qdl-v2/phase103-shared-primary-$(date -u +%Y%m%dT%H%M%SZ)
export QDL_RUNTIME_DIR="$QDL_PACKET_DIR/runtime"

python -B scripts/phase103_prepare_shared_primary_packet.py \
  --output-dir "$QDL_PACKET_DIR" \
  --host-runtime-dir "$QDL_RUNTIME_DIR" \
  --rust-image-digest 'sha256:<approved-rust-image-id>' \
  --python-image-digest 'sha256:<approved-python-image-id>' \
  --source-commit '<commit-used-to-build-those-images>' \
  --actor BobbyAxerol \
  --change-ticket QDL-PHASE103-HANDOFF \
  --observation-seconds 300

python -B scripts/phase103_validate_shared_primary_packet.py \
  --packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
  --runtime-dir "$QDL_RUNTIME_DIR"
```

The second command must print `status=PASS`. It verifies packet expiry,
SHA256 image format, authority/route binding, the exact topic/ACL/acceptance
scope, host runtime directory, exact file set and every file digest. It is
not an authorization or runtime health result.

Before approval, render Compose with the six non-secret values copied exactly
from `compose_environment` in the validated packet. The packet values override
same-named values in the existing stable environment file only for the render:

```bash
export QDL_STABLE_RUNTIME_DIR='<packet compose_environment value>'
export QDL_STABLE_PYTHON_IMAGE='sha256:<packet Python image>'
export QDL_STABLE_RUST_IMAGE='sha256:<packet Rust image>'
export QDL_STABLE_AUTHORITY_MODE=RUST_PRIMARY
export QDL_STABLE_AUTHORITY_REVISION='<packet authority revision>'
export QDL_CONFIG_REVISION='<packet config revision>'

docker compose \
  --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env \
  -f docker-compose.v2-stable.yml config -q
```

The render must pass and must not resolve `production_core_*`. A packet is
expired after 30 minutes; generate a fresh packet rather than editing its time,
hash, token, image values or runtime files.

## Approved Broker Scope

After the separate approval only, use the bounded helper. Its default is still
review-only; `--apply` needs the exact token from the sealed packet.

```bash
python -B scripts/phase103_apply_shared_primary_broker_scope.py \
  --packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
  --runtime-dir "$QDL_RUNTIME_DIR" \
  --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env

python -B scripts/phase103_apply_shared_primary_broker_scope.py \
  --apply \
  --confirm 'APPLY_QDL_PHASE103_<packet-prefix>' \
  --packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
  --runtime-dir "$QDL_RUNTIME_DIR" \
  --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env
```

The apply helper is limited to the following idempotent broker actions:

- create or verify `md.raw.realtime.v2` with six partitions, RF=3, min ISR=2,
  producer compression, `cleanup.policy=delete` and unclean election disabled;
- grant `phase8-producer` WRITE/DESCRIBE on that raw topic and
  `IdempotentWrite` at cluster scope;
- grant `phase8-core` READ/DESCRIBE on that raw topic, READ on only
  `qdl-v2-realtime-core-v2`, WRITE/DESCRIBE on `md.canonical.v2` and
  `md.quarantine.stable.v1`, the fixed transactional ID prefix, and
  `IdempotentWrite` at cluster scope.

It has no command path for offsets, deletion, reset, flush, V1, Docker service
control or consumer-route mutation. A policy mismatch aborts the operation.

## Approved Service Handoff

Use the six packet environment values and recreate only these services in this
order. The command must be issued one line at a time and stop on the first
failure. Do not use `down`, `-v`, a wildcard, a profile, or `production_core_*`.

```bash
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate rust_core
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate rust_core_2
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate rust_core_3
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate ingestor_binance_usdm
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate ingestor_okx_swap
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate binance_bar_edge
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate projector_v2 projector_v2_2 projector_v2_3
docker compose --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env -f docker-compose.v2-stable.yml up -d --no-deps --force-recreate query_v2_1 query_v2_2 stream_v2_active stream_v2_passive
```

The environment variables must remain exported for every command. The stable
env file provides secrets; neither it nor a private key belongs in the packet,
logs or evidence.

## 300-Second Acceptance

Observe the full 300 seconds with the existing mTLS read-only inspection
identity and a fresh permitted audit consumer group. Do not commit offsets.
Capture only bounded aggregate evidence and hashes:

1. `md.raw.realtime.v2` has every 12 demanded Binance/OKX binding, zero
   malformed/out-of-scope/identity/revision mismatch records and no test
   provenance.
2. The three Rust cores report `RUST_PRIMARY`, one shared group, strict scope,
   canonical progress and no unexplained quarantines/gaps/duplicates.
3. Query and stream replicas report the sealed configuration revision, V2
   freshness, no demanded-slice gap and bounded projector/consumer lag.
4. Trading System paper adapter and one representative alpha SDK consumer
   receive V2 snapshot, cursor replay and live update without a direct venue
   connection or order submission.
5. The manifest-only `V2_PRIMARY -> V1_FALLBACK -> V2_PRIMARY` drill passes
   with consumer, reason, source age and recovery evidence.
6. Record CPU, RAM, I/O, Kafka lag, cache/event growth, reconnect and fallback
   counts. Any stale demanded feed, unresolved gap, missing binding, divergence
   between query replicas or unintended fallback fails acceptance closed.

VN remains capability-present but excluded from V2 primary until its own
in-session provider admission passes.

## Rollback

If any acceptance gate fails, preserve Kafka/cursor/audit evidence, apply only
the approved V1 consumer-route revision, then stop only the named 13 V2
services. Do not reset or seek Kafka, delete a topic, flush state, restart V1,
or change alpha/order configuration. A later retry requires a new packet and a
new approval; do not edit or reuse an expired packet.

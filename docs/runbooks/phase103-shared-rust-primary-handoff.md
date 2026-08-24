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
6. the packet SHA256 and confirmation token; and
7. the embedded Trading System route-lock SHA256 for its named
   `market_data` consumer.

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
export QDL_STABLE_PYTHON_IMAGE_DIGEST='sha256:<approved-python-image-id>'
export QDL_STABLE_PYTHON_IMAGE_REF="qdl-v2-python@${QDL_STABLE_PYTHON_IMAGE_DIGEST}"
export QDL_STABLE_RUST_IMAGE_DIGEST='sha256:<approved-rust-image-id>'
export QDL_SOURCE_COMMIT='<commit-used-to-build-those-images>'

umask 077
mkdir "$QDL_PACKET_DIR"

docker run --rm --read-only --network none \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace:ro" \
  -v "$QDL_PACKET_DIR:$QDL_PACKET_DIR" \
  -w /workspace \
  "$QDL_STABLE_PYTHON_IMAGE_REF" \
  python -B scripts/phase103_prepare_shared_primary_packet.py \
    --output-dir "$QDL_PACKET_DIR" \
    --host-runtime-dir "$QDL_RUNTIME_DIR" \
    --rust-image-digest "$QDL_STABLE_RUST_IMAGE_DIGEST" \
    --python-image-digest "$QDL_STABLE_PYTHON_IMAGE_DIGEST" \
    --source-commit "$QDL_SOURCE_COMMIT" \
    --actor BobbyAxerol \
    --change-ticket QDL-PHASE103-HANDOFF \
    --observation-seconds 300

docker run --rm --read-only --network none \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --user "$(id -u):$(id -g)" \
  -v "$PWD:/workspace:ro" \
  -v "$QDL_PACKET_DIR:$QDL_PACKET_DIR:ro" \
  -w /workspace \
  "$QDL_STABLE_PYTHON_IMAGE_REF" \
  python -B scripts/phase103_validate_shared_primary_packet.py \
    --packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
    --runtime-dir "$QDL_RUNTIME_DIR"
```

The second command must print `status=PASS`. It verifies packet expiry,
SHA256 image format, authority/route binding, the exact topic/ACL/acceptance
scope, host runtime directory, exact file set and every file digest. It is
not an authorization or runtime health result.

Before an approved Trading System `market_data` recreate, verify the two
candidate artifacts against the packet's `trading_system_handoff.route_lock`.
They must match exactly; a matching service name alone is insufficient:

```bash
export QDL_TRADING_SYSTEM_SOURCE_ROOT=/home/bobby/<reviewed-trading-system-checkout>
export QDL_TRADING_SYSTEM_IMAGE='sha256:<approved-trading-system-image>'

sha256sum \
  "$QDL_TRADING_SYSTEM_SOURCE_ROOT/config/_config/data_layer_v2_routes.yaml" \
  "$QDL_TRADING_SYSTEM_SOURCE_ROOT/docker-compose.data-layer-v2-primary.yml"

docker run --rm --entrypoint sha256sum "$QDL_TRADING_SYSTEM_IMAGE" \
  /app/config/_config/data_layer_v2_routes.yaml \
  /app/docker-compose.data-layer-v2-primary.yml
```

The first and second pair of hashes must agree with the route-lock values in
the validated packet: route revision `2`, exactly the eight BTC/ETH
Binance-USD-M/OKX-Swap TRADE/final-BAR-1m identities, and no wildcard. This is
a read-only artifact check; it neither recreates Trading System nor changes a
consumer route.

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
python3 -B scripts/phase103_apply_shared_primary_broker_scope.py \
  --packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
  --runtime-dir "$QDL_RUNTIME_DIR" \
  --env-file /home/bobby/.local/state/qdl-v2/<current-stable>/stable.env

python3 -B scripts/phase103_apply_shared_primary_broker_scope.py \
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

### Governed Trading System And Alpha SDK Receipt

Run this once, inside the same 300-second approved handoff window, after the
three cores, edges, projectors, query replicas and stream replicas have become
ready. It is a disposable SDK client, not an alpha/container restart and not
an execution action. It connects only to V2 query/stream aliases, validates
the sealed packet before any SDK request, and deletes its local cursor
directory before it exits.

```bash
cd /home/bobby/data_layer

export QDL_RELEASE_ROOT=/home/bobby/.local/state/qdl-v2/<approved-stable-release>
export QDL_PACKET_DIR=/home/bobby/.local/state/qdl-v2/<approved-phase103-packet>
export QDL_RUNTIME_DIR="$QDL_PACKET_DIR/runtime"

docker run --rm --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  --network executor_network \
  -v "$PWD:/workspace:ro" \
  -v "$QDL_RELEASE_ROOT:/bundle:ro" \
  -v "$QDL_PACKET_DIR:$QDL_PACKET_DIR:ro" \
  -w /workspace \
  "$QDL_STABLE_PYTHON_IMAGE" \
  python -B scripts/phase103_consumer_receipt_acceptance.py \
    --handoff-packet "$QDL_PACKET_DIR/shared-primary-handoff-packet.json" \
    --runtime-dir "$QDL_RUNTIME_DIR" \
    --primary-url https://query_v2_1:8200 \
    --secondary-url https://query_v2_2:8200 \
    --grpc-target qdl-v2-stream-a:8210,qdl-v2-stream-b:8210 \
    --tls-ca-file /bundle/identities/trading-system/ca.crt \
    --trading-tls-certificate-file /bundle/identities/trading-system/client.crt \
    --trading-tls-private-key-file /bundle/identities/trading-system/client.key \
    --trading-jwt-private-key-file /bundle/identities/trading-system-jwt/private.key \
    --trading-jwt-key-id stable-trading-system-rs256-v1 \
    --alpha-tls-certificate-file /bundle/identities/alpha-binance/client.crt \
    --alpha-tls-private-key-file /bundle/identities/alpha-binance/client.key \
    --alpha-jwt-private-key-file /bundle/identities/alpha-binance-jwt/private.key \
    --alpha-jwt-key-id stable-alpha-binance-rs256-v1 \
    --timeout-seconds 15 --concurrency 4
```

The one JSON result must be `status=PASS`, have `expected_authority=RUST_PRIMARY`,
match the sealed `packet_sha256`, report exactly `18` products (`16` durable,
`2` pass-through) and no payload/cursor/secret values. It proves:

- `trading-system.paper.stable` receives the full declared Binance USD-M and
  OKX Swap `TRADE`/`QUOTE`/final `BAR 1m` surface through V2;
- `alpha.binance.paper.stable` receives its durable products plus explicit
  `BAR 15m` provider pass-through without falsely claiming durable replay;
- durable products complete snapshot/warmup, stream ACK and cursor-resume;
- immutable final `BAR 1m` content agrees between query replicas, while live
  TRADE/QUOTE are compared for governed typed identity/quality rather than
  unstable byte equality; and
- no direct provider connection, Gateway request or order submission occurs.

Do not redirect the result into a source-controlled file. Retain only its
bounded hash/offset/latency summary under the approved evidence location. A
failure is a fail-closed handoff failure: preserve compact evidence, apply the
pre-reviewed V1 route only if needed, and stop only the 13 approved V2
services. Do not reset broker offsets, delete data, flush caches or restart V1.

## Rollback

If any acceptance gate fails, preserve Kafka/cursor/audit evidence, apply only
the approved V1 consumer-route revision, then stop only the named 13 V2
services. Do not reset or seek Kafka, delete a topic, flush state, restart V1,
or change alpha/order configuration. A later retry requires a new packet and a
new approval; do not edit or reuse an expired packet.

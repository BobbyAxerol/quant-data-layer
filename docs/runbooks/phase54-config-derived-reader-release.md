# Config-Derived Reader Release

## Purpose

This runbook promotes a tested Data Layer `dev` revision into one canonical
Python reader image and a secret-free alpha binding release bundle. It does
not create data-plane demand, a new worker, a new identity, a provider
connection, a paper order, or a broker request.

The bundle is consumed later by a temporary alpha no-order proof. Query and
stream remain shared services; binding files are not a reason to create a
container per alpha, symbol, interval, or venue.

## Immutable Inputs

Record the following before build:

- Data Layer `dev` source SHA and `qdl_sdk` version.
- Execution Alpha inventory SHA generated from actual Compose/config.
- Data Layer catalog, reference manifest, release-routing and policy hashes.
- Existing reader image digest and its four exact role names.

The candidate tag format is `qdl-v2-python:<service-semver>-<git-sha>`.
The Docker image ID, rather than a mutable tag, is the rollout coordinate.
Rollback is a complete immutable mapping for each of the four reader roles;
do not assume the previous query and stream roles share one image.

## Build And Source Gates

1. Build exactly one image from the canonical Phase C worktree with OCI
   revision equal to the recorded source SHA.
2. Inspect the image: revision label, release label, non-root `10001:10001`
   user and immutable image ID must match the packet.
3. Run the selected binding/release/reference/L2/query/stream test matrix from
   that image using `--network none`, read-only root filesystem and tmpfs-only
   test state. No source mount is permitted for the image provenance test.
4. Export the real alpha Compose/config inventory in a disposable no-network
   container. Compile it twice against the candidate's source artifacts. The
   inventory SHA, compilation SHA and every binding SHA must be identical on
   the second run.

## Bundle Layout

Create a new `0700` directory outside the source checkout:

```text
/home/bobby/.local/state/qdl-v2/releases/<semver>-<git-sha>/
  inventory.json
  bindings/
    <deployment>.binding.json
    compilation-report.json
  reader-image.override.yml
  reader-rollback.override.yml
  release-manifest.json
```

`release-manifest.json` contains hashes, IDs, roles, source revision and
provenance only. It must not contain a credential, key, token, provider
payload, cursor, database dump, Redis/Kafka state or log body.

Bindings are intentionally public-to-their-declared-workload control-plane
artifacts: each `bindings/*.binding.json` is immutable mode `0444` so the
non-root alpha UID can parse its sealed route policy. All other bundle files
remain operator-only mode `0640`. A binding never carries TLS/JWT material.

## Workload Identity Mounts

Never make a protected source private key world-readable or run an alpha as the
host user. Before a real alpha handoff, materialize one distinct mount under
the governed state root with `scripts/materialize_alpha_v2_identity.py`:

```text
/home/bobby/.local/state/qdl-v2/workload-identities/<workload-id>/
  tls/  # ca.crt, client.crt: 0444; client.key: 0400
  jwt/  # public.pem: 0444; private.key: 0400
  identity-manifest.json  # operator-only hashes/modes, no secret bytes
```

The source TLS/JWT directories stay private and unchanged. The helper must run
as root only to set the declared immutable container UID/GID ownership (today
alpha `1000:1000`) and performs a staging-directory rename. Mount only `tls/`
and `jwt/` read-only into that alpha; do not mount the parent state root. The
alpha UID must parse its binding and read both private keys in an isolated
no-network container before any V2 provider or Gateway/Risk call.

`reader-image.override.yml` changes all four services to the one candidate
image. `reader-rollback.override.yml` restores the exact pre-roll image for
each service, so either file changes only these service image fields:

```text
query_v2_1
query_v2_2
stream_v2_active
stream_v2_passive
```

The manifest stores `rollback_images` with one immutable reference/ID pair per
role. A missing role, mutable reference, duplicate service, or candidate-equal
rollback ID fails bundle generation. It is appended as the final Compose
override from the canonical checkout `/home/bobby/data_layer`. Existing
security, TLS, authority and bar-edge
override files are retained only after a rendered `docker compose config
--quiet` proves they are still needed. A `/tmp` override or feature worktree
is never a production selector.

## Rolling Packet

Before each serial recreate, capture only bounded metadata: role name, image
ID, health, restart count, RSS, manifest checksum and query/stream lag.

Recreate the two query roles first, then inspect the stream lease and recreate
the currently observed standby followed by the current lease holder:

1. `query_v2_2`
2. `query_v2_1`
3. stream replica with `gateway_lease=STANDBY`
4. stream replica with `gateway_lease=READY`

The query replicas must return `/health/ready` `200`. Stream replicas use a
cooperative lease: the required condition is exactly one `gateway_lease=READY`
and one `gateway_lease=STANDBY`, with every non-lease dependency ready. A
standby deliberately returns `503` from `/health/ready`; inspect its
`/health/dependencies` instead. Lease ownership may move during the serial
roll, so the container name is not an authority assertion. The next role starts
only when this condition holds, every changed role has no restart/OOM state,
all four report the expected image/config generation and stay within their
existing resource limits. If one role fails, stop the sequence and recreate
only that role with the recorded old image and selector set.

V1, Kafka topology/offsets, Redis, SQLite, Rust core, ingestors, bar edge,
projectors, Trading System, alpha services and order paths are excluded.

## Exit And Cleanup

Phase C passes only when the canonical image/bundle provenance is sealed, both
query replicas are ready, the stream pair holds exactly one clean leader and
one clean standby on the new image, and their original phase-name worktree or
temporary image override is not part of the live Compose command. The
subsequent Phase D alpha proof uses real Binance and OKX data but remains
no-order.

Retain the active reader image and one named rollback image. Remove only
disposable test containers, temporary no-order state and unreferenced test
images after reachability checks; record disk before/after in the main plan.

# Phase 7.1 Isolated Beta Runtime Runbook

## Authority And Scope

Phase 7.1 deploys only read-only `query_v2` and active/passive `stream_v2`
roles. V1 remains the sole production authority. Beta does not own venue
connections, write legacy Redis keys/channels, share V1 state paths, migrate a
consumer or become a sole execution dependency.

The stream topology is active/passive per gateway shard. Redis stores a
monotonic fencing epoch and one expiring lease. A passive replica reports
`STANDBY` and rejects stream work. After takeover, any operation carrying the
old epoch fails closed.

## Isolation Contract

- Application images are immutable and run as UID/GID `10001` with a read-only
  root filesystem, dropped capabilities and bounded CPU, memory and PIDs.
- Redis runs non-root with a dedicated beta-only AOF volume. The AOF preserves
  the monotonic fencing epoch across a Redis process restart; it owns only
  lease, fencing and shared request-quota keys below `qdl:beta:v2:*`. The
  volume is never shared with V1 and is deleted by beta rollback/certification.
- Query and stream state use dedicated bounded volumes. They never mount V1
  history, Redis persistence, data or cursor paths.
- Redis is reachable only through `qdl_beta_internal`. Query/stream routes use
  a separate beta ingress network and publish only on host loopback.
- JWT issuer, audience, keyring, cursor keyring, consumer group and audit chain
  are beta-only. Real secrets must come from the deployment secret manager.

## Preflight

1. Resolve the application, Redis and init-helper images to immutable registry
   digests or local `sha256:` image IDs.
2. Confirm every registered beta manifest retains `execution_dependency: FORBIDDEN`.
3. Confirm the V2 OpenAPI digest and cursor TTL match the frozen beta contract.
4. Capture V1 container IDs, image IDs, restart counts, networks, mounts and the
   count of `qdl:beta:v2:*` keys in production Redis.
5. Set independent beta JWT and cursor signing keys. Never reuse V1 secrets.

Required variables are documented in
`config/examples/phase7_beta_runtime.env.example`.

## Start And Readiness

```bash
docker compose -p qdl_phase71_beta \
  -f docker-compose.phase7-beta.yml --profile phase7-beta up -d
```

- `/health/live` only claims that the process is alive.
- `/health/ready` is dependency-derived. Query requires identity, manifest,
  bounded query/durable stores, signer, authority manifest and shared quota.
- Exactly one stream replica must return `READY`; the other must return
  `STANDBY`. Two active replicas or two ready replicas are a failed gate.
- A Redis/quota/lease/signing/store failure returns `NOT_READY` or `STANDBY`;
  it never degrades into an unauthenticated or unbounded service.
- Data routes remain manifest-protected. Phase 7.1 intentionally has no
  consumer data activation; instrument-specific data returns `DATA_NOT_READY`
  until Phase 7.2 binds the approved canonical catalog/query source.

## Failover And Recovery

1. Record active owner and fencing epoch from `/health/dependencies`.
2. Stop only the active stream replica.
3. Require the passive replica to become ready within the bounded lease window.
4. Require the new epoch to be strictly greater than the old epoch.
5. Verify the old owner cannot publish, open, replay or advance a cursor.
6. Verify durable append precedes fan-out and replay resumes any append that was
   fenced before delivery. Never delete the spool to make recovery pass.

Per-partition barriers join replay registration to live fan-out without a gap.
Blocking durable I/O is moved off the event loop; unrelated partitions remain
concurrent. Replay, request bytes, decompression, deadlines, HTTP/RPC
concurrency, subscriber count and outbound buffers are all bounded.

## Reproducible Gate

```bash
make phase71-test
make phase71-topology-test
```

The topology script authenticates a beta request, exercises active/passive
takeover, removes all beta containers/volumes/networks, then compares canonical
V1 topology and production Redis namespace before/after. Any mismatch fails.

## Rollback And Cleanup

```bash
docker compose -p qdl_phase71_beta \
  -f docker-compose.phase7-beta.yml --profile phase7-beta \
  down -v --remove-orphans
```

Revoke beta credentials and confirm no beta project container, network, volume
or `qdl:beta:v2:*` key remains. V1 needs no restart, replay, source resubscribe
or data repair. Phase 7.2 consumer canary and Phase 7.3 beta decision remain
separate approval gates.

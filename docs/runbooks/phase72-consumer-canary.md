# Phase 7.2 Consumer Canary Runbook

## Scope

Phase 7.2 activates one monitoring consumer and then one disposable paper-alpha
consumer against the isolated V2 beta. V1 remains authoritative. The canary is
read-only, uses the V2 SDK, and cannot be an execution dependency.

The only component attached to `bobby_network` is the bounded V1 read-only
bridge. Query and stream roles stay on beta-only networks and share only the
isolated durable spool. The bridge accepts catalog-bound final bars from the V1
API; it cannot call a venue host directly.

## Prerequisites

- The canonical V1 `data_layer` service is healthy on `127.0.0.1:8100`.
- Phase 7.0 and 7.1 gates are green.
- `redis:7.2-alpine` is available locally.
- Use two disposable JWT verification keys to prove credential rotation.
- Never place production credentials in the environment example or evidence.

## Unit And Semantic Gates

```bash
make phase72-test
```

This covers strict catalog identity, final-only authenticated ingest,
idempotency, exact decimals, stale/gap fail-closed behavior, cursor consumer
scope and expiry, bounded slow-consumer recovery, and Phase 7.0/7.1 regression.

## Real V1 Canary

```bash
make phase72-topology-test
```

The command uses real closed BTCUSDT 1m rows from V1. It starts monitoring
before paper alpha, verifies exact V1/V2 parity, rotates credentials, fails over
the active stream gateway, resumes from the applied checkpoint, rebuilds paper
signal state, stops V2 query, proves V1 fallback, then deletes all beta state.

The gate fails unless V1 container identity, image, mounts, networks and restart
count remain exactly unchanged and production Redis has zero `qdl:beta:v2:*`
keys before and after. Evidence is written to
`upgrade/evidence/phase72-topology-canary.json`.

## Persistent Read-Only Bridge

The persistent bridge is opt-in and is not required by the deterministic
topology gate:

```bash
docker compose -p qdl_phase72_canary \
  -f docker-compose.phase7-beta.yml --profile phase7-canary up -d
```

Do not use this profile as a production cutover. It remains V1-authoritative,
beta-only and execution-forbidden.

## Rollback

```bash
docker compose -p qdl_phase72_canary \
  -f docker-compose.phase7-beta.yml --profile phase7-canary \
  down -v --remove-orphans
```

Revoke both disposable JWT keys and the bridge HMAC secret. V1 needs no restart,
replay, schema migration or venue reconnect.

## Non-Goals

- No V2 source authority or execution dependency.
- No production durable consumer group.
- No direct Binance/OKX/DNSE connection from a canary consumer.
- No VN rolling-future activation until expiry/revision ownership is explicit.
- Burst capacity and final `BETA-GO` remain Phase 7.3.

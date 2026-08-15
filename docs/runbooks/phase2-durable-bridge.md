# Phase 2 Durable Bridge Runbook

This runbook applies only to the dark Phase 2 shadow slice. It does not start,
stop or modify the current production `data_layer_service` or
`redis_marketdata` containers.

## Contract And Correctness

```bash
make contract-check
make phase2-test
make rust-test
```

## Isolated Redis Recovery

```bash
QDL_TEST_IMAGE=data-layer:v0.1.0 scripts/phase2_redis_rebuild_smoke.sh
```

The script creates a unique Redis container/network and temporary SQLite spool,
tests projection, Redis restart, `FLUSHDB`, deterministic replay rebuild and
then removes every resource it created. It must never receive a production
Redis URL.

## Read-Only Live Shadow Check

```bash
docker run --rm --network host \
  -v /root/bobby/data_layer:/app -w /app data-layer:v0.1.0 \
  python scripts/phase2_shadow_v1_smoke.py
```

The smoke reads latest BTCUSDT/ETHUSDT USD-M V1 snapshots. Raw, canonical and
projection writes stay inside a temporary local directory. Expected
`production_writes` is exactly `0`.

## Benchmark

```bash
make phase2-benchmark
```

Interpret p50/p95/p99/p99.9 as durable batch acknowledgement latency. Compare
throughput against the selected feed slice, not a broad-universe target. Disk
amplification includes SQLite DB/WAL/SHM. A promotion proposal must include
event-rate burst measurements, replay horizon and recovery requirements.

## Failure Semantics

- Retryable outage: publisher state is `DEGRADED`; bounded retries continue.
- Capacity/disk reserve/corruption/collision: state is `BLOCKED`; no silent
  overwrite or drop is allowed.
- Cursor older than retention: `CursorExpired`; consumer must resnapshot and use
  an explicit new watermark.
- Poison canonicalization: raw event remains durable and a bounded quarantine
  record references its event ID/hash.

## Cleanup

Test scripts clean themselves. Before closing an incident, verify no names with
prefix `qdl_phase2_` remain in `docker ps -a` or `docker network ls`. Production
spool removal follows ADR 0006 and requires an approved authority migration.

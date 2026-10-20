# Quant Data Layer v2.2.5

Compact release: configuration, container definitions, mesh certificates and one Query fix. No Rust binary changed.
Deployed to production in the maintenance window of 2026-10-20 (01:28-02:54Z, forward path, no rollback).
Certificate: [CERTIFICATE.md](CERTIFICATE.md) / [certificate.json](certificate.json): **PASS**. G5 includes the
pre-registered isolated A/B of old vs new Query (hot reads during warmup bursts and memory-maintenance runs: paired
p95/p99 deltas -41.9 / -19.5 ms; warmup responses byte-identical for all 65 TS r11 requirements outside per-request
fields), [query-ab-20261020/](query-ab-20261020/).

## Contents

- **IDR-127**: the repository carries what production runs: catalog r12 (716 bindings, +4 OKX BTC-USD-SWAP inverse
  bindings), consumer manifests TS Paper r11 and alpha-okx r15, routing r29, crypto demand r8, promotion scope r11,
  acquisition r20 (port-less OKX URLs) ([semantic-parity.json](semantic-parity.json)).
- **Versioned compose for the KN roles**: `deploy/compose/qdl-v2-stable-kn.yml` + per-release env files (images by
  digest); healthchecks on stream_kn, market_projector and rust_core; 15 s cadence on Kafka, Query, stable_redis;
  stable_redis log rotation. `scripts/kn_versioned_compose.py parity` compares `cpus` with a 1e-6 vCPU tolerance
  (compose passes `cpus` through float32: 2.25 -> NanoCpus 2249999872).
- **IDR-129**: Query releases warmup memory: warmup bodies leave in 256 KiB slices, refused batch items drop their
  traceback frames, and an idle gc + malloc_trim runs above `QDL_QUERY_MEMORY_SOFT_LIMIT_MIB` (default 448, 0
  disables; at most once per 120 s, only with the large-warmup lane idle). Isolated A/B, 12 x 64 warmups: RSS
  361-445 MiB vs 472 -> 683 MiB before; warmup latency and 429 counts unchanged.
- **Mesh mTLS rotation**: generation 20261019T084200Z (one CA, 365-day leaves, notAfter 2027-10-19, private material
  permission-hardened), replacing the generation that expires 2026-11-20 14:43Z.

## Runtime (docker inspect, 2026-10-20)

| Role | Image |
|---|---|
| query_kn x2 | `qdl-v2-python:2.2.5-9b6747f` `sha256:753604f8abaa866e2de30c7ccaf36168986bc6991550ee326d53e51b83d1550a` |
| ingestors x2, stream_kn x2 | `qdl-v2-rust:2.2.4-9dab224` `sha256:27c79b53...` (unchanged) |
| market_projector x2 | `qdl-v2-rust:2.2.4-c7ce7d3` `sha256:60f83a51...` (unchanged) |
| rust_core x3 | `qdl-v2-rust:2.2.3-rc22-a4e7b3b` `sha256:7c848aad...` (unchanged) |
| kafka1-3, market_cache, stable_redis | `9516fb76...`, `dfa18828...` (unchanged) |
| binance_bar_edge | `qdl-v2-python:2.2.3-rc14-a388537` `sha256:08f356a8...` (held, scoped out) |
| TS writer | `2deb90b0` unchanged (W12-A), new mesh identity, STRICT |
| TS monitor | not started (W14 deferred) |

SDK: `qdl-sdk 2.4.1`, the same wheel as v2.2.4 (`9f79e032...`, byte-identical rebuild).

## Window (2026-10-20)

- Consumer-visible outage **545 s** (writer stop 01:31:04Z -> STRICT READY 64/64 01:40:09Z), including ~2 min of a
  coordinator wait-loop bug; execution-ready 53 = pre-window baseline.
- R1 PASS: 17 roles healthy, RestartCount 0, images = the v2.2.5 env; new TS identity accepted by Query and both
  Streams, the old one refused; Kafka 3 brokers, URP 0; core 3 members; certificates notAfter 2027-10-19.
- R2 PASS (863 checks, 0 problems) after the `cpus` tolerance fix; R3 build receipt PASS.
- R4 60-min passive soak PASS: writer READY 59/59, 0 auth/protocol failures, 0 restarts, Query 263-277 MiB and flat
  over the last 40 min; request latency QUOTE p95 <=25 ms (n=7261), TRADE p95 <=25 ms (n=1800); served event age
  NOT_MEASURED.

## Limits

- The G5 A/B is isolated (one 1-vCPU replica, private cache copy, beside other agents' test containers) and judged on
  paired deltas; it is not a production latency figure. A maintenance run blocks its replica's loop ~130-150 ms, at
  most once per 120 s and only above 448 MiB RSS (it ran 0 times in production after the window).
- **Supported load (IDR-128 OPEN)**: the inherited latency envelope was measured with unrelated heavy stacks paused;
  a mixed portal workload on the same host is outside the certified scope. The window ran with the portal running.
- Inherited from v2.2.4: G11 PASS only under post-hoc A5; G14 CPU above budget (not a gate); IDR-108 open.
- W14 deferred: no monitor / watchdog runs (GHCR read:packages missing).
- Single host, not HA.
- Three stopped TS containers still mount the 2026-08-22 identity; their removal waits for the owner's approval.
- bar edge stays on its v2.2.3 image; inactive client CAs in the consumer bundles expire 2026-11-24 .. 2026-12-26
  and are used by no running consumer.

## Rollback

Old generation + v2.2.4 env until 2026-11-20 14:43Z (rehearsed: 4.5-8.7 min outage). After that date only an
image-only rollback with the new generation remains (not rehearsed). `QDL_QUERY_MEMORY_SOFT_LIMIT_MIB=0` turns the
maintenance hook off without an image change.

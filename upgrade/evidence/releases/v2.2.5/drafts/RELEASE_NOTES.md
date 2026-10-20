# Data Layer v2.2.5 release notes (DRAFT, not released)

Status: DRAFT. v2.2.5 is published only after the maintenance window (`MAINTENANCE_WINDOW_PACKET.md`) passes its
acceptance R1-R4 and the certificate scope (`CERTIFICATE_SCOPE.md`) is issued. Numbers marked TBD come from the
window receipts.

## Contents

- IDR-127: the repository carries what production runs: catalog r12 (716 bindings, +4 OKX BTC-USD-SWAP inverse
  bindings), consumer manifests TS Paper r11 and alpha-okx r15, routing r29, crypto demand r8, promotion scope r11,
  acquisition r20 (port-less OKX URLs) (`semantic-parity.json`).
- Versioned compose for the KN roles: `deploy/compose/qdl-v2-stable-kn.yml` + per-release env files (images by
  digest); healthchecks on stream_kn, market_projector and rust_core; 15 s cadence on Kafka, Query, stable_redis.
- IDR-129: Query releases warmup memory (sliced 256 KiB warmup bodies, request traceback release, idle gc +
  malloc_trim above `QDL_QUERY_MEMORY_SOFT_LIMIT_MIB`, default 448); isolated proof 361-445 MiB over 12 bursts vs
  472 -> 683 MiB before, latency unchanged.
- Mesh mTLS rotation: generation 20261019T084200Z (one CA, 365-day leaves, notAfter 2027-10-19, private material
  permission-hardened), replacing the generation that expires 2026-11-20 14:43Z.

## Runtime after the window

| Role | Image |
|---|---|
| query_kn x2 | qdl-v2-python:2.2.5-9b6747f (sha256:753604f8...), DL dev 9b6747f with IDR-129 |
| rust_core x3, ingestors, stream_kn x2, market_projector x2, Kafka, Redis, bar edge | unchanged v2.2.4 digests |
| TS writer | 2deb90b0 unchanged (W12-A), new mesh identity |
| TS monitor | TS v2.0.1 (W14) |

## Limits (carried into the certificate)

- **Supported load (IDR-128, owner decision 2026-10-19):** the latency envelope was measured with unrelated heavy
  stacks paused; a mixed portal workload on the same host is outside the certified scope. IDR-128 stays OPEN for a
  later host split / IO isolation; no new tuning campaign in this release.
- Single host, not HA (unchanged).
- G5/G6 rest on the IDR-129 delta proof, not on inheritance; the rest per `CERTIFICATE_SCOPE.md` section 2.
- bar edge stays on its v2.2.3 image (catalog r11 / acquisition r19 checkpoint); inactive client CAs in the consumer
  bundles expire 2026-11-24 .. 2026-12-26 and are not used by any running consumer.
- Window outage: TBD (rehearsed 251-519 s over two isolated runs).

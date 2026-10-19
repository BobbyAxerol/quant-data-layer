# Data Layer v2.2.5 certificate scope (DRAFT proposal for the owner)

v2.2.5 is a compact release (owner 2026-10-19 ~09:00Z: "phải gọn lại"; TS is the main work). It changes
configuration, container definitions and the mesh certificates; it does not change any Rust binary. This document
proposes the smallest honest re-verification set so that the v2.2.5 certificate binds the post-window runtime
without re-running the v2.2.4 campaign.

## 1. Runtime deltas against the v2.2.4 certificate

| Delta | Evidence that behaviour is unchanged | Residual risk |
|---|---|---|
| Mesh TLS generation 20260822T144323Z -> 20261019T084200Z (new CA, same CN/SAN per identity, same RSA sizes and openssl profile, 365 days) | `rotate_mesh_tls.py verify` 118/118 PASS; rehearsal B/C (handshakes, principals, refusal of the other generation) | none beyond the procedure, which the rehearsal covers |
| Container definitions from `deploy/compose/qdl-v2-stable-kn.yml` (healthchecks added/retuned, mount sources by generation, stable_redis log rotation) | `kn_versioned_compose.py parity` PASS against live (847 field checks, only the listed intended deltas) | healthchecks are liveness only (IDR-108 open) |
| Query python image 2.2.4-31a5155 -> 2.2.5-<sha7> | build receipt: every /app file equal to 3e9e4d3a except the git-expected set (config/consumers bytes of IDR-127 + apparatus), pip freeze equal; Query reads catalog/acquisition/realm from mounts that do not change | none expected |
| stable_redis recreated (ephemeral quota/session state) | rehearsal B/C | quota counters restart from zero (by design) |
| `__consumer_offsets` dirty ratio 0.05 -> default (K6) | `kafka_internal_topic_compaction.py --remove-temp` verifies the overrides | coordinator load bounded by decision C (unchanged) |
| TS writer image (TS release) | TS closure-equivalence proof (TS certificate) | owned by TS |

Unchanged by digest: core 7c848aad, ingestors/stream 27c79b53, projectors 60f83a51, Kafka 9516fb76, Redis
dfa18828, bar edge 08f356a8 (held, still scoped out).

## 2. Proposed minimal re-verification set

Required for v2.2.5:

1. **R1 window acceptance** (packet section 8, items 1-5) on production, recorded as a receipt.
2. **R2 compose parity after the window**: `parity` with the v2.2.5 env and the new generation against live: PASS.
3. **R3 Query image equivalence**: the build receipt (file and pip-freeze verdicts against 3e9e4d3a).
4. **R4 60-minute passive soak** after the window: writer demanded = ready = 64, 0 refusals, no restart, lag and
   URP normal, and the passive four-latency snapshot (writer request-latency histograms, served age at read)
   inside the v2.2.4 certified envelope. Passive means no load generator, so it does not need the stage-100 host
   quiet window; it is not a substitute for G1-G11, it is the check that nothing regressed.
5. **R5 G13** for the writer image actually deployed: owned by the TS release (its certificate); the DL
   certificate references it. If the writer image is unchanged (fallback W12), G13 is inherited from v2.2.4.

Inherited from v2.2.4 by equivalence (no re-run), following the v2.2.4 `g12-g13-inheritance` pattern:
G1-G11 and G14 (counted run p184-final-campaign-20261019T040311Z under G4-A1, G4-A2, A3, A4 v2, post-hoc A5) and
G12 (isolated fault harness) - the binaries that produce every measured quantity are byte-identical, the Query
image differs only in files Query does not read at runtime, and the TLS change keeps key sizes and protocol
versions. The inheritance is stated per gate in the v2.2.5 certificate with this argument.

Not proposed (owner may request): a stage-50/100 re-run. It would re-measure the same binaries on a host whose
contention (IDR-128) dominates the result; it adds a 30-60 min quiet-host window without testing anything the
deltas above could have changed.

## 3. Deferred, not in v2.2.5

- K3/K4: Stream gateway Kafka fetch: `fetch.wait.max.ms` is env-tunable (`QDL_KN_KAFKA_FETCH_WAIT_MS`, default 10,
  `rust/qdl-stream-gateway/src/main.rs:57`), `fetch.min.bytes=1` is compiled in (`src/reader.rs:75`). Either change
  trades broker fetch CPU against delivery latency, so it needs a production-shaped latency A/B (one variable,
  stage-100 envelope); the window rehearsal runs narrowed data under CPU caps and cannot measure it. Replica-fetch
  broker defaults: same. Deferred, not in v2.2.5.
- K2: core batch_wait 25 -> 50 ms (adds latency). Deferred by the owner.
- IDR-108: Stream readiness before the hot view serves needs a stream gateway code change (Hub hot-ready gate).
- bar edge roll to a python image with catalog r12 / acquisition r20 needs a checkpoint migration for a catalog
  revision change (the migrator refuses it today).
- Inactive client trust anchors in the consumer CA bundles (phase105b external client CAs, notAfter 2026-11-24 to
  2026-12-19; consumer-realms CA 2026-12-26) are kept unchanged: no running consumer uses them; they must be
  re-issued with the identities they belong to when such a consumer is activated.

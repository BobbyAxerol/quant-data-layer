# Data Layer v2.2.5 certificate scope (DRAFT proposal for the owner)

v2.2.5 is a compact release (owner 2026-10-19 ~09:00Z: "phải gọn lại"; TS is the main work). It changes
configuration, container definitions and the mesh certificates, and the Query python image by configuration files
only; no Rust binary changes. This document proposes the smallest honest re-verification set so that the v2.2.5
certificate binds the post-window runtime without re-running the v2.2.4 campaign. Codex review
`p184-v225-bounded-window-review-20261019.md` section D is applied: a per-gate matrix, inheritance conditional on
the W12 branch, data-policy refusals counted apart from auth/protocol failures, READY != execution-eligible, and
each latency quantity named with its clock or marked NOT_MEASURED.

## 1. Runtime deltas against the v2.2.4 certificate

| Delta | Delta proof | Residual |
|---|---|---|
| D1 mesh TLS generation 20260822T144323Z -> 20261019T084200Z (new CA; same CN/SAN per identity; RSA 3072 CA / 2048 leaves as before; same openssl profile; 365 days) | `rotate_mesh_tls.py verify` 118/118 PASS; rehearsal A->B->C->D handshakes, principals and refusal of the other generation | procedure risk only; covered by the rehearsal and the window acceptance |
| D2 container definitions from `deploy/compose/qdl-v2-stable-kn.yml` (healthchecks added/retuned; mount sources by generation; stable_redis log rotation) | `kn_versioned_compose.py parity` PASS (847 field checks; only the intended deltas) | healthchecks are liveness only (IDR-108 open) |
| D3 Query image 2.2.4-31a5155 (3e9e4d3a) -> v2.2.5 rebuild **with the IDR-129 fix** (Query code changes; b7056141 was the IDR-127-only candidate: 16 git-expected config/apparatus files, pip freeze equal) | IDR-129 fix tests on its PR + an isolated 64-concurrent-warmup burst on the fixed Query (RSS peak and return, warmup latency, 429 counts) vs 3e9e4d3a + the new build receipt (changed files = git diff) | Query code changed: delta proof, not inheritance |
| D4 stable_redis recreated (quota/session state, ephemeral) | rehearsal B/C/D: writer admitted and ready after the recreate | quota counters restart from zero (by design) |
| D5 K6 (`__consumer_offsets` dirty ratio) | observed 2026-10-19 10:35Z: the temporary override is already ABSENT (removed 2026-10-16 14:12Z, plan 73687); decision C keys unchanged | none: K6 is a no-op in the window (re-observed in W5) |
| D6 TS writer: W12-A (default) keeps 2deb90b0 and switches only its identity mount; W12-B rolls the TS release image | W12-A: rehearsal (same image, BOOTSTRAP -> STRICT lifecycle); W12-B: TS closure-equivalence + affected correctness/performance proof (TS certificate) | W12-B is outside this DL certificate |

Unchanged by digest: core 7c848aad, ingestors/stream 27c79b53, projectors 60f83a51, Kafka 9516fb76, Redis
dfa18828, bar edge 08f356a8 (held; still scoped out).

## 2. Per-gate matrix

Legend: S = source/image/config the gate measures; Dep = changed dependency; Inh = inherited receipt; Proof = delta
proof that makes the inheritance valid.

| Gate (v2.2.4) | S unchanged? | Dep changed | Inherited receipt | Proof required for v2.2.5 |
|---|---|---|---|---|
| G1 BOOK / G2 TRADE-QUOTE-MARK / G3 BAR transit (stage-100 040311Z) | rust binaries yes; Query D3 config-only | D1, D2, D4 | v2.2.4 counted run under G4-A1, G4-A2, A3, A4 v2, post-hoc A5 | R1 + R4 (regression only); valid for W12-A; W12-B needs the TS proof |
| G4 per-row delivery | yes | D1, D2 | same run | R1 + R4 |
| G5 request latency | **no: Query code (IDR-129)** | D1 (TLS session cost), D2, D3 | v2.2.4 run as baseline only | delta proof: isolated warmup-burst A/B (3e9e4d3a vs fixed image) + R4 request-latency histograms within the certified envelope |
| G6 execution eligibility | Query code changed (IDR-129) | D3 | v2.2.4 run as baseline only | delta proof: IDR-129 tests show no eligibility path change + R1 item 3 (READY and execution-eligible counted separately) |
| G7-G10 (integrity, lossless TRADE, cursor, ordering) | yes | D1, D4 | same run | rehearsal offset/cursor continuity + R1 |
| G11 recovery clock (A5) | yes | D1 | same run | window recovery measured (R1), not a gate re-run |
| G12 isolated fault | binaries yes | none for the harness | g12-g13-inheritance (v2.2.4) | none |
| G13 writer acceptance | W12-A: writer unchanged | D1 identity mount | v2.2.4 G13 76/76 | W12-A: rehearsal strict resume + R1; W12-B: TS re-run |
| G14 CPU | yes | D2 healthchecks (probes are cheaper than before) | v2.2.4 | R4 CPU sample (not a gate) |

Inheritance is conditional: it holds for W12-A and K6 no-op; a W12-B roll moves G1-G3/G5/G13 to the TS evidence.
The v2.2.4 host-isolation condition (IDR-128: measured with unrelated stacks paused) stays a limit until measured
otherwise; nothing here claims capacity beyond v2.2.4.

## 3. Required for v2.2.5 (minimal set)

1. R1 window acceptance (packet section 8) recorded as a receipt.
2. R2 compose parity after the window (v2.2.5 env + new generation vs live): PASS, intended deltas only.
3. R3 Query image: build receipt of the IDR-129 rebuild (changed files = git diff) + the IDR-129 delta proof (warmup
   burst RSS/latency A/B on an isolated Query, its PR tests); not an equivalence claim.
4. R4 60-minute passive soak, regression/availability only (no load added). Reported separately:
   - auth/protocol failures (401, UNAUTHENTICATED, TLS verify, STRICT_RESUME_REFUSAL, HANDOFF_RESUME_REQUIRED): must
     be 0 after W12 settles;
   - data-policy refusals (DATA_STALE, SESSION_STATE, EVENT_AGE, SOURCE_UNAVAILABLE, OPEN_SEQUENCE_GAP): counted per
     feed with cause; quiet-venue staleness is legitimate and is never converted into a fresh price;
   - writer READY slices (demanded vs ready) and execution-eligible execution views, as two numbers;
   - four latency quantities: (1) request latency = writer `native_probe` histograms (basis
     QUERY_REQUEST_START_TO_REDIS_APPLY_DECISION_MONOTONIC); (2) served event age = Query `quality.freshness_ms` in
     the execution views; (3) delivery lag venue -> canonical: NOT_MEASURED passively (inherited from v2.2.4 G4);
     (4) end-to-end venue -> TS cache incl. apply/durable cursor: NOT_MEASURED passively (inherited from G2).
5. R5 G13: inherited for W12-A; TS re-run for W12-B.

Not proposed: a stage-50/100 re-run. It re-measures byte-identical binaries on a host whose contention (IDR-128)
dominates; the owner may still request it.

## 4. Deferred, not in v2.2.5

- K3/K4: `fetch.wait.max.ms` is env-tunable (`QDL_KN_KAFKA_FETCH_WAIT_MS`, default 10,
  `rust/qdl-stream-gateway/src/main.rs:57`), `fetch.min.bytes=1` is compiled in (`src/reader.rs:75`); either change
  trades broker fetch CPU against delivery latency and needs a production-shaped one-variable latency A/B; replica
  fetch broker defaults likewise. K2 (core batch_wait 25 -> 50 ms) deferred by the owner.
- IDR-108: Stream readiness before the hot view serves needs a stream gateway code change.
- bar edge roll to a python image with catalog r12 / acquisition r20: refused today by its checkpoint (offline probe
  2026-10-19: image 2.2.5 catalog 12 / acquisition 20 vs checkpoint 11 / 19); needs a checkpoint migration for a
  catalog revision (the migrator refuses it).
- Inactive client trust anchors in the consumer CA bundles (phase105b external client CAs notAfter 2026-11-24 ..
  2026-12-19; consumer-realms CA 2026-12-26) are kept unchanged: no running consumer uses them; re-issue with the
  identities they belong to when such a consumer is activated.
- Acquisition r20 identity: the repository r20 adds the inverse L2 materialization the core already runs, so its
  identity_digest differs from the deployed r20 file (equal without those two rows). Bump to r21 at the next bar
  edge roll (which needs a checkpoint migration anyway), or keep r20: owner decision.

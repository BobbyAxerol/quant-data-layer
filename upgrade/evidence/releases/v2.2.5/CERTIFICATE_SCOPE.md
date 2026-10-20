# Data Layer v2.2.5 certificate scope

Status: applied. This was the DRAFT proposal used for the window; the issued certificate is `CERTIFICATE.md` /
`certificate.json` (same folder), which records where the evidence met, or did not meet, each item below
(notably D3 (b) "concurrent hot-read latency": NOT_MEASURED, see `CERTIFICATE.md` section 4).

v2.2.5 is a compact release (owner 2026-10-19 ~09:00Z: "phải gọn lại"; TS is the main work). It changes
configuration, container definitions, the mesh certificates and the Query python image (IDR-127 configuration files
plus the IDR-129 Query code fix); no Rust binary changes. The TS writer slot is W12-A (`W12_DECISION.md`): writer
image 2deb90b0 unchanged, identity mount switched. This document proposes the smallest honest re-verification set so that the v2.2.5
certificate binds the post-window runtime without re-running the v2.2.4 campaign. Codex review
`p184-v225-bounded-window-review-20261019.md` section D is applied: a per-gate matrix, inheritance conditional on
the W12 branch, data-policy refusals counted apart from auth/protocol failures, READY != execution-eligible, and
each latency quantity named with its clock or marked NOT_MEASURED.

## 1. Runtime deltas against the v2.2.4 certificate

| Delta | Delta proof | Residual |
|---|---|---|
| D1 mesh TLS generation 20260822T144323Z -> 20261019T084200Z (new CA; same CN/SAN per identity; RSA 3072 CA / 2048 leaves as before; same openssl profile; 365 days) | `rotate_mesh_tls.py verify` 118/118 PASS; rehearsal A->B->C->D handshakes, principals and refusal of the other generation | procedure risk only; covered by the rehearsal and the window acceptance |
| D2 container definitions from `deploy/compose/qdl-v2-stable-kn.yml` (healthchecks added/retuned; mount sources by generation; stable_redis log rotation) | `kn_versioned_compose.py parity` PASS (847 field checks; only the intended deltas) | healthchecks are liveness only (IDR-108 open) |
| D3 Query image 2.2.4-31a5155 (3e9e4d3a) -> v2.2.5 rebuild **with the IDR-129 fix** (Query code changes; b7056141 was the IDR-127-only candidate: 16 git-expected config/apparatus files, pip freeze equal) | (a) IDR-129 fix tests on its PR (warmup success, failed and cancelled requests release what they hold; returned rows/limits/order unchanged); (b) the IDR-129 isolated A/B on a production-volume private cache copy: 3e9e4d3a vs the fixed image, repeated 64-warmup bursts, without manual gc/malloc_trim in the scored arms, RSS per burst (plateau, no per-burst step), warmup latency, 429 counts and concurrent hot-read latency; (c) the final rehearsal's 64-warmup burst on the window-config Query (rehearsal data volume); (d) the build receipt (changed files = git diff, unexpected 0, pip freeze equal) | Query code changed: G5/G6 move from inheritance to this delta proof; a missing item of (b) is NOT_MEASURED and blocks the G5 claim |
| D4 stable_redis recreated (quota/session state, ephemeral) | rehearsal B/C/D: writer admitted and ready after the recreate | quota counters restart from zero (by design) |
| D5 K6 (`__consumer_offsets` dirty ratio) | observed 2026-10-19 10:35Z: the temporary override is already ABSENT (removed 2026-10-16 14:12Z, plan 73687); decision C keys unchanged | none: K6 is a no-op in the window (re-observed in W5) |
| D6 TS writer: W12-A (decided) keeps 2deb90b0 and switches only its identity mount | rehearsal (same image, BOOTSTRAP -> STRICT lifecycle, cursor items unchanged) + window W12 receipts (`kn_window_writer.py render` deltas, `ready`) | none for DL; W12-B is a later TS roll with its own proof |

TS side (outside this DL certificate, recorded for the window): TS v2.0.1 (main 875aeeb1, image digest 3aaa1a79...)
is released and runs only the monitor in this window (W14); the writer stays 2deb90b0 (W12-A). The TS DR inheritance
after v2.2.5 is decided by TS `scripts/p184_dr_equivalence.py` (TS dev 57c94e1b) from the pre-window snapshot taken in
packet step W0.5, the post-window snapshot, this release's image build receipt and `semantic-parity.json`.

Unchanged by digest: core 7c848aad, ingestors/stream 27c79b53, projectors 60f83a51, Kafka 9516fb76, Redis
dfa18828, bar edge 08f356a8 (held; still scoped out).

## 2. Per-gate matrix

Legend: S = source/image/config the gate measures; Dep = changed dependency; Inh = inherited receipt; Proof = delta
proof that makes the inheritance valid.

| Gate (v2.2.4) | S unchanged? | Dep changed | Inherited receipt | Proof required for v2.2.5 |
|---|---|---|---|---|
| G1 BOOK / G2 TRADE-QUOTE-MARK / G3 BAR transit (stage-100 040311Z) | rust binaries yes (Stream/core path); the Query-served parts (warmup, MARK/reference reads) changed with D3 | D1, D2, D4 | v2.2.4 counted run under G4-A1, G4-A2, A3, A4 v2, post-hoc A5 | R1 + R4 (regression only) for the Stream-delivered feeds; the Query-served parts are covered by the G5/G6 delta proof |
| G4 per-row delivery | yes | D1, D2 | same run | R1 + R4 |
| G5 request latency | **no: Query code (IDR-129)** | D1 (TLS session cost), D2, D3 | v2.2.4 run as baseline only, not inherited | delta proof D3 (b) warmup and hot-read latency A/B (3e9e4d3a vs fixed image) + R4 request-latency histograms (writer native_probe) within the certified envelope |
| G6 execution eligibility | Query code changed (IDR-129) | D3 | v2.2.4 run as baseline only, not inherited | delta proof D3 (a): the IDR-129 diff and tests show no eligibility/freshness path change (else the affected path is re-proven) + R1 item 3 and R4 (READY and execution-ready counted separately) |
| G7-G10 (integrity, lossless TRADE, cursor, ordering) | yes | D1, D4 | same run | rehearsal offset/cursor continuity + R1 |
| G11 recovery clock (A5) | yes | D1 | same run | window recovery measured (R1), not a gate re-run |
| G12 isolated fault | binaries yes | none for the harness | g12-g13-inheritance (v2.2.4) | none |
| G13 writer acceptance | W12-A: writer unchanged | D1 identity mount | v2.2.4 G13 76/76 | rehearsal BOOTSTRAP -> STRICT + R1 item 3 |
| G14 CPU | yes | D2 healthchecks (probes are cheaper than before) | v2.2.4 | R4 CPU sample (not a gate) |

Inheritance is conditional: it holds for W12-A (decided) and K6 no-op (W5 compares the exact pre-window state); a
later W12-B roll moves G1-G3/G5/G13 to the TS evidence. G5/G6 are not inherited in any branch (IDR-129).
The v2.2.4 host-isolation condition (IDR-128: measured with unrelated stacks paused) stays a limit until measured
otherwise; nothing here claims capacity beyond v2.2.4.

## 3. Required for v2.2.5 (minimal set)

1. R1 window acceptance (packet section 8) recorded as a receipt.
2. R2 compose parity after the window (v2.2.5 env + new generation vs live): PASS, intended deltas only.
3. R3 Query image: build receipt of the IDR-129 rebuild (changed files = git diff) + the D3 delta proof items (a),
   (b) and (c); not an equivalence claim.
4. R4 60-minute passive soak, regression/availability only (no load added). Reported separately:
   - auth/protocol failures (401, UNAUTHENTICATED, TLS verify, STRICT_RESUME_REFUSAL, HANDOFF_RESUME_REQUIRED): must
     be 0 after W12 settles;
   - data-policy refusals (DATA_STALE, SESSION_STATE, EVENT_AGE, SOURCE_UNAVAILABLE, OPEN_SEQUENCE_GAP): counted per
     feed with cause; quiet-venue staleness is legitimate and is never converted into a fresh price;
   - writer READY slices (demanded vs ready) and execution-eligible execution views, as two numbers;
   - four latency quantities: (1) request latency = writer `native_probe` histograms differenced over the soak
     (basis QUERY_REQUEST_START_TO_REDIS_APPLY_DECISION_MONOTONIC; `scripts/kn_window_writer.py soak`); (2) served
     event age: NOT_MEASURED passively (the writer heartbeat carries no served-age counter; inherited from v2.2.4
     G5/G6, the changed Query path is covered by D3); (3) delivery lag venue -> canonical: NOT_MEASURED passively
     (inherited from v2.2.4 G4); (4) end-to-end venue -> TS cache incl. apply/durable cursor: NOT_MEASURED passively
     (inherited from G2).
5. R5 G13: inherited (W12-A).

Not proposed: a stage-50/100 re-run. It re-measures byte-identical binaries on a host whose contention (IDR-128)
dominates; the owner may still request it.

## 3b. Supported-load limitation (owner decision 2026-10-19 ~18:00Z; same operational condition as v2.2.4)

- The v2.2.4 latency envelope that v2.2.5 inherits (and the R4 comparison uses) was measured with unrelated heavy
  stacks paused (15 portal containers paused, Codex tree stopped). A mixed portal workload on the same host is
  **outside the certified scope**: with the portal running, stage-100 021044Z FAILed G4 and G8, and the 2026-10-19
  portal cpu.weight A/B (arms 111458Z / 123515Z) FAILed G4 in both arms, so CPU weight alone does not restore the
  envelope.
- IDR-128 stays OPEN for a later host split or IO isolation (owner); no new tuning campaign in v2.2.5.
- Production runbook condition: no unrelated heavy workload on the DL host while the certified envelope is claimed;
  the window soak (R4) and any v2.2.5 latency statement name whether the portal was running.

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

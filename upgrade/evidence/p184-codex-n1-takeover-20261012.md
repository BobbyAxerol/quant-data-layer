# P18.4 DL Takeover Alongside N1 - 2026-10-12

Status: DOCS_REVIEWED. Documentation only; runtime acceptance OPEN.
The user approved takeover broadly. The parent delegated this slice to exactly this guide, the
[Unified Plan](../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md) and [ledger](../../CERTIFICATION_LEDGER.md).
The parent owns review/commit and TS N1. This delegated slice performs no runtime action, tests, builds,
observation acquisition, rollout-tool edit or commit; those limits do not narrow the broader takeover approval.
Source/tool repair and versioned tools/tests are already authorized after this slice; runtime remains
separately governed by its exact packet and rollback scope.
Lock expiry does not expand this scope. Historical receipts and sealed packets remain untouched.

Governing documents: [architecture](../quant-data-layer-fund-grade-upgrade-architecture.md),
[DR runtime runbook](p184-dr-runtime-runbook.md), and the active plan's step-1/takeover entries.
The required parallel-review index was read at
`/home/bobby/.worktrees/ts-codex-p184-review/upgrade/P184_CODEX_CONTRIBUTIONS.md`.
Recommendations below are not additional runtime approval or measured results.

## 1. Evidence And Status

Path aliases below are identifiers, not shell instructions:
- P1 = `/home/bobby/.local/state/trading-system/p184-20261005/runtime/p1`
- QDL = `/home/bobby/.local/state/qdl-v2`
- SCRATCH = `/tmp/claude-1001/-home-bobby/798e80a2-9f96-4c92-b157-75b07ed3460c/scratchpad`

| Artifact | Audited status and provenance |
| --- | --- |
| Step-1 source | `98b421f798fca70e82188e30e8fbe4cc8bce5721`, clean before docs, `fix/p184-dl-closure`. Earlier "not committed" wording corrected; historical tests are not rerun. |
| rc17 packet | `P1/packet-rollout-rc17-20261012-r4.md`, SHA-256 `05270acde609d5d9a3ecba6dff007ae6b40bc7ef23918d961f6bdbc40e7edf30`. RUST17 unresolved, required sidecar absent at audit, section H unfilled; "Executed" is not supported by receipts. |
| rc17 build | `SCRATCH/rc17/logs/build-rust.log` ends `CANCELED` / `context canceled`. No completed Rust image digest or rc17 rollout. Python image preparation is not Rust deployment evidence. |
| rc17 receipt location | `QDL/p184-d3-capacity-20261009/rollout-rc17/` absent at audit. Do not invent a successful roll or rollback receipt. |
| Protected tool | `P1/rc17/rollout_rc17.py`, SHA-256 `1ced8a9c1b4fdc182f2b036f69e1d17753b92036c178e25b5c2ebe6df3d1afa8`; static findings only, unchanged. |
| G13 v4.2 | `QDL/p184-g13-20261012T130309Z/summary.json`, SHA-256 `e3663d6e1442707937fc43cb3d501257ea195599e696a2e2624e315e306082fd`. Three successful harness groups; failed dr01-deployed. TS journal records 28/35 closed, verifier CASE_OPEN only. Harness exit success alone is not cell acceptance. |
| G13 v4.3 | `P1/packet-g13-v4.3-20261012.md`, SHA-256 `d052cbd07ec6be35598ca4e5ad78336f7b67d891ee2cf97ad616c1f95b6a6a6d`, matches sealed sidecar. Seven-cell dr01-deployed subset pending, no execution receipt found at audit. Draft check/offline binding parity is not live PASS. |

The previous DL runner journal's "no real G13 run yet" is superseded as a current status by v4.2, not erased.
The v4.3 input correction changes compiled alpha release routing 27 -> 28, not products; Binance JWT stays 16,
OKX 15. Its draft check acknowledges active checkout HEAD != runner pin; use its declared clean detached
DL `daa8841` and TS `f1b52f63` checkouts, not main's dirty N1 tree. No main checkout edits are delegated here.

G13 v4.3 is independent of N1 but pins the pre-rc17 role map `P1/rc16/roles-v4.json` and writer `2beedc04`.
Finish its separately authorized window before changing those pins, or prepare a new approved versioned packet.
It permits one subset run, no second retry, no TS stack/FX seed/alpha up. Verify seven new cells alone and with
v4.2's 28; expected combined subset is 35/35, NOT full G13 or the 336-cell matrix. Preserve the old failure.
Any final-image reuse after rc17/N1 requires explicit affected-path provenance/inheritance disposition.

## 2. rc17 Tool Findings And Proposed Cases

All findings are static, not reproduced production failures. DO NOT edit the tool or packet in this slice.
The parent has authorized source/tool repair under the takeover; versioned successor tools and tests may
follow this docs slice without a new owner-approval gate. Preserve existing seals and record new tool hashes.
Runtime execution remains separately governed by the exact packet, pins, scope and rollback.
No in-place resealing, receipt overwrite, or claim that a docs change repairs executable guardrails.

| ID / source anchor in protected tool | Exact proposed correction | Cases to run later; NOT_EXECUTED |
| --- | --- | --- |
| T1: apply(), main() lines 243-258 | Require compose exit 0 AND inspected immutable image equal to target AND exact allowed config/mount/CPU delta, plus existing readiness/log/budget gates. Never accept old healthy runtime after failed recreation. | Nonzero compose with old healthy role rejects; zero exit with wrong digest rejects; wrong core mount/hash or unexpected config rejects; exact image/config plus all gates accepts. |
| T2: main() mutation through receipt/rollback, lines 243-262 | Capture rollback before mutation; route every catchable post-mutation timeout/error through one bounded rollback attempt. Persist sanitized phase/failure and rollback result; stop the sequence regardless. For uncatchable process loss, leave an explicit interrupted receipt/recovery procedure, not automatic success. | Compose timeout after partial recreate, inspect failure, log-read timeout and receipt-write error each stop and attempt only scoped rollback; pre-mutation failure does not recreate anything; rollback timeout records unresolved state and stops. |
| T3: rollback branch, lines 260-262 | Rollback success requires exit 0, old image digest, restored mounts/config hashes/CPU/environment, stable role state and writer READY 64/64 within budget. Save a durable rollback receipt, not stdout-only health. No cascading roll-forward. | Healthy wrong rollback digest fails; nonzero rollback compose fails; wrong old JSON/CPU fails; writer not ready fails; exact restoration passes. Multiple-core recovery remains the packet's explicit operator decision. |
| T4: candidate(), lines 197-200 | Bind CPU accounting to the frozen pre-roll caps for BOTH ingestors. Sum positive deltas <= 1.5 vCPU across invocations, enforce quarter-core steps and require the specified measured utilization/throttle evidence. A prior raise cannot become a fresh baseline. | Deltas +0.75 and +0.75 allowed with evidence; deltas +1.0 and +0.75 rejected; repeated invocation cannot reset budget; off-grid/nonfinite value, missing evidence or changed baseline rejects; unchanged 0.5 caps need no recreate. |
| T5: log_check()/rebalance_budget(), lines 125/166-170 | Anchor the zero-retry 120 s settle interval to the new core's actual first-progress event, not wall time after readiness polling. Use immutable container identity and bounded timestamped log coverage; missing coverage fails closed. | Retry after first progress but before budget function entry fails; exact boundary handling is fixed in tests; log gap/missing timestamp rejects; only events in the declared roll/window count; clean complete window passes. |

Add regression coverage for the existing seven-role allow-list, forbidden Redis/Kafka/query/projector/bar-edge
roles, no unexpected env/mount changes, dry-run zero writes/calls, survivor fencing, >2 survivor retries/restarts,
>1 new-core retry, stop_drain_failed/inflight failure, and cancellation of further roles after any failure.
Use mocked orchestration and immutable saved inspect fixtures first under the existing takeover authorization
and host-lock discipline. Runtime fault/recovery execution remains exact-packet scoped. All proposed cases
remain NOT_EXECUTED until testing begins; do not substitute them for executed evidence.

The corrected tool still cannot itself authorize missing packet fields or bypass coordinator gates (own lock,
no foreign heavy jobs, load/disk limits, exact role identity and protected-role restart checks).

## 3. Scope And Rollback Blockers

- rc17 seven-role envelope: cores x3 (image + batch2048 JSON), ingestors x2 (image; measured bounded CPU only),
  stream_kn x2 (image). Kafka, stable_redis, market_cache, projectors, query, bar_edge, catalog, realms and writer
  remain excluded, except a separately authorized G11 restart as declared by its packet.
- Keep rc16 core rollback `f41ecb66` plus the original `v2.2.1-0e00c81-recovery/rust_core*.json`; ingestor/stream
  rollback is rc14 `ce629061`, original caps/mounts, streams retaining realm r16. Capture per-role composes before
  each later authorized mutation. Rollback images' continued availability is NOT re-probed by this docs slice.
- Bar-edge per-binding publish is in 98b421f but outside the envelope. Python rc17 differs from deployed rc14 in
  five config/consumer files; acquisition/checkpoint coupling C.19/C.32 is a STOP. A separate scoped proof and
  decision is required; do not roll it incidentally with Rust or discard raw lineage/checkpoint semantics.
- The packet reports writer trace expiry. Re-arm needs a writer recreate, excluded from DL scope. With no trace,
  G1/G2/G7/G10 remain NOT_MEASURED. Main owns any writer packet; DL must not use --allow-no-trace as acceptance.
- Owner's offline recorded-replay gate at 6k/9k per second precedes a live stage-100 gate. Isolated synthetic
  DL throughput in the old journal is not end-to-end provider replay acceptance, N1 qualification or HA proof.
- Repair tool/packet readiness and collect missing evidence before rollout; no release/merge/cleanup is implied.

## 4. Bounded Periodic raw_age Observation Specification

Status: PROPOSED / NOT_EXECUTED. This specifies a later diagnostic window, not a live gate, new approved SLA,
load generator, or authority change. Main must approve/freeze exact collector commands and resource/output
limits below in a separate packet. It can be prepared independently of N1; acquisition must not overlap N1
builds/tests/benchmarks or other foreign heavy work. Do not create or delete the host lock in this docs slice.

Question: do periodic age maxima arise from REST/non-WebSocket batching, delayed WebSocket reads/produce,
core transaction/consumption delay, or a combination? The old ~15-minute/all-six-partition description is a
hypothesis, not proof that six WebSocket partitions stalled simultaneously.

Source semantics at 98b421f:
- `rust/qdl-kafka/src/bin/qdl-realtime-core.rs` Progress/observe_raw_age/record_commit (lines 267-339): total raw age
  retains all frames; WebSocket is split from every other transport (the latter is labeled REST). Reports reset
  after metrics_every_batches, not fixed seconds. Transport totals and per-partition totals are separate, not
  a partition-by-transport cross-tab; no per-event peak timestamp is supplied by SpanSummary.
- `rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs` LaneTelemetry (lines 1120-1274): ten-second progress carries
  read gaps, enqueue/produce spans with max_at_ns, inflight, last-frame age and lane/generation. Read gap includes
  ping/pong and can reflect provider quiet; it is not by itself CPU starvation or loss.
- `SCRATCH/stall15/{obs.py,obs2.py,obs3.py,spikes.py,spikes_300ms.txt}` are retained offline analysis inputs/tools,
  not an acquisition protocol. Aggregate threshold selection (300 ms there) is diagnostic, not a contract gate.

### Window, Sources And Bounds

1. Freeze input hashes, exact observed role/container/image/config IDs, partition ownership/generations, clock
   units, UTC start/end and source retention. Keep bar_edge unchanged to distinguish causes. If split telemetry
   is not deployed, label that boundary NOT_MEASURED; do not deploy it as part of the observer. Use only retained
   provider-derived observations and existing role logs/counters; no new market stream, raw Kafka consumer,
   group offset commit, provider request, Redis MONITOR, key dump or synthetic production input.
2. Prefer bounded retrospective joins on existing evidence. New collection, only after separate approval:
   45 minutes observation, 60 minutes hard wall-clock deadline including setup/flush, lock deadline <=60 minutes.
   Aim to cover at least three predicted quarter-hour boundaries; record actual coverage. No spike or fewer
   usable boundaries yields INCONCLUSIVE, never "stall fixed"; do not extend/retry without a new decision.
3. Reuse an existing bounded host observer, no new container/service/network. Proposed observer cap: one process,
   <=0.1 mean vCPU over each 60-second interval, <=128 MiB RSS; stop if either is exceeded. Sample existing cgroup
   CPU usage/throttling and host pressure at most once per 10 seconds; timestamp reads and retain interval deltas,
   not lifetime percentages. No Kafka/Redis setting changes or extra diagnostic server installation.
4. Read only allowed core x3, ingestor x2 and existing bar-edge structured events since the previous UTC cursor,
   at most once per 30 seconds, at most 10 seconds per collection command. Bound EACH read and the whole run:
   100,000 retained lines OR 128 MiB total, whichever comes first. Stop on cap; record truncation and INCOMPLETE.
   Store sanitized metrics/IDs/hashes only in a fresh approved external evidence directory; never raw inspect
   env, DSNs, tokens, private keys or full provider payloads. No repository/runtime-state writes.
5. A pre-existing canonical observation/TS trace may be joined offline ONLY for its already captured exact time
   range. It does not authorize starting a subscriber, rearming the writer or broadening log capture. Missing
   event identity, clock, transport or partition joins stay explicit; do not infer them from neighboring maxima.

### Attribution And Exit Criteria

- Retain per-event identity/hash (when captured), venue/feed, source/transport, received time, partition/offset,
  generation and core/producer/observer clocks with their meanings. Distinguish producer timestamp from broker
  commit time and read-uncommitted log end from LSO/durable processing. Do not manufacture absent clocks.
- Report total, WebSocket and non-WebSocket raw-age n/min/mean/max with actual report intervals; correlate lane
  read_gap/enqueue/produce maxima and timestamps, commit/hidden/wait spans, frame rates, ownership and cgroup
  deltas. Aggregated correlation is a candidate explanation, not event-level causality or percentile evidence.
- Compare existing feed/transport slices and quiet/burst windows, not mixed maxima. A REST-only peak can explain
  an aggregate spike but cannot clear WebSocket latency without coverage. Simultaneous six-partition attribution
  requires six usable partition/event joins; source telemetry alone cannot supply that cross-tab.
- Record denominators, missing/late/duplicate samples, sample gaps, clock regressions, collection overhead and
  foreign-job intervals. A foreign heavy job taints the window and triggers stop; never grant PASS from it.
- Stop on deadline/output/resource bound, lock ownership loss/expiry, source identity/config drift, any observed
  role restart, unexpected required-source failure, or main's cancellation. No auto restart/recreate/repair.
  Preserve partial evidence with the exact reason. A missing metric fails attribution closed, not the runtime.
- Outcome: ATTRIBUTED only with supporting same-event evidence; otherwise CORRELATED or INCONCLUSIVE/INCOMPLETE/
  TAINTED. No production-ready, latency PASS, release or rollback decision follows solely from this observation.
  Receipt: pinned inputs, UTC bounds, coverage, clock definitions, bounded findings, limitations and cleanup.
- Cleanup for a later approved observer: stop only its process; remove only its owned lock after work ends and
  approved scratch exports; retain sanitized receipt/hash evidence. No broad prune, shared-state deletion or
  deletion of another agent's lock. This docs slice performs none of these actions.

## 5. Independent Handoff And Provenance

The parent owns review/stage/commit of these three delegated documents. Source/tool repair and versioned
tools/tests may proceed after this slice under the existing takeover authorization, without a new owner gate.
Tests remain NOT_EXECUTED until they begin. Runtime work, including G13 and observation, remains separately
exact-packet scoped with the applicable pins, rollback, lock and preflight;
G13 must not race rc17, and measurement must not compete with N1. Final stage-100 waits for replay and trace gates.

Canonical DL: `/home/bobby/data_layer`, `main@5bf5cd540498b82959342c7cfb2ebeaac3007426`.
Active DL worktree: `/home/bobby/.worktrees/data-layer-p184-closure`, `fix/p184-dl-closure@98b421f`.
TS main-owned worktree at audit: `trading-system-next-upgrade`, `feat/v2-rust-first-okx-demo@af6227d9`, dirty;
no edits or current runtime inference from that branch. Review worktree remains active at `9f287f97`.
Stable release evidence: [v2.2.3](releases/v2.2.3/RELEASE_NOTES.md), unchanged; no tag/release operation here.

Recorded, NOT freshly probed, service set from `P1/rc16/roles-v4.json` (full immutable digests there):
- rust_core, rust_core_2, rust_core_3: `f41ecb66`; old recovery core JSONs, batch256.
- query_kn_1/2: `7d43121e`; stream_kn_1/2: `ce629061`, realm bundle `63efd9ff`, Binance16/OKX15.
- ingestor_binance_usdm/ingestor_okx_swap and market_projector_1/2: `ce629061`.
- binance_bar_edge: `08f356a8`; writer market_data_service_p184w: `2beedc04`, READ_AHEAD256 TRADE/QUOTE.
- Seven infra roles are outside this rollout; no runtime/digest/config/restart inventory refreshed here.
Planned rc17 core JSON location `QDL/releases/v2.2.3-rc17-core-batch2048` is not evidence that it is mounted live.
Retain active images and named rc16/rc14 rollback images/configs. No cleanup, disk measurement or worktree removal;
no resources created. Historical local/same-host measurements are not independent-failure-domain/HA evidence.

Verification for this documentation transaction: scoped diff/whitespace/link checks passed; all five protected
tool/packet/seal/receipt hashes unchanged. Exactly three delegated docs dirty, index empty; historical prefixes
preserved except the exact step-1 status correction. Actual commands/results are recorded in the Unified Plan. No tests/builds/probes, no new test counts, no runtime mutations. Proposed cases above are
NOT_EXECUTED. The user permitted exact-match Python editing after both apply_patch forms failed with mountinfo;
only these three files are written. Main owns staging/commit. No source readiness claim is upgraded here.
